"""
Pure Python Generic BER Decoder
================================
XML-driven: works with ANY NE type (MSC, SGSN, GGSN, etc.).
No hardcoded type names, tags, or structure — everything derived from XML.
Custom decode functions imported from decode_functions.py.

Usage:
    python ber_decoder.py <format.xml> <input.dat> <output.csv>
"""
import posixpath
import shutil

import xml.etree.ElementTree as ET
from datetime import datetime
from decode_functions import *
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems

import time
import sys, re, ast, configparser
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Tuple, List
import os
import logging


# Keep a module-level logger (recommended)
LOGGER = logging.getLogger("prism.asn_decoder")   # same name you used in pipeline logging
_schema_ref: Optional[Dict] = None   # set after load_schema; used by decode_primitive


from decode_functions import (
    decode_int, decode_octet_string, decode_octet_time, decode_octet_datetime,
    decode_bcd_time, decode_bcd_datetime, decode_tbcd_string, decode_bcd_string,
    decode_smsc_date, decode_ip
)

def _decode_ip_safe(data: bytes) -> str:
    """
    Decode IP address, handling TLV-wrapped values.
    GSNAddress in SGSN may arrive as [tag][len][ip_bytes] when the
    outer field has custom_type='ip' but the value still contains
    an inner CHOICE/IMPLICIT wrapper (e.g. 8004d4b790d0).
    """
    if len(data) in (4, 16):
        return decode_ip(data)
    # Try stripping one TLV wrapper: [tag 1B][len 1B][value]
    if len(data) > 2:
        inner_len = data[1]
        if not (data[1] & 0x80) and 2 + inner_len == len(data):
            inner = data[2:]
            if len(inner) in (4, 16):
                return decode_ip(inner)
    return decode_ip(data)   # fallback to original


CUSTOM_DECODERS = {
    'int':            decode_int,
    'octet_string':   decode_octet_string,
    'octet_time':     decode_octet_time,
    'octet_datetime': decode_octet_datetime,
    'bcd_time':       decode_bcd_time,
    'bcd_datetime':   decode_bcd_datetime,
    'tbcd_string':    decode_tbcd_string,
    'smsc_date':      decode_smsc_date,
    'ip':             _decode_ip_safe,
    'bcd_string':     decode_bcd_string,
}




# ═══════════════════════════════════════════════════════════════
#  SECTION 1 — BER TLV Parser
# ═══════════════════════════════════════════════════════════════

def read_tag(data: bytes, offset: int) -> Tuple[int, bool, int, int]:
    """Returns (tag_number, is_constructed, tag_class, bytes_consumed)."""
    b              = data[offset]
    tag_class      = (b >> 6) & 0x03
    is_constructed = bool((b >> 5) & 0x01)
    tag_num        = b & 0x1F
    consumed       = 1
    if tag_num == 0x1F:
        tag_num = 0
        while True:
            b = data[offset + consumed]; consumed += 1
            tag_num = (tag_num << 7) | (b & 0x7F)
            if not (b & 0x80): break
    return tag_num, is_constructed, tag_class, consumed


def read_length(data: bytes, offset: int) -> Tuple[int, int]:
    """Returns (length, bytes_consumed). Returns (-1, 1) for indefinite form 0x80."""
    b = data[offset]
    if b == 0x80: return -1, 1
    if b & 0x80:
        n = b & 0x7F
        return int.from_bytes(data[offset+1:offset+1+n], 'big'), 1+n
    return b, 1


def _find_indef_end(data: bytes, offset: int) -> int:
    """
    Scan from offset (byte after 0x80) to matching EOC (0x00 0x00).
    Returns first byte after EOC pair. Handles nested indefinite structures.
    """
    depth, pos = 1, offset
    while pos < len(data) - 1:
        if data[pos] == 0x00 and data[pos+1] == 0x00:
            depth -= 1
            if depth == 0: return pos + 2
            pos += 2; continue
        b    = data[pos]
        is_c = bool((b >> 5) & 1)
        tn   = b & 0x1F
        tl   = 1
        if tn == 0x1F:
            while data[pos + tl] & 0x80: tl += 1
            tl += 1
        lb = data[pos + tl]
        if lb == 0x80:
            if is_c: depth += 1
            pos += tl + 1
        elif lb & 0x80:
            n    = lb & 0x7F
            pos += tl + 1 + n + int.from_bytes(data[pos+tl+1:pos+tl+1+n], 'big')
        else:
            pos += tl + 1 + lb
    return pos


def parse_tlv(data: bytes, offset: int, end: int) -> List[Tuple[int, bool, int, bytes]]:
    """
    Parse all TLV elements in [offset, end).
    Handles definite and indefinite length encoding.
    Returns list of (tag_num, is_constructed, tag_class, value_bytes).
    """
    elements = []
    while offset < end:
        if data[offset] == 0x00: offset += 1; continue
        tag_num, is_c, tag_cls, tl = read_tag(data, offset);   offset += tl
        length,  ll                = read_length(data, offset); offset += ll
        if length == -1:
            indef_end = _find_indef_end(data, offset)
            value     = data[offset: indef_end - 2]
            offset    = indef_end
        else:
            value  = data[offset: offset + length]
            offset += length
        elements.append((tag_num, is_c, tag_cls, value))
    return elements


# ═══════════════════════════════════════════════════════════════
#  SECTION 2 — XML Schema Loader
# ═══════════════════════════════════════════════════════════════


def _parse_tag(tag_str: str) -> Optional[int]:
    if not tag_str: return None
    m = re.match(r'(?:[CP]\s*)?(\d+)', tag_str)
    return int(m.group(1)) if m else None

def _build_tag_map(type_name: str, members: Dict, tag_maps: Dict):
    """
    Build { tag_int -> field_name } for a type and recurse into sub-members.
    For untagged members (tag=None) with Sub_Members (inline CHOICE):
      promote each Sub_Member tag to parent type level, mapped to parent field name.
    """
    tm: Dict[int, str] = {}
    for field_name, fd in members.items():
        if fd['tag'] is not None:
            tm[fd['tag']] = field_name
        elif fd['members']:
            # Untagged inline CHOICE — promote sub tags to parent level
            for sub_name, sd in fd['members'].items():
                if sd['tag'] is not None:
                    tm[sd['tag']] = field_name
        if fd['members']:
            _build_tag_map(field_name, fd['members'], tag_maps)
    tag_maps[type_name] = tm


def load_schema(filename_or_filelike: str) -> Tuple[Dict, Dict]:
    """
    Parse XML format file into:
      schema   : { TypeName -> { type, schema, element, entries, members } }
      tag_maps : { TypeName -> { tag_int -> field_name } }
    ENUMERATED <entry value=N name=X> loaded into entries = {int(N): X}.
    """

    try:
        tree = ET.parse(filename_or_filelike)
        root = tree.getroot()
    except Exception:
        # If it's not a path, treat as XML string
        root = ET.fromstring(filename_or_filelike)


    schema: Dict = {}
    for module in root.findall(".//Module"):
        for te in module.findall("Type"):
            tname = te.get("name")
            td: Dict = {
                "type":    te.get("type", ""),
                "schema":  te.get("schema"),
                "element": te.get("element"),
                "entries": {int(e.get("value")): e.get("name")
                            for e in te.findall("entry")},
                "members": {},
            }
            for me in te.findall("Member"):
                md: Dict = {
                    "type":        me.get("type", ""),
                    "tag":         _parse_tag(me.get("tag", "")),
                    "custom_type": me.get("custom_type"),
                    "skip":        me.get("skip", "False") == "True",
                    "element":     me.get("element"),
                    "members":     {},
                }
                for se in me.findall("Sub_Member"):
                    md["members"][se.get("name")] = {
                        "type":        se.get("type", ""),
                        "tag":         _parse_tag(se.get("tag", "")),
                        "custom_type": se.get("custom_type"),
                        "skip":        se.get("skip", "False") == "True",
                        "members":     {},
                    }
                td["members"][me.get("name")] = md
            schema[tname] = td

    tag_maps: Dict[str, Dict[int, str]] = {}
    for tname, td in schema.items():
        _build_tag_map(tname, td["members"], tag_maps)

    return schema, tag_maps


# ═══════════════════════════════════════════════════════════════
#  SECTION 3 — Generic Structure Discovery
# ═══════════════════════════════════════════════════════════════

class NEStructure:
    """
    Describes the BER navigation structure for any NE type,
    discovered purely from the XML schema.

    Generic BER layout (derived from XML):
      L0: Start CHOICE tag
           -> single record variant   (SEQUENCE)
           -> composite variant        (SEQUENCE OF)
      L1: callModule CHOICE tag (from untagged CHOICE Sub_Members)
           -> eventModule tags         (constructed, from EventModule CHOICE)
      L2: SET/SEQUENCE fields

    For flat NEs (no inner callModule CHOICE):
      L0: Start CHOICE tag -> L1: SET fields directly
    """

    def __init__(self, schema: Dict, tag_maps: Dict):
        self.schema   = schema
        self.tag_maps = tag_maps

        # Discovered from XML:
        self.start_type:         str  = ''
        self.single_tags:        Dict[int, Tuple[str, str]] = {}  # tag->(name,seq_type)
        self.composite_tags:     Dict[int, Tuple[str, str]] = {}  # tag->(name,item_type)
        self.sequence_type:      str  = ''   # main SEQUENCE type name
        self.call_module_field:  str  = ''   # e.g. 'callModule'
        self.call_module_tags:   Dict[int, Tuple[str, str]] = {}  # tag->(name,type)
        self.event_module_field: str  = ''   # e.g. 'eventModule'
        self.event_module_tags:  Dict[int, Tuple[str, str]] = {}  # tag->(name,type)
        self.flat_record:        bool = False # True if no inner callModule CHOICE

        self._discover()

    def _discover(self):
        # 1. Find Start type
        start = None
        for name, td in self.schema.items():
            if td.get('schema') == 'Start':
                start = td; self.start_type = name; break
        if not start:
            print("❌ No Type with schema='Start' found in XML"); sys.exit(1)

        # 2. Classify Start CHOICE members
        for fname, fd in start.get('members', {}).items():
            tag       = fd['tag']
            type_name = fd['type']
            td        = self.schema.get(type_name, {})
            base      = td.get('type', '')
                
            if base == 'SEQUENCE':
                # MSC pattern: SEQUENCE wrapper contains callModule/eventModule inside
                self.single_tags[tag] = (fname, type_name)
                if not self.sequence_type:
                    self.sequence_type = type_name
                    self._discover_inner(td)

            elif base in ('SET', 'SET OF') or (td.get('members') and base not in ('SEQUENCE OF',)):
                # SGSN/GGSN pattern: Start CHOICE member IS the record type directly
                # Each tag maps directly to a concrete SET type — no wrapper
                self.single_tags[tag] = (fname, type_name)
                self.flat_record = True
                # sequence_type stays empty — each tag has its own type

            elif base == 'SEQUENCE OF':
                # Composite: multiple records wrapped in a SEQUENCE OF
                elem = td.get('element') or fd.get('element', '')
                self.composite_tags[tag] = (fname, elem)

    def _discover_inner(self, seq_td: Dict):
        """
        Find callModule (untagged CHOICE) and eventModule (SEQUENCE OF)
        inside the main SEQUENCE type.
        """
        members = seq_td.get('members', {})

        for fname, fd in members.items():
            tag       = fd['tag']
            type_name = fd['type']
            td        = self.schema.get(type_name, {})
            base      = td.get('type', '') if td else type_name

            # Untagged CHOICE = callModule equivalent
            if tag is None and (base == 'CHOICE' or type_name == 'CHOICE'):
                
                self.call_module_field = fname
                # Sub_Members are the actual record type variants
                for sub_name, sd in fd.get('members', {}).items():
                    if sd['tag'] is not None:
                        self.call_module_tags[sd['tag']] = (sub_name, sd['type'])

            # SEQUENCE OF = eventModule equivalent (has element= attribute)
            elif base == 'SEQUENCE OF' or type_name == 'SEQUENCE OF':
                self.event_module_field = fname
                # element type is a CHOICE whose Members are the event variants
                elem_name = (td.get('element') if td else None) or fd.get('element', '')
                elem_td   = self.schema.get(elem_name, {})
                for em_name, em_fd in elem_td.get('members', {}).items():
                    if em_fd['tag'] is not None:
                        self.event_module_tags[em_fd['tag']] = (em_name, em_fd['type'])

        if not self.call_module_tags:
            self.flat_record = True

    def summary(self) -> str:
        lines = [
            f"Start type     : {self.start_type}",
            f"Flat record    : {self.flat_record}",
            f"Single tags    : {sorted(self.single_tags.keys())}  "
            f"-> {[v[0] for v in self.single_tags.values()]}",
            f"Composite tags : {sorted(self.composite_tags.keys())}",
        ]
        if self.flat_record:
            lines.append(
                f"Record types   : {len(self.single_tags)} "
                f"({', '.join(v[1] for v in self.single_tags.values())})"
            )
        else:
            lines += [
                f"Sequence type  : {self.sequence_type}",
                f"CallModule fld : {self.call_module_field}  ({len(self.call_module_tags)} types)",
                f"EventModule fld: {self.event_module_field} ({len(self.event_module_tags)} types)",
            ]
        return '\n   '.join(lines)


# ═══════════════════════════════════════════════════════════════
#  SECTION 4 — Primitive Value Decoder
# ═══════════════════════════════════════════════════════════════

def _is_printable_ascii(data: bytes) -> bool:
    try: return data.decode('ascii').isprintable()
    except: return False


def _resolve_base_type(type_name: str) -> tuple:
    """
    Chase alias chain through schema to find the final base ASN.1 type.
    Returns (base_type_str, type_info_dict, resolved_type_name).
    resolved_type_name is the last named schema type before hitting a keyword.
    e.g. GSNAddress -> IPAddress -> CHOICE
         returns ('CHOICE', IPAddress_info, 'IPAddress')
    """
    ASN1_KEYWORDS = {
        'INTEGER', 'BOOLEAN', 'NULL', 'OCTET STRING', 'BIT STRING',
        'IA5String', 'UTF8String', 'PrintableString', 'NumericString',
        'TBCDString', 'ENUMERATED', 'OBJECT IDENTIFIER',
        'SET', 'SEQUENCE', 'SET OF', 'SEQUENCE OF', 'CHOICE',
    }
    visited: set = set()
    current = type_name
    last_info: Dict = {}
    last_named: str = type_name          # last schema-defined type name
    while current and current not in visited:
        if current in ASN1_KEYWORDS:
            return current, last_info, last_named
        info = (_schema_ref or {}).get(current, {})
        if not info:
            return current, last_info, last_named
        last_info  = info
        last_named = current             # track last named type
        if info.get('entries'):
            return 'ENUMERATED', info, last_named
        base = info.get('type', '')
        if not base or base == current:
            return base or current, info, last_named
        visited.add(current)
        current = base
    return current or type_name, last_info, last_named



def _flatten_choice_tags(type_name: str, schema: Dict, visited: set = None) -> Dict:
    """
    Recursively collect all reachable { tag -> (field_name, fd) } for a CHOICE type.
    Handles untagged intermediate CHOICE members (e.g. IPAddress has untagged
    ipBinaryAddress which itself has tagged iPBinV4Address [0], iPBinV6Address [1]).
    This lets us resolve deeply nested CHOICEs with shared tag namespaces.
    """
    if visited is None: visited = set()
    if type_name in visited: return {}
    visited.add(type_name)
    td = schema.get(type_name, {})
    result: Dict = {}
    for fname, fd in td.get('members', {}).items():
        tag = fd.get('tag')
        if tag is not None:
            result[tag] = (fname, fd)
        else:
            # Untagged CHOICE member — recurse into its sub-type
            sub_type = fd.get('type', '')
            sub_td   = schema.get(sub_type, {})
            if sub_td.get('type') in ('CHOICE', 'SET', 'SEQUENCE'):
                result.update(_flatten_choice_tags(sub_type, schema, visited.copy()))
    return result


def decode_primitive(data: bytes, type_name: str, custom_type: Optional[str]) -> str:
    """
    Decode raw bytes to a human-readable string.

    Priority:
      1. custom_type  → CUSTOM_DECODERS
      2. ENUMERATED   → <entry> values from XML
      3. INTEGER      → unsigned int string
      4. BOOLEAN      → True / False
      5. NULL         → empty string
      6. BIT STRING   → hex
      7. TBCDString   → raw hex
      8. String types → ASCII
      9. Constructive (CHOICE/SET/SEQUENCE resolved via alias) → hex
     10. Default      → printable ASCII or hex
    """
    if not data: return ''

    #print(f"0. Decoding primitive: type_name={type_name}, custom_type={custom_type}, data={data.hex()}")
    # 1. Custom decoder always wins
    if custom_type and custom_type in CUSTOM_DECODERS:
        return str(CUSTOM_DECODERS[custom_type](data) or '')

    # Resolve alias chain once for all remaining checks
    base_type, type_info, resolved_name = _resolve_base_type(type_name)
    #print(f"1. Resolved base type: {base_type}, type_info: {type_info}")
    # 2. ENUMERATED
    if base_type == 'ENUMERATED' or type_info.get('entries'):
        val = int.from_bytes(data, 'big')
        return type_info.get('entries', {}).get(val, str(val))

    # 3. INTEGER
    if base_type == 'INTEGER':
        return str(int.from_bytes(data, 'big'))

    # 4. BOOLEAN
    if base_type == 'BOOLEAN':
        return 'True' if data[0] != 0 else 'False'

    # 5. NULL
    if base_type == 'NULL':
        return ''

    # 6. BIT STRING
    if base_type == 'BIT STRING':
        return data.hex()

    # 7. TBCDString → raw hex
    if base_type == 'TBCDString':
        return data.hex()

    # 8. String types → ASCII
    if base_type in ('IA5String', 'UTF8String', 'PrintableString', 'NumericString'):
        try: return data.decode('ascii', errors='replace').strip()
        except: return data.hex()

    # 9. CHOICE resolved via alias — decode using flattened tag map
    #    Handles deeply nested CHOICEs with untagged intermediate members
    #    e.g. GSNAddress->iPAddress->IPAddress->ipBinaryAddress->iPBinV4Address
    if base_type == 'CHOICE' and _schema_ref:
        # Build flat {tag -> (name, fd)} using the resolved type name
        # e.g. type_name='GSNAddress' resolves to 'IPAddress' (CHOICE)
        # We need 'IPAddress' to find its members, not 'GSNAddress' (which is an alias)
        flat_tags = _flatten_choice_tags(resolved_name, _schema_ref)

        #print(f"2. Flattened CHOICE tags for {resolved_name}: {flat_tags}")

        # Step 1: Try parsing data as a TLV — another CHOICE layer
        if len(data) >= 2:
            try:
                inner = parse_tlv(data, 0, len(data))
                if inner:
                    itag, ic, icl, iv = inner[0]
                    #print(f"3. Trying tag {itag} for type {resolved_name}")
                    if itag in flat_tags:
                        #print(f"3.1 Found matching tag {itag} in flattened CHOICE tags")
                        fname, mfd = flat_tags[itag]
                        return decode_primitive(iv, mfd.get('type',''), mfd.get('custom_type'))
            except Exception:
                pass

        # Step 2: Raw value (CHOICE already resolved in BER) — match by data characteristics
        for tag, (fname, mfd) in flat_tags.items():
            #print("4. i am in CHOICE raw value matching")
            mtype   = mfd.get('type', '')
            mcustom = mfd.get('custom_type')
            mbase, _, _ = _resolve_base_type(mtype)
            if mcustom in CUSTOM_DECODERS:
                return decode_primitive(data, mtype, mcustom)
            if mbase in ('IA5String','UTF8String','PrintableString','NumericString','INTEGER','BOOLEAN')                and _is_printable_ascii(data):
                return decode_primitive(data, mtype, mcustom)

        return data.hex()  # safe fallback

    # 10. Default: printable ASCII or hex
    if _is_printable_ascii(data): return data.decode('ascii')
    return data.hex()

def decode_set_of_primitive(value: bytes, elem_type: str = '') -> str:
    """
    Decode SET OF / SEQUENCE OF whose items are primitive.
    Uses type-aware decode_primitive for each item.
    Items joined with '|'.
    """
    parts = []
    #print(f"Decoding SET OF primitive: elem_type={elem_type}, value={value.hex()}")
    for (_, _, _, iv) in parse_tlv(value, 0, len(value)):
        parts.append(decode_primitive(iv, elem_type, None))
    return '|'.join(parts) if parts else value.hex()


# ═══════════════════════════════════════════════════════════════
#  SECTION 5 — Generic BER Decoder
# ═══════════════════════════════════════════════════════════════

class BERDecoder:

    def __init__(self, schema: Dict, tag_maps: Dict, ne_struct: NEStructure):
        self.schema     = schema
        self.tag_maps   = tag_maps
        self.ne_struct  = ne_struct

    # ── Public entry ──────────────────────────────────────────

    def decode_record(self, raw: bytes) -> List[Dict[str, str]]:
        """
        Decode one top-level BER blob (Start CHOICE).
        Handles both single and composite record variants.
        """
        tag_num, is_c, tag_cls, tl = read_tag(raw, 0)
        length, ll = read_length(raw, tl)
        if length == -1:
            bs       = tl + ll
            l0_value = raw[bs: _find_indef_end(raw, bs) - 2]
        else:
            l0_value = raw[tl+ll: tl+ll+length]

        ns = self.ne_struct

        # Composite variant: SEQUENCE OF — decode each item
        if tag_num in ns.composite_tags:
            _, item_type = ns.composite_tags[tag_num]
            records = []
            for (_, _, _, item_val) in parse_tlv(l0_value, 0, len(l0_value)):
                flat: Dict[str, str] = {}
                self._decode_l1(item_val, flat)
                if flat: records.append(flat)
            return records

        # Single variant — l0_value is the record body
        elif tag_num in ns.single_tags:
            fname, type_name = ns.single_tags[tag_num]
            flat: Dict[str, str] = {}
            if ns.flat_record:
                # SGSN/GGSN: outer tag identifies record type, value = SET body directly
                flat['recordType'] = fname
                self._decode_structured(type_name, l0_value, flat, prefix='')
            else:
                # MSC: value is the L1 SEQUENCE body containing callModule/eventModule
                self._decode_l1(l0_value, flat)
            return [flat] if flat else []

        else:
            return [{'unknown_start_tag': str(tag_num), 'raw': l0_value.hex()}]

    # ── L1: SEQUENCE body ─────────────────────────────────────

    def _decode_l1(self, data: bytes, flat: Dict):
        """
        Decode the body of a SEQUENCE record.

        If flat_record (no inner callModule CHOICE):
          → SET fields appear directly at this level

        Otherwise:
          → ONE callModule TLV (identified by tag in call_module_tags)
            whose VALUE is the callModule SET body
          → ZERO or MORE eventModule TLVs (CONSTRUCTED tags in event_module_tags)
            appearing after the callModule TLV
        """
        ns = self.ne_struct

        if ns.flat_record:
            # No callModule wrapper — decode SET fields directly
            # Determine the SET type from the sequence type's first member
            self._decode_structured(ns.sequence_type, data, flat, prefix='')
            return

        call_decoded = False
        event_idx    = 0

        for (tag_num, is_c, tag_cls, value) in parse_tlv(data, 0, len(data)):

            # eventModule: CONSTRUCTED tag in event_module_tags (after callModule)
            if call_decoded and is_c and tag_num in ns.event_module_tags:
                fname, tname = ns.event_module_tags[tag_num]
                flat[ns.event_module_field] = fname
                # Strip explicit outer wrapper (one extra TLV layer)
                inner    = parse_tlv(value, 0, len(value))
                set_body = inner[0][3] if inner else value
                self._decode_structured(tname, set_body, flat, prefix='')
                event_idx += 1

            # callModule: tag in call_module_tags
            elif not call_decoded and tag_num in ns.call_module_tags:
                fname, tname = ns.call_module_tags[tag_num]
                flat[ns.call_module_field] = fname
                self._decode_structured(tname, value, flat, prefix='')
                call_decoded = True

            else:
                flat[f'unknown_l1_tag_{tag_num}'] = value.hex()

    # ── SET / SEQUENCE body ───────────────────────────────────

    def _decode_structured(self, type_name: str, data: bytes,
                            flat: Dict, prefix: str):
        """Decode a SET/SEQUENCE body using the XML tag map for type_name."""
        schema  = self.schema.get(type_name, {})
        members = schema.get('members', {})
        tags    = self.tag_maps.get(type_name, {})

        for (tag_num, is_c, tag_cls, value) in parse_tlv(data, 0, len(data)):
            field_name = tags.get(tag_num)
            if field_name is None:
                flat[f'{prefix}{type_name}_unk_{tag_num}'] = value.hex()
                continue

            fd = members.get(field_name, {})
            if fd.get('skip', False): continue

            field_type  = fd.get('type', '')
            custom_type = fd.get('custom_type')
            full_key    = f'{prefix}{field_name}'

            # Untagged CHOICE member: store value as hex under parent field name
            if fd.get('tag') is None and fd.get('members'):
                flat[full_key] = value.hex()
                continue

            # Resolve alias chain to find actual base type
            # e.g. field_type='GSNAddress' -> resolved_base='CHOICE'
            # e.g. field_type='ListOfTV'   -> resolved_base='SEQUENCE OF'
            resolved_base, _, resolved_name = _resolve_base_type(field_type)
            schema_type = self.schema.get(field_type, {}).get('type', '') or                           (field_type if field_type in
                           ('SET', 'SEQUENCE', 'SET OF', 'SEQUENCE OF', 'CHOICE') else '')
            is_structured = (
                is_c or
                schema_type in ('SET', 'SEQUENCE', 'SET OF', 'SEQUENCE OF', 'CHOICE') or
                resolved_base in ('SET', 'SEQUENCE', 'SET OF', 'SEQUENCE OF', 'CHOICE')
            )
            if is_structured:
                self._decode_constructed(full_key, field_type, fd, value, flat, prefix)
            else:
                flat[full_key] = decode_primitive(value, field_type, custom_type)

    # ── Constructed (nested) field ────────────────────────────

    def _decode_constructed(self, full_key: str, field_type: str,
                              fd: Dict, value: bytes, flat: Dict, prefix: str):
        """Route a constructed TLV to the correct nested decoder."""
        schema    = self.schema.get(field_type, {})
        # field_type may be an inline keyword ('SEQUENCE OF', 'SET OF', 'SET', 'SEQUENCE')
        # rather than a named Type — use it directly as base_type in that case
        base_type = schema.get('type', '') or                     (field_type if field_type in
                     ('SET', 'SEQUENCE', 'SET OF', 'SEQUENCE OF', 'CHOICE') else '')

        if base_type in ('SEQUENCE OF', 'SET OF'):
            # element type: from named Type schema, or from Member element= attr
            elem_type = schema.get('element') or fd.get('element', '') or field_type
            elem_info = self.schema.get(elem_type, {})
            elem_base = elem_info.get('type', '')
            elem_tags = self.tag_maps.get(elem_type, {})
            first_items = parse_tlv(value, 0, len(value))
            first_is_constructed = first_items[0][1] if first_items else False


            if not first_is_constructed and                (elem_base in ('OCTET STRING', 'TBCDString', 'IA5String', 'INTEGER')
                or not elem_tags):
                # All items are primitive — join hex with '|'
                flat[full_key] = decode_set_of_primitive(value, elem_type)
            else:
                # Items are constructed (SEQUENCE/SET) — decode each into temp dict
                # then merge into flat, appending repeated field names with '|'
                for idx, (it, ic, icl, iv) in enumerate(first_items):
                    item_flat: Dict[str, str] = {}
                    if ic and elem_tags:
                        # Schema has tag map for element type — full named decode
                        self._decode_structured(elem_type, iv, item_flat, prefix='')
                    elif ic:
                        # No schema for element type — decode inner TLVs by tag number
                        # e.g. ChangeOfCharCondition with context tags 1,2,3...
                        for (t2, ic2, tc2, v2) in parse_tlv(iv, 0, len(iv)):
                            k2 = f'{elem_type}_tag{t2}'
                            item_flat[k2] = v2.hex() if ic2 else                                 decode_primitive(v2, '', None)
                    else:
                        item_flat[elem_type] = decode_primitive(iv, elem_type, None)
                    # Merge into flat record — append with '|' if key already exists
                    for k, v in item_flat.items():
                        flat[k] = flat[k] + '|' + v if k in flat else v

        elif base_type in ('SET', 'SEQUENCE'):
            self._decode_structured(field_type, value, flat, prefix=prefix)

        elif base_type == 'CHOICE':
            inner = parse_tlv(value, 0, len(value))
            if not inner: return
            itag, ic, icl, iv = inner[0]

            # Resolve the CHOICE schema — handle alias and untagged intermediate members:
            #   GSNAddress (alias) -> IPAddress (CHOICE, members may be untagged)
            #   IPAddress members: ipBinaryAddress (no tag) -> IPBinaryAddress (CHOICE, tags 0,1)
            #                      ipTextRepAddress (no tag) -> IPTextRepAddress (CHOICE, tags 2,3)
            _, _, rname = _resolve_base_type(field_type)
            resolved_schema  = self.schema.get(rname, schema)
            resolved_members = resolved_schema.get('members', {})

            # Find which direct member of the resolved CHOICE owns itag
            # by checking each member's own tag map (handles untagged intermediates)
            sub_field = None
            sub_fd    = {}
            for mname, mfd in resolved_members.items():
                mtag = mfd.get('tag')
                if mtag == itag:
                    sub_field = mname; sub_fd = mfd; break
                elif mtag is None:
                    # Untagged intermediate — check if itag lives in its sub-type
                    mtype   = mfd.get('type', '')
                    mtags   = _flatten_choice_tags(mtype, self.schema)
                    if itag in mtags:
                        sub_field = mname; sub_fd = mfd; break

            if sub_field is None:
                flat[full_key] = iv.hex()
                return

            sub_type = sub_fd.get('type', '')
            sub_info = self.schema.get(sub_type, {})
            sub_base = sub_info.get('type', '')

            # Resolve custom_type: inner variant wins, outer field as fallback
            effective_custom = sub_fd.get('custom_type') or fd.get('custom_type')
            schema = resolved_schema
            sub_members = resolved_members

            if ic and sub_base in ('SET', 'SEQUENCE') and sub_info.get('members'):
                # Structured CHOICE variant with named sub-fields — write variant name
                flat[full_key] = sub_field
                self._decode_constructed(
                    f'{prefix}{sub_field}', sub_type, sub_fd, iv, flat, prefix)
            elif ic:
                # Constructed CHOICE variant (nested CHOICE, SEQUENCE OF, etc.)
                # Merge effective custom_type into sub_fd so it propagates
                merged_fd = {**sub_fd, 'custom_type': effective_custom}
                self._decode_constructed(full_key, sub_type, merged_fd, iv, flat, prefix)
            else:
                # Primitive value — sub_type may itself be a CHOICE that was already
                # resolved in BER (no further TLV wrapping). Use effective_custom.
                flat[full_key] = decode_primitive(iv, sub_type, effective_custom)

        else:
            if self.tag_maps.get(field_type):
                self._decode_structured(field_type, value, flat, prefix=prefix)
            else:
                flat[full_key] = decode_primitive(value, field_type, fd.get('custom_type'))


def read_int_from_bytes(data, pos, length, byteorder="big"):
    """
    Read integer from data[pos : pos + length].
    """
    if length <= 0:
        return 0

    end = pos + length

    if pos < 0 or end > len(data):
        raise ValueError(
            f"Invalid integer read. pos={pos}, length={length}, data_len={len(data)}"
        )

    return int.from_bytes(data[pos:end], byteorder=byteorder)

def find_cdr_length(data,pos,length,cdr_header_size,byteorder="big"):

    cdr_pos = pos  + cdr_header_size 
    tag_num, is_c, tag_cls, tl = read_tag(data, cdr_pos)
    cdr_length, ll = read_length(data, cdr_pos + tl)
    if cdr_length == -1:
        total_len = read_int_from_bytes(data, pos, length, byteorder=byteorder)
    else:
        total_len = cdr_length + tl +ll
    return total_len


def extract_full_asn_payload_from_binary_mix(
    binary_dump,
    

    # File Header
    FileHeaderSizeIsDynamic=False,
    FileHeaderSize=0,
    FileHeaderSizePos=0,
    FileHeaderSizeLength=0,

    # CDR Header - before each ASN record
    CDRHeaderSizeIsDynamic=False,
    CDRHeaderSize=0,
    CDRHeaderSizePos=0,
    CDRHeaderSizeLength=0,

    # ASN Payload Size
    CDRSizeIsDynamic=False,
    CDRSize=0,
    CDRSizePos=0,
    CDRSizeLength=0,

    byteorder="big",
    debug=False
):
    """
    Extract full ASN payload from binary + ASN mixed dump.

    Expected structure:

        FILE_HEADER
        CDR_HEADER_1
        ASN_PAYLOAD_1
        CDR_HEADER_2
        ASN_PAYLOAD_2
        CDR_HEADER_3
        ASN_PAYLOAD_3
        ...

    Returns:
        bytes -> complete ASN payload joined together

    Notes:
        - FileHeaderSizePos is relative to file start.
        - CDRHeaderSizePos is relative to each record start.
        - CDRSizePos is relative to each record start.
        - record_start means start of CDR header.
        - CDRSize means ASN payload size only.
    """

    if binary_dump is None:
        return b""

    # If hex string is passed
    if isinstance(binary_dump, str):
        binary_dump = bytes.fromhex(binary_dump)

    if not isinstance(binary_dump, (bytes, bytearray)):
        raise TypeError("binary_dump must be bytes, bytearray, or hex string")

    data = bytes(binary_dump)
    data_len = len(data)


    # --------------------------------------------------
    # Step 1: Calculate File Header Size
    # --------------------------------------------------
    if FileHeaderSizeIsDynamic:
        file_header_size = read_int_from_bytes(
            data,
            FileHeaderSizePos,
            FileHeaderSizeLength,
            byteorder=byteorder
        )
    else:
        file_header_size = FileHeaderSize
    print(file_header_size)
    if file_header_size < 0 or file_header_size > data_len:
        raise ValueError(
            f"Invalid FileHeaderSize={file_header_size}, data_len={data_len}"
        )

    if debug:
        print("========== FILE HEADER ==========")
        print(f"Total file size            : {data_len}")
        print(f"FileHeaderSizeIsDynamic    : {FileHeaderSizeIsDynamic}")
        print(f"FileHeaderSize             : {file_header_size}")
        print(f"First CDR header starts at : {file_header_size}")
        print(f"File header hex            : {data[:file_header_size].hex().upper()}")

    # This will hold only ASN payloads
    full_asn_payload_parts = []

    # First record starts after file header
    pos = file_header_size
    record_no = 1

    # --------------------------------------------------
    # Step 2: Process each record
    # --------------------------------------------------
    while pos < data_len:

        record_start = pos  # This is CDR header start

        # ----------------------------------------------
        # Calculate CDR Header Size
        # ----------------------------------------------
        if CDRHeaderSizeIsDynamic:
            cdr_header_size = read_int_from_bytes(
                data,
                record_start + CDRHeaderSizePos,
                CDRHeaderSizeLength,
                byteorder=byteorder
            )
        else:
            cdr_header_size = CDRHeaderSize

        if cdr_header_size < 0:
            raise ValueError(
                f"Invalid CDRHeaderSize={cdr_header_size} at record {record_no}"
            )

        cdr_header_start = record_start
        cdr_header_end = record_start + cdr_header_size
        asn_start = cdr_header_end

        if asn_start > data_len:
            raise ValueError(
                f"ASN start exceeds file size. "
                f"record_no={record_no}, "
                f"record_start={record_start}, "
                f"cdr_header_size={cdr_header_size}, "
                f"asn_start={asn_start}, "
                f"data_len={data_len}"
            )

        # ----------------------------------------------
        # Calculate ASN Payload Size
        # ----------------------------------------------
        if CDRSizeIsDynamic:
            asn_payload_size = find_cdr_length(
                data,
                record_start + CDRSizePos,
                CDRSizeLength,
                cdr_header_size,
                byteorder=byteorder
            )
        else:
            asn_payload_size = CDRSize

        if asn_payload_size < 0:
            raise ValueError(
                f"Invalid CDRSize={asn_payload_size} at record {record_no}"
            )

        # If CDRSize is 0, consume remaining file as ASN payload
        if asn_payload_size == 0:
            asn_end = data_len
        else:
            asn_end = asn_start + asn_payload_size

        if asn_end > data_len:
            raise ValueError(
                f"ASN end exceeds file size. "
                f"record_no={record_no}, "
                f"asn_start={asn_start}, "
                f"asn_payload_size={asn_payload_size}, "
                f"asn_end={asn_end}, "
                f"data_len={data_len}"
            )

        # Extract only ASN payload
        asn_payload = data[asn_start:asn_end]

        # Append to final ASN payload
        full_asn_payload_parts.append(asn_payload)

        if debug:
            print("")
            print(f"========== RECORD {record_no} ==========")
            print(f"Record start / CDR header start : {record_start}")
            print(f"CDRHeaderSizeIsDynamic          : {CDRHeaderSizeIsDynamic}")
            print(f"CDRHeaderSize                   : {cdr_header_size}")
            print(f"CDR header start                : {cdr_header_start}")
            print(f"CDR header end                  : {cdr_header_end}")
            print(f"CDR header hex                  : {data[cdr_header_start:cdr_header_end].hex().upper()}")
            print(f"ASN payload start               : {asn_start}")
            print(f"CDRSizeIsDynamic                : {CDRSizeIsDynamic}")
            print(f"ASN payload size                : {len(asn_payload)}")
            print(f"ASN payload end                 : {asn_end}")
            print(f"ASN first 30 bytes              : {asn_payload[:30].hex().upper()}")

        # Next record starts after current ASN payload.
        # This position should be next CDR header.
        pos = asn_end
        record_no += 1

        # If size was not available, we consumed remaining file
        if asn_payload_size == 0:
            break

    # --------------------------------------------------
    # Step 3: Return full ASN payload as one bytes object
    # --------------------------------------------------
    full_asn_payload = b"".join(full_asn_payload_parts)

    if debug:
        print("")
        print("========== FINAL ASN PAYLOAD ==========")
        print(f"Total ASN payload size : {len(full_asn_payload)}")
        print(f"Total records extracted: {record_no - 1}")
        print(f"ASN first 50 bytes     : {full_asn_payload[:50].hex().upper()}")

    return full_asn_payload




def _join_path(base: str, name: str) -> str:
    """Join base + filename for local paths and gs:// paths safely."""
    if base.startswith("gs://"):
        return posixpath.join(base.rstrip("/"), name.replace("\\", "/"))
    return os.path.join(base, name)


def _move_file(src: str, dest_dir: str) -> str:
    """
    Move a file from src to dest_dir for both local and GCS.
    GCS move = copy + delete.
    Returns destination path.
    """
    base = os.path.basename(src).replace("\\", "/")
    dest = _join_path(dest_dir, base)

    # Create dest dir for local filesystem
    if not dest.startswith("gs://"):
        os.makedirs(dest_dir, exist_ok=True)

    # Beam FileSystems supports both local and gs://
    try:
        FileSystems.copy([src], [dest])
        FileSystems.delete([src])
    except Exception:
        # Fallback local rename if FileSystems fails (rare)
        if not src.startswith("gs://") and not dest.startswith("gs://"):
            shutil.move(src, dest)
        else:
            raise

    return dest


def decode_bytes_to_dicts(decoder,
                          binary_data,
                          required_fields=None,
                          add_filename=None,
                          max_records=None,
                          strict=False      # PRISM: True = raise on a bad record instead of silently stopping
                         ):
    """
    Generator: yields dict per decoded record.

    required_fields: list of column names you want in output dict
    add_filename: if provided, sets DIM_FILENAME
    """
        
    # ═══════════════════════════════════════════════════════════════
    #  SECTION 6 — Main
    # ═══════════════════════════════════════════════════════════════



    offset, record_count = 0, 0



    while offset < len(binary_data):
        if max_records not in (None, 'Full') and isinstance(max_records, int):
            if record_count >= max_records: break

        while offset < len(binary_data) and binary_data[offset] == 0x00:
            offset += 1
        if offset >= len(binary_data): break

        try:
            tag_num, is_c, tag_cls, tl = read_tag(binary_data, offset)
            
            length, ll = read_length(binary_data, offset + tl)
            if length == -1:
                bs        = offset + tl + ll
                total_len = _find_indef_end(binary_data, bs) - offset
            else:
                total_len = tl + ll + length

            results = decoder.decode_record(binary_data[offset: offset + total_len])


            

                        
            for result in results:
                if required_fields:
                    row = {k: result.get(k, "") for k in required_fields}
                else:
                    row = result.copy()
                        
            
            if add_filename:
                row["filename"] = add_filename

                    
            yield row

            
            offset += total_len
            record_count += 1

        except Exception as e:
            if strict:
                raise RuntimeError(f"BER decode failed at offset {offset} (record #{record_count}): {e}") from e
            print(f"Error at offset {offset}: {e}")
            import traceback; traceback.print_exc()
            break




class AsnDecodeDoFn(beam.DoFn):



    def __init__(
        self,
        format_xml_path,
        required_fields=None,
        ne_name="Dummy",
        max_records=None,
        sample_debug_rows=1,
        archive_prefix=None,          # e.g. r"C:\done" or "gs://bucket/done"
        move_after_decode=True,
        
        AsnHavingBinaryMix=False,

        # File Header
        FileHeaderSizeIsDynamic=False,
        FileHeaderSize=0,
        FileHeaderSizePos=0,
        FileHeaderSizeLength=0,

        # CDR Header - before each ASN record
        CDRHeaderSizeIsDynamic=False,
        CDRHeaderSize=0,
        CDRHeaderSizePos=0,
        CDRHeaderSizeLength=0,

        # ASN Payload Size
        CDRSizeIsDynamic=False,
        CDRSize=0,
        CDRSizePos=0,
        CDRSizeLength=0,

        byteorder="big",
        debug=False
    ):

        self.format_xml_path = format_xml_path
        self.required_fields = required_fields
        self.ne_name = ne_name

        
        self.archive_prefix = archive_prefix.rstrip("/\\") if archive_prefix else None
        self.move_after_decode = move_after_decode
        self.delete_input_source = not(move_after_decode)


        # Optional controls
        self.max_records = max_records          # allow override from config
        self.sample_debug_rows = sample_debug_rows

        self.AsnHavingBinaryMix = AsnHavingBinaryMix
        # File Header
        self.FileHeaderSizeIsDynamic = FileHeaderSizeIsDynamic
        self.FileHeaderSize = FileHeaderSize
        self.FileHeaderSizePos = FileHeaderSizePos
        self.FileHeaderSizeLength = FileHeaderSizeLength

        # CDR Header - before each ASN record
        self.CDRHeaderSizeIsDynamic = CDRHeaderSizeIsDynamic
        self.CDRHeaderSize = CDRHeaderSize
        self.CDRHeaderSizePos = CDRHeaderSizePos
        self.CDRHeaderSizeLength = CDRHeaderSizeLength

        # ASN Payload Size
        self.CDRSizeIsDynamic = CDRSizeIsDynamic
        self.CDRSize = CDRSize
        self.CDRSizePos = CDRSizePos
        self.CDRSizeLength = CDRSizeLength

        self.byteorder = byteorder
        self.debug = debug



        # Runtime caches (per worker)
        self.format_xml_text = None
        

    def setup(self):
        # Called once per worker
        t0 = time.time()

        global _schema_ref
        
        
        self.format_xml_text = self._read_text(self.format_xml_path)
        self.schema, self.tag_maps = load_schema(self.format_xml_text)
        print(f"   {len(self.schema)} types, {sum(len(v) for v in self.tag_maps.values())} tag mappings.")

        _schema_ref = self.schema

        print("Discovering NE structure from XML...")
        self.ne_struct = NEStructure(self.schema, self.tag_maps)
        print(f"   {self.ne_struct.summary()}")

        self.decoder = BERDecoder(self.schema, self.tag_maps, self.ne_struct)
   

        LOGGER.debug(
            "[%s] Decoder setup complete. format_xml_path=%s setup_ms=%d",
            self.ne_name,
            self.format_xml_path,
            int((time.time() - t0) * 1000),
        )

    def _read_text(self, path):
        if path.startswith("gs://"):
            with FileSystems.open(path) as f:
                return f.read().decode("utf-8")
        else:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()

    def process(self, rf):
      
        
        
        inner_path = rf.metadata.path
        if inner_path.startswith("gs://"):
            filename_only = posixpath.basename(inner_path)
        else:
            filename_only = os.path.basename(inner_path)

        t0 = time.time()
        out_count = 0
        sampled = 0

        try:
            with rf.open() as f:
                binary_dump = f.read()
                if self.AsnHavingBinaryMix:
                    binary_dump = extract_full_asn_payload_from_binary_mix(
    
                    binary_dump,
                  
                    # File Header
                    self.FileHeaderSizeIsDynamic,
                    self.FileHeaderSize,
                    self.FileHeaderSizePos,
                    self.FileHeaderSizeLength,

                    # CDR Header - before each ASN record
                    self.CDRHeaderSizeIsDynamic,
                    self.CDRHeaderSize,
                    self.CDRHeaderSizePos,
                    self.CDRHeaderSizeLength,

                    # ASN Payload Size
                    self.CDRSizeIsDynamic,
                    self.CDRSize,
                    self.CDRSizePos,
                    self.CDRSizeLength,

                    self.byteorder,
                    self.debug)

                for row in decode_bytes_to_dicts(
                    self.decoder,
                    binary_dump,
                    required_fields=self.required_fields,
                    add_filename=filename_only,
                    max_records=self.max_records if isinstance(self.max_records, int) else None
                ):
                    out_count += 1

                    # Optional: log first few rows at DEBUG only
                    if self.sample_debug_rows and sampled < self.sample_debug_rows:
                        LOGGER.debug("[%s] Sample decoded row (%s #%d): %s",
                                    self.ne_name, filename_only, out_count, row)
                        sampled += 1

                    yield row

                # ---- End log (success) ----
                LOGGER.info(
                    "[%s] Decode done. inner=%s records=%d elapsed_ms=%d",
                    self.ne_name,
                    filename_only,
                    out_count,
                    int((time.time() - t0) * 1000),
                )



        except Exception:
            LOGGER.exception(
                "[%s] Decode FAILED. src=%s  path=%s after_records=%d",
                self.ne_name,
                filename_only,
                inner_path,
                out_count,
            )
            raise
        
        finally:
            # IMPORTANT:
            # rf.metadata.path is the *source* file you matched.
            # If inner_path == source path, do NOT delete/move it unless you really want to archive inputs.
            src_path = rf.metadata.path

            if self.move_after_decode and self.archive_prefix and inner_path:
                
                # Avoid moving the original input unless that's intended:
                
                try:
                    dest = _move_file(inner_path, self.archive_prefix)
                    LOGGER.info("[%s] Moved decoded file to archive: %s -> %s",
                                    self.ne_name, inner_path, dest)
                except Exception:
                    LOGGER.exception("[%s] Failed to move decoded file: %s",
                                    self.ne_name, inner_path)

            elif self.delete_input_source and inner_path:
                # only delete temp/extracted files, not original input
                try:
                    os.remove(inner_path)
                except Exception:
                    pass
                    