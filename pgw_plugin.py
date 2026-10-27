"""
pgw_plugin.py - PGW source plugin. Uses YOUR existing code unchanged:
    beam_ber_asn_decoder.AsnDecodeDoFn / decode_bytes_to_dicts   (BER/ASN.1 decoder)
    transformation_PGW.transform_record                         (lookups + transformation)

Differences from the old pipeline_pgw.py flow:
  * archives (.gz/.zip/.tar...) are read IN MEMORY here - no extracted copy is written
    back to the input folder, and nothing is moved/deleted by Dataflow.
    (The orchestrator moves the archive only after the file is committed.)
  * decoder runs with required_fields=None (ALL decoded fields). Your old pipeline passed
    INPUT_FIELDS=[recordType,varName,varValue,recordProperty], which blanks every CDR field.
  * decoder runs in strict mode: a bad record raises instead of silently truncating the file.
"""
import bz2
import gzip
import io
import logging
import lzma
import os
import tarfile
import zipfile
from datetime import datetime

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems

from beam_ber_asn_decoder import AsnDecodeDoFn, decode_bytes_to_dicts
from pgw_schema import DETAIL_SCHEMA
from prism_plugin_base import RejectRecord, SourcePlugin
from transformation_PGW import transform_record

log = logging.getLogger("prism.pgw")

# same file your old pipeline_pgw.py used
FORMAT_XML_PATH = "gs://vf-uk-raid-pre-prod-working/dataflow/formats/format_pgw.xml"

# True  = any undecodable record fails the WHOLE file (retried, then quarantined)
# False = old behaviour (stop at the bad record, keep what was decoded) - only use if your
#         real files legitimately end with filler/garbage bytes
STRICT_DECODE = True

_TYPES = dict(DETAIL_SCHEMA)


# ----------------------------------------------------------------- archive reading
def iter_inner_files(path: str):
    """Yield (inner_file_name, bytes) for every file in `path`.
    Naming matches your old ExtractCompressedFilesToPath:
      zip/tar members -> '<archive_name>__<member>'   single .gz/.bz2/.xz/plain -> name without ext
    """
    low, base = path.lower(), os.path.basename(path)

    def raw() -> bytes:
        with FileSystems.open(path, compression_type=CompressionTypes.UNCOMPRESSED) as f:
            return f.read()

    if low.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(raw())) as zf:
            for m in zf.infolist():
                if not m.is_dir():
                    yield f"{base}__{os.path.basename(m.filename)}", zf.read(m)
    elif low.endswith((".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz")):
        with tarfile.open(fileobj=io.BytesIO(raw()), mode="r:*") as tf:
            for m in tf:
                if m.isfile():
                    f = tf.extractfile(m)
                    if f is not None:
                        yield f"{base}__{os.path.basename(m.name)}", f.read()
    elif low.endswith(".gz"):
        yield base[:-3], gzip.decompress(raw())
    elif low.endswith(".bz2"):
        yield base[:-4], bz2.decompress(raw())
    elif low.endswith(".xz"):
        yield base[:-3], lzma.decompress(raw())
    else:
        yield base, raw()


# ----------------------------------------------------------------- typing
def _coerce(name: str, value, typ: str):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None
    if typ == "STRING":
        return str(value)
    if typ == "INT64":
        return int(value.strip()) if isinstance(value, str) else int(value)
    if typ == "FLOAT64":
        return float(value)
    if typ == "DATE":
        s = str(value).strip()
        datetime.strptime(s, "%Y-%m-%d")        # validate: bad date -> record is rejected, load never fails
        return s
    return value


class PgwPlugin(SourcePlugin):
    def setup(self):
        # decoder: loads format XML, builds BER decoder, sets the module-level schema ref
        self.asn = AsnDecodeDoFn(format_xml_path=FORMAT_XML_PATH, required_fields=None, ne_name="PGW")
        self.asn.setup()
        # transformation + lookups (loaded ONCE per worker, shared by all files of the job)
        self.tr = transform_record()
        self.tr.setup()

    def decode(self, path):
        for inner_name, data in iter_inner_files(path):
            log.info("decoding %s (%d bytes)", inner_name, len(data))
            yield from decode_bytes_to_dicts(
                self.asn.decoder, data, required_fields=None,
                add_filename=inner_name, strict=STRICT_DECODE)

    def transform(self, rec):
        rows = []
        for out in self.tr.process(rec):             # your transform yields several rows per CDR
            out = dict(out)                          # copy at yield time (it mutates one dict)
            try:
                rows.append({c: _coerce(c, out.get(c), t) for c, t in DETAIL_SCHEMA
                             if c not in ("file_id", "loaded_at")})
            except (ValueError, TypeError) as e:
                raise RejectRecord(f"bad value: {e}")
        return rows


Plugin = PgwPlugin        # loaded by dataflow_pipeline.py via importlib
