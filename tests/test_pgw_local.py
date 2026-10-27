"""
Local check of the PGW plugin on a real file - no GCP needed (lookups use LOCAL_MODE samples).

    python tests/test_pgw_local.py /path/to/pgwld5_..._file9492649.u.gz [max_cdrs]

Mirrors what ProcessFileDoFn does and prints the numbers the orchestrator will reconcile.
"""
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pgw_plugin                                   # noqa: E402
from LookupsManager import LookupManager            # noqa: E402
from pgw_schema import DETAIL_SCHEMA                # noqa: E402
from prism_plugin_base import RejectRecord          # noqa: E402

# No BigQuery needed: fake the lookups. DIM lookups (lk_VFUK_C_UC_*) return VALUE=7 so the
# INT64 columns are exercised; reference lookups fall back to the default your code passes.
LookupManager.load_selected = classmethod(lambda cls, names: None)
LookupManager.get_lookup = classmethod(
    lambda cls, name, *keys, default=None:
    {"VALUE": "7"} if name.startswith("lk_VFUK_C_UC_") else (default if default is not None else {}))
pgw_plugin.FORMAT_XML_PATH = os.path.join(os.path.dirname(__file__), "..", "format_pgw.xml")

path = sys.argv[1]
max_cdrs = int(sys.argv[2]) if len(sys.argv) > 2 else None

plugin = pgw_plugin.PgwPlugin()
plugin.setup()

t0, cdrs, rows_out, rejects = time.time(), 0, 0, Counter()
sample = None
try:
    for cdrs, rec in enumerate(plugin.decode(path), start=1):
        try:
            out = plugin.transform(rec)
        except RejectRecord as e:
            rejects[str(e)[:60]] += 1
            continue
        except Exception as e:
            rejects[f"ERR {type(e).__name__}: {str(e)[:50]}"] += 1
            continue
        rows_out += len(out)
        if out and sample is None:
            sample = out[0]
        if max_cdrs and cdrs >= max_cdrs:
            break
    file_error = None
except Exception as e:                              # strict decode: whole file would be FAILED
    file_error = f"{type(e).__name__}: {e}"

cols = [c for c, _ in DETAIL_SCHEMA if c not in ("file_id", "loaded_at")]
print(f"CDRs decoded        : {cdrs}")
print(f"detail rows (staged): {rows_out}")
print(f"rejects             : {sum(rejects.values())} {dict(rejects.most_common(3))}")
print(f"records_read (rows+rejects): {rows_out + sum(rejects.values())}")
print(f"file-level error    : {file_error}")
print(f"elapsed             : {time.time() - t0:.1f}s")
if sample:
    print(f"columns match schema: {sorted(sample) == sorted(cols)}")
    print({k: sample[k] for k in ("STARTTIME", "EVT_DATE", "SERVED_MSISDN", "RATING_GROUP",
                                  "MES_DATA_VOLUME", "FILENAME", "NE_ID", "MES_SEQ_NUM",
                                  "DIM_EVENT_TYPE_ID", "DIM_ENTITY")})
