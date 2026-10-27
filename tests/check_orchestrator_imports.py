"""
Guards against "No module named X" in the Cloud Run image.   python tests/check_orchestrator_imports.py
Every local module reachable from run_prism_batch.py, recon_runner.py and config_admin.py must be COPYed by Dockerfile.orchestrator,
and none may be Dataflow-worker code (those need Apache Beam, which this image does not install).
"""
import ast
import os
import re
import sys

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
local = {f[:-3] for f in os.listdir(HERE) if f.endswith(".py")}
WORKER_ONLY = {"dataflow_pipeline", "prism_plugin_base", "pgw_schema", "beam_ber_asn_decoder", "decode_functions",
               "LookupsManager", "Lookups_config"} | {m for m in local if m.endswith("_plugin") or m.startswith("transformation_")}


def imports(mod):
    names = set()
    for n in ast.walk(ast.parse(open(os.path.join(HERE, mod + ".py"), encoding="utf-8").read())):
        if isinstance(n, ast.Import):
            names |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
            names.add(n.module.split(".")[0])
    return names & local


need, todo = set(), ["run_prism_batch", "recon_runner", "config_admin"]
while todo:
    m = todo.pop()
    if m not in need:
        need.add(m); todo += list(imports(m))
docker = {m[:-3] for m in re.findall(r"([A-Za-z_0-9]+\.py)", open(os.path.join(HERE, "Dockerfile.orchestrator")).read())}
problems = [f"{m}: not copied by Dockerfile.orchestrator" for m in sorted(need - docker)]
problems += [f"{m}: worker-only module reachable from the orchestrator" for m in sorted(need & WORKER_ONLY)]
print("orchestrator modules:", ", ".join(sorted(need)))
if problems:
    print("PROBLEMS:"); [print("  -", p) for p in problems]; sys.exit(1)
print("OK: Dockerfile.orchestrator contains every module the orchestrator and recon need")
