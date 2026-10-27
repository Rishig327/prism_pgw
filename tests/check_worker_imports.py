"""
Guards against "No module named X" on Dataflow workers. Run before every deploy:
    python tests/check_worker_imports.py

Every local module the workers can import (reachable from dataflow_pipeline.py, plus every
*_plugin.py because plugins are loaded dynamically) must be:
  * listed in setup.py py_modules   (--setup_file launch)
  * listed in the Dockerfile COPY   (Flex Template image)
and must NOT depend on orchestrator-only code (config, file_tracker, run_prism_batch).
"""
import ast
import os
import re
import sys

HERE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
ORCHESTRATOR_ONLY = {"config", "file_tracker", "run_prism_batch"}
local = {f[:-3] for f in os.listdir(HERE) if f.endswith(".py")}


def imports(mod):
    tree = ast.parse(open(os.path.join(HERE, mod + ".py"), encoding="utf-8").read())
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            names |= {a.name.split(".")[0] for a in n.names}
        elif isinstance(n, ast.ImportFrom) and n.module and n.level == 0:
            names.add(n.module.split(".")[0])
    return names & local


roots = ["dataflow_pipeline"] + sorted(m for m in local if m.endswith("_plugin"))
need, todo = set(), list(roots)
while todo:
    m = todo.pop()
    if m not in need:
        need.add(m)
        todo += list(imports(m))

setup_src = open(os.path.join(HERE, "setup.py")).read()
listed = set(re.findall(r'"([A-Za-z_0-9]+)"', setup_src.split("py_modules")[1].split("]")[0]))
docker = {m[:-3] for m in re.findall(r"([A-Za-z_0-9]+\.py)", open(os.path.join(HERE, "Dockerfile")).read())}

problems = []
for m in sorted(need):
    if m not in listed:
        problems.append(f"{m}: missing from setup.py py_modules")
    if m not in docker:
        problems.append(f"{m}: missing from Dockerfile COPY")
    bad = imports(m) & ORCHESTRATOR_ONLY
    if bad:
        problems.append(f"{m}: imports orchestrator-only module(s) {sorted(bad)}")
for m in sorted(listed - local):
    problems.append(f"setup.py lists {m} but {m}.py does not exist")

print("worker modules:", ", ".join(sorted(need)))
if problems:
    print("\nPROBLEMS:"); [print("  -", p) for p in problems]; sys.exit(1)
print("OK: every worker-side module is in setup.py and the Dockerfile; no orchestrator imports")
