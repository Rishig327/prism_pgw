"""
Offline checks of commit_batch and the orchestrator flow (no GCP, no Beam).
    python tests/test_commit_flow.py
BigQuery/GCS are replaced by fakes, so this verifies LOGIC (statement order, rollback, which files
move/fail), not real BigQuery behaviour - still run a small real batch afterwards.
"""
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def install_stubs():
    class P:
        def __init__(self, *a, **k):
            self.a, self.k = a, k
    bq = types.ModuleType("google.cloud.bigquery")
    for n in ("ScalarQueryParameter", "ArrayQueryParameter", "StructQueryParameter",
              "ConnectionProperty", "EncryptionConfiguration"):
        setattr(bq, n, P)
    bq.QueryJobConfig = type("QueryJobConfig", (), {"__init__": lambda s, **k: None})
    bq.Client = P
    st = types.ModuleType("google.cloud.storage"); st.Client = P
    g, gc = types.ModuleType("google"), types.ModuleType("google.cloud")
    gc.bigquery, gc.storage = bq, st
    g.cloud = gc
    for name, m in (("google", g), ("google.cloud", gc), ("google.cloud.bigquery", bq),
                    ("google.cloud.storage", st)):
        sys.modules[name] = m
    ga = types.ModuleType("googleapiclient"); gd = types.ModuleType("googleapiclient.discovery")
    gd.build = lambda *a, **k: None
    sys.modules["googleapiclient"], sys.modules["googleapiclient.discovery"] = ga, gd


install_stubs()
import file_tracker as ft            # noqa: E402

R = types.SimpleNamespace


class Job:
    def __init__(self, n=None, sid=None):
        self.num_dml_affected_rows, self.session_info = n, R(session_id=sid)


def make_tracker(insert_rows=None, registry_updates=None, owned=None):
    t = ft.FileTracker.__new__(ft.FileTracker)
    t.reg, t.logt = "`reg`", "`log`"
    stmts = []
    t.stmts = stmts

    def _run(sql, params=None, session_id=None, create_session=False):
        s = " ".join(sql.split())
        stmts.append((s[:50], "SESSION" if session_id else ("NEW" if create_session else "-")))
        if s.startswith("BEGIN"):
            return Job(sid="sess-1")
        if s.startswith("INSERT INTO `p.d.detail`"):
            return Job(insert_rows)
        if s.startswith("UPDATE `reg` T SET status='COMPLETED'"):
            return Job(registry_updates)
        return Job(0)
    t._run = _run

    def _rows(sql, params=None):
        if "claimed_by=@run AND status='IN_PROGRESS'" in sql and sql.startswith("SELECT file_id"):
            return [R(file_id=f) for f in owned]
        return []
    t._rows = _rows
    return t


def items(n, loaded=10):
    return [ft.CommitItem(R(file_id=f"F{i}", object_name=f"in/f{i}", base_name=f"f{i}", source_type="PGW"), 1, loaded, loaded, 0)
            for i in range(n)]


tgt = ft.TargetConfig("p.d.detail", "p.d.stg")
order = lambda t: [s[0].split()[0] + ("*" if s[1] == "SESSION" else "") for s in t.stmts]

# 1. happy path: 3 files, one transaction
t = make_tracker(insert_rows=30, registry_updates=3, owned=["F0", "F1", "F2"])
done = t.commit_batch(tgt, "run1", items(3))
print("happy path statements:", order(t))
assert done == ["F0", "F1", "F2"]
seq = [s[0] for s in t.stmts]
b, c = seq.index("BEGIN TRANSACTION;"), seq.index("COMMIT TRANSACTION;")
assert [s[0].split()[0] for s in t.stmts[b:c + 1]] == ["BEGIN", "DELETE", "INSERT", "COMMIT"]
assert not any("reg" in s[0] and s[1] == "SESSION" for s in t.stmts), "registry must be OUTSIDE the transaction"
assert any(s[0].startswith("UPDATE `reg` T SET status='COMPLETED'") for s in t.stmts[c:])

# 2. insert count mismatch -> rollback, nothing marked COMPLETED
t = make_tracker(insert_rows=29, registry_updates=3, owned=["F0", "F1", "F2"])
try:
    t.commit_batch(tgt, "run1", items(3)); raise SystemExit("should have failed")
except RuntimeError as e:
    print("mismatch ->", e)
assert t.stmts[-1][0].startswith("ROLLBACK")
assert not any("COMPLETED" in s[0] for s in t.stmts)

# 3. one file lost its lease -> committed without it
t = make_tracker(insert_rows=20, registry_updates=2, owned=["F0", "F2"])
done = t.commit_batch(tgt, "run1", items(3))
print("lease lost for F1 ->", done)
assert done == ["F0", "F2"]

# 4. nobody owns anything -> no transaction at all
t = make_tracker(owned=[])
assert t.commit_batch(tgt, "run1", items(2)) == [] and not any(s[0].startswith("BEGIN") for s in t.stmts)

# 5. aggregate guard
try:
    t.commit_batch(ft.TargetConfig("a", "b", agg_table="x"), "r", items(1)); raise SystemExit("no guard")
except NotImplementedError:
    pass
print("tracker checks passed")

# ------------------------------------------------------------ orchestrator flow with a fake tracker
import config                         # noqa: E402
import run_prism_batch as rpb         # noqa: E402


class FakeTracker:
    def __init__(self, **k):
        self.k = k; self.log = []; FakeTracker.last = self
    def discover(self, src): self.log.append("discover")
    def quarantine_exhausted(self, src, m): self.log.append("quarantine")
    def get_eligible(self, st, m, limit=0):
        return [R(file_id=f"F{i}", object_name=f"in/f{i}", base_name=f"f{i}") for i in range(4)]
    def claim_batch(self, recs, run, m, lease): return {r.file_id: 1 for r in recs}
    def heartbeat_batch(self, run, lease, source_type): raise RuntimeError("DML conflict")   # must not kill the run
    def collect_counts(self, t, claimed):
        return {"F0": (10, 10, 0), "F1": (100, 99, 1), "F2": (10, 5, 0), "F3": (0, 0, 0)}
    def file_errors(self, claimed): return {}
    def mark_failed_batch(self, t, failures, run): self.log.append(("failed", sorted(f[0].file_id for f in failures)))
    def commit_batch(self, t, run, items):
        self.log.append(("commit", [i.rec.file_id for i in items])); return [i.rec.file_id for i in items]
    def move_files(self, recs, prefix):
        self.log.append(("move", [r.file_id for r in recs])); return len(recs)


rpb.FileTracker = FakeTracker
rpb.write_manifest = lambda *a: "gs://b/m.jsonl"
rpb.run_pipeline_from_vm = lambda *a: True
rpb._resolve = lambda st: R(globals={"MAX_ATTEMPTS": 3, "BATCH_FILES": 200, "LEASE_MIN": 180, "MAX_JOB_WAIT_MIN": 240},
                            settings=config.SOURCES["PGW"], version="v", enabled=True)
rpb.run("PGW", "vm")                                   # VM test mode = the original code path
log = FakeTracker.last.log
print("orchestrator:", log)
assert ("failed", ["F2", "F3"]) in log          # F2 count mismatch, F3 empty
assert ("commit", ["F0", "F1"]) in log          # F1: 1% rejects is within the 5% limit
assert ("move", ["F0", "F1"]) in log            # only committed files move
assert log.index(("commit", ["F0", "F1"])) < log.index(("move", ["F0", "F1"]))
rpb.safe_heartbeat(FakeTracker(), "r", "PGW")          # a failing heartbeat must not raise
print("orchestrator checks passed")
