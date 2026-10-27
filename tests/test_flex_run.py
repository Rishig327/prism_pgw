"""
Offline checks of flex mode (Cloud Run): run tracking, stand-down guard, SIGTERM handling and recovery of
orphaned runs.   python tests/test_flex_run.py
Everything BigQuery / Dataflow / Cloud Storage is replaced by fakes, so this verifies the DECISIONS the
orchestrator takes (which statuses, which files fail, whether a second Dataflow job is launched).
"""
import dataclasses
import os
import signal
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def install_stubs():
    class P:
        def __init__(self, *a, **k): self.a, self.k = a, k
    bq = types.ModuleType("google.cloud.bigquery")
    for n in ("ScalarQueryParameter", "ArrayQueryParameter", "StructQueryParameter", "ConnectionProperty",
              "EncryptionConfiguration", "QueryJobConfig", "Client"):
        setattr(bq, n, P)
    st = types.ModuleType("google.cloud.storage"); st.Client = P
    g, gc = types.ModuleType("google"), types.ModuleType("google.cloud")
    gc.bigquery, gc.storage = bq, st; g.cloud = gc
    for name, m in (("google", g), ("google.cloud", gc), ("google.cloud.bigquery", bq), ("google.cloud.storage", st)):
        sys.modules[name] = m
    ga = types.ModuleType("googleapiclient"); gd = types.ModuleType("googleapiclient.discovery")
    gd.build = lambda *a, **k: None
    sys.modules["googleapiclient"], sys.modules["googleapiclient.discovery"] = ga, gd


install_stubs()
import config                          # noqa: E402
import run_prism_batch as rpb          # noqa: E402

R = types.SimpleNamespace
BASE = config.SOURCES["PGW"]


def rec(i, run="OLD"):
    return R(file_id=f"F{i}", object_name=f"in/f{i}", base_name=f"f{i}", source_type="PGW")


class FakeTracker:
    """Records every call; behaviour is set per scenario through class attributes."""
    live_before, orphans, orphan_claim_ok = [], [], True
    eligible, orphan_files, die_in_discover = [], [], False

    def __init__(self, **k):
        self.calls, self.runs = [], {}
        FakeTracker.last = self

    # ---- run tracking
    def run_register(self, run_id, st, status="STARTING", config_version=None):
        self.runs[run_id] = [status]; self.calls.append(("register", run_id, config_version))
    def run_update(self, run_id, st, status=None, job_id=None, manifest_uri=None, note=None, adopted_by=None, finished=False):
        if status:
            self.runs.setdefault(run_id, []).append(status)
        self.calls.append(("update", run_id, status, job_id, note))
    def run_heartbeat(self, run_id, st): self.calls.append(("run_heartbeat", run_id))
    def live_runs_before(self, st, run_id, m): return list(FakeTracker.live_before)
    def find_orphan_runs(self, st, run_id, m): return list(FakeTracker.orphans)
    def claim_orphan(self, oid, st, exp, me):
        self.calls.append(("claim_orphan", oid, exp)); return FakeTracker.orphan_claim_ok
    def claimed_by_run(self, st, run_id):
        recs = [rec(i) for i in FakeTracker.orphan_files]
        return recs, {r.file_id: 1 for r in recs}
    def release_claims(self, st, run_id): self.calls.append(("release_claims", run_id)); return 2
    # ---- files
    def discover(self, src):
        if FakeTracker.die_in_discover: raise rpb.Shutdown()
        self.calls.append(("discover",))
    def quarantine_exhausted(self, src, m): return []
    def get_eligible(self, st, m, limit=0): return [rec(i) for i in FakeTracker.eligible]
    def claim_batch(self, recs, run, m, lease): return {r.file_id: 1 for r in recs}
    def heartbeat_batch(self, run, lease, st): return 1
    def collect_counts(self, t, claimed): return {f: (10, 10, 0) for f in claimed}
    def file_errors(self, claimed): return {}
    def mark_failed_batch(self, t, failures, run): self.calls.append(("failed", run, sorted(f[0].file_id for f in failures)))
    def commit_batch(self, t, run, items):
        self.calls.append(("commit", run, [i.rec.file_id for i in items])); return [i.rec.file_id for i in items]
    def move_files(self, recs, prefix): self.calls.append(("move", [r.file_id for r in recs])); return len(recs)

    def has(self, kind): return [c for c in self.calls if c[0] == kind]


GLOBALS = {"MAX_ATTEMPTS": 3, "BATCH_FILES": 200, "LEASE_MIN": 180, "MAX_JOB_WAIT_MIN": 240}
launched, waits = [], []
state = {"job_state": "JOB_STATE_DONE", "wait_ok": True, "by_name": None, "wait_raises": False}


def reset(**cfg_over):
    FakeTracker.live_before, FakeTracker.orphans, FakeTracker.orphan_claim_ok = [], [], True
    FakeTracker.eligible, FakeTracker.orphan_files, FakeTracker.die_in_discover = [], [], False
    launched.clear(); waits.clear()
    state.update(job_state="JOB_STATE_DONE", wait_ok=True, by_name=None, wait_raises=False)
    s = dataclasses.replace(BASE, **cfg_over)
    rpb._resolve = lambda st, s=s: R(globals=dict(GLOBALS), settings=s, version="global@1|PGW@1", enabled=True)


def fake_wait(tr, job_id, run_id, st, poll_s=60, track_run=False, also_beat=None):
    waits.append((job_id, run_id, track_run, also_beat))
    if state["wait_raises"]: raise rpb.Shutdown()
    return state["wait_ok"]


orig_signal = signal.signal
signal.signal = lambda *a, **k: None
rpb.FileTracker = FakeTracker
rpb.write_manifest = lambda *a: "gs://b/m.jsonl"
rpb.launch_dataflow = lambda cfg, uri, run_id: (launched.append(run_id) or "JOB-NEW")
rpb.wait_for_job = fake_wait
rpb.get_job_state = lambda job_id: state["job_state"]
rpb.find_job_by_name = lambda name: state["by_name"]
orphan = lambda status="LAUNCHED", job="J1": R(run_id="OLD", status=status, job_id=job, manifest_uri="gs://b/old.jsonl")   # noqa: E731

try:
    # ---- A. normal run: statuses and a single launch
    reset(); FakeTracker.eligible = [1, 2]
    rpb.run("PGW"); t = FakeTracker.last
    mine = next(v for k, v in t.runs.items() if k.startswith("PGW-"))
    assert mine == ["STARTING", "CLAIMED", "LAUNCHED", "FINALISING", "DONE"], mine
    assert len(launched) == 1 and t.has("commit")[0][2] == ["F0"][:0] + ["F1", "F2"] and t.has("move")
    assert waits[0][2] is True                                   # run row heartbeat while waiting
    print("A normal run: STARTING > CLAIMED > LAUNCHED > FINALISING > DONE, one Dataflow job")

    # ---- B. guard: an earlier live run -> stand down, nothing touched
    reset(); FakeTracker.eligible = [1]; FakeTracker.live_before = ["PGW-EARLIER"]
    rpb.run("PGW"); t = FakeTracker.last
    assert next(v for v in t.runs.values())[-1] == "ABANDONED" and not t.has("discover") and not launched
    reset(skip_if_running=False); FakeTracker.eligible = [1]; FakeTracker.live_before = ["PGW-EARLIER"]
    rpb.run("PGW")
    assert len(launched) == 1                                     # switched off in config -> runs anyway
    print("B skip_if_running: stands down when an earlier run is alive; can be switched off")

    # ---- C. orphan whose job already finished: finished by the next run, NO second Dataflow job
    reset(); FakeTracker.orphans = [orphan()]; FakeTracker.orphan_files = [1, 2]
    rpb.run("PGW"); t = FakeTracker.last
    assert t.has("commit")[0][1] == "OLD" and t.has("commit")[0][2] == ["F1", "F2"] and not launched
    assert t.runs["OLD"][-1] == "DONE" and not waits
    print("C orphan, job DONE: files committed under the old run id, no new job launched")

    # ---- D. orphan whose job is still running: wait for it, keep both runs alive
    reset(); FakeTracker.orphans = [orphan()]; FakeTracker.orphan_files = [1]; state["job_state"] = "JOB_STATE_RUNNING"
    rpb.run("PGW"); t = FakeTracker.last
    assert waits and waits[0][0] == "J1" and waits[0][1] == "OLD" and waits[0][3].startswith("PGW-")
    assert t.has("commit") and not launched
    print("D orphan, job RUNNING: adopted and waited for, then committed")

    # ---- E. orphan whose job failed: its files go back to FAILED, nothing committed
    reset(); FakeTracker.orphans = [orphan()]; FakeTracker.orphan_files = [1, 2]; state["job_state"] = "JOB_STATE_FAILED"
    rpb.run("PGW"); t = FakeTracker.last
    assert t.has("failed")[0][1:] == ("OLD", ["F1", "F2"]) and not t.has("commit") and t.runs["OLD"][-1] == "FAILED"
    print("E orphan, job FAILED: files marked failed for retry, nothing committed")

    # ---- F/G. orphan that never saved a job id
    reset(); FakeTracker.orphans = [orphan("CLAIMED", None)]; FakeTracker.orphan_files = [1]
    rpb.run("PGW"); t = FakeTracker.last
    assert t.has("release_claims") and t.runs["OLD"][-1] == "ABANDONED" and not t.has("commit")
    reset(); FakeTracker.orphans = [orphan("CLAIMED", None)]; FakeTracker.orphan_files = [1]; state["by_name"] = "J-FOUND"
    rpb.run("PGW"); t = FakeTracker.last
    assert not t.has("release_claims") and t.has("commit") and not launched
    print("F/G orphan without job id: job found by name -> adopted; none -> files released")

    # ---- H. someone else adopted it first
    reset(); FakeTracker.orphans = [orphan()]; FakeTracker.orphan_files = [1]; FakeTracker.orphan_claim_ok = False
    rpb.run("PGW"); t = FakeTracker.last
    assert not t.has("commit") and not t.has("failed")
    print("H orphan already adopted elsewhere: skipped")

    # ---- I. SIGTERM while waiting for Dataflow: run ORPHANED, files NOT failed, job NOT cancelled
    reset(); FakeTracker.eligible = [1, 2]; state["wait_raises"] = True
    rpb.run("PGW"); t = FakeTracker.last                          # must return normally (exit 0)
    mine = next(v for k, v in t.runs.items() if k.startswith("PGW-"))
    assert mine[-1] == "ORPHANED" and not t.has("failed") and not t.has("commit")
    print("I SIGTERM during the job: run ORPHANED, files not failed, job left running")

    # ---- J. SIGTERM before anything was claimed
    reset(); FakeTracker.die_in_discover = True
    rpb.run("PGW"); t = FakeTracker.last
    assert next(v for v in t.runs.values())[-1] == "ABANDONED" and not t.has("failed")
    print("J SIGTERM before claiming: run ABANDONED")

    # ---- K. Dataflow job failed in a normal run
    reset(); FakeTracker.eligible = [1]; state["wait_ok"] = False
    try:
        rpb.run("PGW"); raise SystemExit("job failure must raise")
    except RuntimeError:
        t = FakeTracker.last
        assert t.has("failed") and next(v for k, v in t.runs.items() if k.startswith("PGW-"))[-1] == "FAILED"
    print("K Dataflow failure: files failed, run FAILED, error raised")

    # ---- M. the configuration version is recorded with the run; a disabled source does nothing
    reset(); FakeTracker.eligible = [1]
    rpb.run("PGW"); t = FakeTracker.last
    assert t.has("register")[0][2] == "global@1|PGW@1"
    rpb._resolve = lambda st: R(globals=dict(GLOBALS), settings=BASE, version="v", enabled=False)
    FakeTracker.last = None; launched.clear()
    rpb.run("PGW")
    assert FakeTracker.last is None and not launched
    print("M config version stored on the run; a disabled source starts nothing")

    # ---- L. the shutdown signal cannot be swallowed by 'except Exception'
    assert not issubclass(rpb.Shutdown, Exception)
    try:
        rpb._raise_shutdown(15, None); raise SystemExit("handler must raise")
    except rpb.Shutdown:
        pass
    print("L Shutdown is a BaseException (not caught by except Exception)")
finally:
    signal.signal = orig_signal
print("ALL FLEX RUN CHECKS PASSED")
