"""
Offline checks: every registry statement is source-filtered, and the discovery lookups are limited to the
per-source window (SourceConfig.lookup_window_days).     python tests/test_lookup_window.py
"""
import datetime as dt
import logging
import os
import re
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


install_stubs()
import file_tracker as ft            # noqa: E402

R = types.SimpleNamespace
UTC = dt.timezone.utc
SRC_FILTER = re.compile(r"source_type\s*(=|IN)")


def param(params, name):
    return next(p for p in params if p.a[0] == name).a[-1]


class Blob:
    def __init__(self, name, crc, gen=1, updated=None):
        self.name, self.crc32c, self.md5_hash, self.size, self.generation = name, crc, None, 10, gen
        self.updated = updated or (dt.datetime.now(UTC) - dt.timedelta(hours=1))   # old enough to pass min_age_seconds
        self.bucket = R(name="b")

    def reload(self): pass


class Job:
    def __init__(self, n=None, sid=None):
        self.num_dml_affected_rows, self.session_info = n, R(session_id=sid)


def tracker(rows_fn=None, expected_insert=None):
    t = ft.FileTracker.__new__(ft.FileTracker)
    t.reg, t.runs, t.bucket_name, t.alert_fn = "`reg`", "`runs`", "b", lambda *a: None
    t.seen = []                                    # (kind, sql, params) of every statement touching the registry

    def _rows(sql, params=None):
        if "`reg`" in sql or "`runs`" in sql:
            t.seen.append(("SELECT", sql, params or []))
        return rows_fn(sql, params) if rows_fn else []

    def _run(sql, params=None, session_id=None, create_session=False):
        if "`reg`" in sql or "`runs`" in sql:
            t.seen.append((sql.split()[0], sql, params or []))
        if sql.startswith("BEGIN"):
            return Job(sid="s1")
        if sql.startswith("INSERT INTO `p.d.detail`"):
            return Job(expected_insert)
        return Job(1)
    t._rows, t._run = _rows, _run
    t._events_many = lambda rows: None
    t._move_blob = lambda *a: "x/y"
    return t


def assert_all_source_filtered(t, label):
    assert t.seen, f"{label}: no registry statements captured"
    for kind, sql, _ in t.seen:
        if kind == "INSERT":                       # an insert has no WHERE: it must write the source_type column
            assert "source_type" in sql, f"{label}: INSERT without source_type column:\n{sql[:200]}"
            continue
        assert SRC_FILTER.search(sql), f"{label}: registry {kind} without source filter:\n{sql[:200]}"
    print(f"{label}: {len(t.seen)} registry statement(s), all source-filtered")


# ---------------------------------------------------------------- 1. discovery lookups: source + window
def run_discovery(window_days, blob_updated=None, stale_rows=()):
    src = ft.SourceConfig("PGW", "in/", "done/", "dup/", "quar/", name_glob="pgw*", lookup_window_days=window_days)
    blobs = [Blob("in/pgw_a", "CA", updated=blob_updated)]
    t = tracker(lambda sql, params: list(stale_rows) if "SELECT file_id, object_name" in sql else [])
    t.gcs = R(list_blobs=lambda bucket, prefix=None: blobs)
    t._insert_new = lambda s, rows: None
    t._record_moves = lambda m, st=None: None
    t.discover(src)
    return t


before = dt.datetime.now(UTC)
t = run_discovery(30)
selects = [s for s in t.seen if s[0] == "SELECT"]
kinds = {"cache": "SELECT object_name, generation, crc32c", "known": "SELECT file_id, status",
         "content": "crc32c IN UNNEST", "same_name": "base_name IN UNNEST", "path": "SELECT file_id, object_name"}
for name, frag in kinds.items():
    hit = [s for s in selects if frag in s[1]]
    assert hit, f"lookup '{name}' did not run"
    sql, params = hit[0][1], hit[0][2]
    assert "source_type=@s" in sql and "discovered_at >= @win" in sql, f"{name}: missing source/window filter"
    win = param(params, "win")
    assert abs((before - dt.timedelta(days=30) - win).total_seconds()) < 5
print("1 lookups: cache, known, content, same-name and path all carry source + 30-day window")

t7 = run_discovery(7)
win7 = param([s for s in t7.seen if "SELECT file_id, status" in s[1]][0][2], "win")
assert abs((before - dt.timedelta(days=7) - win7).total_seconds()) < 5
print("2 window is per source and configurable (7 days -> 7-day cut-off)")

# ---------------------------------------------------------------- 2. registration MERGE
t = tracker()
rows = [{"blob": Blob("in/pgw_a", "CA"), "base": "pgw_a", "size": 10, "crc": "CA", "file_id": "f1",
         "status": "DISCOVERED", "err": None, "supersedes": None}]
t._insert_new(ft.SourceConfig("PGW", "in/", "d/", "u/", "q/", lookup_window_days=30), rows)
merge = t.seen[0][1]
assert "T.source_type = @src" in merge and "T.discovered_at >= @win" in merge
print("3 registration MERGE is limited to this source and the window")

# ---------------------------------------------------------------- 3. stale-input warning
class Capture(logging.Handler):
    def __init__(self): super().__init__(); self.msgs = []
    def emit(self, r): self.msgs.append(r.getMessage())


cap = Capture(); logging.getLogger("prism.file_tracker").addHandler(cap)
run_discovery(30, blob_updated=dt.datetime.now(UTC) - dt.timedelta(days=45))
assert any("older than the 30-day lookup window" in m for m in cap.msgs)
cap.msgs.clear()
run_discovery(30, blob_updated=dt.datetime.now(UTC) - dt.timedelta(days=2))
assert not any("lookup window" in m for m in cap.msgs)
print("4 a file older than the window logs a warning; a recent one does not")

# ---------------------------------------------------------------- 4. every other registry statement is source-filtered
recs = [R(file_id="F1", object_name="in/f1", base_name="f1", source_type="PGW"),
        R(file_id="F2", object_name="in/f2", base_name="f2", source_type="PGW")]

t = tracker(lambda sql, p: [R(file_id="F1", attempt_count=1)]); t.claim_batch(recs, "run1", 3, 60)
assert_all_source_filtered(t, "claim_batch")

t = tracker(); t.heartbeat_batch("run1", 60, "PGW")
assert_all_source_filtered(t, "heartbeat_batch")

tgt = ft.TargetConfig("p.d.detail", "p.d.stg")
t = tracker(lambda sql, p: [R(file_id="F1"), R(file_id="F2")] if sql.startswith("SELECT file_id FROM") else [],
            expected_insert=20)
t.cleanup_staging_many = lambda *a: None
items = [ft.CommitItem(r, 1, 10, 10, 0) for r in recs]
assert t.commit_batch(tgt, "run1", items) == ["F1", "F2"]
assert_all_source_filtered(t, "commit_batch")

t = tracker(); t.cleanup_staging_many = lambda *a: None
t.mark_failed_batch(tgt, [(recs[0], 1, "boom")], "run1")
assert_all_source_filtered(t, "mark_failed_batch")

t = tracker(); t._record_moves([("F1", "x/y")], "PGW")
assert_all_source_filtered(t, "_record_moves")

q_rows = [R(file_id="F1", object_name="in/f1", base_name="f1", last_error="e", generation=5),
          R(file_id="F2", object_name="in/f2", base_name="f2", last_error="e", generation=5)]
t = tracker(lambda sql, p: q_rows)
t.gcs = R(bucket=lambda n: R(get_blob=lambda name: R(generation=9) if name == "in/f2" else R(generation=5)))
t._record_moves = lambda m, st=None: None
t.quarantine_exhausted(ft.SourceConfig("PGW", "in/", "d/", "u/", "q/"), 1)
assert_all_source_filtered(t, "quarantine_exhausted")

t = tracker(lambda sql, p: [R(file_id="OLD", object_name="in/pgw_a")] if "SELECT file_id, object_name" in sql else [])
t._supersede_replaced(ft.SourceConfig("PGW", "in/", "d/", "u/", "q/"), [{"blob": R(name="in/pgw_a"), "file_id": "NEW"}], {})
assert_all_source_filtered(t, "_supersede_replaced")

# run tracking (flex mode): every statement on the runs table / the files it claims is source-filtered too
t = tracker(lambda sql, p: [R(started_at=dt.datetime.now(UTC), run_id="R1")])
t.run_register("R1", "PGW"); t.run_update("R1", "PGW", status="LAUNCHED", job_id="J")
t.live_runs_before("PGW", "R1", 15); t.find_orphan_runs("PGW", "R1", 15); t.claim_orphan("O1", "PGW", "LAUNCHED", "R1")
assert_all_source_filtered(t, "run tracking")
t = tracker(lambda sql, p: []); t.claimed_by_run("PGW", "R1"); t.release_claims("PGW", "R1")
assert_all_source_filtered(t, "claimed_by_run / release_claims")

# no unfiltered single-file helpers are left behind
assert not hasattr(ft.FileTracker, "claim") and not hasattr(ft.FileTracker, "heartbeat")
print("ALL LOOKUP WINDOW / SOURCE FILTER CHECKS PASSED")
