"""
Offline checks for "the sender overwrote the SAME path with a corrected file".   python tests/test_replaced_path.py
Old rows for that path that were never loaded must become SUPERSEDED (so they never retry and never get
their path quarantined), matched by FULL PATH, never touching rows with a live lease or other folders.
"""
import datetime as dt
import os
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
FID = lambda name, crc: ft.FileTracker.make_file_id("PGW", name, crc)    # noqa: E731
src = ft.SourceConfig("PGW", "in/", "done/", "dup/", "quar/", name_glob="pgw*")


class Blob:
    def __init__(self, name, crc, gen=1):
        self.name, self.crc32c, self.md5_hash, self.size, self.generation = name, crc, None, 10, gen
        self.updated = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self.bucket = R(name="b")


def discovery(blob, known_rows, stale_rows):
    t = ft.FileTracker.__new__(ft.FileTracker)
    t.reg, t.bucket_name, t.alert_fn = "reg", "b", lambda *a: None
    calls, stale_sql = [], []

    def _rows(sql, params=None):
        if "SELECT file_id, status" in sql:
            return known_rows
        if "SELECT file_id, object_name" in sql:                       # emulate the SQL filter on object_name
            stale_sql.append(sql)
            paths = params[1].a[2]
            return [r for r in stale_rows if r.object_name in paths]
        return []
    t._rows = _rows
    t._insert_new = lambda s, rows: calls.append(("insert", [r["base"] for r in rows]))
    t._run = lambda sql, params=None, **k: calls.append(("update", sql, params[1].a[2] if params else None))
    t._events_many = lambda rows: calls.append(("events", [r[0] for r in rows]))
    t._move_blob = lambda *a: None
    t._record_moves = lambda m, st=None: None
    stats = {"new": 0, "duplicate": 0, "conflict": 0, "moved_completed": 0, "seen": 0}
    t._discover_batch(src, [blob], stats, {})
    return t, calls, stats, stale_sql


def updates(calls):
    return [c for c in calls if c[0] == "update"]


# 1. corrected file overwrote the path; old FAILED row (old content) -> superseded, AFTER the new row is inserted
old = FID("pgw_x", "OLDCRC")
t, calls, stats, ssql = discovery(Blob("in/pgw_x", "NEWCRC"), [],
                                  [R(file_id=old, object_name="in/pgw_x")])
ups = updates(calls)
assert len(ups) == 1 and ups[0][2] == [old] and "status='SUPERSEDED'" in ups[0][1]
assert [c[0] for c in calls].index("insert") < [c[0] for c in calls].index("update")
assert stats["replaced"] == 1
print("1 overwritten path: old failed row superseded after the new row is inserted")

# 2. same content again (the FAILED row IS this file) -> untouched
same = FID("pgw_x", "SAMECRC")
_, calls, stats, _ = discovery(Blob("in/pgw_x", "SAMECRC"), [R(file_id=same, status="FAILED")],
                               [R(file_id=same, object_name="in/pgw_x")])
assert not updates(calls) and "replaced" not in stats
print("2 same content: nothing superseded")

# 3. same NAME but a different folder -> never touched (match is by full path)
_, calls, stats, _ = discovery(Blob("in/pgw_x", "NEWCRC"), [],
                               [R(file_id=old, object_name="in/other_folder/pgw_x")])
assert not updates(calls)
print("3 same name, other folder: untouched")

# 4. content goes A -> B -> A: B (DISCOVERED) is superseded, A (this content) stays eligible
a_id, b_id = FID("pgw_x", "A"), FID("pgw_x", "B")
_, calls, _, _ = discovery(Blob("in/pgw_x", "A"), [R(file_id=a_id, status="FAILED")],
                           [R(file_id=a_id, object_name="in/pgw_x"), R(file_id=b_id, object_name="in/pgw_x")])
assert updates(calls)[0][2] == [b_id]
print("4 A -> B -> A: only the obsolete row superseded")

# 5. a row with a LIVE lease is never selected or updated
_, calls, stats, ssql = discovery(Blob("in/pgw_x", "NEWCRC"), [], [])
assert "lease_expiry < CURRENT_TIMESTAMP()" in ssql[0]
up = [c for c in calls if c[0] == "update"]
t2, c2, _, _ = discovery(Blob("in/pgw_x", "NEWCRC"), [], [R(file_id=old, object_name="in/pgw_x")])
assert "lease_expiry < CURRENT_TIMESTAMP()" in updates(c2)[0][1]
print("5 live-lease rows excluded in both the select and the update")


# 6. quarantine guard: replaced object is NOT moved; real exhausted files still are
def quarantine(blobs_by_name):
    t = ft.FileTracker.__new__(ft.FileTracker)
    t.reg, t.bucket_name, t.alert_fn = "reg", "b", lambda *a: None
    rows = [R(file_id="f1", object_name="in/p1", base_name="p1", last_error="e", generation=5),
            R(file_id="f2", object_name="in/p2", base_name="p2", last_error="e", generation=5),
            R(file_id="f3", object_name="in/p3", base_name="p3", last_error="e", generation=5)]
    t._rows = lambda sql, params=None: rows
    t.gcs = R(bucket=lambda n: R(get_blob=lambda name: blobs_by_name.get(name)))
    log = {"updates": [], "moved": []}
    t._run = lambda sql, params=None, **k: log["updates"].append((sql.split("SET status='")[1].split("'")[0], next(x for x in params if x.a[0] == "ids").a[2]))
    t._events_many = lambda rows_: None
    t._move_blob = lambda name, *a: (log["moved"].append(name) or "x/" + name)
    t._record_moves = lambda m, st=None: None
    return t.quarantine_exhausted(src, 1), log


done, log = quarantine({"in/p1": R(generation=5),          # unchanged -> quarantine
                        "in/p2": R(generation=9),          # overwritten -> superseded, NOT moved
                        "in/p3": None})                    # object gone -> quarantine (nothing to move)
assert ("SUPERSEDED", ["f2"]) in log["updates"] and ("QUARANTINED", ["f1", "f3"]) in log["updates"]
assert "in/p2" not in log["moved"] and "in/p1" in log["moved"] and done == ["f1", "f3"]
print("6 quarantine: overwritten object not moved, row superseded")
print("ALL REPLACED-PATH CHECKS PASSED")
