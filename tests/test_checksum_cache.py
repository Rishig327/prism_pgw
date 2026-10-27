"""
Offline check of checksum reuse in discover() - no GCP.   python tests/test_checksum_cache.py
Verifies: files already in the registry (same name AND generation) do not trigger a Cloud Storage
fetch; new files, and re-dropped files with a different generation, do; a failed cache lookup
falls back to fetching everything; file_id is identical whether the checksum came from the cache
or from storage.
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
    for n in ("ScalarQueryParameter", "ArrayQueryParameter", "StructQueryParameter",
              "ConnectionProperty", "EncryptionConfiguration", "QueryJobConfig", "Client"):
        setattr(bq, n, P)
    st = types.ModuleType("google.cloud.storage"); st.Client = P
    g, gc = types.ModuleType("google"), types.ModuleType("google.cloud")
    gc.bigquery, gc.storage = bq, st; g.cloud = gc
    for name, m in (("google", g), ("google.cloud", gc), ("google.cloud.bigquery", bq),
                    ("google.cloud.storage", st)):
        sys.modules[name] = m


install_stubs()
import file_tracker as ft            # noqa: E402

R = types.SimpleNamespace
reloads = []


class Blob:
    def __init__(self, name, crc, gen):
        self.name, self._crc, self.crc32c, self.md5_hash = name, crc, None, None
        self.size, self.generation = 10, gen
        self.updated = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
        self.bucket = R(name="b")

    def reload(self):                          # what a real Cloud Storage fetch does
        reloads.append(self.name)
        self.crc32c = self._crc


def make(cache_rows, fail_cache=False):
    t = ft.FileTracker.__new__(ft.FileTracker)
    t.reg, t.bucket_name, t.alert_fn = "reg", "b", lambda *a: None
    blobs = [Blob("in/pgw_a", "CA", 1), Blob("in/pgw_b", "CB", 2),   # both registered earlier, same generation
             Blob("in/pgw_c", "CC", 3),                              # registered earlier, but overwritten: new generation
             Blob("in/pgw_d", "CD", 4)]                              # brand new
    t.gcs = R(list_blobs=lambda bucket, prefix=None: blobs)
    registered = []

    def _rows(sql, params=None):
        if "SELECT object_name, generation, crc32c" in sql:     # the checksum-cache query only
            if fail_cache:
                raise RuntimeError("bigquery down")
            return cache_rows
        return []
    t._rows = _rows
    t._insert_new = lambda s, rows: registered.extend((r["base"], r["crc"], r["file_id"]) for r in rows)
    t._move_blob = lambda *a: None
    t._record_moves = lambda m: None
    return t, registered


src = ft.SourceConfig("PGW", "in/", "done/", "dup/", "quar/", name_glob="pgw*")
cache = [R(object_name="in/pgw_a", generation=1, crc32c="CA"),
         R(object_name="in/pgw_b", generation=2, crc32c="CB"),
         R(object_name="in/pgw_c", generation=99, crc32c="OLD")]      # old generation -> must NOT be reused

# 1. normal: only c (changed) and d (new) are fetched
reloads.clear()
t, reg = make(cache)
t.discover(src)
print("fetched from storage:", sorted(reloads))
assert sorted(reloads) == ["in/pgw_c", "in/pgw_d"]
by = {b: (crc, fid) for b, crc, fid in reg}
assert by["pgw_a"][0] == "CA" and by["pgw_c"][0] == "CC", by       # c uses the FRESH checksum, not 'OLD'
assert by["pgw_a"][1] == ft.FileTracker.make_file_id("PGW", "pgw_a", "CA")

# 2. cache lookup fails -> everything is fetched, discovery still works
reloads.clear()
t, reg = make(cache, fail_cache=True)
t.discover(src)
print("cache down, fetched:", sorted(reloads))
assert sorted(reloads) == ["in/pgw_a", "in/pgw_b", "in/pgw_c", "in/pgw_d"] and len(reg) == 4

# 3. same file_id whether checksum was cached or fetched
reloads.clear(); t1, r1 = make(cache); t1.discover(src)
reloads.clear(); t2, r2 = make([]); t2.discover(src)
assert {x[2] for x in r1} == {x[2] for x in r2}
print("ALL CHECKSUM CACHE CHECKS PASSED")
