"""Offline checks of the configuration tables (an in-memory fake stands in for BigQuery).   python tests/test_config_store.py"""
import dataclasses
import datetime as dt
import json
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
    ga = types.ModuleType("googleapiclient"); gd = types.ModuleType("googleapiclient.discovery")
    gd.build = lambda *a, **k: None
    sys.modules["googleapiclient"], sys.modules["googleapiclient.discovery"] = ga, gd



install_stubs()
import config                          # noqa: E402
import config_store as cs             # noqa: E402
import recon_logic as rl              # noqa: E402

BASE = config.SOURCES["PGW"]
DEFAULTS = {"MAX_ATTEMPTS": 3, "BATCH_FILES": 200, "LEASE_MIN": 180, "MAX_JOB_WAIT_MIN": 240}


class Row(dict):
    def __getattr__(self, k): return self[k]


class FakeBq:
    """Understands just the statements config_store issues; keeps versions in memory."""
    def __init__(self):
        self.tables = {cs.T_GLOBAL: [], cs.T_SOURCE: [], cs.T_RECON: []}
        self.clock = dt.datetime(2026, 10, 10, 8, 0, tzinfo=dt.timezone.utc)
        self.sql = []

    def _table(self, sql):
        return next(n for n in self.tables if f".{n}`" in sql)

    def _latest(self, rows, key=None):
        best = {}
        for r in sorted(rows, key=lambda r: r["changed_at"]):
            best[r[key] if key else None] = r
        return list(best.values())

    def rows(self, sql, params=None):
        self.sql.append(sql)
        p = {x.a[0]: x.a[2] for x in (params or [])}
        tb = self._table(sql)
        rows = self.tables[tb]
        if "QUALIFY" in sql and tb == cs.T_RECON:
            return [Row(r) for r in self._latest(rows, "recon_id")]
        if "DISTINCT source_type" in sql:
            return [Row(source_type=s) for s in sorted({r["source_type"] for r in self.tables[cs.T_SOURCE]})]
        if "ORDER BY changed_at DESC LIMIT" in sql:
            rows = [r for r in rows if "k" not in p or r["source_type"] == p["k"]]
            return [Row(r) for r in sorted(rows, key=lambda r: r["changed_at"], reverse=True)]
        if "changed_at <= @t" in sql:
            rows = [r for r in rows if r["source_type"] == p["s"] and r["changed_at"] <= p["t"]]
            return [Row(r) for r in self._latest(rows)]
        if tb == cs.T_SOURCE:
            return [Row(r) for r in self._latest([r for r in rows if r["source_type"] == p["s"]])]
        return [Row(r) for r in self._latest(rows)]

    def run(self, sql, params=None):
        self.sql.append(sql)
        assert sql.startswith("INSERT INTO"), sql
        p = {x.a[0]: x.a[2] for x in params}
        tb = self._table(sql)
        cols = sql.split("(", 1)[1].split(")", 1)[0].replace(" ", "").split(",")
        self.clock += dt.timedelta(minutes=5)
        row = {c: p.get(c) for c in cols if c not in ("changed_at", "changed_by", "note")}
        row.update(changed_at=self.clock, changed_by=p.get("changed_by", p.get("by")), note=p.get("note", p.get("n")))
        self.tables[tb].append(row)


def fails(fn, *frags):
    try:
        fn(); raise SystemExit("expected a ConfigError")
    except cs.ConfigError as e:
        for f in frags:
            assert f in str(e), (f, str(e))


# 1. validation lists every problem at once
bad = {"lookup_window_days": 0, "conflict_policy": "ignore", "input_prefix": "dataflow/in", "max_reject_ratio": 2,
       "detail_table": "only.two", "flex_template_gcs_path": "gs://b/t.txt", "df_temp_location": "/tmp/x",
       "ignore_suffixes": "tmp", "orphan_after_min": "15", "nope": 1}
fails(lambda: cs.validate_row(bad, cs.SOURCE_SPEC, "x"), "lookup_window_days", "conflict_policy", "input_prefix",
      "max_reject_ratio", "detail_table", "flex_template_gcs_path", "df_temp_location", "ignore_suffixes",
      "orphan_after_min", "unknown column 'nope'")
cs.validate_row({"lookup_window_days": None, "df_network": None}, cs.SOURCE_SPEC, "x")      # NULL is allowed
print("1 validation: all problems reported together, NULL allowed")

# 2. command-line text -> typed values
assert cs.coerce("lookup_window_days", "45", cs.SOURCE_SPEC) == 45
assert cs.coerce("max_reject_ratio", "0.02", cs.SOURCE_SPEC) == 0.02
assert cs.coerce("skip_if_running", "false", cs.SOURCE_SPEC) is False and cs.coerce("enabled", "Yes", cs.SOURCE_SPEC) is True
assert cs.coerce("df_network", "null", cs.SOURCE_SPEC) is None
fails(lambda: cs.coerce("lookup_window_days", "many", cs.SOURCE_SPEC), "not a valid int")
fails(lambda: cs.coerce("lookup_windw_days", "5", cs.SOURCE_SPEC), "unknown column")
print("2 coercion ok")

# 3. every setting survives settings -> row -> settings (lossless)
row = cs.settings_to_row(BASE)
built = cs.settings_from_row("PGW", row, BASE.kms_key_name)
assert dataclasses.replace(built, launch_mode=BASE.launch_mode) == BASE, "a field is missing from SOURCE_SPEC"
print("3 round trip is lossless")

# 3b. NO defaults: an empty column stops the run and is named; only the df_* Dataflow options may be empty
GOOD_GLOBAL = {"changed_at": dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), "max_attempts": 3, "batch_files": 200,
               "lease_min": 180, "max_job_wait_min": 240}


class OneRow:
    def __init__(self, src_row, glob=None): self.src, self.glob = src_row, glob or GOOD_GLOBAL
    def rows(self, sql, params=None):
        data = dict(self.src) if "prism_config_source" in sql else dict(self.glob)
        data.setdefault("changed_at", dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc))
        return [Row(data)]


def resolve_with(glob=None, **over):
    return cs.ConfigStore(OneRow(dict(row, **over), glob), "p", "VPDB_ADM").resolve("PGW", BASE.kms_key_name)


fails(lambda: resolve_with(lookup_window_days=None, conflict_policy=None, enabled=None, input_prefix=None, allow_empty_files=None),
      "lookup_window_days", "conflict_policy", "enabled", "input_prefix", "allow_empty_files", "No default values")
fails(lambda: resolve_with(glob=dict(GOOD_GLOBAL, max_attempts=None, lease_min=None)), "max_attempts", "lease_min")
res = resolve_with(df_network=None, df_subnetwork=None, df_max_workers=None, df_machine_type=None, df_service_account=None,
                   df_temp_location=None, df_staging_location=None)
assert res.settings.dataflow_env == {}                                  # nothing passed: Dataflow's own defaults apply
assert resolve_with(enabled=False).enabled is False and resolve_with(enabled=True).enabled is True
assert resolve_with(ignore_suffixes="").settings.src.ignore_suffixes == ()   # an EMPTY string is a value (no suffixes)
print("3b no defaults: every empty column is named together; only df_* may be empty; '' is a value")

# 4. the store: seed, resolve, change, history, rollback, enable/disable
bq = FakeBq(); st = cs.ConfigStore(bq, "p", "VPDB_ADM")
fails(lambda: st.resolve("PGW", BASE.kms_key_name), "seed")
assert st.seed({"PGW": BASE}, DEFAULTS, "rishi") == ["global", "PGW"]
assert st.seed({"PGW": BASE}, DEFAULTS, "rishi") == []                      # never overwrites
res = st.resolve("PGW", BASE.kms_key_name)
assert dataclasses.replace(res.settings, launch_mode=BASE.launch_mode) == BASE and res.globals == DEFAULTS and res.enabled and res.version.startswith("global@")
v1 = res.version
d = st.set_source("PGW", {"lookup_window_days": 45, "conflict_policy": "supersede"}, "rishi", "corrections expected")
assert sorted(k for k, _, _ in d) == ["conflict_policy", "lookup_window_days"]
res = st.resolve("PGW", BASE.kms_key_name)
assert res.settings.src.lookup_window_days == 45 and res.version != v1
n_rows = len(bq.tables[cs.T_SOURCE])
assert st.set_source("PGW", {"lookup_window_days": 45}, "rishi") == [] and len(bq.tables[cs.T_SOURCE]) == n_rows   # no-op writes nothing
fails(lambda: st.set_source("PGW", {"lookup_window_days": -3}, "rishi"), "lookup_window_days")
assert len(bq.tables[cs.T_SOURCE]) == n_rows                                  # bad value never written
last = bq.tables[cs.T_SOURCE][-1]
assert last["changed_by"] == "rishi" and last["note"] == "corrections expected"
assert all("CURRENT_TIMESTAMP()" in s for s in bq.sql if s.startswith("INSERT"))
hist = st.history(cs.T_SOURCE, "source_type", "PGW", 5)
assert sorted(hist[0]["_changed"]) == ["conflict_policy", "lookup_window_days"] and hist[-1]["changed_by"] == "rishi"
as_of = bq.tables[cs.T_SOURCE][0]["changed_at"]                               # the seeded version
st.rollback_source("PGW", as_of, "rishi")
res = st.resolve("PGW", BASE.kms_key_name)
assert res.settings.src.lookup_window_days == 30 and len(bq.tables[cs.T_SOURCE]) == n_rows + 1   # restored as the NEWEST version
st.set_source("PGW", {"enabled": False}, "rishi", "disable")
assert st.resolve("PGW", BASE.kms_key_name).enabled is False
st.set_global({"max_attempts": 1, "batch_files": 50}, "rishi")
g = st.resolve("PGW", BASE.kms_key_name).globals
assert g["MAX_ATTEMPTS"] == 1 and g["BATCH_FILES"] == 50 and g["LEASE_MIN"] == 180
print("4 seed / resolve / change / no-op / reject bad value / history / rollback / disable / global ok")

# 4b. a blank can never be STORED either
rows_now = len(bq.tables[cs.T_SOURCE])
fails(lambda: st.set_source("PGW", {"lookup_window_days": cs.coerce("lookup_window_days", "null", cs.SOURCE_SPEC)}, "rishi"),
      "lookup_window_days", "No default values")
fails(lambda: st.set_source("PGW", {"enabled": None}, "rishi"), "enabled")
assert len(bq.tables[cs.T_SOURCE]) == rows_now
st.set_source("PGW", {"df_network": cs.coerce("df_network", "null", cs.SOURCE_SPEC)}, "rishi", "use Dataflow default network")
assert "network" not in st.resolve("PGW", BASE.kms_key_name).settings.dataflow_env          # df_* may be emptied on purpose
st.set_source("PGW", {"ignore_suffixes": cs.coerce("ignore_suffixes", "none", cs.SOURCE_SPEC)}, "rishi")
assert st.resolve("PGW", BASE.kms_key_name).settings.src.ignore_suffixes == ()
print("4b blanks are rejected on write; df_* can be emptied; ignore_suffixes=none stores an empty value")

# 4c. the TABLE decides which sources exist: a typo is explained, a source that is only a row works
fails(lambda: st.resolve("pgw", BASE.kms_key_name), "pgw", "['PGW']", "case-sensitive")
st._insert(cs.T_SOURCE, cs.SOURCE_SPEC, "source_type", "NEWSRC", cs.settings_to_row(BASE), "rishi", "new source")
assert st.sources() == ["NEWSRC", "PGW"]
assert st.resolve("NEWSRC", BASE.kms_key_name).settings.src.source_type == "NEWSRC"
print("4c unknown name explained with the configured sources; a source defined only in the table resolves")

# 5. recon definitions
ex = json.load(open(os.path.join(os.path.dirname(__file__), "..", "recon_definitions.example.json")))["recons"][0]
fails(lambda: st.put_recon("R1", json.dumps(ex), "rishi"), "PLACEHOLDER")        # unfinished template refused
txt = json.dumps(ex)
for ph, val in (("<SIDE_B_DETAIL_TABLE>", "p.d.b"), ("<SIDE_B_SOURCE_TYPE>", "SRC_B"), ("<SIDE_B_TIMESTAMP_EXPRESSION>", "TIMESTAMP(d)"),
                ("<SIDE_B_NE_COLUMN>", "ne"), ("<SIDE_B_APN_COLUMN>", "apn"), ("<SIDE_B_VOLUME>", "vol"),
                ("<SIDE_B_SESSION_COLUMN>", "sid"), ("<SIDE_B_RATING_GROUP_COLUMN>", "rg")):
    txt = txt.replace(ph, val)
st.put_recon("R1", txt, "rishi", "first version")
defs = rl.parse_definitions(list(st.latest_recons().values()))
assert "R1" in defs and defs["R1"]["enabled"] is True
st.put_recon("R1", txt, "rishi", "pause", enabled=False)
assert rl.parse_definitions(list(st.latest_recons().values()))["R1"]["enabled"] is False
print("5 recon definitions: placeholders refused, stored, versioned, can be disabled")
print("ALL CONFIG STORE CHECKS PASSED")
