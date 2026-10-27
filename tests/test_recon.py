"""
Offline checks of the reconciliation code (no GCP).   python tests/test_recon.py
Verifies LOGIC: definition validation, generated SQL shape, the alarm state machine, the provisional
gate, the cost guard and the order of statements. It does NOT prove the SQL is valid BigQuery SQL -
run `python recon_runner.py run <id> --dry-run` against your project for that.
"""
import copy
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))


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
import recon_logic as rl            # noqa: E402
import recon_runner as rr           # noqa: E402

NOW = datetime(2026, 10, 7, 12, 30, tzinfo=timezone.utc)
ex = json.load(open(os.path.join(HERE, "..", "recon_definitions.example.json")))["recons"][0]

# ------------------------------------------------------------------ 1. definitions
try:
    rl.validate_definition(ex); raise SystemExit("placeholders must be rejected")
except ValueError as e:
    assert "PLACEHOLDER" in str(e)

good = copy.deepcopy(ex)
text = json.dumps(good)
for ph, val in (("<SIDE_B_DETAIL_TABLE>", "p.d.b_detail"), ("<SIDE_B_SOURCE_TYPE>", "SRC_B"),
                ("<SIDE_B_TIMESTAMP_EXPRESSION>", "TIMESTAMP(event_date)"), ("<SIDE_B_NE_COLUMN>", "ne"),
                ("<SIDE_B_APN_COLUMN>", "apn"), ("<SIDE_B_VOLUME>", "vol"),
                ("<SIDE_B_SESSION_COLUMN>", "sid"), ("<SIDE_B_RATING_GROUP_COLUMN>", "rg")):
    text = text.replace(ph, val)
d = rl.validate_definition(json.loads(text))
assert d["pending_grace_minutes"] == 120 and d["required_runs"] == 2

bad = json.loads(text); bad["grain"] = "WEEK"; bad["measures"][0]["name"] = "bad name"; bad["dimensions"] = []
try:
    rl.validate_definition(bad); raise SystemExit("bad definition accepted")
except ValueError as e:
    msg = str(e)
    assert "grain" in msg and "bad name" in msg and "dimension" in msg      # ALL problems reported at once
print("definitions: ok")

# ------------------------------------------------------------------ 2. window range
frm, to = rl.window_range(d, NOW)
assert frm == datetime(2026, 10, 4, tzinfo=timezone.utc)            # 3 days back, day start
assert to == datetime(2026, 10, 7, tzinfo=timezone.utc)             # now - 12h = 00:30 -> day start
dh = dict(d, grain="HOUR", settle_hours=6)
assert rl.window_range(dh, NOW)[1] == datetime(2026, 10, 7, 6, tzinfo=timezone.utc)
print("windows: ok")

# ------------------------------------------------------------------ 3. aggregate SQL shape
sql = rl.build_aggregate_sql(d, "p.VPDB_ADM.recon_result")
for frag in ("MERGE `p.VPDB_ADM.recon_result` T", "FULL OUTER JOIN", "TIMESTAMP_TRUNC(TIMESTAMP(EVT_DATE), DAY)",
             "TIMESTAMP_TRUNC(TIMESTAMP(event_date), DAY)", "TO_JSON_STRING(STRUCT(NE_ID AS NE_ID, APN AS APN))",
             "TO_JSON_STRING(STRUCT(ne AS NE_ID, apn AS APN))", "SUM(MES_DATA_VOLUME) AS m_data_volume",
             "SUM(vol) AS m_data_volume", "'records' AS measure", "'data_volume' AS measure",
             "WHEN NOT MATCHED BY SOURCE", "MISSING_IN_A", "MISSING_IN_B", "@from_ts", "@to_ts", "@provisional"):
    assert frag in sql, frag
assert sql.count("UNION ALL") == len(d["measures"]) - 1
assert sql.count("(") == sql.count(")") and "{" not in sql and "}" not in sql
print("aggregate sql: ok")

# ------------------------------------------------------------------ 4. alarm state machine
W = datetime(2026, 10, 6, tzinfo=timezone.utc)
breach = {W: {"severity": "MEDIUM", "summary": "{}"}}
ids = iter(f"ALM-{i}" for i in range(100))
nid = lambda r, w: next(ids)                                         # noqa: E731
T0 = NOW


def step(existing, breached, evaluated, runs=2, prov=False, t=T0):
    return rl.evaluate_alarms(existing, breached, evaluated, "R", runs, prov, t, nid)


up, nt = step({}, breach, {W})                                       # run 1: PENDING, silent
assert up[0]["state"] == "PENDING" and nt == []
cur = {W: up[0]}
up, nt = step(cur, breach, {W}, t=T0 + timedelta(hours=1))           # run 2: OPEN + notify
assert up[0]["state"] == "OPEN" and up[0]["breach_runs"] == 2 and len(nt) == 1
cur = {W: up[0]}
up, nt = step(cur, breach, {W}, t=T0 + timedelta(hours=2))           # run 3: stays OPEN, no repeat notify
assert up[0]["state"] == "OPEN" and nt == []
cur = {W: up[0]}
esc = {W: {"severity": "HIGH", "summary": "{}"}}
up, nt = step(cur, esc, {W}, t=T0 + timedelta(hours=3))              # severity increase -> notify again
assert len(nt) == 1 and up[0]["severity"] == "HIGH"
cur = {W: up[0]}
up, nt = step(cur, {}, {W}, t=T0 + timedelta(hours=4))               # numbers match again -> RESOLVED
assert up[0]["state"] == "RESOLVED" and up[0]["resolved_at"] is not None and nt == []
cur = {W: up[0]}
up, nt = step(cur, breach, {W}, t=T0 + timedelta(hours=5))           # breach after RESOLVED -> NEW alarm
assert up[0]["alarm_id"] != cur[W]["alarm_id"] and up[0]["state"] == "PENDING"

# a PENDING alarm that clears resolves silently
p1, _ = step({}, breach, {W}); p2, n2 = step({W: p1[0]}, {}, {W})
assert p2[0]["state"] == "RESOLVED" and n2 == []
# provisional run changes nothing
assert step({W: p1[0]}, {}, {W}, prov=True) == ([], []) and step({}, breach, {W}, prov=True) == ([], [])
# acknowledged stays acknowledged while breaching; suppressed is never touched
ack = dict(p1[0], state="ACKNOWLEDGED", breach_runs=3)
assert step({W: ack}, breach, {W})[0][0]["state"] == "ACKNOWLEDGED"
sup = dict(p1[0], state="SUPPRESSED")
assert step({W: sup}, breach, {W}) == ([], [])
# required_runs = 1 opens at once
u1, n1 = step({}, breach, {W}, runs=1)
assert u1[0]["state"] == "OPEN" and len(n1) == 1
assert rl.severity_of(d, 1, 0) == "HIGH" and rl.severity_of(d, 0, 5.0) == "HIGH"
assert rl.severity_of(d, 0, 1.5) == "MEDIUM" and rl.severity_of(d, 0, 0.2) == "LOW"
print("alarm state machine: ok")

# ------------------------------------------------------------------ 5. detail SQL
ssql, psql, dp = rl.build_detail_sql(d, {"APN": "internet"}, "p.c.sum", "p.c.sample")
assert dp == {"dim_APN": "internet"}
for frag in ("INSERT INTO `p.c.sum`", "FULL OUTER JOIN", "ONLY_IN_A", "ONLY_IN_B", "VALUE_DIFF", "MATCHED",
             "CAST(APN AS STRING) = @dim_APN", "CAST(apn AS STRING) = @dim_APN", "@w_from", "@w_to",
             "TO_JSON_STRING(STRUCT(SESSION_ID AS session_id, RATING_GROUP AS rating_group))", "GROUP BY k"):
    assert frag in ssql, frag
assert "QUALIFY ROW_NUMBER()" in psql and "@sample_rows" in psql and "category != 'MATCHED'" in psql
try:
    rl.build_detail_sql(d, {"NOPE": "x"}, "s", "p"); raise SystemExit("unknown dim accepted")
except ValueError:
    pass
print("detail sql: ok")


# ------------------------------------------------------------------ 6. runner flow with a fake BigQuery
class Row:
    def __init__(self, **k): self.__dict__.update(k)
    def items(self): return self.__dict__.items()


def fake_recon(pending=0, breached_rows=None, evaluated=None, alarms=None, dry_bytes=1000):
    r = rr.Recon.__new__(rr.Recon)
    r.result_t, r.alarm_t, r.req_t = "res", "alm", "req"
    r.sum_t, r.sample_t, r.reg_t = "sum", "smp", "reg"
    r.notified, r.executed, r.alarm_store = [], [], list(alarms or [])
    r.notify_fn = lambda a: r.notified.append(a["alarm_id"])

    def _rows(sql, params=None):
        if "FROM `reg`" in sql and "COUNT(*) AS c" in sql: return [Row(c=pending)]
        if "breach_rows" in sql: return breached_rows or []
        if "SELECT DISTINCT window_start" in sql: return [Row(window_start=w) for w in (evaluated or [])]
        if "QUALIFY ROW_NUMBER" in sql: return [Row(**a) for a in r.alarm_store]
        if "FROM `alm` WHERE alarm_id" in sql: return [Row(recon_id=d["recon_id"], window_start=W)]
        if "GROUP BY category" in sql: return [Row(category="ONLY_IN_B", row_count=7)]
        return []
    r._rows = _rows
    r._run = lambda sql, params=None, max_bytes=None: (r.executed.append(sql.split("\n")[0][:60]) or Row(num_dml_affected_rows=1))
    r._dry = lambda sql, params=None: dry_bytes
    r._upsert_alarms = lambda rows: (r.alarm_store.clear(), r.alarm_store.extend(rows))
    return r


worst = json.dumps([{"dim_key": "{}", "measure": "records", "diff": 5}])
br = [Row(window_start=W, breach_rows=2, missing_rows=0, max_pct=2.0, worst=worst)]

rc = fake_recon(pending=3, breached_rows=br, evaluated=[W])           # files in flight -> provisional
out = rc.run_aggregate(d, NOW)
assert out["provisional"] and out["alarms_written"] == 0 and rc.notified == []

rc = fake_recon(pending=0, breached_rows=br, evaluated=[W])           # run 1: PENDING, no notification
out = rc.run_aggregate(d, NOW)
assert out["alarms_written"] == 1 and rc.notified == [] and rc.alarm_store[0]["state"] == "PENDING"
rc._rows = (lambda f: (lambda sql, params=None: [Row(**a) for a in rc.alarm_store] if "QUALIFY ROW_NUMBER" in sql else f(sql, params)))(rc._rows)
out = rc.run_aggregate(d, NOW + timedelta(hours=1))                   # run 2: OPEN + one notification
assert rc.alarm_store[0]["state"] == "OPEN" and len(rc.notified) == 1
print("runner aggregate flow: ok")

defs = {d["recon_id"]: d}
rc = fake_recon(dry_bytes=10 * 1024 ** 4)                             # 10 TiB per statement -> refused
res = rc.run_detail(defs, "ALM-1", "rishi")
assert res["status"] == "REFUSED" and not any(s.startswith("INSERT INTO `sum`") for s in rc.executed)
rc = fake_recon(dry_bytes=1000)
res = rc.run_detail(defs, "ALM-1", "rishi", dims={"APN": "internet"})
assert res["status"] == "DONE" and res["summary"][0]["category"] == "ONLY_IN_B"
ins = [s for s in rc.executed if s.startswith("INSERT INTO")]
assert sum("`sum`" in s for s in ins) == 3 and sum("`smp`" in s for s in ins) == 1   # summary + 2 context rows, sample
assert rc.executed[-1].startswith("UPDATE `req`") or any(s.startswith("UPDATE `req`") for s in rc.executed)
res = rc.run_detail(defs, "ALM-1", "rishi", dry_run=True)
assert res["dry_run"] and res["bytes_estimated"] == 2000
print("runner detail flow: ok")
print("ALL RECON CHECKS PASSED")
