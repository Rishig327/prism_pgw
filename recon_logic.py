"""
recon_logic.py - pure logic for PRISM reconciliation (no BigQuery calls, fully unit-testable).

  * load_definitions / validate_definition : recon definitions live in a JSON file (git-controlled)
  * build_aggregate_sql                    : side A vs side B at an agreed grain -> MERGE into recon_result
  * evaluate_alarms                        : alarm state machine (PENDING -> OPEN -> RESOLVED, ACK, SUPPRESSED)
  * build_detail_sql                       : record-level comparison for ONE alarm (manual drill-down)

Orchestrator-only code: runs on the VM, never shipped to Dataflow workers.
"""
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Tuple

IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
GRAINS = {"HOUR": timedelta(hours=1), "DAY": timedelta(days=1)}
ACTIVE = ("PENDING", "OPEN", "ACKNOWLEDGED")
SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3}


# ------------------------------------------------------------------ definitions
def validate_definition(d: dict) -> dict:
    """Returns the definition with defaults filled in, or raises ValueError with ALL problems."""
    errs: List[str] = []

    def need(cond, msg):
        if not cond:
            errs.append(msg)

    need(IDENT.match(str(d.get("recon_id", "")) or ""), "recon_id must be letters/digits/underscore")
    need(d.get("grain") in GRAINS, "grain must be HOUR or DAY")
    for s in ("side_a", "side_b"):
        side = d.get(s) or {}
        need(side.get("table"), f"{s}.table is required")
        need(side.get("window_expr"), f"{s}.window_expr is required (an expression returning a TIMESTAMP)")
        need(side.get("source_types"), f"{s}.source_types is required (registry source types feeding this side)")
    dims = d.get("dimensions") or []
    need(dims, "at least one dimension is required")
    need(len({x.get("name") for x in dims}) == len(dims), "dimension names must be unique")
    for x in dims:
        need(IDENT.match(str(x.get("name", ""))), f"dimension name {x.get('name')!r} must be an identifier")
        need(x.get("a") and x.get("b"), f"dimension {x.get('name')} needs both 'a' and 'b' expressions")
    ms = d.get("measures") or []
    need(ms, "at least one measure is required")
    need(len({x.get("name") for x in ms}) == len(ms), "measure names must be unique")
    for x in ms:
        need(IDENT.match(str(x.get("name", ""))), f"measure name {x.get('name')!r} must be an identifier")
        need(x.get("a") and x.get("b"), f"measure {x.get('name')} needs both 'a' and 'b' aggregate expressions")
    if d.get("detail"):
        dt = d["detail"]
        need(dt.get("key"), "detail.key is required")
        for x in dt.get("key", []) + dt.get("compare", []):
            need(IDENT.match(str(x.get("name", ""))), f"detail name {x.get('name')!r} must be an identifier")
            need(x.get("a") and x.get("b"), f"detail {x.get('name')} needs 'a' and 'b'")
    if "<" in json.dumps(d):
        errs.append("definition still contains <PLACEHOLDER> values; replace them with real tables/columns")
    if errs:
        raise ValueError(f"recon definition {d.get('recon_id')!r} invalid:\n  - " + "\n  - ".join(errs))

    out = json.loads(json.dumps(d))
    out.setdefault("enabled", True)
    out.setdefault("settle_hours", 6)
    out.setdefault("lookback_days", 3)
    out.setdefault("required_runs", 2)
    out.setdefault("gate_on_pending_files", True)
    out.setdefault("pending_grace_minutes", 120)   # only RECENT unfinished files make a run provisional
    out.setdefault("severity", {"high_pct": 5.0, "medium_pct": 1.0})
    for s in ("side_a", "side_b"):
        out[s].setdefault("where", "TRUE")
        out[s].setdefault("label", s.upper())
    for m in out["measures"]:
        m.setdefault("abs_tol", 0)
        m.setdefault("pct_tol", 0)
    if out.get("detail"):
        out["detail"].setdefault("compare", [])
        out["detail"].setdefault("file_col_a", "file_id")
        out["detail"].setdefault("file_col_b", "file_id")
        out["detail"].setdefault("max_bytes", 200 * 1024 ** 3)
        out["detail"].setdefault("sample_rows", 100)
    return out


def load_definitions(path: str) -> Dict[str, dict]:
    return parse_definitions(json.load(open(path, encoding="utf-8")))


def parse_definitions(raw) -> Dict[str, dict]:
    """raw = parsed JSON ({"recons": [...]} or a list). Used for local files and for gs:// files."""
    items = raw["recons"] if isinstance(raw, dict) else raw
    out = {}
    for d in items:
        v = validate_definition(d)
        out[v["recon_id"]] = v
    return out


def window_range(d: dict, now: datetime) -> Tuple[datetime, datetime]:
    """[from, to): settled windows only. 'to' is now - settle_hours, cut down to the grain."""
    to = now - timedelta(hours=d["settle_hours"])
    to = to.replace(minute=0, second=0, microsecond=0)
    if d["grain"] == "DAY":
        to = to.replace(hour=0)
    frm = (now - timedelta(days=d["lookback_days"])).replace(hour=0, minute=0, second=0, microsecond=0)
    return frm, to


# ------------------------------------------------------------------ aggregate SQL
def _side_cte(d: dict, which: str) -> str:
    s = d["side_a" if which == "a" else "side_b"]
    g = d["grain"]
    wexpr = s["window_expr"]
    dims = ", ".join(f"{x[which]} AS {x['name']}" for x in d["dimensions"])
    meas = ",\n    ".join(f"{m[which]} AS m_{m['name']}" for m in d["measures"])
    return f"""{which} AS (
  SELECT
    TIMESTAMP_TRUNC({wexpr}, {g}) AS window_start,
    TO_JSON_STRING(STRUCT({dims})) AS dim_key,
    COUNT(*) AS n_rows,
    {meas}
  FROM `{s['table']}`
  WHERE ({s['where']}) AND ({wexpr}) >= @from_ts AND ({wexpr}) < @to_ts
  GROUP BY window_start, dim_key
)"""


def build_aggregate_sql(d: dict, result_table: str) -> str:
    """One MERGE: recompute every settled window in [@from_ts, @to_ts) and replace the old rows
    (also deletes rows that no longer exist). Safe to re-run any time."""
    names = [m["name"] for m in d["measures"]]
    sel_j = ",\n    ".join(f"a.m_{n} AS a_{n}, b.m_{n} AS b_{n}" for n in names)
    branches = "\n  UNION ALL\n  ".join(
        f"SELECT window_start, dim_key, '{m['name']}' AS measure, a_{m['name']} AS a_value, "
        f"b_{m['name']} AS b_value, a_rows, b_rows, {float(m['abs_tol'])} AS abs_tol, "
        f"{float(m['pct_tol'])} AS pct_tol FROM j" for m in d["measures"])
    return f"""MERGE `{result_table}` T
USING (
  WITH
  {_side_cte(d, 'a')},
  {_side_cte(d, 'b')},
  j AS (
    SELECT
      COALESCE(a.window_start, b.window_start) AS window_start,
      COALESCE(a.dim_key, b.dim_key) AS dim_key,
      a.n_rows AS a_rows, b.n_rows AS b_rows,
      {sel_j}
    FROM a FULL OUTER JOIN b ON a.window_start = b.window_start AND a.dim_key = b.dim_key
  ),
  u AS (
  {branches}
  ),
  d AS (
    SELECT *, IFNULL(b_value, 0) - IFNULL(a_value, 0) AS diff,
      SAFE_DIVIDE(ABS(IFNULL(b_value, 0) - IFNULL(a_value, 0)),
                  GREATEST(ABS(IFNULL(a_value, 0)), ABS(IFNULL(b_value, 0)))) * 100 AS diff_pct
    FROM u
  )
  SELECT @recon_id AS recon_id, window_start, dim_key, measure,
         CAST(a_value AS FLOAT64) AS a_value, CAST(b_value AS FLOAT64) AS b_value,
         CAST(diff AS FLOAT64) AS diff, diff_pct, a_rows, b_rows,
         CASE WHEN a_rows IS NULL THEN 'MISSING_IN_A'
              WHEN b_rows IS NULL THEN 'MISSING_IN_B'
              WHEN ABS(diff) <= abs_tol OR diff_pct <= pct_tol THEN 'MATCH'
              ELSE 'MISMATCH' END AS status
  FROM d
) S
ON T.recon_id = S.recon_id AND T.window_start = S.window_start
   AND T.dim_key = S.dim_key AND T.measure = S.measure
WHEN MATCHED THEN UPDATE SET
  a_value = S.a_value, b_value = S.b_value, diff = S.diff, diff_pct = S.diff_pct,
  a_rows = S.a_rows, b_rows = S.b_rows, status = S.status,
  provisional = @provisional, run_id = @run_id, computed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED BY TARGET THEN INSERT
  (recon_id, window_start, dim_key, measure, a_value, b_value, diff, diff_pct, a_rows, b_rows,
   status, provisional, run_id, computed_at)
  VALUES (S.recon_id, S.window_start, S.dim_key, S.measure, S.a_value, S.b_value, S.diff, S.diff_pct,
          S.a_rows, S.b_rows, S.status, @provisional, @run_id, CURRENT_TIMESTAMP())
WHEN NOT MATCHED BY SOURCE AND T.recon_id = @recon_id AND T.window_start >= @from_ts
     AND T.window_start < @to_ts THEN DELETE"""


def build_breach_sql(result_table: str) -> str:
    """Breached windows of the run: one row per window with counts and the 5 worst rows."""
    return f"""SELECT window_start,
  COUNT(*) AS breach_rows,
  COUNTIF(status LIKE 'MISSING%') AS missing_rows,
  IFNULL(MAX(IF(status = 'MISMATCH', diff_pct, NULL)), 0) AS max_pct,
  TO_JSON_STRING(ARRAY_AGG(STRUCT(dim_key, measure, a_value, b_value, diff, diff_pct, status)
                 ORDER BY IFNULL(diff_pct, 100) DESC LIMIT 5)) AS worst
FROM `{result_table}`
WHERE recon_id = @recon_id AND window_start >= @from_ts AND window_start < @to_ts AND status != 'MATCH'
GROUP BY window_start"""


def build_evaluated_sql(result_table: str) -> str:
    return (f"SELECT DISTINCT window_start FROM `{result_table}` "
            "WHERE recon_id = @recon_id AND window_start >= @from_ts AND window_start < @to_ts")


def severity_of(d: dict, missing_rows: int, max_pct: float) -> str:
    sev = d["severity"]
    if missing_rows > 0 or max_pct >= sev["high_pct"]:
        return "HIGH"
    if max_pct >= sev["medium_pct"]:
        return "MEDIUM"
    return "LOW"


# ------------------------------------------------------------------ alarm state machine
def evaluate_alarms(existing: Dict[datetime, dict], breached: Dict[datetime, dict], evaluated: set,
                    recon_id: str, required_runs: int, provisional: bool, now: datetime,
                    new_id: Optional[Callable[[str, datetime], str]] = None) -> Tuple[List[dict], List[dict]]:
    """
    existing  : window_start -> latest alarm row for that window (any state)
    breached  : window_start -> {"severity", "summary"}  (windows with at least one non-MATCH row)
    evaluated : every window_start that was recomputed in this run
    Returns (rows_to_upsert, alarms_to_notify).

    Rules
      * provisional run (files still pending): change nothing - no new alarms, no resolutions.
      * breach, no active alarm : new alarm, PENDING (OPEN at once if required_runs == 1)
      * breach, active alarm    : breach_runs + 1; PENDING -> OPEN after required_runs; notify on
                                  becoming OPEN and on a severity increase
      * no breach, active alarm : RESOLVED (a PENDING one resolves silently)
      * SUPPRESSED / RESOLVED alarms are never touched by a breach on the same window; a breach
        after RESOLVED opens a NEW alarm.
    """
    if provisional:
        return [], []
    new_id = new_id or (lambda rid, w: f"ALM-{rid}-{w:%Y%m%d%H}-{uuid.uuid4().hex[:4]}")
    upserts, notify = [], []

    for w, b in breached.items():
        cur = existing.get(w)
        if cur and cur["state"] == "SUPPRESSED":
            continue
        if cur and cur["state"] in ACTIVE:
            row = dict(cur)
            row["breach_runs"] = cur["breach_runs"] + 1
            row["last_seen_at"], row["summary"] = now, b["summary"]
            old_sev = cur["severity"]
            row["severity"] = b["severity"]
            if cur["state"] == "PENDING" and row["breach_runs"] >= required_runs:
                row["state"] = "OPEN"
                notify.append(row)
            elif cur["state"] in ("OPEN", "ACKNOWLEDGED") and SEVERITY_RANK[b["severity"]] > SEVERITY_RANK[old_sev]:
                notify.append(row)
            upserts.append(row)
        else:
            state = "OPEN" if required_runs <= 1 else "PENDING"
            row = {"alarm_id": new_id(recon_id, w), "recon_id": recon_id, "window_start": w,
                   "state": state, "severity": b["severity"], "breach_runs": 1,
                   "first_raised_at": now, "last_seen_at": now, "resolved_at": None,
                   "acknowledged_by": None, "acknowledged_at": None, "note": None,
                   "summary": b["summary"], "notified_at": None}
            upserts.append(row)
            if state == "OPEN":
                notify.append(row)

    for w in evaluated:
        if w in breached:
            continue
        cur = existing.get(w)
        if cur and cur["state"] in ACTIVE:
            row = dict(cur)
            row["state"], row["resolved_at"], row["last_seen_at"] = "RESOLVED", now, now
            upserts.append(row)
    for n in notify:
        n["notified_at"] = now
    return upserts, notify


# ------------------------------------------------------------------ detail (record-level) SQL
def build_detail_sql(d: dict, dims_filter: Optional[Dict[str, str]], summary_table: str,
                     sample_table: str) -> Tuple[str, str, Dict[str, str]]:
    """
    Returns (summary_insert_sql, sample_insert_sql, dim_params).
    Records of ONE window (and optionally one dimension slice) are matched on detail.key and
    classified: MATCHED, ONLY_IN_A, ONLY_IN_B, VALUE_DIFF. Each side is pre-aggregated per key,
    so several rows per key (for example one per rating group) are compared as totals.
    Query parameters expected: @request_id @w_from @w_to @sample_rows and @dim_<name> for each filter.
    """
    dt = d["detail"]
    dims_filter = dims_filter or {}
    by_name = {x["name"]: x for x in d["dimensions"]}
    unknown = set(dims_filter) - set(by_name)
    if unknown:
        raise ValueError(f"unknown dimension(s) in filter: {sorted(unknown)}")
    params = {f"dim_{k}": str(v) for k, v in dims_filter.items()}

    def side(which):
        s = d["side_a" if which == "a" else "side_b"]
        key = ", ".join(f"{x[which]} AS {x['name']}" for x in dt["key"])
        cmp_ = "".join(f",\n    {c[which]} AS c_{c['name']}" for c in dt["compare"])
        filt = "".join(f" AND CAST({by_name[k][which]} AS STRING) = @dim_{k}" for k in dims_filter)
        wexpr = s["window_expr"]
        return f"""{which} AS (
  SELECT TO_JSON_STRING(STRUCT({key})) AS k, ANY_VALUE({dt['file_col_' + which]}) AS file_id,
    COUNT(*) AS n_rows{cmp_}
  FROM `{s['table']}`
  WHERE ({s['where']}) AND ({wexpr}) >= @w_from AND ({wexpr}) < @w_to{filt}
  GROUP BY k
)"""

    diffs = " OR ".join(["a.n_rows != b.n_rows"] + [
        f"ABS(IFNULL(a.c_{c['name']}, 0) - IFNULL(b.c_{c['name']}, 0)) > {float(c.get('tol', 0))}"
        for c in dt["compare"]])
    a_cols = ", ".join(["a.n_rows AS n_rows"] + [f"a.c_{c['name']} AS {c['name']}" for c in dt["compare"]])
    b_cols = ", ".join(["b.n_rows AS n_rows"] + [f"b.c_{c['name']} AS {c['name']}" for c in dt["compare"]])
    cte = f"""WITH
  {side('a')},
  {side('b')},
  j AS (
    SELECT COALESCE(a.k, b.k) AS k, a.file_id AS a_file, b.file_id AS b_file,
      CASE WHEN a.k IS NULL THEN 'ONLY_IN_B' WHEN b.k IS NULL THEN 'ONLY_IN_A'
           WHEN {diffs} THEN 'VALUE_DIFF' ELSE 'MATCHED' END AS category,
      TO_JSON_STRING(STRUCT({a_cols})) AS a_values, TO_JSON_STRING(STRUCT({b_cols})) AS b_values
    FROM a FULL OUTER JOIN b ON a.k = b.k
  )"""
    summary = f"""INSERT INTO `{summary_table}` (request_id, category, a_file_id, b_file_id, row_count, created_at)
{cte}
SELECT @request_id, category, IF(category = 'MATCHED', NULL, a_file), IF(category = 'MATCHED', NULL, b_file),
       COUNT(*), CURRENT_TIMESTAMP()
FROM j GROUP BY category, 3, 4"""
    sample = f"""INSERT INTO `{sample_table}` (request_id, category, record_key, a_file_id, b_file_id, a_values, b_values, created_at)
{cte}
SELECT @request_id, category, k, a_file, b_file, a_values, b_values, CURRENT_TIMESTAMP()
FROM j WHERE category != 'MATCHED'
QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY k) <= @sample_rows"""
    return summary, sample, params


def window_end(start: datetime, grain: str) -> datetime:
    return start + GRAINS[grain]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
