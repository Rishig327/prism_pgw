"""
recon_runner.py - PRISM reconciliation: aggregate recon -> alarms -> manual detailed recon.
Runs on the VM (orchestrator side), schedule it like the ingestion jobs.

    python recon_runner.py run  [RECON_ID ...]            # all enabled definitions if none given
    python recon_runner.py run  RECON_ID --dry-run        # validate SQL on BigQuery + show bytes, writes nothing
    python recon_runner.py alarms [--state OPEN --state PENDING]
    python recon_runner.py ack      ALARM_ID --by NAME [--note TEXT]
    python recon_runner.py suppress ALARM_ID --by NAME [--note TEXT]
    python recon_runner.py detail   ALARM_ID --by NAME [--dim APN=internet] [--force] [--dry-run]

Definitions: stored in VPDB_ADM.prism_recon_definition (config_admin.py recon-put / recon-show; template: recon_definitions.example.json).
For a quick local test add  --defs-file path.json  before the command. Alarms are written to
VPDB_ADM.recon_alarm and logged as one ERROR line starting PRISM_RECON_ALARM, which a Cloud Logging
log-based alert can turn into email / Teams / paging.
"""
import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery

import config_store as cs

import recon_logic as rl
from config import CTL_DATASET, KMS_KEY, LOCATION, PROJECT

log = logging.getLogger("prism.recon")
S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
ALARM_COLS = ["alarm_id", "recon_id", "window_start", "state", "severity", "breach_runs", "first_raised_at",
              "last_seen_at", "resolved_at", "acknowledged_by", "acknowledged_at", "note", "summary", "notified_at"]
ALARM_TYPES = {"breach_runs": "INT64", "window_start": "TIMESTAMP", "first_raised_at": "TIMESTAMP",
               "last_seen_at": "TIMESTAMP", "resolved_at": "TIMESTAMP", "acknowledged_at": "TIMESTAMP",
               "notified_at": "TIMESTAMP"}


class Recon:
    def __init__(self, project=PROJECT, location=LOCATION, ctl=CTL_DATASET, kms_key=KMS_KEY, notify_fn=None):
        self.enc = bigquery.EncryptionConfiguration(kms_key_name=kms_key) if kms_key else None
        self.bq = bigquery.Client(project=project, location=location)
        t = lambda n: f"{project}.{ctl}.{n}"          # noqa: E731
        self.result_t, self.alarm_t, self.req_t = t("recon_result"), t("recon_alarm"), t("recon_detail_request")
        self.sum_t, self.sample_t, self.reg_t = t("recon_detail_summary"), t("recon_detail_sample"), t("file_registry")
        self.notify_fn = notify_fn or self._log_alarm

    # ------------------------------------------------------------ BigQuery helpers (CMEK on every job)
    def _cfg(self, params, dry_run=False, max_bytes=None):
        cfg = bigquery.QueryJobConfig(query_parameters=params or [])
        if self.enc is not None:
            cfg.destination_encryption_configuration = self.enc
        if dry_run:
            cfg.dry_run, cfg.use_query_cache = True, False
        if max_bytes:
            cfg.maximum_bytes_billed = int(max_bytes)
        return cfg

    def _run(self, sql, params=None, max_bytes=None):
        job = self.bq.query(sql, job_config=self._cfg(params, max_bytes=max_bytes))
        job.result()
        return job

    def _rows(self, sql, params=None):
        return list(self._run(sql, params).result())

    def _dry(self, sql, params=None) -> int:
        return self.bq.query(sql, job_config=self._cfg(params, dry_run=True)).total_bytes_processed or 0

    # ------------------------------------------------------------ aggregate recon
    def _recent_pending_files(self, d) -> int:
        """Files still moving through ingestion (recently discovered, not yet COMPLETED). While they
        exist, differences may just be data in flight, so results are provisional and no alarm is raised.
        Files stuck for longer than the grace period do NOT gate - that is a real problem to alarm on."""
        types = sorted(set(d["side_a"]["source_types"]) | set(d["side_b"]["source_types"]))
        rows = self._rows(
            f"SELECT COUNT(*) AS c FROM `{self.reg_t}` WHERE source_type IN UNNEST(@t) "
            "AND status IN ('DISCOVERED','IN_PROGRESS','FAILED') "
            "AND discovered_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @g MINUTE)",
            [A("t", "STRING", types), S("g", "INT64", int(d["pending_grace_minutes"]))])
        return rows[0].c if rows else 0

    def run_aggregate(self, d: dict, now=None, dry_run=False) -> dict:
        now = now or rl.utcnow()
        frm, to = rl.window_range(d, now)
        if to <= frm:
            return {"recon_id": d["recon_id"], "skipped": "no settled windows yet"}
        pending = self._recent_pending_files(d) if d["gate_on_pending_files"] else 0
        provisional = pending > 0
        run_id = f"RECON-{d['recon_id']}-{now:%Y%m%d%H%M%S}"
        params = [S("recon_id", "STRING", d["recon_id"]), S("from_ts", "TIMESTAMP", frm),
                  S("to_ts", "TIMESTAMP", to), S("provisional", "BOOL", provisional), S("run_id", "STRING", run_id)]
        sql = rl.build_aggregate_sql(d, self.result_t)
        if dry_run:
            return {"recon_id": d["recon_id"], "dry_run": True, "bytes": self._dry(sql, params), "sql": sql}
        self._run(sql, params)
        out = {"recon_id": d["recon_id"], "run_id": run_id, "from": frm, "to": to,
               "pending_files": pending, "provisional": provisional}
        out.update(self._evaluate(d, frm, to, provisional, now))
        return out

    def _existing_alarms(self, recon_id, frm, to) -> dict:
        rows = self._rows(
            f"SELECT * FROM `{self.alarm_t}` WHERE recon_id=@r AND window_start >= @f AND window_start < @t "
            "QUALIFY ROW_NUMBER() OVER (PARTITION BY window_start ORDER BY first_raised_at DESC) = 1",
            [S("r", "STRING", recon_id), S("f", "TIMESTAMP", frm), S("t", "TIMESTAMP", to)])
        return {r.window_start: dict(r.items()) for r in rows}

    def _evaluate(self, d, frm, to, provisional, now) -> dict:
        p = [S("recon_id", "STRING", d["recon_id"]), S("from_ts", "TIMESTAMP", frm), S("to_ts", "TIMESTAMP", to)]
        breached_rows = self._rows(rl.build_breach_sql(self.result_t), p)
        evaluated = {r.window_start for r in self._rows(rl.build_evaluated_sql(self.result_t), p)}
        breached = {}
        for r in breached_rows:
            breached[r.window_start] = {
                "severity": rl.severity_of(d, r.missing_rows, r.max_pct),
                "summary": json.dumps({"breach_rows": r.breach_rows, "missing_rows": r.missing_rows,
                                       "max_pct": r.max_pct, "worst": json.loads(r.worst)})}
        existing = self._existing_alarms(d["recon_id"], frm, to)
        upserts, notify = rl.evaluate_alarms(existing, breached, evaluated, d["recon_id"],
                                             d["required_runs"], provisional, now)
        if upserts:
            self._upsert_alarms(upserts)
        for a in notify:
            self.notify_fn(a)
        return {"windows_evaluated": len(evaluated), "windows_breached": len(breached),
                "alarms_written": len(upserts), "alarms_notified": len(notify)}

    def _upsert_alarms(self, rows):
        arr = [bigquery.StructQueryParameter(None, *[
            S(c, ALARM_TYPES.get(c, "STRING"), r[c]) for c in ALARM_COLS]) for r in rows]
        sets = ", ".join(f"{c}=S.{c}" for c in ALARM_COLS if c not in ("alarm_id", "recon_id", "window_start", "first_raised_at"))
        self._run(f"""MERGE `{self.alarm_t}` T USING UNNEST(@rows) S ON T.alarm_id = S.alarm_id
            WHEN MATCHED THEN UPDATE SET {sets}
            WHEN NOT MATCHED THEN INSERT ({', '.join(ALARM_COLS)})
            VALUES ({', '.join('S.' + c for c in ALARM_COLS)})""", [A("rows", "STRUCT", arr)])

    @staticmethod
    def _log_alarm(a: dict):
        log.error("PRISM_RECON_ALARM %s", json.dumps({
            "alarm_id": a["alarm_id"], "recon_id": a["recon_id"], "window_start": str(a["window_start"]),
            "state": a["state"], "severity": a["severity"], "breach_runs": a["breach_runs"],
            "summary": json.loads(a["summary"]) if a.get("summary") else None}, default=str))

    # ------------------------------------------------------------ alarm handling
    def list_alarms(self, states):
        return self._rows(
            f"SELECT alarm_id, recon_id, window_start, state, severity, breach_runs, last_seen_at, acknowledged_by "
            f"FROM `{self.alarm_t}` WHERE state IN UNNEST(@s) ORDER BY window_start DESC LIMIT 200",
            [A("s", "STRING", states)])

    def _set_state(self, alarm_id, state, by, note) -> bool:
        job = self._run(
            f"UPDATE `{self.alarm_t}` SET state=@st, acknowledged_by=@by, acknowledged_at=CURRENT_TIMESTAMP(), "
            "note=@n, last_seen_at=CURRENT_TIMESTAMP() WHERE alarm_id=@id AND state IN ('PENDING','OPEN','ACKNOWLEDGED')",
            [S("st", "STRING", state), S("by", "STRING", by), S("n", "STRING", note), S("id", "STRING", alarm_id)])
        return job.num_dml_affected_rows == 1

    def ack(self, alarm_id, by, note=None):
        return self._set_state(alarm_id, "ACKNOWLEDGED", by, note)

    def suppress(self, alarm_id, by, note=None):
        return self._set_state(alarm_id, "SUPPRESSED", by, note)

    # ------------------------------------------------------------ manual detailed recon
    def run_detail(self, defs, alarm_id, requested_by, dims=None, force=False, dry_run=False) -> dict:
        rows = self._rows(f"SELECT recon_id, window_start FROM `{self.alarm_t}` WHERE alarm_id=@id", [S("id", "STRING", alarm_id)])
        if not rows:
            raise ValueError(f"alarm {alarm_id} not found")
        d = defs[rows[0].recon_id]
        if not d.get("detail"):
            raise ValueError(f"recon {d['recon_id']} has no 'detail' section in its definition")
        w_from = rows[0].window_start
        w_to = rl.window_end(w_from, d["grain"])
        summary_sql, sample_sql, dim_params = rl.build_detail_sql(d, dims, self.sum_t, self.sample_t)
        request_id = f"REQ-{alarm_id}-{uuid.uuid4().hex[:4]}"
        params = [S("request_id", "STRING", request_id), S("w_from", "TIMESTAMP", w_from),
                  S("w_to", "TIMESTAMP", w_to), S("sample_rows", "INT64", int(d["detail"]["sample_rows"]))]
        params += [S(k, "STRING", v) for k, v in dim_params.items()]

        est = self._dry(summary_sql, params) + self._dry(sample_sql, params)
        limit = int(d["detail"]["max_bytes"])
        out = {"request_id": request_id, "alarm_id": alarm_id, "window_start": w_from,
               "bytes_estimated": est, "max_bytes": limit}
        if dry_run:
            return {**out, "dry_run": True}
        self._run(f"INSERT INTO `{self.req_t}` (request_id, alarm_id, recon_id, requested_by, requested_at, status, dims_filter, bytes_estimated) "
                  "VALUES (@r, @a, @c, @by, CURRENT_TIMESTAMP(), @st, @df, @b)",
                  [S("r", "STRING", request_id), S("a", "STRING", alarm_id), S("c", "STRING", d["recon_id"]),
                   S("by", "STRING", requested_by), S("st", "STRING", "RUNNING" if (est <= limit or force) else "REFUSED"),
                   S("df", "STRING", json.dumps(dims or {})), S("b", "INT64", est)])
        if est > limit and not force:
            out["status"] = "REFUSED"
            out["reason"] = f"estimated {est} bytes exceeds the limit {limit}; narrow with --dim or use --force"
            return out
        try:
            self._run(summary_sql, params, max_bytes=max(est * 2, limit))
            self._run(sample_sql, params, max_bytes=max(est * 2, limit))
            self._context(d, request_id, w_from, w_to)
            self._finish(request_id, "DONE", None)
        except Exception as e:
            self._finish(request_id, "FAILED", str(e))
            raise
        out["status"] = "DONE"
        out["summary"] = [dict(r.items()) for r in self._rows(
            f"SELECT category, SUM(row_count) AS row_count FROM `{self.sum_t}` WHERE request_id=@r GROUP BY category ORDER BY row_count DESC",
            [S("r", "STRING", request_id)])]
        return out

    def _context(self, d, request_id, w_from, w_to):
        """File health around the window, so a difference can be tied to failed/quarantined/pending files."""
        for label, side in (("A", d["side_a"]), ("B", d["side_b"])):
            self._run(
                f"INSERT INTO `{self.sum_t}` (request_id, category, a_file_id, b_file_id, row_count, created_at) "
                f"SELECT @r, CONCAT('CONTEXT_{label}:', source_type, ':', status), NULL, NULL, COUNT(*), CURRENT_TIMESTAMP() "
                f"FROM `{self.reg_t}` WHERE source_type IN UNNEST(@t) "
                "AND status IN ('FAILED','QUARANTINED','DISCOVERED','IN_PROGRESS','DUPLICATE') "
                "AND discovered_at >= TIMESTAMP_SUB(@f, INTERVAL 1 DAY) AND discovered_at < TIMESTAMP_ADD(@e, INTERVAL 2 DAY) "
                "GROUP BY source_type, status",
                [S("r", "STRING", request_id), A("t", "STRING", side["source_types"]),
                 S("f", "TIMESTAMP", w_from), S("e", "TIMESTAMP", w_to)])

    def _finish(self, request_id, status, error):
        self._run(f"UPDATE `{self.req_t}` SET status=@s, finished_at=CURRENT_TIMESTAMP(), error=@e WHERE request_id=@r",
                  [S("s", "STRING", status), S("e", "STRING", (error or "")[:2000] or None), S("r", "STRING", request_id)])


# ---------------------------------------------------------------------- CLI
def main(argv=None):
    ap = argparse.ArgumentParser(description="PRISM reconciliation")
    ap.add_argument("--defs-file", help="use this local JSON instead of the table (testing)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run"); r.add_argument("recon_ids", nargs="*"); r.add_argument("--dry-run", action="store_true")
    a = sub.add_parser("alarms"); a.add_argument("--state", action="append")
    for name in ("ack", "suppress"):
        x = sub.add_parser(name); x.add_argument("alarm_id"); x.add_argument("--by", required=True); x.add_argument("--note")
    dt = sub.add_parser("detail"); dt.add_argument("alarm_id"); dt.add_argument("--by", required=True)
    dt.add_argument("--dim", action="append", default=[], help="NAME=VALUE slice of the alarm window")
    dt.add_argument("--force", action="store_true"); dt.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    rc = Recon()
    if args.cmd == "alarms":
        for row in rc.list_alarms(args.state or ["PENDING", "OPEN", "ACKNOWLEDGED"]):
            print(dict(row.items()))
        return 0
    if args.cmd in ("ack", "suppress"):
        ok = getattr(rc, args.cmd)(args.alarm_id, args.by, args.note)
        print("done" if ok else "no active alarm with that id")
        return 0 if ok else 1

    if args.defs_file:
        defs = rl.load_definitions(args.defs_file)
    else:
        store = cs.ConfigStore(cs.BqHelper(PROJECT, LOCATION, KMS_KEY), PROJECT, CTL_DATASET)
        defs = rl.parse_definitions(list(store.latest_recons().values()))
        if not defs:
            print("no recon definitions in the table; add one with: python config_admin.py recon-put ID file.json --by NAME")
            return 1
    if args.cmd == "run":
        ids = args.recon_ids or [k for k, v in defs.items() if v["enabled"]]
        failed = 0
        for rid in ids:
            try:
                res = rc.run_aggregate(defs[rid], dry_run=args.dry_run)
                print(json.dumps({k: v for k, v in res.items() if k != "sql"}, default=str))
                if args.dry_run:
                    print(res["sql"])
            except Exception:
                failed += 1
                log.exception("recon %s failed", rid)
        return 1 if failed else 0
    if args.cmd == "detail":
        dims = dict(x.split("=", 1) for x in args.dim) or None
        print(json.dumps(rc.run_detail(defs, args.alarm_id, args.by, dims, args.force, args.dry_run),
                         indent=2, default=str))
        return 0


if __name__ == "__main__":
    sys.exit(main())
