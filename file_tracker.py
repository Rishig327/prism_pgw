"""
file_tracker.py - PRISM file-level idempotency / partial-load recovery.

Flow:
  discover (list input/, fetch checksums, register, move known files)
  -> get_eligible -> claim_batch -> (Dataflow writes staging) -> collect_counts
  -> commit per file (staging -> detail in one transaction) -> move_file
  on error: mark_failed (+ cleanup_staging). Retried until max_attempts, then quarantined.

Requires: google-cloud-bigquery, google-cloud-storage
"""
import fnmatch
from concurrent.futures import ThreadPoolExecutor
import hashlib
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional

from google.cloud import bigquery, storage

log = logging.getLogger("prism.file_tracker")

CHECKSUM_WORKERS = 200      # parallel metadata fetches (see _fill_checksums)


# ---------------------------------------------------------------- config
@dataclass
class SourceConfig:
    source_type: str                      # e.g. "MSC"
    input_prefix: str                     # e.g. "input/msc/"
    processed_prefix: str                 # e.g. "processed/msc/"
    duplicate_prefix: str                 # e.g. "duplicate/msc/"
    quarantine_prefix: str                # e.g. "quarantine/msc/"
    ignore_suffixes: tuple = (".tmp", ".part", ".filepart", ".crdownload")
    min_age_seconds: int = 60             # skip files still being uploaded
    name_glob: str = "*"                  # only files whose NAME matches, e.g. "pgw*"
    # same name + different content as an already COMPLETED file:
    #   "quarantine" (default, manual decision) or "supersede" (replace old rows)
    conflict_policy: str = "quarantine"
    # Registry lookups at discovery (known file, same content, same name, replaced path, checksum reuse)
    # only look at rows discovered within the last N days. A file that comes back after that is simply
    # loaded again - no other check is made. Keep it longer than any file can wait in the input folder.
    lookup_window_days: int = 30


@dataclass
class TargetConfig:
    detail_table: str                     # "project.dataset.detail_pgw"
    staging_table: str                    # "project.dataset.stg_detail_pgw"
    # The aggregate is NOT built here: build it later from detail (ddl.sql section 4).
    # Kept only so existing configs that pass agg_table=None keep working; any value raises in commit().
    agg_table: Optional[str] = None


@dataclass
class FileRecord:
    file_id: str
    source_type: str
    bucket: str
    object_name: str
    base_name: str
    crc32c: str
    size_bytes: int
    status: str
    attempt_count: int


@dataclass
class CommitItem:
    """One file that passed all checks and is ready to be committed."""
    rec: FileRecord
    attempt_no: int
    records_read: int
    records_loaded: int
    records_rejected: int


# ---------------------------------------------------------------- tracker
class FileTracker:
    def __init__(self, project: str, bucket: str, ctl_dataset: str = "VPDB_ADM",
                 location: Optional[str] = None, kms_key: Optional[str] = None,
                 alert_fn: Optional[Callable[[str, str], None]] = None):
        self.project = project
        self.bucket_name = bucket
        # CMEK org policy: every query job must carry an encryption key (added in _run).
        self._enc = (bigquery.EncryptionConfiguration(kms_key_name=kms_key) if kms_key else None)
        self.bq = bigquery.Client(project=project, location=location)
        self.gcs = storage.Client(project=project)
        # default requests pool = 10 connections; widen it to match CHECKSUM_WORKERS
        # (uses the client's internal session - private attribute, re-check after library upgrades)
        from requests.adapters import HTTPAdapter
        self.gcs._http.mount("https://", HTTPAdapter(pool_connections=CHECKSUM_WORKERS,
                                                     pool_maxsize=CHECKSUM_WORKERS))
        self.reg = f"`{project}.{ctl_dataset}.file_registry`"
        self.logt = f"`{project}.{ctl_dataset}.file_processing_log`"
        self.read_counts = f"`{project}.{ctl_dataset}.file_read_counts`"
        self.reject_log = f"`{project}.{ctl_dataset}.file_reject_log`"
        self.runs = f"`{project}.{ctl_dataset}.prism_runs`"
        self.alert_fn = alert_fn or (lambda subject, body: log.warning("ALERT %s: %s", subject, body))

    # ------------------------------------------------------------ helpers
    @staticmethod
    def make_file_id(source_type: str, base_name: str, crc32c: str) -> str:
        return hashlib.sha256(f"{source_type}|{base_name}|{crc32c}".encode()).hexdigest()

    def _run(self, sql: str, params: Optional[list] = None, session_id: Optional[str] = None,
             create_session: bool = False):
        """Every BigQuery statement goes through here so the CMEK key is always attached."""
        cfg = bigquery.QueryJobConfig(query_parameters=params or [])
        if self._enc is not None:
            cfg.destination_encryption_configuration = self._enc
        if create_session:
            cfg.create_session = True
        elif session_id:
            cfg.connection_properties = [bigquery.ConnectionProperty("session_id", session_id)]
        job = self.bq.query(sql, job_config=cfg)
        job.result()
        return job

    def _rows(self, sql: str, params: Optional[list] = None):
        return list(self._run(sql, params).result())

    @staticmethod
    def _win(src: "SourceConfig") -> datetime:
        """Start of the registry lookup window of this source."""
        return datetime.now(timezone.utc) - timedelta(days=src.lookup_window_days)

    @staticmethod
    def _chunks(seq, n):
        for i in range(0, len(seq), n):
            yield seq[i:i + n]

    def _events_many(self, rows):
        """rows: (file_id, attempt_no, run_id, event, records_count, message). One INSERT per 1000."""
        try:
            S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
            for chunk in self._chunks(list(rows), 1000):
                arr = [bigquery.StructQueryParameter(
                    None, S("file_id", "STRING", r[0]), S("attempt_no", "INT64", r[1]),
                    S("run_id", "STRING", r[2]), S("event", "STRING", r[3]),
                    S("cnt", "INT64", r[4]), S("msg", "STRING", (r[5] or "")[:2000])) for r in chunk]
                self._run(
                    f"INSERT INTO {self.logt} (file_id, attempt_no, run_id, event, event_ts, records_count, message) "
                    "SELECT file_id, attempt_no, run_id, event, CURRENT_TIMESTAMP(), cnt, msg FROM UNNEST(@rows)",
                    [A("rows", "STRUCT", arr)])
        except Exception:                                   # logging must never break the flow
            log.exception("could not write processing log")

    def _event(self, file_id, attempt_no, run_id, event, count=None, message=None):
        self._events_many([(file_id, attempt_no, run_id, event, count, message)])

    # ------------------------------------------------------------ GCS moves
    def _move_blob(self, name: str, dest_prefix: str, base_name: str, file_id: str) -> Optional[str]:
        bucket = self.gcs.bucket(self.bucket_name)
        src = bucket.blob(name)
        if not src.exists():
            return None                                    # already moved
        dest = f"{dest_prefix}{datetime.now(timezone.utc):%Y/%m/%d}/{base_name}"
        if bucket.blob(dest).exists():
            dest = f"{dest}.{file_id[:8]}"
        bucket.rename_blob(src, dest)                      # copy + delete
        return dest

    def _record_moves(self, moves: List[tuple], source_type: str):
        for chunk in self._chunks(moves, 1000):
            arr = [bigquery.StructQueryParameter(
                None,
                bigquery.ScalarQueryParameter("file_id", "STRING", fid),
                bigquery.ScalarQueryParameter("path", "STRING", path)) for fid, path in chunk]
            self._run(
                f"UPDATE {self.reg} T SET current_path=S.path, moved_at=CURRENT_TIMESTAMP(), "
                "updated_at=CURRENT_TIMESTAMP() FROM UNNEST(@moves) S "
                "WHERE T.source_type=@src AND T.file_id=S.file_id",
                [bigquery.ArrayQueryParameter("moves", "STRUCT", arr),
                 bigquery.ScalarQueryParameter("src", "STRING", source_type)])

    def move_file(self, rec: FileRecord, dest_prefix: str):
        return self.move_files([rec], dest_prefix)

    def move_files(self, recs: List[FileRecord], dest_prefix: str) -> int:
        """Call AFTER commit_batch(). Moves in parallel, then records all paths with ONE update.
        A file whose move fails stays in input/; discovery moves it next run (it is COMPLETED)."""
        if not recs:
            return 0

        def one(rec):
            try:
                return rec.file_id, self._move_blob(rec.object_name, dest_prefix, rec.base_name, rec.file_id)
            except Exception:
                log.exception("move failed for %s", rec.object_name)
                return rec.file_id, None

        with ThreadPoolExecutor(min(32, len(recs))) as ex:
            results = list(ex.map(one, recs))
        moved = [(fid, dest) for fid, dest in results if dest]
        if moved:
            self._record_moves(moved, recs[0].source_type)
            self._events_many([(fid, None, None, "MOVED", None, dest) for fid, dest in moved])
        return len(moved)

    # ------------------------------------------------------------ discover
    def _is_candidate(self, b, src: SourceConfig) -> bool:
        if b.name.endswith("/") or not b.size:
            return False
        if b.name.lower().endswith(tuple(s.lower() for s in src.ignore_suffixes)):
            return False
        if not fnmatch.fnmatch(os.path.basename(b.name), src.name_glob):
            return False
        age = (datetime.now(timezone.utc) - b.updated).total_seconds()
        return age >= src.min_age_seconds

    def _cached_checksums(self, src: SourceConfig, blobs) -> Dict[tuple, str]:
        """Checksums already stored in the registry, keyed by (object_name, generation).
        Cloud Storage gives an overwritten object a NEW generation, so a match means the very same
        bytes and the stored checksum can be reused. Any miss (or a failed lookup) just means the
        checksum is fetched from Cloud Storage as before."""
        if not blobs:
            return {}
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        wanted = {(b.name, b.generation) for b in blobs}
        found: Dict[tuple, str] = {}
        try:
            for chunk in self._chunks([b.name for b in blobs], 5000):
                rows = self._rows(
                    f"SELECT object_name, generation, crc32c FROM {self.reg} "
"WHERE source_type=@s AND object_name IN UNNEST(@names) AND discovered_at >= @win",
                    [S("s", "STRING", src.source_type), A("names", "STRING", chunk),
                     S("win", "TIMESTAMP", self._win(src))])
                for r in rows:
                    if (r.object_name, r.generation) in wanted and r.crc32c:
                        found[(r.object_name, r.generation)] = r.crc32c
        except Exception:
            log.warning("checksum cache lookup failed; fetching all checksums from storage", exc_info=True)
            return {}
        return found

    def _fill_checksums(self, blobs):
        """The bucket listing returns no crc32c/md5 here, but a metadata GET (reload) does.
        Done in parallel and only for files that passed the name/size/age filters."""
        missing = [b for b in blobs if not b.crc32c]
        if missing:
            log.info("fetching checksums for %d file(s)", len(missing))
            with ThreadPoolExecutor(min(CHECKSUM_WORKERS, len(missing))) as ex:
                list(ex.map(lambda b: b.reload(), missing))

    def discover(self, src: SourceConfig) -> Dict[str, int]:
        """List input/ only, register unseen files, move already-known ones."""
        stats = {"new": 0, "duplicate": 0, "conflict": 0, "moved_completed": 0, "seen": 0}
        blobs = [b for b in self.gcs.list_blobs(self.bucket_name, prefix=src.input_prefix)
                 if self._is_candidate(b, src)]
        win = self._win(src)
        stale_inputs = [b.name for b in blobs if b.updated and b.updated < win]
        if stale_inputs:
            log.warning("%d input file(s) are older than the %d-day lookup window (for example %s); their "
                        "registry rows may be outside the window, so they can be registered again",
                        len(stale_inputs), src.lookup_window_days, stale_inputs[0])
        known = self._cached_checksums(src, blobs)
        todo = [b for b in blobs if (b.name, b.generation) not in known]
        log.info("checksums: %d reused from registry, %d to fetch", len(known), len(todo))
        self._fill_checksums(todo)           # listing omits crc32c in this bucket: fetch only what is new
        for batch in self._chunks(blobs, 2000):
            self._discover_batch(src, batch, stats, known)
        log.info("discover %s: %s", src.source_type, stats)
        return stats

    def _discover_batch(self, src: SourceConfig, blobs, stats, known_crc: Optional[Dict[tuple, str]] = None):
        items = []
        for b in blobs:
            base = os.path.basename(b.name)
            crc = (known_crc or {}).get((b.name, b.generation)) or b.crc32c or b.md5_hash
            if not crc:
                raise RuntimeError(f"no checksum available for gs://{b.bucket.name}/{b.name}")
            items.append({"blob": b, "base": base, "crc": crc, "size": b.size,
                          "file_id": self.make_file_id(src.source_type, base, crc)})
        S = bigquery.ScalarQueryParameter
        A = bigquery.ArrayQueryParameter
        win = self._win(src)

        known = {r.file_id: r.status for r in self._rows(
            f"SELECT file_id, status FROM {self.reg} WHERE source_type=@s AND file_id IN UNNEST(@ids) "
            "AND discovered_at >= @win",
            [S("s", "STRING", src.source_type), A("ids", "STRING", [i["file_id"] for i in items]),
             S("win", "TIMESTAMP", win)])}

        content = {(r.crc32c, r.size_bytes): r.file_id for r in self._rows(
            f"SELECT crc32c, size_bytes, file_id FROM {self.reg} "
            "WHERE source_type=@s AND crc32c IN UNNEST(@crcs) "
            "AND status NOT IN ('DUPLICATE','SUPERSEDED','QUARANTINED') AND discovered_at >= @win",
            [S("s", "STRING", src.source_type), A("crcs", "STRING", list({i["crc"] for i in items})),
             S("win", "TIMESTAMP", win)])}

        same_name = {r.base_name: r.file_id for r in self._rows(
            f"SELECT base_name, ANY_VALUE(file_id) AS file_id FROM {self.reg} "
            "WHERE source_type=@s AND status='COMPLETED' AND base_name IN UNNEST(@n) "
            "AND discovered_at >= @win GROUP BY base_name",
            [S("s", "STRING", src.source_type), A("n", "STRING", list({i["base"] for i in items})),
             S("win", "TIMESTAMP", win)])}

        new_rows, moves_dest = [], []          # moves_dest: (item, dest_prefix)
        seen_content = {}
        for it in items:
            stats["seen"] += 1
            fid, key = it["file_id"], (it["crc"], it["size"])
            if fid in known:
                st = known[fid]
                if st == "COMPLETED":
                    moves_dest.append((it, src.processed_prefix)); stats["moved_completed"] += 1
                elif st == "DUPLICATE":
                    moves_dest.append((it, src.duplicate_prefix))
                elif st == "QUARANTINED":
                    moves_dest.append((it, src.quarantine_prefix))
                continue                        # otherwise eligible for (re)processing

            status, err, supersedes = "DISCOVERED", None, None
            other = content.get(key) or seen_content.get(key)
            if other:
                status, err = "DUPLICATE", f"same content as {other}"
                stats["duplicate"] += 1
            elif it["base"] in same_name:       # same name, different content, already COMPLETED
                if src.conflict_policy == "supersede":
                    supersedes = same_name[it["base"]]
                else:
                    status, err = "QUARANTINED", f"same name as completed file {same_name[it['base']]} but different content"
                    stats["conflict"] += 1
                    self.alert_fn("PRISM file conflict", f"{it['blob'].name}: {err}")
            if status == "DISCOVERED":
                stats["new"] += 1
                seen_content[key] = fid
            new_rows.append({**it, "status": status, "err": err, "supersedes": supersedes})
            if status == "DUPLICATE":
                moves_dest.append((it, src.duplicate_prefix))
            elif status == "QUARANTINED":
                moves_dest.append((it, src.quarantine_prefix))

        if new_rows:
            self._insert_new(src, new_rows)
        self._supersede_replaced(src, items, stats)     # AFTER the insert: the object always has a live row
        moved = []
        for it, prefix in moves_dest:
            dest = self._move_blob(it["blob"].name, prefix, it["base"], it["file_id"])
            if dest:
                moved.append((it["file_id"], dest))
        if moved:
            self._record_moves(moved, src.source_type)

    def _supersede_replaced(self, src: SourceConfig, items: list, stats: dict):
        """A sender can overwrite the SAME path with a corrected file (new content, new checksum).
        Rows registered for that path with the OLD content that were never loaded (DISCOVERED, FAILED,
        or IN_PROGRESS with an expired lease) now point at the wrong bytes: retrying them would load the
        new content under the old file_id (duplicates) and quarantining them would move the new file.
        Nothing was loaded for them, so they are simply marked SUPERSEDED (no data to delete, no policy
        decision). Matched by FULL PATH, so the same file name in another folder is never touched.
        Rows with a live lease are left alone (a run is working on them)."""
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        stale_cond = "(status IN ('DISCOVERED','FAILED') OR (status='IN_PROGRESS' AND lease_expiry < CURRENT_TIMESTAMP()))"
        current = {i["blob"].name: i["file_id"] for i in items}
        ids = set()
        for chunk in self._chunks(list(current), 5000):
            for r in self._rows(
                    f"SELECT file_id, object_name FROM {self.reg} WHERE source_type=@s "
f"AND object_name IN UNNEST(@paths) AND discovered_at >= @win AND {stale_cond}",
                    [S("s", "STRING", src.source_type), A("paths", "STRING", chunk),
                     S("win", "TIMESTAMP", self._win(src))]):
                if r.file_id != current.get(r.object_name):
                    ids.add(r.file_id)
        if not ids:
            return
        ids = sorted(ids)
        msg = "object at this path was replaced by a file with different content"
        self._run(f"UPDATE {self.reg} SET status='SUPERSEDED', claimed_by=NULL, lease_expiry=NULL, "
                  "last_error=@m, updated_at=CURRENT_TIMESTAMP() "
                  f"WHERE source_type=@src AND file_id IN UNNEST(@ids) AND {stale_cond}",
                  [S("m", "STRING", msg), A("ids", "STRING", ids), S("src", "STRING", src.source_type)])
        stats["replaced"] = stats.get("replaced", 0) + len(ids)
        self._events_many([(i, None, None, "SUPERSEDED", None, msg) for i in ids])
        log.warning("%d never-loaded registry row(s) superseded because their path got a new file", len(ids))

    def _insert_new(self, src: SourceConfig, rows: List[dict]):
        S = bigquery.ScalarQueryParameter
        for chunk in self._chunks(rows, 1000):
            arr = []
            for r in chunk:
                b = r["blob"]
                arr.append(bigquery.StructQueryParameter(
                    None,
                    S("file_id", "STRING", r["file_id"]),
                    S("object_name", "STRING", b.name),
                    S("base_name", "STRING", r["base"]),
                    S("generation", "INT64", b.generation),
                    S("size_bytes", "INT64", r["size"]),
                    S("crc32c", "STRING", r["crc"]),
                    S("gcs_updated_at", "TIMESTAMP", b.updated),
                    S("status", "STRING", r["status"]),
                    S("last_error", "STRING", r["err"]),
                    S("supersedes", "STRING", r["supersedes"])))
            self._run(f"""
                MERGE {self.reg} T
                USING UNNEST(@rows) S ON T.source_type = @src AND T.discovered_at >= @win AND T.file_id = S.file_id
                WHEN NOT MATCHED THEN INSERT
                  (file_id, source_type, bucket, object_name, base_name, current_path, generation,
                   size_bytes, crc32c, gcs_updated_at, status, attempt_count, last_error,
                   supersedes_file_id, discovered_at, updated_at)
                VALUES
                  (S.file_id, @src, @bucket, S.object_name, S.base_name, S.object_name, S.generation,
                   S.size_bytes, S.crc32c, S.gcs_updated_at, S.status, 0, S.last_error,
                   S.supersedes, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())""",
                [bigquery.ArrayQueryParameter("rows", "STRUCT", arr),
                 S("src", "STRING", src.source_type), S("bucket", "STRING", self.bucket_name),
                 S("win", "TIMESTAMP", self._win(src))])

    # ------------------------------------------------------------ eligibility / claim
    def get_eligible(self, source_type: str, max_attempts: int = 3, limit: int = 500) -> List[FileRecord]:
        sql = f"""
            SELECT file_id, source_type, bucket, object_name, base_name, crc32c,
                   size_bytes, status, attempt_count
            FROM {self.reg}
            WHERE source_type=@s AND attempt_count < @max
              AND (status IN ('DISCOVERED','FAILED')
                   OR (status='IN_PROGRESS' AND lease_expiry < CURRENT_TIMESTAMP()))
            ORDER BY gcs_updated_at, object_name
            LIMIT @lim"""
        rows = self._rows(sql, [bigquery.ScalarQueryParameter("s", "STRING", source_type),
                                bigquery.ScalarQueryParameter("max", "INT64", max_attempts),
                                bigquery.ScalarQueryParameter("lim", "INT64", limit)])
        return [FileRecord(**dict(r.items())) for r in rows]

    # ------------------------------------------------------------ batch mode (Dataflow)
    def claim_batch(self, recs: List[FileRecord], run_id: str, max_attempts: int = 3,
                    lease_minutes: int = 180) -> Dict[str, int]:
        """Claim many files in ONE statement. Returns {file_id: attempt_no} actually claimed."""
        ids = [r.file_id for r in recs]
        if not ids:
            return {}
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        src_type = recs[0].source_type
        self._run(f"""
            UPDATE {self.reg}
            SET status='IN_PROGRESS', attempt_count=attempt_count+1, claimed_by=@run,
                lease_expiry=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @lease MINUTE),
                started_at=CURRENT_TIMESTAMP(), updated_at=CURRENT_TIMESTAMP()
            WHERE source_type=@src AND file_id IN UNNEST(@ids) AND attempt_count < @max
              AND (status IN ('DISCOVERED','FAILED')
                   OR (status='IN_PROGRESS' AND lease_expiry < CURRENT_TIMESTAMP()))""",
            [S("run", "STRING", run_id), S("lease", "INT64", lease_minutes),
             S("max", "INT64", max_attempts), A("ids", "STRING", ids), S("src", "STRING", src_type)])
        rows = self._rows(f"SELECT file_id, attempt_count FROM {self.reg} "
                          "WHERE source_type=@src AND claimed_by=@run AND status='IN_PROGRESS'",
                          [S("run", "STRING", run_id), S("src", "STRING", src_type)])
        return {r.file_id: r.attempt_count for r in rows}

    def heartbeat_batch(self, run_id: str, lease_minutes: int, source_type: str) -> int:
        job = self._run(
            f"UPDATE {self.reg} SET lease_expiry=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @l MINUTE) "
            "WHERE source_type=@src AND claimed_by=@r AND status='IN_PROGRESS'",
            [bigquery.ScalarQueryParameter("l", "INT64", lease_minutes),
             bigquery.ScalarQueryParameter("r", "STRING", run_id),
             bigquery.ScalarQueryParameter("src", "STRING", source_type)])
        return job.num_dml_affected_rows

    def collect_counts(self, t: TargetConfig, claimed: Dict[str, int]) -> Dict[str, tuple]:
        """After the Dataflow job: per file (records_read, records_loaded, records_rejected).
        read     <- file_read_counts  (Dataflow writes one row per file/attempt)
        loaded   <- COUNT(*) in staging
        rejected <- file_reject_log"""
        pairs = [bigquery.StructQueryParameter(
            None, bigquery.ScalarQueryParameter("file_id", "STRING", f),
            bigquery.ScalarQueryParameter("attempt_no", "INT64", a)) for f, a in claimed.items()]
        rows = self._rows(f"""
            WITH ids AS (SELECT file_id, attempt_no FROM UNNEST(@pairs)),
            rd AS (SELECT file_id, attempt_no, SUM(records_read) c FROM {self.read_counts}
                   WHERE file_id IN (SELECT file_id FROM ids) GROUP BY 1, 2),
            st AS (SELECT file_id, attempt_no, COUNT(*) c FROM `{t.staging_table}`
                   WHERE file_id IN (SELECT file_id FROM ids) GROUP BY 1, 2),
            rj AS (SELECT file_id, attempt_no, COUNT(*) c FROM {self.reject_log}
                   WHERE file_id IN (SELECT file_id FROM ids) GROUP BY 1, 2)
            SELECT i.file_id, IFNULL(rd.c,0) AS r, IFNULL(st.c,0) AS l, IFNULL(rj.c,0) AS j
            FROM ids i
            LEFT JOIN rd USING (file_id, attempt_no)
            LEFT JOIN st USING (file_id, attempt_no)
            LEFT JOIN rj USING (file_id, attempt_no)""",
            [bigquery.ArrayQueryParameter("pairs", "STRUCT", pairs)])
        return {r.file_id: (r.r, r.l, r.j) for r in rows}

    def file_errors(self, claimed: Dict[str, int]) -> Dict[str, str]:
        """Whole-file decode failures written by Dataflow (record_offset = -1) for THIS attempt."""
        rows = self._rows(
            f"SELECT file_id, attempt_no, ANY_VALUE(reason) AS reason FROM {self.reject_log} "
            "WHERE file_id IN UNNEST(@ids) AND record_offset = -1 GROUP BY file_id, attempt_no",
            [bigquery.ArrayQueryParameter("ids", "STRING", list(claimed))])
        return {r.file_id: r.reason for r in rows if claimed.get(r.file_id) == r.attempt_no}


    # ------------------------------------------------------------ runs (flex / Cloud Run mode)
    LIVE_RUN_STATUSES = ("STARTING", "CLAIMED", "LAUNCHED", "FINALISING", "ADOPTED")
    RUN_LOOKBACK_DAYS = 7           # runs older than this are ignored (also prunes partitions)

    def _since(self) -> datetime:
        return datetime.now(timezone.utc) - timedelta(days=self.RUN_LOOKBACK_DAYS)

    def run_register(self, run_id: str, source_type: str, status: str = "STARTING",
                     config_version: Optional[str] = None):
        S = bigquery.ScalarQueryParameter
        self._run(f"INSERT INTO {self.runs} (run_id, source_type, status, started_at, heartbeat_at, config_version) "
                  "VALUES (@r, @s, @st, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @cv)",
                  [S("r", "STRING", run_id), S("s", "STRING", source_type), S("st", "STRING", status),
                   S("cv", "STRING", config_version)])

    def run_update(self, run_id: str, source_type: str, status: Optional[str] = None,
                   job_id: Optional[str] = None, manifest_uri: Optional[str] = None,
                   note: Optional[str] = None, adopted_by: Optional[str] = None, finished: bool = False):
        """Updates only the given fields; always refreshes heartbeat_at (a run that stops calling this
        is treated as dead after orphan_after_min)."""
        S = bigquery.ScalarQueryParameter
        sets, params = ["heartbeat_at=CURRENT_TIMESTAMP()"], [S("r", "STRING", run_id), S("s", "STRING", source_type),
                                                              S("since", "TIMESTAMP", self._since())]
        for col, val in (("status", status), ("job_id", job_id), ("manifest_uri", manifest_uri),
                         ("note", note), ("adopted_by", adopted_by)):
            if val is not None:
                sets.append(f"{col}=@{col}")
                params.append(S(col, "STRING", val))
        if finished:
            sets.append("finished_at=CURRENT_TIMESTAMP()")
        self._run(f"UPDATE {self.runs} SET {', '.join(sets)} "
                  "WHERE source_type=@s AND run_id=@r AND started_at >= @since", params)

    def run_heartbeat(self, run_id: str, source_type: str):
        self.run_update(run_id, source_type)

    def live_runs_before(self, source_type: str, run_id: str, orphan_after_min: int) -> List[str]:
        """Other runs of this source that are alive (recent heartbeat) and STARTED EARLIER than this run.
        If two runs start together, the later one sees the earlier one and stands down; the earlier one
        sees nobody and proceeds, so exactly one wins."""
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        mine = self._rows(f"SELECT started_at FROM {self.runs} WHERE source_type=@s AND run_id=@r AND started_at >= @since",
                          [S("s", "STRING", source_type), S("r", "STRING", run_id), S("since", "TIMESTAMP", self._since())])
        if not mine:
            return []
        rows = self._rows(
            f"SELECT run_id, started_at FROM {self.runs} WHERE source_type=@s AND started_at >= @since "
            "AND run_id != @r AND status IN UNNEST(@live) "
            "AND heartbeat_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE)",
            [S("s", "STRING", source_type), S("since", "TIMESTAMP", self._since()), S("r", "STRING", run_id),
             A("live", "STRING", list(self.LIVE_RUN_STATUSES)), S("m", "INT64", orphan_after_min)])
        me = (mine[0].started_at, run_id)
        return [r.run_id for r in rows if (r.started_at, r.run_id) < me]

    def find_orphan_runs(self, source_type: str, run_id: str, orphan_after_min: int) -> list:
        """Runs that stopped without finishing: a live status but no heartbeat for orphan_after_min,
        or one that marked itself ORPHANED on shutdown."""
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        return self._rows(
            f"SELECT run_id, status, job_id, manifest_uri FROM {self.runs} WHERE source_type=@s "
            "AND started_at >= @since AND run_id != @r AND (status = 'ORPHANED' OR (status IN UNNEST(@live) "
            "AND heartbeat_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @m MINUTE))) ORDER BY started_at",
            [S("s", "STRING", source_type), S("since", "TIMESTAMP", self._since()), S("r", "STRING", run_id),
             A("live", "STRING", list(self.LIVE_RUN_STATUSES)), S("m", "INT64", orphan_after_min)])

    def claim_orphan(self, orphan_id: str, source_type: str, expected_status: str, my_run_id: str) -> bool:
        """Atomic: only ONE run can adopt an orphan (the update only matches while its status is unchanged)."""
        S = bigquery.ScalarQueryParameter
        job = self._run(
            f"UPDATE {self.runs} SET status='ADOPTED', adopted_by=@me, heartbeat_at=CURRENT_TIMESTAMP() "
            "WHERE source_type=@s AND run_id=@o AND status=@exp AND started_at >= @since",
            [S("me", "STRING", my_run_id), S("s", "STRING", source_type), S("o", "STRING", orphan_id),
             S("exp", "STRING", expected_status), S("since", "TIMESTAMP", self._since())])
        return job.num_dml_affected_rows == 1

    def claimed_by_run(self, source_type: str, run_id: str):
        """Files still IN_PROGRESS under a run id -> ([FileRecord], {file_id: attempt_no})."""
        S = bigquery.ScalarQueryParameter
        rows = self._rows(
            f"SELECT file_id, source_type, bucket, object_name, base_name, crc32c, size_bytes, status, attempt_count "
            f"FROM {self.reg} WHERE source_type=@s AND claimed_by=@r AND status='IN_PROGRESS'",
            [S("s", "STRING", source_type), S("r", "STRING", run_id)])
        recs = [FileRecord(**dict(r.items())) for r in rows]
        return recs, {r.file_id: r.attempt_count for r in recs}

    def release_claims(self, source_type: str, run_id: str) -> int:
        """Give files back (no Dataflow job was launched for them): eligible again, attempt not consumed."""
        S = bigquery.ScalarQueryParameter
        job = self._run(
            f"UPDATE {self.reg} SET status='DISCOVERED', attempt_count=GREATEST(attempt_count-1, 0), "
            "claimed_by=NULL, lease_expiry=NULL, updated_at=CURRENT_TIMESTAMP() "
            "WHERE source_type=@s AND claimed_by=@r AND status='IN_PROGRESS'",
            [S("s", "STRING", source_type), S("r", "STRING", run_id)])
        return job.num_dml_affected_rows

    # ------------------------------------------------------------ commit
    def commit_batch(self, t: TargetConfig, run_id: str, items: List[CommitItem]) -> List[str]:
        """Commit ALL given files in ONE transaction on the detail table, then update the registry.

        Transaction (only this source's detail table is touched, so other sources are not blocked):
            DELETE detail rows of these files (and of files they supersede)
            INSERT detail FROM staging (rows of these file_id + attempt_no)
            row count must equal sum(records_loaded) or everything rolls back
        After COMMIT (separate short statements, shared registry is NOT locked during the load):
            registry -> COMPLETED for all files, SUPERSEDED for replaced files, log, staging cleanup
        If the VM dies between COMMIT and the registry update the files stay IN_PROGRESS and are
        retried; the delete-then-insert reload makes that safe.

        Why a session (not a script): with the CMEK org policy every query job needs the key, and
        child jobs of a script do not inherit it. In a session each statement is its own job.
        Returns the file_ids that were committed.
        """
        if t.agg_table:
            raise NotImplementedError("aggregate is built later from detail (ddl.sql section 4)")
        if not items:
            return []
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        det, stg = f"`{t.detail_table}`", f"`{t.staging_table}`"
        src_type = items[0].rec.source_type

        # 1. lease check: only commit files this run still owns
        ids = [i.rec.file_id for i in items]
        owned = {r.file_id for r in self._rows(
            f"SELECT file_id FROM {self.reg} WHERE source_type=@src AND file_id IN UNNEST(@ids) "
            "AND claimed_by=@run AND status='IN_PROGRESS'",
            [A("ids", "STRING", ids), S("run", "STRING", run_id), S("src", "STRING", src_type)])}
        lost = [i.rec.object_name for i in items if i.rec.file_id not in owned]
        if lost:
            log.warning("lease lost for %d file(s), not committed: %s", len(lost), lost[:5])
        items = [i for i in items if i.rec.file_id in owned]
        if not items:
            return []
        ids = [i.rec.file_id for i in items]
        expected = sum(i.records_loaded for i in items)

        old_ids = [r.supersedes_file_id for r in self._rows(
            f"SELECT supersedes_file_id FROM {self.reg} WHERE source_type=@src AND file_id IN UNNEST(@ids) "
            "AND supersedes_file_id IS NOT NULL", [A("ids", "STRING", ids), S("src", "STRING", src_type)])]
        pairs = [bigquery.StructQueryParameter(
            None, S("file_id", "STRING", i.rec.file_id), S("attempt_no", "INT64", i.attempt_no))
            for i in items]

        # 2. the transaction
        t0 = datetime.now(timezone.utc)
        sid = self._run("BEGIN TRANSACTION;", create_session=True).session_info.session_id
        try:
            def q(sql, params=None):
                return self._run(sql, params, session_id=sid)

            q(f"DELETE FROM {det} WHERE file_id IN UNNEST(@ids) OR file_id IN UNNEST(@old)",
              [A("ids", "STRING", ids), A("old", "STRING", old_ids)])
            j = q(f"INSERT INTO {det} SELECT s.* EXCEPT(attempt_no) FROM {stg} s "
                  "JOIN UNNEST(@pairs) p ON s.file_id = p.file_id AND s.attempt_no = p.attempt_no",
                  [A("pairs", "STRUCT", pairs)])
            if j.num_dml_affected_rows != expected:
                raise RuntimeError(f"inserted {j.num_dml_affected_rows} rows, expected {expected}")
            q("COMMIT TRANSACTION;")
        except BaseException:                      # includes Shutdown (SIGTERM): always try to roll back
            try:
                self._run("ROLLBACK TRANSACTION;", session_id=sid)
            except Exception:
                log.exception("rollback failed; aborting session %s", sid)
                try:
                    self._run("CALL BQ.ABORT_SESSION();", session_id=sid)
                except Exception:
                    log.exception("could not abort session %s", sid)
            raise
        log.info("committed %d files, %d rows in %.1fs", len(items), expected,
                 (datetime.now(timezone.utc) - t0).total_seconds())

        # 3. registry (outside the transaction)
        updated = 0
        for chunk in self._chunks(items, 1000):
            arr = [bigquery.StructQueryParameter(
                None, S("file_id", "STRING", i.rec.file_id), S("rr", "INT64", i.records_read),
                S("rl", "INT64", i.records_loaded), S("rj", "INT64", i.records_rejected)) for i in chunk]
            job = self._run(f"""UPDATE {self.reg} T
                SET status='COMPLETED', records_read=S.rr, records_loaded=S.rl, records_rejected=S.rj,
                    completed_at=CURRENT_TIMESTAMP(), updated_at=CURRENT_TIMESTAMP(),
                    last_error=NULL, lease_expiry=NULL
                FROM UNNEST(@rows) S
                WHERE T.source_type=@src AND T.file_id=S.file_id AND T.claimed_by=@run
                  AND T.status='IN_PROGRESS'""",
                [A("rows", "STRUCT", arr), S("run", "STRING", run_id), S("src", "STRING", src_type)])
            updated += job.num_dml_affected_rows
        if updated != len(items):
            log.warning("registry updated %d of %d files; the rest stay IN_PROGRESS and will be "
                        "retried (reload is idempotent)", updated, len(items))
        if old_ids:
            self._run(f"UPDATE {self.reg} SET status='SUPERSEDED', updated_at=CURRENT_TIMESTAMP() "
                      "WHERE source_type=@src AND file_id IN UNNEST(@old)",
                      [A("old", "STRING", old_ids), S("src", "STRING", src_type)])
        self._events_many([(i.rec.file_id, i.attempt_no, run_id, "COMPLETED", i.records_loaded, None)
                           for i in items])
        self.cleanup_staging_many(t, ids)
        return ids

    # ------------------------------------------------------------ failure / cleanup
    def cleanup_staging_many(self, t: TargetConfig, file_ids: List[str]):
        try:
            for chunk in self._chunks(file_ids, 5000):
                self._run(f"DELETE FROM `{t.staging_table}` WHERE file_id IN UNNEST(@ids)",
                          [bigquery.ArrayQueryParameter("ids", "STRING", chunk)])
        except Exception:
            log.exception("staging cleanup failed (partition expiry will clear it)")

    def cleanup_staging(self, t: TargetConfig, file_id: str):
        self.cleanup_staging_many(t, [file_id])

    def mark_failed_batch(self, t: TargetConfig, failures: list, run_id: str):
        """failures: list of (FileRecord, attempt_no, error). One UPDATE, one log insert, one cleanup."""
        if not failures:
            return
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        src_type = failures[0][0].source_type
        for chunk in self._chunks(failures, 1000):
            arr = [bigquery.StructQueryParameter(
                None, S("file_id", "STRING", rec.file_id), S("err", "STRING", str(err)[:2000]))
                for rec, _, err in chunk]
            self._run(
                f"UPDATE {self.reg} T SET status='FAILED', last_error=S.err, lease_expiry=NULL, "
                "updated_at=CURRENT_TIMESTAMP() FROM UNNEST(@rows) S "
                "WHERE T.source_type=@src AND T.file_id=S.file_id AND T.claimed_by=@run AND T.status='IN_PROGRESS'",
                [A("rows", "STRUCT", arr), S("run", "STRING", run_id), S("src", "STRING", src_type)])
        self._events_many([(rec.file_id, att, run_id, "FAILED", None, err) for rec, att, err in failures])
        self.cleanup_staging_many(t, [rec.file_id for rec, _, _ in failures])

    def mark_failed(self, t: TargetConfig, rec: FileRecord, run_id: str, attempt_no: int, error: str):
        self.mark_failed_batch(t, [(rec, attempt_no, error)], run_id)

    def quarantine_exhausted(self, src: SourceConfig, max_attempts: int = 3) -> List[str]:
        """Files that failed/crashed max_attempts times -> QUARANTINED, moved, alert.
        Guard: if the object at the path is no longer the one the row was registered for (the sender
        overwrote it, possibly seconds ago), the row is SUPERSEDED instead and the object is NOT moved."""
        S, A = bigquery.ScalarQueryParameter, bigquery.ArrayQueryParameter
        rows = self._rows(f"""
            SELECT file_id, object_name, base_name, last_error, generation FROM {self.reg}
            WHERE source_type=@s AND attempt_count >= @max
              AND (status='FAILED' OR (status='IN_PROGRESS' AND lease_expiry < CURRENT_TIMESTAMP()))""",
            [S("s", "STRING", src.source_type), S("max", "INT64", max_attempts)])
        if not rows:
            return []
        bucket = self.gcs.bucket(self.bucket_name)
        real, replaced = [], []
        for r in rows:
            blob = bucket.get_blob(r.object_name)
            if blob is not None and r.generation is not None and blob.generation != r.generation:
                replaced.append(r)
            else:
                real.append(r)
        if replaced:
            msg = "object at this path was replaced by a file with different content"
            rid = [r.file_id for r in replaced]
            self._run(f"UPDATE {self.reg} SET status='SUPERSEDED', claimed_by=NULL, lease_expiry=NULL, "
                      "last_error=@m, updated_at=CURRENT_TIMESTAMP() WHERE source_type=@src AND file_id IN UNNEST(@ids)",
                      [S("m", "STRING", msg), A("ids", "STRING", rid), S("src", "STRING", src.source_type)])
            self._events_many([(i, None, None, "SUPERSEDED", None, msg) for i in rid])
            log.warning("%d exhausted row(s) superseded, not quarantined: their path holds a newer file", len(rid))
        if not real:
            return []
        ids = [r.file_id for r in real]
        self._run(f"UPDATE {self.reg} SET status='QUARANTINED', claimed_by=NULL, "
                  "updated_at=CURRENT_TIMESTAMP() WHERE source_type=@src AND file_id IN UNNEST(@ids)",
                  [A("ids", "STRING", ids), S("src", "STRING", src.source_type)])
        moved = []
        for r in real:
            dest = self._move_blob(r.object_name, src.quarantine_prefix, r.base_name, r.file_id)
            if dest:
                moved.append((r.file_id, dest))
        if moved:
            self._record_moves(moved, src.source_type)
        self._events_many([(r.file_id, None, None, "QUARANTINED", None, r.last_error) for r in real])
        self.alert_fn("PRISM files quarantined",
                      f"{src.source_type}: {len(ids)} file(s) exceeded {max_attempts} attempts")
        return ids
