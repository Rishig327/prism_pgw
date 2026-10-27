"""
run_prism_batch.py - orchestrator. One Dataflow job and ONE batch commit per run.

    python run_prism_batch.py PGW

Steps
  1 discover      register new files from input/, move already-known files
  2 quarantine    files that exhausted attempts
  3 claim_batch   lease up to BATCH_FILES files (atomic)
  4 manifest      one JSON line per file {file_id, attempt_no, path} -> GCS
  5 dataflow      run the pipeline, wait (heartbeat leases)  -> staging, read_counts, reject_log
  6 check         per file in Python (decode error, empty, reject ratio, read = loaded + rejected)
  7 commit_batch  all files that passed: ONE transaction on the detail table, then registry COMPLETED
  8 move          committed files -> processed/ (parallel), staging cleaned
Files that fail their checks are marked FAILED (retried next run); they never block the others.

Two launch modes (SourceSettings.launch_mode)
  "vm"   : the pipeline runs as a local process of this program (testing). Behaves exactly as before.
  "flex" : Flex Template through the Dataflow API - for Cloud Run jobs, no VM. Extra behaviour, all
           recorded in the prism_runs table:
             * skip_if_running : a newer run of the same source stands down while an earlier one is alive
             * SIGTERM         : on shutdown the run marks itself ORPHANED; the Dataflow job keeps running
             * recovery        : the next run adopts every orphaned run, waits for its job if it is still
                                 running, then checks/commits/moves its files (no second Dataflow job)

Settings: ALL of them are read from the configuration tables in VPDB_ADM at the start of every run
(config_store.py; change them with config_admin.py). The same settings apply to both launch modes.
Only the project, location, control dataset and encryption key stay in code (config.py / prism_settings.py).

    python run_prism_batch.py PGW                     # flex mode (Cloud Run, the default)
    python run_prism_batch.py PGW --launch-mode vm    # VM test: pipeline runs as a local process
"""
import argparse
import dataclasses
import json
import logging
import os
import signal
import subprocess
import sys
import time
import traceback
import uuid

from google.cloud import storage
from googleapiclient.discovery import build

import config_store as cs
from config import (PROJECT, BUCKET, LOCATION, CTL_DATASET, KMS_KEY, MAX_ATTEMPTS, BATCH_FILES,
                    LEASE_MIN, MAX_JOB_WAIT_MIN, SourceSettings)
from file_tracker import CommitItem, FileTracker

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("prism.batch")

TERMINAL = {"JOB_STATE_DONE", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED",
            "JOB_STATE_UPDATED", "JOB_STATE_DRAINED"}


class Shutdown(BaseException):
    """Raised by the SIGTERM handler. A BaseException on purpose: the many `except Exception` blocks
    must not swallow it, or mark files failed just because the container is being stopped."""


def _raise_shutdown(signum, frame):
    raise Shutdown()


def safe_heartbeat(tr: FileTracker, run_id: str, source_type: str, track_run: bool = False, also_beat: str = None):
    """A failed heartbeat (BigQuery DML conflict/queue) must not kill the run; the lease is long."""
    try:
        tr.heartbeat_batch(run_id, LEASE_MIN, source_type)
        if track_run:
            tr.run_heartbeat(run_id, source_type)
            if also_beat:
                tr.run_heartbeat(also_beat, source_type)
    except Exception:
        log.warning("heartbeat failed, will retry next poll", exc_info=True)


def bq_ref(table: str) -> str:
    """project.dataset.table -> project:dataset.table (Beam format)"""
    p, rest = table.split(".", 1)
    return f"{p}:{rest}"


def job_name(cfg: SourceSettings, run_id: str) -> str:
    return f"prism-{cfg.src.source_type}-{run_id[-12:]}".lower()


def write_manifest(cfg: SourceSettings, run_id, recs, claimed) -> str:
    lines = [json.dumps({"file_id": r.file_id, "attempt_no": claimed[r.file_id],
                         "path": f"gs://{BUCKET}/{r.object_name}"})
             for r in recs if r.file_id in claimed]
    blob_name = f"{cfg.manifest_prefix}{run_id}.jsonl"
    storage.Client(project=PROJECT).bucket(BUCKET).blob(blob_name).upload_from_string("\n".join(lines))
    return f"gs://{BUCKET}/{blob_name}"


def launch_dataflow(cfg: SourceSettings, manifest_uri: str, run_id: str) -> str:
    """Launch the Flex Template. Parameter names must match dataflow_pipeline.py arguments."""
    df = build("dataflow", "v1b3", cache_discovery=False)
    body = {"launchParameter": {
        "jobName": job_name(cfg, run_id),
        "containerSpecGcsPath": cfg.flex_template_gcs_path,
        "parameters": {
            "manifest": manifest_uri,
            "run_id": run_id,
            "source_type": cfg.src.source_type,
            "staging_table": bq_ref(cfg.target.staging_table),
            "reject_table": bq_ref(cfg.reject_table),
            "read_counts_table": bq_ref(cfg.read_counts_table),
            "kms_key_name": cfg.kms_key_name,
        },
        "environment": cfg.dataflow_env,
    }}
    resp = df.projects().locations().flexTemplates().launch(
        projectId=PROJECT, location=LOCATION, body=body).execute()
    job_id = resp["job"]["id"]
    log.info("launched Dataflow job %s", job_id)
    return job_id


def get_job_state(job_id: str) -> str:
    df = build("dataflow", "v1b3", cache_discovery=False)
    return df.projects().locations().jobs().get(
        projectId=PROJECT, location=LOCATION, jobId=job_id).execute()["currentState"]


def find_job_by_name(name: str):
    """A run can stop between 'job launched' and 'job id saved'. The job name is derived from the run id,
    so it can be found again among the most recent jobs."""
    df = build("dataflow", "v1b3", cache_discovery=False)
    resp = df.projects().locations().jobs().list(
        projectId=PROJECT, location=LOCATION, filter="ALL", pageSize=100).execute()
    for j in resp.get("jobs", []):
        if j.get("name") == name:
            return j["id"]
    return None


def wait_for_job(tr: FileTracker, job_id: str, run_id: str, source_type: str, poll_s: int = 60,
                 track_run: bool = False, also_beat: str = None) -> bool:
    deadline = time.time() + MAX_JOB_WAIT_MIN * 60
    while time.time() < deadline:
        state = get_job_state(job_id)
        if state in TERMINAL:
            log.info("job %s finished: %s", job_id, state)
            return state == "JOB_STATE_DONE"
        safe_heartbeat(tr, run_id, source_type, track_run, also_beat)   # keep leases alive while job runs
        time.sleep(poll_s)
    log.error("job %s exceeded %s min", job_id, MAX_JOB_WAIT_MIN)
    return False


ENV_TO_FLAG = {"tempLocation": "--temp_location", "stagingLocation": "--staging_location",
               "serviceAccountEmail": "--service_account_email",
               "network": "--network", "subnetwork": "--subnetwork",
               "maxWorkers": "--max_num_workers", "kmsKeyName": "--dataflow_kms_key",
               "machineType": "--machine_type"}


def run_pipeline_from_vm(tr: FileTracker, cfg: SourceSettings, manifest_uri: str, run_id: str) -> bool:
    """Run dataflow_pipeline.py on THIS machine with DataflowRunner (no Flex Template).
    The process stays alive until the Dataflow job ends; exit code 0 = job succeeded.
    Leases are extended while it runs."""
    here = os.path.dirname(os.path.abspath(__file__))
    cmd = [sys.executable, os.path.join(here, "dataflow_pipeline.py"),
           f"--manifest={manifest_uri}", f"--run_id={run_id}",
           f"--source_type={cfg.src.source_type}",
           f"--staging_table={bq_ref(cfg.target.staging_table)}",
           f"--reject_table={bq_ref(cfg.reject_table)}",
           f"--read_counts_table={bq_ref(cfg.read_counts_table)}",
           f"--kms_key_name={cfg.kms_key_name}",
           "--runner=DataflowRunner", f"--project={PROJECT}", f"--region={LOCATION}",
           f"--job_name={job_name(cfg, run_id)}",
           f"--setup_file={os.path.join(here, 'setup.py')}"]
    for key, flag in ENV_TO_FLAG.items():
        if key in cfg.dataflow_env:
            cmd.append(f"{flag}={cfg.dataflow_env[key]}")
    log.info("starting pipeline: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, cwd=here)
    deadline = time.time() + MAX_JOB_WAIT_MIN * 60
    while proc.poll() is None:
        if time.time() > deadline:
            proc.kill()
            log.error("pipeline exceeded %s min, killed local process", MAX_JOB_WAIT_MIN)
            return False
        safe_heartbeat(tr, run_id, cfg.src.source_type)
        time.sleep(60)
    log.info("pipeline process exited with code %s", proc.returncode)
    return proc.returncode == 0


# ---------------------------------------------------------------------- after the job: check, commit, move
def finalise(tr: FileTracker, cfg: SourceSettings, run_id: str, by_id: dict, claimed: dict):
    """Per-file checks (a bad file fails alone), then ONE transaction for the files that passed,
    then registry, then move. Used by normal runs and by recovery of an orphaned run."""
    src, target = cfg.src, cfg.target
    counts = tr.collect_counts(target, claimed)
    ferrs = tr.file_errors(claimed)                       # decoder crashed on the whole file
    passed, failures = [], []
    for fid, rec in by_id.items():
        attempt = claimed[fid]
        read, loaded, rejected = counts.get(fid, (0, 0, 0))
        try:
            if fid in ferrs:
                raise ValueError(ferrs[fid])
            if read == 0 and not cfg.allow_empty_files:
                raise ValueError("no records read - file not read by Dataflow or empty")
            if read != loaded + rejected:
                raise ValueError(f"count mismatch: read={read} loaded={loaded} rejected={rejected}")
            if read and rejected / read > cfg.max_reject_ratio:
                raise ValueError(f"reject ratio {rejected}/{read} above {cfg.max_reject_ratio}")
            passed.append(CommitItem(rec, attempt, read, loaded, rejected))
        except Exception as e:
            failures.append((rec, attempt, f"{type(e).__name__}: {e}"))
            log.error("file %s failed checks: %s", rec.object_name, e)
    if failures:
        tr.mark_failed_batch(target, failures, run_id)

    done = 0
    if passed:
        try:
            committed = set(tr.commit_batch(target, run_id, passed))
        except Exception:
            err = traceback.format_exc()                  # rolled back: detail untouched, retry next run
            tr.mark_failed_batch(target, [(i.rec, i.attempt_no, err) for i in passed], run_id)
            raise
        done = tr.move_files([i.rec for i in passed if i.rec.file_id in committed], src.processed_prefix)

    tr.quarantine_exhausted(src, MAX_ATTEMPTS)
    log.info("run %s finished: committed+moved=%s failed=%s", run_id, done, len(failures))
    return done, len(failures)


# ---------------------------------------------------------------------- vm mode (testing) - unchanged behaviour
def run_vm(source_type: str, cfg: SourceSettings):
    src, target = cfg.src, cfg.target
    run_id = f"{source_type}-{uuid.uuid4().hex[:12]}"
    tr = FileTracker(project=PROJECT, bucket=BUCKET, ctl_dataset=CTL_DATASET, location=LOCATION,
                     kms_key=cfg.kms_key_name)

    tr.discover(src)
    tr.quarantine_exhausted(src, MAX_ATTEMPTS)
    recs = tr.get_eligible(source_type, MAX_ATTEMPTS, limit=BATCH_FILES)
    if not recs:
        log.info("nothing to process")
        return
    claimed = tr.claim_batch(recs, run_id, MAX_ATTEMPTS, LEASE_MIN)      # {file_id: attempt_no}
    by_id = {r.file_id: r for r in recs if r.file_id in claimed}
    if not by_id:
        return
    log.info("run %s claimed %s files", run_id, len(by_id))

    # ---- Dataflow job (whole-job failure: every file goes back to FAILED, nothing in detail)
    try:
        manifest_uri = write_manifest(cfg, run_id, recs, claimed)
        if cfg.launch_mode == "vm":
            if not run_pipeline_from_vm(tr, cfg, manifest_uri, run_id):
                raise RuntimeError("Dataflow pipeline (vm launch) did not finish successfully")
        else:
            job_id = launch_dataflow(cfg, manifest_uri, run_id)
            if not wait_for_job(tr, job_id, run_id, cfg.src.source_type):
                raise RuntimeError(f"Dataflow job {job_id} did not finish successfully")
    except Exception:
        err = traceback.format_exc()                      # whole job failed: nothing reached detail
        tr.mark_failed_batch(target, [(rec, claimed[fid], err) for fid, rec in by_id.items()], run_id)
        raise
    finalise(tr, cfg, run_id, by_id, claimed)


# ---------------------------------------------------------------------- flex mode (Cloud Run, no VM)
def recover_orphans(tr: FileTracker, cfg: SourceSettings, my_run_id: str):
    """Finish runs that stopped without finishing. Runs BEFORE this run claims anything, so their files
    (still leased to the old run id) are not taken by a new batch. One adopter per orphan (atomic)."""
    st = cfg.src.source_type
    for o in tr.find_orphan_runs(st, my_run_id, cfg.orphan_after_min):
        if not tr.claim_orphan(o.run_id, st, o.status, my_run_id):
            continue                                       # another run adopted it first
        log.warning("adopting orphaned run %s (status %s, job %s)", o.run_id, o.status, o.job_id)
        try:
            recs, claimed = tr.claimed_by_run(st, o.run_id)
            by_id = {r.file_id: r for r in recs}
            if not by_id:
                tr.run_update(o.run_id, st, status="DONE", note=f"adopted by {my_run_id}: no files left", finished=True)
                continue
            job_id = o.job_id or find_job_by_name(job_name(cfg, o.run_id))
            if not job_id:                                 # nothing was launched: give the files back
                n = tr.release_claims(st, o.run_id)
                tr.run_update(o.run_id, st, status="ABANDONED", finished=True,
                              note=f"no Dataflow job found; released {n} files (adopted by {my_run_id})")
                continue
            state = get_job_state(job_id)
            ok = (state == "JOB_STATE_DONE") if state in TERMINAL else \
                wait_for_job(tr, job_id, o.run_id, st, track_run=True, also_beat=my_run_id)
            if not ok:
                err = f"orphaned Dataflow job {job_id} did not finish successfully (state {state})"
                tr.mark_failed_batch(cfg.target, [(rec, claimed[fid], err) for fid, rec in by_id.items()], o.run_id)
                tr.run_update(o.run_id, st, status="FAILED", note=err, finished=True)
                continue
            tr.run_update(o.run_id, st, status="FINALISING")
            finalise(tr, cfg, o.run_id, by_id, claimed)
            tr.run_update(o.run_id, st, status="DONE", note=f"finished by {my_run_id}", finished=True)
        except Shutdown:
            tr.run_update(o.run_id, st, status="ORPHANED", note=f"adopter {my_run_id} shut down")
            raise
        except Exception:
            log.exception("recovery of %s failed", o.run_id)
            tr.run_update(o.run_id, st, status="FAILED", note=traceback.format_exc()[-500:], finished=True)


def run_flex(source_type: str, cfg: SourceSettings, config_version: str = None):
    src, target = cfg.src, cfg.target
    run_id = f"{source_type}-{uuid.uuid4().hex[:12]}"
    tr = FileTracker(project=PROJECT, bucket=BUCKET, ctl_dataset=CTL_DATASET, location=LOCATION,
                     kms_key=cfg.kms_key_name)
    signal.signal(signal.SIGTERM, _raise_shutdown)         # Cloud Run sends SIGTERM, then ~10 s to stop
    tr.run_register(run_id, source_type, config_version=config_version)
    by_id, claimed = {}, {}
    try:
        if cfg.skip_if_running:
            earlier = tr.live_runs_before(source_type, run_id, cfg.orphan_after_min)
            if earlier:
                tr.run_update(run_id, source_type, status="ABANDONED", finished=True,
                              note=f"another run of this source is active: {earlier[0]}")
                log.info("run %s: %s is still active, standing down", run_id, earlier[0])
                return
        recover_orphans(tr, cfg, run_id)
        tr.discover(src)
        tr.run_update(run_id, source_type)                 # heartbeat: discovery can be slow
        tr.quarantine_exhausted(src, MAX_ATTEMPTS)
        recs = tr.get_eligible(source_type, MAX_ATTEMPTS, limit=BATCH_FILES)
        claimed = tr.claim_batch(recs, run_id, MAX_ATTEMPTS, LEASE_MIN) if recs else {}
        by_id = {r.file_id: r for r in recs if r.file_id in claimed}
        if not by_id:
            tr.run_update(run_id, source_type, status="DONE", note="nothing to process", finished=True)
            log.info("nothing to process")
            return
        tr.run_update(run_id, source_type, status="CLAIMED")
        log.info("run %s claimed %s files", run_id, len(by_id))

        manifest_uri = write_manifest(cfg, run_id, recs, claimed)
        job_id = launch_dataflow(cfg, manifest_uri, run_id)
        tr.run_update(run_id, source_type, status="LAUNCHED", job_id=job_id, manifest_uri=manifest_uri)
        if not wait_for_job(tr, job_id, run_id, source_type, track_run=True):
            raise RuntimeError(f"Dataflow job {job_id} did not finish successfully")

        tr.run_update(run_id, source_type, status="FINALISING")
        finalise(tr, cfg, run_id, by_id, claimed)
        tr.run_update(run_id, source_type, status="DONE", finished=True)
    except Shutdown:
        # Never mark files failed and never cancel the job. With claimed files the run becomes ORPHANED
        # at once (the next run decides: job found -> finish it, none -> release the files).
        log.warning("SIGTERM: run %s marked ORPHANED", run_id)
        try:
            if by_id:
                tr.run_update(run_id, source_type, status="ORPHANED",
                              note="shutdown (SIGTERM); the next run finishes it")
            else:
                tr.run_update(run_id, source_type, status="ABANDONED", note="shutdown before claiming", finished=True)
        except BaseException:
            log.exception("could not record shutdown; the run will be treated as orphaned after the heartbeat times out")
    except Exception:
        err = traceback.format_exc()
        if by_id:                                          # guarded by status/claimed_by: finished files are untouched
            tr.mark_failed_batch(target, [(rec, claimed[fid], err) for fid, rec in by_id.items()], run_id)
        tr.run_update(run_id, source_type, status="FAILED", note=err[-500:], finished=True)
        raise


def _resolve(source_type: str):
    """Current configuration of this source from the tables. No default values: an empty column stops the run.
    The table decides which sources exist; config.py SOURCES is not consulted."""
    store = cs.ConfigStore(cs.BqHelper(PROJECT, LOCATION, KMS_KEY), PROJECT, CTL_DATASET)
    return store.resolve(source_type, KMS_KEY)


def run(source_type: str, launch_mode: str = "flex"):
    global MAX_ATTEMPTS, BATCH_FILES, LEASE_MIN, MAX_JOB_WAIT_MIN
    res = _resolve(source_type)
    if not res.enabled:
        log.warning("source %s is disabled in the configuration table; nothing to do", source_type)
        return
    g = res.globals
    MAX_ATTEMPTS, BATCH_FILES, LEASE_MIN, MAX_JOB_WAIT_MIN = (
        g["MAX_ATTEMPTS"], g["BATCH_FILES"], g["LEASE_MIN"], g["MAX_JOB_WAIT_MIN"])
    cfg = dataclasses.replace(res.settings, launch_mode=launch_mode)
    log.info("configuration version %s, launch mode %s", res.version, launch_mode)
    if launch_mode == "flex":
        run_flex(source_type, cfg, res.version)
    else:
        run_vm(source_type, cfg)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("source")
    ap.add_argument("--launch-mode", choices=["vm", "flex"], default="flex",
                    help="vm = pipeline as a local process (testing); flex = Flex Template (Cloud Run)")
    a = ap.parse_args()
    try:
        run(a.source, a.launch_mode)
    except cs.ConfigError as e:
        log.error("%s", e)
        sys.exit(2)
