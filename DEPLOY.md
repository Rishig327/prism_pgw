# PRISM / PGW - deploy and test

## 0. Before anything
- Decide where `run_prism_batch.py` runs (Cloud Run job or Composer task). It needs a service account with
  BigQuery (jobs + edit on VPDB_ADM and VPDB_DAT), Storage object admin on the bucket, Dataflow developer,
  and "act as" on the Dataflow worker service account.
- Upload the format file your old pipeline used:
  `gsutil cp format_pgw.xml gs://vf-uk-raid-pre-prod-working/dataflow/formats/format_pgw.xml`

## 1. Tables
Run `ddl.sql` sections 1-3 (control tables, PGW detail + staging, views).
Section 4 (aggregate rebuild) and 5 (recon) are for later.

## 2. Local check (no GCP needed)
    pip install apache-beam google-cloud-bigquery
    python tests/test_pgw_local.py /path/to/pgwld5_..._file9492649.u.gz
Expect: CDRs decoded = number in the file name, rejects 0, columns match schema = True.

## 3. Build the Flex Template
    gcloud builds submit --tag europe-west2-docker.pkg.dev/vf-uk-raid-pre-prod/<REPO>/prism-dataflow:1.0 .
    gcloud dataflow flex-template build gs://vf-uk-raid-pre-prod-working/dataflow/templates/prism_dataflow.json \
      --image europe-west2-docker.pkg.dev/vf-uk-raid-pre-prod/<REPO>/prism-dataflow:1.0 \
      --sdk-language PYTHON --metadata-file metadata.json
(Check flags against current gcloud docs; they change.)

## 4. First test run
1. Put 2-3 PGW files in `dataflow/PGW/input/`.
2. `python run_prism_batch.py PGW --launch-mode vm`
3. Check: `v_file_status_daily`, ddl.sql 5a and 5b return 0 rows, files moved to `dataflow/PGW/done/YYYY/MM/DD/`.
4. Failure test: copy a file, truncate it, drop it in input/ -> it should go FAILED x3 then QUARANTINED, nothing in detail.
5. Crash test: kill the orchestrator mid-run -> next run re-claims after the lease expires, no duplicate rows.

## 5. Run on a GCP VM (no Flex Template)
See the chat guide. Summary:
    python3 -m venv ~/prism-venv && source ~/prism-venv/bin/activate
    pip install apache-beam[gcp] google-cloud-bigquery google-cloud-storage google-api-python-client
    cd prism_pgw && python run_prism_batch.py PGW --launch-mode vm
Cron (single instance guard):
    */15 * * * * flock -n /tmp/prism_pgw.lock /home/<user>/prism-venv/bin/python /home/<user>/prism_pgw/run_prism_batch.py PGW --launch-mode vm >> /home/<user>/prism_pgw.log 2>&1


## 6. Before every deploy: worker import check
    python tests/check_worker_imports.py
Fails if a module the Dataflow workers import is missing from setup.py / the Dockerfile, or if
worker code imports orchestrator-only files (config.py, file_tracker.py, run_prism_batch.py).
Worker settings live in prism_settings.py; orchestrator settings live in config.py.


## 7. Reconciliation (aggregate recon, alarms, manual detailed recon)
Runs on the VM only (not shipped to Dataflow). Files: recon_logic.py, recon_runner.py, ddl_recon.sql,
recon_definitions.example.json, tests/test_recon.py.

1. Create tables: run ddl_recon.sql once (writes to VPDB_ADM, same CMEK key).
2. Copy recon_definitions.example.json to recon_definitions.json and fill in side B (every <PLACEHOLDER>).
   A definition that still has placeholders is refused. One entry per pair of sources / per grain.
3. Validate the SQL on your project without writing anything:
       python recon_runner.py run <RECON_ID> --dry-run
4. Schedule (after ingestion, for example hourly, with the same lock approach):
       flock -n /tmp/prism_recon.lock python recon_runner.py run
5. Alarms are rows in VPDB_ADM.recon_alarm and one log line "PRISM_RECON_ALARM {json}" at ERROR level.
   Create a Cloud Logging log-based alert on that text to send email / Teams.
6. People: python recon_runner.py alarms | ack <ID> --by NAME | suppress <ID> --by NAME
   Detailed recon for one alarm: python recon_runner.py detail <ID> --by NAME [--dim APN=x] [--dry-run]
   Results: VPDB_ADM.recon_detail_summary / recon_detail_sample (kept 90 days).
7. Run tests/test_recon.py before deploying changes to the recon code.

## 8. Registry lookup window (per source)
`SourceConfig.lookup_window_days` (config.py, default 30). At discovery, the known-file, same-content,
same-name, replaced-path and checksum-reuse lookups, and the registration MERGE, only see registry rows
discovered within the window, and every registry statement is filtered by source_type. A file that comes
back after the window is simply loaded again (no other check). Keep the window longer than any file can
wait in the input folder or be re-sent; a warning is logged for input files older than the window.
Eligible-file selection is NOT windowed (it filters by source and status), so old pending rows are never stranded.
Tests: tests/test_lookup_window.py.

## 9. No VM (Cloud Run jobs + Scheduler)
See DEPLOY_CLOUD_RUN.md: orchestrator image (Dockerfile.orchestrator), settings in Cloud Storage
(config_store.py / config_admin.py, tables in ddl_config.sql), run tracking and recovery of orphaned runs (prism_runs table).
Extra checks: tests/check_orchestrator_imports.py, tests/test_flex_run.py, tests/test_config_store.py.

## 10. Settings now come from BigQuery tables (all modes)
The VM test and Cloud Run read the same tables (ddl_config.sql). First time: `python config_admin.py seed --by NAME`.
The VM test is now `python run_prism_batch.py PGW --launch-mode vm` (flex is the default). Recon definitions are stored
with `config_admin.py recon-put` (or `recon_runner.py --defs-file file.json ...` for a quick local test).
See DEPLOY_CLOUD_RUN.md.
