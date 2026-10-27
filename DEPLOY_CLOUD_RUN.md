# Running without a VM: Cloud Run jobs + Cloud Scheduler, settings in BigQuery tables

Nothing here has been run on your project yet. Check every gcloud flag with `gcloud ... --help` (flags change).

## Where settings live
| What | Where |
|---|---|
| Project, location, control dataset, encryption key | code (`config.py`, `prism_settings.py`): needed before any table can be read |
| Which sources exist | the rows of `prism_config_source` (a source name that has no row is refused with the list of configured names). `SOURCES` in `config.py` is only the one-time input of `seed`: to add a source, add it there and run `seed` (it writes only missing sources). |
| Everything else: paths, window, conflict policy, tables, Dataflow options, batch size, attempts, enable/disable | BigQuery tables in `VPDB_ADM` (`ddl_config.sql`) |
| Recon definitions | `VPDB_ADM.prism_recon_definition` |
| How the program is started | `--launch-mode flex` (default, Cloud Run) or `--launch-mode vm` (VM test) |

The VM test and Cloud Run read the **same tables**, so they always run with the same settings. A change is one
command and applies to the next run: no image, no file.

## One-time setup
1. BigQuery: run `ddl.sql` (control tables, `prism_runs`), `ddl_config.sql`, `ddl_recon.sql`.
2. Write the current code defaults into the tables (never overwrites existing values):
   `python config_admin.py seed --by <you>` then check with `python config_admin.py show`.
3. Flex Template built and its JSON in Cloud Storage (steps in DEPLOY.md); `flex_template_gcs_path` in the
   source row must point to it: `python config_admin.py set PGW flex_template_gcs_path=gs://.../prism_dataflow.json --by <you>`.
4. Give EDIT rights on the three `prism_config_*` / `prism_recon_definition` tables to a few people only.
5. Local checks: `python tests/check_orchestrator_imports.py`, `python tests/check_worker_imports.py`.

## Changing settings (any time, no rebuild)
```
python config_admin.py show PGW
python config_admin.py set PGW conflict_policy=supersede lookup_window_days=45 --by rishi --note "corrections expected"
python config_admin.py set PGW df_max_workers=20 --by rishi --dry-run     # validate only
python config_admin.py set-global max_attempts=1 batch_files=300 --by rishi
python config_admin.py disable PGW --by rishi      # pause a source without touching the scheduler (enable to resume)
python config_admin.py history PGW                 # who changed what, when
python config_admin.py rollback PGW --as-of 2026-10-10T08:00:00Z --by rishi
python config_admin.py recon-put MY_RECON definition.json --by rishi
```
Every change inserts a NEW version row (who, when, note); the newest row wins; nothing is overwritten.
A value is validated before it is written (types, ranges, `gs://` paths, trailing `/`, `project.dataset.table`);
a bad value is rejected with all problems listed. **There are no default values:** every column must hold a value.
A run stops with a message naming every empty column, and a blank cannot be stored (the table is `NOT NULL`, and
`config_admin.py` refuses it). The only columns that may be empty are the `df_*` Dataflow options: an empty one is
not passed, so **Dataflow's own default applies** (for example no worker limit, the default network, the default
service account, which can change cost and access). `ignore_suffixes=none` stores an explicit empty value (nothing
ignored). `config.py` values are copied into the tables once by `seed`; they are not used at run time.
Each run records the version it used in `prism_runs.config_version`.
Code changes (decoders, transformations, new template parameters) still need a new image.

## Build the orchestrator image (no Apache Beam)
```
gcloud builds submit --region=europe-west2 --config=cloudbuild.orchestrator.yaml \
  --substitutions=_IMAGE=europe-west2-docker.pkg.dev/vf-uk-raid-pre-prod/prism-orchestrator-repo/prism-orchestrator:1.0 .
```

## Service accounts
Orchestrator job account: Dataflow Developer; BigQuery Data Editor + Job User; Storage Object Admin on the
bucket (manifests, file moves); Service Account User on the Dataflow worker account; access to the CMEK key;
Artifact Registry Reader. Scheduler account: Cloud Run Invoker on the jobs.

## Create the jobs and schedule
```
IMG=europe-west2-docker.pkg.dev/vf-uk-raid-pre-prod/<REPO>/prism-orchestrator:1.0

gcloud run jobs create prism-orchestrator-pgw --region=europe-west2 --image=$IMG \
  --args=run_prism_batch.py,PGW --task-timeout=<longer than your longest Dataflow run, e.g. 300m> \
  --max-retries=0 --service-account=<ORCH_SA>

gcloud run jobs create prism-recon --region=europe-west2 --image=$IMG \
  --args=recon_runner.py,run --task-timeout=60m --max-retries=0 --service-account=<ORCH_SA>

gcloud scheduler jobs create http prism-orchestrator-pgw-15min --location=europe-west2 \
  --schedule="*/15 * * * *" --http-method=POST \
  --uri="https://run.googleapis.com/v2/projects/vf-uk-raid-pre-prod/locations/europe-west2/jobs/prism-orchestrator-pgw:run" \
  --oauth-service-account-email=<SCHEDULER_SA>
```
No environment variables are needed. `--max-retries=0` on purpose: a retry would just start another run; the next
schedule and the recovery logic already cover a failed run. Stagger the sources (for example :00, :05, :10).
Check the maximum task timeout allowed for jobs in your project.

## First test
`python run_prism_batch.py PGW --launch-mode vm` on the VM (unchanged behaviour), then
`gcloud run jobs execute prism-orchestrator-pgw --region=europe-west2 --wait`; the run appears in
`VPDB_ADM.prism_runs` as STARTING > CLAIMED > LAUNCHED > FINALISING > DONE.

## What happens when something stops (flex mode)
| Event | Result |
|---|---|
| Cloud Run stops the task (SIGTERM) while Dataflow runs | Run marked ORPHANED, files NOT failed, job left running. The next run finishes that job's files. About 10 s are available to record this; otherwise the run counts as orphaned after `orphan_after_min` without a heartbeat. |
| Task killed with no warning | Same recovery, after `orphan_after_min`. |
| Orphan's job already finished | Files checked, committed, moved by the next run; no second Dataflow job. |
| Orphan's job still running | The next run waits for it (both runs keep their heartbeat), then finishes it. |
| Orphan's job failed | Its files go back to FAILED and retry normally. |
| Stopped before any job was launched | Files released (attempt not consumed). |
| Two runs of one source start together | The later one stands down (`skip_if_running`, default true; `set PGW skip_if_running=false` to allow overlap). |

Extra cost of recovery: no second Dataflow job; one small update per minute on `prism_runs` while a run is alive.

## Pause / stop
`python config_admin.py disable PGW --by <you>` (runs start and exit at once), or pause the Scheduler entry.
