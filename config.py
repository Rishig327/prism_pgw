"""
config.py - orchestrator settings (run_prism_batch.py). Runs on the VM only; never imported by
the Dataflow workers (they use prism_settings.py). Edit here.
"""
from dataclasses import dataclass, field

from file_tracker import SourceConfig, TargetConfig
from prism_settings import bq_kms_key_name, my_dataset, my_project

PROJECT = my_project
BUCKET = "vf-uk-raid-pre-prod-working"
LOCATION = "europe-west2"          # BigQuery + Dataflow location
CTL_DATASET = "VPDB_ADM"
DATA_DATASET = my_dataset

KMS_KEY = bq_kms_key_name

# The values below and in SOURCES are NOT used at run time: runs read VPDB_ADM.prism_config_* and the TABLE decides
# which sources exist. SOURCES is only the one-time input of `python config_admin.py seed` (add a source here, seed it).
# They are only copied into the tables ONCE by `python config_admin.py seed` (and give the settings objects
# their shape). Change settings with config_admin.py, not here.
MAX_ATTEMPTS = 3                   # then the file is QUARANTINED
BATCH_FILES = 200                  # max files per Dataflow job
LEASE_MIN = 180                    # must exceed expected Dataflow runtime (heartbeat extends it)
MAX_JOB_WAIT_MIN = 240


@dataclass
class SourceSettings:
    src: SourceConfig
    target: TargetConfig
    flex_template_gcs_path: str
    dataflow_env: dict = field(default_factory=dict)
    # "vm"   = run dataflow_pipeline.py from this machine with DataflowRunner (no Flex Template needed)
    # "flex" = launch the Flex Template through the Dataflow API
    launch_mode: str = "vm"
    manifest_prefix: str = "dataflow/_manifests/"
    kms_key_name: str = KMS_KEY
    reject_table: str = f"{PROJECT}.{CTL_DATASET}.file_reject_log"
    read_counts_table: str = f"{PROJECT}.{CTL_DATASET}.file_read_counts"
    max_reject_ratio: float = 0.05     # fail a file if rejected/read is above this
    # flex mode only (Cloud Run / no VM): a newer run of the same source stands down while an earlier one
    # is alive, and a run that died (no heartbeat for orphan_after_min) is finished by the next run.
    skip_if_running: bool = True
    orphan_after_min: int = 15
    allow_empty_files: bool = False    # read == 0 is a failure unless True


SOURCES = {
    "PGW": SourceSettings(
        src=SourceConfig(
            source_type="PGW",
            input_prefix="dataflow/PGW/input/",            # your INPUT_FILE_PATH
            processed_prefix="dataflow/PGW/done/",         # your PROCESSED_ARCHIVE_PATH
            duplicate_prefix="dataflow/PGW/duplicate/",
            quarantine_prefix="dataflow/PGW/quarantine/",
            name_glob="pgw*",                              # your codec/input pattern
            # Same file NAME, different content, original already COMPLETED:
            #   "quarantine" = quarantine the new file and raise an alert (default, a person decides)
            #   "supersede"  = load the new file and replace the old file's rows in one commit
            conflict_policy="quarantine",
            # Registry lookups at discovery only look at rows discovered in the last N days.
            # A file older than that is simply loaded (no other check). Keep N longer than any file
            # can wait in the input folder or be re-sent.
            lookup_window_days=30,
        ),
        target=TargetConfig(
            detail_table=f"{PROJECT}.{DATA_DATASET}.VP_T_EVENTS_DETAIL_PGW",
            staging_table=f"{PROJECT}.{DATA_DATASET}.TMP_VP_T_EVENTS_DETAIL_PGW_STG",
            agg_table=None,        # aggregate is NOT built in the pipeline (see ddl.sql section 4)
        ),
        flex_template_gcs_path=f"gs://{BUCKET}/dataflow/templates/prism_dataflow.json",
        dataflow_env={                                     # from your PIPELINE_CONFIG
            "tempLocation": f"gs://{BUCKET}/dataflow/temp/",
            "stagingLocation": f"gs://{BUCKET}/dataflow/staging/",   # Dataflow staging path
            "serviceAccountEmail": "567858485002-compute@developer.gserviceaccount.com",
            "network": "vf-uk-raid-pre-prod-vpc-1",
            "subnetwork": "regions/europe-west2/subnetworks/vf-uk-raid-pre-prod-app-subnet",
            "maxWorkers": 10,
            # No "kmsKeyName" here: your old pipeline ran without a Dataflow CMEK key. Adding it makes
            # Dataflow encrypt worker disks with that key, which needs extra KMS grants (see chat).
        },
        manifest_prefix="dataflow/PGW/_manifests/",
        # launch_mode: "vm" = run the pipeline from this machine (testing, no image needed)
        #              "flex" = Flex Template via the Dataflow API (Cloud Run / no VM)
        launch_mode="vm",
        skip_if_running=True,      # flex mode: do not start while an earlier run of this source is alive
        orphan_after_min=15,       # flex mode: a run silent for this long is finished by the next run
    ),
}
