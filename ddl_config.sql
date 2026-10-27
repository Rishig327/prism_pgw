-- =====================================================================
-- PRISM configuration tables (VPDB_ADM). Append-only: a change inserts a NEW row, the newest row wins.
-- Written by config_admin.py (set / seed / rollback). Read at the start of every run.
-- Give EDIT rights on these three tables to a few people only; everyone else read-only.
-- =====================================================================
-- Global settings (every column required)
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.prism_config_global` (
  changed_at TIMESTAMP NOT NULL, changed_by STRING, note STRING,
  max_attempts INT64 NOT NULL, batch_files INT64 NOT NULL, lease_min INT64 NOT NULL, max_job_wait_min INT64 NOT NULL
)
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- One row per source and version. Every column is required (NOT NULL) except the df_* Dataflow options:
-- an empty df_* option is not passed and Dataflow's own default applies.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.prism_config_source` (
  source_type STRING NOT NULL, changed_at TIMESTAMP NOT NULL, changed_by STRING, note STRING,
  enabled BOOL NOT NULL,
  input_prefix STRING NOT NULL, processed_prefix STRING NOT NULL, duplicate_prefix STRING NOT NULL, quarantine_prefix STRING NOT NULL,
  name_glob STRING NOT NULL, conflict_policy STRING NOT NULL, lookup_window_days INT64 NOT NULL, min_age_seconds INT64 NOT NULL,
  ignore_suffixes STRING NOT NULL,       -- comma separated, e.g. '.tmp,.part'; an EMPTY string means no suffixes ignored
  detail_table STRING NOT NULL, staging_table STRING NOT NULL, reject_table STRING NOT NULL, read_counts_table STRING NOT NULL,
  flex_template_gcs_path STRING NOT NULL, manifest_prefix STRING NOT NULL,
  max_reject_ratio FLOAT64 NOT NULL, allow_empty_files BOOL NOT NULL, skip_if_running BOOL NOT NULL, orphan_after_min INT64 NOT NULL,
  -- Dataflow options: optional. Empty = not passed, Dataflow's own default applies.
  df_temp_location STRING, df_staging_location STRING, df_service_account STRING, df_network STRING,
  df_subnetwork STRING, df_max_workers INT64, df_machine_type STRING
)
CLUSTER BY source_type
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Recon definitions: the definition JSON, one row per recon and version.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.prism_recon_definition` (
  recon_id STRING NOT NULL, changed_at TIMESTAMP NOT NULL, changed_by STRING, note STRING,
  enabled BOOL NOT NULL, definition STRING NOT NULL
)
CLUSTER BY recon_id
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- NOTE: BigQuery cannot add NOT NULL to a column of an existing table. If you already created these tables,
-- drop and recreate them (they only hold seeded values) or the code check alone enforces "no blanks".

-- Convenience: current version of each source.
CREATE OR REPLACE VIEW `vf-uk-raid-pre-prod.VPDB_ADM.v_prism_config_source_current` AS
SELECT * FROM `vf-uk-raid-pre-prod.VPDB_ADM.prism_config_source` WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_type ORDER BY changed_at DESC) = 1;

-- prism_runs: record which configuration version each run used (existing table: add the column once)
ALTER TABLE `vf-uk-raid-pre-prod.VPDB_ADM.prism_runs` ADD COLUMN IF NOT EXISTS config_version STRING;
