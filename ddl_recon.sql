-- =====================================================================
-- PRISM reconciliation tables (VPDB_ADM). Run once. Same CMEK key as the other control tables.
-- Written by recon_runner.py. Nothing here is touched by the ingestion pipeline.
-- =====================================================================

-- Aggregate recon result: one row per window x dimension slice x measure (long format).
-- Rebuilt per window by one MERGE, so re-running is always safe.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.recon_result` (
  recon_id     STRING    NOT NULL,
  window_start TIMESTAMP NOT NULL,   -- start of the HOUR or DAY window
  dim_key      STRING    NOT NULL,   -- JSON of the dimension values, e.g. {"NE_ID":"x","APN":"y"}
  measure      STRING    NOT NULL,
  a_value      FLOAT64,
  b_value      FLOAT64,
  diff         FLOAT64,              -- B minus A
  diff_pct     FLOAT64,              -- |diff| / larger side * 100
  a_rows       INT64,                -- detail rows behind side A (NULL = side A has no rows)
  b_rows       INT64,
  status       STRING,               -- MATCH | MISMATCH | MISSING_IN_A | MISSING_IN_B
  provisional  BOOL,                 -- computed while recent files were still in flight
  run_id       STRING,
  computed_at  TIMESTAMP
)
PARTITION BY DATE(window_start)
CLUSTER BY recon_id, status
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Alarms: one row per breached window and recon. State: PENDING -> OPEN -> RESOLVED,
-- plus ACKNOWLEDGED and SUPPRESSED set by people.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.recon_alarm` (
  alarm_id         STRING    NOT NULL,
  recon_id         STRING    NOT NULL,
  window_start     TIMESTAMP NOT NULL,
  state            STRING    NOT NULL,
  severity         STRING,           -- LOW | MEDIUM | HIGH
  breach_runs      INT64,            -- consecutive runs the breach was seen
  first_raised_at  TIMESTAMP,
  last_seen_at     TIMESTAMP,
  resolved_at      TIMESTAMP,
  acknowledged_by  STRING,
  acknowledged_at  TIMESTAMP,
  note             STRING,
  summary          STRING,           -- JSON: counts and the 5 worst rows
  notified_at      TIMESTAMP
)
PARTITION BY DATE(window_start)
CLUSTER BY recon_id, state
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Manual detailed recon: who asked, for which alarm, what it cost, how it ended.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.recon_detail_request` (
  request_id      STRING NOT NULL,
  alarm_id        STRING,
  recon_id        STRING,
  requested_by    STRING,
  requested_at    TIMESTAMP,
  finished_at     TIMESTAMP,
  status          STRING,            -- RUNNING | DONE | FAILED | REFUSED (cost guard)
  dims_filter     STRING,
  bytes_estimated INT64,
  error           STRING
)
PARTITION BY DATE(requested_at)
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Detailed recon output: counts per category (MATCHED, ONLY_IN_A, ONLY_IN_B, VALUE_DIFF) and per file,
-- plus CONTEXT_* rows = file health (failed / quarantined / pending / duplicate) around the window.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.recon_detail_summary` (
  request_id STRING NOT NULL, category STRING, a_file_id STRING, b_file_id STRING,
  row_count INT64, created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY request_id
OPTIONS (partition_expiration_days = 90, kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Sample of differing records (up to detail.sample_rows per category).
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.recon_detail_sample` (
  request_id STRING NOT NULL, category STRING, record_key STRING, a_file_id STRING, b_file_id STRING,
  a_values STRING, b_values STRING, created_at TIMESTAMP
)
PARTITION BY DATE(created_at)
CLUSTER BY request_id
OPTIONS (partition_expiration_days = 90, kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

CREATE OR REPLACE VIEW `vf-uk-raid-pre-prod.VPDB_ADM.v_recon_open_alarms` AS
SELECT alarm_id, recon_id, window_start, state, severity, breach_runs, first_raised_at, last_seen_at,
       acknowledged_by, note, summary
FROM `vf-uk-raid-pre-prod.VPDB_ADM.recon_alarm`
WHERE state IN ('PENDING', 'OPEN', 'ACKNOWLEDGED')
ORDER BY window_start DESC;

-- Handy checks ---------------------------------------------------------
-- Latest result of one recon, mismatches only:
-- SELECT * FROM `vf-uk-raid-pre-prod.VPDB_ADM.recon_result` WHERE recon_id = 'X' AND status != 'MATCH'
-- ORDER BY window_start DESC, ABS(diff) DESC LIMIT 100;
-- Is a result stale? Compare a_rows / b_rows with today's detail count for that window.
