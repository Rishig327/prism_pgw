-- =====================================================================
-- PRISM / PGW - DDL, monitoring, aggregate rebuild and recon queries
-- Project vf-uk-raid-pre-prod, location europe-west2. Run section by section.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Control dataset (default CMEK so every table in it is encrypted)
-- ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM`
OPTIONS (location = 'europe-west2', default_kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- 1a. File registry: one row per file identity
--     file_id = SHA256(source_type | base_name | crc32c)  (stable after the file is moved)
--     status: DISCOVERED | IN_PROGRESS | COMPLETED | FAILED | QUARANTINED | DUPLICATE | SUPERSEDED
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.file_registry` (
  file_id            STRING    NOT NULL,
  source_type        STRING    NOT NULL,   -- PGW
  bucket             STRING    NOT NULL,
  object_name        STRING    NOT NULL,   -- full path in input/ at discovery
  base_name          STRING    NOT NULL,
  current_path       STRING,               -- where the file lives now
  generation         INT64,
  size_bytes         INT64,
  crc32c             STRING    NOT NULL,   -- from GCS metadata
  gcs_updated_at     TIMESTAMP,
  status             STRING    NOT NULL,
  attempt_count      INT64     NOT NULL,
  claimed_by         STRING,               -- run_id holding the lease
  lease_expiry       TIMESTAMP,
  records_read       INT64,
  records_loaded     INT64,
  records_rejected   INT64,
  last_error         STRING,
  supersedes_file_id STRING,
  discovered_at      TIMESTAMP NOT NULL,
  started_at         TIMESTAMP,
  completed_at       TIMESTAMP,
  moved_at           TIMESTAMP,
  updated_at         TIMESTAMP
)
PARTITION BY DATE(discovered_at)
CLUSTER BY source_type, status, file_id;

-- 1b. Append-only attempt history
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.file_processing_log` (
  file_id STRING NOT NULL, attempt_no INT64, run_id STRING, event STRING,
  event_ts TIMESTAMP, records_count INT64, message STRING
)
PARTITION BY DATE(event_ts) CLUSTER BY file_id;

-- 1c. Rejected records written by Dataflow (record_offset = -1 means whole-file error)
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.file_reject_log` (
  file_id STRING, attempt_no INT64, record_offset INT64, reason STRING,
  raw_payload BYTES, created_at TIMESTAMP
)
PARTITION BY DATE(created_at) CLUSTER BY file_id;

-- 1d. Records read per file/attempt, written by Dataflow
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.file_read_counts` (
  file_id STRING NOT NULL, attempt_no INT64 NOT NULL, records_read INT64 NOT NULL,
  created_at TIMESTAMP
)
PARTITION BY DATE(created_at) CLUSTER BY file_id;

-- ---------------------------------------------------------------------
-- 2. PGW detail table (NEW - your repo wrote detail as CSV files, not BigQuery)
--    Columns come from pgw_schema.py. file_id lets us delete/reload one file cleanly.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW` (
  STARTTIME                 STRING,
  ENDTIME                   STRING,
  MES_EVENT_DURATION        FLOAT64,
  SERVED_MSISDN             STRING,
  APN                       STRING,
  CALL_TYPE                 STRING,
  EVENT_TYPE                STRING,
  SUBSCRIBER_TYPE           STRING,
  MES_CHARGED_AMOUNT        FLOAT64,
  CHARGING_CHARACTERISTICS  STRING,
  RATING_GROUP              STRING,
  RATTYPE                   STRING,
  MES_UPLINK_VOLUME         INT64,
  MES_DOWNLINK_VOLUME       INT64,
  SGSN_ADDRESS              STRING,
  GGSN_ADDRESS              STRING,
  SERVED_IMSI               STRING,
  FILTER_CODE               STRING,
  SESSION_ID                STRING,
  SERVICE_TYPE              STRING,
  FILENAME                  STRING,
  ACCESS_POINT_NAME_NI      STRING,
  PROC_DATE                 DATE,
  PROC_HOUR                 STRING,
  NE_ID                     STRING,
  MES_SEQ_NUM               STRING,
  MES_CDR_COUNT             FLOAT64,
  MES_EVENT_COUNT           FLOAT64,
  TRAFF_CAT_900             STRING,
  NETWORK_IN                STRING,
  NETWORK_OUT               STRING,
  EVT_DATE                  DATE,
  DIM_END_DATE              STRING,
  DIM_EVENT_TYPE_ID         INT64,
  DIM_CALL_TYPE_ID          INT64,
  DIM_SUBSCRIBER_TYPE_ID    INT64,
  DIM_SERVICE_TYPE_ID       INT64,
  DIM_NETWORK_IN_ID         INT64,
  DIM_NETWORK_OUT_ID        INT64,
  DIM_FILTER_CODE_ID        INT64,
  DIM_PORTED_ID             INT64,
  DIM_APN                   INT64,
  DIM_OWNER                 INT64,
  DIM_SUBSLOC               INT64,
  DIM_ENTITY                INT64,
  MES_DATA_VOLUME           INT64,
  FIELD1                    FLOAT64,
  FIELD2                    FLOAT64,
  FIELD3                    FLOAT64,
  FIELD4                    FLOAT64,
  FIELD5                    FLOAT64,
  FIELD6                    FLOAT64,
  FIELD7                    FLOAT64,
  FIELD8                    FLOAT64,
  file_id                   STRING,
  loaded_at                 TIMESTAMP
)
PARTITION BY EVT_DATE
CLUSTER BY file_id, DIM_END_DATE
OPTIONS (kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- 2b. Staging: SAME columns, same order, attempt_no LAST. Dataflow loads here (batch load jobs).
--     Rows older than 7 days expire automatically.
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_DAT.TMP_VP_T_EVENTS_DETAIL_PGW_STG`
PARTITION BY DATE(loaded_at)
CLUSTER BY file_id
OPTIONS (partition_expiration_days = 7, kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key')
AS
SELECT *, CAST(NULL AS INT64) AS attempt_no
FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW`
WHERE FALSE;

-- ---------------------------------------------------------------------
-- 3. Monitoring views
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `vf-uk-raid-pre-prod.VPDB_ADM.v_file_status_daily` AS
SELECT DATE(discovered_at) AS day, source_type, status, COUNT(*) AS files,
       SUM(records_loaded) AS records_loaded, SUM(records_rejected) AS records_rejected
FROM `vf-uk-raid-pre-prod.VPDB_ADM.file_registry`
GROUP BY day, source_type, status;

CREATE OR REPLACE VIEW `vf-uk-raid-pre-prod.VPDB_ADM.v_stuck_files` AS
SELECT file_id, source_type, object_name, status, attempt_count, claimed_by, lease_expiry,
       last_error, discovered_at, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), discovered_at, HOUR) AS age_hours
FROM `vf-uk-raid-pre-prod.VPDB_ADM.file_registry`
WHERE status IN ('DISCOVERED','FAILED','QUARANTINED')
   OR (status = 'IN_PROGRESS' AND lease_expiry < CURRENT_TIMESTAMP());

-- ---------------------------------------------------------------------
-- 4. AGGREGATE - built LATER from detail (not in the pipeline).
--    VP_T_EVENTS_READINESS is shared with other sources, so rebuild only THIS source:
--    delete + re-insert for the touched EVT_DATEs and this source's DIM_ENTITY.
--    DIM_ENTITY for PGW = lookup id of 'M100_SWITCHES_PGW' (see query 4a).
-- ---------------------------------------------------------------------
-- 4a. entity id used by PGW rows
-- SELECT DISTINCT DIM_ENTITY FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW`;

-- 4b. dates touched by files completed since @since
-- SELECT DISTINCT d.EVT_DATE
-- FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW` d
-- JOIN `vf-uk-raid-pre-prod.VPDB_ADM.file_registry` r USING (file_id)
-- WHERE r.source_type = 'PGW' AND r.completed_at >= @since;

-- 4c. rebuild (params: @dates ARRAY<DATE>, @entity INT64). Safe to re-run any time.
BEGIN TRANSACTION;

DELETE FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_READINESS`
WHERE EVT_DATE IN UNNEST(@dates) AND DIM_ENTITY = @entity;

INSERT INTO `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_READINESS`
  (EVT_DATE,
  DIM_END_DATE,
  DIM_EVENT_TYPE_ID,
  DIM_CALL_TYPE_ID,
  DIM_SUBSCRIBER_TYPE_ID,
  DIM_SERVICE_TYPE_ID,
  DIM_NETWORK_IN_ID,
  DIM_NETWORK_OUT_ID,
  DIM_FILTER_CODE_ID,
  DIM_PORTED_ID,
  DIM_APN,
  DIM_OWNER,
  DIM_SUBSLOC,
  DIM_ENTITY,
  MES_EVENT_COUNT,
  MES_EVENT_DURATION,
  MES_DATA_VOLUME,
  MES_DOWNLINK_VOLUME,
  MES_UPLINK_VOLUME,
  MES_CHARGED_AMOUNT,
  MES_CDR_COUNT,
  FIELD1,
  FIELD2,
  FIELD3,
  FIELD4,
  FIELD5,
  FIELD6,
  FIELD7,
  FIELD8)
SELECT
  EVT_DATE,
  DIM_END_DATE,
  DIM_EVENT_TYPE_ID,
  DIM_CALL_TYPE_ID,
  DIM_SUBSCRIBER_TYPE_ID,
  DIM_SERVICE_TYPE_ID,
  DIM_NETWORK_IN_ID,
  DIM_NETWORK_OUT_ID,
  DIM_FILTER_CODE_ID,
  DIM_PORTED_ID,
  DIM_APN,
  DIM_OWNER,
  DIM_SUBSLOC,
  DIM_ENTITY,
  SUM(MES_EVENT_COUNT) AS MES_EVENT_COUNT,
  SUM(MES_EVENT_DURATION) AS MES_EVENT_DURATION,
  SUM(MES_DATA_VOLUME) AS MES_DATA_VOLUME,
  SUM(MES_DOWNLINK_VOLUME) AS MES_DOWNLINK_VOLUME,
  SUM(MES_UPLINK_VOLUME) AS MES_UPLINK_VOLUME,
  SUM(MES_CHARGED_AMOUNT) AS MES_CHARGED_AMOUNT,
  SUM(MES_CDR_COUNT) AS MES_CDR_COUNT,
  SUM(FIELD1) AS FIELD1,
  SUM(FIELD2) AS FIELD2,
  SUM(FIELD3) AS FIELD3,
  SUM(FIELD4) AS FIELD4,
  SUM(FIELD5) AS FIELD5,
  SUM(FIELD6) AS FIELD6,
  SUM(FIELD7) AS FIELD7,
  SUM(FIELD8) AS FIELD8
FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW`
WHERE EVT_DATE IN UNNEST(@dates) AND DIM_ENTITY = @entity
GROUP BY
  EVT_DATE,
  DIM_END_DATE,
  DIM_EVENT_TYPE_ID,
  DIM_CALL_TYPE_ID,
  DIM_SUBSCRIBER_TYPE_ID,
  DIM_SERVICE_TYPE_ID,
  DIM_NETWORK_IN_ID,
  DIM_NETWORK_OUT_ID,
  DIM_FILTER_CODE_ID,
  DIM_PORTED_ID,
  DIM_APN,
  DIM_OWNER,
  DIM_SUBSLOC,
  DIM_ENTITY;

COMMIT TRANSACTION;

-- ---------------------------------------------------------------------
-- 5. RECON
-- ---------------------------------------------------------------------
-- 5a. read vs loaded + rejected, per file (must return 0 rows)
SELECT file_id, object_name, records_read, records_loaded, records_rejected
FROM `vf-uk-raid-pre-prod.VPDB_ADM.file_registry`
WHERE source_type='PGW' AND status='COMPLETED'
  AND records_read != records_loaded + records_rejected;

-- 5b. registry vs detail rows, per file (must return 0 rows)
SELECT r.file_id, r.object_name, r.records_loaded, IFNULL(d.cnt,0) AS detail_rows
FROM `vf-uk-raid-pre-prod.VPDB_ADM.file_registry` r
LEFT JOIN (SELECT file_id, COUNT(*) AS cnt FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW` GROUP BY file_id) d
  USING (file_id)
WHERE r.source_type='PGW' AND r.status='COMPLETED' AND IFNULL(d.cnt,0) != r.records_loaded;

-- 5c. detail vs aggregate per EVT_DATE (rows and volume must match)
SELECT d.EVT_DATE, d.detail_rows, a.agg_cdr_count, d.detail_volume, a.agg_volume
FROM (SELECT EVT_DATE, COUNT(*) AS detail_rows, SUM(MES_DATA_VOLUME) AS detail_volume
      FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_DETAIL_PGW` WHERE DIM_ENTITY = @entity GROUP BY EVT_DATE) d
LEFT JOIN (SELECT EVT_DATE, SUM(MES_CDR_COUNT) AS agg_cdr_count, SUM(MES_DATA_VOLUME) AS agg_volume
           FROM `vf-uk-raid-pre-prod.VPDB_DAT.VP_T_EVENTS_READINESS` WHERE DIM_ENTITY = @entity GROUP BY EVT_DATE) a
  USING (EVT_DATE)
WHERE d.detail_rows != IFNULL(a.agg_cdr_count,0) OR d.detail_volume != IFNULL(a.agg_volume,0);

-- ---------------------------------------------------------------------
-- 6. Ops
-- ---------------------------------------------------------------------
-- Retry a quarantined file
-- UPDATE `vf-uk-raid-pre-prod.VPDB_ADM.file_registry`
-- SET status='DISCOVERED', attempt_count=0, claimed_by=NULL, lease_expiry=NULL,
--     last_error=NULL, updated_at=CURRENT_TIMESTAMP()
-- WHERE file_id='<file_id>' AND status='QUARANTINED';
-- (copy the file back into the input folder first)

-- Force-release a stuck lease
-- UPDATE `vf-uk-raid-pre-prod.VPDB_ADM.file_registry`
-- SET lease_expiry=TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
-- WHERE file_id='<file_id>' AND status='IN_PROGRESS';


-- =====================================================================
-- 8. RUN TRACKING (flex / Cloud Run mode only; VM test mode does not write here)
--    One row per orchestrator run. Used to (a) stand down when an earlier run of the same source is alive,
--    (b) find runs that died and finish their Dataflow job instead of launching a second one.
--    status: STARTING | CLAIMED | LAUNCHED | FINALISING | DONE | FAILED | ABANDONED | ORPHANED | ADOPTED
-- =====================================================================
CREATE TABLE IF NOT EXISTS `vf-uk-raid-pre-prod.VPDB_ADM.prism_runs` (
  run_id        STRING    NOT NULL,
  source_type   STRING    NOT NULL,
  status        STRING    NOT NULL,
  job_id        STRING,               -- Dataflow job id once launched
  manifest_uri  STRING,
  adopted_by    STRING,               -- run that is finishing this one after it died
  started_at    TIMESTAMP NOT NULL,
  heartbeat_at  TIMESTAMP,            -- refreshed about once a minute while the run is alive
  finished_at   TIMESTAMP,
  note          STRING,
  config_version STRING               -- which configuration version this run used
)
PARTITION BY DATE(started_at)
CLUSTER BY source_type, status
OPTIONS (partition_expiration_days = 90, kms_key_name = 'projects/vf-uk-raid-prd-kms/locations/europe-west2/keyRings/vf-uk-raid-pre-prod-kms-ring/cryptoKeys/vf-uk-raid-pre-prod-kms-key');

-- Runs that look dead right now (adjust the minutes to orphan_after_min):
-- SELECT * FROM `vf-uk-raid-pre-prod.VPDB_ADM.prism_runs`
-- WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
--   AND (status = 'ORPHANED' OR (status IN ('STARTING','CLAIMED','LAUNCHED','FINALISING','ADOPTED')
--        AND heartbeat_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE)));
