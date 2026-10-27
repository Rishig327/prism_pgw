"""
pgw_schema.py - typed schema of the PGW detail table (pure python, no Beam).
Single source of truth: used by the plugin (type coercion) and by make_pgw_ddl.py.

Order matters: staging = detail columns in this order + attempt_no LAST.
"""
# (column, BigQuery type)
DETAIL_SCHEMA = [
    # ---- your OUTPUT_FIELDS (from the old config_PGW.py) ----
    ("STARTTIME", "STRING"), ("ENDTIME", "STRING"), ("MES_EVENT_DURATION", "FLOAT64"),
    ("SERVED_MSISDN", "STRING"), ("APN", "STRING"), ("CALL_TYPE", "STRING"),
    ("EVENT_TYPE", "STRING"), ("SUBSCRIBER_TYPE", "STRING"), ("MES_CHARGED_AMOUNT", "FLOAT64"),
    ("CHARGING_CHARACTERISTICS", "STRING"), ("RATING_GROUP", "STRING"), ("RATTYPE", "STRING"),
    ("MES_UPLINK_VOLUME", "INT64"), ("MES_DOWNLINK_VOLUME", "INT64"),
    ("SGSN_ADDRESS", "STRING"), ("GGSN_ADDRESS", "STRING"), ("SERVED_IMSI", "STRING"),
    ("FILTER_CODE", "STRING"), ("SESSION_ID", "STRING"), ("SERVICE_TYPE", "STRING"),
    ("FILENAME", "STRING"), ("ACCESS_POINT_NAME_NI", "STRING"), ("PROC_DATE", "DATE"),
    ("PROC_HOUR", "STRING"), ("NE_ID", "STRING"), ("MES_SEQ_NUM", "STRING"),
    ("MES_CDR_COUNT", "FLOAT64"), ("MES_EVENT_COUNT", "FLOAT64"), ("TRAFF_CAT_900", "STRING"),
    ("NETWORK_IN", "STRING"), ("NETWORK_OUT", "STRING"),
    # ---- needed so the aggregate can be built later from detail ----
    ("EVT_DATE", "DATE"), ("DIM_END_DATE", "STRING"),
    ("DIM_EVENT_TYPE_ID", "INT64"), ("DIM_CALL_TYPE_ID", "INT64"),
    ("DIM_SUBSCRIBER_TYPE_ID", "INT64"), ("DIM_SERVICE_TYPE_ID", "INT64"),
    ("DIM_NETWORK_IN_ID", "INT64"), ("DIM_NETWORK_OUT_ID", "INT64"),
    ("DIM_FILTER_CODE_ID", "INT64"), ("DIM_PORTED_ID", "INT64"), ("DIM_APN", "INT64"),
    ("DIM_OWNER", "INT64"), ("DIM_SUBSLOC", "INT64"), ("DIM_ENTITY", "INT64"),
    ("MES_DATA_VOLUME", "INT64"),
    ("FIELD1", "FLOAT64"), ("FIELD2", "FLOAT64"), ("FIELD3", "FLOAT64"), ("FIELD4", "FLOAT64"),
    ("FIELD5", "FLOAT64"), ("FIELD6", "FLOAT64"), ("FIELD7", "FLOAT64"), ("FIELD8", "FLOAT64"),
    # ---- added by PRISM file tracking ----
    ("file_id", "STRING"), ("loaded_at", "TIMESTAMP"),
]
