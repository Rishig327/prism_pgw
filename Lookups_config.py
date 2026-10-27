# =====================================================
# Lookup Configurations
# =====================================================
'''"subscriber_type": {
    "table": "subscriber_type",
    "key_fields": ["key1", "key2"],
    "value_fields": ["value1", "value2"],
    "match_type": "exact"
},

"sms_source": {
    "table": "sms_source",
    "key_fields": ["source_code"],
    "value_fields": ["source_name"],
    "match_type": "exact"
},


"operator_imsi": {
    "table": "operator_imsi",
    "key_fields": ["imsi_prefix"],
    "value_fields": ["operator_name"],
    "match_type": "prefix"
},

"dest_sccp": {
    "table": "dest_sccp",
    "key_fields": ["sccp_prefix", "traffic_type"],
    "value_fields": ["operator_name", "country_name"],
    "match_type": "prefix"
},

"channel_islands": {
    "table": "channel_islands",
    "key_fields": ["number_pattern", "country_code"],
    "value_fields": ["is_channel_island"],
    "match_type": "contains"
},

"data_direct": {
    "table": "data_direct",
    "key_fields": ["direction_code"],
    "value_fields": ["direction_name"],
    "match_type": "exact"
}
'''


LK_CONFIGS = {

    "lk_VFUK_C_UC_EVENT_TYPE": {
        "table": "VFUK_C_UC_EVENT_TYPE",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_SUBSCRIBER_TYPE": {
        "table": "VFUK_C_UC_SUBSCRIBER_TYPE",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_CALL_TYPE": {
        "table": "VFUK_C_UC_CALL_TYPE",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_SERVICE_TYPE": {
        "table": "VFUK_C_UC_SERVICE_TYPE",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },


    "lk_VFUK_C_UC_NETWORK_ID": {
        "table": "VFUK_C_UC_NETWORK_ID",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },


    "lk_VFUK_C_UC_MP_INFO": {
        "table": "VFUK_C_UC_MP_INFO",
        "key_fields": ["ENTITY"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },
    "lk_VFUK_C_UC_CALL_NO_GRP": {
        "table": "VFUK_C_UC_CALL_NO_GRP",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_FILTER_CODE": {
        "table": "VFUK_C_UC_FILTER_CODE",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_DURATION_STEP": {
        "table": "VFUK_C_UC_DURATION_STEP",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_PORTED": {
        "table": "VFUK_C_UC_PORTED",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_APN": {
        "table": "VFUK_C_UC_APN",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_EVENT_STATUS": {
        "table": "VFUK_C_UC_EVENT_STATUS",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_PARTNER": {
        "table": "VFUK_C_UC_PARTNER",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_C_UC_SUBSCRIBER_STATUS": {
        "table": "VFUK_C_UC_SUBSCRIBER_STATUS",
        "key_fields": ["NAME"],
        "value_fields": ["VALUE"],
        "match_type": "exact"
    },

    "lk_VFUK_R_UC_XGSN_IP_ADDRESS": {
        "table": "VFUK_R_UC_XGSN_IP_ADDRESS",
        "key_fields": ["IP"],
        "value_fields": ["VALUE"],
        "match_type": "prefix"
    },

    "lk_VFUK_R_UC_APN_LIST": {
    "table": "VFUK_R_UC_APN_LIST",
    "key_fields": ["APN","TYPE","NETWORK"],
    "value_fields": ["VALUE"],
    "match_type": "exact"
    },
    
    "lk_VFUK_R_UC_MVNO_IMSI": {
    "table": "VFUK_R_UC_MVNO_IMSI",
    "key_fields": ["IMSI_PREFIX"],
    "value_fields": ["MVNO"],
    "match_type": "prefix"
    }


    
}