# lookups.py

import logging
from google.cloud import bigquery

from prism_settings import my_project, my_dataset, bq_kms_key_name

from Lookups_config import LK_CONFIGS


logger = logging.getLogger("LookupManager")


class LookupManager:
    """
    Generic Lookup Manager for BigQuery lookups.

    Supports:
    - load_all()
    - load_selected([...])
    - load_lookup("lookup_name")
    - exact match
    - prefix match
    - contains match
    - one key or multiple keys
    - one value or multiple return values

    For prefix/contains:
    - first key is used for prefix/contains match
    - remaining keys are exact match
    """

    # =====================================================
    # Storage
    # =====================================================

    lookups = {}

    loaded_lookups = set()

    _loaded_all = False

    # Set True for local testing without BigQuery
    LOCAL_MODE = False

    # =====================================================
    # Lookup Configurations
    # =====================================================

    LOOKUP_CONFIGS = LK_CONFIGS

    # =====================================================
    # Load All Lookups
    # =====================================================

    @classmethod
    def load_all(cls):
        """
        Loads all lookup tables defined in LOOKUP_CONFIGS.
        """

        if cls._loaded_all:
            logger.info("All lookups already loaded. Skipping reload.")
            return

        logger.info("Loading all lookups...")

        for lookup_name in cls.LOOKUP_CONFIGS.keys():
            cls.load_lookup(lookup_name)

        cls._loaded_all = True

        logger.info(
            "All lookups loaded successfully. counts=%s",
            cls.lookup_counts()
        )

    # =====================================================
    # Load Selected Lookups
    # =====================================================

    @classmethod
    def load_selected(cls, lookup_names):
        """
        Loads only selected lookup tables.

        Example:
            LookupManager.load_selected([
                "subscriber_type",
                "operator_imsi",
                "dest_sccp"
            ])
        """

        if not lookup_names:
            logger.warning("No lookup names provided to load_selected.")
            return

        logger.info("Loading selected lookups: %s", lookup_names)

        for lookup_name in lookup_names:
            cls.load_lookup(lookup_name)

        logger.info(
            "Selected lookups loaded. counts=%s",
            cls.lookup_counts()
        )

    # =====================================================
    # Load Single Lookup
    # =====================================================

    @classmethod
    def load_lookup(cls, lookup_name):
        """
        Loads one lookup table only once.

        If lookup already loaded, it skips reload.
        """

        if lookup_name in cls.loaded_lookups:
            logger.info("Lookup already loaded. lookup=%s", lookup_name)
            return

        config = cls.LOOKUP_CONFIGS.get(lookup_name)

        if not config:
            raise ValueError(f"Lookup config not found for lookup_name={lookup_name}")

        if cls.LOCAL_MODE:
            cls._load_sample_lookup(lookup_name)
        else:
            client = bigquery.Client(project=my_project)
            cls._load_lookup_from_bigquery(client, lookup_name, config)

        cls.loaded_lookups.add(lookup_name)

        logger.info("Lookup loaded successfully. lookup=%s", lookup_name)

    # =====================================================
    # BigQuery Loader
    # =====================================================

    @classmethod
    def _load_lookup_from_bigquery(cls, client, lookup_name, config):
        """
        Loads one lookup from BigQuery based on config.
        """

        table_name = config["table"]
        key_fields = config["key_fields"]
        value_fields = config["value_fields"]
        match_type = config.get("match_type", "exact")

        all_fields = key_fields + value_fields
        select_cols = ", ".join(all_fields)

        query = f"""
            SELECT
                {select_cols}
            FROM `{my_project}.{my_dataset}.{table_name}`
        """

        logger.info(
            "Loading lookup from BigQuery. lookup=%s table=%s",
            lookup_name,
            table_name
        )

        job_config = bigquery.QueryJobConfig(destination_encryption_configuration=bigquery.EncryptionConfiguration(kms_key_name=bq_kms_key_name   ))

        #rows = client.query(query).result()
        
        rows = client.query(query,job_config=job_config).result()

        data = {}

        for row in rows:
            key = cls._build_key(row, key_fields)
            value = cls._build_value(row, value_fields)

            data[key] = value

        cls.lookups[lookup_name] = {
            "data": data,
            "key_fields": key_fields,
            "value_fields": value_fields,
            "match_type": match_type,
            "source": "bigquery"
        }

        logger.info(
            "BigQuery lookup loaded. lookup=%s count=%s match_type=%s",
            lookup_name,
            len(data),
            match_type
        )

    # =====================================================
    # Sample Loader for Local Testing
    # =====================================================

    @classmethod
    def _load_sample_lookup(cls, lookup_name):
        """
        Loads sample lookup data for local testing without BigQuery.
        """

        sample_data = cls._sample_data()

        if lookup_name not in sample_data:
            raise ValueError(f"Sample data not found for lookup_name={lookup_name}")

        cls.lookups[lookup_name] = sample_data[lookup_name]

        logger.info(
            "Sample lookup loaded. lookup=%s count=%s",
            lookup_name,
            len(sample_data[lookup_name]["data"])
        )

    @classmethod
    def _sample_data(cls):
        """
        Local sample data.
        Used only when LOCAL_MODE = True.
        """

        return {
            "subscriber_type": {
                "match_type": "exact",
                "key_fields": ["key1", "key2"],
                "value_fields": ["value1", "value2"],
                "source": "sample",
                "data": {
                    ("A", "X"): {
                        "value1": "PREPAID",
                        "value2": "100"
                    },
                    ("A", "Y"): {
                        "value1": "POSTPAID",
                        "value2": "200"
                    },
                    ("B", "X"): {
                        "value1": "CORPORATE",
                        "value2": "300"
                    }
                }
            },

            "sms_source": {
                "match_type": "exact",
                "key_fields": ["source_code"],
                "value_fields": ["source_name"],
                "source": "sample",
                "data": {
                    "01": {
                        "source_name": "SMSC_A"
                    },
                    "02": {
                        "source_name": "SMSC_B"
                    },
                    "03": {
                        "source_name": "SMSC_C"
                    }
                }
            },

            "operator_imsi": {
                "match_type": "prefix",
                "key_fields": ["imsi_prefix"],
                "value_fields": ["operator_name"],
                "source": "sample",
                "data": {
                    "23415": {
                        "operator_name": "Vodafone UK"
                    },
                    "23410": {
                        "operator_name": "O2 UK"
                    },
                    "40445": {
                        "operator_name": "Airtel India"
                    },
                    "40566": {
                        "operator_name": "Vodafone India"
                    }
                }
            },

            "dest_sccp": {
                "match_type": "prefix",
                "key_fields": ["sccp_prefix", "traffic_type"],
                "value_fields": ["operator_name", "country_name"],
                "source": "sample",
                "data": {
                    ("4478", "MOBILE"): {
                        "operator_name": "Vodafone",
                        "country_name": "UK"
                    },
                    ("4477", "MOBILE"): {
                        "operator_name": "O2",
                        "country_name": "UK"
                    },
                    ("9122", "FIXED"): {
                        "operator_name": "MTNL",
                        "country_name": "India"
                    }
                }
            },

            "channel_islands": {
                "match_type": "contains",
                "key_fields": ["number_pattern", "country_code"],
                "value_fields": ["is_channel_island"],
                "source": "sample",
                "data": {
                    ("9198", "UK"): {
                        "is_channel_island": "Y"
                    },
                    ("8899", "UK"): {
                        "is_channel_island": "Y"
                    }
                }
            },

            "data_direct": {
                "match_type": "exact",
                "key_fields": ["direction_code"],
                "value_fields": ["direction_name"],
                "source": "sample",
                "data": {
                    "0": {
                        "direction_name": "Incoming"
                    },
                    "1": {
                        "direction_name": "Outgoing"
                    }
                }
            }
        }

    # =====================================================
    # Generic Get Lookup
    # =====================================================

    @classmethod
    def get_lookup(cls, lookup_name, *keys, default=None):
        """
        Generic lookup method.

        If lookup is not loaded, it auto-loads only that lookup.

        Example exact 2 keys:
            LookupManager.get_lookup(
                "subscriber_type",
                "A",
                "X",
                default={}
            )

        Example prefix 1 key:
            LookupManager.get_lookup(
                "operator_imsi",
                "234159999999999",
                default={}
            )

        Example prefix multiple keys:
            LookupManager.get_lookup(
                "dest_sccp",
                "44781234567",
                "MOBILE",
                default={}
            )

        Example contains multiple keys:
            LookupManager.get_lookup(
                "channel_islands",
                "ABC9198XYZ",
                "UK",
                default={}
            )
        """

        if lookup_name not in cls.loaded_lookups:
            cls.load_lookup(lookup_name)

        lookup = cls.lookups.get(lookup_name)

        if not lookup:
            logger.warning("Lookup not found after load. lookup=%s", lookup_name)
            return default

        data = lookup["data"]
        match_type = lookup.get("match_type", "exact")

        clean_keys = tuple(cls._safe_str(k) for k in keys)

        if match_type == "exact":
            return cls._get_exact(data, clean_keys, default)

        if match_type == "prefix":
            return cls._get_prefix(data, clean_keys, default)

        if match_type == "contains":
            return cls._get_contains(data, clean_keys, default)

        logger.warning(
            "Unsupported match_type=%s for lookup=%s",
            match_type,
            lookup_name
        )

        return default

    # =====================================================
    # Exact Match
    # =====================================================

    @classmethod
    def _get_exact(cls, data, keys, default=None):
        """
        Exact match.

        One key:
            data["01"]

        Multiple keys:
            data[("A", "X")]
        """

        lookup_key = cls._normalize_input_key(keys)

        return data.get(lookup_key, default)

    # =====================================================
    # Prefix Match
    # =====================================================

    @classmethod
    def _get_prefix(cls, data, keys, default=None):
        """
        Prefix match.

        Supports one key or multiple keys.

        Rule:
        - first key is prefix field
        - remaining keys exact match

        Example:
            lookup key = ("4478", "MOBILE")
            input keys = ("44781234567", "MOBILE")

            "44781234567".startswith("4478")
            and "MOBILE" == "MOBILE"
        """

        if not keys:
            return default

        input_value = cls._safe_str(keys[0])

        if not input_value:
            return default

        best_match = default
        best_length = 0

        for lookup_key, lookup_value in data.items():

            if isinstance(lookup_key, tuple):
                prefix = cls._safe_str(lookup_key[0])
                remaining_lookup_keys = lookup_key[1:]
            else:
                prefix = cls._safe_str(lookup_key)
                remaining_lookup_keys = ()

            if not prefix:
                continue

            if not input_value.startswith(prefix):
                continue

            if not cls._remaining_keys_match(remaining_lookup_keys, keys[1:]):
                continue

            if len(prefix) > best_length:
                best_match = lookup_value
                best_length = len(prefix)

        return best_match

    # =====================================================
    # Contains Match
    # =====================================================

    @classmethod
    def _get_contains(cls, data, keys, default=None):
        """
        Contains match.

        Supports one key or multiple keys.

        Rule:
        - first key is contains pattern
        - remaining keys exact match

        Example:
            lookup key = ("9198", "UK")
            input keys = ("ABC9198XYZ", "UK")

            "9198" in "ABC9198XYZ"
            and "UK" == "UK"
        """

        if not keys:
            return default

        input_value = cls._safe_str(keys[0])

        if not input_value:
            return default

        for lookup_key, lookup_value in data.items():

            if isinstance(lookup_key, tuple):
                pattern = cls._safe_str(lookup_key[0])
                remaining_lookup_keys = lookup_key[1:]
            else:
                pattern = cls._safe_str(lookup_key)
                remaining_lookup_keys = ()

            if not pattern:
                continue

            if pattern not in input_value:
                continue

            if not cls._remaining_keys_match(remaining_lookup_keys, keys[1:]):
                continue

            return lookup_value

        return default

    # =====================================================
    # Helper Methods
    # =====================================================

    @classmethod
    def _remaining_keys_match(cls, lookup_remaining_keys, input_remaining_keys):
        """
        Checks remaining keys for prefix/contains match.

        For prefix/contains:
        - first key does string matching
        - remaining keys must exact match
        """

        if len(input_remaining_keys) < len(lookup_remaining_keys):
            return False

        for index, lookup_value in enumerate(lookup_remaining_keys):
            input_value = input_remaining_keys[index]

            if cls._safe_str(input_value) != cls._safe_str(lookup_value):
                return False

        return True

    @classmethod
    def _build_key(cls, row, key_fields):
        """
        Builds lookup key from BigQuery row.

        One key:
            "01"

        Multiple keys:
            ("A", "X")
        """

        values = tuple(
            cls._safe_str(getattr(row, field))
            for field in key_fields
        )

        if len(values) == 1:
            return values[0]

        return values

    @classmethod
    def _build_value(cls, row, value_fields):
        """
        Builds lookup return value as dictionary.

        Example:
            {
                "value1": "PREPAID",
                "value2": "100"
            }
        """

        return {
            field: cls._safe_str(getattr(row, field))
            for field in value_fields
        }

    @classmethod
    def _normalize_input_key(cls, keys):
        """
        Converts input keys into dictionary lookup key format.
        """

        if len(keys) == 1:
            return keys[0]

        return keys

    @staticmethod
    def _safe_str(value):
        """
        Converts value to clean string.
        """

        if value is None:
            return ""

        return str(value).strip()

    # =====================================================
    # Status / Debug Methods
    # =====================================================

    @classmethod
    def lookup_counts(cls):
        """
        Returns counts for loaded lookups only.
        """

        return {
            name: len(info["data"])
            for name, info in cls.lookups.items()
        }

    @classmethod
    def is_lookup_loaded(cls, lookup_name):
        """
        Checks whether one lookup is loaded.
        """

        return lookup_name in cls.loaded_lookups

    @classmethod
    def is_all_loaded(cls):
        """
        Checks whether load_all was called successfully.
        """

        return cls._loaded_all

    @classmethod
    def clear(cls):
        """
        Clears all lookup data.

        Useful for local testing.
        """

        cls.lookups = {}
        cls.loaded_lookups = set()
        cls._loaded_all = False

        logger.info("LookupManager cleared.")

# ======================================================
# Local testing
# ======================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    LookupManager.load_all()
    #LookupManager.load_selected(["subscriber_type", "operator_imsi", "dest_sccp"])
    #LookupManager.load_lookup("subscriber_type")

    print("Lookup counts:")
    print(LookupManager.lookup_counts())

    value = LookupManager.get_lookup(
        "subscriber_type",
        "A",
        "X",
        default={"value1": "UNKNOWN", "value2": "UNKNOWN"}
    )


    #print(LookupManager.is_all_loaded())
    

    '''value = LookupManager.get_lookup(
        "sms_source",
        "01",
        default={"source_name": "UNKNOWN"}
    )

    print("sms_source:", value)

    value = LookupManager.get_lookup(
        "operator_imsi",
        "234159999999999",
        default={"operator_name": "UNKNOWN"}
    )

    print("operator_imsi:", value)'''