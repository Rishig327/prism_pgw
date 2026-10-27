"""
prism_settings.py - the ONLY settings the Dataflow workers need.

Imported by LookupsManager.py and beam_ber_asn_decoder.py on the workers, so it is listed in
setup.py and the Dockerfile. It must NOT import config.py or file_tracker.py (those are
orchestrator-only and are not shipped to the workers).
config.py (orchestrator) imports its project/dataset/key from here, so each value lives once.
"""
my_project = "vf-uk-raid-pre-prod"
my_dataset = "VPDB_DAT"



bq_kms_key_name = (
    "projects/vf-uk-raid-prd-kms/"
    "locations/europe-west2/"
    "keyRings/vf-uk-raid-pre-prod-kms-ring/"
    "cryptoKeys/vf-uk-raid-pre-prod-kms-key"
)
