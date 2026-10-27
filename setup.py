from setuptools import setup

setup(
    name="prism_pgw",
    version="1.0.0",
    py_modules=[
        "dataflow_pipeline", "prism_plugin_base", "pgw_plugin", "pgw_schema",
        "beam_ber_asn_decoder", "decode_functions", "transformation_PGW",
        "LookupsManager", "Lookups_config", "prism_settings",
    ],
)
