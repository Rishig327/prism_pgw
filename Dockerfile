FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /template
COPY requirements_worker.txt setup.py dataflow_pipeline.py prism_plugin_base.py pgw_plugin.py pgw_schema.py \
     beam_ber_asn_decoder.py decode_functions.py transformation_PGW.py \
     LookupsManager.py Lookups_config.py prism_settings.py /template/

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/dataflow_pipeline.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=/template/setup.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=/template/requirements_worker.txt

RUN pip install --no-cache-dir -r /template/requirements_worker.txt
