#!/usr/bin/env bash

# need to install GCP dependencies needed to run using GCP DataflowRunner
pip install apache_beam[gcp]

# specify the pipeline dependencies to be installed on the Dataflow workers
# https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/
echo "beam-nuggets" > /tmp/requirements.txt

python write_to_relational_db.py \
    --runner DataflowRunner \
    --project try-dataflow-python \
    --temp_location gs://mohaseeb-bucket/tmp/ \
    --requirements_file /tmp/requirements.txt \
    --drivername postgresql+pg8000 \
    --host 10.38.96.3 \
    --port 5432 \
    --database calendar \
    --username postgres \
    --password postgres \
    --create_if_missing True


# Connect using cloud shell

# create new postgres instance
# enable private IP (keep the public)
# connect create_client_certificate
