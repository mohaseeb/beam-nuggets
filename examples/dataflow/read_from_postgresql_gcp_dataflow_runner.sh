#!/usr/bin/env bash

# need to install GCP dependencies needed to run using GCP DataflowRunner
pip install apache_beam[gcp]

# specify the pipeline dependencies to be installed on the GCP workers
# https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/
echo "beam-nuggets" > /tmp/requirements.txt

python read_from_relational_db.py \
    --runner DataflowRunner \
    --project try-dataflow-python \
    --region us-central1 \
    --temp_location gs://$GCP_BUCKET_NAME/tmp/ \
    --requirements_file /tmp/requirements.txt \
    --drivername postgresql+pg8000 \
    --host $DB_HOST \
    --port $DB_PORT \
    --database calendar3 \
    --username $DB_USER \
    --password $DB_PASSWORD \
    --table months
