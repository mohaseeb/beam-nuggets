from __future__ import division, print_function

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io.relational_db.relational_db_transform import (
    ReadFromRelationalDB,
)


def main():
    table_name = 'students'
    db_uri = "sqlite:////tmp/csv_to_sqlite_dummy.sqlite"  # TODO code to
    # populate it
    output = '/tmp/csv_to_sqlite_dummy_output.csv'
    with beam.Pipeline(options=PipelineOptions()) as p:
        students = p | "Reading students records" >> ReadFromRelationalDB(
            db_uri,
            table_name
        )
        students | 'Writing to output file' >> WriteToText(output)
