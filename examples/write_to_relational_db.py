from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import WriteToRelationalDB

with beam.Pipeline(options=PipelineOptions()) as p:
    students = p | "Reading students records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    students | 'Writing to Sqlite table' >> WriteToRelationalDB(
        drivername='sqlite',
        database='/tmp/csv_to_sqlite_dummy22.sqlite',
        table_name='months',
        create_db_if_missing=True,
        create_table_if_missing=True,
    )
