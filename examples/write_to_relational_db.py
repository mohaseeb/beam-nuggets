from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import WriteToRelationalDB, RelationalDBConfiguration

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    months | 'Writing to Sqlite table' >> WriteToRelationalDB(
        db_config=RelationalDBConfiguration(
            drivername='sqlite',
            database='/tmp/months_db.sqlite'
        ),
        table_name='months',
        create_db_if_missing=True,
        create_table_if_missing=True,
    )
