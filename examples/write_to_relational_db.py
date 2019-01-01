from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import (
    WriteToRelationalDB,
    SourceConfiguration,
    TableConfiguration
)

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    months | 'Writing to Sqlite table' >> WriteToRelationalDB(
        source_config=SourceConfiguration(
            drivername='sqlite',
            database='/tmp/months_db.sqlite',
            create_if_missing=True
        ),
        table_config=TableConfiguration(
            name='months',
            create_table_if_missing=True
        )
    )
