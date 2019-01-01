from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    months | 'Writing to Sqlite table' >> relational_db.Write(
        source_config=relational_db.SourceConfiguration(
            drivername='sqlite',
            database='/tmp/months_db.sqlite',
            create_if_missing=True
        ),
        table_config=relational_db.TableConfiguration(
            name='months',
            create_if_missing=True
        )
    )
