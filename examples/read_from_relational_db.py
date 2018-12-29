from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import ReadFromRelationalDB, RelationalDBConfiguration

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading records from db" >> ReadFromRelationalDB(
        db_config=RelationalDBConfiguration(
            drivername='sqlite',
            database='/tmp/months_db.sqlite'
        ),
        table_name='months'
    )
    months | 'Writing to stdout' >> beam.Map(print)
