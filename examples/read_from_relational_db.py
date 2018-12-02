from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import ReadFromRelationalDB

with beam.Pipeline(options=PipelineOptions()) as p:
    students = p | "Reading students records" >> ReadFromRelationalDB(
        drivername='sqlite',
        database='/tmp/csv_to_sqlite_dummy.sqlite',
        table_name='students',
    )
    students | 'Writing to stdout' >> beam.Map(lambda r: print(r))
