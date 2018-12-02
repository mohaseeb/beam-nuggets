from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import ReadFromCsv


def main():
    csv_path = get_csv_file()
    with beam.Pipeline(options=PipelineOptions()) as p:
        students = p | "Reading students records" >> ReadFromCsv(csv_path)
        students | 'Writing to stdout' >> beam.Map(lambda r: print(r))


def get_csv_file():
    csv_file_path = '/tmp/csv_to_sqlite_dummy.csv'
    with open(csv_file_path, 'wb') as csv_file:
        import csv
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(['firstName', 'lastName', 'level'])
        csv_writer.writerow(['Andrel', 'Norvell', 3])
        csv_writer.writerow(['Dinorah', 'Proudfoot', 8])
        csv_writer.writerow(['Trulal', 'Plotkin', 14])
    return csv_file_path
