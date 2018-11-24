from __future__ import division, print_function

import csv

import apache_beam as beam
from apache_beam.io import WriteToText, Read
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.csv_reader import CsvSource


def main():
    input_ = get_csv_file()
    output_ = '/tmp/csv_to_sqlite_dummy_input.csv'
    with beam.Pipeline(options=PipelineOptions()) as p:
        lines = p | "Reading csv source" >> Read(CsvSource(input_))
        lines | 'Writing to Sqlite table' >> WriteToText(output_)


def get_csv_file():
    csv_file_path = '/tmp/csv_to_sqlite_dummy_input.csv'
    with open(csv_file_path, 'wb') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow(['firstName', 'lastName', 'level'])
        csv_writer.writerow(['Andre', 'Norvell', 3])
        csv_writer.writerow(['Dinorah', 'Proudfoot', 8])
        csv_writer.writerow(['Trula', 'Plotkin', 4])
    return csv_file_path
