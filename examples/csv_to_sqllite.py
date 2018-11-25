from __future__ import division, print_function

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io.csv_reader import CsvSource
from beam_nuggets.io import WriteToRelationalDB


def main():
    csv_path = get_csv_file()
    db_uri, table_name = get_db_with_student_table()
    with beam.Pipeline(options=PipelineOptions()) as p:
        students = p | "Reading students records" >> Read(CsvSource(csv_path))
        students | 'Writing to Sqlite table' >> WriteToRelationalDB(
            db_uri, table_name
        )


FIRST_NAME_FIELD = 'firstName'
LAST_NAME_FIELD = 'lastName'
LEVEL_FIELD = 'level'


def get_csv_file():
    csv_file_path = '/tmp/csv_to_sqlite_dummy.csv'
    import csv
    with open(csv_file_path, 'wb') as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=',')
        csv_writer.writerow([FIRST_NAME_FIELD, LAST_NAME_FIELD, LEVEL_FIELD])
        csv_writer.writerow(['Andrel', 'Norvell', 3])
        csv_writer.writerow(['Dinorah', 'Proudfoot', 8])
        csv_writer.writerow(['Trulal', 'Plotkin', 4])
    return csv_file_path


def get_db_with_student_table():
    table_name = 'students'
    db_uri = "sqlite:////tmp/csv_to_sqlite_dummy.sqlite"

    from sqlalchemy import (
        create_engine, MetaData, Table, Column, Integer, String
    )
    from sqlalchemy_utils import create_database

    # create DB
    create_database(db_uri)

    # create student table
    metadata = MetaData(create_engine(db_uri))
    student_table = Table(
        'students',
        metadata,
        Column(FIRST_NAME_FIELD, String, primary_key=True),
        Column(LAST_NAME_FIELD, String, primary_key=True),
        Column(LEVEL_FIELD, Integer)
    )
    metadata.create_all()

    return db_uri, table_name
