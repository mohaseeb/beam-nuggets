from __future__ import division, print_function

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from beam_nuggets.io import relational_db


def main():
    # get the cmd args
    db_args, pipeline_args = get_args()

    # Target database instance
    source_config = relational_db.SourceConfiguration(
        drivername=db_args.drivername,
        host=db_args.host,
        port=db_args.port,
        database=db_args.database,
        username=db_args.username,
        password=db_args.password,
        create_if_missing=db_args.create_if_missing
    )

    # The data to be written
    records = [
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
        {'name': 'Mar', 'num': 3},
        {'name': 'Apr', 'num': 4},
        {'name': 'May', 'num': 5},
        {'name': 'Jun', 'num': 6},
    ]

    # Target database table
    table_config = relational_db.TableConfiguration(
        name='months',
        create_if_missing=True,  # automatically create the table if not there
        primary_key_columns=['num']  # and use 'num' column as a primary key
    )

    # Create the pipeline
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        months = p | "Reading records" >> beam.Create(records, reshuffle=False)
        months | 'Writing to DB' >> relational_db.Write(
            source_config=source_config,
            table_config=table_config
        )


def get_args():
    parser = argparse.ArgumentParser()
    # adding expected database args
    parser.add_argument('--drivername', dest='drivername', default='sqlite')
    parser.add_argument('--host', dest='host', default=None)
    parser.add_argument('--port', type=int, dest='port', default=None)
    parser.add_argument('--database', dest='database', default='/tmp/test.db')
    parser.add_argument('--username', dest='username', default=None)
    parser.add_argument('--password', dest='password', default=None)
    parser.add_argument(
        '--create_if_missing',
        type=bool,
        dest='create_if_missing',
        default=None
    )

    parsed_db_args, pipeline_args = parser.parse_known_args()

    return parsed_db_args, pipeline_args


if __name__ == '__main__':
    main()
