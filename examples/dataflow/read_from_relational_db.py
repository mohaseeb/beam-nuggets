from __future__ import division, print_function

import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from beam_nuggets.io import relational_db


def main():
    # get the cmd args
    db_args, pipeline_args = get_args()

    # Create the pipeline
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=options) as p:
        source_config = relational_db.SourceConfiguration(
            drivername=db_args.drivername,
            host=db_args.host,
            port=db_args.port,
            database=db_args.database,
            username=db_args.username,
            password=db_args.password,
        )
        rrr = relational_db.Read(
            source_config=source_config,
            table_name=db_args.table
        )

        import ipdb; ipdb.set_trace()
        months = p | "Reading records from db" >> relational_db.Read(
            source_config=source_config,
            table_name=db_args.table
        )
        months | 'Writing to stdout' >> beam.Map(print)


def get_args():
    parser = argparse.ArgumentParser()
    # adding expected database args
    parser.add_argument('--drivername', dest='drivername', default='sqlite')
    parser.add_argument('--host', dest='host', default=None)
    parser.add_argument('--port', type=int, dest='port', default=None)
    parser.add_argument('--database', dest='database', default='/tmp/test.db')
    parser.add_argument('--username', dest='username', default=None)
    parser.add_argument('--password', dest='password', default=None)
    parser.add_argument('--table', dest='table', default="months")

    parsed_db_args, pipeline_args = parser.parse_known_args()

    return parsed_db_args, pipeline_args


if __name__ == '__main__':
    main()
