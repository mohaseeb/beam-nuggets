""":class:`~apache_beam.transforms.ptransform.PTransform` s for reading from
and writing to relational databases. SQLAlchemy_ is used for interfacing the
databases.

.. _SQLAlchemy: https://www.sqlalchemy.org/
"""

from __future__ import division, print_function

from apache_beam import PTransform, Create, ParDo, DoFn

from beam_nuggets.io.relational_db_api import (
    SqlAlchemyDB,
    SourceConfiguration,
    TableConfiguration
)

# It is intended SourceConfiguration and TableConfiguration are imported
# from this module by the library users. Below is to make sure they've been
# imported to this module.
assert SourceConfiguration is not None
assert TableConfiguration is not None


class Read(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    tables on relational databases.

    It outputs a :class:`~apache_beam.pvalue.PCollection` of
    ``dict:s``, each corresponding to a row in the target database table.

    Args:
        source_config (SourceConfiguration): specifies the target database.
        table_name (str): the name of the table to be read.

    Examples:
        Reading from a table on a postgres database. ::

            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import relational_db

            source_config = relational_db.SourceConfiguration(
                drivername='postgresql',
                host='localhost',
                port=5432,
                username='postgres',
                password='password',
                database='calendar',
            )
            table_name = 'months'

            with beam.Pipeline(options=PipelineOptions()) as p:
                records = p | "Reading records from db" >> relational_db.Read(
                    source_config=source_config,
                    table_name=table_name,
                )
                records | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            {'name': 'Jan', 'num': 1}
            {'name': 'Feb', 'num': 2}

        Where "name" and "num" are the column names.
    """

    def __init__(self, source_config, table_name, *args, **kwargs):
        super(Read, self).__init__(*args, **kwargs)
        self._read_args = dict(
            source_config=source_config,
            table_name=table_name
        )

    def expand(self, pcoll):
        return (
            pcoll
            | Create([self._read_args])
            | ParDo(_ReadFromRelationalDBFn())
        )


class _ReadFromRelationalDBFn(DoFn):
    def process(self, element):
        db_args = dict(element)
        table_name = db_args.pop('table_name')
        db = SqlAlchemyDB(**db_args)
        db.start_session()
        try:
            for record in db.read(table_name):
                yield record
        except:
            raise
        finally:
            db.close_session()


class Write(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for writing
    to tables on relational databases.

    Args:
        source_config (SourceConfiguration): specifies the target database. If
            ``source_config.create_if_missing`` is set to True, the target
            database will be created if it was missing.
        table_config (TableConfiguration): specifies the target table, as well
            as other optional parameters:
              - option for specifying custom insert statements.
              - options for creating the target table if it was missing.
            See :class:`~beam_nuggets.io.relational_db_api.TableConfiguration`
            for details.

    Examples:
        Writing to postgres database table. ::

            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import relational_db

            records = [
                {'name': 'Jan', 'num': 1},
                {'name': 'Feb', 'num': 2},
            ]

            source_config = relational_db.SourceConfiguration(
                drivername='postgresql',
                host='localhost',
                port=5432,
                username='postgres',
                password='password',
                database='calendar',
            )

            table_config = relational_db.TableConfiguration(
                name='months',
                create_if_missing=True,
                primary_key_columns=['num']
            )

            with beam.Pipeline(options=PipelineOptions()) as p:
                months = p | "Reading month records" >> beam.Create(records)
                months | 'Writing to DB table' >> relational_db.Write(
                    source_config=source_config,
                    table_config=table_config
                )

    """

    def __init__(self, source_config, table_config, *args, **kwargs):
        super(Write, self).__init__(*args, **kwargs)
        self.source_config = source_config
        self.table_config = table_config

    def expand(self, pcoll):
        return pcoll | ParDo(_WriteToRelationalDBFn(
            source_config=self.source_config,
            table_config=self.table_config
        ))


class _WriteToRelationalDBFn(DoFn):
    def __init__(self, source_config, table_config, *args, **kwargs):
        super(_WriteToRelationalDBFn, self).__init__(*args, **kwargs)
        self.source_config = source_config
        self.table_config = table_config

    def start_bundle(self):
        self._db = SqlAlchemyDB(self.source_config)

    def process(self, element):
        assert isinstance(element, dict)
        self._db.start_session()
        self._db.write_record(self.table_config, element)
        self._db.close_session()

    def finish_bundle(self):
        pass
