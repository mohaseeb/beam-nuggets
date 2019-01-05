from __future__ import division, print_function

from apache_beam import PTransform, Create, ParDo, DoFn

from beam_nuggets.io.relational_db_api import (
    SqlAlchemyDB,
    SourceConfiguration,
    TableConfiguration
)

assert SourceConfiguration is not None
assert TableConfiguration is not None


class Read(PTransform):
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
        self._db.start_session()

    def process(self, element):
        assert isinstance(element, dict)
        self._db.write_record(self.table_config, element)

    def finish_bundle(self):
        self._db.close_session()
