from __future__ import division, print_function

from apache_beam import PTransform, DoFn, ParDo

from sqlalchemy_db import SqlAlchemyDB


class WriteToRelationalDB(PTransform):
    def __init__(self, source_config, table_config, *args, **kwargs):
        super(WriteToRelationalDB, self).__init__(*args, **kwargs)
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
