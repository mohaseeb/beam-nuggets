from __future__ import division, print_function

from apache_beam import PTransform
from apache_beam.io.iobase import Sink, Writer, Write

from sqlalchemy_db import SqlAlchemyDB


class WriteToRelationalDB(PTransform):
    def __init__(
        self,
        table_name,
        drivername,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        create_db_if_missing=False,
        create_table_if_missing=False,
        **kwargs
    ):
        super(WriteToRelationalDB, self).__init__(**kwargs)
        self._db_args = dict(
            host=host,
            port=port,
            drivername=drivername,
            database=database,
            username=username,
            password=password,
            table_name=table_name,
            create_db_if_missing=create_db_if_missing,
            create_table_if_missing=create_table_if_missing,
        )

    def expand(self, pcoll):
        return (pcoll | Write(_RelationalDBSink(self._db_args)))


class _RelationalDBSink(Sink):
    def __init__(self, db_args):
        self._db_args = db_args

    def initialize_write(self):
        init_results = None
        return init_results
        # FIXME

    def open_writer(self, init_result, uid):
        return _RelationalDBWriter(init_result, uid, self._db_args)

    def pre_finalize(self, init_result, writer_results):
        pre_finalize_result = None
        return pre_finalize_result
        # FIXME

    def finalize_write(self, init_result, writer_results, pre_finalize_result):
        pass
        # FIXME


class _RelationalDBWriter(Writer):
    def __init__(self, init_results, uid, db_args):
        self._uid = uid
        self._table_name = db_args.pop('table_name')
        self._db = SqlAlchemyDB(**db_args)
        self._db.start_session()

    def write(self, record):
        assert isinstance(record, dict)
        self._db.write_record(self._table_name, record)

    def close(self):
        self._db.close_session()
        writes_results = self._uid
        return writes_results
        # FIXME
