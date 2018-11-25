from __future__ import division, print_function

from apache_beam import PTransform
from apache_beam.io import iobase

from sqlalchemy_db import SqlAlchemyDB


class WriteToRelationalDB(PTransform):

    def __init__(self, uri, table_name, **kwargs):
        super(WriteToRelationalDB, self).__init__(**kwargs)
        self._uri = uri
        self._table_name = table_name

    def expand(self, pcoll):
        return (
            pcoll | iobase.Write(RelationalDBSink(self._uri, self._table_name))
        )


class RelationalDBSink(iobase.Sink):

    def __init__(self, uri, table_name):
        self._uri = uri
        self._table_name = table_name

    def initialize_write(self):
        init_results = None
        return init_results
        # FIXME

    def open_writer(self, init_result, uid):
        print('init_results: {}, uid: {}'.format(init_result, uid))
        return RelationalDBWriter(init_result, uid, self._uri,
                                  self._table_name)

    def pre_finalize(self, init_result, writer_results):
        pre_finalize_result = None
        return pre_finalize_result
        # FIXME

    def finalize_write(self, init_result, writer_results, pre_finalize_result):
        print(
            'init_results: {}, write_results: {}, pre_finalize_result:{}'
            ''.format(init_result, writer_results, pre_finalize_result)
        )
        # FIXME


class RelationalDBWriter(iobase.Writer):

    def __init__(self, init_results, uid, uri, table_name):
        self._uid = uid
        self._db = SqlAlchemyDB(uri, table_name)
        self._db.start_session()

    def write(self, record):
        assert isinstance(record, dict)
        self._db.write_row(record)

    def close(self):
        self._db.close_session()
        writes_results = self._uid
        return writes_results
        # FIXME
