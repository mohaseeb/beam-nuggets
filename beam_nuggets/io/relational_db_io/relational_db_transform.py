from __future__ import division, print_function

from apache_beam import PTransform
from apache_beam.io.iobase import BoundedSource, Read, Sink, Writer, Write

from sqlalchemy_db import SqlAlchemyDB


class ReadFromRelationalDB(PTransform):

    def __init__(self, uri, table_name, **kwargs):
        super(ReadFromRelationalDB, self).__init__(**kwargs)
        self._uri = uri
        self._table_name = table_name

    def expand(self, pcoll):
        return pcoll | Read(_RelationalDBSource(self._uri, self._table_name))


class _RelationalDBSource(BoundedSource):
    def __init__(self, uri, table_name):
        self._uri = uri
        self._table_name = table_name

    def read(self, range_tracker):
        # FIXME handle concurrent read?
        db = SqlAlchemyDB(self._uri, self._table_name)
        db.start_session()
        for record in db.read():
            yield record
        db.close_session()

    def get_range_tracker(self, start_position, stop_position):
        pass


class WriteToRelationalDB(PTransform):

    def __init__(self, uri, table_name, **kwargs):
        super(WriteToRelationalDB, self).__init__(**kwargs)
        self._uri = uri
        self._table_name = table_name

    def expand(self, pcoll):
        return (
            pcoll | Write(_RelationalDBSink(self._uri, self._table_name)))


class _RelationalDBSink(Sink):

    def __init__(self, uri, table_name):
        self._uri = uri
        self._table_name = table_name

    def initialize_write(self):
        init_results = None
        return init_results
        # FIXME

    def open_writer(self, init_result, uid):
        print('init_results: {}, uid: {}'.format(init_result, uid))
        return _RelationalDBWriter(
            init_result,
            uid,
            self._uri,
            self._table_name
        )

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


class _RelationalDBWriter(Writer):

    def __init__(self, init_results, uid, uri, table_name):
        self._uid = uid
        self._db = SqlAlchemyDB(uri, table_name)
        self._db.start_session()

    def write(self, record):
        assert isinstance(record, dict)
        self._db.write_record(record)

    def close(self):
        self._db.close_session()
        writes_results = self._uid
        return writes_results
        # FIXME
