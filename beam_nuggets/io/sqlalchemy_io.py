from __future__ import division, print_function

from apache_beam.io import iobase


class SimpleKVSink(iobase.Sink):

    def __init__(self, simplekv, url, final_table_name):
        self._simplekv = simplekv
        self._url = url
        self._final_table_name = final_table_name

    def initialize_write(self):
        access_token = self._simplekv.connect(self._url)
        return access_token

    def open_writer(self, access_token, uid):
        table_name = 'table' + uid
        print('00000000000000000000000000 ' + table_name)
        return SimpleKVWriter(self._simplekv, access_token, table_name)

    def pre_finalize(self, init_result, writer_results):
        pass

    def finalize_write(self, access_token, table_names, pre_finalize_result):
        for i, table_name in enumerate(table_names):
            self._simplekv.rename_table(access_token, table_name,
                                        self._final_table_name + str(i))


class SimpleKVWriter(iobase.Writer):

    def __init__(self, simplekv, access_token, table_name):
        self._simplekv = simplekv
        self._access_token = access_token
        self._table_name = table_name
        self._table = self._simplekv.open_table(access_token, table_name)

    def write(self, record):
        key, value = record[0], record

        self._simplekv.write_to_table(self._access_token, self._table, key,
                                      value)

    def close(self):
        return self._table_name


class PostgresStorage(object):
    def connect(self, url):
        print('connect ' + url)

    def open_table(self, access_token, table_name):
        print('open_table ' + table_name)
        # return table

    def write_to_table(self, access_token, table, key, value):
        print(' table write ' + key)

    def rename_table(self, access_token, old_name, new_name):
        print('rename table ' + old_name + ' ' + new_name)
