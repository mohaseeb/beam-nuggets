from __future__ import division, print_function

import csv

from apache_beam import PTransform
from apache_beam.io import Read
from apache_beam.io.filebasedsource import FileBasedSource


class ReadFromCsv(PTransform):

    def __init__(self, file_name, **kwargs):
        super(ReadFromCsv, self).__init__(**kwargs)
        self._file_name = file_name

    def expand(self, pcoll):
        return pcoll | Read(_CsvSource(self._file_name))


class _CsvSource(FileBasedSource):
    def read_records(self, file_name, range_tracker):
        # FIXME handle concurrent read
        self._file = self.open_file(file_name)

        for rec in csv.DictReader(self._file):
            yield rec
