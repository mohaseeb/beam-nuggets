from __future__ import division, print_function

import csv

import apache_beam as beam


# TODO wrap in a transform
class CsvSource(beam.io.filebasedsource.FileBasedSource):
    def read_records(self, file_name, range_tracker):
        # FIXME handle concurrent read
        self._file = self.open_file(file_name)

        for rec in csv.DictReader(self._file):
            yield rec
