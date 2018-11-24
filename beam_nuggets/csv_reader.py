from __future__ import division, print_function

import csv

import apache_beam as beam


class CsvSource(beam.io.filebasedsource.FileBasedSource):
    def read_records(self, file_name, range_tracker):
        self._file = self.open_file(file_name)

        for rec in csv.DictReader(self._file):
            yield rec
