from __future__ import division, print_function

import csv

import apache_beam as beam


class Read(beam.PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading
    csv files.

    It outputs a :class:`~apache_beam.pvalue.PCollection` of
    ``dict:s``, each corresponding to a row in the csv file.

    Args:
        csv_path (str): csv file path.

    Examples:
        Reading content of a csv file. ::

            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import csvio

            path_to_csv = '/path/to/students.csv'
            with beam.Pipeline(options=PipelineOptions()) as p:
                students = p | "Reading students records" >> csvio.Read(path_to_csv)
                students | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            {'lastName': 'Norvell', 'firstName': 'Andrel', 'level': '10'}
            {'lastName': 'Proudfoot', 'firstName': 'Dinorah', 'level': '8'}
            {'lastName': 'Plotkin', 'firstName': 'Trulal', 'level': '14'}

    """

    def __init__(self, csv_path, *args, **kwargs):
        super(Read, self).__init__(*args, **kwargs)
        self._csv_path = csv_path

    def expand(self, pcoll):
        return pcoll | beam.io.Read(_CsvSource(self._csv_path))


class _CsvSource(beam.io.filebasedsource.FileBasedSource):
    def read_records(self, file_name, range_tracker):
        # FIXME handle concurrent read
        self._file = self.open_file(file_name)

        for rec in csv.DictReader(self._file):
            yield rec
