from __future__ import division, print_function

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from nose.tools import assert_equal

from beam_nuggets.io import WriteToRelationalDB
from .test_base import TransformBaseTest


class TestWriteTransform(TransformBaseTest):

    def test_WriteToRelationalDB(self):
        # create write pipeline and execute it
        records, table_name = self.get_test_records_and_table_name()
        with TestPipeline() as p:
            months = p | "Reading month records" >> beam.Create(records)
            months | 'Writing to Sqlite table' >> WriteToRelationalDB(
                table_name=table_name,
                create_db_if_missing=True,
                create_table_if_missing=True,
                primary_key_columns=['num'],
                **self.db_params
            )

        # retrieve the written rows
        table_rows = self.db.read_rows(table_name)

        # compare
        assert_equal(table_rows, records)

    @staticmethod
    def get_test_records_and_table_name():
        table_name = 'months'
        records = [
            {'name': 'Jan', 'num': 1},
            {'name': 'Feb', 'num': 2},
        ]
        return records, table_name


if __name__ == '__main__':
    unittest.main()
