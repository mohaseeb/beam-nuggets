from __future__ import division, print_function

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from nose.tools import assert_equal

from beam_nuggets.io import WriteToRelationalDB
from .test_base import TransformBaseTest


class TestWriteTransform(TransformBaseTest):

    def setUp(self):
        super(TestWriteTransform, self).setUp()
        self.records, self.table_name = self.get_test_records_and_table_name()

    def test_WriteToRelationalDB(self):
        with TestPipeline() as p:
            months = p | "Reading month records" >> beam.Create(self.records)
            months | 'Writing to Sqlite table' >> WriteToRelationalDB(
                db_config=self.db_config,
                table_name=self.table_name,
                create_db_if_missing=True,
                create_table_if_missing=True,
                primary_key_columns=['num']
            )

        # retrieve the written rows
        table_rows = self.db.read_rows(self.table_name)

        # compare
        assert_equal(table_rows, self.records)

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
