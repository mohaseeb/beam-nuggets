from __future__ import division, print_function

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from nose.tools import assert_equal

from beam_nuggets.io import WriteToRelationalDB, TableConfiguration
from .test_base import TransformBaseTest


class TestWriteTransform(TransformBaseTest):

    def setUp(self):
        super(TestWriteTransform, self).setUp()
        self.records, self.table_name = self.get_test_records_and_table_name()

    def test_WriteToRelationalDB(self):
        table_config = TableConfiguration(
            name=self.table_name,
            create_table_if_missing=True,
            primary_key_columns=['num']
        )

        with TestPipeline() as p:
            months = p | "Reading month records" >> beam.Create(self.records)
            months | 'Writing to table' >> WriteToRelationalDB(
                source_config=self.source_config,
                table_config=table_config
            )

        # retrieve the written rows
        table_rows = self.db.read_rows(self.table_name)

        # compare
        assert_equal(table_rows, self.records)

    def test_write_custom_table_definition(self):
        pass

    def test_write_to_existing_table(self):
        pass

    def test_write_no_primary_key(self):
        pass

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
