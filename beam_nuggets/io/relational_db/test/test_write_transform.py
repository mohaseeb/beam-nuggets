from __future__ import division, print_function

import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from nose.tools import assert_equal, assert_not_equal

from beam_nuggets.io import WriteToRelationalDB, TableConfiguration
from .test_base import TransformBaseTest


class TestWriteTransform(TransformBaseTest):

    def setUp(self):
        super(TestWriteTransform, self).setUp()
        self.records, self.table_name = self.get_test_records_and_table_name()

    def execute_pipeline(self, source_config, table_config, records):
        with TestPipeline() as p:
            months = p | "Reading records" >> beam.Create(records)
            months | 'Writing to table' >> WriteToRelationalDB(
                source_config=source_config,
                table_config=table_config
            )

        # retrieve the table rows
        return self.db.read_rows(table_config.name)

    def test_write(self):
        table_config = TableConfiguration(
            name=self.table_name,
            create_table_if_missing=True,
            primary_key_columns=['num']
        )

        # execute the write pipeline and retrieve the table rows
        table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=self.records
        )

        # compare
        assert_equal(table_rows, self.records)

    def test_write_no_primary_key(self):
        table_config = TableConfiguration(
            name=self.table_name,
            create_table_if_missing=True
        )

        # execute the write pipeline and retrieve the table rows
        table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=self.records
        )

        # An auto-increment table called id should have been added
        expected_table_rows = [dict(record) for record in self.records]
        _ = [rec.update(id=i + 1) for i, rec in enumerate(expected_table_rows)]

        assert_equal(table_rows, expected_table_rows)

    def test_write_to_user_defined_table(self):
        user_defined_table = 'my_table'
        assert_not_equal(user_defined_table, self.table_name)

        def define_table(metadata):
            from sqlalchemy import Table, Column, Integer, String
            return Table(
                user_defined_table, metadata,
                Column('name', String, primary_key=True),
                Column('num', Integer)
            )

        table_config = TableConfiguration(
            name=user_defined_table,
            define_table_f=define_table,
            create_table_if_missing=True,
        )

        # execute the write pipeline and retrieve the table rows
        table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=self.records
        )

        # assert nothing written to the test default table
        assert_equal(len(self.db.read_rows(self.table_name)), 0)

        # and all written to the user defined table
        assert_equal(table_rows, self.records)

    def test_write_to_existing_table(self):
        table_name = self.create_table()

        table_config = TableConfiguration(
            name=table_name,
            create_table_if_missing=False
        )

        records = [
            {'name': 'Jan', 'num': 1},
            {'name': 'Feb', 'num': 2},
            {'name': 'Mar', 'num': 3},
            {'name': 'Apr', 'num': 4},
        ]
        part1_size = 2
        part1_records = records[:part1_size]
        part2_records = records[part1_size:]

        # write part1 to the DB
        part1_table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=part1_records
        )
        assert_equal(part1_table_rows, part1_records)

        # write part2 to the DB
        part2_table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=part2_records
        )
        assert_equal(part2_table_rows, part1_records + part2_records)
        # Note, above assumes row are returned in the same order as they were
        # written (i.e. first written first returned)

    def create_table(self):
        table_name = 'months_table'

        def define_table(metadata):
            from sqlalchemy import Table, Column, Integer, String
            return Table(
                table_name, metadata,
                Column('name', String, primary_key=True),
                Column('num', Integer)
            )

        # create test table
        self.db.create_table(
            name=table_name,
            define_table_f=define_table,
            create_table_if_missing=True
        )

        return table_name

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
