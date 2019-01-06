from __future__ import division, print_function

import datetime
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from nose.tools import assert_equal, assert_not_equal
from sqlalchemy import DateTime

from beam_nuggets.io import relational_db
from .test_base import TransformBaseTest


def sort(ls, key):
    return sorted(ls, key=lambda item: item[key])


class TestWriteTransform(TransformBaseTest):

    def setUp(self):
        super(TestWriteTransform, self).setUp()
        self.records, self.table_name = self.get_test_records_and_table_name()

    def execute_pipeline(self, source_config, table_config, records):
        with TestPipeline() as p:
            months = p | "Reading records" >> beam.Create(records)
            months | 'Writing to table' >> relational_db.Write(
                source_config=source_config,
                table_config=table_config
            )

        # retrieve the table rows
        return self.db.read_rows(table_config.name)

    def test_write(self):
        table_config = relational_db.TableConfiguration(
            name=self.table_name,
            create_if_missing=True,
            primary_key_columns=['num']
        )

        # execute the write pipeline and retrieve the table rows
        table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=self.records
        )

        # compare
        assert_equal(sort(table_rows, 'num'), sort(self.records, 'num'))

    def test_write_no_primary_key(self):
        table_config = relational_db.TableConfiguration(
            name=self.table_name,
            create_if_missing=True
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

        assert_equal(sort(table_rows, 'num'), sort(expected_table_rows, 'num'))

    def test_write_to_user_defined_table(self):
        user_defined_table = 'my_table'
        assert_not_equal(user_defined_table, self.table_name)

        def define_table(metadata):
            from sqlalchemy import Table, Column, Integer, String
            return Table(
                user_defined_table, metadata,
                Column('name', String(50), primary_key=True),
                Column('num', Integer)
            )

        table_config = relational_db.TableConfiguration(
            name=user_defined_table,
            define_table_f=define_table,
            create_if_missing=True,
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
        assert_equal(sort(table_rows, 'name'), sort(self.records, 'name'))

    def test_write_to_existing_table(self):
        table_name = self.create_table()

        table_config = relational_db.TableConfiguration(
            name=table_name,
            create_if_missing=False
        )

        part1_size = 2
        part1_records = self.records[:part1_size]
        part2_records = self.records[part1_size:]

        # write part1 to the DB
        part1_table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=part1_records
        )
        assert_equal(sort(part1_table_rows, 'num'), sort(part1_records, 'num'))

        # write part2 to the DB
        part2_table_rows = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=part2_records
        )
        assert_equal(
            sort(part2_table_rows, 'num'),
            sort(part1_records + part2_records, 'num')
        )
        # Note, above assumes row are returned in the same order as they were
        # written (i.e. first written first returned)

    def test_auto_column_type_inference(self):
        import pandas as pd
        from sqlalchemy import Integer, Float, String
        NAME = 'name'
        NUM = 'num'
        TIME_STAMP = 'time_stamp'
        records = [
            {NAME: name, NUM: num, TIME_STAMP: ts}
            for name, num, ts in [
                ['Jan', 1, datetime.datetime.now()],
                ['Feb', 2, datetime.datetime.now()]
            ]
        ]
        expected_column_types = [
            (NAME, String),
            (NUM, Float),
            (TIME_STAMP, DateTime),
            ('id', Integer)  # Auto created  when no primary key is specified
        ]

        table_config = relational_db.TableConfiguration(
            name=self.table_name,
            create_if_missing=True
        )

        # write the records using the pipeline
        _ = self.execute_pipeline(
            source_config=self.source_config,
            table_config=table_config,
            records=records
        )

        # load created table metadata
        table = self.db.load_table_class(self.table_name).__table__
        columns = [col for col in table.columns]
        get_column = lambda name: filter(lambda c: c.name == name, columns)[0]

        # verify inferred column types is as expected
        for col_name, expec_col_type in expected_column_types:
            inferred_col_type = get_column(col_name).type
            assert_equal(isinstance(inferred_col_type, expec_col_type), True)

    def test_write_to_postgres_with_upsert(self):
        if not self.postgres_source_config:
            raise unittest.SkipTest(
                '"{}" should run against postgres instance'.format(self.id())
            )
        self.evaluate_upsert(self.postgres_source_config)

    def test_write_to_mysql_with_upsert(self):
        if not self.mysql_source_config:
            raise unittest.SkipTest(
                '"{}" should run against mysql instance'.format(self.id())
            )
        self.evaluate_upsert(self.mysql_source_config)

    def evaluate_upsert(self, source_config):
        # reconfigure the db for this TC (as the db configured in setup
        # used the default source config, which might be different from the
        # one used in this TC).
        self.destroy_db(self.db)
        self.db = self.configure_db(source_config)


        table_config = relational_db.TableConfiguration(
            name=self.table_name,
            create_if_missing=True,
            primary_key_columns=['num']
        )

        # execute the write pipeline and retrieve the table rows
        table_rows = self.execute_pipeline(
            source_config=source_config,
            table_config=table_config,
            records=self.records
        )

        # compare
        assert_equal(table_rows, self.records)

        # re-write the same records
        table_rows_after_rewrite = self.execute_pipeline(
            source_config=source_config,
            table_config=table_config,
            records=self.records
        )

        # verify the table records are the same
        assert_equal(table_rows_after_rewrite, table_rows)

    def create_table(self):
        table_name = 'months_table'

        def define_table(metadata):
            from sqlalchemy import Table, Column, Integer, String
            return Table(
                table_name, metadata,
                Column('name', String(50), primary_key=True),
                Column('num', Integer)
            )

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
            {'name': 'Mar', 'num': 3},
            {'name': 'Apr', 'num': 4},
            {'name': 'May', 'num': 5},
        ]
        return records, table_name


if __name__ == '__main__':
    unittest.main()
