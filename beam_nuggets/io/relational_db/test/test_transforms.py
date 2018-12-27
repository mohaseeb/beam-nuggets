from __future__ import division, print_function

import unittest

import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
from nose.tools import assert_equal

from beam_nuggets.io import ReadFromRelationalDB, WriteToRelationalDB
from .database import TestDatabase


class TestTransforms(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestTransforms, self).__init__(*args, **kwargs)
        self.db_params = dict(
            drivername='sqlite',
            database='/tmp/beam_nuggest_unittest_db.sqlite',
        )
        self.table_name = 'students'

    def setUp(self):
        # setup the DB
        self.db = TestDatabase(self.db_params)
        self.db.init_db()

    def tearDown(self):
        # clean DB resource
        self.db.destroy_db()
        self.db = None

    def test_ReadFromRelationalDB(self):
        # write test table and populate with test rows to DB
        table_rows = self.db.create_test_table(self.table_name, n_rows=10)

        # create read pipeline, execute it and compare retrieved to actual rows
        with TestPipeline() as p:
            assert_that(
                p | "Reading records from db" >> ReadFromRelationalDB(
                    table_name=self.table_name,
                    **self.db_params
                ),
                equal_to(table_rows)
            )

    def test_WriteToRelationalDB(self):
        # create write pipeline and execute it
        rows = [
            {'name': 'Jan', 'num': 1},
            {'name': 'Feb', 'num': 2},
        ]
        with TestPipeline() as p:
            months = p | "Reading month records" >> beam.Create(rows)
            months | 'Writing to Sqlite table' >> WriteToRelationalDB(
                table_name=self.table_name,
                create_db_if_missing=True,
                create_table_if_missing=True,
                primary_key_columns=['num'],
                **self.db_params
            )

        # retrieve the written rows
        table_rows = self.db.read_rows(self.table_name)

        # compare
        assert_equal(table_rows, rows)


if __name__ == '__main__':
    unittest.main()
