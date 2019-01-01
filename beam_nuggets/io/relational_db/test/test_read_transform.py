from __future__ import division, print_function

import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_nuggets.io import relational_db
from .test_base import TransformBaseTest


class TestReadTransform(TransformBaseTest):

    def setUp(self):
        super(TestReadTransform, self).setUp()
        self.table_name, self.table_rows = (
            self.create_and_populate_test_table(n_rows=10)
        )

    def test_ReadFromRelationalDB(self):
        # create read pipeline, execute it and compare retrieved to actual rows
        with TestPipeline() as p:
            assert_that(
                p | "Reading records from db" >> relational_db.Read(
                    source_config=self.source_config,
                    table_name=self.table_name
                ),
                equal_to(self.table_rows)
            )

    def create_and_populate_test_table(self, n_rows=10):
        from sqlalchemy import Table, Integer, String, Column
        # test table schema and data
        table_name = 'students'
        ID, NAME, AGE = 'id', 'name', 'age'

        def define_table(metadata):
            return Table(
                table_name, metadata,
                Column(ID, Integer, primary_key=True),
                Column(NAME, String),
                Column(AGE, Integer)
            )

        rows = [
            {ID: row_id, NAME: 'Jack{}'.format(row_id), AGE: 20 + row_id}
            for row_id in range(n_rows)
        ]

        # create test table
        self.db.create_table(
            name=table_name,
            define_table_f=define_table,
            create_table_if_missing=True
        )

        # populate
        self.db.write_rows(table_name, rows)
        return table_name, rows


if __name__ == '__main__':
    unittest.main()
