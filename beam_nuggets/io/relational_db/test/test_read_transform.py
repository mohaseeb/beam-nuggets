from __future__ import division, print_function

import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_nuggets.io import ReadFromRelationalDB
from .test_base import TransformBaseTest


class TestReadTransform(TransformBaseTest):

    def test_ReadFromRelationalDB(self):
        # write test table and populate with test rows to DB
        table_name, table_rows = self.create_and_populate_test_table(n_rows=10)

        # create read pipeline, execute it and compare retrieved to actual rows
        with TestPipeline() as p:
            assert_that(
                p | "Reading records from db" >> ReadFromRelationalDB(
                    table_name=table_name,
                    **self.db_params
                ),
                equal_to(table_rows)
            )

    def create_and_populate_test_table(self, n_rows=10):
        # test table schema and data
        table_name = 'students'
        ID, NAME, AGE = 'id', 'name', 'age'
        rows = [
            {ID: row_id, NAME: 'Jack{}'.format(row_id), AGE: 20 + row_id}
            for row_id in range(n_rows)
        ]

        # create test table
        from sqlalchemy import Integer, String, Column
        self.db.create_table(
            name=table_name,
            create_table_if_missing=True,
            get_columns_f=lambda: [
                Column(ID, Integer, primary_key=True),
                Column(NAME, String),
                Column(AGE, Integer)
            ]
        )

        # populate
        self.db.write_rows(table_name, rows)
        return table_name, rows


if __name__ == '__main__':
    unittest.main()
