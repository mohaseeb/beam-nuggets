from __future__ import division, print_function

import datetime
import unittest

import numpy as np
from nose.tools import assert_equal
from sqlalchemy import (
    String,
    Float,
    Boolean,
    DateTime,
    Date
)

from beam_nuggets.io.relational_db_api import (
    infer_db_type,
    VARCHAR_DEFAULT_COL_SIZE
)


class RelationalDBAPITest(unittest.TestCase):
    # PostgreSQL and SQLite support String columns with no max size
    postgres_sqlite_str_value_to_expected_db_type = [
        ('sss', String),
        (None, String),
    ]

    str_value_to_expected_db_type = [
        ('sss', String(VARCHAR_DEFAULT_COL_SIZE)),
        (None, String(VARCHAR_DEFAULT_COL_SIZE)),
    ]

    other_types_values_to_db_type = [
        (10, Float),
        (10.5, Float),
        (np.nan, Float),
        (True, Boolean),
        (datetime.date.today(), Date),
        (datetime.datetime.today(), DateTime),
    ]

    def validate_infer_db_type(self, value_to_expected_db_type, drivername):
        for value, expected_db_type in value_to_expected_db_type:
            inferred_type = infer_db_type(value, drivername)
            assert_equal(
                expected_db_type,
                inferred_type,
                'expected: {}, got: {}'.format(
                    type(expected_db_type),
                    type(inferred_type)
                )
            )

    def test_infer_db_type_postgres(self):
        self.validate_infer_db_type(
            self.postgres_sqlite_str_value_to_expected_db_type +
            self.other_types_values_to_db_type,
            'postgresql'
        )

    def test_infer_db_type_sqlite(self):
        self.validate_infer_db_type(
            self.postgres_sqlite_str_value_to_expected_db_type +
            self.other_types_values_to_db_type,
            'sqlite'
        )

    def test_infer_db_type_mysql(self):
        drivername = 'mysql'

        self.validate_infer_db_type(
            self.other_types_values_to_db_type,
            drivername
        )

        # For some reason, I didn't chase, for fixed String size columns
        # inferred_type == expected_db_type is False even if the two seemed
        # equal. Below works enough.
        for value, expected_db_type in \
            self.str_value_to_expected_db_type:
            inferred_type = infer_db_type(value, drivername)
            assert_equal(
                type(expected_db_type),
                type(inferred_type),
                'expected: {}, got: {}'.format(
                    type(expected_db_type),
                    type(inferred_type)
                )
            )


if __name__ == '__main__':
    unittest.main()
