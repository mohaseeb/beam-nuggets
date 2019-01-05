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

from beam_nuggets.io.relational_db_api import infer_db_type


class RelationalDBAPITest(unittest.TestCase):

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
        value_to_expected_db_type = [
            (10, Float),
            (10.5, Float),
            (np.nan, Float),
            (True, Boolean),
            (datetime.date.today(), Date),
            (datetime.datetime.today(), DateTime),
            ('sss', String),
            (None, String),
        ]

        self.validate_infer_db_type(value_to_expected_db_type, 'postgresql')

    # def test_infer_db_type_mysql(self):
    #     value_to_expected_db_type = [
    #         (10, Float),
    #         (10.5, Float),
    #         (np.nan, Float),
    #         (True, Boolean),
    #         (datetime.date.today(), Date),
    #         (datetime.datetime.today(), DateTime),
    #         ('sss', String(100)),
    #         (None, String(100)),
    #     ]
    #
    #     self.validate_infer_db_type(value_to_expected_db_type, 'mysql')


if __name__ == '__main__':
    unittest.main()
