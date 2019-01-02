from __future__ import division, print_function

import datetime
import unittest

import numpy as np
import pandas as pd
from nose.tools import assert_equal
from sqlalchemy import (
    String,
    Float,
    Boolean,
    DateTime,
    Date
)

from beam_nuggets.io.relational_db.sqlalchemy_db import infer_db_type


class SqlAlchemyDBTest(unittest.TestCase):
    def test_infer_db_type(self):
        value_to_expected_db_type = [
            (10, Float),
            (10.5, Float),
            (np.nan, Float),
            (True, Boolean),
            (datetime.date.today(), Date),
            (datetime.datetime.today(), DateTime),
            (pd.Timestamp('now'), DateTime),
            ('sss', String),
            (None, String),
        ]

        for value, expected_db_type in value_to_expected_db_type:
            inferred_type = infer_db_type(value)
            assert_equal(
                expected_db_type,
                inferred_type,
                'expected: {}, got: {}'.format(expected_db_type, inferred_type)
            )


if __name__ == '__main__':
    unittest.main()
