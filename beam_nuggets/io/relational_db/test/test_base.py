from __future__ import division, print_function

import unittest

from .database import TestDatabase


class TransformBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TransformBaseTest, self).__init__(*args, **kwargs)
        test_db_cls = TestDatabase
        self.db_params = dict(
            drivername='sqlite',
            database='/tmp/delete_me_beam_nuggets_unittest.sqlite',
        )
        self.db = test_db_cls(self.db_params)

    def setUp(self):
        # setup the DB
        self.db.init_db()

    def tearDown(self):
        # clean DB resource
        self.db.destroy_db()  # will, as well, delete created sqllite db file
        self.db = None
