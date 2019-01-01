from __future__ import division, print_function

import unittest

from beam_nuggets.io import relational_db
from .database import TestDatabase


class TransformBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TransformBaseTest, self).__init__(*args, **kwargs)
        test_db_cls = TestDatabase

        self.source_config = relational_db.SourceConfiguration(
            drivername='sqlite',
            database='/tmp/delete_me_beam_nuggets_unittest.sqlite',
            create_if_missing=True,
        )
        self.db = test_db_cls(self.source_config)

    def setUp(self):
        # setup the DB
        self.db.init_db()

    def tearDown(self):
        # clean DB resource
        self.db.destroy_db()  # will, as well, delete created sqllite db file
        self.db = None
