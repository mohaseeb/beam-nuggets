from __future__ import division, print_function

import unittest

import testing.postgresql

from beam_nuggets.io import relational_db
from .database import TestDatabase


class TransformBaseTest(unittest.TestCase):
    db_instance = None

    @classmethod
    def setUpClass(cls):
        cls.db_instance = cls.connect_to_postgresql()
        if cls.db_instance:
            port = cls.db_instance.settings['port']
            cls.source_config = cls.get_postgres_source_config(port=port)
        else:
            cls.source_config = cls.get_sqlite_source_config()
        print(
            '\nrunning tests against temp db instance: {}'
            ''.format(cls.source_config.url)
        )

    @classmethod
    def get_postgres_source_config(cls, port):
        return relational_db.SourceConfiguration(
            drivername='postgresql',
            host='localhost',
            port=port,
            username='postgres',
            database='beam_nuggets_test_db',
            create_if_missing=True,
        )

    @classmethod
    def get_sqlite_source_config(cls):
        return relational_db.SourceConfiguration(
            drivername='sqlite',
            database='/tmp/delete_me_beam_nuggets_unittest.sqlite',
            create_if_missing=True,
        )

    @classmethod
    def connect_to_postgresql(cls):
        postgresql = None
        try:
            postgresql = testing.postgresql.Postgresql()
        except Exception as e:
            print(
                '\n\nFailed to connect to local postgres instance. Error: {}'
                '\nCheck postgresql installed locally to run tests against it '
                '.\n{}\n{}'.format(
                    e,
                    'https://www.postgresql.org/download/linux/ubuntu/',
                    'https://github.com/tk0miya/testing.postgresql'
                )
            )
        return postgresql

    @classmethod
    def tearDownClass(cls):
        if cls.db_instance:
            cls.db_instance.stop()

    def setUp(self):

        self.db = TestDatabase(self.source_config)
        self.db.init_db()

    def tearDown(self):
        # clean DB resource
        self.db.destroy_db()  # will, as well, delete created sqllite db file
        self.db = None
