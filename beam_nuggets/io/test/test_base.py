from __future__ import division, print_function

import unittest

import testing.mysqld
import testing.postgresql

from beam_nuggets.io.relational_db import SourceConfiguration
from .database import TestDatabase


class TransformBaseTest(unittest.TestCase):
    postgres_instance = None
    mysql_instance = None

    postgres_source_config = None
    mysql_source_config = None

    source_config = None  # default for all tests

    @classmethod
    def setUpClass(cls):
        cls.postgres_source_config = cls.get_postgres_source_config()
        cls.mysql_source_config = cls.get_mysql_source_config()

        cls.source_config = (
            cls.postgres_source_config or
            cls.mysql_source_config or
            cls.get_sqlite_source_config()
        )

        print(
            '\nrunning {} tests against temp db instance: {}'
            ''.format(cls.__name__, cls.source_config.url)
        )

    @classmethod
    def get_sqlite_source_config(cls):
        return SourceConfiguration(
            drivername='sqlite',
            database='/tmp/delete_me_beam_nuggets_unittest.sqlite',
            create_if_missing=True,
        )

    @classmethod
    def get_postgres_source_config(cls):
        cls.postgres_instance = cls.connect_to_postgresql()
        if cls.postgres_instance:
            return SourceConfiguration(
                drivername='postgresql',
                host='localhost',
                port=cls.postgres_instance.settings['port'],
                username='postgres',
                database='beam_nuggets_test_db',
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
    def get_mysql_source_config(cls):
        cls.mysql_instance = cls.connect_to_mysql()
        if cls.mysql_instance:
            return SourceConfiguration(
                drivername='mysql+pymysql',
                host='localhost',
                port=cls.mysql_instance.my_cnf['port'],
                username='root',
                database='beam_nuggets_test_db',
                create_if_missing=True,
            )

    @classmethod
    def connect_to_mysql(cls):
        mysql = None
        try:
            # mysql = testing.mysqld.Mysqld(my_cnf={'skip-networking': None})
            mysql = testing.mysqld.Mysqld()
        except Exception as e:
            print(
                '\n\nFailed to connect to local mysql instance. Error: {}'
                '\nCheck mysql installed locally to run tests against it '
                '.\n{}\n{}'.format(
                    e,
                    'https://www.postgresql.org/download/linux/ubuntu/',
                    'https://github.com/tk0miya/testing.postgresql'
                )
            )
        return mysql

    @classmethod
    def tearDownClass(cls):
        if cls.postgres_instance:
            cls.postgres_instance.stop()
        if cls.mysql_instance:
            cls.mysql_instance.stop()

    @staticmethod
    def configure_db(source_config):
        db = TestDatabase(source_config)
        db.init_db()
        return db

    @staticmethod
    def destroy_db(db):
        # need this since some TC:s will use; I don't want to call tearDown
        # from within TC:s
        db.destroy_db()  # will, as well, delete created sqllite db file

    def setUp(self):
        self.db = self.configure_db(self.source_config)

    def tearDown(self):
        # clean DB resource
        self.destroy_db(self.db)
