from __future__ import division, print_function

import os
from contextlib import contextmanager
from copy import copy

from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils.functions import quote

from beam_nuggets.io.relational_db_api import (
    create_table,
    load_table,
    get_column_names_from_table,
    TableConfiguration,
    _create_database_tmp as create_database
)


class TestDatabase(object):
    def __init__(self, source_config):
        self._url = source_config.url
        self._SessionClass = sessionmaker(bind=create_engine(self._url))

    def init_db(self):
        create_database(self._url)

    def destroy_db(self):
        _drop_database_tmp(self._url)  # will also remove the sqlite file

    def create_table(self, name, create_table_if_missing, define_table_f):
        # create the table
        with self.session_scope() as session:
            create_table(
                session=session,
                name=name,
                table_config=TableConfiguration(
                    name=name,
                    define_table_f=define_table_f,
                    create_if_missing=create_table_if_missing
                ),
                record=None
            )

    def load_table_class(self, table_name):
        with self.session_scope() as session:
            return load_table(session, table_name)

    def write_rows(self, table_name, rows):
        with self.session_scope() as session:
            TableCls = load_table(session, table_name)
            session.add_all([TableCls(**row_dict) for row_dict in rows])

    def read_rows(self, table_name):
        with self.session_scope() as session:
            TableCls = load_table(session, table_name)
            if TableCls:
                column_names = get_column_names_from_table(TableCls)

                def to_dict(db_row):
                    return {col: getattr(db_row, col) for col in column_names}

                rows = [to_dict(db_row) for db_row in session.query(TableCls)]
            else:
                rows = []
        return rows

    @contextmanager
    def session_scope(self):
        session = self._SessionClass()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close_all()
            session.bind.dispose()


# TODO Below is a copy of sqlalchemy_utils.drop_database with a fix to
#  support dropping PostgreSQL DBs using pg8000. We should remove this tmp fix
#  when the following PR is merged:
#  https://github.com/kvesteri/sqlalchemy-utils/pull/356
def _drop_database_tmp(url):
    """Issue the appropriate DROP DATABASE statement.

    :param url: A SQLAlchemy engine URL.

    Works similar to the :ref:`create_database` method in that both url text
    and a constructed url are accepted. ::

        drop_database('postgres://postgres@localhost/name')
        drop_database(engine.url)

    """

    url = copy(make_url(url))

    database = url.database

    if url.drivername.startswith('postgres'):
        url.database = 'postgres'
    elif url.drivername.startswith('mssql'):
        url.database = 'master'
    elif not url.drivername.startswith('sqlite'):
        url.database = None

    if url.drivername == 'mssql+pyodbc':
        engine = create_engine(url, connect_args={'autocommit': True})
    elif url.drivername == 'postgresql+pg8000':
        engine = create_engine(url, isolation_level='AUTOCOMMIT')
    else:
        engine = create_engine(url)
    conn_resource = None

    if engine.dialect.name == 'sqlite' and database != ':memory:':
        if database:
            os.remove(database)

    elif engine.dialect.name == 'postgresql' and engine.driver == 'psycopg2':
        from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

        connection = engine.connect()
        connection.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        # Disconnect all users from the database we are dropping.
        version = connection.dialect.server_version_info
        pid_column = (
            'pid' if (version >= (9, 2)) else 'procpid'
        )
        text = '''
        SELECT pg_terminate_backend(pg_stat_activity.%(pid_column)s)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '%(database)s'
          AND %(pid_column)s <> pg_backend_pid();
        ''' % {'pid_column': pid_column, 'database': database}
        connection.execute(text)

        # Drop the database.
        text = 'DROP DATABASE {0}'.format(quote(connection, database))
        connection.execute(text)
        conn_resource = connection
    else:
        text = 'DROP DATABASE {0}'.format(quote(engine, database))
        conn_resource = engine.execute(text)

    if conn_resource is not None:
        conn_resource.close()
    engine.dispose()
