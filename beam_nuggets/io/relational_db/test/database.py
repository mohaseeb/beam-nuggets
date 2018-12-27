from __future__ import division, print_function

from contextlib import contextmanager

from sqlalchemy import MetaData, Table, create_engine, Integer, String, Column
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, drop_database


class TestDatabase(object):
    def __init__(self, db_params):
        self._uri = URL(**db_params)
        self._SessionClass = sessionmaker(bind=create_engine(self._uri))

    def init_db(self):
        _create_test_db(self._uri)

    def destroy_db(self):
        drop_database(self._uri)  # will also remove the sqlite file

    def create_test_table(self, table_name, n_rows=10):
        # create the table
        with self.session_scope() as session:
            TableCls = _create_table(
                session=session,
                name=table_name,
                columns=get_test_table_columns()
            )

        # populate with data
        rows = get_test_rows(n_rows)
        with self.session_scope() as session:
            session.add_all([TableCls(**row_dict) for row_dict in rows])

        return rows

    def read_rows(self, table_name):
        with self.session_scope() as session:
            TableCls, column_names = _load_table(session, table_name)

            def to_dict(db_row):
                return {col: getattr(db_row, col) for col in column_names}

            rows = [to_dict(db_row) for db_row in session.query(TableCls)]
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
            session.close()


def _create_test_db(uri):
    create_database(uri)


def _create_table(session, name, columns):
    metadata = MetaData(bind=session.bind)
    sqlalchemy_table = Table(name, metadata, *columns)
    metadata.create_all()

    class TableClass(declarative_base()):
        __table__ = sqlalchemy_table

    return TableClass


def _load_table(session, name):
    engine = session.bind
    if engine.dialect.has_table(engine, name):
        metadata = MetaData(bind=engine)

        class TableClass(declarative_base()):
            __table__ = Table(name, metadata, autoload=True)

        column_names = [col.name for col in TableClass.__table__.columns]

        return TableClass, column_names


class COLUMNS(object):
    ID = 'id'
    NAME = 'name'
    AGE = 'age'


def get_test_table_columns():
    return [
        Column(COLUMNS.ID, Integer, primary_key=True),
        Column(COLUMNS.NAME, String),
        Column(COLUMNS.AGE, Integer)
    ]


def get_test_rows(n_rows=10):
    return [
        {
            COLUMNS.ID: row_id,
            COLUMNS.NAME: 'Jack{}'.format(row_id),
            COLUMNS.AGE: 20 + row_id
        }
        for row_id in range(n_rows)
    ]
