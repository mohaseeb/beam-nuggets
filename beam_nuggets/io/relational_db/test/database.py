from __future__ import division, print_function

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, drop_database

from beam_nuggets.io.relational_db.sqlalchemy_db import (
    create_table,
    load_table,
    get_column_names_from_table,
    TableConfiguration
)


class TestDatabase(object):
    def __init__(self, source_config):
        self._url = source_config.url
        self._SessionClass = sessionmaker(bind=create_engine(self._url))

    def init_db(self):
        create_database(self._url)

    def destroy_db(self):
        drop_database(self._url)  # will also remove the sqlite file

    def create_table(self, name, create_table_if_missing, define_table_f):
        # create the table
        with self.session_scope() as session:
            create_table(
                session=session,
                name=name,
                table_config=TableConfiguration(
                    name=name,
                    define_table_f=define_table_f,
                    create_table_if_missing=create_table_if_missing
                ),
                record=None
            )

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
            session.close()
