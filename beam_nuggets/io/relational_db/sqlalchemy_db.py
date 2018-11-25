from __future__ import division, print_function

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


class SqlAlchemyDB(object):
    def __init__(self, uri, table_name):
        self._uri = uri
        self._table_name = table_name

        # will be set in self.start_session()
        self._session = None
        self._table_class = None

    def start_session(self):
        self._table_class = get_table(self._uri, self._table_name)
        self._session = get_session(self._uri)

    def close_session(self):
        self._session.close()

    def write_row(self, data):
        try:
            self._session.add(self._table_class(**data))
            self._session.commit()
        except:
            self._session.rollback()
            self._session.close()
            raise


Session = sessionmaker()


def get_session(db_uri):
    engine = create_engine(db_uri)
    Session.configure(bind=engine)
    return Session()


def get_table(db_uri, name):
    Base = declarative_base()
    metadata = MetaData(bind=create_engine(db_uri))

    class TableClass(Base):
        __table__ = Table(name, metadata, autoload=True)

    return TableClass
