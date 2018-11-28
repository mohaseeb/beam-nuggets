from __future__ import division, print_function

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists


class SqlAlchemyDB(object):
    def __init__(self, uri, table_name, create_if_missing=False):
        self._uri = uri
        self._table_name = table_name
        self._create_if_missing = create_if_missing

        # will be set in self.start_session()
        self._session = None
        self._table_class = None
        self._column_names = None

    def start_session(self):
        create_db_if_not_exists(self._uri)
        self._session = get_session(self._uri)
        self._fetch_table()

    def close_session(self):
        self._session.close()
        self._session = None

    def write_record(self, record):
        try:
            if not self._table_class:
                self.require_table(record)
            self._session.add(self._table_class(**record))
            self._session.commit()
        except:
            self._session.rollback()
            self._session.close()
            raise

    def read(self):
        for row_obj in self._session.query(self._table_class):
            yield row_obj_to_dict(row_obj, self._column_names)

    def require_table(self, record):
        create_table(self._table_name, record, self._session)
        self._fetch_table()

    def _fetch_table(self):
        self._table_class = get_table(self._table_name, self._session)
        if self._table_class:
            self._column_names = get_column_names(self._table_class)


Session = sessionmaker()


def create_table(name, record, session):
    engine = session.bind
    metadata = MetaData(bind=engine)
    Table(name, metadata, *columns_from_sample(record))
    metadata.create_all()


def columns_from_sample(record):
    return [Column('id_', Integer, primary_key=True)] + [
        Column(key, db_type_from_value(value))
        for key, value in record.iteritems()
    ]


def db_type_from_value(val):
    return PYTHON_TYPE_TO_DB_TYPE.get(type(val), String)


PYTHON_TYPE_TO_DB_TYPE = {
    int: Integer,
    str: String,
}


def create_db_if_not_exists(uri):
    if not database_exists(uri):
        create_database(uri)


def get_session(db_uri):
    engine = create_engine(db_uri)
    Session.configure(bind=engine)
    return Session()


def get_table(name, session):
    engine = session.bind
    metadata = MetaData(bind=engine)
    table_class = None
    if engine.dialect.has_table(engine, name):
        class TableClass(declarative_base()):
            __table__ = Table(name, metadata, autoload=True)

        table_class = TableClass
    return table_class


def get_column_names(table_class):
    return [col.name for col in table_class.__table__.columns]


def row_obj_to_dict(obj, columns):
    return {col: getattr(obj, col) for col in columns}
