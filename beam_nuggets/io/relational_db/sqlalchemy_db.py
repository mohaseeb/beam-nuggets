from __future__ import division, print_function

import datetime

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Date
)
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists


class SourceConfiguration(object):
    def __init__(
        self,
        drivername,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        create_if_missing=False,
    ):
        self.url = URL(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )
        self.create_if_missing = create_if_missing


class TableConfiguration(object):
    def __init__(
        self,
        name,
        define_table_f=None,
        create_if_missing=False,
        primary_key_columns=None,
    ):
        """

        Args:
            name:
            define_table_f:
                https://docs.sqlalchemy.org/en/latest/core/metadata.html
                https://docs.sqlalchemy.org/en/latest/orm/extensions
                /declarative/table_config.html
            create_if_missing:
            primary_key_columns:
        """
        self.name = name
        self.define_table_f = define_table_f
        self.create_table_if_missing = create_if_missing
        self.primary_key_column_names = primary_key_columns or []


class SqlAlchemyDB(object):
    """
    TDOD
    """

    def __init__(self, source_config):
        """
        Args:
            source_config (SourceConfiguration):
        """
        self._source = source_config

        self._SessionClass = sessionmaker(bind=create_engine(self._source.url))
        self._session = None  # will be set in self.start_session()

        self._name_to_table = {}  # tables metadata cache

    def start_session(self):
        create_if_missing = self._source.create_if_missing
        database_is_missing = lambda: not database_exists(self._source.url)
        if create_if_missing and database_is_missing():
            create_database(self._source.url)
        self._session = self._SessionClass()

    def close_session(self):
        self._session.close()
        self._session = None

    def read(self, table_name):
        table = self._open_table_for_read(table_name)
        for record in table.records(self._session):
            yield record

    def write_record(self, table_config, record_dict):
        """
        https://docs.sqlalchemy.org/en/latest/dialects/postgresql.html
        #insert-on-conflict-upsert
        https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql
        -insert-on-duplicate-key-update
        """
        table = self._open_table_for_write(table_config, record_dict)
        table.write_record(self._session, record_dict)

    def _open_table_for_read(self, name):
        return self._open_table(
            name=name,
            get_table_f=load_table
        )

    def _open_table_for_write(self, table_config, record):
        return self._open_table(
            name=table_config.name,
            get_table_f=create_table,
            table_config=table_config,
            record=record
        )

    def _open_table(self, name, get_table_f, **get_table_f_params):
        table = self._name_to_table.get(name, None)
        if not table:
            self._name_to_table[name] = (
                self._get_table(name, get_table_f, **get_table_f_params)
            )
            table = self._name_to_table[name]
        return table

    def _get_table(self, name, get_table_f, **get_table_f_params):
        table_class = get_table_f(self._session, name, **get_table_f_params)
        if table_class:
            table = _Table(table_class=table_class, name=name)
        else:
            raise SqlAlchemyDbException('Failed to get table {}'.format(name))
        return table


def load_table(session, name):
    table_class = None
    engine = session.bind
    if engine.dialect.has_table(engine, name):
        metadata = MetaData(bind=engine)
        table_class = create_table_class(Table(name, metadata, autoload=True))
    return table_class


def create_table(session, name, table_config, record):
    # Attempt to load from the DB
    table_class = load_table(session, name)

    if not table_class and table_config.create_table_if_missing:
        define_table_f = (
            table_config.define_table_f or
            _get_default_define_f(
                record=record,
                name=name,
                primary_key_column_names=table_config.primary_key_column_names,
            )
        )
        metadata = MetaData(bind=session.bind)
        sqlalchemy_table = define_table_f(metadata)
        metadata.create_all()
        table_class = create_table_class(sqlalchemy_table)

    return table_class


def create_table_class(sqlalchemy_table):
    class TableClass(declarative_base()):
        __table__ = sqlalchemy_table

    return TableClass


def _get_default_define_f(record, name, primary_key_column_names):
    def define_table(metadata):
        columns = _columns_from_sample_record(
            record=record,
            primary_key_column_names=primary_key_column_names
        )
        return Table(name, metadata, *columns)

    return define_table


def _columns_from_sample_record(record, primary_key_column_names):
    if len(primary_key_column_names) > 0:
        primary_key_columns = [
            Column(col, infer_db_type(record[col]), primary_key=True)
            for col in primary_key_column_names
        ]
        other_columns = [
            Column(col, infer_db_type(value))
            for col, value in record.iteritems()
            if col not in primary_key_column_names
        ]
    else:
        pri_col_name = 'id'
        while pri_col_name in record.keys():
            pri_col_name += '_'
        primary_key_columns = [Column(pri_col_name, Integer, primary_key=True)]
        other_columns = [
            Column(col, infer_db_type(value))
            for col, value in record.iteritems()
        ]
    return primary_key_columns + other_columns


class SqlAlchemyDbException(Exception):
    pass


class _Table(object):
    def __init__(self, table_class, name):
        self._Class = table_class
        self.name = name
        self._column_names = get_column_names_from_table(table_class)

    def records(self, session):
        for record in session.query(self._Class):
            yield self._from_db_record(record)

    def write_record(self, session, record_dict):
        try:
            session.add(self._to_db_record(record_dict))
            session.commit()
        except:
            session.rollback()
            session.close()
            raise

    def _to_db_record(self, record_dict):
        return self._Class(**record_dict)

    def _from_db_record(self, db_record):
        return {col: getattr(db_record, col) for col in self._column_names}


def get_column_names_from_table(table_class):
    return [col.name for col in table_class.__table__.columns]


def infer_db_type(val):
    for is_type_f, db_type in PYTHON_TO_DB_TYPE:
        if is_type_f(val):
            return db_type
    return String


def is_number(x):
    try:
        _ = x + 1
    except:
        return False
    return not hasattr(x, '__len__')


def is_pandas_timestamp(x):
    type_name = str(type(x))
    return 'pandas' in type_name and 'Timestamp' in type_name


PYTHON_TO_DB_TYPE = [
    # Order matters!
    (lambda x: isinstance(x, bool), Boolean),
    (is_number, Float),
    (lambda x: isinstance(x, datetime.datetime), DateTime),
    (is_pandas_timestamp, DateTime),
    (lambda x: isinstance(x, datetime.date), Date),
]
