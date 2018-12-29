from __future__ import division, print_function

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists


class RelationalDBConfiguration(object):
    def __init__(
        self,
        drivername,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
    ):
        self.url = URL(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        )


class SqlAlchemyDB(object):
    """
    TDOD
    https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
    """

    def __init__(
        self,
        db_config,
        primary_key_columns=None,
        create_db_if_missing=False,
        create_table_if_missing=False
    ):
        """
        Args:
            db_config (RelationalDBConfiguration):
            primary_key_columns (list):
            create_db_if_missing (bool):
            create_table_if_missing (bool):
        """
        self._url = db_config.url
        self._create_table_if_missing = create_table_if_missing
        self._create_db_if_missing = create_db_if_missing

        self._primary_key_column_names = primary_key_columns or []

        self._SessionClass = sessionmaker(bind=create_engine(self._url))
        self._session = None  # will be set in self.start_session()

        self._name_to_table = {}

    def start_session(self):
        if self._create_db_if_missing and not database_exists(self._url):
            create_database(self._url)
        self._session = self._SessionClass()

    def close_session(self):
        self._session.close()
        self._session = None

    def read(self, table_name):
        table = self._open_table_for_read(table_name)
        for record in table.records(self._session):
            yield record

    def write_record(self, table_name, record_dict):
        """
        https://docs.sqlalchemy.org/en/latest/dialects/postgresql.html
        #insert-on-conflict-upsert
        https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql
        -insert-on-duplicate-key-update
        """
        table = self._open_table_for_write(table_name, record_dict)
        table.write_record(self._session, record_dict)

    def _open_table_for_read(self, name):
        return self._open_table(
            name=name,
            get_table_f=load_table
        )

    def _open_table_for_write(self, name, record):
        return self._open_table(
            name=name,
            get_table_f=create_table,
            create_table_if_missing=self._create_table_if_missing,
            create_columns_f=lambda: _columns_from_sample_record(
                record=record,
                primary_key_column_names=self._primary_key_column_names
            )
        )

    def _open_table(self, name, get_table_f, **kwargs):
        table = self._name_to_table.get(name, None)
        if not table:
            self._name_to_table[name] = (
                self._get_table(name, get_table_f, **kwargs)
            )
            table = self._name_to_table[name]
        return table

    def _get_table(self, name, get_table_f, **kwargs):
        table_class = get_table_f(self._session, name, **kwargs)
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


def create_table(session, name, create_table_if_missing, create_columns_f):
    table_class = load_table(session, name)
    if not table_class and create_table_if_missing:
        engine = session.bind
        metadata = MetaData(bind=engine)
        sqlalchemy_table = Table(name, metadata, *create_columns_f())
        metadata.create_all()
        table_class = create_table_class(sqlalchemy_table)
    return table_class


def create_table_class(sqlalchemy_table):
    class TableClass(declarative_base()):
        __table__ = sqlalchemy_table

    return TableClass


def _columns_from_sample_record(record, primary_key_column_names):
    if len(primary_key_column_names) == 0:
        pri_col_name = 'id'
        while pri_col_name in record.keys():
            pri_col_name += '_'
        primary_key_columns = [Column(pri_col_name, Integer, primary_key=True)]
        other_columns = [
            Column(col, infer_db_type(value))
            for col, value in record.iteritems()
        ]
    else:
        primary_key_columns = [
            Column(col, infer_db_type(record[col]), primary_key=True)
            for col in primary_key_column_names
        ]
        other_columns = [
            Column(col, infer_db_type(value))
            for col, value in record.iteritems()
            if col not in primary_key_column_names
        ]
    return primary_key_columns + other_columns


def infer_db_type(val):
    return PYTHON_TYPE_TO_DB_TYPE.get(type(val), String)


PYTHON_TYPE_TO_DB_TYPE = {
    int: Integer,
    str: String,
}


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
