from __future__ import division, print_function

from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists


class SqlAlchemyDB(object):
    """
    TDOD
    https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
    """

    def __init__(
        self,
        drivername,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        create_db_if_missing=False,
        create_table_if_missing=False
    ):
        self._uri = str(URL(
            drivername=drivername,
            username=username,
            password=password,
            host=host,
            port=port,
            database=database
        ))
        self._create_table_if_missing = create_table_if_missing
        self._create_db_if_missing = create_db_if_missing

        self._SessionClass = sessionmaker(bind=create_engine(self._uri))
        self._session = None  # will be set in self.start_session()

        self._name_to_table = {}

    def start_session(self):
        if self._create_db_if_missing:
            self._create_db_if_not_exists()
        self._session = self._SessionClass()

    def close_session(self):
        self._session.close()
        self._session = None

    def read(self, table_name):
        table = self._open_table_for_read(table_name)
        for record in table.records(self._session):
            yield record

    def write_record(self, table_name, record_dict):
        table = self._open_table_for_write(table_name, record_dict)
        table.write_record(self._session, record_dict)

    def _open_table_for_read(self, name):
        return self._open_table(name, self._load_table)

    def _open_table_for_write(self, name, record):
        return self._open_table(name, self._create_table, record=record)

    def _open_table(self, name, get_table_f, **kwargs):
        table = self._name_to_table.get(name, None)
        if not table:
            self._name_to_table[name] = (
                self._get_table(name, get_table_f, **kwargs)
            )
            table = self._name_to_table[name]
        return table

    @staticmethod
    def _get_table(name, get_table_f, **kwargs):
        table_class = get_table_f(name, **kwargs)
        if table_class:
            table = _Table(table_class=table_class, name=name)
        else:
            raise SqlAlchemyDbException('Failed to get table {}'.format(name))
        return table

    def _load_table(self, name):
        table_class = None
        engine = self._session.bind
        if engine.dialect.has_table(engine, name):
            metadata = MetaData(bind=engine)
            table_class = self._create_table_class(
                Table(name, metadata, autoload=True)
            )
        return table_class

    def _create_table(self, name, record):
        table_class = self._load_table(name)
        if not table_class and self._create_table_if_missing:
            engine = self._session.bind
            metadata = MetaData(bind=engine)
            sqlalchemy_table = Table(
                name,
                metadata,
                *self._columns_from_sample(record)
            )
            metadata.create_all()
            table_class = self._create_table_class(sqlalchemy_table)
        return table_class

    @staticmethod
    def _create_table_class(sqlalchemy_table):
        class TableClass(declarative_base()):
            __table__ = sqlalchemy_table

        return TableClass

    def _columns_from_sample(self, record):
        return (
            [
                Column(self._get_idx_name(record), Integer, primary_key=True)
            ] +
            [
                Column(key, self._db_type_from_value(value))
                for key, value in record.iteritems()
            ]
        )

    @staticmethod
    def _get_idx_name(record):
        idx_col = 'id'
        while idx_col in record.keys():
            idx_col += '_'
        return idx_col

    def _db_type_from_value(self, val):
        return self.PYTHON_TYPE_TO_DB_TYPE.get(type(val), String)

    PYTHON_TYPE_TO_DB_TYPE = {
        int: Integer,
        str: String,
    }

    def _create_db_if_not_exists(self):
        if not database_exists(self._uri):
            create_database(self._uri)


class SqlAlchemyDbException(Exception):
    pass


class _Table(object):
    def __init__(self, table_class, name):
        self._Class = table_class
        self.name = name
        self._column_names = self._get_column_names(table_class)

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

    @staticmethod
    def _get_column_names(table_class):
        return [col.name for col in table_class.__table__.columns]
