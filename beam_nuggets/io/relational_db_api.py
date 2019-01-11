"""Wrapper of SqlAlchemy code used for reading from and writing to databases"""
from __future__ import division, print_function

import datetime
from copy import copy

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Date,
    insert as generic_insert
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine.url import URL, make_url
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists
from sqlalchemy_utils.functions import quote


class SourceConfiguration(object):
    """Holds parameters for accessing a database.

    User to pass database access parameters to
    :class:`~beam_nuggets.io.relational_db_api.SqlAlchemyDB`.

    ``SourceConfiguration.url`` provides the database url used by SqlAlchemy to
    connect to the database.

    Args:
        drivername (str): name of the database backend. It specifies the
            target database type and the driver (DBAPI) used
            by SqlAlchemy to communicate with database. The following
            drivernames are supported by and tested on beam-nuggets:
              - mysql+pymysql: for MySQL (pymysql is the driver name).
              - postgresql: for PostgreSQL (default driver pg8000 is used).
              - sqlite: for SQLite.
            Additional drivers can be used after installing their
            corresponding python libraries. Refer to `SqlAlchemy dialects`_
            for more information on the supported databases and the
            corresponding drivers.
        host (str): database host name or IP.
        port (int): database port number.
        database (str): database name.
        username (str): database username.
        password (str): database password.
        create_if_missing (bool): If set to True, it instructs to create a
            missing database before writing to it.

    Examples:
        MySQL database ::

            from beam_nuggets.io import relational_db
            src_cnf = relational_db.SourceConfiguration(
                drivername='mysql+pymysql',
                host='localhost',
                port=37311,
                username='root',
                database='test',
                create_if_missing=True,
            )
            print(src_cnf.url)
            # mysql+pymysql://root@localhost:37311/test

        PostgreSQL database ::

            from beam_nuggets.io import relational_db
            src_cnf = relational_db.SourceConfiguration(
                drivername='postgresql',
                host='localhost',
                port=42139,
                username='postgres',
                password='pass',
                database='test'
            )
            print(src_cnf.url)
            # postgresql://postgres:pass@localhost:42139/test

        SQLite database ::

            from beam_nuggets.io import relational_db
            src_cnf = relational_db.SourceConfiguration(
                drivername='sqlite',
                database='/tmp/test_db.sqlite'
            )
            print(src_cnf.url)
            # sqlite:////tmp/test_db.sqlite

    .. _SqlAlchemy dialects: https://docs.sqlalchemy.org/en/latest/dialects/
    """

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
    """Holds parameters for a database table.
    Used to pass table parameters to :class:`SqlAlchemyDB`.

    Args:
        name (str): the table name.
        create_insert_f (function): a function that takes as input an
            SqlAlchemy table and a row record, and returns an statement for
            inserting the record into the table. The function doesn't
            execute the insert statement. If not specified, the following
            default implementations are used:
              - :func:`create_upsert_mysql` for MySQL tables.
              - :func:`create_upsert_postgres` for PostgreSQL tables.
              - :func:`create_insert` for other databases.
            As a mechanism to recover from failures, beam runners will
            attempt to apply a transform multiple times on the same data;
            because of that it is recommended to implement idempotent writes
            (e.g. :func:`create_upsert_mysql` and
            :func:`create_upsert_postgres`)
            to avoid data inconsistency issues arising from this beam behavior.
            The function has the following signature:
            (``sqlalchemy.sql.schema.Table``, ``dict``) ->
            ``sqlalchemy.sql.dml.Insert``.
        create_if_missing (bool): if set to True and the table is missing
            :class:`SqlAlchemyDB` will create the table. See below notes on new
            table creation. See below note how this is used when creating
            new tables.
        primary_key_columns (list): list of column names to be used as
            primary key (if multiple columns are specified, a composite key
            is created), when creating the table. See below notes on new table
            creation.
        define_table_f (function): A function for defining an SqlAlchemy
            table (the function doesn't create the table); the definition is
            used when creating the table. The function has the following
            signature: (``Sqlalchemy.Metadata``) -> ``sqlalchemy.Table``. See
            below notes on how this is used when creating missing tables.
            See this `define table tutorial`_ for how to implement the
            function.

    Notes:
        When attempting to write to a missing database table,
        :class:`SqlAlchemyDB` will handle the situation based on the values
        ``create_if_missing``, ``primary_key_columns`` and ``define_table_f``
        of the passed :class:`TableConfiguration`, as follows:
          - If the table is missing and ``create_if_missing`` is set to
            False (default), :class:`SqlAlchemyDB` will raise an exception.
          - Only when the target table is missing and ``create_if_missing``
            is set to True, table creation is attempted. This is the assumed
            state for all the following cases.
          - If ``define_table_f`` is specified, a new table will be created
            using the table definition returned by ``define_table_f``,
            irrespective of ``primary_key_columns``.
          - If ``primary_key_columns`` is specified and ``define_table_f``
            is None, a new table will be created using the columns specified
            in ``primary_key_columns`` as the primary key. The full column
            list and their types are inferred automatically using the first
            record to be written. See :func:`infer_db_type` for information on
            the how the database column types are inferred from the python
            types. If ``primary_key_columns`` is also None, an auto_increment
            Integer column will be created and used as primary key this is
            done as some databases require a primary key to be specified when
            creating tables.

    Examples:
        A configuration for creating the table if missing using the specified
        columns to create the primary key. ::

            from beam_nuggets.io import relational_db

            table_config = relational_db.TableConfiguration(
                name='students',
                create_if_missing=True,
                primary_key_columns=['id']
            )


        A configuration for creating the table if missing using the specified
        definition. ::

            from sqlalchemy import Table, Integer, String, Column
            from beam_nuggets.io import relational_db

            table_name = 'students'

            def define_students_table(metadata):
                return Table(
                    table_name, metadata,
                    Column(ID, Integer, primary_key=True),
                    Column(NAME, String(100)),
                    Column(AGE, Integer)
                )

            table_config = relational_db.TableConfiguration(
                name=table_name,
                create_if_missing=True,
                define_table_f=define_students_table
            )

    .. _define table tutorial:
       https://docs.sqlalchemy.org/en/latest/core/tutorial.html#define-and
       -create-tables
    """

    def __init__(
        self,
        name,
        define_table_f=None,
        create_if_missing=False,
        primary_key_columns=None,
        create_insert_f=None
    ):
        self.name = name
        self.define_table_f = define_table_f
        self.create_table_if_missing = create_if_missing
        self.primary_key_column_names = primary_key_columns or []
        self.create_insert_f = create_insert_f


class SqlAlchemyDB(object):
    """Provides functionality to read and write from and to relational DBs.
    It uses SqlAlchemy.

    Args:
        source_config (SourceConfiguration): Information for accessing the
            target database.
    """

    def __init__(self, source_config):
        self._source = source_config

        self._SessionClass = sessionmaker(bind=create_engine(self._source.url))
        self._session = None  # will be set in self.start_session()

        self._name_to_table = {}  # tables metadata cache

    def start_session(self):
        create_if_missing = self._source.create_if_missing
        is_database_missing = lambda: not database_exists(self._source.url)
        if create_if_missing and is_database_missing():
            _create_database_tmp(self._source.url)
        self._session = self._SessionClass()

    def close_session(self):
        self._session.close()
        self._session = None

    def read(self, table_name):
        table = self._open_table_for_read(table_name)
        for record in table.records(self._session):
            yield record

    def write_record(self, table_config, record_dict):
        """Writes a single record to the specified table.

        Args:
            table_config (TableConfiguration): specifies the target table,
                 how data should inserted and how to create it if it was
                 missing. See :class:`TableConfiguration` notes on table
                 creation.
            record_dict (dict): the record to be written
        """
        table = self._open_table_for_write(table_config, record_dict)
        table.write_record(
            session=self._session,
            create_insert_f=self._get_create_insert_f(table_config),
            record_dict=record_dict
        )

    def _get_create_insert_f(self, table_config):
        create_insert_f = table_config.create_insert_f
        if not create_insert_f:
            if 'postgresql' in self._source.url.drivername:
                create_insert_f = create_upsert_postgres
            elif 'mysql' in self._source.url.drivername:
                create_insert_f = create_upsert_mysql
            else:
                create_insert_f = create_insert
        return create_insert_f

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


class SqlAlchemyDbException(Exception):
    pass


class _Table(object):
    def __init__(self, table_class, name):
        self._Class = table_class
        self._sqlalchemy_table = table_class.__table__
        self.name = name
        self._column_names = get_column_names_from_table(table_class)

    def records(self, session):
        for record in session.query(self._Class):
            yield self._from_db_record(record)

    def write_record(self, session, create_insert_f, record_dict):
        try:
            insert_stmt = create_insert_f(
                table=self._sqlalchemy_table,
                record=record_dict
            )
            session.execute(insert_stmt)
            session.commit()
        except:
            session.rollback()
            session.close()
            raise

    def _to_db_record(self, record_dict):
        return self._Class(**record_dict)

    def _from_db_record(self, db_record):
        return {col: getattr(db_record, col) for col in self._column_names}


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
                drivername=session.bind.url.drivername,
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


def _get_default_define_f(record, name, primary_key_column_names, drivername):
    def define_table(metadata):
        """Defines an SqlAlchemy database table and adds it to the specified
        metadata object.

        Args:
            metadata (sqlalchemy.Metadata): database metadata.

        Returns:
            sqlalchemy.Table: A database table added to the passed metadata.
        """
        columns = _columns_from_sample_record(
            record=record,
            primary_key_column_names=primary_key_column_names,
            drivername=drivername
        )
        return Table(name, metadata, *columns)

    return define_table


def _columns_from_sample_record(record, primary_key_column_names, drivername):
    if len(primary_key_column_names) > 0:
        primary_key_columns = [
            Column(
                col, infer_db_type(record[col], drivername), primary_key=True
            )
            for col in primary_key_column_names
        ]
        other_columns = [
            Column(col, infer_db_type(value, drivername))
            for col, value in record.iteritems()
            if col not in primary_key_column_names
        ]
    else:
        pri_col_name = 'id'
        while pri_col_name in record.keys():
            pri_col_name += '_'
        primary_key_columns = [Column(pri_col_name, Integer, primary_key=True)]
        other_columns = [
            Column(col, infer_db_type(value, drivername))
            for col, value in record.iteritems()
        ]
    return primary_key_columns + other_columns


def create_insert(table, record):
    """Creates a statement for inserting the passed record to the passed table.
    The created statement is not executed by this function.

    For information on creating insert and update statements Refer to these
    SqlAlchemy `documentation`_ and `tutorial`_.

    Args:
        table (sqlalchemy.sql.schema.Table): database table metadata.
        record (dict): a data record, corresponding to one row, to be inserted.

    Returns:
        sqlalchemy.sql.dml.Insert: a statement for inserting the passed
        record to the specified table.


    .. _documentation: https://docs.sqlalchemy.org/en/latest/core/dml.html
    .. _tutorial: https://docs.sqlalchemy.org/en/latest/core/tutorial.html#insert-expressions
    """
    return generic_insert(table).values(record)


def create_upsert_postgres(table, record):
    """Creates a statement for inserting the passed record to the passed
    table; if the record already exists, the existing record will be updated.
    This uses PostgreSQL `on_conflict_do_update` (hence upsert), and that
    why the returned statement is just valid for PostgreSQL tables. Refer to
    this `SqlAlchemy PostgreSQL documentation`_ for more information.

    The created statement is not executed by this function.

    Args:
        table (sqlalchemy.sql.schema.Table): database table metadata.
        record (dict): a data record, corresponding to one row, to be inserted.

    Returns:
        sqlalchemy.sql.dml.Insert: a statement for inserting the passed
        record to the specified table.

    .. _SqlAlchemy PostgreSQL documentation:
       https://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#insert-on-conflict-upsert
    """
    insert_stmt = postgres_insert(table).values(record)
    return insert_stmt.on_conflict_do_update(
        index_elements=[col for col in table.primary_key],
        set_=record
    )


def create_upsert_mysql(table, record):
    """Creates a statement for inserting the passed record to the passed
    table; if the record already exists, the existing record will be updated.
    This uses MySQL `on_duplicate_key_update` (hence upsert), and that
    why the returned statement is valid only for MySQL tables. Refer to this
    `SqlAlchemy MySQL documentation`_ for more information.

    The created statement is not executed by this function.

    Args:
        table (sqlalchemy.sql.schema.Table): database table metadata.
        record (dict): a data record, corresponding to one row, to be inserted.

    Returns:
        sqlalchemy.sql.dml.Insert: a statement for inserting the passed
        record to the specified table.

    .. _SqlAlchemy MySQL documentation:
       https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql-inser-on-duplicate-key-update
    """
    insert_stmt = mysql_insert(table).values(record)
    return insert_stmt.on_duplicate_key_update(**record)
    # passing dict, i.e. ...update(record), isn't working


def get_column_names_from_table(table_class):
    return [col.name for col in table_class.__table__.columns]


def infer_db_type(val, drivername):
    """Infer a database column type for storing values of the same type as the
    passed variable val in a database identified by drivername.

    Column types are inferred based on the following mapping:

    +-----------------------+--------------------------------------------------------------------------------------------------------+
    | Python type           | Column type                                                                                            |
    +=======================+========================================================================================================+
    | ``bool``              | ``Boolean``                                                                                            |
    +-----------------------+--------------------------------------------------------------------------------------------------------+
    | ``<number>``          | ``Float`` (All python numbers are mapped to Float columns)                                             |
    +-----------------------+--------------------------------------------------------------------------------------------------------+
    | ``datetime.datetime`` | ``DateTime``                                                                                           |
    +-----------------------+--------------------------------------------------------------------------------------------------------+
    | ``datetime.date``     | ``Date``                                                                                               |
    +-----------------------+--------------------------------------------------------------------------------------------------------+
    | ``<default>``         | ``String`` for PostgreSQL and SQLite and String(:const:`VARCHAR_DEFAULT_COL_SIZE`) for other databases |
    +-----------------------+--------------------------------------------------------------------------------------------------------+

    Args:
        val (object): value used to infer the database column type.
        drivername: specifies the database type and driver used for
            connecting to the database (the driver information isn't used to
            infer the column type).

    Returns:
        object: one of sqlalchemy column types.
    """
    for is_type_f, db_type in PYTHON_TO_DB_TYPE:
        if is_type_f(val):
            return db_type
    return (
        String if _does_support_varchar(drivername) else
        String(VARCHAR_DEFAULT_COL_SIZE)
        # It seems only PostgreSQL and SQLite support String columns with
        # not specified length.
    )


VARCHAR_DEFAULT_COL_SIZE = 50


def _does_support_varchar(drivername):
    return 'postgresql' in drivername or 'sqlite' in drivername


def _is_number(x):
    try:
        _ = x + 1
    except:
        return False
    return not hasattr(x, '__len__')


PYTHON_TO_DB_TYPE = [
    # Order matters!
    (lambda x: isinstance(x, bool), Boolean),
    (_is_number, Float),
    (lambda x: isinstance(x, datetime.datetime), DateTime),
    (lambda x: isinstance(x, datetime.date), Date),
]


# TODO Below is a copy of sqlalchemy_utils.create_database with a fix to
#  support creating PostgreSQL DBs using pg8000. We should remove this tmp fix
#  when the following PR is merged:
#  https://github.com/kvesteri/sqlalchemy-utils/pull/356
def _create_database_tmp(url, encoding='utf8', template=None):
    """Issue the appropriate CREATE DATABASE statement.

    :param url: A SQLAlchemy engine URL.
    :param encoding: The encoding to create the database as.
    :param template:
        The name of the template from which to create the new database. At the
        moment only supported by PostgreSQL driver.

    To create a database, you can pass a simple URL that would have
    been passed to ``create_engine``. ::

        create_database('postgres://postgres@localhost/name')

    You may also pass the url from an existing engine. ::

        create_database(engine.url)

    Has full support for mysql, postgres, and sqlite. In theory,
    other database engines should be supported.
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
    result_proxy = None

    if engine.dialect.name == 'postgresql':
        if engine.driver == 'psycopg2':
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
            engine.raw_connection().set_isolation_level(
                ISOLATION_LEVEL_AUTOCOMMIT
            )

        if not template:
            template = 'template1'

        text = "CREATE DATABASE {0} ENCODING '{1}' TEMPLATE {2}".format(
            quote(engine, database),
            encoding,
            quote(engine, template)
        )
        result_proxy = engine.execute(text)

    elif engine.dialect.name == 'mysql':
        text = "CREATE DATABASE {0} CHARACTER SET = '{1}'".format(
            quote(engine, database),
            encoding
        )
        result_proxy = engine.execute(text)

    elif engine.dialect.name == 'sqlite' and database != ':memory:':
        if database:
            engine.execute("CREATE TABLE DB(id int);")
            engine.execute("DROP TABLE DB;")

    else:
        text = 'CREATE DATABASE {0}'.format(quote(engine, database))
        result_proxy = engine.execute(text)

    if result_proxy is not None:
        result_proxy.close()
    engine.dispose()
