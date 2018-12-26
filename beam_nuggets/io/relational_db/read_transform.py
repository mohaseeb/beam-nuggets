from __future__ import division, print_function

from apache_beam import PTransform, DoFn, ParDo, Create

from .sqlalchemy_db import SqlAlchemyDB


class ReadFromRelationalDB(PTransform):
    def __init__(
        self,
        table_name,
        drivername,
        host=None,
        port=None,
        database=None,
        username=None,
        password=None,
        *args,
        **kwargs  # TODO check missing *args
    ):
        super(ReadFromRelationalDB, self).__init__(*args, **kwargs)
        self._db_args = dict(
            host=host,
            port=port,
            drivername=drivername,
            database=database,
            username=username,
            password=password,
            table_name=table_name,
        )

    def expand(self, pcoll):
        return (
            pcoll
            | Create([self._db_args])
            | ParDo(_ReadFromRelationalDBFn())
        )


class _ReadFromRelationalDBFn(DoFn):
    def process(self, element):
        table_name = element.pop('table_name')
        db_args = element
        db = SqlAlchemyDB(**db_args)
        db.start_session()
        for record in db.read(table_name):
            yield record
        db.close_session()
