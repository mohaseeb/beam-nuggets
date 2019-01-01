from __future__ import division, print_function

from apache_beam import PTransform, DoFn, ParDo, Create

from .sqlalchemy_db import SqlAlchemyDB


class Read(PTransform):
    def __init__(self, source_config, table_name, *args, **kwargs):
        super(Read, self).__init__(*args, **kwargs)
        self._read_args = dict(
            source_config=source_config,
            table_name=table_name
        )

    def expand(self, pcoll):
        return (
            pcoll
            | Create([self._read_args])
            | ParDo(_ReadFromRelationalDBFn())
        )


class _ReadFromRelationalDBFn(DoFn):
    def process(self, element):
        db_args = dict(element)
        table_name = db_args.pop('table_name')
        db = SqlAlchemyDB(**db_args)
        db.start_session()
        try:
            for record in db.read(table_name):
                yield record
        except:
            raise
        finally:
            db.close_session()
