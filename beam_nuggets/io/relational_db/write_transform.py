from __future__ import division, print_function

from apache_beam import PTransform, DoFn, ParDo

from sqlalchemy_db import SqlAlchemyDB


class WriteToRelationalDB(PTransform):
    def __init__(
        self,
        db_config,
        table_name,
        create_db_if_missing=False,
        create_table_if_missing=False,
        primary_key_columns=None,
        *args,
        **kwargs
    ):
        super(WriteToRelationalDB, self).__init__(*args, **kwargs)
        self._write_args = dict(
            db_config=db_config,
            table_name=table_name,
            primary_key_columns=primary_key_columns,
            create_db_if_missing=create_db_if_missing,
            create_table_if_missing=create_table_if_missing,
        )

    def expand(self, pcoll):
        return pcoll | ParDo(_WriteToRelationalDBFn(self._write_args))


class _WriteToRelationalDBFn(DoFn):
    def __init__(self, write_args, *args, **kwargs):
        super(_WriteToRelationalDBFn, self).__init__(*args, **kwargs)
        self._db_args = dict(write_args)
        self._table_name = self._db_args.pop('table_name')

    def start_bundle(self):
        self._db = SqlAlchemyDB(**self._db_args)
        self._db.start_session()

    def process(self, element):
        assert isinstance(element, dict)
        self._db.write_record(self._table_name, element)

    def finish_bundle(self):
        self._db.close_session()
