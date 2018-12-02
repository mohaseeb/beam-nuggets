A collection of transforms for the Apache beam python SDK.
# !!WORK IN PROGRESS!!
# Installation
```bash
git clone git@github.com:mohaseeb/beam_nuggets.git
cd beam_nuggets
pip install .
```
# Supported transforms
## IO
### Read and write from and to relational databases  
* ReadFromRelationalDB
<!--read from sql database-->
<!--read from postgres postgresql-->
<!--read from mysql-->
<!--read from oracle-->
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import ReadFromRelationalDB

with beam.Pipeline(options=PipelineOptions()) as p:
    students = p | "Reading students records" >> ReadFromRelationalDB(
        drivername='sqlite',
        database='/tmp/csv_to_sqlite_dummy.sqlite',
        table_name='students',
    )
    students | 'Writing to stdout' >> beam.Map(lambda r: print(r))
```
* WriteToRelationalDB
<!--write to sql database-->
<!--write to postgres postgresql-->
<!--write to mysql-->
<!--write to oracle-->
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import WriteToRelationalDB

with beam.Pipeline(options=PipelineOptions()) as p:
    students = p | "Reading students records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    students | 'Writing to Sqlite table' >> WriteToRelationalDB(
        drivername='sqlite',
        database='/tmp/csv_to_sqlite_dummy22.sqlite',
        table_name='months',
        create_db_if_missing=True,
        create_table_if_missing=True,
    )
```

These transforms uses SqlAlchemy, and hence can be used to read/write from/to
any database supported by SqlAlchemy. These transforms are tested against 
below databases:
* Sqlite
<!--* postgres-->
<!--* mysql-->
### Read from CSV
* ReadFromCsv
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import ReadFromCsv

path_to_csv = get_csv_file_path()
with beam.Pipeline(options=PipelineOptions()) as p:
    students = p | "Reading students records" >> ReadFromCsv(path_to_csv)
    students | 'Writing to stdout' >> beam.Map(lambda r: print(r))

```
## Others
* ...
# TODO
* csv_to_sqlite.py in GCP
* WriteToCsv
* Cleanup and DOCs for all transforms
* DOCs DOCs DOCs
* More examples (write to postgres)
* WriteToRelationalDB Support specifying primary key(s) when writing to new 
table
* Investigate using ParDo instead of Source/Sink for RelationalDB Read/Write (
as recommended by beam team)