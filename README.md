A collection of random transforms that I use on my Apache beam python 
pipelines. Many are simple (or trivial) transforms. The most useful ones are 
those for reading/writing from/to relational databases.
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
    records = p | "Reading records from db" >> ReadFromRelationalDB(
        drivername='postgresql',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
        table_name='months',
    )
    records | 'Writing to stdout' >> beam.Map(print)
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
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    months | 'Writing to Sqlite table' >> WriteToRelationalDB(
        drivername='postgresql',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
        table_name='months',
        create_db_if_missing=True,
        create_table_if_missing=True,
    )
```

These transforms uses SqlAlchemy, and hence can be used to read/write from/to
any relational database (sql database) supported by SqlAlchemy. These 
transforms has been tested against below databases:
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
* SelectFromNestedDict
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.transforms import SelectFromNestedDict

inputs = [
    {
        'name': {'first': 'Star', 'second': 'Light'},
        'address': {'st': 'Jupiter', 'flat': 3},
        'email': 's@l.no'
    },
    {
        'name': {'first': 'Mark', 'second': 'Sight'},
        'address': {'st': 'Loon', 'flat': 5},
        'email': 'm@s.no'
    }
]
with beam.Pipeline(options=PipelineOptions()) as p:
    nested = p | "Reading nested dicts" >> beam.Create(inputs)
    transformed = nested | "filtering" >> beam.ParDo(SelectFromNestedDict(
        keys=['name.first', 'address.st', 'email'],
        # deepest_key_as_name=True,
    ))
    transformed | 'Writing to stdout' >> beam.Map(print)

# output: 
# {'address_st': 'Jupiter', 'name_first': 'Star', 'email': 's@l.no'}
# {'address_st': 'Loon', 'name_first': 'Mark', 'email': 'm@s.no'}
# or (if deepest_key_as_name is set to True)
# {'st': 'Jupiter', 'email': 's@l.no', 'first': 'Star'}
# {'st': 'Loon', 'email': 'm@s.no', 'first': 'Mark'}
```
* ParseJson
* AssignUniqueId
# TODO 
* Cleanup and DOCs for all transforms
* WriteToCsv
* run examples on GCP
* More examples (write to postgres)
* Sql queries support in ReadToRelationalDB
* Check JdbcIO for inspiration for WriteToRelationalDB/ReadToRelationalDB
* WriteToRelationalDB, log when db or table is created
* WriteToRelationalDB, extend the automatic column type inference logic.
* WriteToRelationalDB Support specifying primary key(s) when writing to new 
table
* Investigate using ParDo instead of Source/Sink for RelationalDB Read/Write (
as recommended by beam team)