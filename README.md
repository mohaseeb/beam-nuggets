A collection of random transforms that I use on my Apache beam python 
pipelines. Many are simple (or trivial) transforms. The most useful ones are 
those for reading/writing from/to relational databases.
# !!WORK IN PROGRESS!!
See TODO at the bottom.
# Installation
```bash
git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets
pip install .
```
# Supported transforms
## IO
### Read and write from and to relational databases  
* Read
<!--read from sql database-->
<!--read from postgres postgresql-->
<!--read from mysql-->
<!--read from oracle-->
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    source_config = relational_db.SourceConfiguration(
        drivername='postgresql',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
    )
    records = p | "Reading records from db" >> relational_db.Read(
        source_config=source_config,
        table_name='months',
    )
    records | 'Writing to stdout' >> beam.Map(print)
```
* Write
<!--write to sql database-->
<!--write to postgres postgresql-->
<!--write to mysql-->
<!--write to oracle-->
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create([
        {'name': 'Jan', 'num': 1},
        {'name': 'Feb', 'num': 2},
    ])
    source_config = relational_db.SourceConfiguration(
        drivername='postgresql',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
        create_if_missing=True,
    )
    table_config = relational_db.TableConfiguration(
        name='months',
        create_if_missing=True
    )
    months | 'Writing to DB' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    )
```

These transforms uses SqlAlchemy, and hence can be used to read/write from/to
any relational database (sql database) supported by SqlAlchemy. These 
transforms has been tested (brief tests) on below databases:
* Sqlite
* PostgreSQL
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
* Idempotency in WriteToRelationalDB
* Summarize the investigation of using Source/Sink Vs ParDo for IO
    - send to beam mailing list about the "ParDo -> GroupByKey -> ParDo" 
      read pattern.
* Sql queries support in ReadToRelationalDB
* Cleanup and DOCs for all transforms
* upload to pypi
* more nuggets: WriteToCsv
* Example how to run on GCP
* integration tests
* DB transforms failures handling
* DB transforms for unbounded sources (e.g. keep sessions alive)
* more nuggets: Elasticsearch, Mongo 
* WriteToRelationalDB, logging

# Licence
MIT
