A collection of random transforms that I use on my Apache beam python 
pipelines. Many are simple (or trivial) transforms. The most useful ones are 
those for reading/writing from/to relational databases.
# Installation
```bash
git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets
pip install .
```
# Usage
Below example shows how you can use beam-nugget's [relational_db.Read](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Read) 
transform to read from a PostgreSQL database table. 
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
An example to write to PostgreSQL table using beam-nugget's 
[relational_db.Write](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Write) transform.
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

records = [
    {'name': 'Jan', 'num': 1},
    {'name': 'Feb', 'num': 2}
]

source_config = relational_db.SourceConfiguration(
    drivername='postgresql',
    host='localhost',
    port=5432,
    username='postgres',
    password='password',
    database='calendar',
    create_if_missing=True  # create the database if not there 
)

table_config = relational_db.TableConfiguration(
    name='months',
    create_if_missing=True,  # automatically create the table if not there
    primary_key_columns=['num']  # and use 'num' column as primary key
)
    
with beam.Pipeline(options=PipelineOptions()) as p:
    months = p | "Reading month records" >> beam.Create(records)
    months | 'Writing to DB' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    )
```
# Supported transforms
### IO
* [relational_db.Read](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Read) 
for reading from relational database tables. 
* [relational_db.Write](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Write) 
for writing to relational database tables.
<br>Above transforms uses [SqlAlchemy](https://www.sqlalchemy.org/) to communicate with the database, 
and hence they can read from and write to all relational databases supported
 by SqlAlchemy. 
The transforms [are tested](https://github.com/mohaseeb/beam-nuggets/tree/master/beam_nuggets/io/test) against PostgreSQL, MySQL and SQLite.   
<!--read from sql database-->
<!--read from postgres postgresql-->
<!--read from mysql-->
<!--read from oracle-->
<!--write to sql database-->
<!--write to postgres postgresql-->
<!--write to mysql-->
<!--write to oracle-->
* [csvio.Read](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.csvio.html#beam_nuggets.io.csvio.Read)
for reading CSV files.
### Others
* [SelectFromNestedDict](http://mohaseeb.com/beam-nuggets/beam_nuggets.transforms.nested_dict.html#beam_nuggets.transforms.nested_dict.SelectFromNestedDict)
Selects a subset from records formed of nested dictionaries.
* [ParseJson](beam_nuggets.transforms.json_.html#beam_nuggets.transforms.json_.ParseJson)
* [AssignUniqueId](beam_nuggets.transforms.json_.html#beam_nuggets.transforms.json_.ParseJson)
# Documentation
See [here](http://mohaseeb.com/beam-nuggets/).
# Development
* Install
```bash
git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets
export BEAM_NUGGETS_ROOT=`pwd`
pip install -e .[dev]
```
* Make changes on separate branches
* Run tests
```bash
cd $BEAM_NUGGETS_ROOT
python -m unittest discover -v
```
* Generate docs
```bash
cd $BEAM_NUGGETS_ROOT
docs/generate_docs.sh
```
* Create a PR against master.

# Backlog 
* version docs? 
* upload to pypi
* Summarize the investigation of using Source/Sink Vs ParDo for IO
* Example how to run on GCP
* Sql queries support in ReadToRelationalDB
* more nuggets: WriteToCsv
* integration tests
* DB transforms failures handling
* DB transforms for unbounded sources (e.g. keep sessions alive)
* more nuggets: Elasticsearch, Mongo 
* WriteToRelationalDB, logging

# Licence
MIT
