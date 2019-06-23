[![PyPI](https://img.shields.io/pypi/v/beam-nuggets.svg)](https://pypi.org/project/beam-nuggets/) [![PyPI - Downloads](https://img.shields.io/pypi/dm/beam-nuggets.svg)](https://pypi.org/project/beam-nuggets/)

# About
A collection of random transforms for the [Apache beam](https://beam.apache.org/) python SDK . Many are 
simple transforms. The most useful ones are those for 
reading/writing from/to relational databases.
# Installation
* Using pip
```bash
pip install beam-nuggets
```
* From source
```bash
git clone git@github.com:mohaseeb/beam-nuggets.git
cd beam-nuggets
pip install .
```
# Usage
Write data to an SQLite table using beam-nugget's 
[relational_db.Write](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Write) transform.
```python
# write_sqlite.py contents
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

records = [
    {'name': 'Jan', 'num': 1},
    {'name': 'Feb', 'num': 2}
]

source_config = relational_db.SourceConfiguration(
    drivername='sqlite',
    database='/tmp/months_db.sqlite',
    create_if_missing=True  # create the database if not there 
)

table_config = relational_db.TableConfiguration(
    name='months',
    create_if_missing=True,  # automatically create the table if not there
    primary_key_columns=['num']  # and use 'num' column as primary key
)
    
with beam.Pipeline(options=PipelineOptions()) as p:  # Will use local runner
    months = p | "Reading month records" >> beam.Create(records)
    months | 'Writing to DB' >> relational_db.Write(
        source_config=source_config,
        table_config=table_config
    )
```
Execute the pipeline
```bash
python write_sqlite.py 
```
Examine the contents
```bash
sqlite3 /tmp/months_db.sqlite 'select * from months'
# output:
# 1.0|Jan
# 2.0|Feb
```
To write the same data to a PostgreSQL table instead, just create a suitable 
[relational_db.SourceConfiguration](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db_api.html#beam_nuggets.io.relational_db_api.SourceConfiguration) as follows.
```python
source_config = relational_db.SourceConfiguration(
    drivername='postgresql+pg8000',
    host='localhost',
    port=5432,
    username='postgres',
    password='password',
    database='calendar',
    create_if_missing=True  # create the database if not there 
)
```
Click [here](https://github.com/mohaseeb/beam-nuggets/tree/master/examples/dataflow/)
for more examples, including writing to PostgreSQL in Google Cloud Platform 
using the DataFlowRunner. 
<br><br>
An example showing how you can use beam-nugget's [relational_db.Read](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Read) 
transform to read from a PostgreSQL database table. 
```python
from __future__ import print_function
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_nuggets.io import relational_db

with beam.Pipeline(options=PipelineOptions()) as p:
    source_config = relational_db.SourceConfiguration(
        drivername='postgresql+pg8000',
        host='localhost',
        port=5432,
        username='postgres',
        password='password',
        database='calendar',
    )
    records = p | "Reading records from db" >> relational_db.Read(
        source_config=source_config,
        table_name='months',
        query='select num, name from months'  # optional. When omitted, all table records are returned. 
    )
    records | 'Writing to stdout' >> beam.Map(print)
```
See [here](https://github.com/mohaseeb/beam-nuggets/tree/master/examples) for more examples.
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
* [kafkaio.KafkaProduce](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.kafkaio.html#beam_nuggets.io.kafkaio.KafkaProduce) for writing to Kafka topics.
* [kafkaio.KafkaConsume](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.kafkaio.html#beam_nuggets.io.kafkaio.KafkaConsume) for consuming from kafka topics.
* [csvio.Read](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.csvio.html#beam_nuggets.io.csvio.Read)
for reading CSV files.
<!--read from sql database-->
<!--read from postgres postgresql-->
<!--read from mysql-->
<!--read from oracle-->
<!--write to sql database-->
<!--write to postgres postgresql-->
<!--write to mysql-->
<!--write to oracle-->
<!--read from kafka topic-->
<!--write to kafka topic-->
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
* Make changes on dedicated dev branches
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
* After merging the accepted PR and updating the local master, upload a new 
build to pypi.
```bash
cd $BEAM_NUGGETS_ROOT
scripts/build_test_deploy.sh
```
# Backlog 
* versioned docs?
* Summarize the investigation of using Source/Sink Vs ParDo(and GroupBy) for IO
* more nuggets: WriteToCsv
* Investigate readiness of SDF ParDo, and possibility to use for relational_db.Read
* integration tests
* DB transforms failures handling on IO transforms
* more nuggets: Elasticsearch, Mongo 
* WriteToRelationalDB, logging

# Licence
MIT
