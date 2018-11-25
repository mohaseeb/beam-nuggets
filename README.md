A collection of transforms for the Apache beam python SDK.
# WORK IN PROGRESS
# Installation

# Supported transforms
## IO
### Read and write from and to relational databases  
* ReadFromRelationalDB
* WriteToRelationalDB

These transforms uses SqlAlchemy, and hence can be used to read/write from/to
any database supported by SqlAlchemy. These transforms are tested against 
below databases:
* Sqlite
<!--* postgres-->
<!--* mysql-->
### Read from CSV
* ReadFromCsv
## Others
* ...
# TODO
* Extend WriteToRelationalDB to support lazy db and table creation.
* csv_to_sqlite.py in GCP
* WriteToCsv
* Cleanup and DOCs for all transforms
* DOCs DOCs DOCs
* More examples
* Investigate using ParDo instead of Source/Sink for RelationalDB Read/Write (
as recommended by beam team)