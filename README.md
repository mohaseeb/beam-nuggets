A collection of transforms for Apache beam python SDK.
# Installation

# Supported transforms
## IO
* WriteToRelationalDB
## Others
* ...
# TODO
* Extend WriteToRelationalDB to support lazy db and table creation.
* csv_to_sqlite.py in GCP
* Tests for RelationalDBIO
* Cleanup and DOCs for other transforms
* More examples
* Investigate using ParDo instead of Source/Sink for RelationalDB Read/Write (
as recommended by beam team)