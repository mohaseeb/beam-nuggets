#!/usr/bin/env bash

python write_to_relational_db.py

# execute below to examine the written data
# sqlite3 /tmp/test.db 'select * from months'