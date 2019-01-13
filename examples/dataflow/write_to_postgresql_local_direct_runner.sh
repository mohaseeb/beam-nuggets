#!/usr/bin/env bash

python write_to_relational_db.py \
    --drivername postgresql+pg8000 \
    --host localhost \
    --port 5432 \
    --database calendar \
    --username postgres \
    --password postgres
