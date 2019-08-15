## Overview
`write_to_relational_db.py` defines and executes a pipeline that 
writes a list of records to a relational database using beam-nuggets' [relational_db.Write](http://mohaseeb.com/beam-nuggets/beam_nuggets.io.relational_db.html#beam_nuggets.io.relational_db.Write) 
transform. The target database and the 
beam runner on which the pipeline will be executed are passed to 
`write_to_relational_db.py` as command line arguments. Each one of the shell
 scripts invokes `write_to_relational_db.py` using different 
 parameterization of the target database and the beam runner.
  
## Write to SQLite (DirectRunner)
When invoked without arguments, `write_to_relational_db.py` pipeline will be
 executed locally and will records write to an
 SQLite db at `/tmp/test.db`. Follow below steps to run the example.
* Execute the pipline
```bash
./write_to_sqlite_local_direct_runner.sh
``` 
* Check the contents written to the DB
```bash
sqlite3 /tmp/test.db 'select * from months'
```
## Write to PostgreSQL (DirectRunner)
`write_to_postgresql_local_direct_runner.sh` will execute the pipeline locally 
and the records will be written to a PostgreSQL instance running on localhost. Follow 
below steps to run the example (assumes [docker](https://docs.docker.com/install/linux/docker-ce/ubuntu/) is installed).
* Start a PostgreSQL instance
```bash
docker run --rm -d --name test-postgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    postgres
```
* Wait until the instance is started, then execute the pipeline
```bash
./write_to_postgresql_local_direct_runner.sh
```
* Check the contents written to the DB
```bash
docker exec -it test-postgres psql -U postgres -d calendar -c "select * from months;"
```
* To stop the PostgreSQL instance, run
```bash
docker stop test-postgres
```
## Write to PostgreSQL (DataflowRunner)
`write_to_postgresql_gcp_dataflow_runner.sh` will execute the pipeline in 
Google Cloud Platform (GCP) using the DataflowRunner. The records will be 
written to a PostgreSQL instance running on (GCP). Below are the steps needed 
to execute the example.
- GCP environment setup (a crude list)
  - To provide connectivity between the GCP workers (that will be spun to 
  execute the pipeline) and the PostgreSQL instance, all resources (DB, 
  workers) were created on the same GCP region (`us-central1` in my case),
  so the workers can talk to the DB using my GCP project private networks (
  one for the DB, and one for the workers setup. GCP automatically takes care of setting up the 
  necessary connectivity between the DB network and the workers network).
  - A PostgreSQL instance is created on us-central1 region. `Private IP` 
  connectivity 
  is then enabled for the instance (under `Connections` on the PostgreSQL 
  dashboard). The private IP of the instance is used by the pipeline (see `write_to_postgresql_gcp_dataflow_runner.sh`).
  See [here](https://cloud.google.com/sql/docs/postgres/private-ip) for more
   information.
  - A `bucket` on the GCP `Storage` was created.
  - For the script to authenticate against GCP, I followed the
    instructions
    [found here](https://cloud.google.com/docs/authentication/production#auth-cloud-compute-engine-python).
    I used the authetnication strategy that sets the environment
    variable `GOOGLE_APPLICATION_CREDENTIALS`.
  - The pipeline was executed while explicitly specifying the region where 
  the pipeline to be executed (see `write_to_postgresql_gcp_dataflow_runner.sh`). 
  The default region was `us-central1`, at the time of writing this document.  
- To execute the pipleline, run
```bash
./write_to_postgresql_gcp_dataflow_runner.sh
``` 
- To examine the contents, connect to the PostgreSQL instance using, e.g., 
`Connect using Cloud Shell` option on the PostgreSQL dashboard.