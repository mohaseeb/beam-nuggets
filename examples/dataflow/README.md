## Manage a local postgresql instance
start
```bash
docker run --rm -d --name test-postgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=postgres \
    postgres
```
run a command
```bash
docker exec -it test-postgres psql -U postgres -d calendar -c "select * from months;"
```
connect to it
```bash
docker exec -it test-postgres \
    psql -U postgres
```
close
```bash
docker stop test-postgres
```
