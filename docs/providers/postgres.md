# The `postgres` provider

Topics in PostgreSQL 18 (the only version supported and tested). Artifact `rawdata-client-provider-postgres`,
module `io.descoped.rawdata.provider.postgres`.

## Configuration

| Key | Default | |
|---|---|---|
| `rawdata.client.provider` | | `postgres` |
| `postgres.driver.host` | `localhost` | |
| `postgres.driver.port` | `5432` | |
| `postgres.driver.user` | | required |
| `postgres.driver.password` | | required |
| `postgres.driver.database` | | required |
| `postgres.driver.max-pool-size` | `10` | connections in the client's pool (HikariCP) |
| `rawdata.postgres.consumer.prefetch-size` | `100` | messages a consumer reads per query |
| `rawdata.postgres.consumer.prefetch-poll-interval-when-empty` | `250` | ms a waiting consumer sleeps between queries |

Each client owns a connection pool, closed with the client. Connections run at READ COMMITTED, whatever the
database's default, and report `ApplicationName=rawdata-postgres`.

## Tables

Three tables, shared by all topics; the topic is a column, always a bind parameter, never part of the SQL:

| Table | Holds | Key |
|---|---|---|
| `rawdata_message` | id, position, ordering group, sequence number | `(topic, id)`; index on `(topic, position)` |
| `rawdata_content` | a message's named content, in order | `(topic, id, ordinal)`; deleted with the message |
| `rawdata_metadata` | metadata entries | `(topic, name)` |

The first client creates them if they are missing, under an advisory lock, so clients that start together create
them once. A database role that may only `SELECT`, `INSERT`, `UPDATE` and `DELETE` on existing tables can run a
client; creating them needs the `CREATE` privilege once.

## Writing

A publish takes the topic's transaction-level advisory lock (`pg_advisory_xact_lock`), reads the topic's last id,
gives the new ids after it and inserts, all in one transaction, so a consumer that has read up to an id never misses
a later commit. If the last id is not the last this client wrote, another process wrote the topic, and the publish
throws `RawdataWriterConflictException` and rolls back (see
[one writer per topic](../contract.md#one-writer-per-topic)).

A metadata key is at most 2000 bytes in UTF-8, below what a PostgreSQL index entry holds; a longer one is refused
with `IllegalArgumentException`.

## For development

`docker compose up -d` in `rawdata-client-provider-postgres` starts PostgreSQL 18 on port 5432 with user and
database `rdc`, password `rdc`.
