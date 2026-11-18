# Rawdata Client Project

A provider based Java API for writing and reading ordered streams of raw data messages. A topic holds messages
ordered by id, a UUIDv7 laid out as PostgreSQL 18's `uuidv7()`, each with an external position, an optional ordering
group and sequence number, and named binary content; every topic also has a small metadata store. Applications use
`RawdataClient` and pick a provider by name. Consumers are pulled: the application takes one message or a batch of
the size it chooses, and `RawdataCopy` copies a topic between providers in batches.

| Module | What it is |
|---|---|
| `rawdata-client-api` | the API, the provider service, and the `memory` and `discard` providers |
| `rawdata-client-tck` | the contract tests every provider passes, as JUnit 6 tests to extend |
| `rawdata-client-provider-postgres` | provider `postgres`, on PostgreSQL 18 (tables `rawdata_message`, `rawdata_content`, `rawdata_metadata`, created when missing) |
| `rawdata-client-provider-avro` | provider `filesystem` (Avro files in a local folder), and the Avro engine the `gcs` provider shares |
| `rawdata-client-provider-gcs` | provider `gcs` (Avro files in a Google Cloud Storage bucket) |
| `rawdata-copy-util` | `rawdata-copy`, a command-line tool that copies topics from one provider to another |
| `rawdata-client-bom` | BOM to align the versions |

Maven group `io.descoped.rawdata`. Version 3.0.0 is in progress on `master` (Java 17); the last release is 2.0.0.

## Working with the code

Requires JDK 17 or newer, and Docker for the postgres and gcs tests (Testcontainers runs PostgreSQL 18 and a fake
GCS server); Maven comes with the wrapper.

```sh
./mvnw verify                    # build and test every module
./mvnw verify javadoc:javadoc    # plus Javadoc checks
```

For manual runs against PostgreSQL 18: `docker compose up -d` in `rawdata-client-provider-postgres`.

## Copying topics and moving between stores

`rawdata-copy` copies topics from one store to another, keeping ids and order: a backup, or a move from one provider
or cloud to another, for example from `gcs` to `postgres` on another cloud. When a target topic has messages, the copy
continues after its last id, so a copy that stopped can be run again.

It is distributed as `rawdata-copy-<version>.zip` (the jar and `lib/` with every provider) on GitHub releases, not
on Maven Central. To build it: `./mvnw -pl rawdata-copy-util -am package -DskipTests`, which writes
`rawdata-copy-util/target/rawdata-copy-<version>.zip`.

```properties
# copy.properties: a source.* and a target.* provider configuration
source.rawdata.client.provider=gcs
source.gcs.bucket-name=my-bucket
target.rawdata.client.provider=postgres
target.postgres.driver.host=db.example.com
target.postgres.driver.user=rdc
target.postgres.driver.password=secret
target.postgres.driver.database=rdc
```

```sh
unzip rawdata-copy-<version>.zip
java -jar rawdata-copy-<version>/rawdata-copy-util-<version>.jar --config copy.properties --metadata topic-a topic-b:renamed
java -jar rawdata-copy-<version>/rawdata-copy-util-<version>.jar --help
```

It runs on the class path (the Google Cloud libraries do not work on the module path). Exit status 0: copied; 1: a
copy failed, run it again to continue; 2: wrong arguments or configuration, or a store that cannot be opened, nothing
copied.

To move a running system to another store:

1. Copy every topic while the old system runs (`--metadata` to bring checkpoints and other metadata along).
2. Stop the producers on the old store.
3. Run the same copy again: it copies only what came since.
4. Point the producers, and then the consumers, at the new store.

Nothing may write to a target topic before the last copy of step 3. A message written there first would move the
topic's last id past the messages still to be copied, and the copy, which continues after the last id, would skip
them.

## License

Apache License 2.0, see [LICENSE](LICENSE).
