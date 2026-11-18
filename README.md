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
The [documentation](docs/README.md) covers getting started, the contract, each provider, the copy tool and writing a
provider; the [CHANGELOG](CHANGELOG.md) lists what changed from 2.x.

## Working with the code

Requires JDK 17 or newer, and Docker for the postgres and gcs tests (Testcontainers runs PostgreSQL 18 and a fake
GCS server); Maven comes with the wrapper.

```sh
./mvnw verify                    # build and test every module
./mvnw verify javadoc:javadoc    # plus Javadoc checks
src/it/jpms/run.sh               # JPMS smoke test: the providers on the module path and the class path
```

For manual runs against PostgreSQL 18: `docker compose up -d` in `rawdata-client-provider-postgres`.

## Copying topics

`rawdata-copy` copies topics between stores, keeping ids and order: a backup, or a move from one provider or cloud to
another. It continues after the target's last id, so a stopped copy can be run again. It is distributed as a zip on
GitHub releases; see [the copy tool](docs/copy-tool.md).

## License

Apache License 2.0, see [LICENSE](LICENSE).
