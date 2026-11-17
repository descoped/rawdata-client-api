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
| `rawdata-client-provider-postgres` | provider `postgres`, on PostgreSQL 18 |
| `rawdata-client-provider-gcs` | providers `gcs` (Avro files in Google Cloud Storage) and `filesystem` (Avro files in a local folder) |
| `rawdata-copy-util` | copies topics from one provider to another |
| `rawdata-client-bom` | BOM to align the versions |

Maven group `io.descoped.rawdata`. Version 3.0.0 is in progress on `master` (Java 17); the last release is 2.0.0.

## Working with the code

Requires JDK 17 or newer; Maven comes with the wrapper.

```sh
./mvnw verify                    # build and test every module
./mvnw verify javadoc:javadoc    # plus Javadoc checks
```

For manual runs against PostgreSQL 18: `docker compose up -d` in `rawdata-client-provider-postgres`.

## License

Apache License 2.0, see [LICENSE](LICENSE).
