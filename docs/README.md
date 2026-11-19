# Rawdata client documentation

The rawdata client writes and reads ordered streams of raw data messages, with one API over PostgreSQL, a local
folder, Google Cloud Storage and Amazon S3. These pages take you from your first topic to how every part works.

What changed in each release is in the [CHANGELOG](../CHANGELOG.md).

## Start here

1. [Tutorial](tutorial.md): from nothing to a producer and a consumer on a real store, step by step. Start here if
   you have not used the client before.
2. [Concepts](concepts.md): every idea the API rests on, in one place: topics, messages, ids and order, producers,
   consumers, cursors, metadata, the one writer of a topic, visibility and durability, closing.
3. [Logging and the module path](logging-and-the-module-path.md): how to route the client's logging into your
   application's, and how to run it as a JPMS module.

## Modules

One page per module. Each has its coordinates, a quick start, its concepts, how it works, its configuration,
failure modes, testing and a reference.

| Page | Read it when |
|---|---|
| [`rawdata-client-api`](modules/rawdata-client-api.md) | always: the API, ids, the provider service, `memory` and `discard`, `RawdataCopy`, the `spi` base classes |
| [`rawdata-client-bom`](modules/rawdata-client-bom.md) | you add the client to a Maven or Gradle build |
| [`rawdata-client-provider-postgres`](modules/rawdata-client-provider-postgres.md) | you store topics in PostgreSQL 18 |
| [`rawdata-client-provider-filesystem`](modules/rawdata-client-provider-filesystem.md) | you store topics in a local folder |
| [`rawdata-client-provider-gcs`](modules/rawdata-client-provider-gcs.md) | you store topics in a Google Cloud Storage bucket |
| [`rawdata-client-provider-s3`](modules/rawdata-client-provider-s3.md) | you store topics in an Amazon S3 bucket |
| [`rawdata-client-provider-avro`](modules/rawdata-client-provider-avro.md) | you use `filesystem`, `gcs` or `s3`: the upload window, segments, visibility, shutdown and recovery; or you write a provider on files or objects |
| [`rawdata-client-tck`](modules/rawdata-client-tck.md) | you test your application, or write or change a provider |
| [`rawdata-copy-util`](modules/rawdata-copy-util.md) | you copy topics, or move a system to another store or cloud |

How `filesystem`, `gcs` and `s3` buffer and upload, the upload window, is on the
[Avro engine](modules/rawdata-client-provider-avro.md#upload-window) page.

## The contract

- [The contract](contract.md): what every provider guarantees, the same in all of them: topics, ids, ordering,
  reading, metadata and closing. `rawdata-client-tck` checks it.

## Operating

- [Troubleshooting](troubleshooting.md): build, test and usage problems, and what to do about them.

## Developing

- [Architecture](architecture.md): how the modules fit together, finding a provider, ids, how each provider keeps a
  topic in order with one writer, reading, closing.
- [Building and testing](building-and-testing.md): JDK 17, the build, the test suites and the TCK, the JPMS smoke
  test, CI, the IDE.
- [Dependencies](dependencies.md): latest versions, one version of each library, exclusions and bans, adding a
  library.
- [Writing a provider](writing-a-provider.md): the provider service, the base classes, `AvroStore`, the contract
  tests.
- [Releasing](releasing.md): what a release involves. Releases are made only by the owner.
- [Contributing](../CONTRIBUTING.md): issues, pull requests and the rules a change must keep.
