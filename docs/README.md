# Rawdata client documentation

3.0.0 is a clean break from 2.x: ids, storage layouts and configuration keys changed, and 3.0 does not read data
written by 2.x. See the [CHANGELOG](../CHANGELOG.md).

## Using the library

- [Getting started](getting-started.md): dependencies, a first producer and consumer, logging, the module path.
- [The contract](contract.md): topics, ids, ordering, reading, metadata and closing, the same in every provider.
- Providers:
  - [`postgres`](providers/postgres.md): PostgreSQL 18.
  - [`filesystem`](providers/filesystem.md): Avro files in a local folder.
  - [`gcs`](providers/gcs.md): Avro files in a Google Cloud Storage bucket.
  - [`s3`](providers/s3.md): Avro files in an Amazon S3 bucket.
  - [`memory` and `discard`](providers/memory-and-discard.md): for tests.
- [The copy tool](copy-tool.md): copying topics, and moving a system from one store or cloud to another.

## Working on the code

- [Architecture](architecture.md): the modules, finding a provider, ids, the order of a topic and its one writer,
  reading, closing.
- [Building and testing](building-and-testing.md): JDK 17, the build, the test suites and the TCK, the JPMS smoke
  test, CI, the IDE.
- [Dependencies](dependencies.md): latest versions, one version of each library, exclusions and bans, adding a
  library.
- [Writing a provider](writing-a-provider.md): the provider service, the base classes, `AvroStore`, the contract
  tests.
- [Releasing](releasing.md): what a release involves (made only by the owner).
- [Troubleshooting](troubleshooting.md): build, test and usage problems.
