# Rawdata client documentation

- [Getting started](getting-started.md): dependencies, a first producer and consumer, the module path.
- [The contract](contract.md): topics, ids, ordering, reading, metadata and closing, the same in every provider.
- Providers:
  - [`postgres`](providers/postgres.md): PostgreSQL 18.
  - [`filesystem`](providers/filesystem.md): Avro files in a local folder.
  - [`gcs`](providers/gcs.md): Avro files in a Google Cloud Storage bucket.
  - [`memory` and `discard`](providers/memory-and-discard.md): for tests.
- [The copy tool](copy-tool.md): copying topics, and moving a system from one store or cloud to another.
- [Writing a provider](writing-a-provider.md): the provider service, the base classes and the contract tests.

3.0.0 is a clean break from 2.x: ids, storage layouts and configuration keys changed, and 3.0 does not read data
written by 2.x. See the [CHANGELOG](../CHANGELOG.md).
