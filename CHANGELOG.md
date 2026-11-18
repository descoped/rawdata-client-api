# Changelog

## 3.0.0 (unreleased)

A clean break from 2.x: the modules now live in one repository and are released together; ids, the reading API,
storage layouts and some configuration changed. 3.0 does not read data written by 2.x, and there is no migration
code. Java 17 or newer.

### Project

- One repository, `descoped/rawdata-client-project`, for every module, with their history; one version for all,
  managed by `rawdata-client-bom`.
- Every library, plugin and engine at its latest stable version: PostgreSQL 18, JUnit 6, Testcontainers 2,
  service-provider-api 3 (providers are named with `@ProviderName`).
- New `rawdata-client-tck`, the TCK: the provider contract as JUnit 6 tests (`RawdataClientContractTest`) and the
  contract of the Avro engine's stores (`AvroStoreContractTest`). Every provider passes it unchanged, so providers
  can be swapped; providers' own tests only check internals.
- New `io.descoped.rawdata.api.spi`: `OrderedRawdataProducer` and `PollingRawdataConsumer`, base classes for
  providers.
- One version of each library in every module's compile and runtime dependencies, checked by the build: slf4j 2,
  Jackson and commons-codec are declared by the providers at the newest version and excluded from HikariCP, Avro
  and google-cloud-storage (which also no longer brings checker-qual).
- Every module is a JPMS module. CI checks the providers on the module path and the class path.
- Documentation in `docs/`: for users (getting started, the contract, the providers, the copy tool) and for
  developers (architecture, building and testing, dependencies, writing a provider, releasing, troubleshooting).
  The README is the getting-started for the repository.

### API

- Ids are UUIDv7 (`java.util.UUID`) laid out as PostgreSQL 18's `uuidv7()`, instead of ULIDs; the ULID library is
  gone. `RawdataIds` generates, checks and orders them (`RawdataIds.ORDER`, unsigned); `beginningOf(timestamp)` moved
  there from `RawdataConsumer`, and `RawdataProducer.nextMonotonicUlid` is removed.
- Reactive streams are gone: `RawdataFlows`, `RawdataPublisher`, `RawdataSubscriber` and the reactive-streams
  dependency are removed. Consumers are pulled: `receive`, the new `receiveUpTo(max, timeout)` for batches, and
  `receiveAsync`. The new `RawdataCopy.copy` copies a topic in batches.
- A topic only grows at its end: an id the producer gives is greater than every id in the topic, and an id the caller
  gives must be, or the publish throws `IllegalArgumentException` and writes nothing.
- Only one process writes a topic. A publish that finds another process wrote since this client last did throws the
  new `RawdataWriterConflictException` and writes nothing, in every provider.
- NUL (U+0000) is refused in positions, ordering groups, content names and metadata keys, which every provider must
  store. A store that does not exist (a bucket, a database) fails with `RawdataException` instead of reading as empty.
- Topic names follow one rule in every provider (`RawdataTopics`): 1 to 100 of `A-Z a-z 0-9 _ . -`, starting with a
  letter or digit.
- `RawdataMessage` is a sealed interface built only by its builder; content is copied in and out.
- Closing is the same everywhere: after `close()` every method throws `RawdataClosedException`; closing wakes a
  waiting `receive` and fails pending asynchronous calls.

### `postgres`

- Three shared tables keyed by topic (`rawdata_message`, `rawdata_content`, `rawdata_metadata`) instead of tables
  per topic; the topic is only ever a bind parameter. Created when missing; a role without `CREATE` can use existing
  tables.
- A publish holds the topic's advisory lock while it reads the last id and writes, and fails with
  `RawdataWriterConflictException` if another process wrote the topic. Connections run at READ COMMITTED. A metadata
  key is at most 2000 bytes.
- PostgreSQL 18 only. Required configuration is `postgres.driver.user`, `.password` and `.database`; the other keys
  are unchanged and have defaults.
- Fixed: SQL built from topic names, tables dropped on a missing topic, a commit on `close()`, order flips within a
  millisecond, and the consumer's prefetch races.

### `filesystem` and `gcs`

- Split into `rawdata-client-provider-avro` (the engine, module `io.descoped.rawdata.provider.avro`),
  `rawdata-client-provider-filesystem` (module `io.descoped.rawdata.provider.filesystem`) and
  `rawdata-client-provider-gcs` (module `io.descoped.rawdata.provider.gcs`). Packages renamed from
  `io.descoped.rawdata.avro`.
- The upload window of 2.x stays, fixed. A producer buffers on a local disk and uploads one Avro file per window,
  which closes on `avro-file.max.seconds` (now also by a timer, without new publishes), on `avro-file.max.bytes`,
  or when a producer closes. Messages are visible and durable once their window is uploaded.
- Each window is a segment, numbered without gaps and created only if its number is free (a hard link, or an upload
  with the `ifGenerationMatch=0` precondition). A number already taken means another writer
  (`RawdataWriterConflictException`).
- Durability:
  - each Avro block is forced to disk;
  - a closed window is sealed before upload;
  - a failed upload fails the writer loudly and keeps its windows on disk, with no silent close and no deadlock;
  - a restarted writer uploads the windows a crash left, a sealed one unchanged, an open one cut back to its complete
    blocks, but only right after the topic's end;
  - a leftover that no longer fits is kept as `*.invalid`;
  - a lock file keeps a buffer folder to one process;
  - `avro-file.max.pending-uploads` (default 4) bounds the windows waiting for upload.
  - `avro-file.close-timeout-seconds` (default 30) bounds how long `close()` waits for the uploads; what is not
    uploaded by then stays on disk for the next writer.
- New layout: `<topic>/segments/<19 digits>.avro` and `<topic>/metadata/k<hex of the key>`. The Avro record is
  2.x's `RawdataMessage`; only the id's `fixed(16)` type is renamed from `ulid` to `uuid`. On a file system, topic
  folders keep topics that differ only by case apart.
- Configuration:
  - the 2.x keys stay: `local-temp-folder` (required), `avro-file.max.seconds`, `avro-file.max.bytes`,
    `avro-file.sync.interval`, `listing.min-interval-seconds`, `gcs.listing.min-interval-seconds` and
    `gcs.credential-provider` (`service-account`, the default, `compute-engine`, and now `application-default` and
    `none`);
  - they now have defaults (60 s, 64 MiB, 512 KiB, 1 s) and are read with dynamic-configuration's value rules;
  - new: `avro-file.max.pending-uploads`, `avro-file.close-timeout-seconds`, `gcs.prefix`, `gcs.endpoint`, and
    `s3.listing.min-interval-seconds` for `s3`.
- Metadata keys are at most 127 bytes (`filesystem`) or about 500 bytes (`gcs`, `s3`).
- `gcs` no longer uses Google's client libraries: it talks to GCS's JSON API on the JDK's `java.net.http`, and does
  Google's OAuth 2.0 itself (service account keys, gcloud user credentials, the metadata server, and Application
  Default Credentials in Google's order). Its only dependency besides the Avro engine is Jackson: about 56 Google jars
  fewer, and the copy tool's zip is 7 MB instead of 61 MB. `gcs` is a named module and runs on the module path.
  `external_account` (workload identity federation) is not supported yet.
- Fixed: files that started in the same millisecond hid each other; an upload failure deadlocked the producer; a
  window closed only when the next publish came, so a quiet producer never uploaded; a later upload with an older name
  was skipped; topics could escape the storage folder; file handles leaked; a closed consumer threw
  `NullPointerException`; a cursor at the start of a millisecond skipped it.

### `s3` (new)

- `rawdata-client-provider-s3` (module `io.descoped.rawdata.provider.s3`): the Avro engine on an Amazon S3 bucket,
  with the same layout and ordering as `gcs`. A segment is created with `If-None-Match: *`.
- The AWS SDK for Java 2 with its JDK-based HTTP client; the SDK's default credentials chain (environment, web
  identity, profiles with SSO, container and instance credentials) and region chain. Path-style requests for bucket
  names with dots; optional `AES256` or `aws:kms` server-side encryption; `s3.endpoint` for S3-compatible stores.
- Tested against S3Mock, and by opt-in live tests against real S3.

### Copy tool

- New `rawdata-copy` (`rawdata-copy-util`): copies topics between stores, keeping ids and order, and continues after
  the target's last id. All its logging, including `System.Logger` and `java.util.logging`, goes through slf4j.
  Distributed as `rawdata-copy-<version>.zip` on GitHub releases, not on Maven Central.

## 2.0.0

The last release of the separate module repositories (`rawdata-client-api`, `rawdata-client-provider-postgres`,
`rawdata-client-provider-gcs`); their history is in this repository, with tags such as `rawdata-client-api/2.0.0`.
