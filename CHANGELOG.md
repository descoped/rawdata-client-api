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
- New `rawdata-client-tck`: the provider contract as JUnit 6 tests, run for every provider.
- New `io.descoped.rawdata.api.spi`: `OrderedRawdataProducer` and `PollingRawdataConsumer`, base classes for
  providers.
- One version of each library in every module's compile and runtime dependencies, checked by the build: slf4j 2,
  Jackson and commons-codec are declared by the providers at the newest version and excluded from HikariCP, Avro
  and google-cloud-storage (which also no longer brings checker-qual).
- Every module is a JPMS module (`gcs` an automatic one, see below).
- Documentation in `docs/`: for users (getting started, the contract, the providers, the copy tool) and for
  developers (architecture, building and testing, dependencies, writing a provider, releasing, troubleshooting).
  The README is the getting-started for the repository. CI checks the providers on the module path and
  the class path.

### API

- Ids are UUIDv7 (`java.util.UUID`) laid out as PostgreSQL 18's `uuidv7()`, instead of ULIDs; the ULID library is
  gone. `RawdataIds` generates, checks and orders them (`RawdataIds.ORDER`, unsigned); `beginningOf(timestamp)` moved
  there from `RawdataConsumer`, and `RawdataProducer.nextMonotonicUlid` is removed.
- Reactive streams are gone: `RawdataFlows`, `RawdataPublisher`, `RawdataSubscriber` and the reactive-streams
  dependency are removed. Consumers are pulled: `receive`, the new `receiveUpTo(max, timeout)` for batches, and
  `receiveAsync`. The new `RawdataCopy.copy` copies a topic in batches.
- A topic only grows at its end: an id the producer gives is greater than every id in the topic, whichever process
  wrote it, and an id the caller gives must be, or the publish throws `IllegalArgumentException` and writes nothing.
- Topic names follow one rule in every provider (`RawdataTopics`): 1 to 100 of `A-Z a-z 0-9 _ . -`, starting with a
  letter or digit.
- `RawdataMessage` is a sealed interface built only by its builder; content is copied in and out.
- Closing is the same everywhere: after `close()` every method throws `RawdataClosedException`; closing wakes a
  waiting `receive` and fails pending asynchronous calls.

### `postgres`

- Three shared tables keyed by topic (`rawdata_message`, `rawdata_content`, `rawdata_metadata`) instead of tables
  per topic; the topic is only ever a bind parameter. Created when missing; a role without `CREATE` can use existing
  tables.
- Ids follow commit order across processes: a publish holds the topic's advisory lock while it reads the last id and
  writes. Connections run at READ COMMITTED.
- PostgreSQL 18 only. Required configuration is `postgres.driver.user`, `.password` and `.database`; the other keys
  are unchanged and have defaults.
- Fixed: SQL built from topic names, tables dropped on a missing topic, a commit on `close()`, order flips within a
  millisecond, and the consumer's prefetch races.

### `filesystem` and `gcs`

- Split into `rawdata-client-provider-avro` (the engine and `filesystem`, module `io.descoped.rawdata.provider.avro`)
  and `rawdata-client-provider-gcs` (automatic module `io.descoped.rawdata.provider.gcs`; the Google Cloud libraries
  cannot be modules, so they go on the class path). Packages renamed from `io.descoped.rawdata.avro`.
- One Avro file per publish, numbered without gaps, created only if the number is free (a hard link, or an upload
  with the `doesNotExist` precondition): a message is visible and durable when `publish` returns, and ids follow
  commit order across processes. The local temp folder, the upload thread and the time and size windows are gone.
- New layout: `<topic>/segments/<19 digits>.avro` and `<topic>/metadata/k<hex of the key>`. Avro record
  `io.descoped.rawdata.RawdataMessage`: `id` `fixed(16)` named `uuid`, `content` an array of `{name, value}` (was a map
  `data`). On a file system, topic folders keep topics that differ only by case apart.
- Configuration: `local-temp-folder`, `avro-file.*`, `listing.min-interval-seconds` and
  `gcs.listing.min-interval-seconds` are removed. `filesystem.poll-interval-ms`, `gcs.poll-interval-ms`,
  `gcs.prefix`, `gcs.project-id` and `gcs.endpoint` are new. `gcs.credential-provider` is now `gcs.credentials`:
  `application-default` (the default), `service-account` or `none`.
- Metadata keys are at most 127 bytes (`filesystem`) or about 500 bytes (`gcs`).
- `gcs` leaves out what Google's HTTP path never loads: the gRPC transport, Cloud Monitoring and the OpenTelemetry
  SDK, and legacy jars (Apache HttpClient 4, `com.google.android:annotations`, `threeten-extra`). 56 dependencies
  instead of 98.
- Fixed: files that started in the same millisecond hid each other; an upload failure deadlocked the producer;
  messages stayed invisible until a window closed; a later upload with an older name was skipped; topics could
  escape the storage folder; file handles leaked; a closed consumer threw `NullPointerException`; a cursor at the
  start of a millisecond skipped it.

### Copy tool

- New `rawdata-copy` (`rawdata-copy-util`): copies topics between stores, keeping ids and order, and continues after
  the target's last id. All its logging, including `System.Logger` and `java.util.logging`, goes through slf4j.
  Distributed as `rawdata-copy-<version>.zip` on GitHub releases, not on Maven Central.

## 2.0.0

The last release of the separate module repositories (`rawdata-client-api`, `rawdata-client-provider-postgres`,
`rawdata-client-provider-gcs`); their history is in this repository, with tags such as `rawdata-client-api/2.0.0`.
