# CLAUDE.md

Guidance for Claude Code in this repository.

## What this is

The Descoped rawdata client: `RawdataClient`, a provider based API for writing and reading ordered streams of raw
data messages (topics of messages ordered by UUIDv7 id, with binary content and per-topic metadata), its providers, and a
tool that copies topics between providers. One Maven reactor, group `io.descoped.rawdata`, owned by Descoped.

| Module | JPMS module | Role |
|---|---|---|
| `rawdata-client-project` (root) | n/a | parent and aggregator |
| `rawdata-client-bom` | n/a | BOM of the modules below |
| `rawdata-client-api` | `io.descoped.rawdata.api` | `RawdataClient`, `RawdataClientInitializer` (the provider service, on `io.descoped.spi:service-provider-api`), the `memory` and `discard` providers, `RawdataIds` (UUIDv7), `RawdataCopy` |
| `rawdata-client-tck` | `io.descoped.rawdata.tck` | `RawdataClientContractTest`: the provider contract as abstract JUnit 6 tests; its own tests run it for `memory` |
| `rawdata-client-provider-postgres` | `io.descoped.rawdata.postgres` | provider `postgres`: PostgreSQL 18, HikariCP, pgjdbc |
| `rawdata-client-provider-gcs` | `io.descoped.rawdata.avro` | providers `gcs` (Avro files in Google Cloud Storage) and `filesystem` (Avro files in a local folder) |
| `rawdata-copy-util` | n/a yet | copies topics from one provider to another (not written yet) |

Until 2026-11 every module was a repository of its own; their histories were imported here with `git filter-repo`
(old tags prefixed with the module name, e.g. `rawdata-client-api/2.0.0`). 3.0.0 is the first version built from
this repository, and the work toward it is under way module by module.

## Invariants (never violate)

- **Java 17 baseline** (`maven.compiler.release` 17): every class file is Java 17 whatever JDK builds it. CI builds
  and tests on JDK 17 only (`ci.yml`). Nothing above Java 17 in main or test code.
- **3.0.0 is a clean break.** No backward-compatibility shims, deprecated aliases, data-migration code or migration
  guides for 2.x (owner's decision, 2026-11-17).
- **One reactor, one version.** Every module inherits the root POM and is released with it; modules depend on each
  other with `${project.version}`. Dependency versions are properties in the root POM, not `dependencyManagement`, so
  the BOM manages the rawdata modules only.
- **JPMS.** Every module is a named module. A module whose exported API exposes another module's types `requires
  transitive` it. The API module declares `uses` for `RawdataClientInitializer`. Providers are registered both with
  `provides` in `module-info.java` and in `META-INF/services`, so they work on the module path and the class path.
- **One contract for every provider.** Closed clients, producers and consumers, missing positions, ordering and
  metadata behave the same in every provider, and the shared contract tests check it. Topic names are validated and
  never spliced into SQL, file paths or object names.
- **Ids are UUIDv7** (`RawdataIds`), laid out as PostgreSQL 18's `uuidv7()` (48-bit milliseconds, version 7, 12-bit
  sub-millisecond fraction, variant, 62 random bits) and ordered by `RawdataIds.ORDER` (unsigned, as PostgreSQL
  orders `uuid`), never by `UUID.compareTo`, which is signed. No ULID library (owner's decision, 2026-11-17).
- **Pull, not reactive streams.** Consumers are pulled (`receive`, `receiveUpTo`); there is no `Flow` API (owner's
  decision, 2026-11-17, to revisit when the JDK offers something better than `Flow`).
- **Always the latest stable version** of every library, plugin, action and engine (PostgreSQL 18). Pre-releases
  (milestones, RCs, alphas) only on request. Old and outdated code and configuration is removed, not carried along.
- **Releases only with the owner's go-ahead.** A release (`release:prepare`/`release:perform`, an upload to the
  Central Portal) is made by the owner or on his explicit word. As of 2026-11-17 the owner's Central Portal publishing
  limit is used up, so nothing is uploaded until he says it is possible again.
- **Publishing.** Only releases are published, by hand, through the Central Portal (profiles `central,sign`).
  **No SNAPSHOTs** are published, by CI or by hand: the Portal's free tier limits monthly publishing (owner's
  decision, 2026-11-17). CI publishes nothing. Maven 3.10 sends a server's credentials only to the hosts it knows for
  that id, and the id `central` is bound to Maven Central, so the server `central` in `~/.m2/settings.xml` must list
  `https://central.sonatype.com` in `repositoryOrigins`.
- **SonarCloud**: one project, `descoped_rawdata-client-project`, for the whole reactor.

## Testing

- The API's tests are JUnit 6 in a test module of their own (`open module io.descoped.rawdata.api.test`). The
  provider contract is `RawdataClientContractTest` in `rawdata-client-tck`; a provider's test extends it with the
  provider's configuration.
- The postgres and gcs tests are still the 2.x TestNG tests in the main packages, patched into the named module
  (hence the relaxed test-compile lint in the root POM), named `*Tck`. They move to JUnit 6, test modules of their
  own and the shared contract as each module is revised (#5, #6).
- The postgres tests run on H2 in PostgreSQL mode (`h2.enabled`). `docker-compose.yml` in the postgres module starts
  PostgreSQL 18 for manual runs (port 5432, user and database `rdc`, password `rdc`).
- The GCS integration tests need a real bucket and are disabled; the `filesystem` provider shares the engine and
  runs offline.
- Known open defects, each test disabled with its reason until the module's revision fixes it and enables it again:
  `FilesystemAvroRawdataClientTck.thatMultipleGCSFilesCanBeProducedThroughSizeBasedWindowingAndReadBack` (files that
  start in the same millisecond hide each other) and
  three `PostgresRawdataClientTck` tests (`thatMultipleProducersOnSameTopicCanBeProducedAndReadBack`,
  `thatFilesCreatedAfterConsumerHasSubscribedAreUsed`, `thatAllFieldsOfMessageSurvivesStream`): the producer does not
  continue from the topic's last id, so within one millisecond the topic order can flip.

## Maven Wrapper

- `./mvnw` pins Maven (`.mvn/wrapper/maven-wrapper.properties`, type `only-script`, so no jar in the repository) and
  checks the download against `distributionSha256Sum`. CI runs `./mvnw`. When Maven is bumped, by Renovate or by hand,
  set `distributionSha256Sum` to the SHA-256 of the new `apache-maven-<version>-bin.zip`, after checking that zip
  against the `.sha512` Maven publishes next to it. A stale checksum fails the build, it never skips the check.

## Commands

```sh
./mvnw verify                    # compile with -Xlint:all -Werror, tests
./mvnw verify javadoc:javadoc    # plus Javadoc with doclint, warnings fail the build
./mvnw verify -P coverage        # JaCoCo report per module in <module>/target/site/jacoco
```
