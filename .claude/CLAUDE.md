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
| `rawdata-client-api` | `io.descoped.rawdata.api` | `RawdataClient`, `RawdataClientInitializer` (the provider service, on `io.descoped.spi:service-provider-api`), the `memory` and `discard` providers, `RawdataIds` (UUIDv7), `RawdataCopy`; `io.descoped.rawdata.api.spi`: `PollingRawdataConsumer` and `OrderedRawdataProducer`, the base classes of the postgres and Avro providers |
| `rawdata-client-tck` | `io.descoped.rawdata.tck` | `RawdataClientContractTest`: the provider contract as abstract JUnit 6 tests; its own tests run it for `memory` |
| `rawdata-client-provider-postgres` | `io.descoped.rawdata.provider.postgres` | provider `postgres`: PostgreSQL 18 only, HikariCP, pgjdbc; shared tables `rawdata_message`, `rawdata_content`, `rawdata_metadata` keyed by topic |
| `rawdata-client-provider-avro` | `io.descoped.rawdata.provider.avro` | the Avro engine (`AvroRawdataClient` on an `AvroStore`) and provider `filesystem` (Avro files in a local folder) |
| `rawdata-client-provider-gcs` | `io.descoped.rawdata.provider.gcs` | provider `gcs`: the Avro engine on a Google Cloud Storage bucket, through GCS's JSON API on `java.net.http` with Google's OAuth 2.0 done in-house (`GcsClient`, `GcsCredentials`); no Google libraries |
| `rawdata-copy-util` | `io.descoped.rawdata.copy` | `rawdata-copy` (`RawdataCopyTool`): a command-line tool that copies topics between providers and clouds, resuming after the target's last id; distributed as `rawdata-copy-<version>.zip` (jar and `lib/`) on GitHub releases, not on Central and not in the BOM |

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
- **Predictable transitive dependencies.** Applications add these modules next to their own libraries, so every
  module's compile and runtime tree has one version of each library (enforcer `dependencyConvergence`; test and
  provided scope, which applications never get, are excluded). When two libraries bring different versions, the module
  that first needs it declares it at the newest version (a property in the root POM) and excludes it from the others:
  slf4j-api from HikariCP and Avro, Jackson and commons-codec from Avro. Build and test locally on JDK 17, as CI does:
  a newer javac gives fewer warnings, and warnings fail the build.
- **JPMS.** Every module is a named module, with a `module-info` (the gcs exception ended when gcs dropped Google's
  client libraries, 2026-11-18). Applications can put every module and its dependencies on the module path, or
  everything on the class path; the JPMS smoke test runs both. A module whose exported API exposes another module's
  types `requires transitive` it. The API module declares `uses` for `RawdataClientInitializer`. Providers are
  registered both with `provides` in `module-info.java` and in `META-INF/services`, so they work on the module path
  and the class path.
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
  `https://central.sonatype.com` in `repositoryOrigins`. `rawdata-copy-util` is a program, excluded from the Central
  bundle (`excludeArtifacts`); its zip goes on the GitHub release (owner's decision, 2026-11-18).
- **SonarCloud**: one project, `descoped_rawdata-client-project`, for the whole reactor.

## Testing

- The API's tests are JUnit 6 in a test module of their own (`open module io.descoped.rawdata.api.test`). The
  provider contract is `RawdataClientContractTest` in `rawdata-client-tck`; a provider's test extends it with the
  provider's configuration.
- The postgres tests are JUnit 6: the shared contract and the provider's own tests, on a real PostgreSQL 18
  (`postgres:18-alpine`) through Testcontainers, so the build needs Docker. They run on the class path (Testcontainers'
  dependencies are automatic modules with split packages). `docker-compose.yml` in the postgres module starts
  PostgreSQL 18 for manual runs (port 5432, user and database `rdc`, password `rdc`).
- **IntelliJ IDEA** ignores Surefire's `useModulePath=false` and runs the postgres, gcs and copy-util tests on the
  module path, patched into the main module. A profile `intellij` in those three POMs, active only under IntelliJ's
  import (property `idea.maven.embedder.version`), gives its test runs the `--add-reads`/`--add-opens` they need
  through Surefire's `argLine`. Maven and CI never activate it. The root POM's empty `argLine` property keeps
  `@{argLine}` defined when JaCoCo does not set it.
- **Postgres invariants**: the topic is a column and always a bind parameter, never SQL text; a publish holds the
  topic's advisory lock (`pg_advisory_xact_lock`) while it reads the last id and writes, so ids grow in commit order
  across processes; the tables are created idempotently under an advisory lock.
- The `filesystem` tests (shared contract and the provider's own) run offline in a test module of their own,
  `open module io.descoped.rawdata.provider.avro.test`. The `gcs` tests run the same against a fake GCS server
  (`fsouza/fake-gcs-server`) through Testcontainers, on the class path, and its client and credential sources against
  a local server (`jdk.httpserver`) playing GCS, Google's token endpoint and the metadata server.
- **Avro invariants**: a topic is segments numbered 0, 1, 2… without gaps, one Avro file per publish, and a publish
  creates the segment after the last only if it does not exist (a hard link on a file system, an upload with the
  `ifGenerationMatch=0` precondition on GCS). A writer that loses the race reads the new last segment and tries again, so
  ids grow in segment order across processes. A published message is visible and durable when `publish` returns;
  there is no local buffer or upload thread.
- **JPMS smoke test** (`src/it/jpms/run.sh`, run by CI after the build): an application module that requires only the
  API must find all five providers and round-trip through filesystem, PostgreSQL 18 and a fake GCS, both with
  everything on the module path and with everything on the class path; `jdeps --check`
  must find nothing to change in the module declarations. Needs Docker.

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
src/it/jpms/run.sh               # JPMS smoke test (Docker)
```
