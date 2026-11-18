# Rawdata Client

A Java library for writing and reading ordered streams of raw data messages, with one API over several stores.

A **topic** is a stream of messages ordered by id, a UUIDv7 laid out as PostgreSQL 18's `uuidv7()`. Each message has
your own position (for example a key in the source system), optional ordering fields, and named binary content. Every
topic also has a small metadata store, for example for checkpoints. Applications code against `RawdataClient` and
choose the store by configuration: PostgreSQL, Avro files in a folder, Google Cloud Storage or Amazon S3. Consumers
are pulled: the application takes one message, or a batch of the size it chooses.

```java
try (RawdataClient client = RawdataClient.create(Map.of("rawdata.client.provider", "memory"))) {
    client.producer("invoices").publish(RawdataMessage.builder()
            .position("invoice-1001")
            .put("document", "{\"amount\": 42}".getBytes(StandardCharsets.UTF_8))
            .build());
    try (RawdataConsumer consumer = client.consumer("invoices")) {
        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        System.out.println(message.id() + " " + message.position());
    }
}
```

Version 3.0.0 is in progress on `master`; the last release is 2.0.0. 3.0 is a clean break from 2.x, see the
[CHANGELOG](CHANGELOG.md).

## Modules

| Module | What it is |
|---|---|
| `rawdata-client-api` | the API, the provider service, the `spi` base classes, and the `memory` and `discard` providers |
| `rawdata-client-tck` | the contract every provider passes, as JUnit 6 tests to extend |
| `rawdata-client-provider-postgres` | provider `postgres`, on PostgreSQL 18 |
| `rawdata-client-provider-avro` | the Avro engine that `filesystem`, `gcs` and `s3` share |
| `rawdata-client-provider-filesystem` | provider `filesystem` (Avro files in a local folder) |
| `rawdata-client-provider-gcs` | provider `gcs` (Avro files in a Google Cloud Storage bucket) |
| `rawdata-client-provider-s3` | provider `s3` (Avro files in an Amazon S3 bucket) |
| `rawdata-copy-util` | `rawdata-copy`, a command-line tool that copies topics between stores and clouds |
| `rawdata-client-bom` | a BOM that aligns the versions of the modules above |

Maven group `io.descoped.rawdata`, Java 17 or newer, every module a JPMS module.

## Getting started with the code

### Prerequisites

- **JDK 17 or newer.** The build targets Java 17 (`--release 17`), and CI builds on JDK 17. Build on 17 to see what
  CI sees: a newer `javac` gives fewer warnings, and warnings fail the build.
- **Docker**, for the postgres, gcs, s3 and copy tool tests: Testcontainers starts PostgreSQL 18, a fake GCS server
  and S3Mock.
- Maven comes with the wrapper (`./mvnw`); nothing else to install.

### Build and test

```sh
git clone https://github.com/descoped/rawdata-client-project.git
cd rawdata-client-project
./mvnw verify                    # compile (-Xlint:all -Werror) and test every module
./mvnw verify javadoc:javadoc    # plus Javadoc with doclint
./mvnw verify -P coverage        # plus JaCoCo reports in <module>/target/site/jacoco
src/it/jpms/run.sh               # JPMS smoke test: every provider on the module path and the class path
```

`./mvnw verify` takes a few minutes; most of it is the postgres, gcs and s3 tests. To work on one module:
`./mvnw verify -pl rawdata-client-provider-postgres -am`.

### Run something

- Against PostgreSQL 18 on your machine: `docker compose up -d` in `rawdata-client-provider-postgres` (port 5432,
  user and database `rdc`, password `rdc`), then use the
  [postgres configuration](docs/providers/postgres.md#configuration).
- The copy tool: `./mvnw -pl rawdata-copy-util -am package -DskipTests` writes
  `rawdata-copy-util/target/rawdata-copy-<version>.zip`; see [the copy tool](docs/copy-tool.md).

### Repository layout

```
pom.xml                              parent and aggregator: versions as properties, plugins, profiles
rawdata-client-api/                  the API module
rawdata-client-tck/                  the provider contract tests (TCK)
rawdata-client-provider-*/           one module per provider
rawdata-copy-util/                   the copy tool and its zip
rawdata-client-bom/                  the BOM
src/it/jpms/                         the JPMS smoke test (an application module and run.sh)
docs/                                user and developer documentation
.github/workflows/                   CI (build, tests, smoke test, SonarCloud) and the Claude review
.claude/CLAUDE.md                    the project's invariants
```

## Documentation

For using the library:

- [Getting started](docs/getting-started.md): dependencies, a first producer and consumer, logging, the module path.
- [The contract](docs/contract.md): topics, ids, ordering, reading, metadata and closing, the same in every provider.
- The providers: [`postgres`](docs/providers/postgres.md), [`filesystem`](docs/providers/filesystem.md),
  [`gcs`](docs/providers/gcs.md), [`s3`](docs/providers/s3.md),
  [`memory` and `discard`](docs/providers/memory-and-discard.md).
- [The copy tool](docs/copy-tool.md): copying topics, and moving a system to another store or cloud.

For working on the code:

- [Architecture](docs/architecture.md): how the modules fit together, and how each provider keeps a topic in order
  with one writer.
- [Building and testing](docs/building-and-testing.md): the build, the test suites, CI, coverage and the IDE.
- [Dependencies](docs/dependencies.md): versions, convergence, exclusions and how to add a library.
- [Writing a provider](docs/writing-a-provider.md): the provider service, the base classes, `AvroStore`, the TCK.
- [Releasing](docs/releasing.md): how a release is made.
- [Troubleshooting](docs/troubleshooting.md): common build and test failures.

## Contributing

Work goes in pull requests to `master`. A pull request needs a green CI build on JDK 17 and passes a SonarCloud
analysis and an automated review (a pull request that changes only Markdown files skips the build, and gets the
review). Before you open one:

- build and test on JDK 17 (`./mvnw verify javadoc:javadoc`, and `src/it/jpms/run.sh` for module or dependency
  changes);
- keep to the invariants in [`.claude/CLAUDE.md`](.claude/CLAUDE.md): Java 17, one version of each dependency, the
  latest stable libraries, one contract for every provider (a behaviour change goes in the shared contract tests);
- update the docs and the [CHANGELOG](CHANGELOG.md) with the change.

Security issues: see [SECURITY.md](SECURITY.md).

## License

Apache License 2.0, see [LICENSE](LICENSE).
