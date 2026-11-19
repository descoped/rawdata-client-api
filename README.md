# Rawdata Client

[![CI](https://github.com/descoped/rawdata-client-project/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/descoped/rawdata-client-project/actions/workflows/ci.yml)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=descoped_rawdata-client-project&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=descoped_rawdata-client-project)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Java library for writing and reading ordered streams of raw data messages, with one API over several stores.

A **topic** is a stream of messages, each with an id, a UUIDv7 laid out as PostgreSQL 18's `uuidv7()`. Each message has
your own position (for example a key in the source system), optional ordering fields, and named binary content. Every
topic also has a small metadata store, for example for checkpoints. Applications code against `RawdataClient` and
choose the store by configuration: PostgreSQL, Avro files in a folder, Google Cloud Storage or Amazon S3. Consumers
are pulled: the application takes one message, or a batch of the size it chooses.

| Module | What it is |
|---|---|
| [`rawdata-client-api`](docs/modules/rawdata-client-api.md) | the API, the provider service, the `spi` base classes, and the `memory` and `discard` providers |
| [`rawdata-client-bom`](docs/modules/rawdata-client-bom.md) | a BOM that aligns the versions of the rawdata modules |
| [`rawdata-client-tck`](docs/modules/rawdata-client-tck.md) | the contract every provider passes, as JUnit 6 tests to extend |
| [`rawdata-client-provider-postgres`](docs/modules/rawdata-client-provider-postgres.md) | provider `postgres`, on PostgreSQL 18 |
| [`rawdata-client-provider-avro`](docs/modules/rawdata-client-provider-avro.md) | the Avro engine that `filesystem`, `gcs` and `s3` share; not a provider |
| [`rawdata-client-provider-filesystem`](docs/modules/rawdata-client-provider-filesystem.md) | provider `filesystem`: Avro files in a local folder |
| [`rawdata-client-provider-gcs`](docs/modules/rawdata-client-provider-gcs.md) | provider `gcs`: Avro files in a Google Cloud Storage bucket |
| [`rawdata-client-provider-s3`](docs/modules/rawdata-client-provider-s3.md) | provider `s3`: Avro files in an Amazon S3 bucket |
| [`rawdata-copy-util`](docs/modules/rawdata-copy-util.md) | `rawdata-copy`, a command-line tool that copies topics between stores and clouds |

Maven group `io.descoped.rawdata`, Java 17 or newer, every module a JPMS module.

## Features

- **One API, several stores.** Code depends on `rawdata-client-api` only; the provider is a runtime dependency,
  chosen by the `rawdata.client.provider` key and found by the service loader.
- **Ordered topics.** Every message gets a UUIDv7 id when it is published, unless the application gives one, and a
  topic whose ids were published in increasing order is read in that order (`RawdataIds.ORDER`, unsigned, as
  PostgreSQL orders `uuid`).
- **Read from anywhere.** Start a consumer at the beginning, after an id, at a position near a time, or at the last
  message; ask for the last message of a topic directly.
- **Pull consumers.** `receive` takes one message, `receiveUpTo` a batch. There is no reactive streams API.
- **Per-topic metadata.** A small key-value store next to each topic, for checkpoints and similar state.
- **One writer per topic, checked.** One process writes a topic; consumers can be anywhere. A second writing process
  is detected and fails with `RawdataWriterConflictException`, in every provider.
- **One contract.** Every provider passes the same TCK unchanged, so providers can be swapped.
- **Predictable dependencies.** One version of each library in every module's compile and runtime tree, checked by
  the build. The `gcs` provider uses the JDK's HTTP client and no Google libraries.
- **JPMS.** Every module is a named module, and works on the module path or the class path.
- **A copy tool.** `rawdata-copy` copies topics between providers and clouds, and resumes after the target's last id.

## Getting started

### Add the dependencies

Import the BOM, then add the API and the provider you use. The provider is found at run time, so `runtime` scope is
enough:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.descoped.rawdata</groupId>
            <artifactId>rawdata-client-bom</artifactId>
            <version>3.0.1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-api</artifactId>
    </dependency>
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-provider-postgres</artifactId>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

### Publish and read

The `memory` provider is in the API module, so this runs with no store:

```java
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

public class QuickStart {
    public static void main(String[] args) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(Map.of(RawdataClient.PROVIDER_KEY, "memory"))) {

            try (RawdataProducer producer = client.producer("invoices")) {
                producer.publish(RawdataMessage.builder()
                        .position("invoice-1001")
                        .put("document", "{\"amount\": 42}".getBytes(StandardCharsets.UTF_8))
                        .build());
            }

            try (RawdataConsumer consumer = client.consumer("invoices")) {
                for (RawdataMessage message : consumer.receiveUpTo(100, Duration.ofSeconds(1))) {
                    System.out.println(message.id() + " " + message.position());
                }
            }
        }
    }
}
```

To store the messages for real, add a provider and change the configuration; the code stays the same. The
[tutorial](docs/tutorial.md) goes on from here: a real store, consumers that resume, metadata, and closing.

On the module path, an application module needs one line, `requires io.descoped.rawdata.api;`. The providers need
no `requires`: the API's `uses` finds them.

## Providers at a glance

| Provider | Artifact | Store | Required keys | A message is visible |
|---|---|---|---|---|
| `postgres` | `rawdata-client-provider-postgres` | PostgreSQL 18 tables shared by every topic | `postgres.driver.user`, `postgres.driver.password`, `postgres.driver.database` | when `publish` returns |
| `filesystem` | `rawdata-client-provider-filesystem` | Avro files in a local folder | `filesystem.storage-folder`, `local-temp-folder` | when its upload window is uploaded |
| `gcs` | `rawdata-client-provider-gcs` | Avro files in a Google Cloud Storage bucket | `gcs.bucket-name`, `local-temp-folder`, and credentials | when its upload window is uploaded |
| `s3` | `rawdata-client-provider-s3` | Avro files in an Amazon S3 bucket | `s3.bucket-name`, `local-temp-folder` | when its upload window is uploaded |
| `memory` | `rawdata-client-api` | the client's heap | none | when `publish` returns |
| `discard` | `rawdata-client-api` | nowhere: messages are dropped | none | never |

`filesystem`, `gcs` and `s3` share the [Avro engine](docs/modules/rawdata-client-provider-avro.md): a producer buffers
messages on a local disk and uploads them as one large Avro file per window, so it closes its window on time, on
size, or on `close()`. Each provider's page lists every key and default.

## Documentation

- [The documentation index](docs/README.md): every page, by what you want to do.
- [Tutorial](docs/tutorial.md): from nothing to a working producer and consumer on a real store.
- [Concepts](docs/concepts.md): topics, messages, ids and order, producers, consumers, cursors, metadata, the one
  writer, visibility and closing, in one place.
- [Modules](docs/README.md#modules): one page per module, with its configuration, failure modes, testing and
  reference.
- [The contract](docs/contract.md): what every provider guarantees.

## Build

Prerequisites:

- **JDK 17.** The build targets Java 17 (`--release 17`), and CI builds on JDK 17 only. Build on 17 to see what CI
  sees: a newer `javac` gives fewer warnings, and warnings fail the build.
- **Docker**, for the postgres, gcs, s3 and copy tool tests and the JPMS smoke test.
- Maven comes with the wrapper (`./mvnw`, Maven 3.10), which checks the download against its checksum.

```sh
git clone https://github.com/descoped/rawdata-client-project.git
cd rawdata-client-project
./mvnw verify                    # compile with -Xlint:all -Werror, tests
./mvnw verify javadoc:javadoc    # plus Javadoc with doclint, warnings fail the build
./mvnw verify -P coverage        # JaCoCo report per module in <module>/target/site/jacoco
./mvnw install                   # install every module in your local repository
```

To work on one module and what it needs: `./mvnw verify -pl rawdata-client-provider-postgres -am`.

The copy tool: `./mvnw -pl rawdata-copy-util -am package -DskipTests` writes
`rawdata-copy-util/target/rawdata-copy-<version>.zip`; see [the copy tool](docs/modules/rawdata-copy-util.md).

## Test

`./mvnw verify` runs every test suite:

- **Unit tests** of the API, ids and the `spi` base classes (JUnit 6, in their own test module), and of the Avro
  engine's internals (the topic writer and its upload window, ranged reads, the upload limiter).
- **The TCK.** `RawdataClientContractTest` in `rawdata-client-tck` is the contract every provider passes unchanged;
  `AvroStoreContractTest` is the contract of the Avro engine's stores. The TCK runs it against `memory`, and each
  provider runs it against its store, next to its own tests of internals.
- **Testcontainers.** `postgres` runs on a real PostgreSQL 18 (`postgres:18-alpine`), `gcs` against
  `fsouza/fake-gcs-server`, `s3` against `adobe/s3mock`. `filesystem` runs offline.
- **The JPMS smoke test**, `src/it/jpms/run.sh`: an application module that requires only the API finds all six
  providers and round-trips a topic through `filesystem`, `postgres`, `gcs` and `s3`, with everything on the module
  path and with everything on the class path; `jdeps --check` must find nothing to change. Run it after changing a
  `module-info`, a dependency or how providers are found. It needs Docker.
- **Live S3 tests**, opt in and never in CI: `-Drawdata.s3.live=true`, with credentials from the AWS SDK's default
  chain, for example
  `AWS_PROFILE=<profile> ./mvnw verify -pl rawdata-client-provider-s3 -am -Drawdata.s3.live=true`.

For a PostgreSQL 18 to try things against, run `docker compose up -d` in `rawdata-client-provider-postgres` (port
5432, user and database `rdc`, password `rdc`).

CI (`.github/workflows/ci.yml`) builds on JDK 17 with Javadoc, runs the JPMS smoke test, and analyses the code on
SonarCloud (project `descoped_rawdata-client-project`). See [building and testing](docs/building-and-testing.md).

## Release and deploy

- Releases are made **only by the owner**, or on his explicit word, with `release:prepare` and `release:perform`,
  signed with his GPG key. CI publishes nothing to Maven Central.
- The libraries (API, TCK, providers, BOM) go to Maven Central through the Central Portal, with sources, Javadoc
  and signatures. All modules share one version, and the tag is the version with a `v`, for example `v3.0.0`.
- **No SNAPSHOTs are published**, by CI or by hand: the Central Portal's free tier limits monthly publishing.
- After Central, the `Release` workflow publishes the GitHub release for the tag, with the CHANGELOG section as its
  notes and the copy tool, `rawdata-copy-<version>.zip` (the jar and `lib/`), which is not on Central and not in the
  BOM. See [releasing](docs/releasing.md).

## Repository layout

```
pom.xml                              parent and aggregator: versions as properties, plugins, profiles
rawdata-client-api/                  the API module
rawdata-client-bom/                  the BOM
rawdata-client-tck/                  the provider contract tests (TCK)
rawdata-client-provider-*/           the Avro engine and one module per provider
rawdata-copy-util/                   the copy tool and its zip
src/it/jpms/                         the JPMS smoke test (an application module and run.sh)
docs/                                user and developer documentation
.github/workflows/                   CI and the pull request review
```

## Contributing

Pull requests to `master` are welcome. Read [CONTRIBUTING.md](CONTRIBUTING.md) first: it covers the build on JDK 17,
the checks a pull request must pass, where tests go, and the rules for dependencies and docs. Open an issue before
you change a design.

## Security

Report vulnerabilities privately, as [SECURITY.md](SECURITY.md) describes. Do not open a public issue.

## Changelog

What changed in each release is in the [CHANGELOG](CHANGELOG.md).

## History

Ove Ranheim and [Kim Christian Gaarder](https://github.com/kimcs) designed and built the rawdata client in 2019, in
the Data Collection department of Statistics Norway (SSB), as the storage of SSB's Data Collector: data harvested
from external sources was kept exactly as it arrived, hence *raw* data.

SSB published it under the Apache License 2.0 and has since removed its repositories from GitHub. Descoped
continues the project from a fork; this repository has the full history of the original module repositories.

## License

Apache License 2.0, see [LICENSE](LICENSE). The code was first written and published by Statistics Norway (SSB)
under the same license.
