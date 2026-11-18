# Building and testing

## Requirements

- **JDK 17**, the baseline. Every module compiles with `--release 17`, so a newer JDK builds the same class files.
  Its `javac` gives fewer warnings, though, and the build fails on warnings. CI builds on JDK 17 only, so build on 17
  to see what CI sees:
  ```sh
  export JAVA_HOME=$(/usr/libexec/java_home -v 17)     # macOS; or the path of your JDK 17
  ./mvnw -v                                           # check: Java version: 17
  ```
- **Docker**, for the Testcontainers tests (postgres, gcs, s3, the copy tool) and the JPMS smoke test.
- **Maven** comes with the wrapper. `./mvnw` downloads the version in `.mvn/wrapper/maven-wrapper.properties` and
  checks it against `distributionSha256Sum`. A wrong checksum fails the build.

## The build

| Command | Does |
|---|---|
| `./mvnw verify` | compile with `-Xlint:all -Werror`, run every test |
| `./mvnw verify javadoc:javadoc` | plus Javadoc with doclint; warnings fail |
| `./mvnw verify -P coverage` | plus JaCoCo: `<module>/target/site/jacoco/index.html` |
| `./mvnw verify -pl <module> -am` | one module and what it needs |
| `src/it/jpms/run.sh` | the JPMS smoke test (Docker) |

The build fails fast on mistakes that are easy to miss:
- **compiler warnings**, through `-Werror`;
- **Javadoc**: missing or wrong docs on public API;
- **dependencies**: two versions of a library in a module (`dependencyConvergence`), or the wrong Java or Maven
  version (enforcer). See [dependencies](dependencies.md).

## The tests

| Module | Tests | Runs on | Needs |
|---|---|---|---|
| `rawdata-client-api` | the API, ids, the spi base classes | module path, test module `io.descoped.rawdata.api.test` | |
| `rawdata-client-tck` | the contract against `memory` | module path | |
| `rawdata-client-provider-postgres` | the contract and the provider's own tests on PostgreSQL 18 (`postgres:18-alpine`) | class path | Docker |
| `rawdata-client-provider-avro` | the contract and the `filesystem` tests | module path, test module `io.descoped.rawdata.provider.avro.test` | |
| `rawdata-client-provider-gcs` | the contract and the provider's tests against `fsouza/fake-gcs-server`; the client and credentials against a local server | class path | Docker |
| `rawdata-client-provider-s3` | the contract and the provider's tests against `adobe/s3mock`; live tests against real S3 with `-Drawdata.s3.live=true` | class path | Docker (live: AWS credentials) |
| `rawdata-copy-util` | the tool: arguments, copies, resume, filesystem → postgres | class path | Docker |

Testcontainers' own jars split packages, so tests that use it run on the class path. Applications never get test
dependencies, so this doesn't affect what is published.

### The contract tests (TCK)

`RawdataClientContractTest` is the contract every provider keeps: topics, ids and order, reading from every starting
point, metadata, and closing. A provider's test only supplies the configuration:

```java
class PostgresContractTest extends RawdataClientContractTest {
    @Override
    protected Map<String, String> configuration() {
        return Postgres18.configuration();
    }
}
```

Each test gets a new client and its own topic. Behaviour the contract can't see, such as several clients writing one
topic or the layout in the store, goes in the provider's own test class (`*ProviderTest`).

### The JPMS smoke test

`src/it/jpms/run.sh` builds the reactor, starts PostgreSQL 18, a fake GCS and S3Mock in Docker, and runs a small
application module (`src/it/jpms/app`) that requires only `io.descoped.rawdata.api`. It checks that:
- the service loader finds all six providers;
- a topic round-trips through `filesystem`, `postgres`, `gcs` and `s3`, with order, `lastMessage` and metadata;
- this works both with everything on the module path and with everything on the class path;
- `jdeps --check` finds nothing to change in the module declarations.

Run it after changing a `module-info`, a dependency or how providers are found.

## CI

`.github/workflows/ci.yml`, one job on JDK 17, for pull requests to `master`, pushes to `master` and release tags
(`3.0.0`). Changes that touch only Markdown files, `.claude/`, `LICENSE` and similar skip it (`paths-ignore`): a
docs-only pull request gets the review, but no build.
1. `./mvnw verify javadoc:javadoc`, with `-P coverage` for pull requests from this repository and pushes;
2. the JPMS smoke test;
3. the SonarCloud analysis (`-P sonar`, project `descoped_rawdata-client-project`). It doesn't gate the build, but
   its quality gate is a check on the pull request, and it needs 80% coverage of new code.

`.github/workflows/claude.yml` reviews every pull request. Renovate keeps dependencies, plugins and actions at the
latest stable versions.

CI publishes nothing.

## Running against real stores

- **PostgreSQL 18:** `docker compose up -d` in `rawdata-client-provider-postgres` (port 5432, user and database
  `rdc`, password `rdc`).
- **A real GCS bucket:** the [`gcs` configuration](providers/gcs.md#configuration) with
  `gcs.credentials=application-default` after `gcloud auth application-default login`. The copy tool is the easiest
  way to try it.
- **Real S3:** the [live tests](providers/s3.md#tests), or the [`s3` configuration](providers/s3.md#configuration)
  with an AWS profile.

## The IDE

**IntelliJ IDEA** imports the Maven project as is. It runs the postgres, gcs, s3 and copy-util tests on the module path,
even though Surefire uses the class path. The `intellij` profile in those POMs gives those runs the
`--add-reads`, `--add-opens` and `--add-exports` they need. IntelliJ activates the profile itself
(`idea.maven.embedder.version`), and Maven never does. After changing a POM, reload the Maven project.

Set the project SDK to JDK 17.
