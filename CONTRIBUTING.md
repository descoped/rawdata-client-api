# Contributing

Thank you for helping with the rawdata client. This page says how to propose a change and what a change must keep.
The project's rules are few, but the build enforces most of them, so it pays to know them before you start.

## Issues

- **Bugs:** open an issue with what you did, what you expected, and what happened: the provider, its configuration
  (without credentials), the exception and its stack trace, and the version.
- **Design changes:** open an issue first, and wait for an answer before you write code. Design and architecture
  are the owner's decisions; see [design decisions](#design-decisions).
- **Security issues:** never in a public issue. Follow [SECURITY.md](SECURITY.md).

## Pull requests

1. Branch from `master`, and open the pull request against `master`.
2. Say in its description what it changes and why.
3. Build and test on JDK 17 before you push (see [the build](#the-build)).
4. Update the docs and the [CHANGELOG](CHANGELOG.md) in the same pull request (see [docs](#docs-and-the-changelog)).

A pull request goes through these checks:

| Check | What it does |
|---|---|
| **CI** (`.github/workflows/ci.yml`) | `./mvnw verify javadoc:javadoc` on JDK 17, then the JPMS smoke test `src/it/jpms/run.sh`; it must be green |
| **SonarCloud** | analyses the reactor (project `descoped_rawdata-client-project`); it does not fail the CI build, but its quality gate, which needs 80% coverage of new code, shows as a check on the pull request |
| **The review** | every pull request is reviewed; each finding is checked against the code and fixed, or answered with the reason it is not |

A pull request that changes only Markdown files, `LICENSE`, `.claude/`, `.github/workflows/claude.yml`,
`.github/renovate.json` or `.gitignore` skips the CI build (`paths-ignore` in `ci.yml`), and still gets the review. Pull requests from forks get no SonarCloud analysis, since a fork's build has no
access to the token.

## The build

You need **JDK 17** and **Docker**. Maven comes with the wrapper.

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 17)     # macOS; or the path of your JDK 17
./mvnw -v                                           # check: Java version: 17
./mvnw verify javadoc:javadoc                       # what CI runs
src/it/jpms/run.sh                                  # the JPMS smoke test (Docker)
```

Build on JDK 17, as CI does. Every module compiles with `--release 17`, so a newer JDK builds the same class files,
but its `javac` gives fewer warnings, and warnings fail the build. A pull request that is green on JDK 21 can be red
in CI.

Run the JPMS smoke test after you change a `module-info`, a dependency, or how providers are found. To work on one
module: `./mvnw verify -pl <module> -am`. [Building and testing](docs/building-and-testing.md) explains the test
suites, coverage (`-P coverage`) and the IDE setup.

## Code

- **Java 17 only**, in main and test code. Nothing from a later Java release.
- **No warnings.** The compiler runs with `-Xlint:all -Werror`, so a warning fails the build.
- **Javadoc on the public API.** Javadoc runs with doclint `all` and fails on warnings, so every public type and
  member needs correct docs.
- **JPMS.** Every module is a named module. A module whose exported API exposes another module's types
  `requires transitive` it. A provider is registered both with `provides` in its `module-info.java` and in
  `META-INF/services`, so it works on the module path and the class path.
- **Owner decisions the code keeps:** one process writes a topic; consumers are pulled, with no reactive streams;
  ids are UUIDv7 ordered by `RawdataIds.ORDER`, never by `UUID.compareTo`; topic names are validated and never
  spliced into SQL, file paths or object names.
- **No backward compatibility with 2.x.** No compatibility shims, deprecated aliases or migration code.
- **Remove what is outdated.** Old code and configuration is removed, not carried along.

## Tests

- **Behaviour goes in the TCK.** Anything an application can see through the API behaves the same in every
  provider. A test of such behaviour goes in `RawdataClientContractTest` (or, for the Avro engine's stores,
  `AvroStoreContractTest`) in `rawdata-client-tck`, never in one provider's tests. Every provider must then pass it
  unchanged; one that fails is fixed, or the behaviour is not the contract yet.
- **A provider's own tests check internals only:** SQL, files and objects, configuration, credentials.
- Tests are JUnit 6. The postgres, gcs, s3 and copy tool tests use Testcontainers, so they need Docker.
- [The TCK](docs/modules/rawdata-client-tck.md#changing-the-contract) says how to change the contract, step by step.

## Dependencies

- **The latest stable version** of every library, plugin, action and engine; no milestones, release candidates or
  alphas. Renovate opens the updates.
- **One version of each library** in every module's compile and runtime dependencies. The enforcer's
  `dependencyConvergence` fails the build otherwise. When two libraries bring different versions, declare the newest
  once, in the module that first needs it, and exclude it from the others.
- **Versions are properties in the root POM**, not `dependencyManagement`. The BOM manages the rawdata modules only.
- **No SNAPSHOT dependencies.** Modules depend on each other with `${project.version}`.
- **The JDK first.** Ask whether the JDK or a library already in the tree does the job before you add one.
- When Maven is bumped, set `distributionSha256Sum` in `.mvn/wrapper/maven-wrapper.properties` to the SHA-256 of
  the new `apache-maven-<version>-bin.zip`, after checking that zip against the `.sha512` Maven publishes next to it.

[Dependencies](docs/dependencies.md) has the details: the exclusions, the banned HTTP clients in `s3`, and how to
add a library.

## Docs and the changelog

- Update the docs in the same pull request as the code: the module's page in [`docs/modules/`](docs/modules), and
  [concepts](docs/concepts.md), [the contract](docs/contract.md) or [the tutorial](docs/tutorial.md) when what an
  application sees changes.
- Add a line to the [CHANGELOG](CHANGELOG.md), in the next release's section at the top (start one if there is none),
  for every change an application or a contributor notices.
- Write as the existing docs do: plain, direct English, short sentences, Markdown wrapped at 120 characters, headings
  in sentence case. Every claim must be true of the code.

## Design decisions

Design and architecture are decided by the owner. Before you change how something works, as opposed to fixing a bug
in it, open an issue that describes the problem and the options, and say which you recommend and why. The rawdata
client is a lightweight, cheap way to stream over object storage, not a pub/sub system or a Kafka competitor.

## Releases

Releases are made only by the owner. Nothing is published as a SNAPSHOT, and CI publishes nothing to Maven Central. See
[releasing](docs/releasing.md).

## License

The project is licensed under the [Apache License 2.0](LICENSE). A contribution you submit is licensed under the
same terms (section 5 of the license).
