# Troubleshooting

## The build

**"warnings found and -Werror specified", in CI but not locally.** You are building on a newer JDK than 17. Its
`javac` gives fewer warnings: JDK 17's warns, for example, about annotation classes missing from the class path.
Build on JDK 17 (`./mvnw -v` should say 17); see [building and testing](building-and-testing.md#requirements).

**"Dependency convergence error for …".** A module now has two versions of a library, usually after a dependency
update. Declare the newest version once and exclude it from the other path; see
[dependencies](dependencies.md#convergence).

**"… banned via the exclude/include list" in `gcs`.** A Google update brings an artifact the provider never loads.
Add it to the exclusions on `google-cloud-storage`; see [dependencies](dependencies.md#exclusions-and-bans-in-gcs).

**The Maven wrapper fails its checksum.** `distributionSha256Sum` in `.mvn/wrapper/maven-wrapper.properties` doesn't
match the Maven version, usually after a Maven update. Set it to the SHA-256 of the new
`apache-maven-<version>-bin.zip`, after checking that zip against the `.sha512` Maven publishes next to it.

## The tests

**Postgres, gcs or copy-util tests fail with "Could not find a valid Docker environment".** Testcontainers needs
Docker. Start Docker Desktop (or another Docker host), and check with `docker info`.

**Tests in IntelliJ fail with `InaccessibleObjectException` or `IllegalAccessError`.** IntelliJ runs them on the
module path. Reload the Maven project, so IntelliJ picks up the `intellij` profile; see
[building and testing](building-and-testing.md#the-ide).

**The smoke test says "PostgreSQL did not come up within 60 s".** It prints the container's log. A slow first image
pull is the usual cause; run it again.

## Using the library

**`RawdataClient.create` says a provider is unknown.** The provider's jar isn't on the path, or another provider on
the class path fails to load and stops the service loader. Check that the provider's artifact is a runtime
dependency.

**"SLF4J(W): No SLF4J providers were found".** Nothing in your application binds slf4j, so HikariCP's and Avro's log
messages are dropped. Add a backend such as logback. To route the providers' `System.Logger` and the Google
libraries' `java.util.logging` there too, see [getting started](getting-started.md#logging).

**`gcs` on the module path fails with `NoClassDefFoundError: com/fasterxml/jackson/core/JsonFactory`.** Google's
libraries are on the class path and Jackson is on the module path, unresolved. Add `--add-modules ALL-MODULE-PATH`;
see [getting started](getting-started.md#the-module-path).

**A metadata key is refused with `IllegalArgumentException`.** The file-based providers limit a key's length:
- `filesystem`: 127 bytes in UTF-8;
- `gcs`: about 500 bytes, because the object name, with the prefix and topic, must fit in 1024 bytes.
