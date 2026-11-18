# Troubleshooting

## The build

**"warnings found and -Werror specified", in CI but not locally.** You are building on a newer JDK than 17. Its
`javac` gives fewer warnings: JDK 17's warns, for example, about annotation classes missing from the class path.
Build on JDK 17 (`./mvnw -v` should say 17); see [building and testing](building-and-testing.md#requirements).

**"Dependency convergence error for …".** A module now has two versions of a library, usually after a dependency
update. Declare the newest version once and exclude it from the other path; see
[dependencies](dependencies.md#convergence).

**The Maven wrapper fails its checksum.** `distributionSha256Sum` in `.mvn/wrapper/maven-wrapper.properties` doesn't
match the Maven version, usually after a Maven update. Set it to the SHA-256 of the new
`apache-maven-<version>-bin.zip`, after checking that zip against the `.sha512` Maven publishes next to it.

## The tests

**Postgres, gcs, s3 or copy-util tests fail with "Could not find a valid Docker environment".** Testcontainers needs
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
messages are dropped. Add a backend such as logback. To route the providers' `System.Logger` and the PostgreSQL
driver's `java.util.logging` there too, see [getting started](getting-started.md#logging).

**`gcs` says "No Application Default Credentials".** None of the three sources was found: set
`GOOGLE_APPLICATION_CREDENTIALS` to a key file, run `gcloud auth application-default login`, or run on Google Cloud.
See the [`gcs` credentials](providers/gcs.md#credentials).

**`s3` fails with "Unable to load credentials from any of the providers in the chain" or "Unable to load region".**
The SDK's default chains found nothing: set `AWS_PROFILE` (and run `aws sso login` for an SSO profile), or the
`AWS_*` environment variables, or `s3.region`. See the [`s3` credentials](providers/s3.md#credentials-and-region).

**`s3` fails with a TLS error on a bucket name with dots.** Virtual-hosted HTTPS cannot serve such names; don't set
`s3.path-style-access=false` for them. See [bucket names with dots](providers/s3.md#bucket-names-with-dots).

**A metadata key is refused with `IllegalArgumentException`.** The file-based providers limit a key's length:
- `filesystem`: 127 bytes in UTF-8;
- `gcs`, `s3`: about 500 bytes, because the object name, with the prefix and topic, must fit in 1024 bytes.
