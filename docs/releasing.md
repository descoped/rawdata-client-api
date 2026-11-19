# Releasing

Releases are made **only by the owner, or on his explicit word**. CI never publishes, and SNAPSHOTs are never
published: the Central Portal's free tier limits monthly publishing. This page says what a release involves.

## What is released

- Every module except the copy tool goes to **Maven Central** through the Central Portal: the API, the TCK, the
  providers and the BOM, with sources, Javadoc and signatures.
- The copy tool, `rawdata-copy-<version>.zip`, goes on the **GitHub release**. It is a program, not a library, and is
  excluded from the Central bundle (`excludeArtifacts`).

All modules share one version, and a tag is the bare version, for example `3.0.0`.

## Before a release

- `master` is green: CI, the smoke test and SonarCloud.
- `CHANGELOG.md` has the release's section, with its date.
- The checks that CI can't make are done, for example a copy against a real cloud bucket with real credentials.
- `~/.m2/settings.xml` has the server `central` with the Central Portal token and
  `<repositoryOrigins>https://central.sonatype.com</repositoryOrigins>`. Maven 3.10 only sends a server's credentials
  to the hosts it knows for that id. GPG is set up for signing.

## The steps

```sh
./mvnw release:prepare            # sets the release version, tags it, sets the next SNAPSHOT
./mvnw release:perform            # builds the tag with -P central,sign and uploads the bundle
```

- `release:perform` uploads the bundle to the Central Portal without publishing it (`autoPublish=false`,
  `waitUntil=validated`). Publish it in the Portal after checking the deployment.
- Build the copy tool from the tag: `./mvnw -pl rawdata-copy-util -am package -DskipTests` in
  `target/checkout`. Then attach `rawdata-copy-util/target/rawdata-copy-<version>.zip` to a GitHub release for the
  tag, with the CHANGELOG section as its notes.
