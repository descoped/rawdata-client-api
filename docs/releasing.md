# Releasing

Releases are made **only by the owner, or on his explicit word**. SNAPSHOTs are never published: the Central
Portal's free tier limits monthly publishing.

A release is two things, in this order:

1. **Maven Central**: every module except the copy tool, with sources, Javadoc and GPG signatures. Signed and
   uploaded from the owner's machine; CI never publishes to Central.
2. **The GitHub release** for the tag `v<version>`, for example `v3.0.0`: the CHANGELOG section as its notes, and
   the copy tool, `rawdata-copy-<version>.zip`, with its SHA-256. The copy tool is a program, not a library, and is
   left out of the Central bundle (`excludeArtifacts`). The `Release` workflow publishes it once the version is on
   Central.

All modules share one version. The tag is the version with a `v`: `v3.0.0`.

## Before a release

- `master` is green: CI, the smoke test and SonarCloud.
- `CHANGELOG.md` has a section `## <version>` with the release's changes; the GitHub release takes its notes from
  it.
- The checks that CI can't make are done, for example a copy against a real cloud bucket with real credentials.
- `~/.m2/settings.xml` has the server `central` with the Central Portal token and
  `<repositoryOrigins>https://central.sonatype.com</repositoryOrigins>`. Maven 3.10 only sends a server's credentials
  to the hosts it knows for that id. On the owner's machine the token is decrypted by `~/.mavenrc`, only for a run
  with `-P secrets`; without it the upload fails with `401`.
- GPG signs with its default key, and the key's public part is on a key server Central checks
  (`keys.openpgp.org` or `keyserver.ubuntu.com`).

## The steps

### 1. Prepare: version, tag

```sh
./mvnw release:prepare
```

It asks for the release version (`3.0.0`), the tag (`v3.0.0`) and the next development version
(`3.0.1-SNAPSHOT`). It builds and tests, commits the release version, tags it, commits the next SNAPSHOT and pushes
both commits and the tag. The tag push runs CI on the tag.

### 2. Perform: sign and upload to Central

```sh
java -version                     # 17, as CI and the Release workflow build
MVN_SECRETS=central ./mvnw release:perform -P secrets
```

Run it on JDK 17: a newer JDK writes another `Build-Jdk-Spec` into the jars, and the copy tool's jars would no longer be
the bytes on Central. It checks out the tag into `target/checkout` and runs `deploy -P central,sign`: every artifact is
signed with your GPG key (gpg-agent asks for the passphrase) and the bundle is uploaded to the Central Portal. It is not
published: `autoPublish=false`, and the upload waits until the Portal has validated it. The log ends with the
deployment id and `has been validated`.

### 3. Publish on Central

In the Central Portal, under Deployments, check the deployment (the modules, their signatures, the POMs) and press
Publish. Publishing is final: a version on Central can never be changed or removed. It is on
`repo1.maven.org` after it is synced, usually within half an hour.

### 4. Publish the GitHub release

```sh
gh workflow run release.yml -f version=3.0.0
gh run watch
```

The `Release` workflow (`.github/workflows/release.yml`):

1. checks that `3.0.0` is on Maven Central, and stops if it is not yet;
2. checks out the tag `v3.0.0` and builds the copy tool on JDK 17. The build is reproducible, so the jars in the zip
   are the bytes on Central;
3. takes the section `## 3.0.0` of `CHANGELOG.md` as the release notes, and stops if there is none;
4. publishes the GitHub release `v3.0.0` with those notes, `rawdata-copy-3.0.0.zip` and
   `rawdata-copy-3.0.0.zip.sha256`.

If it stops because the version is not on Central yet, run it again later. The release is then on
`https://github.com/descoped/rawdata-client-project/releases/tag/v3.0.0`.

## If something goes wrong

- **`release:prepare` fails before it pushes**: `./mvnw release:rollback`, fix, and prepare again.
- **`release:perform` fails, or the Portal rejects the deployment**: drop the deployment in the Portal, if there is
  one, fix, and run `release:perform` again. `release.properties` still points at the tag, so it builds the same
  tag. The tag was tested by `release:prepare`, so a rerun can skip the tests: `-Darguments=-DskipTests`. Nothing is
  public until you press Publish.
- **The upload fails with `401`**: the token was not loaded. Run `release:perform` again with `-P secrets`.
- **The `Release` workflow fails**: fix the cause and run it again. A rerun updates a release that was created
  already: it uploads the assets again and replaces the notes. It reads the CHANGELOG from the tag, so if the tag has
  no section for the version, create the release by hand: `gh release create v<version>` with the notes and the zip
  built from the tag.
