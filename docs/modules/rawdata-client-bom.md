# rawdata-client-bom

`rawdata-client-bom` is a bill of materials: a POM with no code that pins the version of every rawdata library
module. Import it once, then add the API and the providers you use without a version, and they are always the same
release. You need it whenever your build uses more than one rawdata module, which is almost always (the API and a
provider).

This page also covers `rawdata-client-project`, the parent POM of every module, because the BOM inherits from it.

## Coordinates

| | |
|---|---|
| groupId | `io.descoped.rawdata` |
| artifactId | `rawdata-client-bom` |
| packaging | `pom` (import it with `<type>pom</type>` and `<scope>import</scope>`) |
| version | the rawdata release, for example `3.0.0`; every module of a release has the same version |
| JPMS module | none: a BOM holds no classes |

## Quick start

### Maven

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.descoped.rawdata</groupId>
            <artifactId>rawdata-client-bom</artifactId>
            <version>3.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- The code depends on the API only. -->
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-api</artifactId>
    </dependency>
    <!-- A provider is found at run time by the service loader, so runtime scope is enough. -->
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-provider-postgres</artifactId>
        <scope>runtime</scope>
    </dependency>
    <!-- Optional: the provider contract, to run against your own setup in tests. -->
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-tck</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### Gradle

Gradle reads a BOM's `dependencyManagement` as a platform:

```kotlin
dependencies {
    implementation(platform("io.descoped.rawdata:rawdata-client-bom:3.0.0"))
    implementation("io.descoped.rawdata:rawdata-client-api")
    runtimeOnly("io.descoped.rawdata:rawdata-client-provider-postgres")
    testImplementation("io.descoped.rawdata:rawdata-client-tck")
}
```

The platform on `implementation` also aligns the runtime and test class paths, since they extend `implementation`.

To check what you got: `mvn dependency:tree` or `gradle dependencies --configuration runtimeClasspath`. Every
`io.descoped.rawdata` artifact should show the same version.

## Concepts

### A BOM aligns versions, it adds nothing

Importing the BOM adds no dependency to your build. It only says "if you use one of these artifacts, use this
version". You still list each module you need, without a `<version>`. This matters because the modules of a release
are built and tested together and depend on each other with exactly the same version (`${project.version}`): an API
from one release with a provider from another is a combination nobody tested, and across a major version
it may not even load.

### What the BOM manages

Exactly seven artifacts, every library module of the reactor (see [`pom.xml`](../../rawdata-client-bom/pom.xml)):

| Artifact | What it is | Typical scope |
|---|---|---|
| `rawdata-client-api` | the API (`RawdataClient`), the `memory` and `discard` providers, `RawdataIds`, `RawdataCopy`, the provider base classes | compile |
| `rawdata-client-provider-postgres` | provider `postgres`, PostgreSQL 18 | runtime |
| `rawdata-client-provider-filesystem` | provider `filesystem`, Avro files in a local folder | runtime |
| `rawdata-client-provider-gcs` | provider `gcs`, Avro files in a Google Cloud Storage bucket | runtime |
| `rawdata-client-provider-s3` | provider `s3`, Avro files in an Amazon S3 bucket | runtime |
| `rawdata-client-provider-avro` | the Avro engine that `filesystem`, `gcs` and `s3` share; not a provider | none: it comes with them |
| `rawdata-client-tck` | the TCK, the provider contract as JUnit 6 tests; brings JUnit's API at compile scope | test |

You rarely list `rawdata-client-provider-avro` yourself: the three Avro providers depend on it. It is in the BOM so
that its version is managed too when something else in your tree brings it, or when you write an `AvroStore` of your
own.

`rawdata-copy-util` is **not** in the BOM. It is a program, not a library: it is distributed as
`rawdata-copy-<version>.zip` on the GitHub release and is not published to Maven Central. See
[rawdata-copy-util](rawdata-copy-util.md).

### Why the BOM manages only rawdata modules

The BOM does not pin slf4j, Jackson, Avro, HikariCP, pgjdbc, the AWS SDK or any other library. Your application has
its own versions of these, and a BOM that pinned them would override your choices wherever you import it, often
silently. So the root POM keeps every third-party version in a property (`slf4j.version`, `jackson.version`, …), not
in `dependencyManagement`. The BOM inherits from the root POM; had the root POM a `dependencyManagement`, the BOM
would inherit it and pass it on to every importer. Checked with `help:effective-pom`: the BOM's effective
`dependencyManagement` holds the seven rawdata artifacts and nothing else.

The libraries still arrive with sensible versions, as ordinary transitive dependencies of the modules. Each module's
compile and runtime tree has one version of each library, the newest stable when the release was made, and the build
fails otherwise (the enforcer's `dependencyConvergence`). Maven picks the version nearest to your project, so if you
declare a library yourself, yours wins. See [Dependencies](../dependencies.md) for which module declares what and
what it excludes.

### The parent POM, `rawdata-client-project`

`rawdata-client-project` is the root of the repository: the aggregator that lists the modules and the parent every
module inherits, including the BOM. It is published with each release because the modules' POMs name it as their
parent, and Maven needs it to read them.

What it holds, and what that means for you:

| In the parent | Reaches an application? |
|---|---|
| the release version, shared by every module | yes, through the BOM's `${project.version}` |
| licence (Apache 2.0), SCM, developers | as metadata of each published POM |
| dependency versions as properties (`slf4j.version`, …) | no: properties are not imported, and they only fix the versions the modules were built with |
| `maven.compiler.release` 17, `-Xlint:all -Werror`, Javadoc doclint | no: build settings of this repository |
| the enforcer (Java 17, Maven `[3.6.3,4)`, `dependencyConvergence`) | no |
| plugin versions, profiles `central`, `sign`, `coverage`, `sonar` | no |

Import the BOM; do not use `rawdata-client-project` as your own parent. It is made to build this repository: it would
give your project its compiler flags, its enforcer rules (including the ban on Maven 4) and its release
configuration, and it has no `dependencyManagement` to offer you.

## How it works

```
rawdata-client-project (parent, packaging pom)
  properties: versions of third-party libraries
  no dependencyManagement
        |
        | inherits
        v
rawdata-client-bom (packaging pom)
  dependencyManagement: the 7 rawdata artifacts at ${project.version}
        |
        | <scope>import</scope>
        v
your application
  dependencyManagement now pins the 7 rawdata artifacts
  you list the ones you use, without a version
```

When Maven imports a BOM it reads the BOM's effective POM, parent included, and copies its `dependencyManagement`
into yours. Nothing else is copied: no properties, plugins or dependencies. Your own `dependencyManagement` entries
win over imported ones, and among several imported BOMs the first one listed wins for an artifact they both manage.

Managed versions reach transitive dependencies too. If another library in your tree brings, say, an older
`rawdata-client-api`, the BOM's version replaces it.

## Configuration

The BOM has no configuration keys. The only choice is its version.

## Failure modes and troubleshooting

| Symptom | Cause and fix |
|---|---|
| `'dependencies.dependency.version' for io.descoped.rawdata:… is missing` | The BOM is not imported, or it is under `<dependencies>` instead of `<dependencyManagement>`, or `<type>pom</type>` or `<scope>import</scope>` is missing. |
| Maven cannot resolve `rawdata-client-bom:<version>` | That version is not on Maven Central: it is not released yet, or it is a SNAPSHOT, and SNAPSHOTs are never published. Use a released version, or run `./mvnw install` in this repository first. |
| Maven cannot resolve `rawdata-client-project` | The parent POM is needed to read the BOM. It is published with every release; with a SNAPSHOT, `./mvnw install` from the repository root installs it. |
| Maven cannot resolve `rawdata-copy-util` | It is not on Central and not in the BOM. Download the zip from the GitHub release. |
| A rawdata module shows a different version in `dependency:tree` | Something manages it before the BOM: an entry of your own in `dependencyManagement`, or another BOM imported earlier. Remove it, or list the rawdata BOM first. |
| A library (slf4j, Jackson, …) has a version you did not expect | The BOM does not manage it. Maven took the nearest declaration in your tree. Declare the version you want yourself; add the enforcer's `dependencyConvergence` to your build to catch conflicts. |
| Gradle ignores the BOM | It was added as a plain dependency. Use `platform(...)` (or `enforcedPlatform(...)` to override versions other libraries ask for). |

## Testing

The BOM has no tests of its own; it has no code. The reactor builds it with every other module, and
`help:effective-pom` shows what it manages:

```sh
./mvnw -N -f rawdata-client-bom/pom.xml help:effective-pom
```

When a module is added to the reactor, it is added to the BOM by hand, unless it is a program like the copy tool.

In your application, a test of the BOM import is `mvn dependency:tree` (or Gradle's `dependencies`) showing one
rawdata version. To test your code against the client, add `rawdata-client-tck` at test scope through the BOM, or
use the `memory` provider in `rawdata-client-api`; see [rawdata-client-tck](rawdata-client-tck.md) and
[`memory` and `discard`](rawdata-client-api.md#the-memory-and-discard-providers).

## Reference

| File | What it is |
|---|---|
| [`rawdata-client-bom/pom.xml`](../../rawdata-client-bom/pom.xml) | the BOM: `dependencyManagement` of the seven rawdata artifacts |
| [`pom.xml`](../../pom.xml) | the parent and aggregator: modules, version properties, plugins, enforcer, release profiles |

## See also

- [Concepts](../concepts.md) and the [tutorial](../tutorial.md)
- [Logging and the module path](../logging-and-the-module-path.md): routing the client's logging, JPMS
- [Dependencies](../dependencies.md): one version of each library, exclusions and bans
- [Releasing](../releasing.md): what is published, and by whom
- Modules: [rawdata-client-api](rawdata-client-api.md), [rawdata-client-tck](rawdata-client-tck.md),
  [rawdata-client-provider-postgres](rawdata-client-provider-postgres.md),
  [rawdata-client-provider-avro](rawdata-client-provider-avro.md),
  [rawdata-client-provider-filesystem](rawdata-client-provider-filesystem.md),
  [rawdata-client-provider-gcs](rawdata-client-provider-gcs.md),
  [rawdata-client-provider-s3](rawdata-client-provider-s3.md), [rawdata-copy-util](rawdata-copy-util.md)
