# Dependencies

Applications add these modules next to their own libraries, so what the modules bring must be predictable. The
rules, and how the build keeps them.

## The rules

1. **The latest stable version** of every library, plugin, action and engine; no milestones or release candidates.
   Renovate opens the updates and merges them when CI passes. A new PostgreSQL major stays a pull request for the
   owner.
2. **One version of each library** in every module's compile and runtime dependencies.
3. **Versions are properties in the root POM** (`slf4j.version`, `jackson.version`, …), not `dependencyManagement`.
   The BOM (`rawdata-client-bom`) manages the rawdata modules only.
4. **Few dependencies.** The JDK first: `gcs` talks to GCS on `java.net.http` instead of Google's client libraries.
5. **No SNAPSHOT dependencies**, and modules depend on each other with `${project.version}`.

## How the build keeps them

### Convergence

The root POM's enforcer runs `dependencyConvergence` for compile and runtime scope. Test and provided scope are
excluded, since applications never get them.

When two libraries bring different versions of a third, Maven would pick the one nearest to the root of the tree,
not the newer. So:
- the module that first needs the library declares it at the newest version, through a root POM property;
- the module excludes it from the libraries that bring it.

| Library | Declared in | Excluded from |
|---|---|---|
| `slf4j-api` | postgres, avro, s3 | HikariCP, Avro, the AWS SDK |
| `jackson-core`, `jackson-databind` | avro (and gcs, which uses it) | Avro |
| `commons-codec` | avro | Avro (through commons-compress) |

### The AWS SDK in `s3`

The SDK brings an HTTP client of its own choosing: Apache by default, Netty for async. `s3` uses the SDK's
`url-connection-client` (the JDK's `HttpURLConnection`) and excludes `apache-client`, `apache5-client` and
`netty-nio-client`; the enforcer execution `aws-http-clients` bans them, the CRT, Netty and Apache HttpComponents, so
an SDK update cannot bring them back unnoticed.

To see what a module resolves: `./mvnw dependency:tree -pl <module>`.

## Adding a library

1. Ask first whether the JDK or a library already in the tree does the job.
2. Add the version as a property in the root POM, at the latest stable release, and use it in the module.
3. `./mvnw verify` on JDK 17. If convergence fails, follow the rule above: declare the newest version once and
   exclude it elsewhere.
4. For a module-path dependency, check that it is a module or has an `Automatic-Module-Name`, and run
   `src/it/jpms/run.sh`.
5. Mention it in the CHANGELOG if applications see it.
