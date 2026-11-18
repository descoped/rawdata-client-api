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
4. **Nothing a provider never loads**: unused transports and telemetry are excluded, and banned so they stay out.
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
| `slf4j-api` | postgres, avro | HikariCP, Avro, google-cloud-storage |
| `jackson-core`, `jackson-databind` | avro | Avro, google-cloud-storage (and `jackson-annotations`) |
| `commons-codec` | avro | Avro (through commons-compress), google-cloud-storage |
| `checker-qual` | gcs, provided scope only | google-cloud-storage |

To see what a module resolves: `./mvnw dependency:tree -pl <module>`.

### Exclusions and bans in `gcs`

`google-cloud-storage` brings far more than the provider loads. The provider uses Google's JSON API over HTTP, so
`gcs` excludes what that path never loads:
- gRPC (except `grpc-api` and `grpc-context`) and its GCS stubs;
- Conscrypt;
- the OpenTelemetry SDK and Cloud Monitoring (metrics export);
- Apache HttpClient 4 and Google's adapter to it;
- `com.google.android:annotations` and `threeten-extra`.

The enforcer execution `google-exclusions` bans the same artifacts. If a Google update adds or renames one, the build
fails, and the new artifact goes on the exclusions list.

(Replacing `google-cloud-storage` with a small client on the JDK's `HttpClient` is planned; it removes this list.)

## Adding a library

1. Ask first whether the JDK or a library already in the tree does the job.
2. Add the version as a property in the root POM, at the latest stable release, and use it in the module.
3. `./mvnw verify` on JDK 17. If convergence fails, follow the rule above: declare the newest version once and
   exclude it elsewhere.
4. For a module-path dependency, check that it is a module or has an `Automatic-Module-Name`, and run
   `src/it/jpms/run.sh`.
5. Mention it in the CHANGELOG if applications see it.
