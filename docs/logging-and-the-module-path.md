# Logging and the module path

Two things every application sets up once: where the client's logging goes, and whether the client runs on the
module path or the class path.

## Logging

The client chooses no logging backend. Three logging APIs are in play:

| Who | Logs through |
|---|---|
| the Avro engine (`filesystem`, `gcs`, `s3`): window recovery, a failed writer | the JDK's `System.Logger` |
| HikariCP (`postgres`), Apache Avro, the AWS SDK (`s3`) | slf4j 2 |
| the PostgreSQL driver, pgjdbc (`postgres`) | `java.util.logging` |

The API, the `postgres` provider itself and `gcs`'s own HTTP and credential code log nothing.

With slf4j and a backend such as logback in your application:

| To send | Add | |
|---|---|---|
| `System.Logger` to slf4j | `org.slf4j:slf4j-jdk-platform-logging` (runtime) | nothing else; it is a `System.LoggerFinder` |
| `java.util.logging` to slf4j | `org.slf4j:jul-to-slf4j` | and call `SLF4JBridgeHandler.removeHandlersForRootLogger(); SLF4JBridgeHandler.install();` at startup |

Without the first, `System.Logger` goes to `java.util.logging`, which prints INFO and above to standard error.
Without any slf4j backend, slf4j prints a warning once and drops the libraries' logging. Use the slf4j version the
providers bring (`mvn dependency:tree -Dincludes=org.slf4j`).

The [copy tool](modules/rawdata-copy-util.md#logging) does both, with `slf4j-simple` at `warn`.

## The module path

Every module is a named JPMS module. An application module requires only the API:

```java
module com.example.app {
    requires io.descoped.rawdata.api;
}
```

The providers need no `requires`: put them and their dependencies on the module path, and the API's
`uses RawdataClientInitializer` finds them:

```sh
java --module-path app.jar:lib --module com.example.app/com.example.app.Main
```

The provider modules (`postgres`, `filesystem`, `gcs`, `s3`) export nothing. Code that casts to `AvroRawdataClient`,
for example to call `flush` in a test, also needs `requires io.descoped.rawdata.provider.avro;`: the Avro engine's
module exports its package.

Everything on the class path works too: each provider is registered both with `provides` in its `module-info` and in
`META-INF/services`. CI checks both layouts with the JPMS smoke test (`src/it/jpms/run.sh`); see
[building and testing](building-and-testing.md#the-jpms-smoke-test).
