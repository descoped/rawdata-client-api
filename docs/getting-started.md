# Getting started

## Dependencies

Java 17 or newer. Import the BOM, then add the API and the providers you use:

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
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-api</artifactId>
    </dependency>
    <dependency>
        <groupId>io.descoped.rawdata</groupId>
        <artifactId>rawdata-client-provider-postgres</artifactId>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

| Artifact | Provider |
|---|---|
| `rawdata-client-api` | the API, and `memory` and `discard` |
| `rawdata-client-provider-postgres` | `postgres` |
| `rawdata-client-provider-filesystem` | `filesystem` (brings `rawdata-client-provider-avro`) |
| `rawdata-client-provider-gcs` | `gcs` (brings `rawdata-client-provider-avro`) |
| `rawdata-client-provider-s3` | `s3` (brings `rawdata-client-provider-avro` and the AWS SDK) |

Code depends on the API only; a provider is chosen by configuration and found by the service loader.

Every module's compile and runtime dependencies have one version of each library, the newest stable when the
release was made; the build fails otherwise (the enforcer's `dependencyConvergence`). The libraries the providers
share (slf4j-api, Jackson, commons-codec) are declared by the providers themselves and excluded from the libraries
that bring them (HikariCP, Avro). Maven picks the version nearest to your project, and the
first declared when two are as near, so they resolve to the providers' versions unless your project, or a library
it lists first, brings another. `mvn dependency:tree` shows what you get; the enforcer's `dependencyConvergence`
rule in your own build makes a conflict fail.

## A first producer and consumer

```java
Map<String, String> configuration = Map.of(
        RawdataClient.PROVIDER_KEY, "postgres",          // "rawdata.client.provider"
        "postgres.driver.host", "localhost",
        "postgres.driver.user", "rdc",
        "postgres.driver.password", "rdc",
        "postgres.driver.database", "rdc");

try (RawdataClient client = RawdataClient.create(configuration)) {
    try (RawdataProducer producer = client.producer("invoices")) {
        producer.publish(RawdataMessage.builder()
                .position("invoice-1001")                // your own key of the message, for example a source id
                .put("document", documentBytes)
                .put("metadata", metadataBytes)
                .build());
    }
    try (RawdataConsumer consumer = client.consumer("invoices")) {
        List<RawdataMessage> batch = consumer.receiveUpTo(100, Duration.ofSeconds(5));
        for (RawdataMessage message : batch) {
            System.out.println(message.id() + " " + message.position());
        }
    }
}
```

The provider gives each message a UUIDv7 id when it is published; the topic is read in id order. A consumer that
should continue where it stopped saves the id of the last message it handled, for example in the topic's
[metadata](contract.md#metadata), and starts from it next time: `client.consumer(topic, lastId)`.

## Logging

The providers log through the JDK's `System.Logger`; the libraries they use log through slf4j 2 (HikariCP, Avro) and
`java.util.logging` (the PostgreSQL driver). None of them chooses where logging goes. With slf4j and, for
example, logback in your application:

| To send | Add | |
|---|---|---|
| `System.Logger` to slf4j | `org.slf4j:slf4j-jdk-platform-logging` (runtime) | nothing else; it is a `System.LoggerFinder` |
| `java.util.logging` to slf4j | `org.slf4j:jul-to-slf4j` | and call `SLF4JBridgeHandler.removeHandlersForRootLogger(); SLF4JBridgeHandler.install();` at startup |

Without the first, `System.Logger` goes to `java.util.logging`, which prints INFO and above to standard error. Use
the slf4j version the providers bring (`mvn dependency:tree -Dincludes=org.slf4j`). The copy tool does both, with
slf4j-simple at warn.

## The module path

Every module is a JPMS module. An application module requires only the API:

```java
module com.example.app {
    requires io.descoped.rawdata.api;
}
```

The providers need no `requires`: put them and their dependencies on the module path, and the API's `uses` finds
them:

```sh
java --module-path app.jar:lib --module com.example.app/com.example.app.Main
```

Everything on the class path works too. CI checks both layouts (`src/it/jpms/run.sh`).
