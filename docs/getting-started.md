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
| `rawdata-client-provider-avro` | `filesystem` |
| `rawdata-client-provider-gcs` | `gcs` (brings `rawdata-client-provider-avro`) |

Code depends on the API only; a provider is chosen by configuration and found by the service loader.

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

## The module path

Every module is a JPMS module. An application module requires only the API:

```java
module com.example.app {
    requires io.descoped.rawdata.api;
}
```

The providers need no `requires`: put them on the module path and the API's `uses` finds them. The one exception is
`gcs`: the Google Cloud libraries cannot all be modules (they split packages among themselves), so put them on the
class path, keep the rawdata jars on the module path, and add `--add-modules ALL-MODULE-PATH`:

```sh
java --module-path app.jar:rawdata-jars... --class-path google-jars... --add-modules ALL-MODULE-PATH \
     --module com.example.app/com.example.app.Main
```

Everything on the class path works too. CI checks both layouts (`src/it/jpms/run.sh`).
