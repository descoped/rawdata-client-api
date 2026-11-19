# Tutorial: from zero to a resumable reader

This tutorial builds a small program that writes invoices to a topic and reads them back, step by step. It runs on
the `filesystem` provider, so you need nothing but Java and Maven: no database, no cloud account. At the end you switch
the same code to PostgreSQL, Google Cloud Storage or Amazon S3 by changing the configuration only.

You will learn to:

1. add the dependencies;
2. configure a client;
3. publish messages;
4. close a producer to make what it published visible;
5. read a topic with a consumer;
6. start reading after an id, or at a position, with a cursor;
7. seek by time;
8. read a topic's last message;
9. keep a checkpoint in the topic's metadata;
10. handle the errors you will meet;
11. see what happens when two processes write one topic;
12. switch the store.

Each step has code you can run and the output you should see. Ids and times in the output are from one run; yours
differ. The [concepts](concepts.md) page explains every idea used here in depth.

## Before you start

You need:

- **JDK 17 or newer.** `java -version` should print 17 or higher.
- **Maven 3.9 or newer.** `mvn -version` should print it, and the JDK above.

## Step 1: create the project

Make an empty folder, for example `rawdata-tutorial`, and work in it for the rest of the tutorial.

Create `pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>rawdata-tutorial</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.release>17</maven.compiler.release>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <rawdata.version>3.0.0</rawdata.version>
    </properties>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>io.descoped.rawdata</groupId>
                <artifactId>rawdata-client-bom</artifactId>
                <version>${rawdata.version}</version>
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
            <artifactId>rawdata-client-provider-filesystem</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.20</version>
            <scope>runtime</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>exec-maven-plugin</artifactId>
                <version>3.7.0</version>
            </plugin>
        </plugins>
    </build>
</project>
```

What each part does:

- **The BOM** (`rawdata-client-bom`, imported in `dependencyManagement`) sets the version of every rawdata module, so
  the dependencies below need none. It manages rawdata modules only, never your other libraries.
- **`rawdata-client-api`** is what your code compiles against. Your code never names a provider class.
- **`rawdata-client-provider-filesystem`** is the store, at `runtime` scope: it is found at run time by the name in
  the configuration.
- **`slf4j-simple`** gives Avro, which the provider uses, somewhere to log. Without a logging backend, slf4j prints a
  warning on every run. In an application, use your own backend; see
  [logging](logging-and-the-module-path.md#logging).
- **`exec-maven-plugin`** runs the tutorial's programs with `mvn exec:java`.

Read more: [the BOM](modules/rawdata-client-bom.md), [the API module](modules/rawdata-client-api.md).

## Step 2: configure a client

A client is configured by a map of strings. The key `rawdata.client.provider` names the provider, and the provider
reads its own keys from the same map. The tutorial keeps the map in a file, so that step 12 can switch the store
without touching the code.

Create `rawdata.properties` in the project folder:

```properties
rawdata.client.provider=filesystem
filesystem.storage-folder=data/topics
local-temp-folder=data/buffer
```

- `filesystem.storage-folder` is where the topics are kept. It is created if it is missing.
- `local-temp-folder` is where producers buffer messages before they write them to the topic (more in step 4).

Relative paths are resolved against the folder you run Maven from, so everything lands in `data/`.

Create `src/main/java/com/example/tutorial/Store.java`, which reads the file:

```java
package com.example.tutorial;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Reads the client's configuration from rawdata.properties in the working directory. */
final class Store {

    static final String TOPIC = "invoices";

    private Store() {
    }

    static Map<String, String> configuration() {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(Path.of("rawdata.properties"), StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, String> configuration = new HashMap<>();
        properties.stringPropertyNames().forEach(key -> configuration.put(key, properties.getProperty(key)));
        return configuration;
    }
}
```

Every program below calls `RawdataClient.create(Store.configuration())`. A client is meant to be made once per
process and shared; these programs are small processes of their own, so each makes one.

Read more: [concepts: configuration](concepts.md#configuration),
[filesystem configuration](modules/rawdata-client-provider-filesystem.md#configuration).

## Step 3: publish messages

A message has a **position**, your own identifier of it (here the invoice number), and named binary **content**. The
producer gives it an **id** when it publishes.

Create `src/main/java/com/example/tutorial/Publish.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Publishes one invoice per argument, in one publish call. */
public class Publish {

    public static void main(String[] args) {
        List<RawdataMessage> messages = new ArrayList<>();
        for (String number : args) {
            messages.add(RawdataMessage.builder()
                    .position("invoice-" + number)                       // your own id of the message
                    .put("document", ("<invoice id='" + number + "'/>").getBytes(StandardCharsets.UTF_8))
                    .put("source", "billing".getBytes(StandardCharsets.UTF_8))
                    .build());
        }
        try (RawdataClient client = RawdataClient.create(Store.configuration());
             RawdataProducer producer = client.producer(Store.TOPIC)) {
            producer.publish(messages);
            System.out.println("Published " + messages.size() + (messages.size() == 1 ? " message" : " messages")
                    + " to " + Store.TOPIC);
        } // closing the producer uploads what it published; closing the client closes the rest
    }
}
```

Run it with three invoice numbers:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Publish -Dexec.args="1001 1002 1003"
```

You should see:

```text
Published 3 messages to invoices
```

The topic `invoices` did not exist before: a topic exists from its first use. Look at what was written:

```sh
find data -type f | sort
```

```text
data/buffer/invoices/.lock
data/topics/invoices/segments/0000000000000000000.avro
```

The three messages are in one Avro file, segment 0 of the topic. `.lock` is the lock a writing client holds on its
buffer folder for the topic.

What happened:

- `publish` wrote the whole call, all three messages or none, in the order given.
- The producer gave each message a UUIDv7 id, greater than every id already in the topic.
- The try-with-resources closed the producer, which uploaded the messages as a segment, then closed the client.

Read more: [concepts: message](concepts.md#message), [concepts: producer](concepts.md#producer),
[concepts: segments](concepts.md#segments).

## Step 4: close the producer to make messages visible

In the `filesystem`, `gcs` and `s3` providers, `publish` does not write to the topic directly. It appends the
messages to the topic's **upload window**, a local file in `local-temp-folder`. The window is uploaded as the next
segment when it closes: after `avro-file.max.seconds` (60 by default), when it reaches `avro-file.max.bytes`
(64 MiB), or when a producer of the topic closes. Until then, no consumer sees the messages.

See it happen. Create `src/main/java/com/example/tutorial/Window.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Shows when a published message becomes visible to a consumer. */
public class Window {

    static final String TOPIC = "window-demo";

    public static void main(String[] args) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(Store.configuration())) {
            RawdataMessage end = client.lastMessage(TOPIC);           // null while the topic is empty
            UUID start = end == null ? null : end.id();               // read only what this run publishes

            RawdataProducer producer = client.producer(TOPIC);
            producer.publish(RawdataMessage.builder()
                    .position("hello")
                    .put("text", "Hello, rawdata".getBytes(StandardCharsets.UTF_8))
                    .build());
            try (RawdataConsumer consumer = client.consumer(TOPIC, start)) {
                System.out.println("Before close: " + text(consumer.receive(2, TimeUnit.SECONDS)));
            }

            producer.close();                                         // uploads the producer's window
            try (RawdataConsumer consumer = client.consumer(TOPIC, start)) {
                System.out.println("After close:  " + text(consumer.receive(2, TimeUnit.SECONDS)));
            }
        }
    }

    static String text(RawdataMessage message) {
        return message == null ? "nothing" : new String(message.get("text"), StandardCharsets.UTF_8);
    }
}
```

Run it:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Window
```

You should see, after a two-second wait:

```text
Before close: nothing
After close:  Hello, rawdata
```

The first consumer waited two seconds and got nothing, because the message was still in the open window. Closing
the producer uploaded the window, and the second consumer read the message at once.

This is the one behaviour that differs between providers. In `postgres` and `memory` a message is visible when
`publish` returns, and both lines print `Hello, rawdata`. In every provider, closing a producer makes what it
published visible and durable, so close producers when you are done with them, and close the client at shutdown.

Read more: [concepts: visibility and durability](concepts.md#visibility-and-durability),
[concepts: the upload window](concepts.md#the-upload-window),
[Avro engine: upload window](modules/rawdata-client-provider-avro.md#upload-window).

## Step 5: read a topic

A consumer reads one topic in id order, from a starting point, and is **pulled**: nothing arrives until you ask.
`receiveUpTo(max, timeout)` waits up to the timeout for the first message, then takes what is there at once, up to
`max`, and returns an empty list if nothing came.

Create `src/main/java/com/example/tutorial/Read.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/** Reads the topic from the beginning and prints every message. */
public class Read {

    public static void main(String[] args) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(Store.configuration());
             RawdataConsumer consumer = client.consumer(Store.TOPIC)) {
            printAll(consumer);
        }
    }

    /** Pulls batches until a receive finds nothing new for one second. */
    static void printAll(RawdataConsumer consumer) throws InterruptedException {
        List<RawdataMessage> batch = consumer.receiveUpTo(100, Duration.ofSeconds(1));
        while (!batch.isEmpty()) {
            for (RawdataMessage message : batch) {
                print(message);
            }
            batch = consumer.receiveUpTo(100, Duration.ofSeconds(1));
        }
    }

    static void print(RawdataMessage message) {
        System.out.println(message.id() + "  " + Instant.ofEpochMilli(message.timestamp()) + "  "
                + message.position() + "  " + new String(message.get("document"), StandardCharsets.UTF_8));
    }
}
```

Run it:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Read
```

You should see the three invoices, in the order they were published:

```text
01a1f120-a0ae-7610-acd1-8310ba3c8bfb  2026-11-19T00:06:49.774Z  invoice-1001  <invoice id='1001'/>
01a1f120-a0ae-76dd-ad9f-ea09037cd59d  2026-11-19T00:06:49.774Z  invoice-1002  <invoice id='1002'/>
01a1f120-a0ae-7764-9158-cefa857766dc  2026-11-19T00:06:49.774Z  invoice-1003  <invoice id='1003'/>
```

Each line is the id, the time in the id (`message.timestamp()`), the position and the content. The ids start with
the time, so they sort by time; within one millisecond they still increase. The program ends one second after the
last message, when a receive finds nothing new.

Now publish two more invoices, a few seconds later, so that the next steps have two batches written at different
times:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Publish -Dexec.args="1004 1005"
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Read
```

```text
Published 2 messages to invoices
01a1f120-a0ae-7610-acd1-8310ba3c8bfb  2026-11-19T00:06:49.774Z  invoice-1001  <invoice id='1001'/>
01a1f120-a0ae-76dd-ad9f-ea09037cd59d  2026-11-19T00:06:49.774Z  invoice-1002  <invoice id='1002'/>
01a1f120-a0ae-7764-9158-cefa857766dc  2026-11-19T00:06:49.774Z  invoice-1003  <invoice id='1003'/>
01a1f120-ed07-70cc-afa9-86d782e0f976  2026-11-19T00:07:09.319Z  invoice-1004  <invoice id='1004'/>
01a1f120-ed07-7312-88a8-62ddfe1af4a8  2026-11-19T00:07:09.319Z  invoice-1005  <invoice id='1005'/>
```

The second batch is segment 1. A consumer reads across segments without noticing them.

If you sort ids in your own code, use `RawdataIds.ORDER`, never `UUID.compareTo`, which compares signed numbers and
gets the order of some ids wrong.

Read more: [concepts: consumer](concepts.md#consumer), [concepts: pull reading](concepts.md#pull-reading),
[concepts: id and order](concepts.md#id-and-order).

## Step 6: start after an id, or at a position

A consumer can start anywhere. `client.consumer(topic, id)` starts after the message with that id, which is how a
reader continues where it stopped. `client.cursorOf(topic, position, inclusive, approxTimestamp, tolerance)` finds the
first message with a position, searching only the messages written within `approxTimestamp ± tolerance`: a position
is your identifier, not the store's, and stores do not index it.

Create `src/main/java/com/example/tutorial/After.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCursor;

import java.time.Duration;
import java.util.UUID;

/** Starts reading after an id, and at a position. */
public class After {

    public static void main(String[] args) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(Store.configuration())) {
            UUID id = UUID.fromString(args[0]);
            System.out.println("After " + id + ":");
            try (RawdataConsumer consumer = client.consumer(Store.TOPIC, id)) {
                Read.printAll(consumer);
            }

            System.out.println("From invoice-1002:");
            RawdataCursor cursor = client.cursorOf(Store.TOPIC, "invoice-1002", true,
                    System.currentTimeMillis(), Duration.ofDays(1));
            try (RawdataConsumer consumer = client.consumer(Store.TOPIC, cursor)) {
                Read.printAll(consumer);
            }
        }
    }
}
```

Run it with the id of `invoice-1002` from your own output of step 5:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.After -Dexec.args="01a1f120-a0ae-76dd-ad9f-ea09037cd59d"
```

```text
After 01a1f120-a0ae-76dd-ad9f-ea09037cd59d:
01a1f120-a0ae-7764-9158-cefa857766dc  2026-11-19T00:06:49.774Z  invoice-1003  <invoice id='1003'/>
01a1f120-ed07-70cc-afa9-86d782e0f976  2026-11-19T00:07:09.319Z  invoice-1004  <invoice id='1004'/>
01a1f120-ed07-7312-88a8-62ddfe1af4a8  2026-11-19T00:07:09.319Z  invoice-1005  <invoice id='1005'/>
From invoice-1002:
01a1f120-a0ae-76dd-ad9f-ea09037cd59d  2026-11-19T00:06:49.774Z  invoice-1002  <invoice id='1002'/>
01a1f120-a0ae-7764-9158-cefa857766dc  2026-11-19T00:06:49.774Z  invoice-1003  <invoice id='1003'/>
01a1f120-ed07-70cc-afa9-86d782e0f976  2026-11-19T00:07:09.319Z  invoice-1004  <invoice id='1004'/>
01a1f120-ed07-7312-88a8-62ddfe1af4a8  2026-11-19T00:07:09.319Z  invoice-1005  <invoice id='1005'/>
```

The first consumer starts **after** the id, so `invoice-1002` itself is not read. The cursor was made with
`inclusive` true, so the second consumer starts **at** `invoice-1002`. `client.consumer(topic, id, true)` does the
same for an id.

Read more: [concepts: cursor and seek](concepts.md#cursor-and-seek), [concepts: position](concepts.md#position).

## Step 7: seek by time

Because an id starts with its time, a time is a starting point too. `consumer.seek(timestamp)` moves a consumer to
the first message written at or after a time, in milliseconds since 1970.

Create `src/main/java/com/example/tutorial/Seek.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;

import java.time.Instant;

/** Reads the messages written at or after a time. */
public class Seek {

    public static void main(String[] args) throws InterruptedException {
        long time = Instant.parse(args[0]).toEpochMilli();
        try (RawdataClient client = RawdataClient.create(Store.configuration());
             RawdataConsumer consumer = client.consumer(Store.TOPIC)) {
            consumer.seek(time);
            Read.printAll(consumer);
        }
    }
}
```

Run it with a time between your two batches, for example a second before `invoice-1004` was written:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Seek -Dexec.args="2026-11-19T00:07:08Z"
```

```text
01a1f120-ed07-70cc-afa9-86d782e0f976  2026-11-19T00:07:09.319Z  invoice-1004  <invoice id='1004'/>
01a1f120-ed07-7312-88a8-62ddfe1af4a8  2026-11-19T00:07:09.319Z  invoice-1005  <invoice id='1005'/>
```

Only the second batch is read. `RawdataIds.beginningOf(millis)` gives the same starting point as an id, for
`client.cursorOf(topic, RawdataIds.beginningOf(millis), true)`.

Read more: [concepts: cursor and seek](concepts.md#cursor-and-seek),
[Avro engine: reading](modules/rawdata-client-provider-avro.md#reading).

## Step 8: read the last message

`client.lastMessage(topic)` returns the message with the greatest id, or null for an empty topic. It is cheap in every
provider, and it is where a writer or a copy continues.

Create `src/main/java/com/example/tutorial/Last.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataMessage;

/** Prints the last message of a topic. */
public class Last {

    public static void main(String[] args) {
        try (RawdataClient client = RawdataClient.create(Store.configuration())) {
            for (String topic : new String[]{Store.TOPIC, "orders"}) {
                RawdataMessage last = client.lastMessage(topic);
                System.out.println(topic + ": " + (last == null ? "empty" : last.position() + " " + last.id()));
            }
        }
    }
}
```

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Last
```

```text
invoices: invoice-1005 01a1f120-ed07-7312-88a8-62ddfe1af4a8
orders: empty
```

`orders` was never written. It is not an error: an unknown topic is an empty topic.

Read more: [concepts: last message](concepts.md#last-message).

## Step 9: keep a checkpoint in the topic's metadata

A real reader must continue where it stopped after a restart. It saves the id of the last message it handled, and
starts after it next time. The topic's **metadata**, a small key-value store that belongs to the topic, is the place
for that checkpoint.

Create `src/main/java/com/example/tutorial/Checkpoint.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** A reader that remembers, in the topic's metadata, the last message it handled. */
public class Checkpoint {

    static final String KEY = "billing-reader.checkpoint";

    public static void main(String[] args) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(Store.configuration())) {
            RawdataMetadataClient metadata = client.metadata(Store.TOPIC);
            byte[] saved = metadata.get(KEY);
            UUID checkpoint = saved == null ? null : UUID.fromString(new String(saved, StandardCharsets.UTF_8));
            System.out.println("Checkpoint: " + (checkpoint == null ? "none, reading from the beginning" : checkpoint));

            int handled = 0;
            try (RawdataConsumer consumer = client.consumer(Store.TOPIC, checkpoint)) {  // null: the beginning
                List<RawdataMessage> batch = consumer.receiveUpTo(100, Duration.ofSeconds(1));
                while (!batch.isEmpty()) {
                    for (RawdataMessage message : batch) {
                        System.out.println("Handled " + message.position());
                        handled++;
                    }
                    UUID last = batch.get(batch.size() - 1).id();
                    metadata.put(KEY, last.toString().getBytes(StandardCharsets.UTF_8));
                    batch = consumer.receiveUpTo(100, Duration.ofSeconds(1));
                }
            }
            System.out.println(handled + " new messages; metadata keys: " + metadata.keys());
        }
    }
}
```

Run it twice:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Checkpoint
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Checkpoint
```

```text
Checkpoint: none, reading from the beginning
Handled invoice-1001
Handled invoice-1002
Handled invoice-1003
Handled invoice-1004
Handled invoice-1005
5 new messages; metadata keys: [billing-reader.checkpoint]
Checkpoint: 01a1f120-ed07-7312-88a8-62ddfe1af4a8
0 new messages; metadata keys: [billing-reader.checkpoint]
```

The second run found the checkpoint and had nothing new to do. Publish one more invoice and run the reader again:

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Publish -Dexec.args="1006"
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Checkpoint
```

```text
Published 1 message to invoices
Checkpoint: 01a1f120-ed07-7312-88a8-62ddfe1af4a8
Handled invoice-1006
1 new messages; metadata keys: [billing-reader.checkpoint]
```

Things to know about metadata:

- It is written to the store when `put` returns. It does not go through the upload window.
- Every metadata client of the topic sees the same entries, in any process.
- A key is any string without NUL. Keep keys within 127 bytes of UTF-8 and they work in every provider;
  `filesystem` takes no more.
- The reader saves the checkpoint after it has handled a batch. If it crashes in between, it handles that batch
  again on restart. Make the handling safe to repeat, or save the checkpoint more often.

Read more: [concepts: metadata](concepts.md#metadata), [API: metadata](modules/rawdata-client-api.md#metadata).

## Step 10: handle errors

The client reports a wrong argument with `IllegalArgumentException` or `NullPointerException`, and a problem with the
store or the operation with `RawdataException` or one of its subclasses. All are unchecked.

Create `src/main/java/com/example/tutorial/Errors.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;

import java.time.Duration;

/** The exceptions an application meets most often. */
public class Errors {

    public static void main(String[] args) {
        RawdataClient client = RawdataClient.create(Store.configuration());

        try {
            client.producer("invoices/2026");
        } catch (IllegalArgumentException e) {
            System.out.println("Bad topic name:   " + e.getMessage());
        }

        try {
            RawdataMessage.builder().put("document", new byte[0]).build();
        } catch (IllegalArgumentException e) {
            System.out.println("No position:      " + e.getMessage());
        }

        try {
            client.cursorOf(Store.TOPIC, "invoice-9999", true, System.currentTimeMillis(), Duration.ofDays(1));
        } catch (RawdataNoSuchPositionException e) {
            System.out.println("Unknown position: " + e.getMessage());
        }

        client.close();
        try {
            client.lastMessage(Store.TOPIC);
        } catch (RawdataClosedException e) {
            System.out.println("Closed client:    " + e.getMessage());
        }
    }
}
```

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Errors
```

```text
Bad topic name:   Invalid topic name (1-100 of A-Z a-z 0-9 _ . -, starting with a letter or digit): 'invoices/2026'
No position:      position cannot be null
Unknown position: Position not found in topic invoices: invoice-9999
Closed client:    client is closed
```

| Exception | Means | What to do |
|---|---|---|
| `IllegalArgumentException` | a bad argument: topic name, missing position, NUL in a key, an id that is not a UUIDv7, a missing configuration key | fix the call or the configuration |
| `RawdataNoSuchPositionException` | no message with that position in the time window | widen the window, or start from an id |
| `RawdataClosedException` | the client, producer or consumer was closed | do not use it after `close()` |
| `RawdataWriterConflictException` | another process wrote the topic (step 11) | stop the other writer, then make a new client |
| `RawdataException` | the store failed, or does not exist | check the store; a missing bucket or database is never read as an empty topic |
| `ProviderException` | no provider has the configured name | add the provider's dependency |

Read more: [API: exceptions](modules/rawdata-client-api.md#exceptions),
[troubleshooting](troubleshooting.md).

## Step 11: one writer per topic

Only one process writes a topic. Several producers in that process may publish, and consumers may be anywhere, but a
second writing process is a mistake, and the client tells you so: the writer that finds the topic moved under it
fails with `RawdataWriterConflictException` and keeps failing for that topic. Two writers never merge into one stream.

This program makes two clients on one store, each with a buffer folder of its own, as two processes would have:

Create `src/main/java/com/example/tutorial/Conflict.java`:

```java
package com.example.tutorial;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataWriterConflictException;

import java.util.HashMap;
import java.util.Map;

/** Two clients, standing in for two processes, write one topic: the first one fails. */
public class Conflict {

    public static void main(String[] args) {
        try (RawdataClient first = RawdataClient.create(process("data/buffer-first"));
             RawdataClient second = RawdataClient.create(process("data/buffer-second"))) {
            publish(first, "first-1");
            System.out.println("first wrote first-1");
            publish(second, "second-1");
            System.out.println("second wrote second-1");
            try {
                publish(first, "first-2");
                System.out.println("first wrote first-2");
            } catch (RawdataWriterConflictException e) {
                System.out.println("first failed: " + e.getMessage() + " (topic " + e.topic() + ")");
            }
        }
    }

    /** The store of rawdata.properties, with a buffer folder of its own, as another process would have. */
    static Map<String, String> process(String bufferFolder) {
        Map<String, String> configuration = new HashMap<>(Store.configuration());
        configuration.put("local-temp-folder", bufferFolder);
        return configuration;
    }

    static void publish(RawdataClient client, String position) {
        try (RawdataProducer producer = client.producer("conflict-demo")) {
            producer.publish(RawdataMessage.builder().position(position).build());
        }
    }
}
```

```sh
mvn -q compile exec:java -Dexec.mainClass=com.example.tutorial.Conflict
```

```text
first wrote first-1
second wrote second-1
first failed: Another client published to topic 'conflict-demo': only one process may write a topic (topic conflict-demo)
```

The `filesystem` provider also logs the conflict on standard error, through the JDK's `System.Logger`, naming the
buffer folder where it keeps what it could not write:

```text
Nov 19, 2026 1:07:34 AM io.descoped.rawdata.provider.avro.AvroTopicWriter fail
SEVERE: Another process writes topic conflict-demo; this client stops writing it. Windows not uploaded are kept as *.invalid in .../data/buffer-first/conflict-demo
```

`first` remembered where it last wrote the topic. When it published again, `second` had written after that point,
so `first` failed and wrote nothing. `second` can go on writing; a new client made after `second` stops starts from
the topic's end.

When the conflict shows depends on the provider. In `postgres` it is the `publish` that finds it. In `filesystem`,
`gcs` and `s3` it is found when a window opens or uploads, so it may also come from a later `publish`, from `flush` or
from `close`. Two clients that share one buffer folder fail even earlier, with "The buffer folder … is in use by
another client": give each writing process its own `local-temp-folder`.

Read more: [concepts: one writer per topic](concepts.md#one-writer-per-topic),
[the contract](contract.md#one-writer-per-topic).

## Step 12: switch the store

Every provider keeps the same contract, checked by the same tests (the [TCK](concepts.md#the-tck)), so the programs
above run unchanged on another store. Two things change: a runtime dependency in `pom.xml`, and `rawdata.properties`.
Keep a copy of the filesystem configuration if you want to come back to it: `cp rawdata.properties
filesystem.properties`.

The examples below run each store locally in Docker, so you can try them without an account.

### PostgreSQL

Add the provider to `pom.xml`, next to the filesystem one:

```xml
<dependency>
    <groupId>io.descoped.rawdata</groupId>
    <artifactId>rawdata-client-provider-postgres</artifactId>
    <scope>runtime</scope>
</dependency>
```

Start PostgreSQL 18:

```sh
docker run -d --name rawdata-postgres -p 5432:5432 \
  -e POSTGRES_USER=rdc -e POSTGRES_PASSWORD=rdc -e POSTGRES_DB=rdc postgres:18-alpine
```

Replace `rawdata.properties` with:

```properties
rawdata.client.provider=postgres
postgres.driver.host=localhost
postgres.driver.port=5432
postgres.driver.user=rdc
postgres.driver.password=rdc
postgres.driver.database=rdc
```

Run the steps again, starting with step 3. The first client creates the provider's tables. The output is the same,
with new ids, except for step 4, where the message is visible before the producer closes:

```text
Before close: Hello, rawdata
After close:  Hello, rawdata
```

In `postgres` each `publish` call is one committed transaction, so it is visible and durable when `publish` returns.
The provider needs PostgreSQL 18; see [the postgres provider](modules/rawdata-client-provider-postgres.md) for the
keys, the tables and the privileges a role needs.

### Google Cloud Storage

Add `rawdata-client-provider-gcs` the same way. Start a fake GCS server and create a bucket:

```sh
docker run -d --name rawdata-gcs -p 4443:4443 fsouza/fake-gcs-server:1.56.1 -scheme http -port 4443 -backend memory
curl -X POST -H 'Content-Type: application/json' -d '{"name":"rawdata-tutorial"}' \
  "http://localhost:4443/storage/v1/b?project=tutorial"
```

```properties
rawdata.client.provider=gcs
gcs.bucket-name=rawdata-tutorial
gcs.endpoint=http://localhost:4443
gcs.credential-provider=none
local-temp-folder=data/buffer-gcs
```

The output is the same as for `filesystem`. On real GCS, leave out `gcs.endpoint`, and use your credentials, for
example `gcs.credential-provider=application-default` after `gcloud auth application-default login`. See
[the gcs provider](modules/rawdata-client-provider-gcs.md#credentials).

### Amazon S3

Add `rawdata-client-provider-s3` the same way. Start S3Mock and create a bucket:

```sh
docker run -d --name rawdata-s3 -p 9090:9090 adobe/s3mock:5.2.3
curl -X PUT http://localhost:9090/rawdata-tutorial
```

```properties
rawdata.client.provider=s3
s3.bucket-name=rawdata-tutorial
s3.endpoint=http://localhost:9090
s3.region=us-east-1
s3.path-style-access=true
s3.credentials=static
s3.access-key-id=tutorial
s3.secret-access-key=tutorial
local-temp-folder=data/buffer-s3
```

The output is the same as for `filesystem`. On real S3, set `s3.bucket-name` and `s3.region` and leave out the rest:
credentials then come from the AWS SDK's default chain (environment, profile, SSO, container or instance role). See
[the s3 provider](modules/rawdata-client-provider-s3.md#configuration).

The same code now runs on four stores. That is what the TCK promises, and why [the copy
tool](modules/rawdata-copy-util.md) can move a topic from one to another.

## Clean up

```sh
rm -rf data
docker rm -f rawdata-postgres rawdata-gcs rawdata-s3
```

## Where next

- [Concepts](concepts.md): every idea in depth, with a glossary.
- [The contract](contract.md): what every provider guarantees.
- The module pages: [API](modules/rawdata-client-api.md), [postgres](modules/rawdata-client-provider-postgres.md),
  [filesystem](modules/rawdata-client-provider-filesystem.md), [gcs](modules/rawdata-client-provider-gcs.md),
  [s3](modules/rawdata-client-provider-s3.md), [Avro engine](modules/rawdata-client-provider-avro.md),
  [TCK](modules/rawdata-client-tck.md), [BOM](modules/rawdata-client-bom.md),
  [copy tool](modules/rawdata-copy-util.md).
- Testing your own code: the `memory` provider needs no store; see
  [API: testing your code](modules/rawdata-client-api.md#testing-your-code).
