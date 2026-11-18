# Architecture

How the modules fit together, and how each provider keeps the [contract](contract.md), in particular the order of a
topic and its one writer.

## Modules and their dependencies

```
                         rawdata-client-api  ◄── rawdata-client-tck (tests; also the store TCK)
                         (API, spi, memory, discard)
                        ▲                     ▲
        rawdata-client-provider-postgres   rawdata-client-provider-avro
        (HikariCP, pgjdbc)                 (the Avro engine: AvroRawdataClient, AvroStore)
                                              ▲              ▲              ▲
                         rawdata-client-provider-filesystem  -gcs           -s3
                         (local folder)        (java.net.http, Jackson)   (AWS SDK for Java 2)

   rawdata-copy-util: the API at compile time, every provider at run time
```

- **`rawdata-client-api`** (`io.descoped.rawdata.api`) is the only module an application compiles against. It holds:
  - the interfaces: `RawdataClient`, `RawdataProducer`, `RawdataConsumer`, `RawdataMetadataClient`,
    `RawdataMessage`;
  - the rules: `RawdataIds` for UUIDv7 ids and their order, `RawdataTopics` for topic names;
  - `RawdataCopy`, and the provider service `RawdataClientInitializer`.
  
  The `memory` and `discard` providers are in it too.
- **`io.descoped.rawdata.api.spi`** (exported from the API module) has two base classes for providers:
  - `OrderedRawdataProducer`: argument checks, asynchronous publish, closing, and `withIds`, which gives ids after the
    topic's last id;
  - `PollingRawdataConsumer`: the receive loop, waking, `receiveAsync`, `seek` and closing.
  
  `postgres` and the Avro engine are built on them.
- **`rawdata-client-tck`** holds `RawdataClientContractTest`, the contract as abstract JUnit 6 tests. Each provider's
  test module extends it with its configuration.
- **The providers** depend on the API, and `filesystem`, `gcs` and `s3` also on the Avro engine, which is not a
  provider itself. No provider depends on another one's
  internals.

## Finding a provider

`RawdataClient.create(configuration)` reads `rawdata.client.provider` and asks the service provider API
(`io.descoped.spi:service-provider-api`, `ProviderConfigurator`) for the `RawdataClientInitializer` with that
`@ProviderName`. The service loader finds it:
- through `provides` in the provider's `module-info` on the module path;
- through `META-INF/services/io.descoped.rawdata.api.RawdataClientInitializer` on the class path.

Every provider registers both ways. `configurationKeys()` names the required keys, and `create` refuses a
configuration without them. `initialize` then builds the client.

## Ids and the order of a topic

An id is a UUIDv7 (`RawdataIds`), with the layout PostgreSQL 18's `uuidv7()` uses:
- 48 bits of Unix milliseconds;
- version 7;
- a 12-bit fraction of the millisecond;
- the variant;
- 62 random bits.

`RawdataIds.ORDER` compares ids as unsigned 128-bit numbers, the order PostgreSQL sorts `uuid` in. `UUID.compareTo`
is signed, and is never used for ids.

A topic only grows at its end, and only one process writes it. A provider must keep the producers of its client in
order, and must notice when another process wrote: a client remembers where it last wrote a topic (before its first
publish, where the topic ended), and a publish that finds more after that throws `RawdataWriterConflictException`
and writes nothing. The providers do it in two ways.

### `postgres`: an advisory lock per topic

`PostgresRawdataProducer` writes a publish in one transaction:
1. It takes `pg_advisory_xact_lock(TOPIC_LOCK, hashtext(topic))`, a lock per topic that PostgreSQL releases at commit
   or rollback.
2. It reads the topic's last id from `rawdata_message`. If this client wrote the topic before and the last id is not
   the last it wrote, another process wrote: the publish throws `RawdataWriterConflictException` and rolls back.
3. `withIds` gives the new messages ids after that id.
4. It inserts them into `rawdata_message` and `rawdata_content` and commits.

Producers of one client take turns per topic, so they never look like another writer to each other. Connections run
at READ COMMITTED, so the read in step 2 sees every earlier commit. The topic is always a bind parameter, never part
of the SQL.

The consumer (`PostgresRawdataConsumer`) reads in pages of `prefetch-size` after the last id it has read, and polls
when it finds nothing.

### The Avro engine: conditional create of numbered segments

`AvroRawdataClient` keeps a topic in an `AvroStore` as segments numbered 0, 1, 2… without gaps. Each segment is one
Avro file holding one publish. The store guarantees one thing: `create(topic, n, bytes)` succeeds only if segment `n`
does not exist, and readers see the whole segment or nothing.

- **filesystem:** a hard link from a temporary file.
- **gcs:** an upload with the `ifGenerationMatch=0` precondition, through GCS's JSON API on the JDK's HTTP client
  (`GcsClient`), with tokens from `GcsCredentials`.
- **s3:** a `PutObject` with `If-None-Match: *`, through the AWS SDK for Java 2.

Producers buffer in an [upload window](providers/upload-window.md) (`AvroTopicWriter`, one per topic and client):
1. A publish appends to the open window, an Avro file in `local-temp-folder`, with ids after the window's last id.
2. When the window closes (on time, on size, or when a producer closes), it is forced to disk, sealed, and queued.
3. A background upload creates it as segment `n + 1`, after the last segment `n` this client created. Before the
   client's first publish to the topic, `n` is the topic's last segment.

If segment `n + 1` exists, another process wrote the topic, and the writer fails with
`RawdataWriterConflictException`. (A create that finds its own bytes, after a retry whose first response was lost,
counts as created.) Segments are thus created one after another, and ids grow in segment order. When the writer
starts, it recovers the windows a crash left; see [recovery](providers/upload-window.md#recovery-after-a-crash).

- **Finding the last segment** (`lastSegment`): from the last one seen, probe forward in doubling steps, then halve
  the step between the last that exists and the first that does not.
- **Starting a consumer at an id** (`firstSegmentReaching`): a binary search over the segments' last ids.
- **A message is visible and durable once its window is uploaded.** Closing a producer uploads the open window and
  waits; closing the client does it for every topic. That is graceful shutdown.

## Reading: pulled, not pushed

Consumers are pulled: `receive`, `receiveUpTo` and `receiveAsync`. There is no `Flow` or reactive-streams API.
`PollingRawdataConsumer`:
- holds a buffer that the provider's `fill` refills, one page or segment at a time;
- when it finds nothing, waits for the poll interval, or until a publish through the same client wakes it;
- never holds its turn while waiting, so `close()` or `seek` are never blocked behind a waiting receive.

## Metadata

Each topic has a small key-value store (`RawdataMetadataClient`):
- **postgres:** rows in `rawdata_metadata`;
- **Avro engine:** one file or object per key, named `k` plus the hex of the key's UTF-8 bytes, so any key makes a
  safe name.

Writes replace the whole value atomically.

## Closing

A client owns everything it creates. `close()` closes its producers, consumers and metadata clients, stops its
executor (which fails pending asynchronous calls), and closes the store or the connection pool. Every method of a
closed object throws `RawdataClosedException`, except `isClosed` and `close`.

## The copy tool

`RawdataCopyTool` reads `source.*` and `target.*` configurations and opens a client on each. For each topic, it
starts a consumer after the target's last id and runs `RawdataCopy.copy`, which publishes in batches with the source's
ids. Because a target topic only grows at its end and the ids are kept, a copy that stopped can be run again.
