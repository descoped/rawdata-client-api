# Architecture

How the modules fit together, and how each provider keeps the [contract](contract.md), in particular the order of a
topic across processes.

## Modules and their dependencies

```
                         rawdata-client-api  ◄── rawdata-client-tck (tests)
                         (API, spi, memory, discard)
                        ▲          ▲           ▲
        rawdata-client-provider-postgres   rawdata-client-provider-avro
        (HikariCP, pgjdbc)                 (Avro engine, filesystem)
                                                   ▲
                                     rawdata-client-provider-gcs
                                     (google-cloud-storage)

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
- **The providers** depend on the API, and `gcs` also on the Avro engine. No provider depends on another one's
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

A topic only grows at its end. This is the hard part of every provider: two writers, in two processes, must never
both write after the same last id. Otherwise a consumer that has read up to an id could miss a message committed
later with a smaller one. The providers solve it in two ways.

### `postgres`: an advisory lock per topic

`PostgresRawdataProducer` writes a publish in one transaction:
1. It takes `pg_advisory_xact_lock(TOPIC_LOCK, hashtext(topic))`, a lock per topic that PostgreSQL releases at commit
   or rollback.
2. It reads the topic's last id from `rawdata_message`.
3. `withIds` gives the new messages ids after that id.
4. It inserts them into `rawdata_message` and `rawdata_content` and commits.

Writers of a topic therefore commit one at a time, in id order. Connections run at READ COMMITTED, so the read in
step 2 sees every earlier commit. The topic is always a bind parameter, never part of the SQL.

The consumer (`PostgresRawdataConsumer`) reads in pages of `prefetch-size` after the last id it has read, and polls
when it finds nothing.

### The Avro engine: conditional create of numbered segments

`AvroRawdataClient` keeps a topic in an `AvroStore` as segments numbered 0, 1, 2… without gaps. Each segment is one
Avro file holding one publish. The store guarantees one thing: `create(topic, n, bytes)` succeeds only if segment `n`
does not exist, and readers see the whole segment or nothing.

- **filesystem:** a hard link from a temporary file.
- **gcs:** an upload with the `ifGenerationMatch=0` precondition.

A publish:
1. finds the last segment `n` and its last id;
2. gives ids after it;
3. creates segment `n + 1`.

If another writer created `n + 1` first, the create fails. The publish then reads the new last segment and tries
again with fresh ids. Segments are thus created one after another, and ids grow in segment order.

- **Finding the last segment** (`lastSegment`): from the last one seen, probe forward in doubling steps, then halve
  the step between the last that exists and the first that does not.
- **Starting a consumer at an id** (`firstSegmentReaching`): a binary search over the segments' last ids.
- **A message is visible and durable when `publish` returns.** There is no local buffer and no upload thread.

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
