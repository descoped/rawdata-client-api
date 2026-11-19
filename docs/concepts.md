# Concepts

This page explains every idea in the rawdata client, from a topic to the TCK. It goes in teaching order, so read it
from the top. Each concept says what it is, why it is that way, and where to read more. The
[glossary](#glossary) at the end gives each term in one line.

For a hands-on start, follow the [tutorial](tutorial.md) first and come back here.

## Contents

- [The data model](#the-data-model): [topic](#topic), [message](#message), [id and order](#id-and-order),
  [position](#position), [ordering group and sequence number](#ordering-group-and-sequence-number)
- [Writing and reading](#writing-and-reading): [client](#client), [producer](#producer), [consumer](#consumer),
  [pull reading](#pull-reading), [cursor and seek](#cursor-and-seek), [last message](#last-message),
  [metadata](#metadata), [closing](#closing)
- [Providers and configuration](#providers-and-configuration): [provider and discovery](#provider-and-discovery),
  [configuration](#configuration), [the value rules](#the-value-rules)
- [Guarantees](#guarantees): [one writer per topic](#one-writer-per-topic),
  [visibility and durability](#visibility-and-durability)
- [Inside the Avro engine](#inside-the-avro-engine): [the upload window](#the-upload-window),
  [segments](#segments), [the segment header and ranged reads](#the-segment-header-and-ranged-reads),
  [crash recovery](#crash-recovery), [throttling and AIMD](#throttling-and-aimd), [checksums](#checksums)
- [Compatibility](#compatibility): [the TCK](#the-tck), [JPMS](#jpms), [copying](#copying)
- [Glossary](#glossary)

## The data model

### Topic

**What.** A topic is a named, ordered stream of messages, together with a small key-value store of
[metadata](#metadata). A topic exists from its first use: there is no "create topic" call, and an unknown topic reads
as empty.

A topic name is 1 to 100 characters of `A-Z a-z 0-9 _ . -`, and starts with a letter or a digit. Names are
case-sensitive: `Invoices` and `invoices` are two topics, in every provider. `RawdataTopics.isValid(name)` checks a
name, and every method that takes a topic refuses an invalid one with `IllegalArgumentException`.

**Why.** A provider puts the name into a table row, a file path or an object name. A strict rule (no `/`, no
whitespace, no `..`) means a name can never escape its place in any store, and the same name works everywhere. The
`postgres` provider keeps the topic in a column and always binds it as a parameter, never as SQL text; the
`filesystem` provider encodes upper-case letters in the folder name, so two topics never share a folder on a
case-insensitive file system.

**Read more.** [API: topic](modules/rawdata-client-api.md#topic),
[filesystem: the topic folder](modules/rawdata-client-provider-filesystem.md#the-topic-folder),
[postgres: topics are rows, not tables](modules/rawdata-client-provider-postgres.md#topics-are-rows-not-tables).

### Message

**What.** A `RawdataMessage` is one immutable record in a topic:

| Field | Set by | Meaning |
|---|---|---|
| `id()` | the producer, or you | a UUIDv7; the topic is read in the order of ids |
| `timestamp()` | derived | the millisecond in the id |
| `position()` | you, required | your own identifier of the message in its source system |
| `orderingGroup()` | you, optional | the name of a sequence that several threads write in parallel |
| `sequenceNumber()` | you, optional | the message's number in its ordering group; 0 when not used |
| `keys()`, `get(key)`, `data()` | you | named binary content, in the order the keys were put |

You build a message with `RawdataMessage.builder()`, or start from an existing one with `message.copy()`.
`RawdataMessage` is a sealed interface, so the builder is the only way to make one. Content is copied in and out:
changing an array you published, or one you read, changes nothing in the store.

The position, the ordering group and the content keys may be any string without the NUL character (U+0000). The
builder refuses NUL with `IllegalArgumentException`.

**Why.** The client stores raw data as it came from a source, so content is bytes with names, and the store never
interprets it. Immutability and copying mean a message read from the store is always the message that was written.
NUL is refused because a PostgreSQL text column cannot hold it, and every provider must store the same messages.

**Read more.** [API: message](modules/rawdata-client-api.md#message),
[API reference: `RawdataMessage`](modules/rawdata-client-api.md#rawdatamessage-and-rawdatamessagebuilder).

### Id and order

**What.** Every message id is a UUIDv7 ([RFC 9562](https://www.rfc-editor.org/rfc/rfc9562)), laid out as
PostgreSQL 18's `uuidv7()` lays it out:

```text
 48 bits  unix_ts_ms   milliseconds since 1970-01-01T00:00Z
  4 bits  version      7
 12 bits  rand_a       sub-millisecond fraction
  2 bits  variant      0b10
 62 bits  rand_b       random
```

The order of a topic is `RawdataIds.ORDER`: the 128 bits compared as an unsigned number, the order PostgreSQL sorts
`uuid` values in.

- An id the producer assigns is greater than every id in the topic. `RawdataIds.next(previous)` makes it: an id for
  now if that is greater than the previous one, otherwise the previous id plus one. So ids keep increasing also
  within one millisecond and when the clock moves back.
- An id you give is kept as it is, in any order, as in 2.x. An application that makes its ids before it publishes,
  from several threads, publishes them as they come. Ids must be unique, which is the caller's to keep.

`RawdataIds` also has `generate()`, `isValid(id)`, `timestamp(id)`, `beginningOf(millis)` (the smallest id of a
millisecond) and `beginningOfTime()`.

**Why.** An id that starts with the time sorts by time, so a topic in id order is a topic in time order, and a time
is easy to turn into a starting point. A consumer of a topic that is still written gets no guarantee of arrival: a
message whose id lands behind its position is not read by it. A completed topic, read from the beginning, gives every
message once. The layout is PostgreSQL's so the
`postgres` provider can order, index and compare ids in the database the way the API does.

**Pitfall.** Never sort ids with `UUID.compareTo`, a `TreeMap<UUID, ?>` without a comparator, or
`Comparator.naturalOrder()`. `UUID.compareTo` compares two signed `long`s and gets the order wrong whenever the top
bit of either half differs. Use `RawdataIds.ORDER`. An id from `UUID.randomUUID()` (version 4) is refused.

**Read more.** [API: id and order](modules/rawdata-client-api.md#id-and-order),
[API: how ids are assigned](modules/rawdata-client-api.md#how-ids-are-assigned),
[postgres: ids are PostgreSQL UUIDv7s](modules/rawdata-client-provider-postgres.md#ids-are-postgresql-uuidv7s).

### Position

**What.** The position is your own identifier of a message, for example the key of the record in the system it came
from: `invoice-1001`. Every message must have one (`build()` without it throws `IllegalArgumentException`). It need
not be unique and need not sort.

You can start reading at a position, but only with a time window:
`cursorOf(topic, position, inclusive, approxTimestamp, tolerance)` searches the messages written in
`[approxTimestamp - tolerance, approxTimestamp + tolerance]` and throws `RawdataNoSuchPositionException` if no message
with that position is there.

**Why.** The id belongs to the store; the position belongs to you. It lets you find "the message for invoice 1001"
without keeping the id. Because a position need not be unique, and because file and object stores cannot index it,
a search must be bounded by time. `postgres` searches an index on `(topic, position)` within the window; the Avro
providers stream the segments that cover the window.

**Read more.** [API: cursor](modules/rawdata-client-api.md#cursor),
[Avro engine: reading](modules/rawdata-client-provider-avro.md#reading).

### Ordering group and sequence number

**What.** Two optional fields for a sequence that several threads or producers write in parallel. Messages of one
sequence share an ordering group, a name, and are numbered 1, 2, 3… without gaps in `sequenceNumber`. A sequence
number of 0, the default, means the field is not used.

**Why.** When several threads publish parts of one logical sequence, the order of ids is the order the calls
arrived, which may differ from the order the source meant. The two fields carry the intended order with the messages.
Providers store them and give them back; restoring the order is up to the consumer.

**Read more.** [API: message](modules/rawdata-client-api.md#message).

## Writing and reading

### Client

**What.** A `RawdataClient` is a connection to one store: a database, a folder, a bucket, or the client's own memory.
`RawdataClient.create(configuration)` makes one. It makes producers, consumers and metadata clients for topics, finds
cursors, and reads a topic's last message.

**Why.** One object per store keeps the resources a provider needs in one place: a connection pool in `postgres`,
upload threads and buffer folders in the Avro providers. A client is meant to be shared by the whole process and
closed once, at shutdown.

**Read more.** [API: client](modules/rawdata-client-api.md#client),
[API reference: `RawdataClient`](modules/rawdata-client-api.md#rawdataclient).

### Producer

**What.** A `RawdataProducer`, from `client.producer(topic)`, publishes messages to one topic.
`publish(RawdataMessage...)` and `publish(List<RawdataMessage>)` write one call: all of it or none of it, in the order
given. `publishAsync` does the same on the client's threads and returns a `CompletableFuture<Void>`. A producer may be
used from several threads, and a client may have several producers on one topic: the client keeps them in one order.

**Why.** A call written whole means a batch that belongs together is never half written, which is what a copy or an
import needs. The client, not the producer, owns the order of a topic, so any number of producers in one process can
feed it.

**Read more.** [API: producer](modules/rawdata-client-api.md#producer),
[postgres: a publish](modules/rawdata-client-provider-postgres.md#a-publish),
[Avro engine: the life of a message](modules/rawdata-client-provider-avro.md#the-life-of-a-message).

### Consumer

**What.** A `RawdataConsumer`, from `client.consumer(...)`, reads one topic in id order from a starting point, and
keeps its own position. It is for one thread at a time, and at most one `receiveAsync` may be outstanding.

| Call | Does |
|---|---|
| `receive(timeout, unit)` | the next message, waiting up to the timeout; null if none came |
| `receiveUpTo(max, timeout)` | waits up to the timeout for the first message, then takes what is there at once, up to `max` |
| `receiveAsync()` | a future of the next message, completed on the client's threads |
| `seek(timestamp)` | moves the position to the first message whose id time is at or after the timestamp |

**Why.** A consumer is cheap and holds its own position, so each reading thread makes its own. Consumers may be in
any process: reading needs no coordination with the writer.

**Read more.** [API: consumer](modules/rawdata-client-api.md#consumer),
[API: how a receive waits](modules/rawdata-client-api.md#how-a-receive-waits).

### Pull reading

**What.** Reading is pulled by the caller. Nothing is delivered until you call `receive`, `receiveUpTo` or
`receiveAsync`, so you take only as much as you can handle. There is no reactive-streams or `Flow` API.

How a waiting receive learns of new messages depends on the provider:

- `memory` wakes on the next publish to the topic.
- `postgres` queries again every `rawdata.postgres.consumer.prefetch-poll-interval-when-empty` milliseconds (250).
  Publishes do not wake it.
- `filesystem`, `gcs` and `s3` look for the next segment every `listing.min-interval-seconds`,
  `gcs.listing.min-interval-seconds` or `s3.listing.min-interval-seconds` (1 second), and a consumer of the client
  that uploaded is woken at once.

**Why.** Pulling gives back-pressure for free: a slow consumer simply asks less often, and no buffer grows between
the store and your code. The owner decided on pull and decided against `Flow`; it will be looked at again when the JDK
offers something better.

**Read more.** [API: consumer](modules/rawdata-client-api.md#consumer),
[Architecture: reading, pulled, not pushed](architecture.md#reading-pulled-not-pushed).

### Cursor and seek

**What.** A `RawdataCursor` is a starting point for a consumer, made by the client and specific to its provider.
The `consumer(...)` overloads make one for you:

| Consumer | Starts |
|---|---|
| `consumer(topic)` | at the beginning |
| `consumer(topic, id)` | after the message with that id |
| `consumer(topic, id, true)` | at the message with that id |
| `consumer(topic, position, approxTimestamp, tolerance)` | after the first message with that position in the time window |
| `consumer(topic, position, true, approxTimestamp, tolerance)` | at that message |
| `consumer(topic, cursor)` | at a cursor from `cursorOf` |

A null id, position or cursor means the beginning. The id need not be the id of a message: reading starts at the
first message with a greater id (or an equal one, if inclusive). So `cursorOf(topic, RawdataIds.beginningOf(t), true)`
starts at the first message written at or after time `t`, and `consumer.seek(t)` moves an open consumer there.

A cursor of one provider given to another provider's client is refused with `IllegalArgumentException`.

**Why.** A starting point by id is exact and cheap in every store, and because ids carry time, a time is also an id.
Resuming after the last id you handled is the normal way to continue where you stopped.

**Read more.** [API: cursor](modules/rawdata-client-api.md#cursor),
[Avro engine: reading](modules/rawdata-client-provider-avro.md#reading),
[postgres: reading](modules/rawdata-client-provider-postgres.md#reading).

### Last message

**What.** `client.lastMessage(topic)` returns the message with the greatest id, or null if the topic is empty.

**Why.** The end of a topic is where a writer starts and where a copy resumes: the copy tool reads the target's last
message and continues after its id. It is cheap in every provider: one indexed query in `postgres`, and in the Avro
providers the header and the last block of the last segment.

**Read more.** [API reference: `RawdataClient`](modules/rawdata-client-api.md#rawdataclient),
[copy tool: resuming after the target's last id](modules/rawdata-copy-util.md#resuming-after-the-targets-last-id).

### Metadata

**What.** `client.metadata(topic)` returns a `RawdataMetadataClient`: a small key-value store (a string key, a byte
array value) that belongs to the topic and lasts as long as its messages. Every metadata client of a topic sees the
same entries, also across clients of a shared store. A typical entry is a consumer's checkpoint: the id of the last
message it handled.

| Call | Does |
|---|---|
| `keys()` | an unmodifiable snapshot of the keys |
| `get(key)` | a copy of the value, or null |
| `put(key, value)` | writes, replacing any value; returns the metadata client |
| `remove(key)` | removes; does nothing for a missing key; returns the metadata client |

A key is any string without NUL, the empty string included, and case matters. How long a key may be depends on the
store; every provider takes at least `RawdataMetadataClient.PORTABLE_KEY_BYTES`, 127 bytes of UTF-8:

| Provider | Longest key, UTF-8 bytes |
|---|---|
| `filesystem` | 127 (the key is a file name in hex) |
| `gcs`, `s3` | (1024 − the bytes of `<prefix><topic>/metadata/k`) / 2, about 500 |
| `postgres` | 2000 |
| `memory` | no limit |

A longer key is refused by `put`, `get` and `remove` with `IllegalArgumentException`. Metadata is written to the
store at once, also in the Avro providers: it does not go through the upload window.

**Why.** A checkpoint next to the data it describes moves with the topic (the copy tool copies it with `--metadata`)
and needs no second database. Keep keys within 127 bytes and they work in every provider.

**Read more.** [API: metadata](modules/rawdata-client-api.md#metadata),
[Avro engine: metadata entries](modules/rawdata-client-provider-avro.md#metadata-entries),
[postgres: metadata](modules/rawdata-client-provider-postgres.md#metadata).

### Closing

**What.** `client.close()` closes the client and every producer, consumer and metadata client it made. After that,
every method throws `RawdataClosedException`, except `isClosed()` and `close()`, which may be called any number of
times. Closing wakes a waiting `receive`, which then throws, and fails a pending `receiveAsync` or `publishAsync`
future with `RawdataClosedException`. Closing a producer or a consumer closes only that one.

In the Avro providers, closing does real work: `producer.close()` uploads its topic's open window and waits for the
topic's uploads, and `client.close()` does so for every topic. Neither waits longer than
`avro-file.close-timeout-seconds` (30).

**Why.** A client holds threads, connections and buffers. A clear end makes shutdown graceful: an application closes
the client on SIGTERM, and what it published is in the store. The client registers no shutdown hook of its own.

**Read more.** [API: closed state](modules/rawdata-client-api.md#closed-state),
[Avro engine: graceful shutdown](modules/rawdata-client-provider-avro.md#graceful-shutdown).

## Providers and configuration

### Provider and discovery

**What.** A provider is the implementation of the API for one kind of store, found by name at run time:

| Name | Module | Store |
|---|---|---|
| `memory` | `rawdata-client-api` | the client's own memory; for tests |
| `discard` | `rawdata-client-api` | none: accepts every publish and keeps nothing |
| `postgres` | `rawdata-client-provider-postgres` | PostgreSQL 18 |
| `filesystem` | `rawdata-client-provider-filesystem` | Avro files in a local folder |
| `gcs` | `rawdata-client-provider-gcs` | Avro files in a Google Cloud Storage bucket |
| `s3` | `rawdata-client-provider-s3` | Avro files in an Amazon S3 bucket |

A provider is a `RawdataClientInitializer` (a `ProviderInitializer<RawdataClient>` from
`io.descoped.spi:service-provider-api`) with a `@ProviderName` annotation. `RawdataClient.create` reads the name in
`rawdata.client.provider`, loads every initializer with the JDK's `ServiceLoader`, picks the one with that name, checks
that the configuration has its required keys, and calls it. Every provider is registered twice, with `provides` in
its `module-info.java` and in `META-INF/services/io.descoped.rawdata.api.RawdataClientInitializer`, so it is found on
the module path and on the class path.

`filesystem`, `gcs` and `s3` share one implementation, the Avro engine (`rawdata-client-provider-avro`), and differ
only in their store. The engine is not a provider itself.

**Why.** Your code depends on the API alone, and the store is a runtime dependency chosen by configuration. Moving
from a database to a bucket is a change of configuration and of one dependency, not of code.

**Pitfall.** `ProviderException: No provider named 'postgres'; available: [discard, memory]` means the provider's jar
is not on the class path or module path.

**Read more.** [API: creating a client](modules/rawdata-client-api.md#creating-a-client),
[API: the memory and discard providers](modules/rawdata-client-api.md#the-memory-and-discard-providers),
[Avro engine: the engine and the store](modules/rawdata-client-provider-avro.md#the-engine-and-the-store),
[Writing a provider](writing-a-provider.md).

### Configuration

**What.** A client is configured by one `Map<String, String>`. The API reads one key, `rawdata.client.provider`
(`RawdataClient.PROVIDER_KEY`), and passes the whole map to the provider, which reads its own keys:

| Provider | Required keys | Module page |
|---|---|---|
| `memory`, `discard` | none | [API](modules/rawdata-client-api.md#configuration) |
| `postgres` | `postgres.driver.user`, `postgres.driver.password`, `postgres.driver.database` | [postgres](modules/rawdata-client-provider-postgres.md#configuration) |
| `filesystem` | `filesystem.storage-folder`, `local-temp-folder` | [filesystem](modules/rawdata-client-provider-filesystem.md#configuration) |
| `gcs` | `gcs.bucket-name`, `local-temp-folder`, and with the default credentials `gcs.service-account.key-file` | [gcs](modules/rawdata-client-provider-gcs.md#configuration) |
| `s3` | `s3.bucket-name`, `local-temp-folder`, and a region from `s3.region` or the AWS SDK's chain | [s3](modules/rawdata-client-provider-s3.md#configuration) |

The upload window keys (`avro-file.*`) are the same in `filesystem`, `gcs` and `s3`; see
[the upload window](#the-upload-window).

**Why.** A flat map of strings comes from anywhere: a properties file, environment variables, or a configuration
library. A missing required key fails `RawdataClient.create` with an `IllegalArgumentException` that names it, so a
wrong configuration fails at start, not on the first publish.

**Read more.** The configuration section of each module page:
[postgres](modules/rawdata-client-provider-postgres.md#configuration),
[filesystem](modules/rawdata-client-provider-filesystem.md#configuration),
[gcs](modules/rawdata-client-provider-gcs.md#configuration), [s3](modules/rawdata-client-provider-s3.md#configuration),
[Avro engine](modules/rawdata-client-provider-avro.md#configuration).

### The value rules

**What.** The Avro engine, and so `filesystem`, `gcs` and `s3`, reads values as the
[dynamic-configuration](https://github.com/descoped/dynamic-configuration) library reads them:

- **Numbers** are strict: `60`, not `60s` and not ` 60`. A blank number is an error.
- **A default covers a missing key only**, never a malformed or blank value.
- **Strings** are trimmed. A blank optional string takes its default; a blank required string counts as missing.
- **Enums** (`gcs.credential-provider`) are case-insensitive, trimmed, and read `-` as `_`.
- **Every error names its key**, as an `IllegalArgumentException`.

Two providers differ in places. `s3` matches the values of `s3.credentials`, `s3.path-style-access` and `s3.sse`
exactly and case-sensitively. `postgres` trims its numbers and gives a blank number its default.

**Why.** Strict values catch a typo at start instead of silently running with a default. Following
dynamic-configuration means an application can pass `config.asMap()` or a `config.subset(..)` straight to
`RawdataClient.create`.

**Read more.** [gcs: how values are read](modules/rawdata-client-provider-gcs.md#how-values-are-read),
[Avro engine: configuration](modules/rawdata-client-provider-avro.md#configuration).

## Guarantees

### One writer per topic

**What.** Only one process writes a topic. In that process any number of producers of its client may publish;
consumers may be anywhere. If a second process publishes anyway, the provider notices: a client remembers where it
last wrote a topic (before its first publish, where the topic ended), and a publish that finds more after that point
throws `RawdataWriterConflictException`, which names the topic (`topic()`). The client then keeps failing for that
topic, so two writers never merge into one stream. A new client, created after the other writer stopped, starts from
the topic's current end.

How each provider sees the other writer:

| Provider | Check | When it shows |
|---|---|---|
| `postgres` | under the topic's advisory lock, the topic's last id is not the one this client wrote | that `publish`, which writes nothing |
| `filesystem`, `gcs`, `s3` | the next segment number is already taken when a window opens or uploads | that `publish`, or a later `publish`, `flush` or `close`; the windows not uploaded are kept as `*.invalid` files |
| `memory`, `discard` | none: nothing a second process could see | never |

The create of a segment is atomic in each store: a hard link in `filesystem`, an upload with `ifGenerationMatch=0` in
`gcs`, a put with `If-None-Match: *` in `s3`. A create that finds its own bytes, after a lost response, counts as
created, so a retry is never mistaken for another writer.

**Why.** A single writer gives one total order with no coordination between processes, which is what lets a consumer
read "everything after id X" and never miss or repeat a message. Supporting several writing processes would need a
distributed order that the stores cannot give cheaply. The owner chose one writer, and nothing supports or tests
several writing processes. The check exists so that a misconfiguration (an old instance still running, two services
on one topic) fails loudly instead of corrupting the order.

**Read more.** [The contract: one writer per topic](contract.md#one-writer-per-topic),
[postgres: the one-writer check](modules/rawdata-client-provider-postgres.md#the-one-writer-check),
[Avro engine: one writer per topic](modules/rawdata-client-provider-avro.md#one-writer-per-topic),
[TCK: the second-writer test](modules/rawdata-client-tck.md#the-second-writer-test).

### Visibility and durability

**What.** When `publish` returns, the messages are not necessarily visible to consumers or safe in the store yet.
When they are depends on the provider:

| Provider | Visible to consumers and durable |
|---|---|
| `memory` | when `publish` returns, in this client only; gone at `close` |
| `postgres` | when `publish` returns: the call is one committed transaction |
| `filesystem`, `gcs`, `s3` | when the topic's upload window is uploaded as a segment |
| `discard` | never |

In every provider, closing a producer makes what it published visible and durable. In the Avro providers,
`AvroRawdataClient.flush(topic)` does the same without closing anything.

**Why.** `postgres` is a database and commits each call. File and object stores are cheap for large files and
expensive for many small ones, so the Avro providers buffer and upload in windows, trading latency for throughput
and cost. See [the upload window](#the-upload-window).

**Pitfall.** A test that publishes to `filesystem`, `gcs` or `s3` and reads at once sees nothing until the window
closes (60 seconds by default). Close the producer first.

**Read more.** [API: when a message is visible and
durable](modules/rawdata-client-api.md#when-a-message-is-visible-and-durable),
[Avro engine: visibility and durability](modules/rawdata-client-provider-avro.md#visibility-and-durability),
[postgres: writes are visible when `publish`
returns](modules/rawdata-client-provider-postgres.md#writes-are-visible-when-publish-returns).

## Inside the Avro engine

`filesystem`, `gcs` and `s3` run one engine, `AvroRawdataClient`, on a store of their own (`AvroStore`). This part
explains how the engine writes and reads. You need it to tune and operate these providers; an application that only
publishes and consumes can skip it.

### The upload window

**What.** A producer does not upload each `publish`. It appends the call to the topic's open window, an Avro file in
`local-temp-folder`, and the window is uploaded as the topic's next segment when it closes. All producers of a topic in
one client share the topic's window. A window closes when:

- it is `avro-file.max.seconds` old (60): a timer closes it, so a producer that goes quiet still uploads;
- it is at least `avro-file.max.bytes` (64 MiB) after a call;
- a producer of the topic closes, the topic is flushed, or the client closes.

A closed window is forced to disk, sealed and uploaded in the background, one window at a time per topic, while
publishes go on in a new window. If `avro-file.max.pending-uploads` (4) sealed windows of the topic are already
waiting, the publish that seals the next one waits too.

A window file's name shows its state, in `local-temp-folder/<topic folder>/`:

| File | State |
|---|---|
| `<segment>.window.avro` | open: publishes append to it |
| `<segment>.sealed.avro` | sealed: closed, forced to disk, waiting for or in its upload |
| `*.invalid` | set aside after a writer conflict; never uploaded, never deleted |
| `.lock` | the writer's lock: one client per buffer folder and topic |

Inside a window, messages are written in Avro blocks of about `avro-file.sync.interval` bytes (512 KiB). A block ends
between two calls and is then forced to disk, so a block always holds whole calls.

**Why.** An object per publish would cost a request each, make consumers read millions of small objects, and fill a
bucket with them. A window turns a stream of small publishes into a few large files, which are cheap to store and fast
to read. These are the 2.x keys and the 2.x design, so a deployment tunes it as before.

**Pitfalls.**

- `local-temp-folder` is required, also for a client that only reads.
- Each running upload holds its window in heap: up to `avro-file.upload.max-concurrency` × `avro-file.max.bytes`,
  1 GiB at the defaults.
- `avro-file.close-timeout-seconds` (30) equals Kubernetes' default `terminationGracePeriodSeconds`; lower the one or
  raise the other.
- Two clients on one buffer folder and topic, even in one JVM, fail with "The buffer folder … is in use by another
  client". Give each writing process its own folder.

**Read more.** [Avro engine: upload window](modules/rawdata-client-provider-avro.md#upload-window),
[Avro engine: configuration](modules/rawdata-client-provider-avro.md#configuration),
[filesystem: the buffer folder and its lock
file](modules/rawdata-client-provider-filesystem.md#the-buffer-folder-and-its-lock-file).

### Segments

**What.** In the store, a topic is a sequence of segments numbered 0, 1, 2… without gaps. Each segment is one Avro
file holding the messages of one upload window, in id order. A segment is created once, only if its number is free,
and never changed or removed. Next to the segments, each metadata entry is one file or object named `k` plus the
lowercase hex of the key's UTF-8 bytes.

| Provider | Segment | Created with |
|---|---|---|
| `filesystem` | `<storage folder>/<topic folder>/segments/<19 digits>.avro` | a temporary file, then a hard link that fails if the name exists |
| `gcs` | `<prefix><topic>/segments/<19 digits>.avro` | an upload with the precondition `ifGenerationMatch=0` |
| `s3` | `<prefix><topic>/segments/<19 digits>.avro` | a `PutObject` with `If-None-Match: *` |

The engine finds the end of a topic, its tail, by probing `exists` forward from the last segment it saw in doubling
steps, then bisecting.

**Why.** "Create segment n unless it exists" is the one atomic operation every store offers. From it alone the engine
gets a total order, a cheap way to find the end, and the detection of a second writer: a taken number means somebody
else wrote. Gapless numbers make the tail easy to find without listing a bucket.

**Pitfall.** Never edit, rename or delete segments while a topic is in use. A gap breaks the numbering, and a
replaced segment makes open readers fail.

**Read more.** [Avro engine: segment](modules/rawdata-client-provider-avro.md#segment),
[filesystem: writing a
segment](modules/rawdata-client-provider-filesystem.md#writing-a-segment-a-temporary-file-and-a-hard-link),
[gcs: generations and preconditions](modules/rawdata-client-provider-gcs.md#generations-and-preconditions),
[s3: the conditional put](modules/rawdata-client-provider-s3.md#the-conditional-put-how-one-writer-is-guaranteed).

### The segment header and ranged reads

**What.** A segment is an uncompressed Avro container file of records in the 2.x `RawdataMessage` schema (`id`
`fixed(16)` named `uuid`, `orderingGroup`, `sequenceNumber`, `position`, `data` a map of bytes). Its header carries a
summary: `rawdata.first-id` (the segment's lowest id), `rawdata.last-id` (the topic's greatest id up to and with the
segment), `rawdata.count` and `rawdata.last-block` (the byte offset of the last block), filled in when the window is sealed, and `rawdata.segment` and `rawdata.previous-id` for recovery.

Readers use the summary so they never read a whole segment to find something:

| Operation | Reads |
|---|---|
| a consumer | the segment in 4 MiB ranges, buffering at most 1000 messages |
| `consumer(topic, id)`, `seek(timestamp)` | a binary search over the segments' last ids, one header (an 8 KiB range) per step |
| `lastMessage(topic)` | the last segment's header and its last block |

Every read is pinned to the object it opened: the GCS generation, the S3 ETag, or the file's key, size and
modification time. A read returns that object's bytes or fails, never a mix of two objects.

**Why.** Segments can be large (64 MiB by default), and a consumer or a seek should cost a few small requests, not a
download. Pinning protects a reader from a segment replaced by hand while it reads.

**Read more.** [Avro engine: header summary](modules/rawdata-client-provider-avro.md#header-summary),
[Avro engine: the segment's Avro file](modules/rawdata-client-provider-avro.md#the-segments-avro-file),
[Avro engine: pinned segment](modules/rawdata-client-provider-avro.md#pinned-segment).

### Crash recovery

**What.** A crash of the writing process loses at most the calls in the block being filled, about
`avro-file.sync.interval` bytes, because every block is forced to disk when it ends. When a writer of the topic starts
again on the same `local-temp-folder`, its first publish looks at what the crash left, in segment order:

- a sealed window is uploaded unchanged, or removed if the store already has it with the same bytes;
- an open window is cut back to its complete blocks, then uploaded;
- an empty window is removed;
- a window is uploaded only where it belongs, as the segment right after the tail and after the id it was written
  after. One that no longer belongs, because another writer went on, is set aside as `*.invalid` with every later
  one, and the start fails once with `RawdataWriterConflictException`.

**Why.** Consumers must never see a gap, a duplicate or messages out of order. Recovering in place, with the header
fields `rawdata.segment` and `rawdata.previous-id` saying where a window belongs, keeps that promise after a crash.

**Pitfall.** A lost disk (an ephemeral container volume) loses everything not yet uploaded. Put `local-temp-folder`
on storage that outlives the process if that matters.

**Read more.** [Avro engine: recovery after a crash](modules/rawdata-client-provider-avro.md#recovery-after-a-crash).

### Throttling and AIMD

**What.** Uploads of one topic always run one at a time, in order. Across its topics, a client runs several uploads
at once, and adapts how many with additive increase, multiplicative decrease (AIMD):

- it starts at 4, or at `avro-file.upload.max-concurrency` (16) if that is lower;
- it adds one after a round of as many successful uploads at the limit as the limit, up to
  `avro-file.upload.max-concurrency`;
- it halves when a segment upload is throttled or times out, at most once a second, and never goes below 1.

A store reports throttling through `AvroStore.onThrottled`: `gcs` on a 429 or 503 or a timeout of a segment upload
attempt, `s3` on a 503 or 429 or a timeout of a conditional segment put. Reads and metadata writes are not reported.

**Why.** Buckets limit request rates, and the right number of concurrent uploads depends on the bucket and the
network. AIMD finds it without configuration: it backs off quickly when the store pushes back and grows slowly when
it does not. Only segment uploads count, because a busy read or a busy metadata entry says nothing about upload
capacity.

**Read more.**
[Avro engine: upload concurrency (AIMD)](modules/rawdata-client-provider-avro.md#upload-concurrency-aimd),
[gcs: throttling report](modules/rawdata-client-provider-gcs.md#throttling-report),
[s3: throttling feedback](modules/rawdata-client-provider-s3.md#throttling-feedback).

### Checksums

**What.** Every segment upload to GCS and S3 carries the CRC32C of its bytes (`X-Goog-Hash: crc32c=…` on GCS,
`x-amz-checksum-crc32c` on S3), and the service refuses bytes that changed on the way; the retry sends them again.
After a failed precondition, `gcs` and `s3` compare the stored object's size and CRC32C with their own bytes, and only
if both match compare the bytes themselves: equal bytes mean the client's own upload succeeded and its response was
lost.

**Why.** A damaged segment would be a permanent error in an append-only topic, so it is stopped at the door. The
cheap checksum comparison avoids downloading a segment to tell a lost response from another writer.

**Read more.** [Avro engine: checksum](modules/rawdata-client-provider-avro.md#checksum),
[gcs: the CRC32C upload hash](modules/rawdata-client-provider-gcs.md#the-crc32c-upload-hash),
[s3: telling a lost response from another
writer](modules/rawdata-client-provider-s3.md#telling-a-lost-response-from-another-writer).

## Compatibility

### The TCK

**What.** The TCK (technology compatibility kit, `rawdata-client-tck`) is the provider contract written as abstract
JUnit 6 tests. `RawdataClientContractTest` checks everything an application can see through the API;
`AvroClientContractTest` adds the upload window; `AvroStoreContractTest` checks an `AvroStore` directly. A provider's
test class extends one, describes the provider through hooks (`configuration()`, `clientsShareTheStore()`,
`metadataKeyLimit(topic)`, `missingStoreConfiguration()`, `awaitPublished(client, topic)`), and runs every inherited
test.

A provider is compliant only if it passes the TCK unchanged: no skipped, overridden or edited test. Hooks describe a
provider; they never relax a behaviour. Two rules follow:

- A behaviour test goes in the TCK, never in one provider's tests, which only check internals: SQL, files and
  objects, configuration, credentials.
- Backend limits are declared, not failed: a provider declares its longest metadata key, which must be at least 127
  bytes.

**Why.** Providers can be swapped only if they behave the same. One set of tests, run against every provider, is the
proof, and the reason [code tested against one provider holds for another](tutorial.md#step-12-switch-the-store).

**Read more.** [TCK](modules/rawdata-client-tck.md),
[TCK: compliance means passing unchanged](modules/rawdata-client-tck.md#compliance-means-passing-unchanged),
[Building and testing: the contract tests](building-and-testing.md#the-contract-tests-tck).

### JPMS

**What.** Every module is a named JPMS module:

| Module | JPMS name |
|---|---|
| `rawdata-client-api` | `io.descoped.rawdata.api` |
| `rawdata-client-tck` | `io.descoped.rawdata.tck` (open) |
| `rawdata-client-provider-postgres` | `io.descoped.rawdata.provider.postgres` |
| `rawdata-client-provider-avro` | `io.descoped.rawdata.provider.avro` |
| `rawdata-client-provider-filesystem` | `io.descoped.rawdata.provider.filesystem` |
| `rawdata-client-provider-gcs` | `io.descoped.rawdata.provider.gcs` |
| `rawdata-client-provider-s3` | `io.descoped.rawdata.provider.s3` |
| `rawdata-copy-util` | `io.descoped.rawdata.copy` |

An application module needs only `requires io.descoped.rawdata.api;`. The API declares
`uses RawdataClientInitializer`, so provider modules that no module requires are still resolved and found. Everything
on the class path works too. CI's JPMS smoke test (`src/it/jpms/run.sh`) checks both layouts.

**Why.** Applications choose their own layout. With both `provides` and `META-INF/services`, a provider works either
way. A module whose exported API shows another module's types `requires transitive` it, so an application's
`requires` list stays short: the API brings `io.descoped.service.provider.api`, for its `ProviderException`.

**Pitfall.** The provider modules export nothing. To call `AvroRawdataClient.flush` on the module path, add
`requires io.descoped.rawdata.provider.avro;`.

**Read more.** [Logging and the module path](logging-and-the-module-path.md#the-module-path),
[Building and testing: the JPMS smoke test](building-and-testing.md#the-jpms-smoke-test).

### Copying

**What.** `RawdataCopy.copy(from, to, batchSize, idleTimeout)` copies from a consumer to a producer in batches,
keeping ids and order, until the source has had nothing new for the idle timeout. The copy tool, `rawdata-copy`, is a
command-line program on top of it that copies topics between any two providers and resumes after the target's last
id.

**Why.** Because every provider keeps the same contract and ids are kept, a copy is the way to move a system from one
store or cloud to another. Resuming after the target's last id makes a copy safe to run again, as long as nothing
else writes the target topic.

**Read more.** [API: copying](modules/rawdata-client-api.md#copying), [The copy tool](modules/rawdata-copy-util.md).

## Glossary

| Term | Meaning | More |
|---|---|---|
| AIMD | additive increase, multiplicative decrease: how a client adapts its concurrent uploads | [Throttling and AIMD](#throttling-and-aimd) |
| `AvroRawdataClient` | the Avro engine's client, shared by `filesystem`, `gcs` and `s3` | [Avro engine](modules/rawdata-client-provider-avro.md) |
| `AvroStore` | the backend interface a provider on the Avro engine implements | [Avro engine: the store](modules/rawdata-client-provider-avro.md#the-store-avrostore) |
| block | an Avro block in a window, forced to disk when it ends; a crash loses at most one | [The upload window](#the-upload-window) |
| BOM | `rawdata-client-bom`, which pins the version of every rawdata library module | [BOM](modules/rawdata-client-bom.md) |
| checkpoint | a metadata entry holding the id of the last message a consumer handled | [Metadata](#metadata) |
| client | `RawdataClient`, a connection to one store | [Client](#client) |
| consumer | `RawdataConsumer`, reads one topic in id order | [Consumer](#consumer) |
| CRC32C | the checksum every segment upload to GCS and S3 carries | [Checksums](#checksums) |
| cursor | `RawdataCursor`, a provider's starting point for a consumer | [Cursor and seek](#cursor-and-seek) |
| `discard` | the provider that accepts every publish and keeps nothing | [Provider and discovery](#provider-and-discovery) |
| ETag / generation pinning | reads of a segment bound to the object found when it was opened | [The segment header and ranged reads](#the-segment-header-and-ranged-reads) |
| header summary | first id, last id, count and last block offset in a segment's Avro header | [The segment header and ranged reads](#the-segment-header-and-ranged-reads) |
| id | a message's UUIDv7, which orders the topic | [Id and order](#id-and-order) |
| last message | `lastMessage(topic)`, the message with the greatest id | [Last message](#last-message) |
| `local-temp-folder` | the local folder where Avro producers buffer their windows | [The upload window](#the-upload-window) |
| `memory` | the provider that keeps topics in the client's own memory, for tests | [Provider and discovery](#provider-and-discovery) |
| message | `RawdataMessage`, an immutable record of id, position and named bytes | [Message](#message) |
| metadata | a topic's small key-value store | [Metadata](#metadata) |
| ordering group | the name of a sequence written by several threads | [Ordering group and sequence number](#ordering-group-and-sequence-number) |
| `ORDER` | `RawdataIds.ORDER`, the unsigned order of ids | [Id and order](#id-and-order) |
| position | your own identifier of a message | [Position](#position) |
| `PORTABLE_KEY_BYTES` | 127, the metadata key length every provider takes | [Metadata](#metadata) |
| producer | `RawdataProducer`, publishes to one topic | [Producer](#producer) |
| provider | the implementation of the API for one kind of store, chosen by name | [Provider and discovery](#provider-and-discovery) |
| pull | reading only when the caller asks | [Pull reading](#pull-reading) |
| `rawdata.client.provider` | the configuration key that names the provider | [Configuration](#configuration) |
| `RawdataWriterConflictException` | thrown when another process wrote a topic this client writes | [One writer per topic](#one-writer-per-topic) |
| sealed window | a closed window, forced to disk, waiting for its upload | [The upload window](#the-upload-window) |
| seek | `consumer.seek(timestamp)`, moves a consumer to a time | [Cursor and seek](#cursor-and-seek) |
| segment | one uploaded window, a numbered Avro file in the store | [Segments](#segments) |
| sequence number | a message's number in its ordering group, from 1 | [Ordering group and sequence number](#ordering-group-and-sequence-number) |
| tail | a topic's last segment and its last id | [Segments](#segments) |
| TCK | the provider contract as abstract JUnit 6 tests | [The TCK](#the-tck) |
| topic | a named, ordered stream of messages with its metadata | [Topic](#topic) |
| upload window | the local file a topic's producers append to before it is uploaded | [The upload window](#the-upload-window) |
| UUIDv7 | a time-ordered UUID, laid out as PostgreSQL 18's `uuidv7()` | [Id and order](#id-and-order) |
| visibility | when a published message can be read; depends on the provider | [Visibility and durability](#visibility-and-durability) |
