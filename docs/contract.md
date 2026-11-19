# The contract

Every provider keeps this contract, and the shared contract tests (`RawdataClientContractTest` in `rawdata-client-tck`)
check it for each one: a provider is compliant when it passes them unchanged. Code written against one provider works
against another, and a system can move from one provider to another with the [copy tool](modules/rawdata-copy-util.md).
A few limits follow the store and are on each provider's page: how long a metadata key may be, and what a store accepts
at most.

## One writer per topic

**Only one process writes a topic.** In that process, any number of producers of its client may publish to it; the
client keeps them in order. Consumers may be in any process.

If another process publishes to the topic anyway (a misconfiguration, or a new writer started while the old one
still runs), the writer that finds the topic moved under it fails with `RawdataWriterConflictException`, which names
the topic. The client keeps failing to publish to that topic, so two writers never merge into one stream. The other
writer goes on; a client created after the other writer stopped starts from the topic's current end. Every provider
detects it the same way: a client remembers where it last wrote a topic (or where the topic ended when it started
writing), and finds more after that.

When the failure shows depends on when the provider writes:
- `postgres` writes in `publish`, so that `publish` throws and writes nothing.
- `filesystem`, `gcs` and `s3` buffer in an [upload window](modules/rawdata-client-provider-avro.md#upload-window). The
  conflict is found when a window opens or uploads, so it is thrown by that `publish`, or by a later `publish`, `flush`
  or `close`. Messages already accepted into windows that were not uploaded are never written to the topic; they are
  kept on disk as `*.invalid` files.

## Topics

A topic is an ordered stream of messages with a set of metadata entries. It exists from its first use; an unknown
topic is empty.

A topic name is 1 to 100 characters: letters `A-Z a-z`, digits, `_`, `.` and `-`, starting with a letter or digit
(`RawdataTopics`). Every method that takes a topic refuses another name with `IllegalArgumentException`. Names are
case-sensitive: `Invoices` and `invoices` are two topics, in every provider.

## Messages

A message has:

- `id()`: a UUIDv7, given by the producer when it publishes, or by you (see below).
- `position()`: your own identifier of the message, for example its key in the source system. Required; it need not
  be unique or sortable.
- `orderingGroup()` and `sequenceNumber()`: optional, for producers that write one sequence from several threads.
- content: named byte arrays, `put(name, bytes)` / `get(name)`, in the order they were put.

The position, the ordering group and the content names may be any string without the NUL character (U+0000), which
a PostgreSQL text column cannot hold; the builder refuses NUL with `IllegalArgumentException`, so every provider
stores the same messages.

Messages are immutable, and content is copied in and out: changing an array you published, or one you read, changes
nothing in the store.

## Ids and order

Ids are UUIDv7 as PostgreSQL 18's `uuidv7()` makes them: 48 bits of milliseconds, a 12-bit fraction of the
millisecond, and random bits (`RawdataIds`). `message.timestamp()` is the millisecond of the id. Ids are ordered as
unsigned 128-bit numbers, `RawdataIds.ORDER`, the order PostgreSQL sorts `uuid` in; never compare ids with
`UUID.compareTo`, which is signed.

Ids, as in 2.x:

- An id the producer gives is greater than every id in the topic, also within one millisecond and when the clock
  goes back.
- An id you give is kept as it is, in any order: it may be lower than ids already in the topic, and lower than the
  one before it in the same call. Ids must be unique; keeping them so is the caller's job (`memory` replaces a
  message with the same id, `postgres` refuses it, the Avro providers store both).
- A topic whose ids were published in increasing order is read in that order. A lower id published later is read
  where the provider puts it: in id order in `memory` and `postgres`, after what was published before it in
  `filesystem`, `gcs` and `s3`.
- A `publish` call is written whole or not at all.
- **When published messages are visible and durable depends on the provider.** In `postgres` and `memory`, as soon as
  `publish` returns. In `filesystem`, `gcs` and `s3`, once their [upload
  window](modules/rawdata-client-provider-avro.md#upload-window) is uploaded: after `avro-file.max.seconds` or
  `avro-file.max.bytes`, or when the producer or client closes. Closing a producer always makes what it published
  visible and durable, in every provider.

A consumer of a topic that is still written gets no guarantee of arrival: it may miss a message whose id lands
behind its position. A consumer of a completed topic, read from the beginning, gets every message once.
`lastMessage` is the last message such a reader gets.

## Reading

A consumer reads one topic from a starting point, in the order given under [Ids and order](#ids-and-order), and is
pulled by the caller: nothing arrives until it asks, so it takes only as much as it can handle.

| Call | Does |
|---|---|
| `receive(timeout, unit)` | the next message, waiting up to the timeout; null if none came |
| `receiveUpTo(max, timeout)` | up to `max` messages: waits up to the timeout for the first, then takes what is there |
| `receiveAsync()` | a future of the next message, on the client's threads |
| `seek(timestamp)` | moves to the first message written at or after a time |

Starting points:

| Consumer | Starts at |
|---|---|
| `client.consumer(topic)` | the beginning |
| `client.consumer(topic, id)` | after the message with that id; `consumer(topic, id, true)` at it |
| `client.consumer(topic, position, approxTimestamp, tolerance)` | after the first message with that position written within `approxTimestamp ± tolerance` (both ends included); `RawdataNoSuchPositionException` if there is none |

`client.lastMessage(topic)` returns the newest message, or null for an empty topic.

A consumer is for one thread at a time. A client can be shared.

## Metadata

`client.metadata(topic)` reads and writes the topic's entries: a key and bytes, for example a checkpoint. Every
metadata client of a topic sees the same entries. `keys()` is a sorted snapshot, `get` returns a copy or null, `put`
replaces, `remove` does nothing for a missing key.

A key is any string without NUL, the empty string, `..` and `/` included, and case matters. Every provider takes
keys of up to 127 bytes in UTF-8 (`RawdataMetadataClient.PORTABLE_KEY_BYTES`); some take longer ones:

| Provider | Longest key, UTF-8 bytes |
|---|---|
| `filesystem` | 127 |
| `gcs`, `s3` | (1024 − the prefix, topic and `/metadata/k`) / 2, about 500 |
| `postgres` | 2000 |
| `memory` | no limit |

A longer key, or one with NUL, is refused by `put`, `get` and `remove` with `IllegalArgumentException`.

## A store that does not exist

A client on a store that does not exist (a missing bucket or database, a folder that cannot be created) fails with
`RawdataException`, when it is created or on its first use. It never reports an empty topic.

## Closing

`client.close()` closes every producer, consumer and metadata client it made. After that, every method throws
`RawdataClosedException`, except `isClosed()` and `close()`, which may be called any number of times. Closing wakes a
waiting `receive`, which then throws, and fails a pending `receiveAsync` or `publishAsync` future with
`RawdataClosedException`. Closing a producer or consumer closes only that one.

## Copying

`RawdataCopy.copy(from, to, batchSize, idleTimeout)` copies from a consumer to a producer in batches, keeping ids and
order, until the source has had nothing new for the idle timeout. The [copy tool](modules/rawdata-copy-util.md) is built
on it.
