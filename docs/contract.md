# The contract

Every provider keeps this contract, and the shared contract tests (`RawdataClientContractTest` in
`rawdata-client-tck`) check it for each one. Code written against one provider works against another.

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

Messages are immutable, and content is copied in and out: changing an array you published, or one you read, changes
nothing in the store.

## Ids and order

Ids are UUIDv7 as PostgreSQL 18's `uuidv7()` makes them: 48 bits of milliseconds, a 12-bit fraction of the
millisecond, and random bits (`RawdataIds`). `message.timestamp()` is the millisecond of the id. Ids are ordered as
unsigned 128-bit numbers, `RawdataIds.ORDER`, the order PostgreSQL sorts `uuid` in; never compare ids with
`UUID.compareTo`, which is signed.

A topic only grows at its end, whichever producer writes, in this process or another:

- An id the producer gives is greater than every id in the topic, also within one millisecond and when the clock
  goes back.
- An id you give must be greater than every id in the topic and than the id before it in the same call; otherwise
  the publish throws `IllegalArgumentException` and writes nothing. Giving ids is for copies, which keep the ids of
  the source.
- A `publish` call is written whole or not at all, and when it returns its messages are visible to every consumer
  and durable.

So a consumer that has read up to an id never misses a message published later.

## Reading

A consumer reads one topic in id order, from a starting point, and is pulled by the caller: nothing arrives until it
asks, so it takes only as much as it can handle.

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
replaces, `remove` does nothing for a missing key. A file-based provider limits the length of a key (see its page).

## Closing

`client.close()` closes every producer, consumer and metadata client it made. After that, every method throws
`RawdataClosedException`, except `isClosed()` and `close()`, which may be called any number of times. Closing wakes a
waiting `receive`, which then throws, and fails a pending `receiveAsync` or `publishAsync` future with
`RawdataClosedException`. Closing a producer or consumer closes only that one.

## Copying

`RawdataCopy.copy(from, to, batchSize, idleTimeout)` copies from a consumer to a producer in batches, keeping ids and
order, until the source has had nothing new for the idle timeout. The [copy tool](copy-tool.md) is built on it.
