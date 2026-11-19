# rawdata-client-provider-avro

`rawdata-client-provider-avro` is the Avro engine: one `RawdataClient`, `AvroRawdataClient`, that keeps topics as
large Avro files in a folder or a bucket. The [`filesystem`](rawdata-client-provider-filesystem.md),
[`gcs`](rawdata-client-provider-gcs.md) and [`s3`](rawdata-client-provider-s3.md) providers are this engine on three
different stores; each of them only implements `AvroStore`, the small interface the engine writes to.

The engine is not a provider: it has no provider name and `RawdataClient.create` never returns it on its own. You
meet it in two ways. As an application developer, its upload window decides when your messages become visible,
how much heap and disk the client uses and how it shuts down, so read this page if you use `filesystem`, `gcs` or
`s3`. As a provider author, you build on it when your store keeps files or objects.

## Coordinates

| | |
|---|---|
| groupId | `io.descoped.rawdata` |
| artifactId | `rawdata-client-provider-avro` |
| version | managed by [`rawdata-client-bom`](rawdata-client-bom.md) |
| JPMS module | `io.descoped.rawdata.provider.avro` |
| exports | `io.descoped.rawdata.provider.avro` |
| requires | `io.descoped.rawdata.api` (transitive), `org.apache.avro` (an automatic module) |

You rarely declare it yourself: `rawdata-client-provider-filesystem`, `-gcs` and `-s3` bring it. Declare it with
compile scope only when your code uses its types, for example `AvroRawdataClient.flush` in a test, or `AvroStore`
in a provider of your own.

```xml
<dependency>
    <groupId>io.descoped.rawdata</groupId>
    <artifactId>rawdata-client-provider-avro</artifactId>
</dependency>
```

It brings Apache Avro, and declares slf4j-api, Jackson (core and databind) and commons-codec itself at the newest
version, excluded from Avro, so that each resolves to one version (see [dependencies](../dependencies.md)).

## Quick start

### Through a provider

The smallest working setup is the `filesystem` provider: two folders on a local disk. `gcs` and `s3` behave the same;
only the store keys differ.

```java
Map<String, String> configuration = Map.of(
        RawdataClient.PROVIDER_KEY, "filesystem",              // "rawdata.client.provider"
        "filesystem.storage-folder", "/var/lib/rawdata/store", // the segments: the provider's "bucket"
        "local-temp-folder", "/var/lib/rawdata/buffer",        // the upload windows: a local disk
        "avro-file.max.seconds", "10");                        // upload at least every 10 seconds

try (RawdataClient client = RawdataClient.create(configuration)) {
    try (RawdataProducer producer = client.producer("invoices")) {
        producer.publish(
                RawdataMessage.builder().position("invoice-1001").put("document", invoice1001).build(),
                RawdataMessage.builder().position("invoice-1002").put("document", invoice1002).build());
    } // close() uploads the open window and waits: the two messages are now segment 0, visible to every reader

    try (RawdataConsumer consumer = client.consumer("invoices")) {
        for (RawdataMessage message : consumer.receiveUpTo(100, Duration.ofSeconds(5))) {
            System.out.println(message.id() + " " + message.position());
        }
    }
}
```

The one thing to take from it: **`publish` returning does not make a message visible.** It is in a local upload
window until the window is uploaded, which happens after `avro-file.max.seconds`, when the window reaches
`avro-file.max.bytes`, or when a producer of the topic or the client closes. [How it works](#how-it-works) explains
why.

To make published messages visible without closing anything, for example in a test, flush the topic:

```java
((AvroRawdataClient) client).flush("invoices"); // closes the open window and waits for its upload
```

### On a store of your own

A provider on the engine implements `AvroStore` and hands it to the client, with the window read from the
configuration:

```java
@Override
public RawdataClient initialize(Map<String, String> configuration) {
    String bucket = AvroConfiguration.required(configuration, "mystore.bucket");
    long pollInterval = AvroConfiguration.pollIntervalMillis(configuration, "mystore.listing.min-interval-seconds", 1);
    AvroUploadWindow window = AvroUploadWindow.of(configuration); // local-temp-folder and the avro-file.* keys
    return new AvroRawdataClient(new MyAvroStore(bucket), window, pollInterval);
}
```

The client owns the store and closes it. What a store must do is under [the store](#the-store-avrostore); the
whole procedure, with the contract tests, is in [writing a provider](../writing-a-provider.md).

## Concepts

### The engine and the store

The engine is everything a provider on files or objects needs: ids and order, the upload window, segments, crash
recovery, the one-writer check, consumers, seeks and metadata. The store is the only part that knows the backend.
It has eight methods to implement (`create`, `open`, `exists`, four for metadata, `close`) and one optional hook
(`onThrottled`). This split is why `filesystem`, `gcs` and `s3` behave identically, and why each passes the same
[TCK](rawdata-client-tck.md) unchanged.

### Segment

A topic in a store is a sequence of **segments** numbered 0, 1, 2… **without gaps**. Each segment is one Avro file
holding the messages of one upload window, in id order. A segment is written once and never changed or removed.

Why numbered segments: the store only has to offer one atomic operation, "create segment `n` unless it exists". From
that alone the engine gets a total order (segment by segment), a cheap way to find the end of a topic, and the
detection of a second writer. Object stores offer it as a conditional put; a file system as a hard link.

### Upload window

A producer does not upload each `publish`. It appends the messages to the topic's open **window**, an Avro file in
`local-temp-folder`, and uploads the whole window as the next segment when it closes. All producers of a topic in one
client share that topic's window.

Why: an object per publish is expensive in requests, slow for consumers, and produces millions of small objects. A
window turns a stream of small publishes into a few large files, which are cheap to store and fast to read. The cost
is latency: a message becomes visible when its window is uploaded, not when `publish` returns. The keys are 2.x's,
so you tune it as before: [configuration](#configuration).

A window file goes through three states, visible in its name:

| File in `local-temp-folder/<topic folder>/` | State |
|---|---|
| `<segment>.window.avro` | **open**: publishes append to it |
| `<segment>.sealed.avro` | **sealed**: closed, forced to disk, its summary filled in, waiting for (or in) its upload |
| `<segment>.window.avro.invalid`, `<segment>.sealed.avro.invalid` | **set aside**: it no longer belongs to the topic (another writer went on); never uploaded, never deleted |
| `.lock` | the writer's lock on the folder |

`<segment>` is the segment number in 19 digits, so the names sort in segment order.

### Block

Inside a window, messages are written in Avro **blocks**. A block ends between two publish calls, once it holds about
`avro-file.sync.interval` bytes, and is then forced to disk. A block therefore always holds whole calls, and the
blocks on disk are what survives a crash. The size is the engine's estimate of a message's encoded size (16 bytes of
id, the position, the ordering group and each content entry's key and bytes), as 2.x estimated it.

### Header summary

Every segment's Avro header carries a summary of the segment: its first id, last id, message count, and the byte
offset of its last block. Readers use it to avoid reading whole segments: a seek reads only headers, and
`lastMessage` reads only the last block. See [the segment's Avro file](#the-segments-avro-file).

### Tail and the topic writer

The **tail** of a topic is its last segment and that segment's last id (segment -1 for an empty topic). The client
caches the last tail it saw per topic and finds the current one by probing the store forward from it.

The **topic writer** (`AvroTopicWriter`) is the upload window of one topic in one client. It is started by the
client's first publish to the topic: it takes the folder lock, recovers what a crash left, and starts from the
topic's tail. From then on, each window becomes the segment after the last one the writer sealed.

### One writer per topic

Only one process writes a topic ([the contract](../contract.md#one-writer-per-topic)). The engine checks it through
the segment numbers: the writer remembers the segment it wrote last, so the next window must become the segment
after it. If that number is already taken when a window opens or uploads, another process wrote the topic, and the
writer fails with `RawdataWriterConflictException`. It never merges its messages into the other writer's stream.

### Upload concurrency (AIMD)

Uploads of one topic run one at a time, in segment order. Across topics, a client runs several uploads at once,
and adapts how many with **additive increase, multiplicative decrease** (`AvroUploadLimiter`):
- it starts at 4 (or at `avro-file.upload.max-concurrency`, if lower);
- it adds one after a round of uploads at the limit, up to `avro-file.upload.max-concurrency`;
- it halves when an upload is throttled or times out, at most once a second, never below 1.

### Pinned segment

`AvroStore.open` returns a `Segment` **pinned** to the object found at that moment (the GCS generation, the S3
ETag, the file's key, size and modification time on disk). Its reads return that object's bytes or fail, never a
mix of two objects. It holds no connection or file, so a reader can drop it without closing.

### Checksum

Every segment upload to GCS and S3 carries the CRC32C of its bytes (`AvroChecksums.crc32c`), so the service refuses
bytes damaged on the way, and the store's retry sends them again.

### Metadata entries

Each topic has a small key-value store ([metadata](../contract.md#metadata)). The engine keeps each entry as one
file or object named `k` followed by the lowercase hex of the key's UTF-8 bytes, so any key becomes a safe name,
distinct even on case-insensitive file systems. Metadata does not go through the window: a `put` is written to the
store at once and is visible when it returns.

## How it works

### The life of a message

```
  publish(m1, m2)            window closes              upload (background)
 ─────────────────▶ open ─────────────────▶ sealed ─────────────────────────▶ segment n+1 in the store
                    window   on age, size,   window   one at a time per topic,   visible to consumers,
                    (local   producer close  (local   a few at once per client   durable in the store
                    disk)    or flush        disk)    (AIMD)
```

1. **Publish.** The producer takes the topic's publish lock (producers of one client take turns on a topic), gives
   the messages without an id a UUIDv7 id after the window's last id, and checks given ids against it (a given id
   must be greater than the one before, by `RawdataIds.ORDER`). It appends the whole call to the open window,
   opening one if there is none. A call always goes into one window whole, never split across two.
2. **End of block.** If the block being filled has reached `avro-file.sync.interval`, the call ends it: the block is
   written and forced to disk.
3. **Window closes.** The window is closed (sealed) when, after a call, it is at least `avro-file.max.bytes`; when a
   timer finds it `avro-file.max.seconds` old (so a producer that goes quiet still uploads; a publish to a window
   that old also seals it first); when a producer of the topic closes; or on `flush` and client close. An empty
   window is removed instead.
4. **Seal.** The last block is written, the header summary is filled in, the file is forced to disk and renamed
   `<segment>.sealed.avro`. It is queued for upload, and the next publish opens a new window.
5. **Back-pressure.** If `avro-file.max.pending-uploads` sealed windows of the topic are already waiting, the one
   being uploaded included, sealing waits, and so does the `publish` that sealed. A slow store therefore cannot
   fill the disk.
6. **Upload.** A background thread uploads the topic's sealed windows one at a time, in order, each as one
   `AvroStore.create` call, taking a place from the client's upload limiter. On success the file is deleted, the
   tail moves on, and this client's consumers of the topic are woken.

### Visibility and durability

| After | Visible to consumers | Survives a crash of the process |
|---|---|---|
| `publish` returns | no | only if its block has ended (forced to disk); the calls since the last block end are lost |
| the window is sealed | no | yes, on the local disk; the next writer uploads it |
| the window is uploaded | yes, in every process | yes, in the store |

So a crash of the process loses at most the calls in the block being filled, about `avro-file.sync.interval`
bytes. A lost disk (an ephemeral container volume, for example) loses everything not yet uploaded: up to
`avro-file.max.seconds` of messages, or more while uploads are failing. Put `local-temp-folder` on storage that
outlives the process if that matters.

A consumer in the same client is woken when a segment is uploaded. A consumer elsewhere sees it on its next poll,
after the provider's poll interval (`listing.min-interval-seconds` in `filesystem`,
`gcs.listing.min-interval-seconds`, `s3.listing.min-interval-seconds`; 1 second by default, at least 10 ms).

### Reading

A consumer reads a topic segment by segment:
- it streams a segment through `AvroSegmentReader`, reading 4 MiB ranges, so only one range of a segment is in
  memory, and buffers at most 1000 messages;
- at the end of a segment it opens the next; if there is none yet, the receive waits for the poll interval or a
  wake-up, then looks again;
- Avro checks each block's sync marker, so a range that comes back wrong fails instead of giving wrong messages;
- if a read fails (a store error), the receive throws `RawdataException`; the next receive opens the segment again
  and goes on after the last message it gave, with no gap and no repeat.

Starting somewhere else:

| Start | How the engine finds it | Cost |
|---|---|---|
| `consumer(topic)` | segment 0 | none |
| `consumer(topic, id)`, a cursor from `cursorOf(topic, id, inclusive)` | as 2.x: binary search over the segments' `last-id`s, from their headers, to the first segment whose `first-id` is at or below the id; there it finds the message with the id and reads everything after it in segment order, also a lower id published later; if no segment holds the id, it starts as a seek | one header (an 8 KiB range) per step |
| `seek(timestamp)` | as an id: the beginning of that millisecond, inclusive | as above |
| `cursorOf(topic, position, inclusive, approxTimestamp, tolerance)` | from the segment reaching `approxTimestamp - tolerance`, reads messages until one has the position or an id past `approxTimestamp + tolerance` | streams the segments in the time range; throws `RawdataNoSuchPositionException` if not found |
| `lastMessage(topic)` | the tail's segment: header, then its last block only | two small reads; `null` for an empty topic |

**Finding the tail.** Segments have no gaps, so the client probes `exists` forward from the last segment it saw, in
doubling steps (n+1, n+2, n+4…), then halves the step between the last that exists and the first that does not. A
reader of a long topic needs about 2 log₂(n) cheap requests the first time, and one per new segment after that.

### Graceful shutdown

- `producer.close()` uploads its topic's open window and waits until every window of the topic is uploaded.
- `client.close()` does the same for every topic, then closes the producers, consumers and the store.
- Neither waits longer than `avro-file.close-timeout-seconds` (30). For `client.close()` that is one limit for all
  topics together, not one per topic.
- When the time runs out, `close()` throws `RawdataException` ("… timed out …"), and the windows not uploaded stay
  on disk; the next writer of the topic uploads them when it starts.
  - After a producer's close times out, the producer is closed, but its topic's uploads go on and the client can
    still publish to the topic.
  - After the client's close times out, the client starts no new upload. One already under way may still complete;
    its file is then left for the next writer, which finds the segment stored with the same bytes and removes the
    file.

Set the limit below the grace period of the platform that stops your process, for example Kubernetes'
`terminationGracePeriodSeconds`, so that `close()` returns before the process is killed. The default, 30, equals
Kubernetes' default grace period: lower the limit or raise the grace period, leaving room for your application's
other shutdown work.

This is 2.x's `AutoCloseable` design: an application shuts down gracefully, for example on SIGTERM, by closing the
client. The client registers no shutdown hook of its own. Its threads (`rawdata-avro-<n>` for uploads and
asynchronous calls, `rawdata-avro-window-<n>` for the window timers) are daemon threads, so they do not keep the JVM
alive: a JVM that exits without `close()` loses the open windows' last blocks and leaves the sealed windows to the
next writer.

`AvroRawdataClient.flush(topic)` closes a topic's open window and waits for its uploads, with no time limit, without
closing anything. It does nothing for a topic this client has not published to.

## Configuration

These keys are read by `AvroUploadWindow.of`, the same in `filesystem`, `gcs` and `s3`. They are 2.x's keys.

| Key | Default | Meaning |
|---|---|---|
| `local-temp-folder` | none, required | where windows are buffered: a local disk, one folder per topic. Made absolute and created if missing. |
| `avro-file.max.seconds` | `60` | the longest a window stays open, in seconds. Bounds how late a message becomes visible, and how much a lost disk loses. |
| `avro-file.max.bytes` | `67108864` (64 MiB) | the size at which a window closes, checked after each call; a window exceeds it by at most its last call |
| `avro-file.sync.interval` | `524288` (512 KiB) | the size of an Avro block, in estimated bytes; what a crash may lose |
| `avro-file.max.pending-uploads` | `4` | sealed windows of a topic that may wait for upload (the one uploading included) before `publish` waits |
| `avro-file.close-timeout-seconds` | `30` | the longest `producer.close()` or `client.close()` waits for the uploads |
| `avro-file.upload.max-concurrency` | `16` | the most uploads of a client at once, across its topics; the limit starts at 4 and adapts |

Each provider adds its store keys and a poll interval (see its page). Every value must be 1 or more.

Values are read as [dynamic-configuration](https://github.com/descoped/dynamic-configuration) reads them
(`AvroConfiguration`):
- numbers are strict: `60`, not `60s` and not ` 60`; a blank number is an error, not the default;
- a default covers a missing key, never a malformed value;
- every error is an `IllegalArgumentException` that names its key, for example
  `Configuration 'avro-file.max.seconds' must be a whole number: '60s'`.

So an application can pass `config.asMap()` or `config.subset(..)` from dynamic-configuration to
`RawdataClient.create`.

### Choosing values

No values suit every deployment. Tune them to your message rate and size, and to how soon consumers need the
messages:
- **Latency** is up to `avro-file.max.seconds` plus the upload time. Lower it if consumers need messages sooner; each
  window is then smaller, and the store holds more, smaller segments.
- **Segment size** is the smaller of `avro-file.max.bytes` and what arrives in `avro-file.max.seconds`. A topic
  with a slow trickle of messages makes one small segment a minute at the defaults.
- **Heap.** Each running upload holds its window in memory, so uploads take up to
  `avro-file.upload.max-concurrency` × `avro-file.max.bytes` of heap: 1 GiB at the defaults (16 × 64 MiB). Size the
  heap for it, or lower either. Each topic being written also holds about 1.25 × `avro-file.sync.interval` for its
  open window (640 KiB at the default), so a very large interval means a large buffer per topic.
- **Disk.** Each topic needs room for its open window and up to `avro-file.max.pending-uploads` sealed windows:
  about 5 × 64 MiB per topic at the defaults.
- **Crash loss** is at most a block, `avro-file.sync.interval`. A smaller block forces to disk more often.

## The store: `AvroStore`

What the engine relies on, and what [`AvroStoreContractTest`](rawdata-client-tck.md) checks for every store:

| Method | Must |
|---|---|
| `create(topic, segment, content)` | create the segment **only if it does not exist**, atomically, so readers see all of it or nothing. Return `false`, writing nothing, if it exists with other bytes; return `true` if it now holds these bytes (also when a retry after a lost response finds its own write). |
| `open(topic, segment)` | return the segment pinned to the object found now (`size()`, `read(offset, length)`, short only at the end), or empty. It must hold no open resources. |
| `read(topic, segment)` | a default method: the whole segment through `open` |
| `exists(topic, segment)` | say whether it exists; called often, so make it cheap |
| `metadataNames`, `readMetadata`, `writeMetadata`, `deleteMetadata` | keep the metadata entries; a write replaces the value atomically; a delete of a missing entry does nothing; a name too long for the store is refused with `IllegalArgumentException` |
| `onThrottled(listener)` | optional: call the listener on each throttled attempt of a segment upload (a 429, a 503, a timeout), also one the store retries; never for reads or metadata |
| `close()` | release clients and pools; called once, by the client |

Store failures are `IOException`s; the client wraps them in `RawdataException`. A missing bucket or folder is a
failure, never an empty topic. Topic names reach the store validated and are case-sensitive. An Avro file carries a
random sync marker, so equal bytes on a retry mean the write was yours.

## Failures

- **An upload that fails after the store's retries fails the topic's writer.** Every later `publish`, `flush` and
  `close` of the topic in this client throws `RawdataException` ("The writer of topic … failed: …"), and the sealed
  windows stay on disk, the open one too (sealed). Nothing is lost. Fix the cause, then create a client again: its
  first publish to the topic uploads them, in order. `close()` throws the failure if no earlier call reported it, or
  if windows are left that were not uploaded.
- **A disk failure** while writing the window fails the writer the same way. If it came in the middle of a call,
  the window is left as a crash would leave it, its complete blocks on disk, and recovery cuts it back.
- **Another process writing the topic** shows as a segment number that is already taken, when a window opens or when
  it is uploaded. The writer fails with `RawdataWriterConflictException`, which names the topic, and logs an error.
  Its windows not yet uploaded, the open one included, are set aside as `*.invalid` files and never deleted. Every
  later publish of that client to the topic fails. Because the check happens at window open and upload, the
  exception may come from a later `publish`, `flush` or `close` than the one whose messages are affected; those
  messages are never written to the topic.
- **Throttling.** An upload that is throttled (GCS 429 or 503, S3 503 `SlowDown` or 429) or times out, on any
  attempt, halves the client's upload limit, at most once a second. Only segment uploads count: a consumer's read or a
  metadata write says nothing about the bucket's upload capacity. The limit grows back by one after each round of
  uploads at the limit: as many successful uploads as the limit, while uploads keep it full. A limit that uploads do
  not reach does not grow, so a burst across many topics starts where the limit was earned.
- **Damaged uploads.** Every GCS and S3 upload carries its CRC32C (`X-Goog-Hash` on GCS, `x-amz-checksum-crc32c` on
  S3); the service refuses bytes that changed on the way, and the upload is retried.
- **One process per buffer folder.** A writer holds an OS file lock on `.lock` in its topic's folder. A second
  client using the same folder for the topic, in this process or another, fails with "The buffer folder … is in use
  by another client". The lock is released when the client closes or the process dies.
- **A segment missing below the tail** fails a read with "Segment n of topic t is missing; segments are never
  removed, so the store was changed by hand".
- **A closed client.** Every call on a closed client, producer or consumer throws `RawdataClosedException`; a store
  error that comes while the client closes is reported that way too.

## Recovery after a crash

Each block of `avro-file.sync.interval` bytes is forced to disk when it ends, so a crash loses at most the block
being written. When a writer of the topic starts again (on the first publish of a new client), it looks at the
windows the crash left, in segment order:

- A **sealed** window is uploaded unchanged. If the store already has the segment with the same bytes, because the
  crash came after the upload but before the file was removed, the file is removed.
- An **open** window is cut back to its complete blocks (Avro checks each block's sync marker, so a torn block is
  recognised and left out), encoded again with its summary, then uploaded.
- A window with no complete block, or not even a whole header, is removed.
- A window is uploaded only where it belongs: as the segment right after the topic's end, written after the id that
  ends the topic. The header's `rawdata.segment` and `rawdata.previous-id` record that. Consumers therefore never see
  a gap, a duplicate or messages out of order.
- A window that no longer belongs, because another writer went on in the meantime, is set aside as `*.invalid` and
  never uploaded, and so is every window after it. The writer fails fast once with `RawdataWriterConflictException`;
  the next publish starts a writer again, from the topic's end. 2.x left its temporary files on disk the same way.

Recovery logs each window it uploads at INFO, and each it sets aside at ERROR, through `System.Logger` (see
[logging](../logging-and-the-module-path.md#logging)).

## The segment's Avro file

A segment is a plain, uncompressed Avro object container file: any Avro tool can read it. Record `RawdataMessage`,
the 2.x schema:

| Field | Type |
|---|---|
| `id` | `fixed(16)` named `uuid`, the UUIDv7 most significant byte first (2.x named it `ulid`) |
| `orderingGroup` | nullable string |
| `sequenceNumber` | long, default 0 |
| `position` | string |
| `data` | map of bytes: the message's content, read back in the order it was put |

The file's header also holds:
- `rawdata.segment` and `rawdata.previous-id`: the segment the window becomes, and the id of the message before it.
  Recovery uses them.
- The summary: `rawdata.first-id`, the lowest id in the segment; `rawdata.last-id`, the greatest id in the topic up to
  and with the segment (it grows from segment to segment, also when ids are given out of order); `rawdata.count`; and
  `rawdata.last-block` (the offset of the last block), as 2.x kept them in the file name. A seek reads only headers,
  and `lastMessage` only the last block.

A window's header is written when it opens, before its messages are known, so the summary starts as fixed-width
placeholders (a zero UUID, 19-digit zeros) and is filled in place when the window is sealed. A file whose count is
still 0 is an open window, never a segment, and a reader refuses it. Filling in searches the header only, so message
bytes that happen to contain the header's keys do no harm.

Where segments live is the store's business: for example
`<storage-folder>/<topic folder>/segments/0000000000000000000.avro` in `filesystem`, and an object per segment under
the prefix in `gcs` and `s3` (see each provider's page). Topic folders on a local disk, in the store of `filesystem`
and in `local-temp-folder`, are named by `AvroTopicFolders`: `^` and the lower-case letter for each upper-case
letter, `^` after a trailing `.`, `~` before a Windows device name, so topics that differ only by case stay apart on
case-insensitive file systems.

## Troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| Messages are not visible right after `publish` | they are in the open window | wait up to `avro-file.max.seconds`, close the producer, or call `flush(topic)`; lower `avro-file.max.seconds` if latency matters |
| `publish` blocks for a long time | `avro-file.max.pending-uploads` windows wait for a slow or throttled store | look at the store; raise the key only if the disk has room |
| `close()` throws "Waiting for the uploads of topic … timed out" | the store did not take the windows within `avro-file.close-timeout-seconds` | nothing is lost: the windows stay in `local-temp-folder` and the next writer uploads them; raise the limit within your platform's grace period |
| "The writer of topic … failed" | an upload failed after the store's retries | fix the store; create a new client; its first publish uploads what was left |
| `RawdataWriterConflictException` | another process wrote the topic, or a leftover window no longer fits | find and stop the second writer; see [one writer per topic](../contract.md#one-writer-per-topic) |
| "The buffer folder … is in use by another client" | two clients share a topic's folder in `local-temp-folder` | give each process its own folder |
| `*.invalid` files in `local-temp-folder` | windows set aside after a conflict | look at them with any Avro tool if you need to; delete them when you no longer need them |
| `OutOfMemoryError` under load | up to `max-concurrency` × `max.bytes` of windows in heap | raise the heap, or lower `avro-file.upload.max-concurrency` or `avro-file.max.bytes` |
| `IllegalArgumentException` "Configuration '…' must be …" | a malformed or out-of-range value | write plain whole numbers of 1 or more |

## Testing

### How the module is tested

The module has three test classes of its own, in the engine's package on the class path
(`useModulePath=false`), for what a real store cannot be made to do on cue:

| Test | Covers |
|---|---|
| [`AvroTopicWriterTest`](../../rawdata-client-provider-avro/src/test/java/io/descoped/rawdata/provider/avro/AvroTopicWriterTest.java) | the window's failure paths, with an in-memory store whose uploads can be held back, failed or refused: a failed upload keeps every window sealed, a taken segment sets every window aside, back-pressure, the close timeout and recovery by the next writer, leftovers without messages |
| [`AvroRangedReadTest`](../../rawdata-client-provider-avro/src/test/java/io/descoped/rawdata/provider/avro/AvroRangedReadTest.java) | the header summary, `lastMessage` reading only the last block, a seek reading only headers, a consumer going on after a failed read, malformed ranges |
| [`AvroUploadLimiterTest`](../../rawdata-client-provider-avro/src/test/java/io/descoped/rawdata/provider/avro/AvroUploadLimiterTest.java) | AIMD: start at 4, one more per round at the limit up to the cap, halving at most once a second, never below 1 |

The provider contract is tested through the providers: `filesystem` (offline), `gcs` (a fake GCS server) and `s3`
(S3Mock) each run `RawdataClientContractTest` and `AvroStoreContractTest` from the [TCK](rawdata-client-tck.md).
See [building and testing](../building-and-testing.md).

### Testing code that uses it

- Use the `filesystem` provider on a JUnit `@TempDir`: it needs no Docker and behaves exactly as `gcs` and `s3`.
- Make messages visible before you assert on them: close the producer, or call `flush`:

  ```java
  @TempDir
  Path folder;

  @Test
  void readsWhatWasPublished() throws Exception {
      Map<String, String> configuration = Map.of(
              RawdataClient.PROVIDER_KEY, "filesystem",
              "filesystem.storage-folder", folder.resolve("store").toString(),
              "local-temp-folder", folder.resolve("buffer").toString());
      try (RawdataClient client = RawdataClient.create(configuration)) {
          client.producer("t").publish(RawdataMessage.builder().position("a").put("k", new byte[]{1}).build());
          ((AvroRawdataClient) client).flush("t");
          assertEquals("a", client.lastMessage("t").position());
      }
  }
  ```

  The cast needs `rawdata-client-provider-avro` on the test compile path; without it, close the producer instead.
- To test a provider of your own on the engine, extend `AvroStoreContractTest` with your store and
  `RawdataClientContractTest` with your configuration ([writing a provider](../writing-a-provider.md)).

## Reference

Public types (package `io.descoped.rawdata.provider.avro`):

| Type | Key members |
|---|---|
| [`AvroRawdataClient`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroRawdataClient.java) | `AvroRawdataClient(AvroStore store, AvroUploadWindow window, long pollIntervalMillis)`; `void flush(String topic)`; the `RawdataClient` methods: `producer`, `consumer`, `cursorOf`, `lastMessage`, `metadata`, `isClosed`, `close` |
| [`AvroUploadWindow`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroUploadWindow.java) | record `AvroUploadWindow(Path localTempFolder, long maxSeconds, long maxBytes, int syncInterval, int maxPendingUploads, long closeTimeoutSeconds, int maxUploadConcurrency)`; `static AvroUploadWindow of(Map<String, String> configuration)`; key constants `LOCAL_TEMP_FOLDER`, `MAX_SECONDS`, `MAX_BYTES`, `SYNC_INTERVAL`, `MAX_PENDING_UPLOADS`, `CLOSE_TIMEOUT_SECONDS`, `MAX_UPLOAD_CONCURRENCY` |
| [`AvroStore`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroStore.java) | `boolean create(String topic, long segment, byte[] content)`; `Optional<Segment> open(String topic, long segment)`; `default Optional<byte[]> read(String topic, long segment)`; `boolean exists(String topic, long segment)`; `Set<String> metadataNames(String topic)`; `Optional<byte[]> readMetadata(String topic, String name)`; `void writeMetadata(String topic, String name, byte[] value)`; `void deleteMetadata(String topic, String name)`; `default void onThrottled(Runnable listener)`; `void close()` |
| `AvroStore.Segment` | `long size()`; `byte[] read(long offset, int length)` |
| [`AvroConfiguration`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroConfiguration.java) | `static String required(Map<String, String>, String key)`; `static String optional(Map<String, String>, String key, String defaultValue)`; `static long positive(Map<String, String>, String key, long defaultValue)`; `static long nonNegative(Map<String, String>, String key, long defaultValue)`; `static <E extends Enum<E>> E enumValue(Map<String, String>, String key, Class<E> type, E defaultValue)`; `static long pollIntervalMillis(Map<String, String>, String key, long defaultSeconds)`; `static String prefix(Map<String, String>, String key)`; `MIN_POLL_MILLIS` (10), `MAX_PREFIX_BYTES` (659) |
| [`AvroChecksums`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroChecksums.java) | `static String crc32c(byte[] content)`: base64 of the 4 bytes, big-endian, as GCS and S3 expect |
| [`AvroTopicFolders`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroTopicFolders.java) | `static String folderName(String topic)` |

`AvroConfiguration.prefix` reads an optional object-name prefix for stores whose names are at most 1024 bytes: empty,
or ending with `/` (added if missing), never starting with `/`, and at most `MAX_PREFIX_BYTES` so that the longest
topic and a 127-byte metadata key still fit. `enumValue` is case-insensitive and reads `-` as `_`.

Internal classes, for contributors:

| Class | Role |
|---|---|
| [`AvroTopicWriter`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroTopicWriter.java) | the upload window of one topic: append, seal, upload queue, failures, recovery |
| [`AvroUploadLimiter`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroUploadLimiter.java) | the AIMD limit on a client's concurrent uploads |
| [`AvroSegments`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroSegments.java) | the schema, the header and its summary, encoding, decoding the last block, reading leftovers |
| [`AvroSegmentReader`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroSegmentReader.java) | ranged reads: the summary, the last message, the message stream |
| [`AvroRawdataProducer`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroRawdataProducer.java) | an `OrderedRawdataProducer` that appends to the topic's window |
| [`AvroRawdataConsumer`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroRawdataConsumer.java) | a `PollingRawdataConsumer` that reads segment by segment |
| [`AvroRawdataMetadataClient`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroRawdataMetadataClient.java) | metadata entries named `k` plus hex |
| [`AvroCursor`](../../rawdata-client-provider-avro/src/main/java/io/descoped/rawdata/provider/avro/AvroCursor.java) | a start position: an id and whether it is included; a cursor of another provider is refused with `IllegalArgumentException` |

## See also

- [Concepts](../concepts.md) and the [tutorial](../tutorial.md): topics, messages, ids and the rest of the API.
- [The contract](../contract.md): what every provider, this engine included, guarantees.
- The providers on the engine: [`filesystem`](rawdata-client-provider-filesystem.md),
  [`gcs`](rawdata-client-provider-gcs.md), [`s3`](rawdata-client-provider-s3.md).
- [`rawdata-client-api`](rawdata-client-api.md): `RawdataClient`, `RawdataIds`, and the base classes
  `OrderedRawdataProducer` and `PollingRawdataConsumer`.
- [`rawdata-client-tck`](rawdata-client-tck.md): `RawdataClientContractTest` and `AvroStoreContractTest`.
- [`rawdata-client-provider-postgres`](rawdata-client-provider-postgres.md): the provider that writes in `publish`,
  without a window.
- [Architecture](../architecture.md) and [writing a provider](../writing-a-provider.md).
