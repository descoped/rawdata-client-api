# The upload window (`filesystem`, `gcs`, `s3`)

The providers on the Avro engine write a topic the way 2.x did. A producer buffers messages on a local disk, then
uploads them as one large Avro file. Uploading a file per publish would be expensive for a bucket and slow for
consumers. Large files are cheap to store and fast to read.

## How a window works

- `publish` appends the call's messages to the topic's open window: an Avro file in `local-temp-folder`. A call is
  always written whole into one window.
- The window closes when:
  - it is `avro-file.max.seconds` old (a timer closes it, so a producer that goes quiet still uploads);
  - it is larger than `avro-file.max.bytes`;
  - a producer of the topic closes.
- A closed window is forced to disk and sealed. It is then uploaded in the background as the topic's next segment,
  while publishes go on in a new window, so publishing never waits for an upload.
- **A message is visible to consumers, and durable in the bucket, once its window is uploaded.**
- Uploads of a topic run one at a time, in order. If `avro-file.max.pending-uploads` sealed windows are already
  waiting, `publish` waits too, so a slow store can't fill the disk.

All producers of a topic in one client share the topic's window.

## Configuration

These are the 2.x keys. No values suit every deployment: tune them to your message rate and size, and to how soon
consumers need the messages.

| Key | Default | |
|---|---|---|
| `local-temp-folder` | | required; a local disk, one folder per topic |
| `avro-file.max.seconds` | `60` | the longest a window stays open |
| `avro-file.max.bytes` | `67108864` (64 MiB) | the size at which a window closes |
| `avro-file.sync.interval` | `524288` (512 KiB) | the size of an Avro block |
| `avro-file.max.pending-uploads` | `4` | sealed windows that may wait for upload before `publish` waits |
| `avro-file.close-timeout-seconds` | `30` | the longest `close()` waits for the uploads |

Values are read as [dynamic-configuration](https://github.com/descoped/dynamic-configuration) reads them:
- numbers are strict: `60`, not `60s` and not ` 60`;
- a default covers a missing key, never a malformed value;
- every error names its key.

So an application can pass `config.asMap()` or `config.subset(..)` from dynamic-configuration to
`RawdataClient.create`.

## Closing: graceful shutdown

- `producer.close()` uploads its topic's open window and waits until every window of the topic is uploaded.
- `client.close()` does the same for every topic, then closes the client.
- Neither waits longer than `avro-file.close-timeout-seconds` (30), so a store that hangs can't hold up a shutdown.
  For `client.close()` it is one limit for all topics together. When the time runs out, `close()` throws
  `RawdataException`, and the windows not uploaded stay on disk; the next writer of the topic uploads them when it
  starts. After a producer's close times out, the uploads go on and the client can still publish. After the
  client's close times out, the client starts no new upload; one already under way may still complete, and its
  file is then left for the next writer, which sees the segment stored with the same bytes and removes the file.

Set the limit below the grace period of the platform that stops the process, for example Kubernetes'
`terminationGracePeriodSeconds`, so that `close()` returns before the process is killed. The default limit, 30,
equals Kubernetes' default grace period: on Kubernetes, lower the limit or raise the grace period, leaving room
for the application's other shutdown work.

This is 2.x's `AutoCloseable` design: an application shuts down gracefully, for example on SIGTERM, by closing the
client. The client registers no shutdown hook of its own.

`AvroRawdataClient.flush(topic)` closes a topic's window and waits for its uploads without closing anything. The
shared tests use it.

## Failures

- **An upload that fails after the store's retries fails the topic's writer.** Every later `publish`, `flush` and
  `close` throws `RawdataException`, and the sealed windows stay on disk. The next writer of the topic uploads them
  when it starts. Nothing is lost.
- **Another process writing the topic** shows as a segment number that is already taken, when a window opens or
  when it is uploaded. The writer fails with `RawdataWriterConflictException`. Its windows not yet uploaded are kept
  as `*.invalid` files and never deleted. Every later publish of that client to the topic fails (see
  [one writer per topic](../contract.md#one-writer-per-topic)).
- **One process per buffer folder.** A writer holds a lock file in its topic's folder. A second client using the
  same folder fails with "in use".

## Recovery after a crash

Each block of `avro-file.sync.interval` bytes is forced to disk when it is written, so a crash loses at most the
block being written. When a writer of the topic starts again, it looks at the windows the crash left:

- A **sealed** window is uploaded unchanged. If the store already has it with the same bytes, because the crash came
  after the upload but before the file was removed, the file is removed.
- An **open** window is cut back to its complete blocks, then uploaded.
- A window is uploaded only where it belongs: right after the topic's end, after the id it was written after.
  Consumers therefore never see a gap, a duplicate or messages out of order.
- A window that no longer belongs, because another writer went on in the meantime, is set aside as `*.invalid` and
  never uploaded. The writer fails fast once with `RawdataWriterConflictException`; its next publish starts from the
  topic's end. 2.x left its temporary files on disk the same way.

## The segment's Avro file

Record `RawdataMessage`, the 2.x schema:

| Field | Type |
|---|---|
| `id` | `fixed(16)` named `uuid` (2.x named it `ulid`; the bytes are now a UUIDv7) |
| `orderingGroup` | nullable string |
| `sequenceNumber` | long, default 0 |
| `position` | string |
| `data` | map of bytes, the message's content, in the order it was put |

The file's header also holds:
- `rawdata.segment` and `rawdata.previous-id`: the segment the window becomes, and the id of the message before it.
  Recovery uses them.
- The summary: `rawdata.first-id`, `rawdata.last-id`, `rawdata.count` and `rawdata.last-block` (the offset of the last
  block), as 2.x kept them in the file name. A seek reads only headers, and `lastMessage` only the last block. They
  are filled in when the window is sealed; until then they are placeholders.
