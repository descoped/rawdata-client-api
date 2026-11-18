# The `filesystem` provider

Topics as Avro files in a local folder. Artifact `rawdata-client-provider-avro`, module
`io.descoped.rawdata.provider.avro`; it also holds the Avro engine that [`gcs`](gcs.md) shares.

## Configuration

| Key | Default | |
|---|---|---|
| `rawdata.client.provider` | | `filesystem` |
| `filesystem.storage-folder` | | required; created if missing |
| `filesystem.poll-interval-ms` | `250` | how often a waiting consumer looks for new messages |

## Layout

```
<storage-folder>/<topic folder>/segments/0000000000000000000.avro
                                         0000000000000000001.avro
                               /metadata/k<hex of the key>
```

Each publish is one segment, a plain Avro file of its messages (record `io.descoped.rawdata.RawdataMessage`: `id` as
`fixed(16)`, `orderingGroup`, `sequenceNumber`, `position`, `content` as an array of `{name, value}`). Segments are
numbered 0, 1, 2… without gaps.

The topic folder is the topic name, with `^` and the letter in lower case for each upper-case letter, `^` after a
trailing `.`, and `~` before a Windows device name (`con`, `nul`, `com1`…), so topics that differ only by case stay
apart on case-insensitive file systems such as macOS's and Windows'. Metadata files are named `k` and the hexadecimal
of the key's UTF-8 bytes, so a metadata key is at most 127 bytes.

## Order across processes

A publish writes its segment to a temporary file in the topic folder, then hard-links it to the next segment number,
which fails if another writer took that number first; the publish then reads the new last segment and tries the next
number. Writers in several processes on one folder so keep the topic in order, and readers never see part of a file.
The folder must be on a file system with hard links and atomic moves: a local file system on Linux, macOS or
Windows. A writer that crashes between the two steps leaves a `.*.tmp` file behind; nothing reads it, and it can be
deleted.

Messages are visible and durable when `publish` returns. Many small publishes make many small files: publish in
batches (`publish(List)`) where you can.
