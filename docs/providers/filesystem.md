# The `filesystem` provider

Topics as Avro files in a local folder. Artifact `rawdata-client-provider-filesystem`, module
`io.descoped.rawdata.provider.filesystem`, on the Avro engine (`rawdata-client-provider-avro`) that [`gcs`](gcs.md)
and [`s3`](s3.md) share.

## Configuration

| Key | Default | |
|---|---|---|
| `rawdata.client.provider` | | `filesystem` |
| `filesystem.storage-folder` | | required; created if missing |
| `listing.min-interval-seconds` | `1` | how often a waiting consumer looks for new messages, in seconds; `0` as often as it asks |
| `local-temp-folder` | | required; where producers buffer, see [the upload window](upload-window.md) |
| `avro-file.max.seconds`, `avro-file.max.bytes`, `avro-file.sync.interval`, `avro-file.max.pending-uploads`, `avro-file.close-timeout-seconds`, `avro-file.upload.max-concurrency` | `60`, 64 MiB, 512 KiB, `4`, `30`, `16` | the window; see [the upload window](upload-window.md) |

## Layout

```
<storage-folder>/<topic folder>/segments/0000000000000000000.avro
                                         0000000000000000001.avro
                               /metadata/k<hex of the key>
```

Each upload window is one segment, a plain Avro file of its messages in the 2.x schema (see
[the upload window](upload-window.md#the-segments-avro-file)). Segments are numbered 0, 1, 2… without gaps.

The topic folder is the topic name, with `^` and the letter in lower case for each upper-case letter, `^` after a
trailing `.`, and `~` before a Windows device name (`con`, `nul`, `com1`…), so topics that differ only by case stay
apart on case-insensitive file systems such as macOS's and Windows'. Metadata files are named `k` and the hexadecimal
of the key's UTF-8 bytes, so a metadata key is at most 127 bytes.

## Writing a segment

Producers buffer in [upload windows](upload-window.md). Uploading a window writes it to a temporary file in the topic
folder, then hard-links it to the next segment number. The link fails if the number is taken: another process wrote
the topic, and the writer fails with `RawdataWriterConflictException` (see
[one writer per topic](../contract.md#one-writer-per-topic)). Readers never see part of a file.
The folder must be on a file system with hard links and atomic moves: a local file system on Linux, macOS or
Windows. A writer that crashes between the two steps leaves a `.*.tmp` file behind; nothing reads it, and it can be
deleted.

Messages are visible once their window is uploaded.
