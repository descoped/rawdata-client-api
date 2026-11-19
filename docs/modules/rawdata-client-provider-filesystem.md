# rawdata-client-provider-filesystem

The `filesystem` provider keeps topics as Avro files in a folder on a local disk. It is the Avro engine
([`rawdata-client-provider-avro`](rawdata-client-provider-avro.md)) on a store that writes files, so it behaves
exactly like [`gcs`](rawdata-client-provider-gcs.md) and [`s3`](rawdata-client-provider-s3.md), without a cloud
account. Use it for a single machine, for development and tests that need real files, and as the source or target of
the [copy tool](rawdata-copy-util.md).

| | |
|---|---|
| Maven | `io.descoped.rawdata:rawdata-client-provider-filesystem`, version managed by `rawdata-client-bom` |
| JPMS module | `io.descoped.rawdata.provider.filesystem` (exports nothing) |
| Provider name | `filesystem` |
| Brings | `rawdata-client-provider-avro`, and through it `rawdata-client-api` and Apache Avro |

## Contents

- [Add the dependency](#add-the-dependency)
- [Quick start](#quick-start)
- [Concepts](#concepts)
- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Failure modes and troubleshooting](#failure-modes-and-troubleshooting)
- [Testing](#testing)
- [Reference](#reference)
- [See also](#see-also)

## Add the dependency

Import the BOM once, then add the API for your code and the provider at runtime:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.descoped.rawdata</groupId>
            <artifactId>rawdata-client-bom</artifactId>
            <version>3.0.0</version>
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
</dependencies>
```

Your code depends on the API only. The service loader finds the provider by the name in the configuration. The module
is registered both with `provides` in its `module-info.java` and in `META-INF/services`, so it is found on the module
path and on the class path alike.

## Quick start

Two folders are all the provider needs: where topics are kept, and a local folder where producers buffer.

```java
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

Map<String, String> configuration = Map.of(
        RawdataClient.PROVIDER_KEY, "filesystem",              // "rawdata.client.provider"
        "filesystem.storage-folder", "/var/lib/rawdata/topics",  // where the topics are; created if missing
        "local-temp-folder", "/var/lib/rawdata/buffer");         // where producers buffer; a local disk

try (RawdataClient client = RawdataClient.create(configuration)) {
    try (RawdataProducer producer = client.producer("invoices")) {
        producer.publish(
                RawdataMessage.builder()
                        .position("invoice-1001")
                        .put("document", "<invoice id='1001'/>".getBytes(StandardCharsets.UTF_8))
                        .build(),
                RawdataMessage.builder()
                        .position("invoice-1002")
                        .put("document", "<invoice id='1002'/>".getBytes(StandardCharsets.UTF_8))
                        .build());
    } // closing the producer uploads its window: from here on the messages are visible

    try (RawdataConsumer consumer = client.consumer("invoices")) {
        List<RawdataMessage> batch = consumer.receiveUpTo(100, Duration.ofSeconds(1));
        for (RawdataMessage message : batch) {
            System.out.println(message.id() + " " + message.position());
        }
    }

    client.metadata("invoices").put("checkpoint", "invoice-1002".getBytes(StandardCharsets.UTF_8));
}
```

After it has run, the two folders hold:

```
/var/lib/rawdata/topics/invoices/segments/0000000000000000000.avro    the two messages, one Avro file
/var/lib/rawdata/topics/invoices/metadata/k636865636b706f696e74       the entry "checkpoint"
/var/lib/rawdata/buffer/invoices/.lock                                 the writer's lock file
```

Note the `close()` of the producer. A producer buffers its messages in an
[upload window](rawdata-client-provider-avro.md) and writes them to the topic when the window closes: after
`avro-file.max.seconds` (60 by default), when it reaches `avro-file.max.bytes`, or when a producer of the topic
closes. A consumer that started reading right after `publish`, with the producer still open, would wait up to a
minute for the messages.

## Concepts

### The storage folder

`filesystem.storage-folder` is the root of every topic the client sees. Each topic is a folder in it. The provider
creates the storage folder, and any missing parent, when the client is created. Several clients, in one process or
in many, may point at the same storage folder: each reads every topic in it, and one of them writes each topic (see
[one writer per topic](#one-writer-per-topic)).

### The topic folder

A topic's folder is named after the topic, with three changes so that every topic gets a folder of its own on every
file system:

| Rule | Why | Topic | Folder |
|---|---|---|---|
| An upper-case letter is written as `^` and the letter in lower case | macOS and Windows file systems are case-insensitive by default, and topic names are case-sensitive | `Invoices` | `^invoices` |
| A trailing `.` gets a `^` after it | Windows drops a trailing dot from a name | `logs.` | `logs.^` |
| A Windows device name gets `~` before it, also with an extension | Windows reserves `con`, `prn`, `aux`, `nul`, `com0`-`com9` and `lpt0`-`lpt9`, in any case | `CON`, `nul.txt` | `~^c^o^n`, `~nul.txt` |

`^` and `~` are not allowed in topic names (1 to 100 characters of `A-Z a-z 0-9 _ . -`, starting with a letter or
digit, checked by `RawdataTopics` before a name reaches the provider). So two different topics never map to the same
folder, and a topic name can never escape the storage folder: no `/`, no `..`.

The same rule names the topic's folder under `local-temp-folder`. It is `AvroTopicFolders.folderName(String)` in the
Avro engine.

### Segments

A topic's messages are in `<topic folder>/segments/`, as numbered Avro files: `0000000000000000000.avro`,
`0000000000000000001.avro`, and so on. The number has 19 digits, so the names sort in segment order. Each segment is
one upload window: the messages a producer buffered, in the order they were published. Segments are numbered 0, 1,
2… without gaps. With ids the producer generates, the messages of segment *n + 1* all come after those of segment *n*
in `RawdataIds.ORDER`; a given id lower than earlier ones stays where it was published.

A segment is a plain Avro data file, uncompressed, in the record schema `RawdataMessage` that 2.x used (fields `id`,
`orderingGroup`, `sequenceNumber`, `position` and `data`). Its header also holds a summary (first id, last id, count,
offset of the last block) so that readers can find their way without reading the whole file. Any Avro tool can open a
segment. The schema and the header keys are described in
[Avro engine: the segment's Avro file](rawdata-client-provider-avro.md#the-segments-avro-file).

A segment is written once and never changed, replaced or deleted by the provider.

### Writing a segment: a temporary file and a hard link

A segment appears in one step, whole, and only if its number is free. The store does it in two steps:

1. It writes the bytes to a temporary file in the topic folder, named `.<random>.tmp`.
2. It creates a **hard link** from the segment's name to that file (`Files.createLink`), then removes the
   temporary name.

Creating a link fails if the name exists. That is the file system's "create if absent", and it gives the two
properties the Avro engine relies on:

- **Readers never see part of a segment.** The name appears only once the file is complete.
- **Two writers never both create a segment.** If another process created that number first, the link fails, and
  the writer learns that the topic moved under it.

A rename would not do: a rename replaces an existing file. A file opened with `CREATE_NEW` would give "create if
absent", but readers could see it while it is written.

If the link finds the name taken, the store reads the existing segment. When it holds the very same bytes, the
create counts as done. That happens when a writer retries after a crash that came after the link but before it
recorded the upload. An Avro file has a random 16-byte sync marker, so another writer's segment never has the same
bytes.

### Metadata entries

A topic's metadata is a set of key-value pairs, for example a checkpoint of a consumer. Each entry is a file in
`<topic folder>/metadata/`, named `k` followed by the lowercase hexadecimal of the key's UTF-8 bytes:

| Key | File |
|---|---|
| `checkpoint` | `k636865636b706f696e74` |
| `Checkpoint` | `k436865636b706f696e74` |
| `` (empty) | `k` |
| `../escape` | `k2e2e2f657363617065` |

So any key works, including an empty one, one with `/`, `..` or a leading `.`, and keys that differ only by case: none
of them reaches the file system as given. The cost is length. A file name is at most 255 characters, so a key is at
most **127 bytes in UTF-8** (127 ASCII characters, fewer for other scripts). That equals
`RawdataMetadataClient.PORTABLE_KEY_BYTES`, the length every provider accepts. A longer key is refused with
`IllegalArgumentException`.

`put` writes the value to a temporary file in the topic folder and moves it over the entry's name with an atomic
move. A reader sees the old value or the new one, never a mix. Metadata is not part of the one-writer check: the last
`put` wins.

### The buffer folder and its lock file

`local-temp-folder` is where producers buffer their upload windows, one folder per topic, named like the topic folder.
It must be on a local disk. When a client first publishes to a topic, it opens `<local-temp-folder>/<topic
folder>/.lock` and takes an exclusive lock on it (`FileChannel.tryLock`). It holds the lock until the topic's writer
closes. A second client that tries to write the same topic from the same buffer folder, in this JVM or another,
fails with:

```
The buffer folder <folder> of topic '<topic>' is in use by another client: only one process may write a topic
```

The lock protects the buffer, not the topic: two processes with buffer folders of their own are kept apart by the
segment numbers instead (see [one writer per topic](#one-writer-per-topic)). The lock file is left in place after the
lock is released; it holds no data.

Next to `.lock` you may see the window files:

| File | Meaning |
|---|---|
| `<segment>.window.avro` | the open window, being filled |
| `<segment>.sealed.avro` | a closed window, waiting for upload |
| `<segment>.window.avro.invalid`, `<segment>.sealed.avro.invalid` | set aside after another writer was found; never uploaded, never deleted |

A client that only reads never takes the lock and writes nothing there. It still needs `local-temp-folder` in its
configuration, because the key is required.

### Pinned segments

A consumer opens a segment and then reads it in ranges, so only a part of a large segment is in memory. Each range
is read from the file that was opened, or fails. On every read the store checks that the name still leads to the same
file: the same file key (the inode on Linux and macOS; Java gives no file key on Windows), the same size and the same
modification time. The size and time are checked as well because a file system may give a new file a deleted file's
inode. The provider never replaces a segment, so this only fails when someone changed the files by hand. The reader
then gets an `IOException` ("is no longer the file that was opened"), which the consumer reports as a
`RawdataException`, instead of a mix of two files.

### One writer per topic

Only one process writes a topic (see [the contract](../contract.md#one-writer-per-topic)). In that process, any number
of producers of one client may publish to it; they share the topic's window and are kept in order. Consumers may be
anywhere that sees the storage folder.

The `filesystem` provider detects a second writer as every provider does. When the client starts writing a topic,
it notes where the topic ends: its last segment. From then on it creates each window as the segment after the last one
it wrote. If that number exists when a window opens, or the hard link fails when the window is uploaded, another
process wrote the topic. The client fails with `RawdataWriterConflictException`:

```
Another client published to topic '<topic>': only one process may write a topic
```

It keeps failing to publish to that topic. Its windows not yet uploaded are renamed `*.invalid` and kept, never
uploaded. The other writer goes on undisturbed.

## How it works

### The write path

```
publish(m1, m2)                                   background upload thread
      |                                                     |
      v                                                     |
<local-temp-folder>/invoices/0000000000000000003.window.avro |
      |  window closes: age, size, or producer close         |
      v                                                     |
<local-temp-folder>/invoices/0000000000000000003.sealed.avro ----> FilesystemAvroStore.create(topic, 3, bytes)
                                                                    |
                                     write  <storage>/invoices/.<random>.tmp
                                     link   <storage>/invoices/segments/0000000000000000003.avro
                                            (fails if the name exists: another writer)
                                     delete <storage>/invoices/.<random>.tmp
                                                                    |
                                     delete the sealed window; wake consumers of this client
```

1. `publish` appends the call's messages to the topic's open window in the buffer folder. A call is always written
   whole into one window. Each Avro block of `avro-file.sync.interval` bytes is forced to disk as it ends.
2. The window closes when it is `avro-file.max.seconds` old, when it reaches `avro-file.max.bytes`, or when a
   producer of the topic closes. It is forced to disk and renamed `.sealed.avro`.
3. A background thread uploads sealed windows one at a time, in segment order: the store writes the temporary file and
   links it to the segment's name. Then the sealed window is removed.
4. Publishing goes on in a new window meanwhile. If `avro-file.max.pending-uploads` sealed windows already wait,
   `publish` waits too.

The upload window, its timers, the limit on concurrent uploads and recovery after a crash belong to the Avro engine and
are the same for `gcs` and `s3`. They are described in full on the [Avro
engine](rawdata-client-provider-avro.md#how-it-works) page.

### Visibility and durability

- **A message is visible once its window is uploaded**, that is, once its segment's hard link exists. Consumers of the
  same client are woken at once. Consumers of other clients see it at their next poll, at most
  `listing.min-interval-seconds` later.
- **Order is the segment order, then the order in the segment**, which is publish order. Ids the producer generates
  grow in `RawdataIds.ORDER`, so such a topic is read in id order; a given lower id published later is read after
  what was published before it.
- **What survives a crash of the writing process.** The open window's complete blocks and every sealed window are on
  disk. When a writer of the topic starts again, it uploads what belongs right after the topic's end, cut back to its
  complete blocks, and sets aside what no longer belongs. A temporary `.*.tmp` file in a topic folder can also be
  left, when the crash came between writing it and linking it. Nothing reads it, and it may be deleted.
- **What survives a crash of the machine.** The buffer's blocks and sealed windows are forced to disk (`fsync`). The
  store's own writes are not: the segment's temporary file, the hard link and metadata files are written through the
  operating system's cache, without `fsync` of the file or its folder. Once the sealed window has been removed from the
  buffer, a power loss before the operating system writes its cache can lose a segment that was already visible.
  Choose a file system and mount options that suit the durability you need.

### The read path

A consumer reads a topic segment by segment:

1. **Find the start.** Without a start position it begins at segment 0. With a cursor or a `seek`, it does a binary
   search over the segments' header summaries for the first segment whose last id reaches the start, then skips the
   messages before it. Only headers are read.
2. **Stream the segment.** It opens the segment (pinned, see above) and reads it in ranges, buffering at most 1000
   messages at a time.
3. **Wait for the next.** When the next segment does not exist yet, `receive` waits, checking for it every
   `listing.min-interval-seconds` (1 by default; `0` means as often as `receive` asks, but no more often than every
   10 ms). An upload through the same client wakes its consumers at once.

`lastMessage(topic)` and a writer's first publish find the topic's last segment by probing segment names forward in
growing steps, then halving: a few `exists` checks even for a long topic, since segments have no gaps. The last
message is then read from that segment's last block.

### What the store requires of the file system

The storage folder must be on a file system with hard links and atomic moves. The local file systems of Linux, macOS
and Windows (for example ext4, XFS, APFS and NTFS) have both. FAT32 and exFAT have no hard links, so the first upload
fails there. The provider is built and tested for local file systems; network file systems are not tested.

The temporary file and the segment are in the same topic folder, so the link never crosses a file system.

## Configuration

Pass these keys in the map given to `RawdataClient.create`. Values are read strictly, as
[dynamic-configuration](https://github.com/descoped/dynamic-configuration) reads them: numbers are whole numbers with
no spaces or units (`60`, not `60s` or ` 60`); a default covers a missing key, never a malformed value; every error
is an `IllegalArgumentException` that names its key.

| Key | Default | Meaning |
|---|---|---|
| `rawdata.client.provider` | | `filesystem` (`RawdataClient.PROVIDER_KEY`) |
| `filesystem.storage-folder` | required | the folder of the topics; created with its parents if missing; a relative path is resolved against the working directory |
| `local-temp-folder` | required | where producers buffer, one folder per topic; a local disk |
| `listing.min-interval-seconds` | `1` | how often a waiting consumer looks for the next segment, in whole seconds; `0` as often as `receive` asks (at least 10 ms apart) |
| `avro-file.max.seconds` | `60` | the longest a window stays open before it is uploaded |
| `avro-file.max.bytes` | `67108864` (64 MiB) | the size at which a window closes |
| `avro-file.sync.interval` | `524288` (512 KiB) | the size of an Avro block; each block is forced to disk, so a crash loses at most the block being written |
| `avro-file.max.pending-uploads` | `4` | sealed windows that may wait for upload before `publish` waits |
| `avro-file.close-timeout-seconds` | `30` | the longest `close()` of a producer or the client waits for the uploads |
| `avro-file.upload.max-concurrency` | `16` | the most uploads of a client at once, across its topics |

Every `avro-file.*` value must be 1 or more; `listing.min-interval-seconds` must be 0 or more.
`filesystem.storage-folder` and `local-temp-folder` must not be blank. The `avro-file.*` keys are explained,
with advice on tuning them, in [Avro engine: configuration](rawdata-client-provider-avro.md#configuration).

`FilesystemRawdataClientInitializer.configurationKeys()` reports the two required keys,
`filesystem.storage-folder` and `local-temp-folder`.

### Choosing values

- **Latency.** Consumers see a message at most about `avro-file.max.seconds` after it was published, plus the poll
  interval for consumers of other clients. Lower `avro-file.max.seconds` for fresher data; each window becomes one
  file, so lower values give more, smaller segments.
- **Memory.** A running upload holds its whole window in memory, so uploads may take up to
  `avro-file.upload.max-concurrency` × `avro-file.max.bytes` of heap (1 GiB at the defaults). Lower either for a small
  heap.
- **Shutdown.** Keep `avro-file.close-timeout-seconds` below the time your platform gives a process to stop, so that
  `client.close()` returns before the process is killed.

## Failure modes and troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| `IllegalArgumentException: Provider 'filesystem' is missing configuration: [filesystem.storage-folder, local-temp-folder]` | a required key is missing (the list names the missing ones) | add the key |
| `IllegalArgumentException: Missing configuration 'filesystem.storage-folder'` (or `'local-temp-folder'`) | a required key is blank | give it a value |
| `IllegalArgumentException: Configuration 'avro-file.max.seconds' must be a whole number: '60s'` | a malformed number | write whole numbers only |
| `RawdataException: Cannot create the storage folder <folder>` at `RawdataClient.create` | the folder cannot be created: no permission, or a regular file in the path | fix the path or its permissions |
| Messages are not visible right after `publish` | they are in the open window | close the producer, wait `avro-file.max.seconds`, or call `AvroRawdataClient.flush(topic)` in tests |
| `RawdataException: The buffer folder <folder> of topic '<topic>' is in use by another client` | another client, in this JVM or another process, writes the topic from the same `local-temp-folder` | write each topic from one client; give each process a buffer folder of its own |
| `RawdataWriterConflictException: Another client published to topic '<topic>'` | another process wrote the topic, with a buffer folder of its own | stop the second writer; a client created after it stopped starts from the topic's end. The windows set aside are kept as `*.invalid` in the buffer folder |
| `IllegalArgumentException: A metadata key of the filesystem provider is at most 127 bytes in UTF-8` | the key's file name would exceed 255 characters | use a key of at most 127 bytes |
| `RawdataException` with "Uploading segment … failed" (for example, permission denied) | the store could not write the segment | fix the folder; the writer stays failed until it is closed, and the sealed windows stay in the buffer folder: the next writer of the topic uploads them, nothing is lost |
| `RawdataException` with "is no longer the file that was opened" | a segment was replaced or removed by hand while it was read | do not change segment files; the provider never does |
| `.*.tmp` files in a topic folder | a writer crashed between writing a file and linking or moving it | delete them; nothing reads them |
| `*.invalid` files in the buffer folder | windows set aside after a writer conflict, or a crash leftover that no longer belongs after the topic's end | inspect them (they are Avro files) and delete them when you are sure they are not needed |
| The first upload fails on a USB stick or SD card | FAT32 and exFAT have no hard links | put the storage folder on a file system with hard links |
| A DEBUG line "Snappy was not available" from `org.apache.avro.file.CodecFactory` | Avro looks for optional codecs when it starts | nothing: segments are uncompressed and need no codec |

**Do not edit, rename or delete segments by hand while the topic is in use.** The engine relies on segments numbered
without gaps and never changed. Deleting a topic means deleting its folder in the storage folder (and in the buffer
folder) while no client uses it.

**Do not share one `local-temp-folder` between processes that write.** The lock file stops the second one, but a
buffer folder per process is simpler. On Kubernetes, that means a volume per pod.

## Testing

### How the module is tested

The provider's tests run with `./mvnw verify`, offline and without Docker, on the class path, patched into the
provider (so that the store contract can reach the package-private `FilesystemAvroStore`):

| Test | What it checks |
|---|---|
| [`FilesystemContractTest`](../../rawdata-client-provider-filesystem/src/test/java/io/descoped/rawdata/provider/filesystem/test/FilesystemContractTest.java) | the whole provider contract (`AvroClientContractTest`, which extends `RawdataClientContractTest`), with a metadata key limit of 127 bytes and a storage folder that cannot exist (one inside a regular file) |
| [`FilesystemStoreContractTest`](../../rawdata-client-provider-filesystem/src/test/java/io/descoped/rawdata/provider/filesystem/FilesystemStoreContractTest.java) | the `AvroStore` contract (`AvroStoreContractTest`), and that an opened segment is pinned to its file |
| [`FilesystemProviderTest`](../../rawdata-client-provider-filesystem/src/test/java/io/descoped/rawdata/provider/filesystem/test/FilesystemProviderTest.java) | the files on disk: the layout, the Avro schema of a segment, metadata keys that are any string, the configuration checks, and recovery of sealed windows a crash left behind |

The JPMS smoke test (`src/it/jpms/run.sh`) also round-trips a topic through `filesystem` with everything on the
module path and with everything on the class path.

```sh
./mvnw verify -pl rawdata-client-provider-filesystem -am
```

### Testing your own code with it

The `filesystem` provider is a good choice for tests that need the real behaviour of the Avro providers (windows,
segments, a second client that reads what the first wrote) with no container. For a plain in-memory stand-in, use
the `memory` provider instead.

```java
class InvoiceImportTest {

    @TempDir
    Path folder;

    private Map<String, String> configuration() {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", folder.resolve("topics").toString(),
                "local-temp-folder", folder.resolve("buffer").toString(),
                "listing.min-interval-seconds", "0");   // consumers of other clients poll without waiting a second
    }

    @Test
    void importedInvoicesAreReadBack() throws Exception {
        try (RawdataClient client = RawdataClient.create(configuration())) {
            try (RawdataProducer producer = client.producer("invoices")) {
                producer.publish(RawdataMessage.builder().position("1001").put("document", new byte[]{1}).build());
            } // the close uploads the window
            try (RawdataConsumer consumer = client.consumer("invoices")) {
                assertEquals("1001", consumer.receive(5, TimeUnit.SECONDS).position());
            }
        }
    }
}
```

Tips:

- Make messages visible by closing the producer. To keep it open, cast the client and call
  `((AvroRawdataClient) client).flush(topic)`; on the module path that needs
  `requires io.descoped.rawdata.provider.avro` in your test module. A small `avro-file.max.seconds` (such as `1`) also
  works, but makes tests wait.
- Give each client that writes a `local-temp-folder` of its own, as each process would have.
- `@TempDir` cleans up after the test. The files are plain: open a segment with Avro's `DataFileReader` to check what
  was written.

## Reference

| Type | Where | What |
|---|---|---|
| `FilesystemRawdataClientInitializer` | [source](../../rawdata-client-provider-filesystem/src/main/java/io/descoped/rawdata/provider/filesystem/FilesystemRawdataClientInitializer.java) | the provider, `@ProviderName("filesystem")`; found by the service loader, not called by applications |
| `FilesystemAvroStore` | [source](../../rawdata-client-provider-filesystem/src/main/java/io/descoped/rawdata/provider/filesystem/FilesystemAvroStore.java) | package-private: the `AvroStore` on a folder |
| `module-info.java` | [source](../../rawdata-client-provider-filesystem/src/main/java/module-info.java) | `requires io.descoped.rawdata.provider.avro`; `provides RawdataClientInitializer` |
| `META-INF/services` | [source](../../rawdata-client-provider-filesystem/src/main/resources/META-INF/services/io.descoped.rawdata.api.RawdataClientInitializer) | the class-path registration |

`FilesystemRawdataClientInitializer`:

| Method | |
|---|---|
| `public FilesystemRawdataClientInitializer()` | called by the service loader |
| `Set<String> configurationKeys()` | `filesystem.storage-folder` and `local-temp-folder` |
| `RawdataClient initialize(Map<String, String> configuration)` | reads the keys, creates the storage folder, and returns an `AvroRawdataClient` on a `FilesystemAvroStore`; throws `IllegalArgumentException` for bad configuration and `RawdataException` if the folder cannot be created |

`FilesystemAvroStore` implements `AvroStore` (in the Avro engine):

| Method | On the file system |
|---|---|
| `boolean create(String topic, long segment, byte[] content)` | temporary file, then `Files.createLink` to `segments/<19 digits>.avro`; `false` if the name exists with other bytes |
| `Optional<Segment> open(String topic, long segment)` | reads the file's attributes; each `read(offset, length)` checks file key, size and modification time |
| `boolean exists(String topic, long segment)` | `Files.exists` on the segment's name |
| `Set<String> metadataNames(String topic)` | the file names in `metadata/`, empty if the folder is missing |
| `Optional<byte[]> readMetadata(String topic, String name)` | reads `metadata/<name>` |
| `void writeMetadata(String topic, String name, byte[] value)` | temporary file, then an atomic move over `metadata/<name>`; `IllegalArgumentException` for a name over 255 characters |
| `void deleteMetadata(String topic, String name)` | `Files.deleteIfExists` |
| `void close()` | nothing to release |

Applications use the API: `RawdataClient`, `RawdataProducer`, `RawdataConsumer`, `RawdataMetadataClient` (see
[rawdata-client-api](rawdata-client-api.md)). The client that `initialize` returns is an `AvroRawdataClient`; its one
method beyond the API is `flush(String topic)`.

## See also

- [Concepts](../concepts.md): topics, messages, ids, positions, cursors and metadata, for every provider.
- [Tutorial](../tutorial.md): a guided first application.
- [The contract](../contract.md): what every provider guarantees, and
  [one writer per topic](../contract.md#one-writer-per-topic).
- [rawdata-client-provider-avro](rawdata-client-provider-avro.md): the Avro engine this provider runs on.
- [rawdata-client-provider-gcs](rawdata-client-provider-gcs.md) and
  [rawdata-client-provider-s3](rawdata-client-provider-s3.md): the same engine on a bucket.
- [rawdata-client-api](rawdata-client-api.md): the API, and the `memory` and `discard` providers.
- [rawdata-client-tck](rawdata-client-tck.md): the contract tests a provider passes.
- [rawdata-copy-util](rawdata-copy-util.md): copy topics between a folder and another provider.
