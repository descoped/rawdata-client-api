# Writing a provider

A provider is a `RawdataClientInitializer` that the service loader finds, named with `@ProviderName`:

```java
@ProviderName("mystore")
public final class MyStoreRawdataClientInitializer implements RawdataClientInitializer {

    public MyStoreRawdataClientInitializer() {
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of("mystore.url");          // the required keys; RawdataClient.create refuses a configuration without them
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        return new MyStoreRawdataClient(configuration.get("mystore.url"));
    }
}
```

Register it both ways, so it works on the module path and the class path:

```java
module com.example.rawdata.mystore {
    requires io.descoped.rawdata.api;

    provides io.descoped.rawdata.api.RawdataClientInitializer with com.example.rawdata.mystore.MyStoreRawdataClientInitializer;
}
```

and `META-INF/services/io.descoped.rawdata.api.RawdataClientInitializer` with the class name. Applications then use
it with `rawdata.client.provider=mystore`.

## The base classes

`io.descoped.rawdata.api.spi` has the parts every provider that reads its store in batches needs:

- `OrderedRawdataProducer`: argument checks, `publishAsync`, closing, and `withIds(batch, lastId)`, which gives ids
  after the topic's last id and checks given ones. You implement `write(batch)`: find the topic's last id, call
  `withIds`, and write the result, all or nothing, after everything in the topic. If the topic moved since this
  client last wrote it (another process wrote), throw `RawdataWriterConflictException` and write nothing.
- `PollingRawdataConsumer`: the receive loop, waking, `receiveAsync`, `seek` and closing. You implement
  `fill(buffer)`, which reads the messages after the last one read, and `reposition(timestamp)`.

Only one process writes a topic, and a provider must notice when another one did (see
[one writer per topic](contract.md#one-writer-per-topic)). The `postgres` provider holds a per-topic advisory lock
while it reads the last id, compares it with the last it wrote, and writes; the Avro engine creates the next numbered
segment only if it does not exist. For a store of files or objects, implement `AvroStore` instead; see below.

## A store of files or objects: `AvroStore`

`rawdata-client-provider-avro` has the whole client for stores that keep files or objects: `AvroRawdataClient`. It
handles the [upload window](modules/rawdata-client-provider-avro.md#upload-window), segments, ids, ordering, recovery,
consumers and metadata. A provider only implements `AvroStore` (`io.descoped.rawdata.provider.avro`) and passes it to
the client:

```java
@Override
public RawdataClient initialize(Map<String, String> configuration) {
    String bucket = AvroConfiguration.required(configuration, "mystore.bucket");
    long pollInterval = AvroConfiguration.pollIntervalMillis(configuration, "mystore.listing.min-interval-seconds", 1);
    AvroUploadWindow window = AvroUploadWindow.of(configuration); // local-temp-folder and the avro-file.* keys
    return new AvroRawdataClient(new MyAvroStore(bucket), window, pollInterval);
}
```

| Method | Must |
|---|---|
| `create(topic, segment, bytes)` | write the segment **only if it does not exist**, atomically. Return false, writing nothing, if it exists with other bytes, and true if it holds these bytes. This is how the engine notices another writer. |
| `open(topic, segment)` | return the segment, or empty: its size, and `read(offset, length)` for ranges, cut short only at its end. Pin it to the object found now (a generation, an ETag, a file key), so a replaced object fails the read. It must hold no connection or file: readers drop it without closing. `read(topic, segment)` is a default method on top of it. |
| `exists(topic, segment)` | say whether it exists; it is called often, so make it cheap (a HEAD, a stat) |
| `metadataNames`, `readMetadata`, `writeMetadata`, `deleteMetadata` | keep a topic's metadata entries. A write replaces the value atomically, and a delete of a missing entry does nothing. |
| `onThrottled(listener)` | optional: call the listener on each throttled response (a 429, a 503, a timeout), also one you retry, so the client lowers its upload concurrency |
| `close()` | release clients and pools; called once, by the client |

What to watch:
- **Conditional create is the contract.** It is a hard link on a file system, `ifGenerationMatch=0` on GCS, and
  `If-None-Match: *` on S3. An "upload, then check" is not enough: it would overwrite another writer's segment.
- **Retries after a lost response.** A client that retries a create after the response was lost sees its own
  segment and a failed condition. Compare the stored bytes with your own: an Avro file carries a random sync marker,
  so equal bytes mean it was your write. Return true then.
- **Names.** Topic names reach the store validated and are case-sensitive. Metadata names are `k` plus lowercase hex.
  Refuse a name that is too long for the store with `IllegalArgumentException`.
- **Errors.** Throw `IOException` for store failures; the client wraps them. A missing bucket or folder is a failure,
  never an empty topic.

The [`filesystem`](modules/rawdata-client-provider-filesystem.md), [`gcs`](modules/rawdata-client-provider-gcs.md) and
[`s3`](modules/rawdata-client-provider-s3.md) stores are short examples.

## The contract tests

A provider is compliant when it passes the shared tests unchanged. Add `rawdata-client-tck` as a test dependency
and extend `RawdataClientContractTest` with your configuration:

```java
class MyStoreContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return Map.of(RawdataClient.PROVIDER_KEY, "mystore", "mystore.url", TestStore.url());
    }

    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of(500); // the longest key your store takes, as your docs say
    }

    @Override
    protected Map<String, String> missingStoreConfiguration() {
        return Map.of(RawdataClient.PROVIDER_KEY, "mystore", "mystore.url", TestStore.url() + "/no-such-store");
    }
}
```

| Hook | Default | |
|---|---|---|
| `configuration()` | | required; every call must give a client on the same store |
| `clientsShareTheStore()` | `true` | false only if each client has its own store (`memory`); the tests with a second client are then skipped |
| `metadataKeyLimit(topic)` | none | the longest metadata key, in UTF-8 bytes; at least 127 |
| `missingStoreConfiguration()` | null | a store that does not exist; the test checks it fails and never reads as empty |
| `awaitPublished(client, topic)` | nothing | waits until what the client published to the topic is visible; a provider that buffers waits for its upload |

A provider on the Avro engine extends `AvroClientContractTest` (package `io.descoped.rawdata.tck.avro`) instead of
`RawdataClientContractTest`. It implements `awaitPublished` and adds the upload window's tests: closing on time and
on size, closing a producer, recovery after a crash, and the buffer folder's lock. Its `configuration()` gives each
call a `local-temp-folder` of its own, as each process has its own disk. The store also passes
`AvroStoreContractTest` with `createStore()` and, if it has one, `createMissingStore()`.

Each test gets a new client and a topic of its own, so the store may keep data between tests. Tests of your own only
add what the API cannot show: the SQL or the objects you write, your configuration and credentials. A behaviour an
application can see belongs in the shared tests, for every provider.
