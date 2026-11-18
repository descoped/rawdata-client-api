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
  `withIds`, and write the result, all or nothing, after everything in the topic.
- `PollingRawdataConsumer`: the receive loop, waking, `receiveAsync`, `seek` and closing. You implement
  `fill(buffer)`, which reads the messages after the last one read, and `reposition(timestamp)`.

The hard part is the order across processes: two writers must not both write after the same last id. The `postgres`
provider holds a per-topic advisory lock while it reads the last id and writes; the Avro engine creates the next
numbered segment only if it does not exist. For a store of files or objects, implement `AvroStore` instead; see
below.

## A store of files or objects: `AvroStore`

`rawdata-client-provider-avro` has the whole client for stores that keep files or objects: `AvroRawdataClient`. It
handles segments, ids, ordering, consumers and metadata. A provider only implements `AvroStore`
(`io.descoped.rawdata.provider.avro`) and passes it to the client:

```java
@Override
public RawdataClient initialize(Map<String, String> configuration) {
    String bucket = AvroConfiguration.required(configuration, "mystore.bucket");
    long pollInterval = AvroConfiguration.positive(configuration, "mystore.poll-interval-ms", 1000);
    return new AvroRawdataClient(new MyAvroStore(bucket), pollInterval);
}
```

| Method | Must |
|---|---|
| `create(topic, segment, bytes)` | write the segment **only if it does not exist**, atomically. Return false, writing nothing, if it exists. This is what keeps the order across processes. |
| `read(topic, segment)` | return the whole segment, or empty |
| `exists(topic, segment)` | say whether it exists; it is called often, so make it cheap (a HEAD, a stat) |
| `metadataNames`, `readMetadata`, `writeMetadata`, `deleteMetadata` | keep a topic's metadata entries. A write replaces the value atomically, and a delete of a missing entry does nothing. |
| `close()` | release clients and pools; called once, by the client |

What to watch:
- **Conditional create is the contract.** It is a hard link on a file system, `ifGenerationMatch=0` on GCS, and
  `If-None-Match: *` on S3. An "upload, then check" is not enough: two writers would both win.
- **Retries after a lost response.** A client that retries a create after the response was lost sees its own
  segment and a failed condition. Compare the stored bytes with your own: an Avro file carries a random sync marker,
  so equal bytes mean it was your write. Return true then.
- **Names.** Topic names reach the store validated and are case-sensitive. Metadata names are `k` plus lowercase hex.
  Refuse a name that is too long for the store with `IllegalArgumentException`.
- **Errors.** Throw `IOException` for store failures; the client wraps them.

The [`filesystem`](providers/filesystem.md) and [`gcs`](providers/gcs.md) stores are short examples.

## The contract tests

Add `rawdata-client-tck` as a test dependency and extend `RawdataClientContractTest` with your configuration:

```java
class MyStoreContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return Map.of(RawdataClient.PROVIDER_KEY, "mystore", "mystore.url", TestStore.url());
    }
}
```

Each test gets a new client and a topic of its own, so the store may keep data between tests. Every provider in this
repository passes all of them; add tests of your own for what the contract cannot see, such as several clients on
one store writing to the same topic.
