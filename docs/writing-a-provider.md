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
numbered segment only if it does not exist. For a store of files or objects, implement `AvroStore` (in
`io.descoped.rawdata.provider.avro`) and give it to `AvroRawdataClient`: it needs an atomic "create this name if it
is free", reads, and metadata entries. The [`gcs`](providers/gcs.md) provider is such a store.

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
