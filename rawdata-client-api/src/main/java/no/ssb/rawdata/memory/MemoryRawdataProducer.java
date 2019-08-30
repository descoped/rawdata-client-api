package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryRawdataProducer implements RawdataProducer {

    final ULID ulid = new ULID();

    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    final MemoryRawdataTopic topic;

    final Map<String, MemoryRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

    final AtomicBoolean closed = new AtomicBoolean(false);

    Consumer<MemoryRawdataProducer> closeAction;

    MemoryRawdataProducer(MemoryRawdataTopic topic, Consumer<MemoryRawdataProducer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return new MemoryRawdataMessage.Builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        MemoryRawdataMessage.Builder builder = (MemoryRawdataMessage.Builder) _builder;
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            for (String position : positions) {
                MemoryRawdataMessage.Builder builder = buffer.remove(position);
                if (builder == null) {
                    throw new RawdataNotBufferedException(String.format("position %s has not been buffered", position));
                }
                if (builder.ulid == null) {
                    ULID.Value value = nextMonotonicUlid(ulid, prevUlid.get());
                    builder.ulid(value);
                }
                MemoryRawdataMessage message = builder.build();
                prevUlid.set(message.ulid());
                topic.write(message);
            }
        } finally {
            topic.unlock();
        }
    }

    static ULID.Value nextMonotonicUlid(ULID generator, ULID.Value previousUlid) {
        // spin until time ticks
        ULID.Value value;
        do {
            value = generator.nextStrictlyMonotonicValue(previousUlid, System.currentTimeMillis()).orElse(null);
        } while (value == null);
        if (previousUlid.timestamp() != value.timestamp()) {
            // start at lsb 1, to avoid inclusive/exclusive semantics when searching
            value = new ULID.Value((value.timestamp() << 16) & 0xFFFFFFFFFFFF0000L, 1L);
        }
        return value;
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(positions));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeAction.accept(this);
        closed.set(true);
    }
}
