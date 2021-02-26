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

    final Map<String, RawdataMessage.Builder> buffer = new ConcurrentHashMap<>();

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
        return RawdataMessage.builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        buffer.put(builder.position(), builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            for (String position : positions) {
                RawdataMessage.Builder builder = buffer.remove(position);
                if (builder == null) {
                    throw new RawdataNotBufferedException(String.format("position %s has not been buffered", position));
                }
                if (builder.ulid() == null) {
                    ULID.Value value = RawdataProducer.nextMonotonicUlid(ulid, prevUlid.get());
                    builder.ulid(value);
                }
                RawdataMessage message = builder.build();
                prevUlid.set(message.ulid());
                topic.write(message);
            }
        } finally {
            topic.unlock();
        }
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
