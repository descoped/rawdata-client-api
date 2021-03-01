package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryRawdataProducer implements RawdataProducer {

    final ULID ulid = new ULID();

    final AtomicReference<ULID.Value> prevUlid = new AtomicReference<>(ulid.nextValue());

    final MemoryRawdataTopic topic;

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
    public void publish(RawdataMessage... messages) throws RawdataClosedException, RawdataNotBufferedException {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            for (RawdataMessage message : messages) {
                if (message == null) {
                    throw new NullPointerException("on of the messages was null");
                }
                ULID.Value ulid = message.ulid();
                if (ulid == null) {
                    ulid = RawdataProducer.nextMonotonicUlid(this.ulid, prevUlid.get());
                }
                prevUlid.set(ulid);
                topic.write(ulid, message);
            }
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        return CompletableFuture.runAsync(() -> publish(messages));
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
