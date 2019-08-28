package no.ssb.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryRawdataConsumer implements RawdataConsumer {

    final MemoryRawdataTopic topic;
    final Consumer<MemoryRawdataConsumer> closeAction;
    final AtomicReference<MemoryRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    MemoryRawdataConsumer(MemoryRawdataTopic topic, MemoryRawdataMessageId initialPosition, Consumer<MemoryRawdataConsumer> closeAction) {
        this.topic = topic;
        this.closeAction = closeAction;
        if (initialPosition == null) {
            initialPosition = new MemoryRawdataMessageId(topic(), new ULID.Value(0, 0));
        } else {
            topic.tryLock(5, TimeUnit.SECONDS);
            try {
                if (!topic.isLegalPosition(initialPosition)) {
                    throw new IllegalArgumentException(String.format("the provided initial position %s is not legal in topic %s", initialPosition.ulid, topic.topic));
                }
            } finally {
                topic.unlock();
            }
        }
        this.position.set(initialPosition);
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public MemoryRawdataMessageContent receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            while (!topic.hasNext(position.get())) {
                if (isClosed()) {
                    throw new RawdataClosedException();
                }
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                topic.awaitProduction(durationNano, TimeUnit.NANOSECONDS);
            }
            MemoryRawdataMessage message = topic.readNext(position.get());
            position.set(message.id());
            return message.content();
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<MemoryRawdataMessageContent> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            position.set(topic.findPositionOfTimestamp(timestamp));
        } finally {
            topic.unlock();
        }
    }

    @Override
    public String toString() {
        return "MemoryRawdataConsumer{" +
                "position=" + position +
                ", closed=" + closed +
                '}';
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closeAction.accept(this);
        closed.set(true);
        if (topic.tryLock()) {
            try {
                topic.signalProduction();
            } finally {
                topic.unlock();
            }
        }
    }
}
