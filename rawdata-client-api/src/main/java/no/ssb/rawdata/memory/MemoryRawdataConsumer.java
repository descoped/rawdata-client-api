package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

class MemoryRawdataConsumer implements RawdataConsumer {

    final String subscription;
    final MemoryRawdataTopic topic;
    final AtomicReference<MemoryRawdataMessageId> position = new AtomicReference<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Consumer<MemoryRawdataConsumer> closeAction;

    MemoryRawdataConsumer(MemoryRawdataTopic topic, String subscription, Consumer<MemoryRawdataConsumer> closeAction) {
        this.subscription = subscription;
        this.topic = topic;
        this.closeAction = closeAction;
        topic.tryLock(5, TimeUnit.SECONDS);
        try {
            MemoryRawdataMessageId initialPosition = topic.getCheckpoint(subscription);
            if (initialPosition == null) {
                initialPosition = new MemoryRawdataMessageId(topic(), -1);
            }
            if (!topic.isLegalPosition(initialPosition)) {
                throw new IllegalArgumentException(String.format("the provided initial position %s is not legal in topic %s", initialPosition.index, topic.topic));
            }
            this.position.set(initialPosition);
        } finally {
            topic.unlock();
        }
    }

    @Override
    public String topic() {
        return topic.topic;
    }

    @Override
    public String subscription() {
        return subscription;
    }

    @Override
    public MemoryRawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
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
            return message;
        } finally {
            topic.unlock();
        }
    }

    @Override
    public CompletableFuture<MemoryRawdataMessage> receiveAsync() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public RawdataMessageId lastAcknowledgedMessageId() throws RawdataClosedException {
        return topic.getCheckpoint(subscription);
    }

    @Override
    public void acknowledgeAccumulative(RawdataMessageId id) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException();
        }
        topic.checkpoint(subscription, (MemoryRawdataMessageId) id);
    }

    @Override
    public String toString() {
        return "MemoryRawdataConsumer{" +
                "subscription='" + subscription + '\'' +
                ", position=" + position +
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
