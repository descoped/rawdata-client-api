package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessageId;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class MemoryRawdataConsumer implements RawdataConsumer {

    final String subscription;
    final MemoryRawdataTopic topic;
    final AtomicReference<MemoryRawdataMessageId> position = new AtomicReference<>();

    MemoryRawdataConsumer(MemoryRawdataTopic topic, String subscription, MemoryRawdataMessageId initialPosition) {
        this.subscription = subscription;
        this.topic = topic;
        topic.tryLock();
        try {
            if (!topic.isLegalPosition(initialPosition)) {
                throw new IllegalArgumentException(String.format("the provided initial position %s is not legal in topic %s", initialPosition.index, topic.topic));
            }
        } finally {
            topic.unlock();
        }
        this.position.set(initialPosition);
    }

    @Override
    public MemoryRawdataMessage receive(long timeout, TimeUnit unit) throws InterruptedException {
        long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
        topic.tryLock();
        try {
            while (!topic.hasNext(position.get())) {
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                topic.awaitProduction(durationNano, TimeUnit.NANOSECONDS);
            }
            MemoryRawdataMessage message = topic.readNext(position.get());
            position.set(message.id()); // auto-acknowledge
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
    public void acknowledgeAccumulative(RawdataMessageId id) {
        // do nothing due to auto-acknowledge feature of receive()
    }

    @Override
    public String toString() {
        return "MemoryRawdataConsumer{" +
                "name='" + subscription + '\'' +
                ", position=" + position.get() +
                '}';
    }

    @Override
    public void close() {
    }
}
