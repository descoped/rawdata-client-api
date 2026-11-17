package io.descoped.rawdata.memory;

import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * One topic: messages by id and the metadata entries. The messages are guarded by the lock; consumers wait on the
 * condition, which producers and closing signal.
 */
final class MemoryRawdataTopic {

    final String name;
    final Map<String, byte[]> metadata = new ConcurrentHashMap<>(); // own copies
    private final NavigableMap<UUID, RawdataMessage> messages = new TreeMap<>(RawdataIds.ORDER);
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition published = lock.newCondition();

    MemoryRawdataTopic(String name) {
        this.name = name;
    }

    /**
     * Writes messages under one lock, so a batch is atomic and generated ids follow the topic's last id.
     */
    void publish(RawdataMessage[] batch) {
        lock.lock();
        try {
            UUID previous = messages.isEmpty() ? RawdataIds.beginningOfTime() : messages.lastKey();
            RawdataMessage[] stored = new RawdataMessage[batch.length];
            for (int i = 0; i < batch.length; i++) {
                RawdataMessage message = batch[i];
                UUID id = message.id();
                if (id == null) {
                    id = RawdataIds.next(previous);
                    stored[i] = message.copy().id(id).build();
                } else {
                    stored[i] = message;
                }
                if (RawdataIds.ORDER.compare(id, previous) > 0) {
                    previous = id;
                }
            }
            for (RawdataMessage message : stored) {
                messages.put(message.id(), message);
            }
            published.signalAll();
        } finally {
            lock.unlock();
        }
    }

    RawdataMessage lastMessage() {
        lock.lock();
        try {
            return messages.isEmpty() ? null : messages.lastEntry().getValue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the first message after the cursor, waiting up to the timeout. Returns null on timeout or when the
     * closed check says so (the caller then throws).
     */
    RawdataMessage next(MemoryCursor cursor, long timeoutNanos, BooleanSupplier closed) throws InterruptedException {
        long deadline = System.nanoTime() + timeoutNanos;
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                if (closed.getAsBoolean()) {
                    return null;
                }
                Map.Entry<UUID, RawdataMessage> entry = cursor.inclusive
                        ? messages.ceilingEntry(cursor.startKey)
                        : messages.higherEntry(cursor.startKey);
                if (entry != null) {
                    return entry.getValue();
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                published.awaitNanos(remaining);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wakes every waiting consumer, so a closed one can notice.
     */
    void wakeUp() {
        lock.lock();
        try {
            published.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The id of the first message with the position in {@code [lowerBound, upperBound)}, or null.
     */
    UUID idOf(String position, UUID lowerBound, UUID upperBound) {
        lock.lock();
        try {
            for (Map.Entry<UUID, RawdataMessage> entry : messages.subMap(lowerBound, true, upperBound, false).entrySet()) {
                if (position.equals(entry.getValue().position())) {
                    return entry.getKey();
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
