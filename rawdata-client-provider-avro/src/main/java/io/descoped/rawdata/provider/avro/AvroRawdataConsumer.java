package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reads a topic segment by segment. When the next segment does not exist yet, receive looks for it at the poll
 * interval until the timeout; a publish through the same client wakes it at once. A start position (a cursor or a
 * seek) is a lower bound on the ids: the consumer finds the first segment that reaches it and skips the messages
 * before it. One receive runs at a time; others, and asynchronous receives, wait their turn.
 */
final class AvroRawdataConsumer implements RawdataConsumer {

    private final AvroRawdataClient client;
    private final String topic;
    private final long pollIntervalNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong wakeUps = new AtomicLong();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wakeUp = lock.newCondition();
    private final Deque<RawdataMessage> buffer = new ArrayDeque<>(); // guarded by lock
    private long next = -1; // guarded by lock: the segment to read next; -1 until the start is found
    private UUID from; // guarded by lock: the lower bound on ids, null once passed
    private boolean fromInclusive; // guarded by lock

    AvroRawdataConsumer(AvroRawdataClient client, String topic, AvroCursor start, long pollIntervalMillis) {
        this.client = client;
        this.topic = topic;
        this.pollIntervalNanos = TimeUnit.MILLISECONDS.toNanos(pollIntervalMillis);
        if (start == null) {
            next = 0;
        } else {
            from = start.id;
            fromInclusive = start.inclusive;
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        checkOpen();
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            for (; ; ) {
                checkOpen();
                long wakeUpsBefore = wakeUps.get();
                if (buffer.isEmpty()) {
                    fill();
                }
                RawdataMessage message = buffer.pollFirst();
                if (message != null) {
                    return message;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                if (wakeUps.get() != wakeUpsBefore) {
                    continue; // a publish came while this receive was reading
                }
                if (wakeUp.awaitNanos(Math.min(remaining, pollIntervalNanos)) > 0 && isClosed()) {
                    throw closedException(); // woken early by close
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads segments into the buffer until it holds a message or there is no next segment. Called with the lock held.
     */
    private void fill() {
        try {
            if (next < 0) {
                next = client.firstSegmentReaching(topic, from);
            }
            while (buffer.isEmpty()) {
                List<RawdataMessage> messages = client.segment(topic, next);
                if (messages == null) {
                    return;
                }
                next++;
                for (RawdataMessage message : messages) {
                    if (from == null) {
                        buffer.addLast(message);
                    } else {
                        int order = RawdataIds.ORDER.compare(message.id(), from);
                        if (order > 0 || order == 0 && fromInclusive) {
                            buffer.addLast(message);
                            from = null; // ids only grow from here
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            if (isClosed()) {
                throw closedException(); // the client closed its store during the read
            }
            throw e;
        }
    }

    @Override
    public CompletableFuture<RawdataMessage> receiveAsync() {
        checkOpen();
        try {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    for (; ; ) {
                        RawdataMessage message = receive(1, TimeUnit.SECONDS);
                        if (message != null) {
                            return message;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // closing the client interrupts its threads
                    throw new CompletionException(isClosed() ? closedException() : e);
                }
            }, client.executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closedException());
        }
    }

    @Override
    public void seek(long timestamp) {
        checkOpen();
        lock.lock();
        try {
            buffer.clear();
            next = -1;
            from = RawdataIds.beginningOf(timestamp);
            fromInclusive = true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get() || client.isClosed();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        client.removed(this);
        wake();
    }

    /**
     * Wakes a receive that waits between polls, to look for a new segment or to see that the consumer or its client
     * is closed. A receive that is reading holds the lock; it sees the wake-up count change and looks again.
     */
    void wake() {
        wakeUps.incrementAndGet();
        if (lock.tryLock()) {
            try {
                wakeUp.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void checkOpen() {
        if (isClosed()) {
            throw closedException();
        }
    }

    private RawdataClosedException closedException() {
        return new RawdataClosedException("consumer of topic " + topic + " is closed");
    }

    @Override
    public String toString() {
        return "AvroRawdataConsumer{topic=" + topic + ", closed=" + isClosed() + '}';
    }
}
