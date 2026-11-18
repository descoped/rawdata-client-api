package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reads a topic in batches of up to the prefetch size. When the topic has nothing new, receive polls it at the poll
 * interval until the timeout. One receive runs at a time; others, and asynchronous receives, wait their turn.
 */
final class PostgresRawdataConsumer implements RawdataConsumer {

    private final PostgresRawdataClient client;
    private final String topic;
    private final int prefetchSize;
    private final long pollIntervalNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wakeUp = lock.newCondition();
    private final Deque<RawdataMessage> buffer = new ArrayDeque<>(); // guarded by lock
    private PostgresCursor position; // guarded by lock: where the next fetch starts

    PostgresRawdataConsumer(PostgresRawdataClient client, String topic, PostgresCursor start, int prefetchSize, long pollIntervalMillis) {
        this.client = client;
        this.topic = topic;
        this.prefetchSize = prefetchSize;
        this.pollIntervalNanos = TimeUnit.MILLISECONDS.toNanos(pollIntervalMillis);
        this.position = start != null ? start : new PostgresCursor(RawdataIds.beginningOfTime(), true);
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
                if (wakeUp.awaitNanos(Math.min(remaining, pollIntervalNanos)) > 0 && isClosed()) {
                    throw closedException(); // woken early by close
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fetches the next batch after the position into the buffer. Called with the lock held.
     */
    private void fill() {
        PostgresCursor from = position;
        try {
            buffer.addAll(client.jdbc().transaction(connection ->
                    PostgresMessages.after(connection, topic, from.id, from.inclusive, prefetchSize)));
        } catch (RuntimeException e) {
            if (isClosed()) {
                throw closedException(); // the client closed its pool during the fetch
            }
            throw e;
        }
        if (!buffer.isEmpty()) {
            position = new PostgresCursor(buffer.peekLast().id(), false);
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
            position = new PostgresCursor(RawdataIds.beginningOf(timestamp), true);
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
     * Wakes a receive that waits between polls, so it sees that the consumer or its client is closed. A receive that
     * is fetching holds the lock and sees the close when the fetch returns, so close does not wait for it.
     */
    void wake() {
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
        return "PostgresRawdataConsumer{topic=" + topic + ", closed=" + isClosed() + '}';
    }
}
