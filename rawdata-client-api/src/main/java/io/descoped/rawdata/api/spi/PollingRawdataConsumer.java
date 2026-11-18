package io.descoped.rawdata.api.spi;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A consumer for providers that read their store in batches and poll it for new messages: the receive loop, waking,
 * asynchronous receive and closing, the same in every such provider. A provider says how to read the next batch
 * ({@link #fill}) and how to move to a time ({@link #reposition}).
 * <p>
 * One receive or seek runs at a time; others, and asynchronous receives, wait their turn. A receive that finds
 * nothing waits for the poll interval, or until {@link #wake()}: a publish through the same client, or a close. The
 * wait does not hold the turn, and a wake-up that comes while the receive reads is not lost.
 */
public abstract class PollingRawdataConsumer implements RawdataConsumer {

    private final String topic;
    private final long pollIntervalNanos;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong wakeUps = new AtomicLong();
    private final Object signal = new Object();
    private final ReentrantLock turn = new ReentrantLock();
    private final Deque<RawdataMessage> buffer = new ArrayDeque<>(); // guarded by turn

    /**
     * Creates the consumer.
     *
     * @param topic              the topic
     * @param pollIntervalMillis how long a receive that finds nothing waits before it reads again, 1 or more
     */
    protected PollingRawdataConsumer(String topic, long pollIntervalMillis) {
        if (pollIntervalMillis < 1) {
            throw new IllegalArgumentException("The poll interval must be 1 ms or more: " + pollIntervalMillis);
        }
        this.topic = topic;
        this.pollIntervalNanos = TimeUnit.MILLISECONDS.toNanos(pollIntervalMillis);
    }

    /**
     * Reads the messages after the last one read, if there are any, into the buffer, in order. Called with the turn
     * held, when the buffer is empty.
     *
     * @param buffer where the messages go
     */
    protected abstract void fill(Deque<RawdataMessage> buffer);

    /**
     * Moves to the first message written at or after a time. Called with the turn held, after the buffer is cleared.
     *
     * @param timestamp the time, milliseconds since 1970-01-01T00:00Z
     */
    protected abstract void reposition(long timestamp);

    /**
     * Whether the client that created this consumer is closed.
     *
     * @return true if it is
     */
    protected abstract boolean isClientClosed();

    /**
     * The executor of asynchronous receives: the client's, which fails its tasks when the client closes.
     *
     * @return the executor
     */
    protected abstract Executor executor();

    /**
     * Called once, when the consumer closes, before waiting receives are woken; for example, to tell the client.
     */
    protected void closed() {
        // nothing by default
    }

    @Override
    public final String topic() {
        return topic;
    }

    @Override
    public final RawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException, RawdataClosedException {
        checkOpen();
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        turn.lockInterruptibly();
        try {
            for (; ; ) {
                checkOpen();
                long wakeUpsBefore = wakeUps.get();
                RawdataMessage message = next();
                if (message != null) {
                    return message;
                }
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return null;
                }
                turn.unlock();
                try {
                    await(wakeUpsBefore, Math.min(remaining, pollIntervalNanos));
                } finally {
                    turn.lock();
                }
            }
        } finally {
            turn.unlock();
        }
    }

    private RawdataMessage next() {
        if (buffer.isEmpty()) {
            try {
                fill(buffer);
            } catch (RuntimeException e) {
                if (isClosed()) {
                    throw closedException(); // the client closed its store during the read
                }
                throw e;
            }
        }
        return buffer.pollFirst();
    }

    private void await(long wakeUpsBefore, long nanos) throws InterruptedException {
        synchronized (signal) {
            if (wakeUps.get() == wakeUpsBefore && !isClosed()) {
                TimeUnit.NANOSECONDS.timedWait(signal, nanos);
            }
        }
    }

    /**
     * Wakes receives that wait, to read again or to see that the consumer or its client is closed. Never blocks.
     */
    public final void wake() {
        wakeUps.incrementAndGet();
        synchronized (signal) {
            signal.notifyAll();
        }
    }

    @Override
    public final CompletableFuture<RawdataMessage> receiveAsync() {
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
            }, executor());
        } catch (RejectedExecutionException e) {
            return CompletableFuture.failedFuture(closedException());
        }
    }

    @Override
    public final void seek(long timestamp) {
        checkOpen();
        turn.lock();
        try {
            buffer.clear();
            reposition(timestamp);
        } finally {
            turn.unlock();
        }
    }

    @Override
    public final boolean isClosed() {
        return closed.get() || isClientClosed();
    }

    @Override
    public final void close() {
        if (closed.compareAndSet(false, true)) {
            closed();
            wake();
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
        return getClass().getSimpleName() + "{topic=" + topic + ", closed=" + isClosed() + '}';
    }
}
