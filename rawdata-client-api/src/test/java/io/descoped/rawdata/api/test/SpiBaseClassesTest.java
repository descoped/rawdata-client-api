package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.spi.OrderedRawdataProducer;
import io.descoped.rawdata.api.spi.PollingRawdataConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider base classes of {@code io.descoped.rawdata.api.spi}, with stores made up in the test.
 */
@Timeout(60)
class SpiBaseClassesTest {

    private static final long HOUR = TimeUnit.HOURS.toMillis(1);

    private final ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void stopExecutor() {
        executor.shutdownNow();
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().id(RawdataIds.generate()).position(position).build();
    }

    /**
     * A consumer on a queue of messages that the test fills; reading takes them from it.
     */
    private class QueueConsumer extends PollingRawdataConsumer {

        final ConcurrentLinkedQueue<RawdataMessage> store = new ConcurrentLinkedQueue<>();
        final AtomicInteger fills = new AtomicInteger();
        final AtomicBoolean clientClosed = new AtomicBoolean();
        final AtomicInteger closedCalls = new AtomicInteger();
        final List<Long> repositions = new ArrayList<>();
        volatile Runnable duringFill = () -> {
        };
        volatile RuntimeException failure;
        volatile Executor asyncExecutor = executor;

        QueueConsumer(long pollIntervalMillis) {
            super("t", pollIntervalMillis);
        }

        @Override
        protected void fill(Deque<RawdataMessage> buffer) {
            fills.incrementAndGet();
            if (failure != null) {
                throw failure;
            }
            RawdataMessage message;
            while ((message = store.poll()) != null) {
                buffer.addLast(message);
            }
            duringFill.run();
        }

        @Override
        protected void reposition(long timestamp) {
            repositions.add(timestamp);
        }

        @Override
        protected boolean isClientClosed() {
            return clientClosed.get();
        }

        @Override
        protected Executor executor() {
            return asyncExecutor;
        }

        @Override
        protected void closed() {
            closedCalls.incrementAndGet();
        }
    }

    @Test
    void receiveReturnsWhatFillReadsInOrder() throws Exception {
        QueueConsumer consumer = new QueueConsumer(10);
        RawdataMessage a = message("a");
        RawdataMessage b = message("b");
        consumer.store.add(a);
        consumer.store.add(b);
        assertSame(a, consumer.receive(1, TimeUnit.SECONDS));
        assertSame(b, consumer.receive(1, TimeUnit.SECONDS));
        assertEquals(1, consumer.fills.get(), "the second message came from the buffer");
        assertNull(consumer.receive(50, TimeUnit.MILLISECONDS));
        assertEquals("t", consumer.topic());
    }

    /**
     * A wake-up that comes while a receive reads, after the read found nothing, is not lost: the receive reads again
     * at once instead of waiting for the poll interval (an hour here).
     */
    @Test
    void aWakeUpDuringARead() throws Exception {
        QueueConsumer consumer = new QueueConsumer(HOUR);
        RawdataMessage late = message("late");
        consumer.duringFill = () -> {
            if (consumer.fills.get() == 1) {
                consumer.store.add(late);
                consumer.wake();
            }
        };
        long start = System.nanoTime();
        assertSame(late, consumer.receive(30, TimeUnit.SECONDS));
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
    }

    @Test
    void aWakeUpEndsTheWait() throws Exception {
        QueueConsumer consumer = new QueueConsumer(HOUR);
        CompletableFuture<RawdataMessage> received = consumer.receiveAsync();
        while (consumer.fills.get() == 0) {
            Thread.onSpinWait();
        }
        RawdataMessage message = message("a");
        consumer.store.add(message);
        consumer.wake();
        assertSame(message, received.get(10, TimeUnit.SECONDS));
    }

    @Test
    void closeWakesAWaitingReceiveAndFailsAsynchronousOnes() throws Exception {
        QueueConsumer consumer = new QueueConsumer(HOUR);
        CompletableFuture<Throwable> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                consumer.receive(60, TimeUnit.SECONDS);
                return null;
            } catch (Throwable t) {
                return t;
            }
        }, executor);
        CompletableFuture<RawdataMessage> pending = consumer.receiveAsync();
        while (consumer.fills.get() < 2) {
            Thread.onSpinWait();
        }
        consumer.close();
        consumer.close();
        assertInstanceOf(RawdataClosedException.class, waiting.get(10, TimeUnit.SECONDS));
        ExecutionException failed = assertThrows(ExecutionException.class, () -> pending.get(10, TimeUnit.SECONDS));
        assertInstanceOf(RawdataClosedException.class, failed.getCause());
        assertEquals(1, consumer.closedCalls.get());
        assertTrue(consumer.isClosed());
        assertThrows(RawdataClosedException.class, () -> consumer.receive(0, TimeUnit.SECONDS));
        assertThrows(RawdataClosedException.class, () -> consumer.seek(0));
        assertThrows(RawdataClosedException.class, consumer::receiveAsync);
    }

    @Test
    void aClosedClientClosesTheConsumer() {
        QueueConsumer consumer = new QueueConsumer(10);
        consumer.clientClosed.set(true);
        assertTrue(consumer.isClosed());
        assertThrows(RawdataClosedException.class, () -> consumer.receive(0, TimeUnit.SECONDS));
    }

    @Test
    void anExecutorThatRefusesFailsTheAsynchronousReceive() {
        QueueConsumer consumer = new QueueConsumer(10);
        consumer.asyncExecutor = task -> {
            throw new RejectedExecutionException("shut down");
        };
        ExecutionException failed = assertThrows(ExecutionException.class, () -> consumer.receiveAsync().get());
        assertInstanceOf(RawdataClosedException.class, failed.getCause());
    }

    @Test
    void seekClearsTheBufferAndRepositions() throws Exception {
        QueueConsumer consumer = new QueueConsumer(10);
        consumer.store.add(message("a"));
        consumer.store.add(message("b"));
        consumer.receive(1, TimeUnit.SECONDS); // "b" is left in the buffer
        consumer.seek(42);
        assertEquals(List.of(42L), consumer.repositions);
        assertNull(consumer.receive(50, TimeUnit.MILLISECONDS), "the buffered message is gone");
    }

    @Test
    void aFailedReadIsReportedOrSeenAsClosed() {
        QueueConsumer consumer = new QueueConsumer(10);
        IllegalStateException failure = new IllegalStateException("store failed");
        consumer.failure = failure;
        assertSame(failure, assertThrows(IllegalStateException.class, () -> consumer.receive(0, TimeUnit.SECONDS)));
        QueueConsumer closedDuringRead = new QueueConsumer(10) {
            @Override
            protected void fill(Deque<RawdataMessage> buffer) {
                clientClosed.set(true); // the client closes its store while the read runs
                throw new IllegalStateException("pool closed");
            }
        };
        assertThrows(RawdataClosedException.class, () -> closedDuringRead.receive(0, TimeUnit.SECONDS));
    }

    @Test
    void thePollIntervalMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> new QueueConsumer(0));
    }

    /**
     * A producer that keeps what it writes in a list, after the last id of the list.
     */
    private class ListProducer extends OrderedRawdataProducer {

        final List<RawdataMessage> store = new ArrayList<>();
        final AtomicBoolean clientClosed = new AtomicBoolean();
        final AtomicInteger closedCalls = new AtomicInteger();
        volatile Executor asyncExecutor = executor;

        ListProducer() {
            super("t");
        }

        @Override
        protected synchronized void write(RawdataMessage[] batch) {
            UUID last = store.isEmpty() ? RawdataIds.beginningOfTime() : store.get(store.size() - 1).id();
            store.addAll(withIds(batch, last));
        }

        @Override
        protected boolean isClientClosed() {
            return clientClosed.get();
        }

        @Override
        protected Executor executor() {
            return asyncExecutor;
        }

        @Override
        protected void closed() {
            closedCalls.incrementAndGet();
        }
    }

    @Test
    void idsAreGivenAfterTheLastAndGivenIdsAreChecked() {
        ListProducer producer = new ListProducer();
        producer.publish(RawdataMessage.builder().position("a").build(), RawdataMessage.builder().position("b").build());
        UUID given = RawdataIds.next(producer.store.get(1).id());
        producer.publish(RawdataMessage.builder().id(given).position("c").build());
        assertEquals(3, producer.store.size());
        assertEquals(given, producer.store.get(2).id());
        for (int i = 1; i < producer.store.size(); i++) {
            assertTrue(RawdataIds.ORDER.compare(producer.store.get(i - 1).id(), producer.store.get(i).id()) < 0);
        }
        RawdataMessage repeated = RawdataMessage.builder().id(given).position("d").build();
        assertThrows(IllegalArgumentException.class, () -> producer.publish(repeated));
        assertEquals(3, producer.store.size());
        assertEquals("t", producer.topic());
    }

    @Test
    void publishChecksItsArguments() {
        ListProducer producer = new ListProducer();
        RawdataMessage[] none = null;
        assertThrows(NullPointerException.class, () -> producer.publish(none));
        RawdataMessage a = message("a");
        assertThrows(NullPointerException.class, () -> producer.publish(a, null));
        producer.publish();
        assertEquals(List.of(), producer.store);
    }

    @Test
    void asynchronousPublishRunsOnTheExecutor() throws Exception {
        ListProducer producer = new ListProducer();
        producer.publishAsync(message("a")).get(10, TimeUnit.SECONDS);
        assertEquals(1, producer.store.size());
        producer.asyncExecutor = task -> {
            throw new RejectedExecutionException("shut down");
        };
        ExecutionException failed = assertThrows(ExecutionException.class, () -> producer.publishAsync(message("b")).get());
        assertInstanceOf(RawdataClosedException.class, failed.getCause());
    }

    @Test
    void aClosedProducerRefusesToPublish() {
        ListProducer producer = new ListProducer();
        producer.close();
        producer.close();
        assertEquals(1, producer.closedCalls.get());
        assertTrue(producer.isClosed());
        RawdataMessage a = message("a");
        assertThrows(RawdataClosedException.class, () -> producer.publish(a));
        ListProducer ofClosedClient = new ListProducer();
        ofClosedClient.clientClosed.set(true);
        assertTrue(ofClosedClient.isClosed());
        assertThrows(RawdataClosedException.class, () -> ofClosedClient.publish(a));
    }
}
