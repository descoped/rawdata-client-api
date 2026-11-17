package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCopy;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RawdataCopyTest {

    private static RawdataClient memory() {
        return RawdataClient.create(Map.of(RawdataClient.PROVIDER_KEY, "memory"));
    }

    private static void fill(RawdataClient client, String topic, int count) {
        try (RawdataProducer producer = client.producer(topic)) {
            for (int i = 0; i < count; i++) {
                producer.publish(RawdataMessage.builder().position("p" + i).put("v", ("value " + i).getBytes(StandardCharsets.UTF_8)).build());
            }
        }
    }

    private static List<RawdataMessage> readAll(RawdataClient client, String topic) throws InterruptedException {
        List<RawdataMessage> messages = new ArrayList<>();
        try (RawdataConsumer consumer = client.consumer(topic)) {
            RawdataMessage message;
            while ((message = consumer.receive(0, TimeUnit.SECONDS)) != null) {
                messages.add(message);
            }
        }
        return messages;
    }

    @Test
    void copiesATopicBetweenClientsKeepingIdsAndOrder() throws Exception {
        try (RawdataClient source = memory(); RawdataClient target = memory()) {
            fill(source, "t", 1000);
            try (RawdataConsumer from = source.consumer("t"); RawdataProducer to = target.producer("t")) {
                assertEquals(1000, RawdataCopy.copy(from, to, 64, Duration.ZERO));
            }
            assertEquals(readAll(source, "t"), readAll(target, "t"));
        }
    }

    @Test
    void publishesInBatchesOfTheChosenSize() throws Exception {
        try (RawdataClient source = memory(); RawdataClient target = memory()) {
            fill(source, "t", 25);
            List<Integer> batches = new ArrayList<>();
            RawdataProducer real = target.producer("t");
            RawdataProducer counting = new ForwardingProducer(real) {
                @Override
                public void publish(RawdataMessage... messages) {
                    batches.add(messages.length);
                    super.publish(messages);
                }
            };
            try (RawdataConsumer from = source.consumer("t")) {
                assertEquals(25, RawdataCopy.copy(from, counting, 10, Duration.ZERO));
            }
            assertEquals(List.of(10, 10, 5), batches);
        }
    }

    @Test
    void waitsForMessagesUntilTheIdleTimeout() throws Exception {
        try (RawdataClient source = memory(); RawdataClient target = memory()) {
            fill(source, "t", 3);
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                fill(source, "t", 2);
            });
            try (RawdataConsumer from = source.consumer("t"); RawdataProducer to = target.producer("t")) {
                assertEquals(5, RawdataCopy.copy(from, to, 100, Duration.ofSeconds(1)));
            }
        }
    }

    @Test
    void aFailingPublishStopsTheCopy() throws Exception {
        try (RawdataClient source = memory(); RawdataClient target = memory()) {
            fill(source, "t", 50);
            AtomicInteger calls = new AtomicInteger();
            RawdataProducer failing = new ForwardingProducer(target.producer("t")) {
                @Override
                public void publish(RawdataMessage... messages) {
                    if (calls.incrementAndGet() == 3) {
                        throw new IllegalStateException("store down");
                    }
                    super.publish(messages);
                }
            };
            try (RawdataConsumer from = source.consumer("t")) {
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> RawdataCopy.copy(from, failing, 10, Duration.ZERO));
                assertEquals("store down", e.getMessage());
            }
            assertEquals(20, readAll(target, "t").size());
        }
    }

    @Test
    void invalidArgumentsAreRefused() {
        try (RawdataClient client = memory(); RawdataConsumer from = client.consumer("t"); RawdataProducer to = client.producer("u")) {
            assertThrows(IllegalArgumentException.class, () -> RawdataCopy.copy(from, to, 0, Duration.ZERO));
            assertThrows(IllegalArgumentException.class, () -> RawdataCopy.copy(from, to, 1, Duration.ofSeconds(-1)));
            assertThrows(NullPointerException.class, () -> RawdataCopy.copy(null, to, 1, Duration.ZERO));
            assertThrows(NullPointerException.class, () -> RawdataCopy.copy(from, null, 1, Duration.ZERO));
        }
    }

    @Test
    void receiveUpToTakesWhatIsAvailable() throws Exception {
        try (RawdataClient client = memory()) {
            fill(client, "t", 7);
            try (RawdataConsumer consumer = client.consumer("t")) {
                assertEquals(5, consumer.receiveUpTo(5, Duration.ZERO).size());
                List<RawdataMessage> rest = consumer.receiveUpTo(5, Duration.ZERO);
                assertEquals(List.of("p5", "p6"), rest.stream().map(RawdataMessage::position).toList());
                long start = System.nanoTime();
                assertTrue(consumer.receiveUpTo(5, Duration.ofMillis(100)).isEmpty());
                assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(90));
            }
        }
    }

    private static class ForwardingProducer implements RawdataProducer {
        private final RawdataProducer delegate;

        ForwardingProducer(RawdataProducer delegate) {
            this.delegate = delegate;
        }

        @Override
        public String topic() {
            return delegate.topic();
        }

        @Override
        public void publish(RawdataMessage... messages) {
            delegate.publish(messages);
        }

        @Override
        public CompletableFuture<Void> publishAsync(RawdataMessage... messages) {
            return delegate.publishAsync(messages);
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
