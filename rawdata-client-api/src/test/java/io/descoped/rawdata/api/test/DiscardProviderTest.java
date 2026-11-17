package io.descoped.rawdata.api.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClosedException;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DiscardProviderTest {

    RawdataClient client;

    @BeforeEach
    void create() {
        client = RawdataClient.create(Map.of(RawdataClient.PROVIDER_KEY, "discard"));
    }

    @AfterEach
    void close() {
        client.close();
    }

    @Test
    void everythingIsAcceptedAndNothingKept() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(RawdataMessage.builder().position("p1").put("k", new byte[1]).build());
            producer.publishAsync(RawdataMessage.builder().position("p2").build()).get(5, TimeUnit.SECONDS);
            assertEquals("the-topic", producer.topic());
        }
        assertNull(client.lastMessage("the-topic"));
        assertNotNull(client.cursorOf("the-topic", RawdataIds.generate(), true));
        assertThrows(RawdataNoSuchPositionException.class, () -> client.cursorOf("the-topic", "p1", true, 0, Duration.ZERO));
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            assertNull(consumer.receive(50, TimeUnit.MILLISECONDS));
            consumer.seek(123);
            assertFalse(consumer.receiveAsync().isDone());
        }
        RawdataMetadataClient metadata = client.metadata("the-topic");
        metadata.put("k", new byte[]{1});
        assertEquals(Set.of(), metadata.keys());
        assertNull(metadata.get("k"));
    }

    @Test
    void receiveWaitsOutTheTimeoutAndCloseWakesIt() throws Exception {
        RawdataConsumer consumer = client.consumer("the-topic");
        long start = System.nanoTime();
        assertNull(consumer.receive(150, TimeUnit.MILLISECONDS));
        assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(140));
        CompletableFuture<Throwable> waiting = CompletableFuture.supplyAsync(() -> {
            try {
                consumer.receive(60, TimeUnit.SECONDS);
                return null;
            } catch (Throwable t) {
                return t;
            }
        });
        Thread.sleep(100);
        consumer.close();
        assertTrue(waiting.get(30, TimeUnit.SECONDS) instanceof RawdataClosedException);
    }

    @Test
    void topicNamesAndClosedStateAreChecked() {
        assertThrows(IllegalArgumentException.class, () -> client.producer("../x"));
        RawdataProducer producer = client.producer("t");
        client.close();
        assertTrue(producer.isClosed());
        assertThrows(RawdataClosedException.class, () -> producer.publish(RawdataMessage.builder().position("p").build()));
        assertThrows(RawdataClosedException.class, () -> client.producer("t"));
        assertThrows(RawdataClosedException.class, () -> client.lastMessage("t"));
    }
}
