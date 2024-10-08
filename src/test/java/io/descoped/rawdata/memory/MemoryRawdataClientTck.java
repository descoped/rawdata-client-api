package io.descoped.rawdata.memory;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataNoSuchPositionException;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class MemoryRawdataClientTck {

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() {
        client = ProviderConfigurator.configure(Map.of(), "memory", RawdataClientInitializer.class);
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatLastPositionOfEmptyTopicCanBeRead() {
        assertNull(client.lastMessage("the-topic"));
    }

    @Test
    public void thatLastPositionOfProducerCanBeRead() {
        RawdataProducer producer = client.producer("the-topic");

        producer.publish(
                RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build()
        );

        assertEquals(client.lastMessage("the-topic").position(), "b");

        producer.publish(RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());

        assertEquals(client.lastMessage("the-topic").position(), "c");
    }

    @Test
    public void thatAllFieldsOfMessageSurvivesStream() throws Exception {
        ULID.Value ulid = new ULID().nextValue();
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().ulid(ulid).orderingGroup("og1").sequenceNumber(1).position("a").put("payload1", new byte[3]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().orderingGroup("og1").sequenceNumber(1).position("b").put("payload1", new byte[4]).put("payload2", new byte[8]).build(),
                    RawdataMessage.builder().orderingGroup("og1").sequenceNumber(1).position("c").put("payload1", new byte[2]).put("payload2", new byte[5]).build()
            );
        }

        try (RawdataConsumer consumer = client.consumer("the-topic", ulid, true)) {
            {
                RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertEquals(message.ulid(), ulid);
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 1);
                assertEquals(message.position(), "a");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[3]);
                assertEquals(message.get("payload2"), new byte[7]);
            }
            {
                RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 1);
                assertEquals(message.position(), "b");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[4]);
                assertEquals(message.get("payload2"), new byte[8]);
            }
            {
                RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
                assertNotNull(message.ulid());
                assertEquals(message.orderingGroup(), "og1");
                assertEquals(message.sequenceNumber(), 1);
                assertEquals(message.position(), "c");
                assertEquals(message.keys().size(), 2);
                assertEquals(message.get("payload1"), new byte[2]);
                assertEquals(message.get("payload2"), new byte[5]);
            }
        }
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.publish(RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        producer.publish(RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());

        RawdataMessage message = future.join();
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.publish(
                RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1.position(), "a");
        assertEquals(message2.position(), "b");
        assertEquals(message3.position(), "c");
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        producer.publish(
                RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0).position(), "a");
        assertEquals(messages.get(1).position(), "b");
        assertEquals(messages.get(2).position(), "c");
    }

    private CompletableFuture<List<RawdataMessage>> receiveAsyncAddMessageAndRepeatRecursive(RawdataConsumer consumer, String endPosition, List<RawdataMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.position())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer1 = client.consumer("the-topic");
        RawdataConsumer consumer2 = client.consumer("the-topic");

        CompletableFuture<List<RawdataMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<RawdataMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        producer.publish(RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
        );

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0).position(), "a");
        assertEquals(messages1.get(1).position(), "b");
        assertEquals(messages1.get(2).position(), "c");

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0).position(), "a");
        assertEquals(messages2.get(1).position(), "b");
        assertEquals(messages2.get(2).position(), "c");
    }

    @Test
    public void thatConsumerCanReadFromBeginning() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build(),
                    RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d", System.currentTimeMillis(), Duration.ofMinutes(1))) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            assertNull(message);
        }
    }

    @Test
    public void thatSeekToWorks() throws Exception {
        long timestampBeforeA;
        long timestampBeforeB;
        long timestampBeforeC;
        long timestampBeforeD;
        long timestampAfterD;
        try (RawdataProducer producer = client.producer("the-topic")) {
            timestampBeforeA = System.currentTimeMillis();
            producer.publish(RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build());
            Thread.sleep(5);
            timestampBeforeB = System.currentTimeMillis();
            producer.publish(RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build());
            Thread.sleep(5);
            timestampBeforeC = System.currentTimeMillis();
            producer.publish(RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampBeforeD = System.currentTimeMillis();
            producer.publish(RawdataMessage.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]).build());
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "d");
            consumer.seek(timestampBeforeB);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "b");
            consumer.seek(timestampBeforeC);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "c");
            consumer.seek(timestampBeforeA);
            assertEquals(consumer.receive(100, TimeUnit.MILLISECONDS).position(), "a");
        }
    }

    @Test
    public void thatPositionCursorOfValidPositionIsFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNotNull(client.cursorOf("the-topic", "a", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "b", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
        assertNotNull(client.cursorOf("the-topic", "c", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfInvalidPositionIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(
                    RawdataMessage.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]).build(),
                    RawdataMessage.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]).build(),
                    RawdataMessage.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]).build()
            );
        }
        assertNull(client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1)));
    }

    @Test(expectedExceptions = RawdataNoSuchPositionException.class)
    public void thatPositionCursorOfEmptyTopicIsNotFound() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
        }
        client.cursorOf("the-topic", "d", true, System.currentTimeMillis(), Duration.ofMinutes(1));
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        RawdataMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        metadata.put("key-2", "Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-1"), StandardCharsets.UTF_8), "Value-1");
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Value-2");
        metadata.put("key-2", "Overwritten-Value-2".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 2);
        assertEquals(new String(metadata.get("key-2"), StandardCharsets.UTF_8), "Overwritten-Value-2");
        metadata.remove("key-1");
        assertEquals(metadata.keys().size(), 1);
        metadata.remove("key-2");
        assertEquals(metadata.keys().size(), 0);
    }
}
