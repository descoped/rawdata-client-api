package no.ssb.rawdata.tck;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class RawdataClientTck {

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

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.publish("a", "b");

        assertEquals(client.lastMessage("the-topic").position(), "b");

        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("c");

        assertEquals(client.lastMessage("the-topic").position(), "c");
    }

    @Test(expectedExceptions = RawdataNotBufferedException.class)
    public void thatPublishNonBufferedMessagesThrowsException() {
        RawdataProducer producer = client.producer("the-topic");
        producer.publish("unbuffered-1");
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.publish("a");

        RawdataMessage message = future.join();
        assertEquals(message.position(), "a");
        assertEquals(message.keys().size(), 2);
    }

    @Test
    public void thatMultipleMessagesCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic");

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

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

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

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

        producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
        producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
        producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
        producer.publish("a", "b", "c");

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
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "a");
        }
    }

    @Test
    public void thatConsumerCanReadFromFirstMessage() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "a")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "b");
        }
    }

    @Test
    public void thatConsumerCanReadFromMiddle() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "b")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c", true)) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "c");
        }
    }

    @Test
    public void thatConsumerCanReadFromRightBeforeLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "c")) {
            RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
            assertEquals(message.position(), "d");
        }
    }

    @Test
    public void thatConsumerCanReadFromLast() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            producer.publish("a", "b", "c", "d");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic", "d")) {
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
            producer.buffer(producer.builder().position("a").put("payload1", new byte[5]).put("payload2", new byte[5]));
            timestampBeforeA = System.currentTimeMillis();
            producer.publish("a");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("b").put("payload1", new byte[3]).put("payload2", new byte[3]));
            timestampBeforeB = System.currentTimeMillis();
            producer.publish("b");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("c").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeC = System.currentTimeMillis();
            producer.publish("c");
            Thread.sleep(5);
            producer.buffer(producer.builder().position("d").put("payload1", new byte[7]).put("payload2", new byte[7]));
            timestampBeforeD = System.currentTimeMillis();
            producer.publish("d");
            Thread.sleep(5);
            timestampAfterD = System.currentTimeMillis();
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            consumer.seek(timestampAfterD);
            assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
            consumer.seek(timestampBeforeD);
            assertEquals("d", consumer.receive(100, TimeUnit.MILLISECONDS).position());
            consumer.seek(timestampBeforeB);
            assertEquals("b", consumer.receive(100, TimeUnit.MILLISECONDS).position());
            consumer.seek(timestampBeforeC);
            assertEquals("c", consumer.receive(100, TimeUnit.MILLISECONDS).position());
            consumer.seek(timestampBeforeA);
            assertEquals("a", consumer.receive(100, TimeUnit.MILLISECONDS).position());
        }
    }
}
