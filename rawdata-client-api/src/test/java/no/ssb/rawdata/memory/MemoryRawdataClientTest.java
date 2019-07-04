package no.ssb.rawdata.memory;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class MemoryRawdataClientTest {

    @Test
    public void thatLastExternalIdOfEmptyTopicCanBeReadByProducer() {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");

        assertEquals(producer.lastExternalId(), null);
    }

    @Test
    public void thatLastExternalIdOfProducerCanBeRead() {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");

        producer.buffer(new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5])));
        producer.buffer(new MemoryRawdataMessageContent("b", Map.of("payload", new byte[3])));
        producer.publish("a", "b");

        assertEquals(producer.lastExternalId(), "b");

        producer.buffer(new MemoryRawdataMessageContent("c", Map.of("payload", new byte[7])));
        producer.publish("c");

        assertEquals(producer.lastExternalId(), "c");
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        MemoryRawdataMessageContent expected1 = new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5]));
        producer.buffer(expected1);
        producer.publish(expected1.externalId());

        RawdataMessage message = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message.content(), expected1);
    }

    @Test
    public void thatSingleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        CompletableFuture<? extends RawdataMessage> future = consumer.receiveAsync();

        MemoryRawdataMessageContent expected1 = new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5]));
        producer.buffer(expected1);
        producer.publish(expected1.externalId());

        RawdataMessage message = future.join();
        assertEquals(message.content(), expected1);
    }

    @Test
    public void thatMultipleMessageCanBeProducedAndConsumerSynchronously() throws InterruptedException {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        MemoryRawdataMessageContent expected1 = new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5]));
        MemoryRawdataMessageContent expected2 = new MemoryRawdataMessageContent("b", Map.of("payload", new byte[3]));
        MemoryRawdataMessageContent expected3 = new MemoryRawdataMessageContent("c", Map.of("payload", new byte[7]));
        producer.buffer(expected1);
        producer.buffer(expected2);
        producer.buffer(expected3);
        producer.publish(expected1.externalId(), expected2.externalId(), expected3.externalId());

        RawdataMessage message1 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message2 = consumer.receive(1, TimeUnit.SECONDS);
        RawdataMessage message3 = consumer.receive(1, TimeUnit.SECONDS);
        assertEquals(message1.content(), expected1);
        assertEquals(message2.content(), expected2);
        assertEquals(message3.content(), expected3);
    }

    @Test
    public void thatMultipleMessageCanBeProducedAndConsumerAsynchronously() {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer = client.consumer("the-topic", "sub1");

        CompletableFuture<List<RawdataMessage>> future = receiveAsyncAddMessageAndRepeatRecursive(consumer, "c", new ArrayList<>());

        MemoryRawdataMessageContent expected1 = new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5]));
        MemoryRawdataMessageContent expected2 = new MemoryRawdataMessageContent("b", Map.of("payload", new byte[3]));
        MemoryRawdataMessageContent expected3 = new MemoryRawdataMessageContent("c", Map.of("payload", new byte[7]));
        producer.buffer(expected1);
        producer.buffer(expected2);
        producer.buffer(expected3);
        producer.publish(expected1.externalId(), expected2.externalId(), expected3.externalId());

        List<RawdataMessage> messages = future.join();

        assertEquals(messages.get(0).content(), expected1);
        assertEquals(messages.get(1).content(), expected2);
        assertEquals(messages.get(2).content(), expected3);
    }

    private CompletableFuture<List<RawdataMessage>> receiveAsyncAddMessageAndRepeatRecursive(RawdataConsumer consumer, String endPosition, List<RawdataMessage> messages) {
        return consumer.receiveAsync().thenCompose(message -> {
            messages.add(message);
            if (endPosition.equals(message.content().externalId())) {
                return CompletableFuture.completedFuture(messages);
            }
            return receiveAsyncAddMessageAndRepeatRecursive(consumer, endPosition, messages);
        });
    }

    @Test
    public void thatMessagesCanBeConsumedByMultipleConsumers() {
        RawdataClient client = new MemoryRawdataClient();
        RawdataProducer producer = client.producer("the-topic");
        RawdataConsumer consumer1 = client.consumer("the-topic", "sub1");
        RawdataConsumer consumer2 = client.consumer("the-topic", "sub2");

        CompletableFuture<List<RawdataMessage>> future1 = receiveAsyncAddMessageAndRepeatRecursive(consumer1, "c", new ArrayList<>());
        CompletableFuture<List<RawdataMessage>> future2 = receiveAsyncAddMessageAndRepeatRecursive(consumer2, "c", new ArrayList<>());

        MemoryRawdataMessageContent expected1 = new MemoryRawdataMessageContent("a", Map.of("payload", new byte[5]));
        MemoryRawdataMessageContent expected2 = new MemoryRawdataMessageContent("b", Map.of("payload", new byte[3]));
        MemoryRawdataMessageContent expected3 = new MemoryRawdataMessageContent("c", Map.of("payload", new byte[7]));
        producer.buffer(expected1);
        producer.buffer(expected2);
        producer.buffer(expected3);
        producer.publish(expected1.externalId(), expected2.externalId(), expected3.externalId());

        List<RawdataMessage> messages1 = future1.join();
        assertEquals(messages1.get(0).content(), expected1);
        assertEquals(messages1.get(1).content(), expected2);
        assertEquals(messages1.get(2).content(), expected3);

        List<RawdataMessage> messages2 = future2.join();
        assertEquals(messages2.get(0).content(), expected1);
        assertEquals(messages2.get(1).content(), expected2);
        assertEquals(messages2.get(2).content(), expected3);
    }
}
