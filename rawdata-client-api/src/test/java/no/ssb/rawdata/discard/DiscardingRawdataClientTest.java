package no.ssb.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataMetadataClient;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class DiscardingRawdataClientTest {

    RawdataClient client;

    @BeforeMethod
    public void createRawdataClient() {
        client = ProviderConfigurator.configure(Map.of(), "discard", RawdataClientInitializer.class);
    }

    @AfterMethod
    public void closeRawdataClient() throws Exception {
        client.close();
    }

    @Test
    public void thatClientMethodsReturnEmpty() throws Exception {
        assertNull(client.lastMessage("the-topic"));
        assertNull(client.cursorOf("the-topic", null, true));
        assertNull(client.cursorOf("the-topic", "p1", true, 0, Duration.ZERO));
        assertFalse(client.isClosed());
    }

    @Test
    public void thatProducerMethodsAcceptAndDiscardAll() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(RawdataMessage.builder()
                    .ulid(new ULID.Value(0, 0))
                    .position("p1")
                    .put("k", new byte[0])
                    .build());
            assertEquals(producer.topic(), "the-topic");
            assertFalse(producer.isClosed());
        }
    }

    @Test
    public void thatConsumerReturnEmpty() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.publish(RawdataMessage.builder()
                    .ulid(new ULID.Value(0, 0))
                    .position("p1")
                    .put("k", new byte[0])
                    .build());
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));
            assertNull(consumer.receiveAsync().join());
            assertEquals(consumer.topic(), "the-topic");
            consumer.seek(123);
            assertFalse(consumer.isClosed());
        }
    }

    @Test
    public void thatMetadataCanBeWrittenListedAndRead() {
        RawdataMetadataClient metadata = client.metadata("the-topic");
        assertEquals(metadata.topic(), "the-topic");
        assertEquals(metadata.keys().size(), 0);
        metadata.put("key-1", "Value-1".getBytes(StandardCharsets.UTF_8));
        assertEquals(metadata.keys().size(), 0);
        assertNull(metadata.get("key-1"));
    }
}
