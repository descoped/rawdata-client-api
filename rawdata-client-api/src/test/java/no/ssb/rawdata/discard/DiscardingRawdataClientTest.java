package no.ssb.rawdata.discard;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataProducer;
import no.ssb.service.provider.api.ProviderConfigurator;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
            producer.buffer(producer.builder().ulid(new ULID.Value(0, 0)).position("p1").put("k", new byte[0]));
            producer.publish("p1");
            assertEquals(producer.topic(), "the-topic");
            assertFalse(producer.isClosed());
        }
    }

    @Test
    public void thatConsumerReturnEmpty() throws Exception {
        try (RawdataProducer producer = client.producer("the-topic")) {
            producer.buffer(producer.builder().ulid(new ULID.Value(0, 0)).position("p1").put("k", new byte[0]));
            producer.publish("p1");
        }
        try (RawdataConsumer consumer = client.consumer("the-topic")) {
            assertNull(consumer.receive(0, TimeUnit.MILLISECONDS));
            assertNull(consumer.receiveAsync().join());
            assertEquals(consumer.topic(), "the-topic");
            consumer.seek(123);
            assertFalse(consumer.isClosed());
        }
    }
}
