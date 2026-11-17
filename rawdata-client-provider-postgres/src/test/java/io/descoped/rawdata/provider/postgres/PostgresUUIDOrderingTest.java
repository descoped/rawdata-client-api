package io.descoped.rawdata.provider.postgres;

import de.huxhorn.sulky.ulid.ULID;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.service.provider.api.ProviderConfigurator;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

@Test
public class PostgresUUIDOrderingTest {

    static Map<String, String> configuration() {
        return Map.ofEntries(
                Map.entry("rawdata.client.provider", "postgres"),
                Map.entry("rawdata.postgres.consumer.prefetch-size", "100"),
                Map.entry("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "1000"),
                Map.entry("postgres.driver.host", "localhost"),
                Map.entry("postgres.driver.port", "5432"),
                Map.entry("postgres.driver.user", "rdc"),
                Map.entry("postgres.driver.password", "rdc"),
                Map.entry("postgres.driver.database", "rdc"),
                Map.entry("h2.enabled", "true"),
                Map.entry("h2.driver.url", "jdbc:h2:mem:rdc;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE")
        );
    }

    PostgresRawdataClient client;

    @BeforeMethod
    public void createRawdataClient() {
        Map<String, String> configuration = configuration();
        client = (PostgresRawdataClient) ProviderConfigurator.configure(RawdataClientInitializer.class, configuration.get("rawdata.client.provider"), configuration);
        dropTables("T1");
    }

    private void dropTables(String topic) {
        client.dropOrCreateTopicTables(topic, "io/descoped/rawdata/provider/postgres/init/init-topic-stream.sql");
    }

    @Test
    public void thatULIDtoUUIDAndBackConversionWorks() {
        ULID ulid = new ULID();
        ULID.Value value = ulid.nextValue();
        convertAndCompare(value);
        convertAndCompare(ulid.nextMonotonicValue(value));
    }

    private void convertAndCompare(ULID.Value value) {
        UUID uuidFromMsbLsb = new UUID(value.getMostSignificantBits(), value.getLeastSignificantBits());
        ULID.Value actual = new ULID.Value(uuidFromMsbLsb.getMostSignificantBits(), uuidFromMsbLsb.getLeastSignificantBits());
        assertEquals(actual, value);
    }

    @Test
    public void thatUUIDOrderingIsPreservedInDatabase() throws Exception {
        Random random = new Random();
        int N = 100000;
        List<String> expectedPositions = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            String position = random.nextInt(N) + "a" + i;
            expectedPositions.add(position);
        }
        long startProduce = System.currentTimeMillis();
        try (RawdataProducer producer = client.producer("T1")) {
            List<RawdataMessage> messages = new ArrayList<>();
            for (String position : expectedPositions) {
                messages.add(RawdataMessage.builder().position(position).put("payload", new byte[8]).build());
            }
            producer.publish(messages);
        }
        long endProduceStartConsume = System.currentTimeMillis();
        List<String> actualPositions = new ArrayList<>(N);
        try (RawdataConsumer consumer = client.consumer("T1")) {
            RawdataMessage message = consumer.receive(100, TimeUnit.MILLISECONDS);
            while (message != null) {
                actualPositions.add(message.position());
                message = consumer.receive(100, TimeUnit.MILLISECONDS);
            }
        }
        long endConsume = System.currentTimeMillis();
        assertEquals(actualPositions, expectedPositions);
        System.out.printf("Production: %d ms, Consumption: %d ms%n", endProduceStartConsume - startProduce, endConsume - endProduceStartConsume);
    }
}
