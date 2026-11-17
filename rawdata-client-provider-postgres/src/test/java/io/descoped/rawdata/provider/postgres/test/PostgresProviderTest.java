package io.descoped.rawdata.provider.postgres.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the contract cannot see from outside: the stored layout, several clients (as several processes would be) on
 * one topic, and the start-up of the schema.
 */
@Timeout(120)
class PostgresProviderTest {

    private static String topic() {
        return "pg-" + UUID.randomUUID();
    }

    private static RawdataClient client() {
        return RawdataClient.create(Postgres18.configuration());
    }

    @Test
    void storedIdsArePostgresUuidV7() throws Exception {
        String topic = topic();
        try (RawdataClient client = client(); RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("a").put("k", new byte[]{1}).build());
            RawdataMessage message = client.lastMessage(topic);
            try (Connection connection = Postgres18.connect();
                 PreparedStatement select = connection.prepareStatement(
                         "SELECT uuid_extract_version(id), (extract(epoch FROM uuid_extract_timestamp(id)) * 1000)::bigint FROM rawdata_message WHERE topic = ?")) {
                select.setString(1, topic);
                try (ResultSet rows = select.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(7, rows.getInt(1));
                    assertEquals(message.timestamp(), rows.getLong(2));
                }
            }
        }
    }

    @Test
    void postgresOrdersIdsAsRawdataIdsDoes() throws Exception {
        List<UUID> ids = new ArrayList<>();
        UUID id = RawdataIds.beginningOf(System.currentTimeMillis());
        for (int i = 0; i < 500; i++) {
            ids.add(id = RawdataIds.next(id));
        }
        ids.add(UUID.fromString("7fffffff-ffff-7fff-bfff-ffffffffffff"));
        ids.add(UUID.fromString("80000000-0000-7000-8000-000000000000"));
        List<UUID> expected = new ArrayList<>(ids);
        expected.sort(RawdataIds.ORDER);
        try (Connection connection = Postgres18.connect(); Statement statement = connection.createStatement()) {
            StringBuilder values = new StringBuilder();
            for (UUID each : ids) {
                values.append(values.length() == 0 ? "" : ",").append("('").append(each).append("'::uuid)");
            }
            List<UUID> sorted = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery("SELECT u FROM (VALUES " + values + ") v(u) ORDER BY u")) {
                while (rows.next()) {
                    sorted.add(rows.getObject(1, UUID.class));
                }
            }
            assertEquals(expected, sorted);
        }
    }

    @Test
    void clientsOnOneTopicKeepItInCommitOrder() throws Exception {
        String topic = topic();
        int clients = 4;
        int perClient = 100;
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        List<RawdataClient> opened = new ArrayList<>();
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                RawdataClient client = client();
                opened.add(client);
                int number = c;
                futures.add(executor.submit(() -> {
                    start.await();
                    try (RawdataProducer producer = client.producer(topic)) {
                        for (int i = 0; i < perClient; i++) {
                            producer.publish(RawdataMessage.builder().position("c" + number + "-" + i).build());
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            opened.forEach(RawdataClient::close);
        }
        try (RawdataClient client = client(); RawdataConsumer consumer = client.consumer(topic)) {
            List<RawdataMessage> read = consumer.receiveUpTo(clients * perClient + 1, Duration.ofSeconds(5));
            while (read.size() < clients * perClient) {
                List<RawdataMessage> more = consumer.receiveUpTo(clients * perClient, Duration.ofSeconds(1));
                if (more.isEmpty()) {
                    break;
                }
                read.addAll(more);
            }
            assertEquals(clients * perClient, read.size());
            for (int i = 1; i < read.size(); i++) {
                assertTrue(RawdataIds.ORDER.compare(read.get(i - 1).id(), read.get(i).id()) < 0);
            }
            for (int c = 0; c < clients; c++) {
                String prefix = "c" + c + "-";
                List<String> own = read.stream().map(RawdataMessage::position).filter(p -> p.startsWith(prefix)).toList();
                for (int i = 0; i < perClient; i++) {
                    assertEquals(prefix + i, own.get(i));
                }
            }
        }
    }

    @Test
    void aConsumerSeesMessagesOfAnotherClient() throws Exception {
        String topic = topic();
        try (RawdataClient reader = client(); RawdataClient writer = client();
             RawdataConsumer consumer = reader.consumer(topic)) {
            assertTrue(consumer.receiveUpTo(10, Duration.ZERO).isEmpty());
            try (RawdataProducer producer = writer.producer(topic)) {
                producer.publish(RawdataMessage.builder().position("from-writer").build());
            }
            assertEquals("from-writer", consumer.receive(10, TimeUnit.SECONDS).position());
        }
    }

    @Test
    void clientsStartingTogetherCreateTheSchemaOnce() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<RawdataClient>> futures = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                futures.add(executor.submit(PostgresProviderTest::client));
            }
            for (Future<RawdataClient> future : futures) {
                future.get(60, TimeUnit.SECONDS).close();
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void aFailedPublishWritesNothing() throws Exception {
        String topic = topic();
        try (RawdataClient client = client(); RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("a").build());
            UUID last = client.lastMessage(topic).id();
            RawdataMessage fine = RawdataMessage.builder().position("b").put("k", new byte[]{1}).build();
            RawdataMessage stale = RawdataMessage.builder().id(last).position("stale").build();
            assertThrows(IllegalArgumentException.class, () -> producer.publish(fine, stale));
            try (Connection connection = Postgres18.connect();
                 PreparedStatement select = connection.prepareStatement(
                         "SELECT (SELECT count(*) FROM rawdata_message WHERE topic = ?), (SELECT count(*) FROM rawdata_content WHERE topic = ?)")) {
                select.setString(1, topic);
                select.setString(2, topic);
                try (ResultSet rows = select.executeQuery()) {
                    assertTrue(rows.next());
                    assertEquals(1, rows.getLong(1));
                    assertEquals(0, rows.getLong(2));
                }
            }
        }
    }

    @Test
    void topicsAreRowsNotTables() throws Exception {
        try (RawdataClient client = client(); RawdataProducer producer = client.producer("Robert.DROP-TABLE_students")) {
            producer.publish(RawdataMessage.builder().position("a").build());
        }
        try (Connection connection = Postgres18.connect(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() ORDER BY table_name")) {
            List<String> tables = new ArrayList<>();
            while (rows.next()) {
                tables.add(rows.getString(1));
            }
            assertEquals(List.of("rawdata_content", "rawdata_message", "rawdata_metadata"), tables);
        }
    }

    @Test
    void missingConfigurationIsReported() {
        Map<String, String> configuration = new java.util.HashMap<>(Postgres18.configuration());
        configuration.remove("postgres.driver.user");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(configuration));
        assertTrue(e.getMessage().contains("postgres.driver.user"));
        configuration.put("postgres.driver.user", "rdc");
        configuration.put("rawdata.postgres.consumer.prefetch-size", "0");
        assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(configuration));
    }

    @Test
    void closingTheClientClosesItsPool() {
        RawdataClient client = client();
        client.close();
        assertTrue(client.isClosed());
        assertFalse(client.toString().isEmpty());
    }
}
