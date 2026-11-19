package io.descoped.rawdata.provider.postgres.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider's internals, which the TCK cannot see: the stored layout, the topic lock, privileges, the start-up of
 * the schema and the pool.
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
                values.append(values.isEmpty() ? "" : ",").append("('").append(each).append("'::uuid)");
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

    /**
     * A publish that waits for the topic lock reads the last id after the lock holder's commit, also when the
     * database defaults to REPEATABLE READ (whose snapshot would be taken before the wait): the pool pins READ
     * COMMITTED.
     */
    @Test
    void publishReadsTheLastIdAfterWaitingForTheTopicLock() throws Exception {
        execute("ALTER ROLE rdc SET default_transaction_isolation = 'repeatable read'");
        String topic = topic();
        UUID future = RawdataIds.next(RawdataIds.beginningOf(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1)));
        try (RawdataClient client = client(); RawdataProducer producer = client.producer(topic);
             Connection holder = Postgres18.connect()) {
            holder.setAutoCommit(false);
            try (PreparedStatement lock = holder.prepareStatement("SELECT pg_advisory_xact_lock(?, hashtext(?))");
                 PreparedStatement insert = holder.prepareStatement(
                         "INSERT INTO rawdata_message (topic, id, position, sequence_number) VALUES (?, ?, 'holder', 0)")) {
                lock.setInt(1, TOPIC_LOCK);
                lock.setString(2, topic);
                lock.execute();
                insert.setString(1, topic);
                insert.setObject(2, future);
                insert.executeUpdate();
            }
            CompletableFuture<Void> publish = CompletableFuture.runAsync(
                    () -> producer.publish(RawdataMessage.builder().position("after-holder").build()));
            Thread.sleep(300);
            assertFalse(publish.isDone(), "the publish waits for the topic lock");
            holder.commit();
            publish.get(30, TimeUnit.SECONDS);
            RawdataMessage last = client.lastMessage(topic);
            assertEquals("after-holder", last.position());
            assertTrue(RawdataIds.ORDER.compare(last.id(), future) > 0, "the id follows the holder's commit");
        } finally {
            execute("ALTER ROLE rdc RESET default_transaction_isolation");
        }
    }

    /**
     * The advisory lock class of a topic, as PostgresSchema.TOPIC_LOCK.
     */
    private static final int TOPIC_LOCK = 0x52445044;

    /**
     * A role that may only read and write the tables can use them once another role has created them.
     */
    @Test
    void aRoleWithoutCreatePrivilegeCanUseExistingTables() throws Exception {
        client().close(); // the tables exist
        execute("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rdc_app') THEN "
                + "CREATE ROLE rdc_app LOGIN PASSWORD 'app'; END IF; END $$");
        execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC");
        execute("GRANT USAGE ON SCHEMA public TO rdc_app");
        execute("GRANT SELECT, INSERT, UPDATE, DELETE ON rawdata_message, rawdata_content, rawdata_metadata TO rdc_app");
        Map<String, String> configuration = new java.util.HashMap<>(Postgres18.configuration());
        configuration.put("postgres.driver.user", "rdc_app");
        configuration.put("postgres.driver.password", "app");
        String topic = topic();
        try (RawdataClient client = RawdataClient.create(configuration); RawdataProducer producer = client.producer(topic)) {
            producer.publish(RawdataMessage.builder().position("a").put("k", new byte[]{1}).build());
            client.metadata(topic).put("checkpoint", new byte[]{2});
            assertEquals("a", client.lastMessage(topic).position());
        }
    }

    private static void execute(String sql) throws Exception {
        try (Connection connection = Postgres18.connect(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
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
            RawdataMessage duplicate = RawdataMessage.builder().id(last).position("duplicate").build();
            assertThrows(RawdataException.class, () -> producer.publish(fine, duplicate), "the primary key refuses a duplicate id");
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
    void closingTheClientClosesItsPool() throws Exception {
        long before = backends();
        RawdataClient client = client();
        client.lastMessage(topic());
        assertTrue(backends() > before, "the client holds connections");
        client.close();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (backends() > before && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertEquals(before, backends(), "closing the client closes its connections");
        assertTrue(client.isClosed());
    }

    private static long backends() throws Exception {
        try (Connection connection = Postgres18.connect(); Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT count(*) FROM pg_stat_activity WHERE application_name = 'rawdata-postgres'")) {
            rows.next();
            return rows.getLong(1);
        }
    }
}
