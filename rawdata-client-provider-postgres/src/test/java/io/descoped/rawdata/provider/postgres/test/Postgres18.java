package io.descoped.rawdata.provider.postgres.test;

import io.descoped.rawdata.api.RawdataClient;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * One PostgreSQL 18 container for all tests of the module, started on first use and removed when the JVM exits.
 * Tests keep apart by using topics of their own.
 */
final class Postgres18 {

    static final String IMAGE = "postgres:18-alpine";

    private static final PostgreSQLContainer CONTAINER = start();

    private Postgres18() {
    }

    private static PostgreSQLContainer start() {
        PostgreSQLContainer container = new PostgreSQLContainer(IMAGE)
                .withDatabaseName("rdc")
                .withUsername("rdc")
                .withPassword("rdc");
        container.start();
        return container;
    }

    static Map<String, String> configuration() {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RawdataClient.PROVIDER_KEY, "postgres");
        configuration.put("postgres.driver.host", CONTAINER.getHost());
        configuration.put("postgres.driver.port", String.valueOf(CONTAINER.getFirstMappedPort()));
        configuration.put("postgres.driver.user", CONTAINER.getUsername());
        configuration.put("postgres.driver.password", CONTAINER.getPassword());
        configuration.put("postgres.driver.database", CONTAINER.getDatabaseName());
        configuration.put("rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "50");
        return configuration;
    }

    static Connection connect() throws SQLException {
        return DriverManager.getConnection(CONTAINER.getJdbcUrl(), CONTAINER.getUsername(), CONTAINER.getPassword());
    }
}
