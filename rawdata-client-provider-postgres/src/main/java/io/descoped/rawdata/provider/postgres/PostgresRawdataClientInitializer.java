package io.descoped.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.HikariPool;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.service.provider.api.ProviderName;

import java.util.Map;
import java.util.Set;

/**
 * The {@code postgres} provider: topics in PostgreSQL 18. Configuration:
 * <table>
 *     <caption>Configuration keys</caption>
 *     <tr><th>Key</th><th>Default</th></tr>
 *     <tr><td>{@code postgres.driver.host}</td><td>{@code localhost}</td></tr>
 *     <tr><td>{@code postgres.driver.port}</td><td>{@code 5432}</td></tr>
 *     <tr><td>{@code postgres.driver.user}</td><td>required</td></tr>
 *     <tr><td>{@code postgres.driver.password}</td><td>required</td></tr>
 *     <tr><td>{@code postgres.driver.database}</td><td>required</td></tr>
 *     <tr><td>{@code postgres.driver.max-pool-size}</td><td>{@code 10}</td></tr>
 *     <tr><td>{@code rawdata.postgres.consumer.prefetch-size}</td><td>{@code 100}: messages per fetch</td></tr>
 *     <tr><td>{@code rawdata.postgres.consumer.prefetch-poll-interval-when-empty}</td><td>{@code 250}: milliseconds
 *     between polls of a topic that has nothing new</td></tr>
 * </table>
 * The tables are created in the user's default schema when they are missing; a role that may not create tables can
 * use tables created by another role. Connections are READ COMMITTED whatever the database's default, and show as
 * application {@code rawdata-postgres} in {@code pg_stat_activity}.
 */
@ProviderName("postgres")
public class PostgresRawdataClientInitializer implements RawdataClientInitializer {

    static final String HOST = "postgres.driver.host";
    static final String PORT = "postgres.driver.port";
    static final String USER = "postgres.driver.user";
    static final String PASSWORD = "postgres.driver.password";
    static final String DATABASE = "postgres.driver.database";
    static final String MAX_POOL_SIZE = "postgres.driver.max-pool-size";
    static final String PREFETCH_SIZE = "rawdata.postgres.consumer.prefetch-size";
    static final String POLL_INTERVAL = "rawdata.postgres.consumer.prefetch-poll-interval-when-empty";
    static final String APPLICATION_NAME = "rawdata-postgres";

    /**
     * Creates the initializer; called by the service loader.
     */
    public PostgresRawdataClientInitializer() {
        // nothing to set up: the client is created in initialize
    }

    /**
     * The required keys; the service provider API refuses a configuration without them. The others have defaults.
     *
     * @return the user, password and database keys
     */
    @Override
    public Set<String> configurationKeys() {
        return Set.of(USER, PASSWORD, DATABASE);
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        int prefetchSize = positive(configuration, PREFETCH_SIZE, 100);
        int pollIntervalMillis = positive(configuration, POLL_INTERVAL, 250);

        HikariConfig config = new HikariConfig();
        config.setPoolName("rawdata-postgres");
        config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        config.addDataSourceProperty("serverName", configuration.getOrDefault(HOST, "localhost"));
        config.addDataSourceProperty("portNumber", positive(configuration, PORT, 5432));
        config.addDataSourceProperty("user", required(configuration, USER));
        config.addDataSourceProperty("password", required(configuration, PASSWORD));
        config.addDataSourceProperty("databaseName", required(configuration, DATABASE));
        config.addDataSourceProperty("ApplicationName", APPLICATION_NAME);
        config.setAutoCommit(false);
        // The commit order of ids relies on it: after the topic lock, the last id must be read with a fresh snapshot.
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        config.setMaximumPoolSize(positive(configuration, MAX_POOL_SIZE, 10));

        HikariDataSource dataSource;
        try {
            dataSource = new HikariDataSource(config);
        } catch (HikariPool.PoolInitializationException e) {
            // the database cannot be reached or does not exist: a store failure, as in every provider
            throw new RawdataException("PostgreSQL: " + e.getMessage(), e);
        }
        try {
            PostgresSchema.create(new Jdbc(dataSource));
        } catch (RuntimeException e) {
            dataSource.close();
            throw e;
        }
        return new PostgresRawdataClient(dataSource, prefetchSize, pollIntervalMillis);
    }

    private static String required(Map<String, String> configuration, String key) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing configuration '" + key + "'");
        }
        return value;
    }

    private static int positive(Map<String, String> configuration, String key, int defaultValue) {
        String value = configuration.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            int number = Integer.parseInt(value.trim());
            if (number > 0) {
                return number;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        throw new IllegalArgumentException("Configuration '" + key + "' must be a positive number: " + value);
    }
}
