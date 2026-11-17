package io.descoped.rawdata.provider.postgres;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
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
 * The tables are created in the user's default schema when they are missing.
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
        config.setAutoCommit(false);
        config.setMaximumPoolSize(positive(configuration, MAX_POOL_SIZE, 10));

        HikariDataSource dataSource = new HikariDataSource(config);
        try {
            PostgresSchema.create(new Jdbc(dataSource));
        } catch (RuntimeException | Error e) {
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
