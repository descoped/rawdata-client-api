package io.descoped.rawdata.provider.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The tables every topic shares. The topic is a column and always a bind parameter, never part of SQL text. The
 * tables are created when missing, under an advisory lock, so clients that start at the same time do not race.
 * <pre>
 * rawdata_message   (topic, id)            one row per message; id is the UUIDv7, ordered as PostgreSQL orders uuid
 * rawdata_content   (topic, id, ordinal)   the named content of a message, in the order the keys were added
 * rawdata_metadata  (topic, name)          the metadata entries of a topic
 * </pre>
 */
final class PostgresSchema {

    /**
     * Advisory lock class ids ("RDPC" and the next): one for creating the tables, one for publishing to a topic,
     * with the topic's hash as the second key.
     */
    static final int SCHEMA_LOCK = 0x52445043;
    static final int TOPIC_LOCK = 0x52445044;

    private static final List<String> DDL = List.of(
            """
            CREATE TABLE IF NOT EXISTS rawdata_message (
                topic           varchar(100) NOT NULL,
                id              uuid         NOT NULL,
                position        varchar      NOT NULL,
                ordering_group  varchar,
                sequence_number bigint       NOT NULL,
                PRIMARY KEY (topic, id)
            )""",
            "CREATE INDEX IF NOT EXISTS rawdata_message_position ON rawdata_message (topic, position)",
            """
            CREATE TABLE IF NOT EXISTS rawdata_content (
                topic   varchar(100) NOT NULL,
                id      uuid         NOT NULL,
                ordinal integer      NOT NULL,
                name    varchar      NOT NULL,
                data    bytea        NOT NULL,
                PRIMARY KEY (topic, id, ordinal),
                UNIQUE (topic, id, name),
                FOREIGN KEY (topic, id) REFERENCES rawdata_message (topic, id) ON DELETE CASCADE
            )""",
            """
            CREATE TABLE IF NOT EXISTS rawdata_metadata (
                topic varchar(100) NOT NULL,
                name  varchar      NOT NULL,
                data  bytea        NOT NULL,
                PRIMARY KEY (topic, name)
            )""");

    private PostgresSchema() {
    }

    static void create(Jdbc jdbc) {
        jdbc.transaction(connection -> {
            lock(connection);
            try (Statement statement = connection.createStatement()) {
                for (String ddl : DDL) {
                    statement.execute(ddl);
                }
            }
            return null;
        });
    }

    private static void lock(Connection connection) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, 0)")) {
            lock.setInt(1, SCHEMA_LOCK);
            lock.execute();
        }
    }
}
