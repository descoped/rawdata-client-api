package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataMetadataClient;

import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A topic's metadata as rows of {@code rawdata_metadata}. A key is at most {@value #MAX_KEY_BYTES} bytes in UTF-8,
 * below what a PostgreSQL btree index entry holds, so a longer key is refused before the database sees it.
 */
final class PostgresRawdataMetadataClient implements RawdataMetadataClient {

    static final int MAX_KEY_BYTES = 2000;

    private final PostgresRawdataClient client;
    private final String topic;

    PostgresRawdataMetadataClient(PostgresRawdataClient client, String topic) {
        this.client = client;
        this.topic = topic;
    }

    private static void requireValidKey(String key) {
        RawdataMetadataClient.requireValidKey(key);
        int bytes = key.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("A metadata key is at most " + MAX_KEY_BYTES + " bytes in UTF-8 in postgres: " + bytes);
        }
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        client.checkOpen();
        return client.jdbc().transaction(connection -> {
            try (PreparedStatement select = connection.prepareStatement("SELECT name FROM rawdata_metadata WHERE topic = ? ORDER BY name")) {
                select.setString(1, topic);
                Set<String> keys = new LinkedHashSet<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        keys.add(rows.getString(1));
                    }
                }
                return Collections.unmodifiableSet(keys); // sorted by name
            }
        });
    }

    @Override
    public byte[] get(String key) {
        requireValidKey(key);
        client.checkOpen();
        return client.jdbc().transaction(connection -> {
            try (PreparedStatement select = connection.prepareStatement("SELECT data FROM rawdata_metadata WHERE topic = ? AND name = ?")) {
                select.setString(1, topic);
                select.setString(2, key);
                try (ResultSet rows = select.executeQuery()) {
                    return rows.next() ? rows.getBytes(1) : null;
                }
            }
        });
    }

    @Override
    public RawdataMetadataClient put(String key, byte[] value) {
        requireValidKey(key);
        Objects.requireNonNull(value, "value");
        client.checkOpen();
        byte[] copy = value.clone();
        client.jdbc().transaction(connection -> {
            try (PreparedStatement upsert = connection.prepareStatement(
                    "INSERT INTO rawdata_metadata (topic, name, data) VALUES (?, ?, ?) ON CONFLICT (topic, name) DO UPDATE SET data = EXCLUDED.data")) {
                upsert.setString(1, topic);
                upsert.setString(2, key);
                upsert.setBytes(3, copy);
                upsert.executeUpdate();
            }
            return null;
        });
        return this;
    }

    @Override
    public RawdataMetadataClient remove(String key) {
        requireValidKey(key);
        client.checkOpen();
        client.jdbc().transaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement("DELETE FROM rawdata_metadata WHERE topic = ? AND name = ?")) {
                delete.setString(1, topic);
                delete.setString(2, key);
                delete.executeUpdate();
            }
            return null;
        });
        return this;
    }
}
