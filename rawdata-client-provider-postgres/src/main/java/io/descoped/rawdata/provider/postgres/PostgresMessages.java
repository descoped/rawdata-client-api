package io.descoped.rawdata.provider.postgres;

import io.descoped.rawdata.api.RawdataMessage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Reads and writes messages: one row in {@code rawdata_message} and one row per content key in
 * {@code rawdata_content}.
 */
final class PostgresMessages {

    private static final String COLUMNS = "SELECT m.id, m.position, m.ordering_group, m.sequence_number, c.name, c.data FROM (";
    private static final String CONTENT = ") m LEFT JOIN rawdata_content c ON c.topic = ? AND c.id = m.id ";

    /**
     * The messages after (or from) an id, in order.
     */
    static final String AFTER = COLUMNS
            + "SELECT id, position, ordering_group, sequence_number FROM rawdata_message WHERE topic = ? AND id > ? ORDER BY id LIMIT ?"
            + CONTENT + "ORDER BY m.id, c.ordinal";
    static final String FROM = COLUMNS
            + "SELECT id, position, ordering_group, sequence_number FROM rawdata_message WHERE topic = ? AND id >= ? ORDER BY id LIMIT ?"
            + CONTENT + "ORDER BY m.id, c.ordinal";
    /**
     * The message with the greatest id.
     */
    static final String LAST = COLUMNS
            + "SELECT id, position, ordering_group, sequence_number FROM rawdata_message WHERE topic = ? ORDER BY id DESC LIMIT 1"
            + CONTENT + "ORDER BY c.ordinal";

    private PostgresMessages() {
    }

    static List<RawdataMessage> after(Connection connection, String topic, UUID id, boolean inclusive, int limit) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(inclusive ? FROM : AFTER)) {
            select.setString(1, topic);
            select.setObject(2, id);
            select.setInt(3, limit);
            select.setString(4, topic);
            return read(select);
        }
    }

    static RawdataMessage last(Connection connection, String topic) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(LAST)) {
            select.setString(1, topic);
            select.setString(2, topic);
            List<RawdataMessage> messages = read(select);
            return messages.isEmpty() ? null : messages.get(0);
        }
    }

    static UUID lastId(Connection connection, String topic) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id FROM rawdata_message WHERE topic = ? ORDER BY id DESC LIMIT 1")) {
            select.setString(1, topic);
            try (ResultSet rows = select.executeQuery()) {
                return rows.next() ? rows.getObject(1, UUID.class) : null;
            }
        }
    }

    static void insert(Connection connection, String topic, List<RawdataMessage> messages) throws SQLException {
        try (PreparedStatement message = connection.prepareStatement(
                "INSERT INTO rawdata_message (topic, id, position, ordering_group, sequence_number) VALUES (?, ?, ?, ?, ?)");
             PreparedStatement content = connection.prepareStatement(
                     "INSERT INTO rawdata_content (topic, id, ordinal, name, data) VALUES (?, ?, ?, ?, ?)")) {
            boolean anyContent = false;
            for (RawdataMessage m : messages) {
                message.setString(1, topic);
                message.setObject(2, m.id());
                message.setString(3, m.position());
                message.setString(4, m.orderingGroup());
                message.setLong(5, m.sequenceNumber());
                message.addBatch();
                int ordinal = 0;
                for (String key : m.keys()) {
                    content.setString(1, topic);
                    content.setObject(2, m.id());
                    content.setInt(3, ordinal++);
                    content.setString(4, key);
                    content.setBytes(5, m.get(key));
                    content.addBatch();
                    anyContent = true;
                }
            }
            message.executeBatch();
            if (anyContent) {
                content.executeBatch();
            }
        }
    }

    /**
     * Groups the rows (one per content key, or one with null content for a message without content) into messages.
     */
    private static List<RawdataMessage> read(PreparedStatement select) throws SQLException {
        List<RawdataMessage> messages = new ArrayList<>();
        try (ResultSet rows = select.executeQuery()) {
            UUID current = null;
            RawdataMessage.Builder builder = null;
            while (rows.next()) {
                UUID id = rows.getObject(1, UUID.class);
                if (!id.equals(current)) {
                    if (builder != null) {
                        messages.add(builder.build());
                    }
                    current = id;
                    builder = RawdataMessage.builder()
                            .id(id)
                            .position(rows.getString(2))
                            .orderingGroup(rows.getString(3))
                            .sequenceNumber(rows.getLong(4));
                }
                String name = rows.getString(5);
                if (name != null) {
                    builder.put(name, rows.getBytes(6));
                }
            }
            if (builder != null) {
                messages.add(builder.build());
            }
        }
        return messages;
    }
}
