package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMessage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The Avro file of a segment: one record per message, in id order, with the schema of 2.x. Only the id's fixed type
 * is named {@code uuid} instead of {@code ulid}: the 16 bytes are a UUIDv7. A message's content is the map
 * {@code data}; its entries are written in the order they were put and read back in that order.
 * <p>
 * A window file carries, in its Avro header, the segment it becomes and the id of the message before it, so a writer
 * that restarts after a crash knows where a leftover window belongs.
 */
final class AvroSegments {

    private static final String ID_FIELD = "id";
    private static final String ORDERING_GROUP = "orderingGroup";
    private static final String SEQUENCE_NUMBER = "sequenceNumber";
    private static final String POSITION = "position";
    private static final String DATA = "data";

    /** Header key: the segment number the window becomes. */
    static final String META_SEGMENT = "rawdata.segment";
    /** Header key: the id of the last message before the window. */
    static final String META_PREVIOUS_ID = "rawdata.previous-id";

    static final Schema MESSAGE = SchemaBuilder.record("RawdataMessage")
            .fields()
            .name(ID_FIELD).type().fixed("uuid").size(16).noDefault()
            .name(ORDERING_GROUP).type().nullable().stringType().noDefault()
            .name(SEQUENCE_NUMBER).type().longType().longDefault(0)
            .name(POSITION).type().stringType().noDefault()
            .name(DATA).type().map().values().bytesType().noDefault()
            .endRecord();

    private static final Schema ID = MESSAGE.getField(ID_FIELD).schema();

    private AvroSegments() {
    }

    /**
     * A segment's Avro file in memory.
     */
    static byte[] encode(Collection<RawdataMessage> messages, long segment, UUID previousId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = writer(segment, previousId)) {
            writer.create(MESSAGE, out);
            for (RawdataMessage message : messages) {
                writer.append(toRecord(message));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Writing to memory failed", e);
        }
        return out.toByteArray();
    }

    /**
     * A writer of a segment's Avro file with its header set; the caller creates it on a stream or a file.
     */
    static DataFileWriter<GenericRecord> writer(long segment, UUID previousId) {
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(MESSAGE));
        writer.setMeta(META_SEGMENT, Long.toString(segment));
        writer.setMeta(META_PREVIOUS_ID, previousId.toString());
        return writer;
    }

    static List<RawdataMessage> decode(byte[] segment) throws IOException {
        try (DataFileStream<GenericRecord> reader = reader(new ByteArrayInputStream(segment))) {
            List<RawdataMessage> messages = new ArrayList<>();
            GenericRecord read = null;
            while (reader.hasNext()) {
                read = reader.next(read);
                messages.add(toMessage(read));
            }
            return messages;
        }
    }

    /**
     * What a window file left on disk holds: where it belongs, and the messages of its complete blocks.
     *
     * @param segment    the segment it becomes
     * @param previousId the id before it
     * @param messages   the messages of its complete blocks
     * @param complete   false if the file ends in a torn block, which is left out
     */
    record Leftover(long segment, UUID previousId, List<RawdataMessage> messages, boolean complete) {
    }

    /**
     * Reads a window file left on disk, keeping every complete block. Avro reads a block whole and checks the sync
     * marker after it before it returns the block's records, so a block cut short by a crash fails, and only the
     * blocks before it are kept.
     *
     * @return the leftover, or null if not even its header is whole, so nothing was ever appended
     */
    static Leftover readLeftover(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            DataFileStream<GenericRecord> reader;
            try {
                reader = reader(in);
            } catch (IOException | AvroRuntimeException e) {
                return null;
            }
            try (reader) {
                byte[] segment = reader.getMeta(META_SEGMENT);
                byte[] previous = reader.getMeta(META_PREVIOUS_ID);
                if (segment == null || previous == null) {
                    return null;
                }
                List<RawdataMessage> messages = new ArrayList<>();
                boolean complete = true;
                try {
                    GenericRecord read = null;
                    while (reader.hasNext()) {
                        read = reader.next(read);
                        messages.add(toMessage(read));
                    }
                } catch (IOException | AvroRuntimeException e) {
                    complete = false;
                }
                return new Leftover(Long.parseLong(new String(segment, StandardCharsets.UTF_8)),
                        UUID.fromString(new String(previous, StandardCharsets.UTF_8)), messages, complete);
            }
        }
    }

    private static DataFileStream<GenericRecord> reader(InputStream in) throws IOException {
        return new DataFileStream<>(in, new GenericDatumReader<>(MESSAGE) {
            @Override
            protected Object newMap(Object old, int size) {
                return new LinkedHashMap<>(); // keep the order the entries were put in
            }
        });
    }

    static GenericRecord toRecord(RawdataMessage message) {
        GenericRecord written = new GenericData.Record(MESSAGE);
        written.put(ID_FIELD, new GenericData.Fixed(ID, toBytes(message.id())));
        written.put(ORDERING_GROUP, message.orderingGroup());
        written.put(SEQUENCE_NUMBER, message.sequenceNumber());
        written.put(POSITION, message.position());
        Map<String, ByteBuffer> data = new LinkedHashMap<>();
        for (String key : message.keys()) {
            data.put(key, ByteBuffer.wrap(message.get(key)));
        }
        written.put(DATA, data);
        return written;
    }

    /**
     * About the bytes a message takes in a block, as 2.x estimated it: enough to know when a block is full.
     */
    static long estimatedSize(RawdataMessage message) {
        long size = 16 + 6 + 2L + message.position().length()
                + 2 + (message.orderingGroup() == null ? 0 : message.orderingGroup().length());
        for (String key : message.keys()) {
            size += 2 + key.length() + 4 + message.get(key).length;
        }
        return size;
    }

    private static RawdataMessage toMessage(GenericRecord stored) {
        RawdataMessage.Builder builder = RawdataMessage.builder()
                .id(fromBytes(((GenericData.Fixed) stored.get(ID_FIELD)).bytes()))
                .sequenceNumber((Long) stored.get(SEQUENCE_NUMBER))
                .position(stored.get(POSITION).toString());
        Object orderingGroup = stored.get(ORDERING_GROUP);
        if (orderingGroup != null) {
            builder.orderingGroup(orderingGroup.toString());
        }
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) stored.get(DATA)).entrySet()) {
            ByteBuffer value = (ByteBuffer) entry.getValue();
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            builder.put(entry.getKey().toString(), bytes);
        }
        return builder.build();
    }

    /**
     * The 16 bytes of an id, most significant first.
     */
    static byte[] toBytes(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }

    static UUID fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
