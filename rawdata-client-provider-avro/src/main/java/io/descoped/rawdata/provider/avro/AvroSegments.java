package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMessage;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * The Avro file of a segment: one record per message, in id order. The content is an array of name and value pairs,
 * so the order of a message's keys survives.
 */
final class AvroSegments {

    private static final String NAMESPACE = "io.descoped.rawdata";
    private static final String ID_FIELD = "id";
    private static final String ORDERING_GROUP = "orderingGroup";
    private static final String SEQUENCE_NUMBER = "sequenceNumber";
    private static final String POSITION = "position";
    private static final String CONTENT_FIELD = "content";
    private static final String NAME = "name";
    private static final String VALUE = "value";

    static final Schema ID = SchemaBuilder.fixed("uuid").namespace(NAMESPACE).size(16);

    static final Schema CONTENT = SchemaBuilder.record("Content").namespace(NAMESPACE)
            .fields()
            .name(NAME).type().stringType().noDefault()
            .name(VALUE).type().bytesType().noDefault()
            .endRecord();

    static final Schema MESSAGE = SchemaBuilder.record("RawdataMessage").namespace(NAMESPACE)
            .fields()
            .name(ID_FIELD).type(ID).noDefault()
            .name(ORDERING_GROUP).type().nullable().stringType().noDefault()
            .name(SEQUENCE_NUMBER).type().longType().noDefault()
            .name(POSITION).type().stringType().noDefault()
            .name(CONTENT_FIELD).type().array().items(CONTENT).noDefault()
            .endRecord();

    private AvroSegments() {
    }

    static byte[] encode(Collection<RawdataMessage> messages) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(MESSAGE))) {
            writer.create(MESSAGE, out);
            for (RawdataMessage message : messages) {
                writer.append(toRecord(message));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Writing to memory failed", e);
        }
        return out.toByteArray();
    }

    static List<RawdataMessage> decode(byte[] segment) throws IOException {
        List<RawdataMessage> messages = new ArrayList<>();
        try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new ByteArrayInputStream(segment), new GenericDatumReader<>(MESSAGE))) {
            GenericRecord read = null;
            while (reader.hasNext()) {
                read = reader.next(read);
                messages.add(toMessage(read));
            }
        }
        return messages;
    }

    private static GenericRecord toRecord(RawdataMessage message) {
        GenericRecord written = new GenericData.Record(MESSAGE);
        written.put(ID_FIELD, new GenericData.Fixed(ID, toBytes(message.id())));
        written.put(ORDERING_GROUP, message.orderingGroup());
        written.put(SEQUENCE_NUMBER, message.sequenceNumber());
        written.put(POSITION, message.position());
        List<GenericRecord> content = new ArrayList<>(message.keys().size());
        for (String key : message.keys()) {
            GenericRecord entry = new GenericData.Record(CONTENT);
            entry.put(NAME, key);
            entry.put(VALUE, ByteBuffer.wrap(message.get(key)));
            content.add(entry);
        }
        written.put(CONTENT_FIELD, new GenericData.Array<>(MESSAGE.getField(CONTENT_FIELD).schema(), content));
        return written;
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
        for (Object item : (List<?>) stored.get(CONTENT_FIELD)) {
            GenericRecord entry = (GenericRecord) item;
            ByteBuffer value = (ByteBuffer) entry.get(VALUE);
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            builder.put(entry.get(NAME).toString(), bytes);
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
