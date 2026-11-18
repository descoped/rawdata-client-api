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

    static final Schema ID = SchemaBuilder.fixed("uuid").namespace("io.descoped.rawdata").size(16);

    static final Schema CONTENT = SchemaBuilder.record("Content").namespace("io.descoped.rawdata")
            .fields()
            .name("name").type().stringType().noDefault()
            .name("value").type().bytesType().noDefault()
            .endRecord();

    static final Schema MESSAGE = SchemaBuilder.record("RawdataMessage").namespace("io.descoped.rawdata")
            .fields()
            .name("id").type(ID).noDefault()
            .name("orderingGroup").type().nullable().stringType().noDefault()
            .name("sequenceNumber").type().longType().noDefault()
            .name("position").type().stringType().noDefault()
            .name("content").type().array().items(CONTENT).noDefault()
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
            GenericRecord record = null;
            while (reader.hasNext()) {
                record = reader.next(record);
                messages.add(toMessage(record));
            }
        }
        return messages;
    }

    private static GenericRecord toRecord(RawdataMessage message) {
        GenericRecord record = new GenericData.Record(MESSAGE);
        record.put("id", new GenericData.Fixed(ID, toBytes(message.id())));
        record.put("orderingGroup", message.orderingGroup());
        record.put("sequenceNumber", message.sequenceNumber());
        record.put("position", message.position());
        List<GenericRecord> content = new ArrayList<>(message.keys().size());
        for (String key : message.keys()) {
            GenericRecord entry = new GenericData.Record(CONTENT);
            entry.put("name", key);
            entry.put("value", ByteBuffer.wrap(message.get(key)));
            content.add(entry);
        }
        record.put("content", new GenericData.Array<>(MESSAGE.getField("content").schema(), content));
        return record;
    }

    private static RawdataMessage toMessage(GenericRecord record) {
        RawdataMessage.Builder builder = RawdataMessage.builder()
                .id(fromBytes(((GenericData.Fixed) record.get("id")).bytes()))
                .sequenceNumber((Long) record.get("sequenceNumber"))
                .position(record.get("position").toString());
        Object orderingGroup = record.get("orderingGroup");
        if (orderingGroup != null) {
            builder.orderingGroup(orderingGroup.toString());
        }
        for (Object item : (List<?>) record.get("content")) {
            GenericRecord entry = (GenericRecord) item;
            ByteBuffer value = (ByteBuffer) entry.get("value");
            byte[] bytes = new byte[value.remaining()];
            value.duplicate().get(bytes);
            builder.put(entry.get("name").toString(), bytes);
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
