package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataIds;
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
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The Avro file of a segment: one record per message, in publish order, with the schema of 2.x. Only the id's fixed
 * type is named {@code uuid} instead of {@code ulid}: the 16 bytes are a UUIDv7. A message's content is the map
 * {@code data}; its entries are written in the order they were put and read back in that order.
 * <p>
 * The file's header carries what a reader needs without reading the messages, as 2.x's file name did:
 * <ul>
 *     <li>{@code rawdata.segment} and {@code rawdata.previous-id}: the segment the window becomes and the id before it,
 *     for recovery after a crash;</li>
 *     <li>the summary: {@code rawdata.first-id}, the lowest id in the segment; {@code rawdata.last-id}, the greatest
 *     id in the topic up to and with this segment, so it grows from segment to segment even when ids are given out of
 *     order; {@code rawdata.count}; and {@code rawdata.last-block}, the offset of the last block. A seek reads only
 *     headers, and {@code lastMessage} only the last block.</li>
 * </ul>
 * A window's header is written when the window opens, before its messages are known, so the summary is written as
 * placeholders of fixed width and filled in place when the window is sealed. A file whose summary is still the
 * placeholders (count 0) is an open window, never a segment.
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
    /** Header key: the id of the segment's first message. */
    static final String META_FIRST_ID = "rawdata.first-id";
    /** Header key: the id of the segment's last message. */
    static final String META_LAST_ID = "rawdata.last-id";
    /** Header key: how many messages the segment holds. */
    static final String META_COUNT = "rawdata.count";
    /** Header key: the offset of the segment's last block. */
    static final String META_LAST_BLOCK = "rawdata.last-block";

    private static final String NO_ID = "00000000-0000-0000-0000-000000000000";
    private static final String NO_NUMBER = number(0);
    /** More than a header ever takes: the schema, the meta entries and the sync marker. */
    private static final int MAX_HEADER_BYTES = 64 * 1024;
    private static final int SYNC_BYTES = 16;
    /** Avro never ends a block by itself; the writer ends blocks between publish calls. */
    static final int MAX_AVRO_SYNC_INTERVAL = 1 << 30;
    /** Avro's smallest sync interval. */
    private static final int MIN_AVRO_SYNC_INTERVAL = 32;

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
     * What a segment's header says about its messages.
     *
     * @param firstId   the lowest id in the segment
     * @param lastId    the greatest id in the topic up to and with the segment
     * @param count     how many messages, 1 or more
     * @param lastBlock the offset of the last block
     */
    record Summary(UUID firstId, UUID lastId, long count, long lastBlock) {
    }

    /**
     * Opens a segment's Avro file on {@code out}, in blocks that only the caller ends ({@link DataFileWriter#sync()}).
     * Avro sizes its block buffer from the sync interval when the file is created, so the file is created with the
     * expected block size and the interval is raised after: a buffer of the block size that grows if it must, never
     * one of 1 GiB.
     *
     * @param blockBytes about how large a block grows before the caller ends it
     */
    static void create(DataFileWriter<GenericRecord> writer, OutputStream out, int blockBytes) throws IOException {
        writer.setSyncInterval(Math.max(MIN_AVRO_SYNC_INTERVAL, Math.min(blockBytes, MAX_AVRO_SYNC_INTERVAL)));
        writer.create(MESSAGE, out);
        writer.setSyncInterval(MAX_AVRO_SYNC_INTERVAL); // Avro never ends a block by itself
    }

    /**
     * The lower of two ids.
     */
    static UUID lowest(UUID a, UUID b) {
        return RawdataIds.ORDER.compare(a, b) <= 0 ? a : b;
    }

    /**
     * The greater of two ids.
     */
    static UUID greatest(UUID a, UUID b) {
        return RawdataIds.ORDER.compare(a, b) >= 0 ? a : b;
    }

    /**
     * A segment's Avro file in memory, in blocks of about {@code blockBytes}, with its summary.
     */
    static byte[] encode(Collection<RawdataMessage> messages, long segment, UUID previousId, int blockBytes) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        UUID firstId = null;
        UUID lastId = null;
        long blockStart;
        long headerEnd;
        long previousBlockStart = 0;
        try (DataFileWriter<GenericRecord> writer = writer(segment, previousId)) {
            create(writer, out, blockBytes);
            blockStart = writer.sync();
            headerEnd = blockStart;
            long bytes = 0;
            for (RawdataMessage message : messages) {
                if (bytes >= blockBytes) {
                    previousBlockStart = blockStart;
                    blockStart = writer.sync();
                    bytes = 0;
                }
                writer.append(toRecord(message));
                bytes += estimatedSize(message);
                firstId = firstId == null ? message.id() : lowest(firstId, message.id());
                lastId = greatest(lastId == null ? previousId : lastId, message.id());
            }
            if (bytes == 0) {
                blockStart = previousBlockStart;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Writing to memory failed", e);
        }
        byte[] file = out.toByteArray();
        if (!messages.isEmpty()) {
            Summary summary = new Summary(firstId, lastId, messages.size(), blockStart);
            for (Map.Entry<Integer, byte[]> value : summaryValues(Arrays.copyOf(file, (int) headerEnd), summary).entrySet()) {
                System.arraycopy(value.getValue(), 0, file, value.getKey(), value.getValue().length);
            }
        }
        return file;
    }

    /**
     * A writer of a segment's Avro file with its header set, the summary as placeholders; the caller creates it on a
     * stream or a file.
     */
    static DataFileWriter<GenericRecord> writer(long segment, UUID previousId) {
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(MESSAGE));
        writer.setMeta(META_SEGMENT, Long.toString(segment));
        writer.setMeta(META_PREVIOUS_ID, previousId.toString());
        writer.setMeta(META_FIRST_ID, NO_ID);
        writer.setMeta(META_LAST_ID, NO_ID);
        writer.setMeta(META_COUNT, NO_NUMBER);
        writer.setMeta(META_LAST_BLOCK, NO_NUMBER);
        return writer;
    }

    /**
     * Fills in the summary of a closed window file, in place, and forces the file to disk. Only the header is searched,
     * never the messages, whose bytes may hold anything.
     *
     * @param headerBytes the header's length: where the first block starts
     */
    static void fillSummary(Path file, Summary summary, long headerBytes) throws IOException {
        if (headerBytes > MAX_HEADER_BYTES) {
            throw new IOException("The header of " + file + " is " + headerBytes + " bytes, more than a header takes");
        }
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer header = ByteBuffer.allocate((int) headerBytes);
            while (header.hasRemaining()) {
                if (channel.read(header, header.position()) < 0) {
                    throw new IOException("The file " + file + " is shorter than its header");
                }
            }
            for (Map.Entry<Integer, byte[]> value : summaryValues(header.array(), summary).entrySet()) {
                channel.write(ByteBuffer.wrap(value.getValue()), value.getKey());
            }
            channel.force(true);
        }
    }

    /**
     * Where each summary value goes in the header, and its bytes.
     *
     * @param header the header alone
     */
    private static Map<Integer, byte[]> summaryValues(byte[] header, Summary summary) {
        Map<Integer, byte[]> values = new LinkedHashMap<>();
        values.put(valueOffset(header, META_FIRST_ID, NO_ID), ascii(summary.firstId().toString()));
        values.put(valueOffset(header, META_LAST_ID, NO_ID), ascii(summary.lastId().toString()));
        values.put(valueOffset(header, META_COUNT, NO_NUMBER), ascii(number(summary.count())));
        values.put(valueOffset(header, META_LAST_BLOCK, NO_NUMBER), ascii(number(summary.lastBlock())));
        return values;
    }

    /**
     * Finds a meta entry's value in the header: the key, as Avro writes a string (its length, zig-zag encoded in one
     * byte, and its bytes), followed by the length of the placeholder value.
     */
    private static int valueOffset(byte[] header, String key, String placeholder) {
        byte[] name = ascii(key);
        byte[] pattern = new byte[name.length + 2];
        pattern[0] = (byte) (name.length << 1);
        System.arraycopy(name, 0, pattern, 1, name.length);
        pattern[pattern.length - 1] = (byte) (placeholder.length() << 1);
        int found = -1;
        for (int i = 0; i + pattern.length + placeholder.length() <= header.length; i++) {
            if (matches(header, i, pattern)) {
                if (found >= 0) {
                    throw new IllegalStateException("The header has " + key + " twice");
                }
                found = i + pattern.length;
            }
        }
        if (found < 0) {
            throw new IllegalStateException("The header has no " + key + " to fill in");
        }
        return found;
    }

    private static boolean matches(byte[] bytes, int at, byte[] pattern) {
        for (int i = 0; i < pattern.length; i++) {
            if (bytes[at + i] != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads a segment's summary from its header.
     *
     * @throws IOException if the header has no summary: the file is not a sealed segment
     */
    static Summary summary(DataFileStream<GenericRecord> segment) throws IOException {
        long count = Long.parseLong(meta(segment, META_COUNT));
        if (count < 1) {
            throw new IOException("The segment's header has no summary: it is an open window, not a segment");
        }
        return new Summary(UUID.fromString(meta(segment, META_FIRST_ID)), UUID.fromString(meta(segment, META_LAST_ID)),
                count, Long.parseLong(meta(segment, META_LAST_BLOCK)));
    }

    private static String meta(DataFileStream<GenericRecord> segment, String key) throws IOException {
        byte[] value = segment.getMeta(key);
        if (value == null) {
            throw new IOException("The segment's header has no " + key);
        }
        return new String(value, StandardCharsets.US_ASCII);
    }

    /**
     * Decodes the last block of a file: its count, its size, its records (the files are not compressed) and its sync
     * marker. The block must fill the bytes exactly, and its records its size exactly, so a wrong offset or a short
     * range fails instead of decoding garbage.
     *
     * @param block the bytes from the block's start to the end of the file
     */
    static List<RawdataMessage> decodeBlock(byte[] block) throws IOException {
        int[] at = {0};
        long count = readVarLong(block, at);
        long size = readVarLong(block, at);
        if (count < 1 || size < 0 || at[0] + size + SYNC_BYTES != block.length) {
            throw new IOException("Not the last block of a segment: " + count + " records of " + size + " bytes in "
                    + block.length + " bytes");
        }
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(block, at[0], (int) size, null);
        GenericDatumReader<GenericRecord> reader = datumReader();
        List<RawdataMessage> messages = new ArrayList<>();
        GenericRecord read = null;
        try {
            for (long i = 0; i < count; i++) {
                read = reader.read(read, decoder);
                messages.add(toMessage(read));
            }
            if (!decoder.isEnd()) {
                throw new IOException("The block's records end before its size");
            }
        } catch (AvroRuntimeException | IndexOutOfBoundsException e) {
            throw new IOException("The block does not decode: " + e.getMessage(), e);
        }
        return messages;
    }

    /**
     * Reads a long as Avro writes it: a zig-zag variable-length integer.
     */
    private static long readVarLong(byte[] bytes, int[] at) throws IOException {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (at[0] >= bytes.length) {
                throw new IOException("A block header cut short");
            }
            int b = bytes[at[0]++] & 0xff;
            value |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return (value >>> 1) ^ -(value & 1);
            }
        }
        throw new IOException("A malformed block header");
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

    static DataFileStream<GenericRecord> reader(InputStream in) throws IOException {
        return new DataFileStream<>(in, datumReader());
    }

    private static GenericDatumReader<GenericRecord> datumReader() {
        return new GenericDatumReader<>(MESSAGE) {
            @Override
            protected Object newMap(Object old, int size) {
                return new LinkedHashMap<>(); // keep the order the entries were put in
            }
        };
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

    static RawdataMessage toMessage(GenericRecord stored) {
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

    private static String number(long value) {
        return String.format("%019d", value);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
