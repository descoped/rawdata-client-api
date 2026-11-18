package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Large segments are read in ranges: a seek reads only headers, {@code lastMessage} only the header and last block,
 * and the header's summary matches the messages. A store that counts the bytes it hands out shows it.
 */
@Timeout(60)
class AvroRangedReadTest {

    private static final String TOPIC = "t";
    private static final int SEGMENTS = 6;
    private static final int CALLS_PER_SEGMENT = 20;
    private static final int PAYLOAD = 10 * 1024;

    @TempDir
    Path folder;

    /**
     * A store in memory that counts the bytes read from its segments.
     */
    static final class CountingStore implements AvroStore {

        final Map<Long, byte[]> segments = new ConcurrentHashMap<>();
        final AtomicLong bytesRead = new AtomicLong();

        @Override
        public boolean create(String topic, long segment, byte[] content) {
            byte[] stored = segments.putIfAbsent(segment, content.clone());
            return stored == null || Arrays.equals(stored, content);
        }

        @Override
        public Optional<Segment> open(String topic, long segment) {
            byte[] content = segments.get(segment);
            if (content == null) {
                return Optional.empty();
            }
            return Optional.of(new Segment() {
                @Override
                public long size() {
                    return content.length;
                }

                @Override
                public byte[] read(long offset, int length) {
                    byte[] range = Arrays.copyOfRange(content, (int) offset, (int) Math.min(content.length, offset + length));
                    bytesRead.addAndGet(range.length);
                    return range;
                }
            });
        }

        @Override
        public boolean exists(String topic, long segment) {
            return segments.containsKey(segment);
        }

        @Override
        public Set<String> metadataNames(String topic) {
            return new TreeSet<>();
        }

        @Override
        public Optional<byte[]> readMetadata(String topic, String name) {
            return Optional.empty();
        }

        @Override
        public void writeMetadata(String topic, String name, byte[] value) {
            // not used
        }

        @Override
        public void deleteMetadata(String topic, String name) {
            // not used
        }

        @Override
        public void close() {
            // nothing to release
        }

        long totalBytes() {
            return segments.values().stream().mapToLong(content -> content.length).sum();
        }
    }

    /**
     * Writes {@value #SEGMENTS} segments of {@value #CALLS_PER_SEGMENT} calls of two messages, each segment in blocks
     * of about 16 KiB.
     *
     * @return the ids, in order
     */
    private List<UUID> write(CountingStore store) {
        List<UUID> ids = new ArrayList<>();
        try (AvroRawdataClient client = new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, Long.MAX_VALUE, 16 * 1024, 4, 30), 10)) {
            RawdataProducer producer = client.producer(TOPIC);
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int call = 0; call < CALLS_PER_SEGMENT; call++) {
                    String position = segment + "-" + call;
                    producer.publish(message(position + "a"), message(position + "b"));
                }
                client.flush(TOPIC);
            }
        }
        try (AvroRawdataClient reader = client(store); RawdataConsumer consumer = reader.consumer(TOPIC)) {
            RawdataMessage message;
            while ((message = receive(consumer)) != null) {
                ids.add(message.id());
            }
        }
        return ids;
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", new byte[PAYLOAD]).build();
    }

    private static RawdataMessage receive(RawdataConsumer consumer) {
        try {
            return consumer.receive(200, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private AvroRawdataClient client(CountingStore store) {
        return new AvroRawdataClient(store, new AvroUploadWindow(folder.resolve("reader"), 3600, 1 << 20, 64, 4, 30), 10);
    }

    /**
     * Each segment's header holds its first and last id, its count and where its last block starts.
     *
     * @throws IOException if decoding fails
     */
    @Test
    void theHeaderSummarisesTheSegment() throws IOException {
        CountingStore store = new CountingStore();
        List<UUID> ids = write(store);
        assertEquals(SEGMENTS, store.segments.size());
        assertEquals(SEGMENTS * CALLS_PER_SEGMENT * 2, ids.size());
        int perSegment = CALLS_PER_SEGMENT * 2;
        for (long segment = 0; segment < SEGMENTS; segment++) {
            byte[] content = store.segments.get(segment);
            AvroSegments.Summary summary;
            try (DataFileStream<GenericRecord> stream = AvroSegments.reader(new ByteArrayInputStream(content))) {
                summary = AvroSegments.summary(stream);
            }
            int first = (int) segment * perSegment;
            assertEquals(ids.get(first), summary.firstId());
            assertEquals(ids.get(first + perSegment - 1), summary.lastId());
            assertEquals(perSegment, summary.count());
            byte[] lastBlock = Arrays.copyOfRange(content, (int) summary.lastBlock(), content.length);
            List<RawdataMessage> inLastBlock = AvroSegments.decodeBlock(lastBlock);
            assertTrue(inLastBlock.size() < perSegment, "a segment of many blocks");
            assertEquals(summary.lastId(), inLastBlock.get(inLastBlock.size() - 1).id());
            assertEquals(0, inLastBlock.size() % 2, "a block holds whole calls");
        }
    }

    /**
     * {@code lastMessage} reads the last segment's header and last block, not the segment.
     */
    @Test
    void theLastMessageReadsTheLastBlockOnly() {
        CountingStore store = new CountingStore();
        List<UUID> ids = write(store);
        try (AvroRawdataClient reader = client(store)) {
            store.bytesRead.set(0);
            RawdataMessage last = reader.lastMessage(TOPIC);
            assertEquals(ids.get(ids.size() - 1), last.id());
            long segmentBytes = store.segments.get((long) SEGMENTS - 1).length;
            assertTrue(store.bytesRead.get() < segmentBytes / 2,
                    store.bytesRead.get() + " bytes read of a segment of " + segmentBytes);
        }
    }

    /**
     * A seek finds its segment by reading headers, then streams from that segment on.
     */
    @Test
    void aSeekReadsHeadersToFindItsSegment() {
        CountingStore store = new CountingStore();
        List<UUID> ids = write(store);
        UUID target = ids.get(ids.size() - 3); // in the last segment
        try (AvroRawdataClient reader = client(store);
             RawdataConsumer consumer = reader.consumer(TOPIC, reader.cursorOf(TOPIC, target, true))) {
            store.bytesRead.set(0);
            assertEquals(target, receive(consumer).id());
            long lastSegment = store.segments.get((long) SEGMENTS - 1).length;
            long headers = (long) SEGMENTS * AvroSegmentReader.HEADER_RANGE;
            assertTrue(store.bytesRead.get() <= lastSegment + headers,
                    store.bytesRead.get() + " bytes read; the last segment is " + lastSegment + " of " + store.totalBytes());
            assertEquals(ids.get(ids.size() - 2), receive(consumer).id());
            assertEquals(ids.get(ids.size() - 1), receive(consumer).id());
            assertNull(receive(consumer));
        }
    }
}
