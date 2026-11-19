package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
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
        volatile boolean failNextRead;

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
                public byte[] read(long offset, int length) throws IOException {
                    if (failNextRead) {
                        failNextRead = false;
                        throw new IOException("the store failed once");
                    }
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
        try (AvroRawdataClient client = new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, Long.MAX_VALUE, 16 * 1024, 4, 30, 16), 10)) {
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
        return new AvroRawdataClient(store, new AvroUploadWindow(folder.resolve("reader"), 3600, 1 << 20, 64, 4, 30, 16), 10);
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

    /**
     * Ids given out of order, as in 2.x: a segment keeps publish order, its header keeps its lowest id and the topic's
     * greatest so far, and a cursor at a message reads everything published after it, also a lower id in a later
     * segment.
     *
     * @throws IOException if reading a header fails
     */
    @Test
    void aCursorReadsWhatWasPublishedAfterItsMessageAlsoLowerIds() throws IOException {
        CountingStore store = new CountingStore();
        UUID early = RawdataIds.next(RawdataIds.beginningOf(1_000_000L));
        UUID b;
        try (AvroRawdataClient client = new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, Long.MAX_VALUE, 64, 4, 30, 16), 10)) {
            RawdataProducer producer = client.producer(TOPIC);
            producer.publish(message("a"), message("b"));
            client.flush(TOPIC);
            b = client.lastMessage(TOPIC).id();
            producer.publish(RawdataMessage.builder().id(early).position("late").build(), message("c"));
            client.flush(TOPIC);
            assertEquals("c", client.lastMessage(TOPIC).position(), "the last message is the last one read");
            try (DataFileStream<GenericRecord> second = AvroSegments.reader(new ByteArrayInputStream(store.segments.get(1L)))) {
                AvroSegments.Summary summary = AvroSegments.summary(second);
                assertEquals(early, summary.firstId(), "the lowest id of the segment");
                assertTrue(RawdataIds.ORDER.compare(summary.lastId(), b) > 0, "the topic's greatest id");
            }
        }
        try (AvroRawdataClient reader = client(store);
             RawdataConsumer consumer = reader.consumer(TOPIC, reader.cursorOf(TOPIC, b, false))) {
            assertEquals("late", receive(consumer).position());
            assertEquals("c", receive(consumer).position());
            assertNull(receive(consumer));
        }
        try (AvroRawdataClient reader = client(store);
             RawdataConsumer consumer = reader.consumer(TOPIC, reader.cursorOf(TOPIC, early, false))) {
            assertEquals("c", receive(consumer).position(), "a cursor at the late message goes on after it");
            assertNull(receive(consumer));
        }
    }

    /**
     * A message may hold any bytes, also the header's own keys: filling in the summary searches the header only.
     *
     * @throws IOException if encoding fails
     */
    @Test
    void aMessageHoldingTheHeaderKeysIsWrittenAndRecovered() throws IOException {
        byte[] header = AvroSegments.encode(List.of(RawdataMessage.builder().id(RawdataIds.next(RawdataIds.beginningOfTime())).position("x").put("k", new byte[1]).build()),
                0, RawdataIds.beginningOfTime(), 64); // its header holds every key
        CountingStore store = new CountingStore();
        RawdataMessage tricky = RawdataMessage.builder().position("tricky").put("payload", header).build();
        try (AvroRawdataClient client = new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, Long.MAX_VALUE, 64, 4, 30, 16), 10)) {
            client.producer(TOPIC).publish(tricky, message("after"));
            client.flush(TOPIC);
            assertEquals("after", client.lastMessage(TOPIC).position());
        }
        List<RawdataMessage> recovered = List.of(RawdataMessage.builder().id(RawdataIds.next(RawdataIds.beginningOfTime()))
                .position("tricky").put("payload", header).build());
        byte[] encoded = AvroSegments.encode(recovered, 1, RawdataIds.beginningOfTime(), 64);
        try (DataFileStream<GenericRecord> stream = AvroSegments.reader(new ByteArrayInputStream(encoded))) {
            assertEquals(1, AvroSegments.summary(stream).count());
        }
    }

    /**
     * A range read that fails leaves the consumer where it was: the next receive goes on after the last message it
     * gave, without a gap or a repeat.
     */
    @Test
    void aConsumerGoesOnAfterAFailedRead() {
        CountingStore store = new CountingStore();
        List<UUID> ids = write(store);
        List<UUID> read = new ArrayList<>();
        try (AvroRawdataClient reader = client(store); RawdataConsumer consumer = reader.consumer(TOPIC)) {
            for (int i = 0; i < 5; i++) {
                read.add(receive(consumer).id());
            }
            int failures = 0;
            for (int segment = 0; segment < SEGMENTS; segment++) {
                store.failNextRead = true; // the next range read fails: here the next segment's first
                for (; ; ) {
                    RawdataMessage message;
                    try {
                        message = receive(consumer);
                    } catch (io.descoped.rawdata.api.RawdataException e) {
                        failures++;
                        continue;
                    }
                    if (message == null) {
                        break;
                    }
                    read.add(message.id());
                    if (read.size() % (CALLS_PER_SEGMENT * 2) == 0) {
                        break;
                    }
                }
            }
            assertTrue(failures > 0, "the store failed");
        }
        assertEquals(ids, read);
    }

    /**
     * A header that does not fit in the first range is read with a larger one.
     *
     * @throws IOException if reading fails
     */
    @Test
    void aHeaderLargerThanTheFirstRangeIsRead() throws IOException {
        CountingStore store = new CountingStore();
        List<UUID> ids = write(store);
        AvroSegments.Summary summary = new AvroSegmentReader(store.open(TOPIC, 0).orElseThrow()).summary(64);
        assertEquals(ids.get(0), summary.firstId());
    }

    /**
     * The last block is checked: bytes from another offset do not decode as a block.
     */
    @Test
    void aLastBlockFromTheWrongOffsetFails() throws IOException {
        CountingStore store = new CountingStore();
        write(store);
        byte[] content = store.segments.get(0L);
        long lastBlock = new AvroSegmentReader(store.open(TOPIC, 0).orElseThrow()).summary().lastBlock();
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                () -> AvroSegments.decodeBlock(Arrays.copyOfRange(content, (int) lastBlock + 7, content.length)));
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                () -> AvroSegments.decodeBlock(Arrays.copyOfRange(content, (int) lastBlock, content.length - 1)));
    }
}
