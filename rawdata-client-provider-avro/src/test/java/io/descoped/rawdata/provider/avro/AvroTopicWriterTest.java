package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataWriterConflictException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The upload window's failure paths, which a real store cannot be made to hit on cue: a store whose uploads wait for
 * the test, then fail or find their segment taken.
 */
@Timeout(60)
class AvroTopicWriterTest {

    @TempDir
    Path folder;

    /**
     * A store in memory whose uploads can be held back, failed or refused.
     */
    static final class GatedStore implements AvroStore {

        enum Outcome { CREATE, FAIL, TAKEN }

        final Map<String, byte[]> segments = new ConcurrentHashMap<>();
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch uploading = new CountDownLatch(1);
        volatile Outcome outcome = Outcome.CREATE;
        volatile boolean gated;

        @Override
        public boolean create(String topic, long segment, byte[] content) throws IOException {
            uploading.countDown();
            if (gated) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
            switch (outcome) {
                case FAIL:
                    throw new IOException("the store is down");
                case TAKEN:
                    return false;
                default:
                    byte[] stored = segments.putIfAbsent(topic + "/" + segment, content.clone());
                    return stored == null || Arrays.equals(stored, content);
            }
        }

        @Override
        public Optional<Segment> open(String topic, long segment) {
            byte[] content = segments.get(topic + "/" + segment);
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
                    return Arrays.copyOfRange(content, (int) offset, (int) Math.min(content.length, offset + length));
                }
            });
        }

        @Override
        public boolean exists(String topic, long segment) {
            return segments.containsKey(topic + "/" + segment);
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
    }

    private AvroRawdataClient client(GatedStore store, long maxBytes, int maxPending) {
        return new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, maxBytes, 64, maxPending, 30), 10);
    }

    private static RawdataMessage message(String position, int size) {
        return RawdataMessage.builder().position(position).put("payload", new byte[size]).build();
    }

    private List<String> files() throws IOException {
        try (Stream<Path> files = Files.list(folder.resolve("t"))) {
            return files.map(file -> file.getFileName().toString()).filter(name -> !name.equals(".lock")).sorted().toList();
        }
    }

    /**
     * An upload that fails while a newer window is open fails the writer; closing throws and keeps both windows on
     * disk, sealed, for the next writer.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aFailedUploadKeepsTheOpenWindowSealedToo() throws Exception {
        GatedStore store = new GatedStore();
        store.gated = true;
        store.outcome = GatedStore.Outcome.FAIL;
        AvroRawdataClient client = client(store, 4000, 4); // above the Avro header
        RawdataProducer producer = client.producer("t");
        producer.publish(message("large", 5000)); // over the size: sealed, its upload waits
        assertTrue(store.uploading.await(10, TimeUnit.SECONDS));
        producer.publish(message("small", 1)); // a new window, open
        store.release.countDown();
        assertThrows(RawdataException.class, client::close);
        assertEquals(List.of("0000000000000000000.sealed.avro", "0000000000000000001.sealed.avro"), files());
    }

    /**
     * An upload that finds its segment taken is another writer: the windows not uploaded, the open one too, are set
     * aside, and closing reports the conflict.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aTakenSegmentSetsAsideEveryWindow() throws Exception {
        GatedStore store = new GatedStore();
        store.gated = true;
        store.outcome = GatedStore.Outcome.TAKEN;
        AvroRawdataClient client = client(store, 4000, 4); // above the Avro header
        RawdataProducer producer = client.producer("t");
        producer.publish(message("large", 5000));
        assertTrue(store.uploading.await(10, TimeUnit.SECONDS));
        producer.publish(message("small", 1)); // a new window, open
        store.release.countDown();
        assertThrows(RawdataWriterConflictException.class, client::close);
        // The open window is set aside open or sealed, as the close and the failed upload happen to meet.
        List<String> files = files();
        assertEquals(2, files.size(), files.toString());
        assertTrue(files.get(0).startsWith("0000000000000000000.") && files.get(1).startsWith("0000000000000000001."), files.toString());
        assertTrue(files.stream().allMatch(name -> name.endsWith(".avro.invalid")), files.toString());
    }

    /**
     * When as many sealed windows wait for upload as {@code avro-file.max.pending-uploads} allows, a publish waits
     * until one is uploaded.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aPublishWaitsWhileTooManyWindowsWaitForUpload() throws Exception {
        GatedStore store = new GatedStore();
        store.gated = true;
        try (AvroRawdataClient client = client(store, 1, 1)) {
            RawdataProducer producer = client.producer("t");
            producer.publish(message("a", 1)); // sealed; its upload waits
            assertTrue(store.uploading.await(10, TimeUnit.SECONDS));
            Thread second = new Thread(() -> producer.publish(message("b", 1)));
            second.start();
            second.join(300);
            assertTrue(second.isAlive(), "waits for the upload before it");
            store.release.countDown();
            second.join(TimeUnit.SECONDS.toMillis(10));
            assertFalse(second.isAlive());
            client.flush("t");
            assertEquals(2, store.segments.size());
        }
    }

    /**
     * Closing a producer waits at most {@code avro-file.close-timeout-seconds} for the uploads; the uploads go on, and
     * the client can still publish. Closing the client waits as long, then stops; the window stays on disk and the
     * next writer uploads it.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void closingWaitsAtMostTheCloseTimeout() throws Exception {
        GatedStore store = new GatedStore();
        store.gated = true;
        AvroRawdataClient client = new AvroRawdataClient(store, new AvroUploadWindow(folder, 3600, 1 << 20, 64, 4, 1), 10);
        RawdataProducer producer = client.producer("t");
        producer.publish(message("a", 1));
        long started = System.nanoTime();
        RawdataException timedOut = assertThrows(RawdataException.class, producer::close);
        assertTrue(timedOut.getMessage().contains("timed out"), timedOut.getMessage());
        client.producer("t").publish(message("b", 1)); // the writer goes on

        RawdataException closing = assertThrows(RawdataException.class, client::close);
        assertTrue(closing.getMessage().contains("timed out"), closing.getMessage());
        assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started) < 10, "bounded by the timeout");
        store.release.countDown();
        List<String> left = files();
        assertTrue(left.contains("0000000000000000001.sealed.avro"), left.toString()); // never uploaded

        GatedStore after = new GatedStore();
        after.segments.putAll(store.segments);
        try (AvroRawdataClient next = client(after, 1 << 20, 4)) {
            next.producer("t").publish(message("c", 1));
            next.flush("t");
        }
        assertEquals(List.of(), files(), "the next writer uploaded what was left");
    }

    /**
     * Leftovers that hold no message (a header cut short, a file without the window's header) are removed, and the
     * writer starts at the topic's end.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void leftoversWithoutMessagesAreRemoved() throws Exception {
        Path topic = Files.createDirectories(folder.resolve("t"));
        Files.write(topic.resolve("0000000000000000000.window.avro"), new byte[]{'O', 'b', 'j'});
        byte[] noHeader = AvroSegments.encode(List.of(), 1, io.descoped.rawdata.api.RawdataIds.beginningOfTime(), 64);
        Files.write(topic.resolve("0000000000000000001.sealed.avro"), noHeader);
        GatedStore store = new GatedStore();
        try (AvroRawdataClient client = client(store, 1 << 20, 4)) {
            client.producer("t").publish(message("a", 1));
            client.flush("t");
        }
        assertEquals(List.of(), files());
        assertEquals(Set.of("t/0"), store.segments.keySet());
    }
}
