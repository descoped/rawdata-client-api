package io.descoped.rawdata.provider.avro.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataProducer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the contract does not cover: several clients on one folder, as several processes would be, the files on disk,
 * metadata names, and the configuration.
 */
@Timeout(120)
class FilesystemProviderTest {

    @TempDir
    Path folder;

    private Map<String, String> configuration() {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", folder.toString(),
                "filesystem.poll-interval-ms", "50");
    }

    private RawdataClient client() {
        return RawdataClient.create(configuration());
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", position.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static List<RawdataMessage> readAll(RawdataClient client, String topic) throws InterruptedException {
        try (RawdataConsumer consumer = client.consumer(topic)) {
            List<RawdataMessage> read = new ArrayList<>();
            List<RawdataMessage> batch;
            while (!(batch = consumer.receiveUpTo(1000, Duration.ofMillis(500))).isEmpty()) {
                read.addAll(batch);
            }
            return read;
        }
    }

    private static void assertIncreasing(List<RawdataMessage> messages) {
        for (int i = 1; i < messages.size(); i++) {
            assertTrue(RawdataIds.ORDER.compare(messages.get(i - 1).id(), messages.get(i).id()) < 0, "ids must strictly increase at " + i);
        }
    }

    /**
     * Clients that share a folder share nothing else, as clients in different processes would not. Each publish
     * still goes after the last, so nothing is lost, ids increase in the order the segments were created, and each
     * producer's own order is kept.
     */
    @Test
    void clientsOnOneFolderKeepTheTopicInCommitOrder() throws Exception {
        int clients = 3;
        int producersPerClient = 2;
        int perProducer = 40;
        List<RawdataClient> opened = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(clients * producersPerClient);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                RawdataClient client = client();
                opened.add(client);
                for (int p = 0; p < producersPerClient; p++) {
                    String prefix = "c" + c + "p" + p + "-";
                    futures.add(executor.submit(() -> {
                        start.await();
                        try (RawdataProducer producer = client.producer("shared")) {
                            for (int i = 0; i < perProducer; i++) {
                                producer.publish(message(prefix + i));
                            }
                        }
                        return null;
                    }));
                }
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            opened.forEach(RawdataClient::close);
        }
        try (RawdataClient client = client()) {
            List<RawdataMessage> read = readAll(client, "shared");
            assertEquals(clients * producersPerClient * perProducer, read.size());
            assertIncreasing(read);
            for (int c = 0; c < clients; c++) {
                for (int p = 0; p < producersPerClient; p++) {
                    String prefix = "c" + c + "p" + p + "-";
                    List<String> own = read.stream().map(RawdataMessage::position).filter(position -> position.startsWith(prefix)).toList();
                    List<String> expected = new ArrayList<>();
                    for (int i = 0; i < perProducer; i++) {
                        expected.add(prefix + i);
                    }
                    assertEquals(expected, own);
                }
            }
        }
    }

    /**
     * A client that is behind, because another wrote since, finds the new last segment and writes after it.
     */
    @Test
    void aProducerBehindAnotherWriterWritesAfterIt() throws Exception {
        try (RawdataClient first = client(); RawdataClient second = client()) {
            first.producer("t").publish(message("a"));
            second.producer("t").publish(message("b")); // second has seen nothing of the topic
            first.producer("t").publish(message("c")); // first has seen only its own segment
            second.producer("t").publish(message("d"));
            List<RawdataMessage> read = readAll(first, "t");
            assertEquals(List.of("a", "b", "c", "d"), read.stream().map(RawdataMessage::position).toList());
            assertIncreasing(read);
        }
        try (Stream<Path> segments = Files.list(folder.resolve("t").resolve("segments"))) {
            assertEquals(List.of("0000000000000000000.avro", "0000000000000000001.avro", "0000000000000000002.avro", "0000000000000000003.avro"),
                    segments.map(path -> path.getFileName().toString()).sorted().toList());
        }
    }

    /**
     * A consumer of one client receives what another client publishes, by polling.
     */
    @Test
    void aConsumerSeesMessagesOfAnotherClient() throws Exception {
        try (RawdataClient reader = client(); RawdataClient writer = client();
             RawdataConsumer consumer = reader.consumer("t")) {
            CompletableFuture<? extends RawdataMessage> received = consumer.receiveAsync();
            Thread.sleep(200);
            writer.producer("t").publish(message("a"));
            assertEquals("a", received.get(30, TimeUnit.SECONDS).position());
        }
    }

    /**
     * Many publishes in a row, most in the same millisecond, are all read, in order: files that start in the same
     * millisecond do not hide each other.
     */
    @Test
    void manySmallPublishesAreAllRead() throws Exception {
        try (RawdataClient client = client()) {
            try (RawdataProducer producer = client.producer("t")) {
                for (int i = 0; i < 300; i++) {
                    producer.publish(message("m" + i));
                }
            }
            List<RawdataMessage> read = readAll(client, "t");
            assertEquals(300, read.size());
            for (int i = 0; i < 300; i++) {
                assertEquals("m" + i, read.get(i).position());
            }
        }
    }

    /**
     * Seek and a position search find their place among many segments.
     */
    @Test
    void seekAndPositionSearchFindTheirSegment() throws Exception {
        long base = System.currentTimeMillis() - 1_000_000;
        try (RawdataClient client = client()) {
            try (RawdataProducer producer = client.producer("t")) {
                for (int i = 0; i < 100; i++) {
                    producer.publish(RawdataMessage.builder().id(RawdataIds.beginningOf(base + 10L * i)).position("m" + i).build());
                }
            }
            try (RawdataConsumer consumer = client.consumer("t")) {
                consumer.seek(base + 505);
                assertEquals("m51", consumer.receive(5, TimeUnit.SECONDS).position());
                consumer.seek(base + 990);
                assertEquals("m99", consumer.receive(5, TimeUnit.SECONDS).position());
                assertNull(consumer.receive(100, TimeUnit.MILLISECONDS));
                consumer.seek(base);
                assertEquals("m0", consumer.receive(5, TimeUnit.SECONDS).position());
            }
            try (RawdataConsumer consumer = client.consumer("t", "m70", base + 700, Duration.ZERO)) {
                assertEquals("m71", consumer.receive(5, TimeUnit.SECONDS).position());
            }
            try (RawdataConsumer consumer = client.consumer("t", RawdataIds.beginningOf(base + 300), true)) {
                assertEquals("m30", consumer.receive(5, TimeUnit.SECONDS).position());
            }
        }
    }

    /**
     * A topic is a folder of segments, one per publish, each a plain Avro file, and metadata; no temporary file is
     * left behind.
     */
    @Test
    void aTopicIsAFolderOfAvroFiles() throws Exception {
        UUID id = RawdataIds.generate();
        try (RawdataClient client = client()) {
            client.producer("t").publish(
                    RawdataMessage.builder().id(id).position("a").put("x", new byte[]{1}).put("y", new byte[]{2}).build(),
                    message("b"));
            client.metadata("t").put("checkpoint", new byte[]{7});
        }
        Path topic = folder.resolve("t");
        try (Stream<Path> files = Files.list(topic)) {
            assertEquals(Set.of("segments", "metadata"), Set.copyOf(files.map(path -> path.getFileName().toString()).toList()));
        }
        Path segment = topic.resolve("segments").resolve("0000000000000000000.avro");
        List<GenericRecord> records = new ArrayList<>();
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new SeekableFileInput(segment.toFile()), new GenericDatumReader<>())) {
            reader.forEach(records::add);
        }
        assertEquals(2, records.size());
        assertEquals("a", records.get(0).get("position").toString());
        assertEquals("b", records.get(1).get("position").toString());
        GenericRecord first = records.get(0);
        byte[] idBytes = ((GenericData.Fixed) first.get("id")).bytes();
        assertEquals(id.getMostSignificantBits(), ByteBuffer.wrap(idBytes).getLong());
        assertEquals(2, ((List<?>) first.get("content")).size());
    }

    /**
     * Metadata keys may be any string, distinct by case, and do not reach the file system as given.
     */
    @Test
    void metadataKeysAreAnyString() throws Exception {
        List<String> keys = List.of("Key", "key", "", "..", "../escape", "a/b", "ü € 😀", ".hidden");
        try (RawdataClient client = client()) {
            RawdataMetadataClient metadata = client.metadata("t");
            for (int i = 0; i < keys.size(); i++) {
                metadata.put(keys.get(i), new byte[]{(byte) i});
            }
            assertEquals(Set.copyOf(keys), metadata.keys());
            for (int i = 0; i < keys.size(); i++) {
                assertArrayEquals(new byte[]{(byte) i}, metadata.get(keys.get(i)));
            }
            metadata.remove("..");
            assertNull(metadata.get(".."));
            assertNotNull(metadata.get("../escape"));
        }
        try (Stream<Path> files = Files.list(folder)) {
            assertEquals(List.of("t"), files.map(path -> path.getFileName().toString()).toList());
        }
    }

    /**
     * A metadata key is at most 127 bytes in UTF-8, so that its file name fits in 255 characters.
     */
    @Test
    void aTooLongMetadataKeyIsRefused() {
        try (RawdataClient client = client()) {
            RawdataMetadataClient metadata = client.metadata("t");
            metadata.put("k".repeat(127), new byte[]{1});
            assertArrayEquals(new byte[]{1}, metadata.get("k".repeat(127)));
            assertThrows(IllegalArgumentException.class, () -> metadata.put("k".repeat(128), new byte[]{1}));
        }
    }

    /**
     * The storage folder is required, created when missing, and the poll interval must be positive.
     */
    @Test
    void configurationIsChecked() {
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                () -> RawdataClient.create(Map.of(RawdataClient.PROVIDER_KEY, "filesystem")));
        assertTrue(missing.getMessage().contains("filesystem.storage-folder"), missing.getMessage());
        Map<String, String> badInterval = new HashMap<>(configuration());
        badInterval.put("filesystem.poll-interval-ms", "0");
        IllegalArgumentException interval = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(badInterval));
        assertTrue(interval.getMessage().contains("filesystem.poll-interval-ms"), interval.getMessage());
        Map<String, String> nested = new HashMap<>(configuration());
        nested.put("filesystem.storage-folder", folder.resolve("a").resolve("b").toString());
        try (RawdataClient client = RawdataClient.create(nested)) {
            client.producer("t").publish(message("a"));
        }
        assertTrue(Files.isDirectory(folder.resolve("a").resolve("b").resolve("t").resolve("segments")));
    }
}
