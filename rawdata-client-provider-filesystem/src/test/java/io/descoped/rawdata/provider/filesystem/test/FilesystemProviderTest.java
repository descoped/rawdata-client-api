package io.descoped.rawdata.provider.filesystem.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataMetadataClient;
import org.apache.avro.Schema;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider's internals, which the TCK cannot see: the files on disk, metadata file names, and the configuration.
 */
@Timeout(120)
class FilesystemProviderTest {

    @TempDir
    Path folder;

    private Map<String, String> configuration() {
        return configuration(folder, folder.resolve("buffer-" + UUID.randomUUID()));
    }

    private static Map<String, String> configuration(Path storage, Path buffer) {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", storage.toString(),
                "local-temp-folder", buffer.toString(),
                "avro-file.max.seconds", "1",
                "listing.min-interval-seconds", "0");
    }

    private RawdataClient client() {
        return RawdataClient.create(configuration());
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", position.getBytes(StandardCharsets.UTF_8)).build();
    }

    /**
     * A topic is a folder of segments, one per upload window, each a plain Avro file, and metadata; no temporary file
     * is left behind.
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
        Schema schema;
        try (DataFileReader<GenericRecord> reader = new DataFileReader<>(new SeekableFileInput(segment.toFile()), new GenericDatumReader<>())) {
            schema = reader.getSchema();
            reader.forEach(records::add);
        }
        // the 2.x schema, the id's fixed type named uuid
        assertEquals("RawdataMessage", schema.getFullName());
        assertEquals(List.of("id", "orderingGroup", "sequenceNumber", "position", "data"),
                schema.getFields().stream().map(Schema.Field::name).toList());
        assertEquals("uuid", schema.getField("id").schema().getName());
        assertEquals(16, schema.getField("id").schema().getFixedSize());
        assertEquals(0L, ((Number) schema.getField("sequenceNumber").defaultVal()).longValue());
        assertEquals(Schema.Type.MAP, schema.getField("data").schema().getType());
        assertEquals(2, records.size());
        assertEquals("a", records.get(0).get("position").toString());
        assertEquals("b", records.get(1).get("position").toString());
        GenericRecord first = records.get(0);
        byte[] idBytes = ((GenericData.Fixed) first.get("id")).bytes();
        assertEquals(id.getMostSignificantBits(), ByteBuffer.wrap(idBytes).getLong());
        assertEquals(2, ((Map<?, ?>) first.get("data")).size());
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
     * The storage folder and the local temp folder are required, the storage folder is created when missing, and
     * numbers are read strictly, as dynamic-configuration reads them.
     */
    @Test
    void configurationIsChecked() {
        Map<String, String> noFolder = Map.of(RawdataClient.PROVIDER_KEY, "filesystem");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noFolder));
        assertTrue(missing.getMessage().contains("filesystem.storage-folder"), missing.getMessage());
        Map<String, String> badInterval = new HashMap<>(configuration());
        badInterval.put("listing.min-interval-seconds", "-1");
        IllegalArgumentException interval = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(badInterval));
        assertTrue(interval.getMessage().contains("listing.min-interval-seconds"), interval.getMessage());
        Map<String, String> noBuffer = new HashMap<>(configuration());
        noBuffer.remove("local-temp-folder");
        IllegalArgumentException buffer = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noBuffer));
        assertTrue(buffer.getMessage().contains("local-temp-folder"), buffer.getMessage());
        for (String malformed : List.of("", " 60", "60s", "-1", "0")) {
            Map<String, String> window = new HashMap<>(configuration());
            window.put("avro-file.max.seconds", malformed);
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(window), malformed);
            assertTrue(refused.getMessage().contains("avro-file.max.seconds"), refused.getMessage());
        }
        Map<String, String> nested = new HashMap<>(configuration());
        nested.put("filesystem.storage-folder", folder.resolve("a").resolve("b").toString());
        try (RawdataClient client = RawdataClient.create(nested)) {
            client.producer("t").publish(message("a"));
        }
        assertTrue(Files.isDirectory(folder.resolve("a").resolve("b").resolve("t").resolve("segments")));
    }

    /**
     * A sealed window a crash left after its upload, before its file was removed, is recognised by its bytes and
     * removed; the writer goes on after it.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aSealedWindowAlreadyUploadedIsRemoved() throws Exception {
        try (RawdataClient client = client(); RawdataProducer producer = client.producer("t")) {
            producer.publish(message("a"));
        }
        Path buffer = folder.resolve("restarted");
        Path leftover = buffer.resolve("t").resolve("0000000000000000000.sealed.avro");
        Files.createDirectories(leftover.getParent());
        Files.copy(folder.resolve("t").resolve("segments").resolve("0000000000000000000.avro"), leftover);
        try (RawdataClient client = RawdataClient.create(configuration(folder, buffer)); RawdataProducer producer = client.producer("t")) {
            producer.publish(message("b"));
        }
        assertFalse(Files.exists(leftover));
        try (Stream<Path> files = Files.list(leftover.getParent())) {
            assertTrue(files.noneMatch(file -> file.getFileName().toString().endsWith(".invalid")));
        }
        try (RawdataClient client = client(); RawdataConsumer consumer = client.consumer("t")) {
            assertEquals("a", consumer.receive(5, TimeUnit.SECONDS).position());
            assertEquals("b", consumer.receive(5, TimeUnit.SECONDS).position());
            assertNull(consumer.receive(200, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * A sealed window a crash left before its upload is uploaded unchanged, byte for byte.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aSealedWindowNotYetUploadedIsUploadedUnchanged() throws Exception {
        Path written = folder.resolve("written");
        try (RawdataClient client = RawdataClient.create(configuration(written, folder.resolve("buffer-written")));
             RawdataProducer producer = client.producer("t")) {
            producer.publish(message("a"), message("b"));
        }
        byte[] window = Files.readAllBytes(written.resolve("t").resolve("segments").resolve("0000000000000000000.avro"));
        Path empty = folder.resolve("empty");
        Path buffer = folder.resolve("crashed");
        Files.createDirectories(buffer.resolve("t"));
        Files.write(buffer.resolve("t").resolve("0000000000000000000.sealed.avro"), window);
        try (RawdataClient client = RawdataClient.create(configuration(empty, buffer)); RawdataProducer producer = client.producer("t")) {
            producer.publish(message("c"));
        }
        assertArrayEquals(window, Files.readAllBytes(empty.resolve("t").resolve("segments").resolve("0000000000000000000.avro")));
        try (RawdataClient client = RawdataClient.create(configuration(empty, folder.resolve("reader")));
             RawdataConsumer consumer = client.consumer("t")) {
            assertEquals("a", consumer.receive(5, TimeUnit.SECONDS).position());
            assertEquals("b", consumer.receive(5, TimeUnit.SECONDS).position());
            assertEquals("c", consumer.receive(5, TimeUnit.SECONDS).position());
        }
    }

    /**
     * An upload that fails fails the writer loudly: the flush and every later publish throw, closing throws, and the
     * window stays on disk, sealed. The next writer uploads it, and nothing is lost.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    void aFailedUploadKeepsTheWindowForTheNextWriter() throws Exception {
        Path buffer = folder.resolve("failing");
        Path segments = folder.resolve("t").resolve("segments");
        RawdataClient failing = RawdataClient.create(configuration(folder, buffer));
        try {
            RawdataProducer producer = failing.producer("t");
            producer.publish(message("a"));
            ((AvroRawdataClient) failing).flush("t");
            Files.setPosixFilePermissions(segments, PosixFilePermissions.fromString("r-xr-xr-x")); // the store fails
            producer.publish(message("b"));
            assertThrows(RawdataException.class, () -> ((AvroRawdataClient) failing).flush("t"));
            RawdataMessage c = message("c");
            assertThrows(RawdataException.class, () -> producer.publish(c), "the writer stays failed");
            assertThrows(RawdataException.class, failing::close, "closing reports the window that was not uploaded");
        } finally {
            Files.setPosixFilePermissions(segments, PosixFilePermissions.fromString("rwxr-xr-x"));
            failing.close();
        }
        try (Stream<Path> files = Files.list(buffer.resolve("t"))) {
            assertEquals(List.of("0000000000000000001.sealed.avro"),
                    files.map(file -> file.getFileName().toString()).filter(name -> name.endsWith(".avro")).toList());
        }
        try (RawdataClient client = RawdataClient.create(configuration(folder, buffer)); RawdataProducer producer = client.producer("t")) {
            producer.publish(message("d"));
        }
        try (RawdataClient client = client(); RawdataConsumer consumer = client.consumer("t")) {
            assertEquals("a", consumer.receive(5, TimeUnit.SECONDS).position());
            assertEquals("b", consumer.receive(5, TimeUnit.SECONDS).position());
            assertEquals("d", consumer.receive(5, TimeUnit.SECONDS).position());
            assertNull(consumer.receive(200, TimeUnit.MILLISECONDS));
        }
    }
}
