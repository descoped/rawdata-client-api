package io.descoped.rawdata.provider.filesystem.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
     * The storage folder is required, created when missing, and the poll interval must be positive.
     */
    @Test
    void configurationIsChecked() {
        Map<String, String> noFolder = Map.of(RawdataClient.PROVIDER_KEY, "filesystem");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noFolder));
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
