package io.descoped.rawdata.provider.filesystem.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.tck.avro.AvroClientContractTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

class FilesystemContractTest extends AvroClientContractTest {

    @TempDir
    static Path folder;

    @Override
    protected Map<String, String> configuration() {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", folder.resolve("topics").toString(),
                "local-temp-folder", folder.resolve("buffer-" + UUID.randomUUID()).toString(), // each client a process of its own
                "avro-file.max.seconds", "1",
                "avro-file.sync.interval", "200",
                "listing.min-interval-seconds", "0");
    }

    /**
     * A file name is at most 255 characters: {@code k} and the key in hex.
     */
    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of(127);
    }

    /**
     * A missing folder is created, so the store that cannot exist is a folder inside a regular file.
     */
    @Override
    protected Map<String, String> missingStoreConfiguration() {
        try {
            Path file = folder.resolve("a-file");
            if (!Files.exists(file)) {
                Files.createFile(file);
            }
            return Map.of(
                    RawdataClient.PROVIDER_KEY, "filesystem",
                    "filesystem.storage-folder", file.resolve("topics").toString(),
                    "local-temp-folder", folder.resolve("buffer").toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
