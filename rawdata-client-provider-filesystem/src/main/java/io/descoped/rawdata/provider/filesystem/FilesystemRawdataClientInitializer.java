package io.descoped.rawdata.provider.filesystem;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.provider.avro.AvroConfiguration;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.service.provider.api.ProviderName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * The {@code filesystem} provider: topics as Avro files in a local folder.
 * <p>
 * Configuration: {@code filesystem.storage-folder}, the folder (required; created if missing), and
 * {@code filesystem.poll-interval-ms}, how often a waiting consumer looks for new messages (default 250).
 * <p>
 * A metadata key is at most 127 bytes in UTF-8, so that its file name fits in 255 characters; a longer key is refused
 * with {@link IllegalArgumentException}.
 */
@ProviderName("filesystem")
public final class FilesystemRawdataClientInitializer implements RawdataClientInitializer {

    static final String STORAGE_FOLDER = "filesystem.storage-folder";
    static final String POLL_INTERVAL = "filesystem.poll-interval-ms";

    /**
     * Creates the initializer; called by the service loader.
     */
    public FilesystemRawdataClientInitializer() {
        // nothing to set up; the configuration comes with initialize
    }

    /**
     * The required configuration keys.
     *
     * @return the storage folder key
     */
    @Override
    public Set<String> configurationKeys() {
        return Set.of(STORAGE_FOLDER);
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        Path folder = Path.of(AvroConfiguration.required(configuration, STORAGE_FOLDER)).toAbsolutePath().normalize();
        long pollInterval = AvroConfiguration.positive(configuration, POLL_INTERVAL, 250);
        try {
            Files.createDirectories(folder);
        } catch (IOException e) {
            throw new RawdataException("Cannot create the storage folder " + folder, e);
        }
        return new AvroRawdataClient(new FilesystemAvroStore(folder), pollInterval);
    }
}
