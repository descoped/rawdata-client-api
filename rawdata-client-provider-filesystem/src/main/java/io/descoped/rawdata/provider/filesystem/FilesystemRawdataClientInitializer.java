package io.descoped.rawdata.provider.filesystem;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.provider.avro.AvroConfiguration;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.rawdata.provider.avro.AvroUploadWindow;
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
 * {@code listing.min-interval-seconds}, how often a waiting consumer looks for new messages, in seconds (default 1;
 * 0 looks as often as a receive asks).
 * <p>
 * Producers buffer on a local disk and upload one Avro file per window, configured with the 2.x keys of
 * {@link AvroUploadWindow}: {@code local-temp-folder} (required), {@code avro-file.max.seconds},
 * {@code avro-file.max.bytes}, {@code avro-file.sync.interval} and {@code avro-file.max.pending-uploads}. A message is
 * visible to consumers once its window is uploaded.
 * <p>
 * A metadata key is at most 127 bytes in UTF-8, so that its file name fits in 255 characters; a longer key is refused
 * with {@link IllegalArgumentException}.
 */
@ProviderName("filesystem")
public final class FilesystemRawdataClientInitializer implements RawdataClientInitializer {

    static final String STORAGE_FOLDER = "filesystem.storage-folder";
    static final String POLL_INTERVAL = "listing.min-interval-seconds";

    /**
     * Creates the initializer; called by the service loader.
     */
    public FilesystemRawdataClientInitializer() {
        // nothing to set up; the configuration comes with initialize
    }

    /**
     * The required configuration keys.
     *
     * @return the storage folder and local temp folder keys
     */
    @Override
    public Set<String> configurationKeys() {
        return Set.of(STORAGE_FOLDER, AvroUploadWindow.LOCAL_TEMP_FOLDER);
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        Path folder = Path.of(AvroConfiguration.required(configuration, STORAGE_FOLDER)).toAbsolutePath().normalize();
        long pollInterval = AvroConfiguration.pollIntervalMillis(configuration, POLL_INTERVAL, 1);
        AvroUploadWindow window = AvroUploadWindow.of(configuration);
        try {
            Files.createDirectories(folder);
        } catch (IOException e) {
            throw new RawdataException("Cannot create the storage folder " + folder, e);
        }
        return new AvroRawdataClient(new FilesystemAvroStore(folder), window, pollInterval);
    }
}
