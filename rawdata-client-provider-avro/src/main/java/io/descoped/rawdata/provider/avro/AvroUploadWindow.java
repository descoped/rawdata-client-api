package io.descoped.rawdata.provider.avro;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * How a producer buffers before it uploads, configured with the 2.x keys. A producer appends to an Avro file in the
 * local folder, in blocks of {@code syncInterval} bytes, and closes the window when it is {@code maxSeconds} old or
 * larger than {@code maxBytes}. A closed window is uploaded in the background as the topic's next segment, while
 * the producer goes on in a new window, and its messages are visible once the upload is done.
 * <p>
 * There are no values that suit every deployment: tune them to the message rate, the message size and how soon
 * consumers need the messages.
 *
 * @param localTempFolder   where windows are buffered: a local disk, one folder per topic ({@value #LOCAL_TEMP_FOLDER},
 *                          required)
 * @param maxSeconds        the longest a window stays open, in seconds ({@value #MAX_SECONDS}, default 60)
 * @param maxBytes          the size at which a window closes ({@value #MAX_BYTES}, default 64 MiB)
 * @param syncInterval      the size of an Avro block in bytes ({@value #SYNC_INTERVAL}, default 512 KiB); a block is
 *                          forced to disk when it is written, so a crash loses at most the block being written
 * @param maxPendingUploads how many closed windows may wait for upload before a publish waits
 *                          ({@value #MAX_PENDING_UPLOADS}, default 4)
 * @param closeTimeoutSeconds the longest closing a producer or the client waits for the uploads, in seconds
 *                          ({@value #CLOSE_TIMEOUT_SECONDS}, default 30); windows not uploaded by then stay on disk
 * @param maxUploadConcurrency the most uploads of a client at once, across its topics ({@value #MAX_UPLOAD_CONCURRENCY},
 *                          default 16); the client starts at 4 and adapts to throttling ({@link AvroUploadLimiter})
 */
public record AvroUploadWindow(Path localTempFolder, long maxSeconds, long maxBytes, int syncInterval, int maxPendingUploads,
                               long closeTimeoutSeconds, int maxUploadConcurrency) {

    /** The local folder that buffers windows. */
    public static final String LOCAL_TEMP_FOLDER = "local-temp-folder";
    /** The longest a window stays open, in seconds. */
    public static final String MAX_SECONDS = "avro-file.max.seconds";
    /** The size in bytes at which a window closes. */
    public static final String MAX_BYTES = "avro-file.max.bytes";
    /** The size of an Avro block, in bytes. */
    public static final String SYNC_INTERVAL = "avro-file.sync.interval";
    /** How many closed windows may wait for upload. */
    public static final String MAX_PENDING_UPLOADS = "avro-file.max.pending-uploads";
    /** The longest closing waits for the uploads, in seconds. */
    public static final String CLOSE_TIMEOUT_SECONDS = "avro-file.close-timeout-seconds";
    /** The most uploads of a client at once. */
    public static final String MAX_UPLOAD_CONCURRENCY = "avro-file.upload.max-concurrency";

    private static final long DEFAULT_MAX_SECONDS = 60;
    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024;
    private static final long DEFAULT_SYNC_INTERVAL = 512L * 1024;
    private static final long DEFAULT_MAX_PENDING_UPLOADS = 4;
    private static final long DEFAULT_CLOSE_TIMEOUT_SECONDS = 30;
    private static final long DEFAULT_MAX_UPLOAD_CONCURRENCY = 16;

    /**
     * Checks the values.
     *
     * @param localTempFolder   where windows are buffered
     * @param maxSeconds        the longest a window stays open, in seconds
     * @param maxBytes          the size at which a window closes
     * @param syncInterval      the size of an Avro block in bytes
     * @param maxPendingUploads how many closed windows may wait for upload
     * @param closeTimeoutSeconds the longest closing waits for the uploads, in seconds
     * @param maxUploadConcurrency the most uploads of a client at once
     * @throws IllegalArgumentException if one is out of range
     */
    public AvroUploadWindow {
        Objects.requireNonNull(localTempFolder, "localTempFolder");
        positive(MAX_SECONDS, maxSeconds);
        positive(MAX_BYTES, maxBytes);
        positive(SYNC_INTERVAL, syncInterval);
        positive(MAX_PENDING_UPLOADS, maxPendingUploads);
        positive(CLOSE_TIMEOUT_SECONDS, closeTimeoutSeconds);
        positive(MAX_UPLOAD_CONCURRENCY, maxUploadConcurrency);
    }

    /**
     * Reads the window from a provider's configuration.
     *
     * @param configuration the configuration
     * @return the window
     * @throws IllegalArgumentException if {@value #LOCAL_TEMP_FOLDER} is missing or a value is malformed
     */
    public static AvroUploadWindow of(Map<String, String> configuration) {
        Path folder = Path.of(AvroConfiguration.required(configuration, LOCAL_TEMP_FOLDER)).toAbsolutePath().normalize();
        return new AvroUploadWindow(folder,
                AvroConfiguration.positive(configuration, MAX_SECONDS, DEFAULT_MAX_SECONDS),
                AvroConfiguration.positive(configuration, MAX_BYTES, DEFAULT_MAX_BYTES),
                Math.toIntExact(AvroConfiguration.positive(configuration, SYNC_INTERVAL, DEFAULT_SYNC_INTERVAL)),
                Math.toIntExact(AvroConfiguration.positive(configuration, MAX_PENDING_UPLOADS, DEFAULT_MAX_PENDING_UPLOADS)),
                AvroConfiguration.positive(configuration, CLOSE_TIMEOUT_SECONDS, DEFAULT_CLOSE_TIMEOUT_SECONDS),
                Math.toIntExact(AvroConfiguration.positive(configuration, MAX_UPLOAD_CONCURRENCY, DEFAULT_MAX_UPLOAD_CONCURRENCY)));
    }

    private static void positive(String key, long value) {
        if (value < 1) {
            throw new IllegalArgumentException("Configuration '" + key + "' must be a positive number: " + value);
        }
    }
}
