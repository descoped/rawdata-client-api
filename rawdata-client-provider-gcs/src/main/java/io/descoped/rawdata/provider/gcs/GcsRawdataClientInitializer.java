package io.descoped.rawdata.provider.gcs;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.provider.avro.AvroConfiguration;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.service.provider.api.ProviderName;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * The {@code gcs} provider: topics as Avro files in a Google Cloud Storage bucket.
 * <p>
 * Configuration:
 * <ul>
 *     <li>{@code gcs.bucket-name}: the bucket (required).</li>
 *     <li>{@code gcs.prefix}: a folder in the bucket for the topics, for example {@code rawdata/} (default none).</li>
 *     <li>{@code gcs.credentials}: {@code application-default} (default: Application Default Credentials),
 *     {@code service-account} (a key file, {@code gcs.service-account.key-file}) or {@code none} (for an
 *     emulator).</li>
 *     <li>{@code gcs.project-id}: the project (default: from the environment).</li>
 *     <li>{@code gcs.endpoint}: another endpoint than Google's, for example an emulator's
 *     {@code http://localhost:4443}.</li>
 *     <li>{@code gcs.poll-interval-ms}: how often a waiting consumer looks for new messages (default 1000).</li>
 * </ul>
 * A metadata key is at most about 500 bytes in UTF-8, so that its object name fits in GCS's 1024 bytes with the prefix
 * and topic; a longer key is refused with {@link IllegalArgumentException}.
 */
@ProviderName("gcs")
public final class GcsRawdataClientInitializer implements RawdataClientInitializer {

    static final String BUCKET = "gcs.bucket-name";
    static final String PREFIX = "gcs.prefix";
    static final String CREDENTIALS = "gcs.credentials";
    static final String KEY_FILE = "gcs.service-account.key-file";
    static final String PROJECT = "gcs.project-id";
    static final String ENDPOINT = "gcs.endpoint";
    static final String POLL_INTERVAL = "gcs.poll-interval-ms";

    /**
     * Creates the initializer; called by the service loader.
     */
    public GcsRawdataClientInitializer() {
        // nothing to set up; the configuration comes with initialize
    }

    /**
     * The required configuration keys.
     *
     * @return the bucket key
     */
    @Override
    public Set<String> configurationKeys() {
        return Set.of(BUCKET);
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String bucket = AvroConfiguration.required(configuration, BUCKET);
        String prefix = prefix(AvroConfiguration.optional(configuration, PREFIX, ""));
        long pollInterval = AvroConfiguration.positive(configuration, POLL_INTERVAL, 1000);
        StorageOptions.Builder options = StorageOptions.newBuilder().setCredentials(credentials(configuration));
        String project = AvroConfiguration.optional(configuration, PROJECT, null);
        if (project != null) {
            options.setProjectId(project);
        }
        String endpoint = AvroConfiguration.optional(configuration, ENDPOINT, null);
        if (endpoint != null) {
            options.setHost(endpoint);
        }
        return new AvroRawdataClient(new GcsAvroStore(options.build().getService(), bucket, prefix), pollInterval);
    }

    private static String prefix(String prefix) {
        if (prefix.isEmpty()) {
            return prefix;
        }
        if (prefix.startsWith("/")) {
            throw new IllegalArgumentException("Configuration '" + PREFIX + "' must not start with '/': " + prefix);
        }
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }

    private static Credentials credentials(Map<String, String> configuration) {
        String kind = AvroConfiguration.optional(configuration, CREDENTIALS, "application-default");
        try {
            switch (kind) {
                case "application-default":
                    return GoogleCredentials.getApplicationDefault();
                case "service-account":
                    try (InputStream key = Files.newInputStream(Path.of(AvroConfiguration.required(configuration, KEY_FILE)))) {
                        return ServiceAccountCredentials.fromStream(key);
                    }
                case "none":
                    return NoCredentials.getInstance();
                default:
                    throw new IllegalArgumentException("Configuration '" + CREDENTIALS
                            + "' must be application-default, service-account or none: " + kind);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load the " + kind + " credentials", e);
        }
    }
}
