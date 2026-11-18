package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.provider.avro.AvroConfiguration;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.service.provider.api.ProviderName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * The {@code gcs} provider: topics as Avro files in a Google Cloud Storage bucket, through the GCS JSON API on the
 * JDK's HTTP client.
 * <p>
 * Configuration:
 * <ul>
 *     <li>{@code gcs.bucket-name}: the bucket (required).</li>
 *     <li>{@code gcs.prefix}: a folder in the bucket for the topics, for example {@code rawdata/} (default none), at most
 *     {@link AvroConfiguration#MAX_PREFIX_BYTES} bytes.</li>
 *     <li>{@code gcs.credentials}: {@code application-default} (default: Application Default Credentials: the key
 *     file in {@code GOOGLE_APPLICATION_CREDENTIALS}, gcloud's application default credentials, or the metadata
 *     server of a Google Cloud runtime), {@code service-account} (a key file, {@code gcs.service-account.key-file})
 *     or {@code none} (for an emulator).</li>
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
        String prefix = AvroConfiguration.prefix(configuration, PREFIX);
        long pollInterval = AvroConfiguration.positive(configuration, POLL_INTERVAL, 1000);
        String endpoint = AvroConfiguration.optional(configuration, ENDPOINT, GcsClient.DEFAULT_ENDPOINT);
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        GcsClient client = new GcsClient(http, endpoint, credentials(configuration, http));
        return new AvroRawdataClient(new GcsAvroStore(client, bucket, prefix), pollInterval);
    }

    private static GcsCredentials credentials(Map<String, String> configuration, HttpClient http) {
        String kind = AvroConfiguration.optional(configuration, CREDENTIALS, "application-default");
        try {
            switch (kind) {
                case "application-default":
                    return GcsCredentials.applicationDefault(System.getenv(), Path.of(System.getProperty("user.home")), http);
                case "service-account":
                    return GcsCredentials.fromFile(Path.of(AvroConfiguration.required(configuration, KEY_FILE)), http);
                case "none":
                    return GcsCredentials.NONE;
                default:
                    throw new IllegalArgumentException("Configuration '" + CREDENTIALS
                            + "' must be application-default, service-account or none: " + kind);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot load the " + kind + " credentials", e);
        }
    }
}
