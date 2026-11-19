package io.descoped.rawdata.provider.s3;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.provider.avro.AvroConfiguration;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.rawdata.provider.avro.AvroUploadWindow;
import io.descoped.service.provider.api.ProviderName;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

/**
 * The {@code s3} provider: topics as Avro files in an Amazon S3 bucket, through the AWS SDK for Java 2.
 * <p>
 * Configuration:
 * <ul>
 *     <li>{@code s3.bucket-name}: the bucket (required).</li>
 *     <li>{@code s3.prefix}: a folder in the bucket for the topics, for example {@code rawdata/} (default none), at most
 *     {@link AvroConfiguration#MAX_PREFIX_BYTES} bytes.</li>
 *     <li>{@code s3.region}: the bucket's region (default: the SDK's region chain: {@code AWS_REGION}, the profile,
 *     the instance metadata).</li>
 *     <li>{@code s3.credentials}: {@code default} (default: the SDK's credentials chain: environment, system
 *     properties, web identity, the profile in {@code AWS_PROFILE}, SSO, container and instance credentials),
 *     {@code profile} (the profile in {@code s3.profile}), {@code static} ({@code s3.access-key-id} and
 *     {@code s3.secret-access-key}) or {@code anonymous}.</li>
 *     <li>{@code s3.endpoint}: another endpoint than AWS's, for an S3-compatible store or an emulator.</li>
 *     <li>{@code s3.path-style-access}: {@code true} for path-style requests (default: true for a bucket name with
 *     dots, which virtual-hosted HTTPS cannot serve, else false).</li>
 *     <li>{@code s3.sse}: server-side encryption to ask for, {@code AES256} or {@code aws:kms} (default: the
 *     bucket's default encryption), and {@code s3.sse-kms-key-id} for {@code aws:kms}.</li>
 *     <li>{@code s3.listing.min-interval-seconds}: how often a waiting consumer looks for new messages, in seconds
 *     (default 1; 0 looks as often as a receive asks).</li>
 * </ul>
 * <p>
 * Producers buffer on a local disk and upload one Avro file per window, configured with the 2.x keys of
 * {@link AvroUploadWindow}: {@code local-temp-folder} (required), {@code avro-file.max.seconds},
 * {@code avro-file.max.bytes}, {@code avro-file.sync.interval} and {@code avro-file.max.pending-uploads}. A message is
 * visible to consumers once its window is uploaded.
 * <p>
 * A metadata key is at most about 500 bytes in UTF-8, so that its object key fits in S3's 1024 bytes with the prefix
 * and topic; a longer key is refused with {@link IllegalArgumentException}.
 */
@ProviderName("s3")
public final class S3RawdataClientInitializer implements RawdataClientInitializer {

    static final String BUCKET = "s3.bucket-name";
    static final String PREFIX = "s3.prefix";
    static final String REGION = "s3.region";
    static final String CREDENTIALS = "s3.credentials";
    static final String PROFILE = "s3.profile";
    static final String ACCESS_KEY_ID = "s3.access-key-id";
    static final String SECRET_ACCESS_KEY = "s3.secret-access-key";
    static final String ENDPOINT = "s3.endpoint";
    static final String PATH_STYLE = "s3.path-style-access";
    static final String SSE = "s3.sse";
    static final String SSE_KMS_KEY_ID = "s3.sse-kms-key-id";
    static final String POLL_INTERVAL = "s3.listing.min-interval-seconds";

    /**
     * Attempts per request, as in {@code gcs}: the SDK retries throttling, 5xx and I/O errors with backoff.
     */
    private static final int MAX_ATTEMPTS = 6;

    /**
     * Creates the initializer; called by the service loader.
     */
    public S3RawdataClientInitializer() {
        // nothing to set up; the configuration comes with initialize
    }

    /**
     * The required configuration keys.
     *
     * @return the bucket and local temp folder keys
     */
    @Override
    public Set<String> configurationKeys() {
        return Set.of(BUCKET, AvroUploadWindow.LOCAL_TEMP_FOLDER);
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String bucket = AvroConfiguration.required(configuration, BUCKET);
        String prefix = AvroConfiguration.prefix(configuration, PREFIX);
        long pollInterval = AvroConfiguration.pollIntervalMillis(configuration, POLL_INTERVAL, 1);
        AvroUploadWindow window = AvroUploadWindow.of(configuration);
        ServerSideEncryption encryption = encryption(configuration);
        String kmsKeyId = AvroConfiguration.optional(configuration, SSE_KMS_KEY_ID, null);
        if (kmsKeyId != null && encryption != ServerSideEncryption.AWS_KMS) {
            throw new IllegalArgumentException("Configuration '" + SSE_KMS_KEY_ID + "' needs '" + SSE + "' aws:kms");
        }
        IdentityProvider<? extends AwsCredentialsIdentity> credentials = credentials(configuration);
        S3Throttling throttling = new S3Throttling();
        S3ClientBuilder builder = S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(10))
                        .socketTimeout(Duration.ofMinutes(5)))
                .credentialsProvider(credentials)
                .overrideConfiguration(override -> override.retryStrategy(retry -> retry.maxAttempts(MAX_ATTEMPTS))
                        .addExecutionInterceptor(throttling))
                .forcePathStyle(pathStyle(configuration, bucket));
        String region = AvroConfiguration.optional(configuration, REGION, null);
        if (region != null) {
            builder.region(Region.of(region));
        }
        String endpoint = AvroConfiguration.optional(configuration, ENDPOINT, null);
        if (endpoint != null) {
            builder.endpointOverride(URI.create(endpoint));
        }
        SdkAutoCloseable closeable = credentials instanceof SdkAutoCloseable sdkCloseable ? sdkCloseable : null;
        S3Client s3;
        try {
            s3 = builder.build();
        } catch (RuntimeException e) {
            if (closeable != null) {
                closeable.close(); // the SDK does not close a credentials provider it was given
            }
            throw e;
        }
        return new AvroRawdataClient(new S3AvroStore(s3, bucket, prefix, encryption, kmsKeyId, closeable, throttling), window, pollInterval);
    }

    private static boolean pathStyle(Map<String, String> configuration, String bucket) {
        String value = AvroConfiguration.optional(configuration, PATH_STYLE, String.valueOf(bucket.contains(".")));
        if (!value.equals("true") && !value.equals("false")) {
            throw new IllegalArgumentException("Configuration '" + PATH_STYLE + "' must be true or false: " + value);
        }
        return Boolean.parseBoolean(value);
    }

    private static ServerSideEncryption encryption(Map<String, String> configuration) {
        String value = AvroConfiguration.optional(configuration, SSE, null);
        if (value == null) {
            return null;
        }
        switch (value) {
            case "AES256":
                return ServerSideEncryption.AES256;
            case "aws:kms":
                return ServerSideEncryption.AWS_KMS;
            default:
                throw new IllegalArgumentException("Configuration '" + SSE + "' must be AES256 or aws:kms: " + value);
        }
    }

    private static IdentityProvider<? extends AwsCredentialsIdentity> credentials(Map<String, String> configuration) {
        String kind = AvroConfiguration.optional(configuration, CREDENTIALS, "default");
        switch (kind) {
            case "default":
                return DefaultCredentialsProvider.builder().build();
            case "profile":
                return ProfileCredentialsProvider.builder().profileName(AvroConfiguration.required(configuration, PROFILE)).build();
            case "static":
                return StaticCredentialsProvider.create(AwsBasicCredentials.create(
                        AvroConfiguration.required(configuration, ACCESS_KEY_ID),
                        AvroConfiguration.required(configuration, SECRET_ACCESS_KEY)));
            case "anonymous":
                return AnonymousCredentialsProvider.create();
            default:
                throw new IllegalArgumentException("Configuration '" + CREDENTIALS
                        + "' must be default, profile, static or anonymous: " + kind);
        }
    }
}
