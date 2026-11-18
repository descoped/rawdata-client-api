package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.api.RawdataClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * S3Mock (adobe/s3mock) in Docker, shared by the tests of a run, with one bucket.
 */
public final class S3Mock {

    /**
     * The bucket the tests use.
     */
    public static final String BUCKET = "rawdata";

    private static final int PORT = 9090;

    @SuppressWarnings("resource") // stopped by Testcontainers when the JVM exits
    private static final GenericContainer<?> SERVER = new GenericContainer<>("adobe/s3mock:5.2.3")
            .withExposedPorts(PORT)
            .waitingFor(Wait.forHttp("/").forPort(PORT));

    static {
        SERVER.start();
        try (S3Client s3 = client()) {
            s3.createBucket(create -> create.bucket(BUCKET));
        }
    }

    private S3Mock() {
    }

    /**
     * The mock's endpoint.
     *
     * @return {@code http://host:port}
     */
    public static String endpoint() {
        return "http://" + SERVER.getHost() + ":" + SERVER.getMappedPort(PORT);
    }

    /**
     * A client of the mock, for checking what the provider wrote.
     *
     * @return a new client; the caller closes it
     */
    public static S3Client client() {
        return S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .endpointOverride(URI.create(endpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .forcePathStyle(true)
                .build();
    }

    /**
     * The objects under a prefix and their content types.
     *
     * @param prefix the prefix
     * @return object key to content type, sorted by key
     */
    public static Map<String, String> objects(String prefix) {
        try (S3Client s3 = client()) {
            Map<String, String> objects = new TreeMap<>();
            for (S3Object object : s3.listObjectsV2Paginator(list -> list.bucket(BUCKET).prefix(prefix)).contents()) {
                HeadObjectResponse head = s3.headObject(h -> h.bucket(BUCKET).key(object.key()));
                objects.put(object.key(), head.contentType());
            }
            return objects;
        }
    }

    static Map<String, String> configuration(String prefix) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RawdataClient.PROVIDER_KEY, "s3");
        configuration.put("s3.bucket-name", BUCKET);
        configuration.put("s3.prefix", prefix);
        configuration.put("s3.region", "us-east-1");
        configuration.put("s3.endpoint", endpoint());
        configuration.put("s3.path-style-access", "true");
        configuration.put("s3.credentials", "static");
        configuration.put("s3.access-key-id", "test");
        configuration.put("s3.secret-access-key", "test");
        configuration.put("s3.listing.min-interval-seconds", "0");
        configuration.put("local-temp-folder", bufferFolder());
        configuration.put("avro-file.max.seconds", "1");
        return configuration;
    }

    private static String bufferFolder() {
        try {
            return java.nio.file.Files.createTempDirectory("rawdata-buffer").toString(); // a folder per client, as per process
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
