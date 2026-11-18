package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.api.RawdataClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Real Amazon S3 for the live tests, which run only with {@code -Drawdata.s3.live=true}. Credentials come from the
 * SDK's default chain, for example {@code AWS_PROFILE=rawdata-test}. The bucket and region default to
 * {@code descoped-rawdata-test} in {@code eu-north-1} ({@code -Drawdata.s3.live.bucket},
 * {@code -Drawdata.s3.live.region}). A run writes under {@code rawdata-client-test/<random>/} and removes it.
 */
final class LiveS3 {

    static final String ENABLED = "rawdata.s3.live";
    static final String BUCKET = System.getProperty("rawdata.s3.live.bucket", "descoped-rawdata-test");
    static final String REGION = System.getProperty("rawdata.s3.live.region", "eu-north-1");
    static final String RUN = "rawdata-client-test/" + UUID.randomUUID() + "/";

    private LiveS3() {
    }

    static Map<String, String> configuration(String prefix) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RawdataClient.PROVIDER_KEY, "s3");
        configuration.put("s3.bucket-name", BUCKET);
        configuration.put("s3.region", REGION);
        configuration.put("s3.prefix", RUN + prefix);
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

    /**
     * Removes everything under a prefix of this run.
     */
    static void remove(String prefix) {
        try (S3Client s3 = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder()).region(Region.of(REGION)).build()) {
            for (S3Object object : s3.listObjectsV2Paginator(list -> list.bucket(BUCKET).prefix(RUN + prefix)).contents()) {
                s3.deleteObject(delete -> delete.bucket(BUCKET).key(object.key()));
            }
        }
    }
}
