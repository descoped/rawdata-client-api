package io.descoped.rawdata.provider.s3;

import io.descoped.rawdata.provider.s3.test.S3Mock;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.IOException;
import java.net.URI;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store's handling of the conditional put, against S3Mock.
 */
class S3AvroStoreTest {

    private static S3AvroStore store() {
        return new S3AvroStore(S3Mock.client(), S3Mock.BUCKET, "store-" + UUID.randomUUID() + "/", null, null);
    }

    /**
     * A create that finds its own bytes, as a retry after a lost response does, counts as created; another writer's
     * bytes do not, and the segment keeps them.
     */
    @Test
    void aRetriedPutIsRecognisedAndAnotherWritersIsNot() throws Exception {
        try (S3AvroStore store = store()) {
            byte[] mine = {1, 2, 3};
            assertTrue(store.create("t", 0, mine));
            assertTrue(store.create("t", 0, mine.clone()), "the same bytes are this writer's put");
            assertFalse(store.create("t", 0, new byte[]{9}), "other bytes are another writer's");
            assertArrayEquals(mine, store.read("t", 0).orElseThrow());
            assertTrue(store.exists("t", 0));
            assertFalse(store.exists("t", 1));
            assertTrue(store.read("t", 1).isEmpty());
        }
    }

    /**
     * Metadata is replaced by the last write, listed, and removed; removing twice is fine.
     */
    @Test
    void metadataIsReplacedAndRemoved() throws Exception {
        try (S3AvroStore store = store()) {
            store.writeMetadata("t", "k01", new byte[]{1});
            store.writeMetadata("t", "k01", new byte[]{2});
            store.writeMetadata("t", "k02", new byte[]{3});
            assertArrayEquals(new byte[]{2}, store.readMetadata("t", "k01").orElseThrow());
            assertEquals(Set.of("k01", "k02"), store.metadataNames("t"));
            store.deleteMetadata("t", "k01");
            store.deleteMetadata("t", "k01");
            assertTrue(store.readMetadata("t", "k01").isEmpty());
            assertEquals(Set.of("k02"), store.metadataNames("t"));
        }
    }

    /**
     * A wrong bucket is an error, not an empty topic; an unreachable endpoint is an IOException after the SDK's
     * retries.
     */
    @Test
    void failuresAreIOExceptions() {
        try (S3AvroStore store = new S3AvroStore(S3Mock.client(), "no-such-bucket", "", null, null)) {
            IOException read = assertThrows(IOException.class, () -> store.read("t", 0));
            assertEquals("NoSuchBucket", errorCode(read));
            IOException exists = assertThrows(IOException.class, () -> store.exists("t", 0), "a wrong bucket is not an empty topic");
            assertEquals("NoSuchBucket", errorCode(exists));
            assertThrows(IOException.class, () -> store.create("t", 0, new byte[]{1}));
            assertThrows(IOException.class, () -> store.writeMetadata("t", "k01", new byte[]{1}));
            assertThrows(IOException.class, () -> store.metadataNames("t"));
            assertThrows(IOException.class, () -> store.readMetadata("t", "k01"));
        }
        S3Client unreachable = S3Client.builder()
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .endpointOverride(URI.create("http://127.0.0.1:1"))
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .forcePathStyle(true)
                .build();
        try (S3AvroStore store = new S3AvroStore(unreachable, S3Mock.BUCKET, "", null, null)) {
            assertThrows(IOException.class, () -> store.exists("t", 0));
            assertThrows(IOException.class, () -> store.read("t", 0));
            assertThrows(IOException.class, () -> store.create("t", 0, new byte[]{1}));
            assertThrows(IOException.class, () -> store.deleteMetadata("t", "k01"));
        }
    }

    /**
     * Server-side encryption with a KMS key id goes on the put: S3Mock checks the key id against the keys it knows,
     * and refuses this one by name.
     */
    @Test
    void aKmsKeyIsSentWithThePut() throws Exception {
        String key = "arn:aws:kms:us-east-1:000000000000:key/unknown";
        try (S3AvroStore store = new S3AvroStore(S3Mock.client(), S3Mock.BUCKET, "kms-" + UUID.randomUUID() + "/",
                ServerSideEncryption.AWS_KMS, key)) {
            IOException refused = assertThrows(IOException.class, () -> store.create("t", 0, new byte[]{1}));
            assertTrue(refused.getMessage().contains(key), refused.getMessage());
            assertTrue(store.toString().contains("kms-"), store.toString());
        }
    }

    /**
     * A 409 (another conditional put on the key in flight) is asked again, after a short wait, until S3 gives the
     * outcome; S3Mock never answers 409, so a stub plays S3 here. The credentials provider is closed with the store.
     */
    @Test
    void aConflictIsAskedAgain() throws Exception {
        AtomicInteger puts = new AtomicInteger();
        AtomicBoolean credentialsClosed = new AtomicBoolean();
        S3Client conflicting = new S3Client() {
            @Override
            public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
                if (puts.incrementAndGet() <= 2) {
                    throw (S3Exception) S3Exception.builder().statusCode(409)
                            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ConditionalRequestConflict").build()).build();
                }
                return PutObjectResponse.builder().build();
            }

            @Override
            public String serviceName() {
                return "s3";
            }

            @Override
            public void close() {
                // nothing to close
            }
        };
        try (S3AvroStore store = new S3AvroStore(conflicting, "b", "", null, null, () -> credentialsClosed.set(true))) {
            assertTrue(store.create("t", 0, new byte[]{1}));
            assertEquals(3, puts.get());
        }
        assertTrue(credentialsClosed.get());

        S3Client alwaysConflicting = new S3Client() {
            @Override
            public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
                puts.incrementAndGet();
                throw (S3Exception) S3Exception.builder().statusCode(409).build();
            }

            @Override
            public String serviceName() {
                return "s3";
            }

            @Override
            public void close() {
                // nothing to close
            }
        };
        puts.set(0);
        try (S3AvroStore store = new S3AvroStore(alwaysConflicting, "b", "", null, null)) {
            assertThrows(IOException.class, () -> store.create("t", 0, new byte[]{1}));
            assertEquals(11, puts.get(), "the first put and ten more");
        }
    }

    private static String errorCode(IOException e) {
        return ((AwsServiceException) e.getCause()).awsErrorDetails().errorCode();
    }

    /**
     * Object keys are at most 1024 bytes; a longer metadata name is refused before the request.
     */
    @Test
    void aTooLongMetadataNameIsRefused() {
        try (S3AvroStore store = store()) {
            String tooLong = "k" + "a".repeat(1100);
            byte[] value = new byte[1];
            assertThrows(IllegalArgumentException.class, () -> store.writeMetadata("t", tooLong, value));
        }
    }
}
