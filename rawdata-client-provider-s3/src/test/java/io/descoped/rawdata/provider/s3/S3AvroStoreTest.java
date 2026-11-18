package io.descoped.rawdata.provider.s3;

import io.descoped.rawdata.provider.avro.AvroChecksums;
import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.s3.test.S3Mock;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ChecksumMode;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;
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
        try (S3AvroStore store = new S3AvroStore(conflicting, "b", "", null, null, () -> credentialsClosed.set(true), null)) {
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
     * An opened segment reads the object it was opened on ({@code If-Match} on its ETag): once the object is replaced
     * (only by hand, the engine never replaces a segment), its reads fail instead of mixing two objects.
     *
     * @throws IOException if S3 fails
     */
    @Test
    void anOpenedSegmentIsPinnedToItsETag() throws IOException {
        S3Client s3 = S3Mock.client();
        String prefix = "pinned-" + UUID.randomUUID() + "/";
        try (S3AvroStore store = new S3AvroStore(s3, S3Mock.BUCKET, prefix, null, null)) {
            assertTrue(store.create("t", 0, new byte[]{1, 2, 3}));
            AvroStore.Segment opened = store.open("t", 0).orElseThrow();
            assertArrayEquals(new byte[]{2, 3}, opened.read(1, 2));
            s3.putObject(put -> put.bucket(S3Mock.BUCKET).key(prefix + "t/segments/0000000000000000000.avro"),
                    RequestBody.fromBytes(new byte[]{9, 9, 9}));
            assertThrows(IOException.class, () -> opened.read(0, 3));
        }
    }

    /**
     * A put that finds its key taken looks at the object's size and CRC32C before it downloads anything: another
     * writer's segment costs one HEAD; a match is confirmed byte for byte.
     *
     * @throws IOException if the store fails
     */
    @Test
    void aTakenKeyIsComparedByChecksumBeforeItIsDownloaded() throws IOException {
        byte[] mine = {1, 2, 3, 4};
        AtomicInteger gets = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<byte[]> stored = new java.util.concurrent.atomic.AtomicReference<>();
        S3Client taken = new S3Client() {
            @Override
            public PutObjectResponse putObject(PutObjectRequest request, RequestBody body) {
                assertEquals(AvroChecksums.crc32c(mine), request.checksumCRC32C(), "the put carries its CRC32C");
                throw (S3Exception) S3Exception.builder().statusCode(412).build();
            }

            @Override
            public HeadObjectResponse headObject(
                    HeadObjectRequest request) {
                assertEquals(ChecksumMode.ENABLED, request.checksumMode());
                return HeadObjectResponse.builder()
                        .contentLength((long) stored.get().length).checksumCRC32C(AvroChecksums.crc32c(stored.get())).build();
            }

            @Override
            public ResponseBytes<GetObjectResponse> getObjectAsBytes(
                    GetObjectRequest request) {
                gets.incrementAndGet();
                return ResponseBytes.fromByteArray(
                        GetObjectResponse.builder().build(), stored.get());
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
        try (S3AvroStore store = new S3AvroStore(taken, "b", "", null, null)) {
            stored.set(new byte[]{9, 9, 9, 9}); // another writer's, same size
            assertFalse(store.create("t", 0, mine));
            assertEquals(0, gets.get(), "told apart by its CRC32C, not downloaded");
            stored.set(mine.clone()); // our own put, its response lost
            assertTrue(store.create("t", 0, mine));
            assertEquals(1, gets.get(), "a match is confirmed byte for byte");
        }
    }

    /**
     * The interceptor reports S3's throttled responses (503 SlowDown) to segment puts, on each attempt the SDK retries,
     * and not those to other requests.
     */
    @Test
    void throttledSegmentPutsAreReported() {
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger throttled = new AtomicInteger();
        SdkHttpClient http = new SdkHttpClient() {
            @Override
            public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
                return new ExecutableHttpRequest() {
                    @Override
                    public HttpExecuteResponse call() {
                        int status = calls.incrementAndGet() % 3 == 0 ? 200 : 503;
                        return HttpExecuteResponse.builder()
                                .response(SdkHttpResponse.builder().statusCode(status)
                                        .putHeader("Content-Length", "0").build())
                                .build();
                    }

                    @Override
                    public void abort() {
                        // nothing to abort
                    }
                };
            }

            @Override
            public void close() {
                // nothing to close
            }
        };
        S3Throttling throttling = new S3Throttling();
        throttling.onThrottled(throttled::incrementAndGet);
        try (S3Client s3 = S3Client.builder().httpClient(http).region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .endpointOverride(URI.create("http://127.0.0.1:1")).forcePathStyle(true)
                .overrideConfiguration(override -> override.addExecutionInterceptor(throttling)
                        .retryStrategy(retry -> retry.maxAttempts(5).backoffStrategy(BackoffStrategy.retryImmediately())
                                .throttlingBackoffStrategy(BackoffStrategy.retryImmediately())))
                .build()) {
            s3.headObject(head -> head.bucket("b").key("k"));
            assertEquals(3, calls.get());
            assertEquals(0, throttled.get(), "a read is not reported");
            s3.putObject(put -> put.bucket("b").key("m"), RequestBody.fromBytes(new byte[]{1}));
            assertEquals(0, throttled.get(), "nor a metadata put");
            s3.putObject(put -> put.bucket("b").key("s").ifNoneMatch("*"), RequestBody.fromBytes(new byte[]{1}));
        }
        assertEquals(9, calls.get());
        assertEquals(2, throttled.get(), "a segment put's two throttled attempts");
    }

    /**
     * A segment put that timed out counts as throttling; another request that timed out does not.
     */
    @Test
    void aTimeoutIsReportedAsThrottling() {
        AtomicInteger throttled = new AtomicInteger();
        S3Throttling throttling = new S3Throttling();
        throttling.onThrottled(throttled::incrementAndGet);
        PutObjectRequest segment = PutObjectRequest.builder().bucket("b").key("s").ifNoneMatch("*").build();
        throttling.onExecutionFailure(failed(ApiCallAttemptTimeoutException.create(1_000), segment), new ExecutionAttributes());
        assertEquals(1, throttled.get());
        throttling.onExecutionFailure(failed(new IllegalStateException("not a timeout"), segment), new ExecutionAttributes());
        assertEquals(1, throttled.get(), "other failures are not throttling");
        throttling.onExecutionFailure(failed(ApiCallAttemptTimeoutException.create(1_000),
                HeadObjectRequest.builder().bucket("b").key("k").build()), new ExecutionAttributes());
        assertEquals(1, throttled.get(), "a read that timed out is not reported");
    }

    private static Context.FailedExecution failed(Throwable failure, SdkRequest request) {
        return new Context.FailedExecution() {
            @Override
            public Throwable exception() {
                return failure;
            }

            @Override
            public SdkRequest request() {
                return request;
            }

            @Override
            public Optional<SdkHttpRequest> httpRequest() {
                return Optional.empty();
            }

            @Override
            public Optional<SdkHttpResponse> httpResponse() {
                return Optional.empty();
            }

            @Override
            public Optional<SdkResponse> response() {
                return Optional.empty();
            }
        };
    }
}
