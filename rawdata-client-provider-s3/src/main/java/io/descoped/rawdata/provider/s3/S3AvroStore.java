package io.descoped.rawdata.provider.s3;

import io.descoped.rawdata.provider.avro.AvroStore;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Topics in a bucket: {@code <prefix><topic>/segments/<number>.avro}, the number in 19 digits, and
 * {@code <prefix><topic>/metadata/<name>}, as in {@code gcs}. A segment is put in one request with
 * {@code If-None-Match: *}, so S3 creates it only if no object has its key: two writers never both create it, and
 * readers see a whole object or none.
 * <p>
 * The SDK retries a failed put. If a put succeeded but its response was lost, the retry fails its condition (412); the
 * store then reads the object, and if it holds the bytes it put (an Avro file carries a random sync marker, so no
 * other writer's file is the same) the segment counts as created. When S3 answers 409 (a conditional put raced
 * another on the same key), the store waits a little and tries again, to get the outcome of the race.
 * <p>
 * A HEAD response has no body, so a missing object and a missing bucket are both a 404 to {@link #exists}. The first
 * time it sees one, the store lists the prefix once, which names a missing bucket ({@code NoSuchBucket}), so a wrong
 * bucket fails instead of looking like an empty topic.
 * <p>
 * A metadata entry is a plain put, which S3 applies atomically: the last write wins, and a retry is harmless.
 */
final class S3AvroStore implements AvroStore {

    private static final int MAX_KEY_BYTES = 1024;
    private static final int NOT_FOUND = 404;
    private static final int CONFLICT = 409;
    private static final int PRECONDITION_FAILED = 412;
    private static final String PUTTING = "Putting ";
    private static final int MAX_CONFLICTS = 10;
    private static final long FIRST_CONFLICT_WAIT_MILLIS = 50;
    private static final long MAX_CONFLICT_WAIT_MILLIS = 1_000;

    private final S3Client s3;
    private final String bucket;
    private final String prefix;
    private final ServerSideEncryption encryption;
    private final String kmsKeyId;
    private final SdkAutoCloseable credentials;
    private volatile boolean bucketSeen;

    S3AvroStore(S3Client s3, String bucket, String prefix, ServerSideEncryption encryption, String kmsKeyId) {
        this(s3, bucket, prefix, encryption, kmsKeyId, null);
    }

    /**
     * Creates a store.
     *
     * @param encryption  the server-side encryption to ask for, or null for the bucket's default
     * @param kmsKeyId    the KMS key for {@code aws:kms}, or null for the account's default key
     * @param credentials the credentials provider to close with the client (the SDK does not close one it was
     *                    given), or null
     */
    S3AvroStore(S3Client s3, String bucket, String prefix, ServerSideEncryption encryption, String kmsKeyId,
                SdkAutoCloseable credentials) {
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = prefix;
        this.encryption = encryption;
        this.kmsKeyId = kmsKeyId;
        this.credentials = credentials;
    }

    @Override
    public boolean create(String topic, long segment, byte[] content) throws IOException {
        String key = segment(topic, segment);
        for (int conflicts = 0; ; conflicts++) {
            try {
                s3.putObject(put(key, "avro/binary").ifNoneMatch("*").build(), RequestBody.fromBytes(content));
                return true;
            } catch (AwsServiceException e) {
                if (e.statusCode() == PRECONDITION_FAILED) {
                    return read(topic, segment).map(stored -> Arrays.equals(content, stored)).orElse(false); // our own put, if a retry found it
                }
                if (e.statusCode() != CONFLICT || conflicts == MAX_CONFLICTS) {
                    throw failed(PUTTING + key, e);
                }
            } catch (SdkException e) {
                throw failed(PUTTING + key, e);
            }
            waitAfterConflict(conflicts); // another conditional put on the key was in flight; ask again for the outcome
        }
    }

    private static void waitAfterConflict(int conflicts) throws InterruptedIOException {
        long ceiling = Math.min(MAX_CONFLICT_WAIT_MILLIS, FIRST_CONFLICT_WAIT_MILLIS << conflicts);
        try {
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(ceiling / 2, ceiling + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted = new InterruptedIOException("Interrupted while putting to S3");
            interrupted.initCause(e);
            throw interrupted;
        }
    }

    /**
     * Opens a segment, pinned to its ETag: every range is read with {@code If-Match} on it.
     */
    @Override
    public Optional<Segment> open(String topic, long segment) throws IOException {
        String key = segment(topic, segment);
        Optional<HeadObjectResponse> found = head(key);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        long size = found.get().contentLength();
        String eTag = found.get().eTag();
        return Optional.of(new Segment() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public byte[] read(long offset, int length) throws IOException {
                int available = (int) Math.max(0, Math.min(length, size - offset));
                if (available == 0) {
                    return new byte[0];
                }
                String range = "bytes=" + offset + "-" + (offset + available - 1);
                byte[] bytes;
                try {
                    bytes = s3.getObjectAsBytes(get -> get.bucket(bucket).key(key).range(range).ifMatch(eTag)).asByteArray();
                } catch (AwsServiceException e) {
                    if (e.statusCode() == PRECONDITION_FAILED || e.statusCode() == NOT_FOUND) {
                        throw new IOException("Segment " + key + " is no longer the object that was opened (ETag " + eTag + ")", e);
                    }
                    throw failed("Getting " + range + " of " + key, e);
                } catch (SdkException e) {
                    throw failed("Getting " + range + " of " + key, e);
                }
                if (bytes.length != available) {
                    throw new IOException("S3 sent " + bytes.length + " bytes of " + key + " for " + range);
                }
                return bytes;
            }
        });
    }

    @Override
    public boolean exists(String topic, long segment) throws IOException {
        return head(segment(topic, segment)).isPresent();
    }

    private Optional<HeadObjectResponse> head(String key) throws IOException {
        try {
            HeadObjectResponse response = s3.headObject(head -> head.bucket(bucket).key(key));
            bucketSeen = true;
            return Optional.of(response);
        } catch (AwsServiceException e) {
            if (e.statusCode() == NOT_FOUND) {
                checkBucket();
                return Optional.empty();
            }
            throw failed("Looking up " + key, e);
        } catch (SdkException e) {
            throw failed("Looking up " + key, e);
        }
    }

    /**
     * Fails if the bucket does not exist; asks S3 only until the bucket has been seen once.
     */
    private void checkBucket() throws IOException {
        if (!bucketSeen) {
            call("Listing " + prefix + " to check the bucket", () -> s3.listObjectsV2(list -> list.bucket(bucket).prefix(prefix).maxKeys(1)));
            bucketSeen = true;
        }
    }

    @Override
    public Set<String> metadataNames(String topic) throws IOException {
        String folder = prefix + topic + "/metadata/";
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucket).prefix(folder).delimiter("/").build();
        return call("Listing " + folder, () -> {
            Set<String> names = new TreeSet<>();
            for (S3Object object : s3.listObjectsV2Paginator(request).contents()) {
                names.add(object.key().substring(folder.length()));
            }
            return names;
        });
    }

    @Override
    public Optional<byte[]> readMetadata(String topic, String name) throws IOException {
        return get(metadata(topic, name));
    }

    @Override
    public void writeMetadata(String topic, String name, byte[] value) throws IOException {
        String key = metadata(topic, name);
        call(PUTTING + key, () -> s3.putObject(put(key, "application/octet-stream").build(), RequestBody.fromBytes(value)));
    }

    @Override
    public void deleteMetadata(String topic, String name) throws IOException {
        String key = metadata(topic, name);
        call("Deleting " + key, () -> s3.deleteObject(delete -> delete.bucket(bucket).key(key)));
    }

    @Override
    public void close() {
        try {
            s3.close();
        } finally {
            if (credentials != null) {
                credentials.close();
            }
        }
    }

    private PutObjectRequest.Builder put(String key, String contentType) {
        PutObjectRequest.Builder put = PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType);
        if (encryption != null) {
            put.serverSideEncryption(encryption);
            if (kmsKeyId != null) {
                put.ssekmsKeyId(kmsKeyId);
            }
        }
        return put;
    }

    private Optional<byte[]> get(String key) throws IOException {
        try {
            return Optional.of(s3.getObjectAsBytes(get -> get.bucket(bucket).key(key)).asByteArray());
        } catch (NoSuchKeyException e) {
            return Optional.empty(); // not NoSuchBucket, which is also a 404: a wrong bucket fails instead of reading empty
        } catch (SdkException e) {
            throw failed("Getting " + key, e);
        }
    }

    private static <T> T call(String what, Supplier<T> call) throws IOException {
        try {
            return call.get();
        } catch (SdkException e) {
            throw failed(what, e);
        }
    }

    private static IOException failed(String what, SdkException e) {
        return new IOException(what + " failed: " + e.getMessage(), e);
    }

    private String segment(String topic, long segment) {
        return prefix + topic + "/segments/" + String.format("%019d.avro", segment);
    }

    private String metadata(String topic, String name) {
        String key = prefix + topic + "/metadata/" + name;
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw new IllegalArgumentException("The metadata key is too long for an object key in S3, at most "
                    + MAX_KEY_BYTES + " bytes in UTF-8 with the prefix and topic: " + key);
        }
        return key;
    }

    @Override
    public String toString() {
        return "S3AvroStore{bucket=" + bucket + ", prefix=" + prefix + '}';
    }
}
