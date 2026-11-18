package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.provider.avro.AvroChecksums;
import io.descoped.rawdata.provider.avro.AvroStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;

/**
 * Topics in a bucket: {@code <prefix><topic>/segments/<number>.avro}, the number in 19 digits, and
 * {@code <prefix><topic>/metadata/<name>}. A segment is uploaded in one request with the precondition that no object
 * has its name ({@code ifGenerationMatch=0}), so two writers never both create it, and readers see a whole object or
 * none.
 * <p>
 * Every upload carries its CRC32C ({@code X-Goog-Hash}), so GCS refuses bytes damaged on the way. The client retries
 * a failed upload. If an upload succeeded but its response was lost, the retry fails its precondition; the store then
 * compares the object's size and CRC32C with its own, and only if they match reads the object. If it holds the bytes
 * it uploaded (an Avro file carries a random sync marker, so no other writer's file is the same) the segment counts
 * as created. Another writer's segment is so told apart without downloading it.
 * <p>
 * A metadata entry is written with a precondition on the generation it replaces (or that there is none), so the client
 * may retry the upload, for example when GCS limits writes to one object name to about one per second. When another
 * writer changed the entry meanwhile, the write starts over, so the last write wins.
 * <p>
 * GCS answers 404 for a missing object and for a missing bucket. The first time the store gets one, it lists the
 * bucket once, so a wrong bucket fails instead of looking like an empty topic.
 */
final class GcsAvroStore implements AvroStore {

    private static final int MAX_NAME_BYTES = 1024;

    private final GcsClient client;
    private final String bucket;
    private final String prefix;
    private volatile boolean bucketSeen;

    GcsAvroStore(GcsClient client, String bucket, String prefix) {
        this.client = client;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    @Override
    public boolean create(String topic, long segment, byte[] content) throws IOException {
        String name = segment(topic, segment);
        if (client.upload(bucket, name, "avro/binary", content, 0)) {
            return true;
        }
        // Taken: by another writer, or by our own upload whose response was lost. The size and CRC32C tell another
        // writer's segment apart without downloading it; only when they match are the bytes compared.
        Optional<GcsClient.ObjectInfo> stored = client.info(bucket, name);
        if (stored.isEmpty() || stored.get().size() != content.length
                || stored.get().crc32c() != null && !stored.get().crc32c().equals(AvroChecksums.crc32c(content))) {
            return false;
        }
        return client.download(bucket, name).map(bytes -> Arrays.equals(content, bytes)).orElse(false);
    }

    @Override
    public void onThrottled(Runnable listener) {
        client.onThrottled(listener);
    }

    /**
     * Opens a segment, pinned to its generation: every range is read from that generation ({@code generation=}).
     */
    @Override
    public Optional<Segment> open(String topic, long segment) throws IOException {
        String name = segment(topic, segment);
        Optional<GcsClient.ObjectInfo> found = client.info(bucket, name);
        if (found.isEmpty()) {
            checkBucket(); // a 404 is a missing object or a missing bucket
            return Optional.empty();
        }
        bucketSeen = true;
        GcsClient.ObjectInfo info = found.get();
        return Optional.of(new Segment() {
            @Override
            public long size() {
                return info.size();
            }

            @Override
            public byte[] read(long offset, int length) throws IOException {
                int available = (int) Math.max(0, Math.min(length, info.size() - offset));
                return available == 0 ? new byte[0] : client.downloadRange(bucket, name, info.generation(), offset, available);
            }
        });
    }

    /**
     * Fails if the bucket does not exist; asks GCS only until the bucket has been seen once.
     */
    private void checkBucket() throws IOException {
        if (!bucketSeen) {
            client.checkBucket(bucket, prefix);
            bucketSeen = true;
        }
    }

    private Optional<byte[]> absentIfTheBucketExists(Optional<byte[]> content) throws IOException {
        if (content.isPresent()) {
            bucketSeen = true;
        } else {
            checkBucket(); // a 404 is a missing object or a missing bucket
        }
        return content;
    }

    @Override
    public boolean exists(String topic, long segment) throws IOException {
        if (client.generation(bucket, segment(topic, segment)).isPresent()) {
            bucketSeen = true;
            return true;
        }
        checkBucket();
        return false;
    }

    @Override
    public Set<String> metadataNames(String topic) throws IOException {
        String folder = prefix + topic + "/metadata/";
        Set<String> names = new TreeSet<>();
        for (String name : client.list(bucket, folder)) {
            names.add(name.substring(folder.length()));
        }
        return names;
    }

    @Override
    public Optional<byte[]> readMetadata(String topic, String name) throws IOException {
        return absentIfTheBucketExists(client.download(bucket, metadata(topic, name)));
    }

    @Override
    public void writeMetadata(String topic, String name, byte[] value) throws IOException {
        String object = metadata(topic, name);
        for (; ; ) {
            OptionalLong current = client.generation(bucket, object);
            if (client.upload(bucket, object, "application/octet-stream", value, current.orElse(0))) {
                return;
            }
            // another writer changed the entry meanwhile; write again over its value
        }
    }

    @Override
    public void deleteMetadata(String topic, String name) throws IOException {
        client.delete(bucket, metadata(topic, name));
    }

    @Override
    public void close() {
        // the JDK's HttpClient holds nothing that needs closing on Java 17
    }

    private String segment(String topic, long segment) {
        return prefix + topic + "/segments/" + String.format("%019d.avro", segment);
    }

    private String metadata(String topic, String name) {
        String object = prefix + topic + "/metadata/" + name;
        if (object.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("The metadata key is too long for an object name in GCS, at most "
                    + MAX_NAME_BYTES + " bytes in UTF-8 with the prefix and topic: " + object);
        }
        return object;
    }

    @Override
    public String toString() {
        return "GcsAvroStore{bucket=" + bucket + ", prefix=" + prefix + '}';
    }
}
