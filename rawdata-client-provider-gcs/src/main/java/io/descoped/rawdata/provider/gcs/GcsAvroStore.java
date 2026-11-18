package io.descoped.rawdata.provider.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.descoped.rawdata.provider.avro.AvroStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Topics in a bucket: {@code <prefix><topic>/segments/<number>.avro}, the number in 19 digits, and
 * {@code <prefix><topic>/metadata/<name>}. A segment is uploaded in one request with the precondition that no object
 * has its name, so two writers never both create it, and readers see a whole object or none.
 * <p>
 * The client retries a failed upload. If an upload succeeded but its response was lost, the retry fails its
 * precondition; the store then reads the object, and if it holds the bytes it uploaded (an Avro file carries a random
 * sync marker, so no other writer's file is the same) the segment counts as created.
 * <p>
 * A metadata entry is written with a precondition on the generation it replaces (or that there is none), so the client
 * may retry the upload, for example when GCS limits writes to one object name to about one per second. When another
 * writer changed the entry meanwhile, the write starts over, so the last write wins.
 */
final class GcsAvroStore implements AvroStore {

    private static final int HTTP_NOT_FOUND = 404;
    private static final int HTTP_PRECONDITION_FAILED = 412;
    private static final int MAX_NAME_BYTES = 1024;
    private static final System.Logger LOG = System.getLogger(GcsAvroStore.class.getName());

    private final Storage storage;
    private final String bucket;
    private final String prefix;

    GcsAvroStore(Storage storage, String bucket, String prefix) {
        this.storage = storage;
        this.bucket = bucket;
        this.prefix = prefix;
    }

    @Override
    public boolean create(String topic, long segment, byte[] content) throws IOException {
        BlobId id = segment(topic, segment);
        try {
            storage.create(BlobInfo.newBuilder(id).setContentType("avro/binary").build(), content, Storage.BlobTargetOption.doesNotExist());
            return true;
        } catch (StorageException e) {
            if (e.getCode() != HTTP_PRECONDITION_FAILED) {
                throw new IOException(e.getMessage(), e);
            }
            return read(id).map(stored -> Arrays.equals(content, stored)).orElse(false); // our own upload, if a retry found it
        }
    }

    @Override
    public Optional<byte[]> read(String topic, long segment) throws IOException {
        return read(segment(topic, segment));
    }

    @Override
    public boolean exists(String topic, long segment) throws IOException {
        try {
            return storage.get(segment(topic, segment), Storage.BlobGetOption.fields(Storage.BlobField.NAME)) != null;
        } catch (StorageException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public Set<String> metadataNames(String topic) throws IOException {
        String folder = prefix + topic + "/metadata/";
        Set<String> names = new TreeSet<>();
        try {
            Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(folder), Storage.BlobListOption.currentDirectory(),
                    Storage.BlobListOption.fields(Storage.BlobField.NAME));
            for (Blob blob : page.iterateAll()) {
                if (!blob.isDirectory()) {
                    names.add(blob.getName().substring(folder.length()));
                }
            }
        } catch (StorageException e) {
            throw new IOException(e.getMessage(), e);
        }
        return names;
    }

    @Override
    public Optional<byte[]> readMetadata(String topic, String name) throws IOException {
        return read(metadata(topic, name));
    }

    @Override
    public void writeMetadata(String topic, String name, byte[] value) throws IOException {
        BlobId id = metadata(topic, name);
        BlobInfo info = BlobInfo.newBuilder(id).setContentType("application/octet-stream").build();
        for (; ; ) {
            try {
                Blob current = storage.get(id, Storage.BlobGetOption.fields(Storage.BlobField.GENERATION));
                Storage.BlobTargetOption replaces = current == null
                        ? Storage.BlobTargetOption.doesNotExist()
                        : Storage.BlobTargetOption.generationMatch(current.getGeneration());
                storage.create(info, value, replaces);
                return;
            } catch (StorageException e) {
                if (e.getCode() != HTTP_PRECONDITION_FAILED) {
                    throw new IOException(e.getMessage(), e);
                }
                // another writer changed the entry meanwhile; write again over its value
            }
        }
    }

    @Override
    public void deleteMetadata(String topic, String name) throws IOException {
        try {
            storage.delete(metadata(topic, name));
        } catch (StorageException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            storage.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // closing releases threads and connections; nothing written is lost if it fails
            LOG.log(System.Logger.Level.DEBUG, "Closing the storage client failed", e);
        }
    }

    private Optional<byte[]> read(BlobId id) throws IOException {
        try {
            return Optional.of(storage.readAllBytes(id));
        } catch (StorageException e) {
            if (e.getCode() == HTTP_NOT_FOUND) {
                return Optional.empty();
            }
            throw new IOException(e.getMessage(), e);
        }
    }

    private BlobId segment(String topic, long segment) {
        return BlobId.of(bucket, prefix + topic + "/segments/" + String.format("%019d.avro", segment));
    }

    private BlobId metadata(String topic, String name) {
        String object = prefix + topic + "/metadata/" + name;
        if (object.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException("The metadata key is too long for an object name in GCS, at most "
                    + MAX_NAME_BYTES + " bytes in UTF-8 with the prefix and topic: " + object);
        }
        return BlobId.of(bucket, object);
    }

    @Override
    public String toString() {
        return "GcsAvroStore{bucket=" + bucket + ", prefix=" + prefix + '}';
    }
}
