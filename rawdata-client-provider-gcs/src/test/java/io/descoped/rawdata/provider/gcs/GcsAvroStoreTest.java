package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.provider.gcs.test.FakeGcs;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store's handling of the upload precondition, against the fake GCS server.
 */
class GcsAvroStoreTest {

    private static GcsAvroStore store() {
        return new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), FakeGcs.BUCKET, "store-" + UUID.randomUUID() + "/");
    }

    /**
     * A create that finds its own bytes, as a retry after a lost response does, counts as created; another writer's
     * bytes do not, and the segment keeps them.
     */
    @Test
    void aRetriedUploadIsRecognisedAndAnotherWritersIsNot() throws Exception {
        GcsAvroStore store = store();
        try {
            byte[] mine = {1, 2, 3};
            assertTrue(store.create("t", 0, mine));
            assertTrue(store.create("t", 0, mine.clone()), "the same bytes are this writer's upload");
            assertFalse(store.create("t", 0, new byte[]{9}), "other bytes are another writer's");
            assertArrayEquals(mine, store.read("t", 0).orElseThrow());
            assertTrue(store.exists("t", 0));
            assertFalse(store.exists("t", 1));
        } finally {
            store.close();
        }
    }

    /**
     * Metadata is written with a precondition on the generation it replaces, so overwriting and writing again work.
     */
    @Test
    void metadataIsReplacedAndRemoved() throws Exception {
        GcsAvroStore store = store();
        try {
            store.writeMetadata("t", "k01", new byte[]{1});
            store.writeMetadata("t", "k01", new byte[]{2});
            store.writeMetadata("t", "k02", new byte[]{3});
            assertArrayEquals(new byte[]{2}, store.readMetadata("t", "k01").orElseThrow());
            assertEquals(Set.of("k01", "k02"), store.metadataNames("t"));
            store.deleteMetadata("t", "k01");
            store.deleteMetadata("t", "k01");
            assertTrue(store.readMetadata("t", "k01").isEmpty());
        } finally {
            store.close();
        }
    }

    /**
     * A wrong bucket is an error, not an empty topic.
     */
    @Test
    void aWrongBucketIsAnError() {
        GcsAvroStore store = new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), "no-such-bucket", "");
        try {
            java.io.IOException exists = assertThrows(java.io.IOException.class, () -> store.exists("t", 0));
            assertTrue(exists.getMessage().contains("HTTP 404"), exists.getMessage());
        } finally {
            store.close();
        }
    }

    /**
     * Object names are at most 1024 bytes; a longer metadata name is refused before the request.
     */
    @Test
    void aTooLongMetadataNameIsRefused() {
        GcsAvroStore store = store();
        try {
            String tooLong = "k" + "a".repeat(1100);
            byte[] value = new byte[1];
            assertThrows(IllegalArgumentException.class, () -> store.writeMetadata("t", tooLong, value));
        } finally {
            store.close();
        }
    }
}
