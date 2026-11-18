package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.provider.gcs.test.FakeGcs;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store's handling of the upload precondition, against the fake GCS server.
 */
class GcsAvroStoreTest {

    private static GcsAvroStore store() {
        return new GcsAvroStore(FakeGcs.storage(), FakeGcs.BUCKET, "store-" + UUID.randomUUID() + "/");
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
            assertArrayEquals(mine, store.read("t", 0));
            assertTrue(store.exists("t", 0));
            assertFalse(store.exists("t", 1));
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
            assertThrows(IllegalArgumentException.class, () -> store.writeMetadata("t", "k" + "a".repeat(1100), new byte[1]));
        } finally {
            store.close();
        }
    }
}
