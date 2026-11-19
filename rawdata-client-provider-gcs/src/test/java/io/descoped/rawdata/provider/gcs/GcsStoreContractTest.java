package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.gcs.test.FakeGcs;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GcsStoreContractTest extends AvroStoreContractTest {

    @Override
    protected AvroStore createStore() {
        return new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), FakeGcs.BUCKET, "store/");
    }

    @Override
    protected AvroStore createMissingStore() {
        return new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), "no-such-bucket", "store/");
    }

    /**
     * An opened segment reads the generation it was opened on: once the object is replaced (only by hand, the engine
     * never replaces a segment), its reads fail instead of mixing two objects.
     *
     * @throws IOException if GCS fails
     */
    @Test
    void anOpenedSegmentIsPinnedToItsGeneration() throws IOException {
        GcsClient client = new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE);
        assertTrue(store.create(topic, 0, new byte[]{1, 2, 3}));
        AvroStore.Segment opened = store.open(topic, 0).orElseThrow();
        assertArrayEquals(new byte[]{2, 3}, opened.read(1, 2));
        String name = "store/" + topic + "/segments/0000000000000000000.avro";
        long generation = client.generation(FakeGcs.BUCKET, name).orElseThrow();
        assertTrue(client.upload(FakeGcs.BUCKET, name, "avro/binary", new byte[]{9, 9, 9}, generation, false));
        assertThrows(IOException.class, () -> opened.read(0, 3));
    }
}
