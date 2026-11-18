package io.descoped.rawdata.provider.gcs;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.gcs.test.FakeGcs;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;

class GcsStoreContractTest extends AvroStoreContractTest {

    @Override
    protected AvroStore createStore() {
        return new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), FakeGcs.BUCKET, "store/");
    }

    @Override
    protected AvroStore createMissingStore() {
        return new GcsAvroStore(new GcsClient(FakeGcs.endpoint(), GcsCredentials.NONE), "no-such-bucket", "store/");
    }
}
