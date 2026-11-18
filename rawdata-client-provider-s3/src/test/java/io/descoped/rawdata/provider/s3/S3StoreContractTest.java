package io.descoped.rawdata.provider.s3;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.s3.test.S3Mock;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;

class S3StoreContractTest extends AvroStoreContractTest {

    @Override
    protected AvroStore createStore() {
        return new S3AvroStore(S3Mock.client(), S3Mock.BUCKET, "store/", null, null);
    }

    @Override
    protected AvroStore createMissingStore() {
        return new S3AvroStore(S3Mock.client(), "no-such-bucket", "store/", null, null);
    }
}
