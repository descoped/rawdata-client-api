package io.descoped.rawdata.provider.gcs.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.util.Map;

class GcsContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return FakeGcs.configuration("contract/");
    }
}
