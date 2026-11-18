package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.util.Map;

class S3ContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return S3Mock.configuration("contract/");
    }
}
