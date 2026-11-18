package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalInt;

class S3ContractTest extends RawdataClientContractTest {

    private static final String PREFIX = "contract/";

    @Override
    protected Map<String, String> configuration() {
        return S3Mock.configuration(PREFIX);
    }

    /**
     * An object key is at most 1024 bytes: the prefix, the topic, {@code /metadata/k} and the key in hex.
     */
    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of((1024 - (PREFIX + topic + "/metadata/k").getBytes(StandardCharsets.UTF_8).length) / 2);
    }

    @Override
    protected Map<String, String> missingStoreConfiguration() {
        Map<String, String> configuration = S3Mock.configuration(PREFIX);
        configuration.put("s3.bucket-name", "no-such-bucket");
        return configuration;
    }
}
