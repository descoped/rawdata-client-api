package io.descoped.rawdata.provider.gcs.test;

import io.descoped.rawdata.tck.avro.AvroClientContractTest;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalInt;

class GcsContractTest extends AvroClientContractTest {

    private static final String PREFIX = "contract/";

    @Override
    protected Map<String, String> configuration() {
        return FakeGcs.configuration(PREFIX);
    }

    /**
     * An object name is at most 1024 bytes: the prefix, the topic, {@code /metadata/k} and the key in hex.
     */
    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of((1024 - (PREFIX + topic + "/metadata/k").getBytes(StandardCharsets.UTF_8).length) / 2);
    }

    @Override
    protected Map<String, String> missingStoreConfiguration() {
        Map<String, String> configuration = FakeGcs.configuration(PREFIX);
        configuration.put("gcs.bucket-name", "no-such-bucket");
        return configuration;
    }
}
