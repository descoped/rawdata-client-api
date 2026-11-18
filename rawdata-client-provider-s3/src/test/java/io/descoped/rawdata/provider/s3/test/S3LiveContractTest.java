package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.tck.avro.AvroClientContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

/**
 * The contract against real Amazon S3; see {@link LiveS3}.
 */
@EnabledIfSystemProperty(named = LiveS3.ENABLED, matches = "true")
class S3LiveContractTest extends AvroClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return LiveS3.configuration("contract/");
    }

    @Override
    protected OptionalInt metadataKeyLimit(String topic) {
        return OptionalInt.of((1024 - (LiveS3.RUN + "contract/" + topic + "/metadata/k").getBytes(StandardCharsets.UTF_8).length) / 2);
    }

    /**
     * A bucket name nobody has; S3 answers NoSuchBucket (or AccessDenied, should someone own it), both failures.
     */
    @Override
    protected Map<String, String> missingStoreConfiguration() {
        Map<String, String> configuration = LiveS3.configuration("contract/");
        configuration.put("s3.bucket-name", "descoped-no-such-bucket-" + UUID.randomUUID());
        return configuration;
    }

    @AfterAll
    static void removeTheTestObjects() {
        LiveS3.remove("contract/");
    }
}
