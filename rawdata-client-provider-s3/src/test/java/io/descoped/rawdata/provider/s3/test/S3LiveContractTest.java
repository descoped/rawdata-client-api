package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.tck.RawdataClientContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Map;

/**
 * The contract against real Amazon S3; see {@link LiveS3}.
 */
@EnabledIfSystemProperty(named = LiveS3.ENABLED, matches = "true")
class S3LiveContractTest extends RawdataClientContractTest {

    @Override
    protected Map<String, String> configuration() {
        return LiveS3.configuration("contract/");
    }

    @AfterAll
    static void removeTheTestObjects() {
        LiveS3.remove("contract/");
    }
}
