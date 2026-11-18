package io.descoped.rawdata.provider.avro.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.tck.RawdataClientContractTest;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

class FilesystemContractTest extends RawdataClientContractTest {

    @TempDir
    static Path folder;

    @Override
    protected Map<String, String> configuration() {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", folder.toString(),
                "filesystem.poll-interval-ms", "50");
    }
}
