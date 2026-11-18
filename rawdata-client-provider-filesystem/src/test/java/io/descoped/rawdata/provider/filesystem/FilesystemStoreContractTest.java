package io.descoped.rawdata.provider.filesystem;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class FilesystemStoreContractTest extends AvroStoreContractTest {

    @TempDir
    static Path folder;

    @Override
    protected AvroStore createStore() {
        return new FilesystemAvroStore(folder);
    }
}
