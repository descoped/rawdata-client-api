package io.descoped.rawdata.provider.filesystem;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.avro.AvroTopicFolders;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilesystemStoreContractTest extends AvroStoreContractTest {

    @TempDir
    static Path folder;

    @Override
    protected AvroStore createStore() {
        return new FilesystemAvroStore(folder);
    }

    /**
     * An opened segment reads the file it was opened on: once the file is replaced (only by hand, the engine never
     * replaces a segment), its reads fail instead of mixing two files.
     *
     * @throws IOException if the file system fails
     */
    @Test
    void anOpenedSegmentIsPinnedToItsFile() throws IOException {
        assertTrue(store.create(topic, 0, new byte[]{1, 2, 3}));
        AvroStore.Segment opened = store.open(topic, 0).orElseThrow();
        assertArrayEquals(new byte[]{2, 3}, opened.read(1, 2));
        Path file = folder.resolve(AvroTopicFolders.folderName(topic)).resolve("segments").resolve("0000000000000000000.avro");
        Files.delete(file);
        Files.write(file, new byte[]{9, 9, 9});
        assertThrows(IOException.class, () -> opened.read(0, 3));
    }
}
