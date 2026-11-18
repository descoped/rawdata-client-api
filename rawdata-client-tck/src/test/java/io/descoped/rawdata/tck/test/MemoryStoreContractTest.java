package io.descoped.rawdata.tck.test;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.tck.avro.AvroStoreContractTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The store contract against the smallest store that keeps it: maps in memory. It checks the TCK itself, as
 * {@link MemoryContractTest} does for the client contract.
 */
class MemoryStoreContractTest extends AvroStoreContractTest {

    @Override
    protected AvroStore createStore() {
        return new MemoryStore(false);
    }

    @Override
    protected AvroStore createMissingStore() {
        return new MemoryStore(true);
    }

    private static final class MemoryStore implements AvroStore {

        private final boolean missing;
        private final Map<String, byte[]> segments = new ConcurrentHashMap<>();
        private final Map<String, Map<String, byte[]>> metadata = new ConcurrentHashMap<>();

        MemoryStore(boolean missing) {
            this.missing = missing;
        }

        private void check() throws IOException {
            if (missing) {
                throw new IOException("No such store");
            }
        }

        private static String key(String topic, long segment) {
            return topic + "/" + segment;
        }

        @Override
        public boolean create(String topic, long segment, byte[] content) throws IOException {
            check();
            byte[] stored = segments.putIfAbsent(key(topic, segment), content.clone());
            return stored == null || Arrays.equals(stored, content);
        }

        @Override
        public Optional<byte[]> read(String topic, long segment) throws IOException {
            check();
            return Optional.ofNullable(segments.get(key(topic, segment))).map(byte[]::clone);
        }

        @Override
        public boolean exists(String topic, long segment) throws IOException {
            check();
            return segments.containsKey(key(topic, segment));
        }

        @Override
        public Set<String> metadataNames(String topic) throws IOException {
            check();
            return new TreeSet<>(metadata.getOrDefault(topic, Map.of()).keySet());
        }

        @Override
        public Optional<byte[]> readMetadata(String topic, String name) throws IOException {
            check();
            return Optional.ofNullable(metadata.getOrDefault(topic, Map.of()).get(name)).map(byte[]::clone);
        }

        @Override
        public void writeMetadata(String topic, String name, byte[] value) throws IOException {
            check();
            metadata.computeIfAbsent(topic, t -> new ConcurrentHashMap<>()).put(name, value.clone());
        }

        @Override
        public void deleteMetadata(String topic, String name) throws IOException {
            check();
            Map<String, byte[]> entries = metadata.get(topic);
            if (entries != null) {
                entries.remove(name);
            }
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
