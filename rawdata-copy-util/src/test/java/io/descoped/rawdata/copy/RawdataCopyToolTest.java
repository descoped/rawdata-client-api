package io.descoped.rawdata.copy;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
class RawdataCopyToolTest {

    @TempDir
    Path folder;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private Map<String, String> filesystem(String name) {
        return Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", folder.resolve(name).toString(),
                "filesystem.poll-interval-ms", "20");
    }

    private Path config(Map<String, String> source, Map<String, String> target) throws IOException {
        Map<String, String> all = new LinkedHashMap<>();
        source.forEach((key, value) -> all.put("source." + key, value));
        target.forEach((key, value) -> all.put("target." + key, value));
        Path file = folder.resolve("copy-" + UUID.randomUUID() + ".properties");
        Files.writeString(file, all.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue().replace("\\", "\\\\"))
                .collect(Collectors.joining("\n")));
        return file;
    }

    private int run(String... args) {
        out.reset();
        err.reset();
        return RawdataCopyTool.run(args, new PrintStream(out, true, StandardCharsets.UTF_8), new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String out() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String err() {
        return err.toString(StandardCharsets.UTF_8);
    }

    private static void publish(Map<String, String> configuration, String topic, int from, int to) {
        try (RawdataClient client = RawdataClient.create(configuration); RawdataProducer producer = client.producer(topic)) {
            for (int i = from; i < to; i++) {
                producer.publish(RawdataMessage.builder().position("m" + i).orderingGroup("g").sequenceNumber(i)
                        .put("payload", ("value " + i).getBytes(StandardCharsets.UTF_8)).build());
            }
        }
    }

    private static List<RawdataMessage> read(Map<String, String> configuration, String topic) throws InterruptedException {
        try (RawdataClient client = RawdataClient.create(configuration); RawdataConsumer consumer = client.consumer(topic)) {
            List<RawdataMessage> read = new ArrayList<>();
            List<RawdataMessage> batch;
            while (!(batch = consumer.receiveUpTo(1000, Duration.ofMillis(300))).isEmpty()) {
                read.addAll(batch);
            }
            return read;
        }
    }

    @Test
    void copiesTopicsKeepingIdsAndOrder() throws Exception {
        Map<String, String> source = filesystem("source");
        Map<String, String> target = filesystem("target");
        publish(source, "a", 0, 25);
        publish(source, "b", 0, 3);
        Path config = config(source, target);
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--batch-size", "7", "--idle-timeout-ms", "200", "a", "b:renamed"), err());
        assertEquals("a -> a: 25 messages\nb -> renamed: 3 messages\n", out().replace(System.lineSeparator(), "\n"));
        assertEquals(read(source, "a"), read(target, "a"));
        assertEquals(read(source, "b"), read(target, "renamed"));
    }

    /**
     * A second run continues after the target's last id and copies only what is new.
     */
    @Test
    void aSecondRunContinuesWhereTheFirstStopped() throws Exception {
        Map<String, String> source = filesystem("source");
        Map<String, String> target = filesystem("target");
        Path config = config(source, target);
        publish(source, "a", 0, 10);
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--idle-timeout-ms", "200", "a"), err());
        UUID last = read(target, "a").get(9).id();
        publish(source, "a", 10, 15);
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--idle-timeout-ms", "200", "a"), err());
        assertEquals("a -> a: 5 messages after " + last + "\n", out().replace(System.lineSeparator(), "\n"));
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--idle-timeout-ms", "200", "a"), err());
        assertTrue(out().startsWith("a -> a: 0 messages after"), out());
        assertEquals(read(source, "a"), read(target, "a"));
    }

    @Test
    void metadataIsCopiedWhenAsked() throws Exception {
        Map<String, String> source = filesystem("source");
        Map<String, String> target = filesystem("target");
        try (RawdataClient client = RawdataClient.create(source)) {
            client.metadata("a").put("checkpoint", new byte[]{1, 2}).put("owner", new byte[]{3});
        }
        Path config = config(source, target);
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--idle-timeout-ms", "100", "a"), err());
        try (RawdataClient client = RawdataClient.create(target)) {
            assertEquals(Set.of(), client.metadata("a").keys());
        }
        assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--idle-timeout-ms", "100", "--metadata", "a"), err());
        assertEquals("a -> a: 0 messages, 2 metadata entries\n", out().replace(System.lineSeparator(), "\n"));
        try (RawdataClient client = RawdataClient.create(target)) {
            assertArrayEquals(new byte[]{1, 2}, client.metadata("a").get("checkpoint"));
            assertArrayEquals(new byte[]{3}, client.metadata("a").get("owner"));
        }
    }

    @Test
    void aCopyCanStartAtAnIdOrATime() throws Exception {
        Map<String, String> source = filesystem("source");
        long base = System.currentTimeMillis() - 100_000;
        try (RawdataClient client = RawdataClient.create(source); RawdataProducer producer = client.producer("a")) {
            for (int i = 0; i < 10; i++) {
                producer.publish(RawdataMessage.builder().id(RawdataIds.beginningOf(base + 1000L * i)).position("m" + i).build());
            }
        }
        Path byId = config(source, filesystem("by-id"));
        assertEquals(RawdataCopyTool.OK, run("--config", byId.toString(), "--idle-timeout-ms", "100",
                "--from-id", RawdataIds.beginningOf(base + 7000).toString(), "a"), err());
        assertEquals(List.of("m7", "m8", "m9"), read(filesystem("by-id"), "a").stream().map(RawdataMessage::position).toList());

        Path byTime = config(source, filesystem("by-time"));
        assertEquals(RawdataCopyTool.OK, run("--config", byTime.toString(), "--idle-timeout-ms", "100",
                "--from-time", Instant.ofEpochMilli(base + 8500).toString(), "a"), err());
        assertEquals(List.of("m9"), read(filesystem("by-time"), "a").stream().map(RawdataMessage::position).toList());

        Path byMillis = config(source, filesystem("by-millis"));
        assertEquals(RawdataCopyTool.OK, run("--config", byMillis.toString(), "--idle-timeout-ms", "100",
                "--from-time", String.valueOf(base + 8000), "a"), err());
        assertEquals(List.of("m8", "m9"), read(filesystem("by-millis"), "a").stream().map(RawdataMessage::position).toList());
    }

    /**
     * A move from the filesystem provider to PostgreSQL 18 keeps every field.
     */
    @Test
    void copiesFromFilesystemToPostgres() throws Exception {
        try (PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18-alpine")) {
            postgres.start();
            Map<String, String> target = Map.of(
                    RawdataClient.PROVIDER_KEY, "postgres",
                    "postgres.driver.host", postgres.getHost(),
                    "postgres.driver.port", String.valueOf(postgres.getFirstMappedPort()),
                    "postgres.driver.user", postgres.getUsername(),
                    "postgres.driver.password", postgres.getPassword(),
                    "postgres.driver.database", postgres.getDatabaseName(),
                    "rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "50");
            Map<String, String> source = filesystem("source");
            publish(source, "a", 0, 50);
            Path config = config(source, target);
            assertEquals(RawdataCopyTool.OK, run("--config", config.toString(), "--batch-size", "20", "--idle-timeout-ms", "200", "a"), err());
            assertEquals(read(source, "a"), read(target, "a"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "--config", "--unknown x", "a", "--config missing.properties a", "--config CONFIG",
            "--config CONFIG --from-id 00000000-0000-4000-8000-000000000000 a",
            "--config CONFIG --from-time yesterday a", "--config CONFIG --from-time 1 --from-id ID a",
            "--config CONFIG a/b", "--config CONFIG --batch-size 0 a", "--config CONFIG --idle-timeout-ms x a"})
    void wrongArgumentsAreReported(String line) throws Exception {
        Path config = config(filesystem("source"), filesystem("target"));
        String[] args = line.isEmpty() ? new String[0] : line.replace("CONFIG", config.toString())
                .replace("ID", RawdataIds.generate().toString()).split(" ");
        assertEquals(RawdataCopyTool.USAGE, run(args), out());
        assertTrue(err().contains("Usage: rawdata-copy"), err());
    }

    @Test
    void aConfigurationWithoutTargetIsReported() throws Exception {
        Path config = config(filesystem("source"), Map.of());
        assertEquals(RawdataCopyTool.USAGE, run("--config", config.toString(), "a"));
        assertTrue(err().contains("target.rawdata.client.provider"), err());
    }

    @Test
    void aFailedCopyIsReported() throws Exception {
        Path config = config(Map.of(RawdataClient.PROVIDER_KEY, "no-such-provider"), filesystem("target"));
        assertEquals(RawdataCopyTool.FAILED, run("--config", config.toString(), "a"));
        assertTrue(err().contains("copy failed"), err());
    }

    @Test
    void helpShowsTheUsage() {
        assertEquals(RawdataCopyTool.OK, run("--help"));
        assertTrue(out().startsWith("Usage: rawdata-copy"), out());
    }
}
