package io.descoped.rawdata.provider.gcs.test;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the contract does not cover: several clients on one bucket, as several processes would be, the objects in the
 * bucket, and the configuration.
 */
@Timeout(120)
class GcsProviderTest {

    private static RawdataClient client(String prefix) {
        return RawdataClient.create(FakeGcs.configuration(prefix));
    }

    private static String prefix() {
        return "provider-" + UUID.randomUUID() + "/";
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", position.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static List<RawdataMessage> readAll(RawdataClient client, String topic) throws InterruptedException {
        try (RawdataConsumer consumer = client.consumer(topic)) {
            List<RawdataMessage> read = new ArrayList<>();
            List<RawdataMessage> batch;
            while (!(batch = consumer.receiveUpTo(1000, Duration.ofMillis(500))).isEmpty()) {
                read.addAll(batch);
            }
            return read;
        }
    }

    /**
     * Clients on one bucket share nothing else, as clients in different processes would not. The precondition on
     * each upload still puts every publish after the last: nothing is lost, ids increase in the order the segments
     * were created, and each producer's own order is kept.
     */
    @Test
    void clientsOnOneBucketKeepTheTopicInCommitOrder() throws Exception {
        String prefix = prefix();
        int clients = 3;
        int perClient = 25;
        List<RawdataClient> opened = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                RawdataClient client = client(prefix);
                opened.add(client);
                String own = "c" + c + "-";
                futures.add(executor.submit(() -> {
                    start.await();
                    try (RawdataProducer producer = client.producer("shared")) {
                        for (int i = 0; i < perClient; i++) {
                            producer.publish(message(own + i));
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(90, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            opened.forEach(RawdataClient::close);
        }
        try (RawdataClient client = client(prefix)) {
            List<RawdataMessage> read = readAll(client, "shared");
            assertEquals(clients * perClient, read.size());
            for (int i = 1; i < read.size(); i++) {
                assertTrue(RawdataIds.ORDER.compare(read.get(i - 1).id(), read.get(i).id()) < 0, "ids must strictly increase at " + i);
            }
            for (int c = 0; c < clients; c++) {
                String own = "c" + c + "-";
                List<String> expected = new ArrayList<>();
                for (int i = 0; i < perClient; i++) {
                    expected.add(own + i);
                }
                assertEquals(expected, read.stream().map(RawdataMessage::position).filter(p -> p.startsWith(own)).toList());
            }
        }
    }

    /**
     * A consumer of one client receives what another client publishes, by polling.
     */
    @Test
    void aConsumerSeesMessagesOfAnotherClient() throws Exception {
        String prefix = prefix();
        try (RawdataClient reader = client(prefix); RawdataClient writer = client(prefix);
             RawdataConsumer consumer = reader.consumer("t")) {
            CompletableFuture<? extends RawdataMessage> received = consumer.receiveAsync();
            writer.producer("t").publish(message("a"));
            assertEquals("a", received.get(30, TimeUnit.SECONDS).position());
        }
    }

    /**
     * Each publish is one object under the prefix, named by its segment number, and each metadata entry another.
     */
    @Test
    void topicsAreObjectsUnderThePrefix() {
        String prefix = prefix();
        try (RawdataClient client = client(prefix)) {
            client.producer("t").publish(message("a"), message("b"));
            client.producer("t").publish(message("c"));
            client.metadata("t").put("checkpoint", new byte[]{1});
        }
        Storage storage = FakeGcs.storage();
        try {
            List<String> names = new ArrayList<>();
            for (Blob blob : storage.list(FakeGcs.BUCKET, Storage.BlobListOption.prefix(prefix)).iterateAll()) {
                names.add(blob.getName());
                if (blob.getName().endsWith(".avro")) {
                    assertEquals("avro/binary", blob.getContentType());
                }
            }
            String checkpoint = "k" + HexFormat.of().formatHex("checkpoint".getBytes(StandardCharsets.UTF_8));
            assertEquals(List.of(
                    prefix + "t/metadata/" + checkpoint,
                    prefix + "t/segments/0000000000000000000.avro",
                    prefix + "t/segments/0000000000000000001.avro"), names.stream().sorted().toList());
        } finally {
            FakeGcs.close(storage);
        }
    }

    /**
     * The bucket is required, the credentials must be a known kind, a service account needs its key file, and the
     * prefix is a relative folder.
     */
    @Test
    void configurationIsChecked() {
        Map<String, String> noBucket = Map.of(RawdataClient.PROVIDER_KEY, "gcs");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noBucket));
        assertTrue(missing.getMessage().contains("gcs.bucket-name"), missing.getMessage());

        Map<String, String> unknown = FakeGcs.configuration("x/");
        unknown.put("gcs.credentials", "magic");
        IllegalArgumentException kind = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(unknown));
        assertTrue(kind.getMessage().contains("gcs.credentials"), kind.getMessage());

        Map<String, String> noKey = FakeGcs.configuration("x/");
        noKey.put("gcs.credentials", "service-account");
        IllegalArgumentException key = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noKey));
        assertTrue(key.getMessage().contains("gcs.service-account.key-file"), key.getMessage());

        Map<String, String> missingKey = FakeGcs.configuration("x/");
        missingKey.put("gcs.credentials", "service-account");
        missingKey.put("gcs.service-account.key-file", "/no/such/key.json");
        assertThrows(UncheckedIOException.class, () -> RawdataClient.create(missingKey));

        Map<String, String> absolute = FakeGcs.configuration("/x/");
        IllegalArgumentException prefix = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(absolute));
        assertTrue(prefix.getMessage().contains("gcs.prefix"), prefix.getMessage());

        Map<String, String> noSlash = FakeGcs.configuration("folder");
        try (RawdataClient client = RawdataClient.create(noSlash)) {
            String topic = "t-" + UUID.randomUUID();
            client.producer(topic).publish(message("a"));
            Storage storage = FakeGcs.storage();
            try {
                assertNotNull(storage.get(FakeGcs.BUCKET, "folder/" + topic + "/segments/0000000000000000000.avro"));
            } finally {
                FakeGcs.close(storage);
            }
        }
    }
}
