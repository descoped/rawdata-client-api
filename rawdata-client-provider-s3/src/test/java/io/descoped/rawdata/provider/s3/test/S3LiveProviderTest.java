package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several writers on one topic, against real Amazon S3, which applies {@code If-None-Match} atomically; see
 * {@link LiveS3}.
 */
@EnabledIfSystemProperty(named = LiveS3.ENABLED, matches = "true")
@Timeout(300)
class S3LiveProviderTest {

    /**
     * Clients on one bucket share nothing else, as clients in different processes would not. The condition on
     * each put still puts every publish after the last: nothing is lost, ids increase in the order the segments
     * were created, and each producer's own order is kept.
     */
    @Test
    void clientsOnOneBucketKeepTheTopicInCommitOrder() throws Exception {
        String prefix = "provider/";
        int clients = 3;
        int perClient = 25;
        List<RawdataClient> opened = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(clients);
        try {
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();
            for (int c = 0; c < clients; c++) {
                RawdataClient client = RawdataClient.create(LiveS3.configuration(prefix));
                opened.add(client);
                String own = "c" + c + "-";
                futures.add(executor.submit(() -> {
                    start.await();
                    try (RawdataProducer producer = client.producer("shared")) {
                        for (int i = 0; i < perClient; i++) {
                            producer.publish(S3ProviderTest.message(own + i));
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
        try (RawdataClient client = RawdataClient.create(LiveS3.configuration(prefix))) {
            List<RawdataMessage> read = S3ProviderTest.readAll(client, "shared");
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

    @AfterAll
    static void removeTheTestObjects() {
        LiveS3.remove("provider/");
    }
}
