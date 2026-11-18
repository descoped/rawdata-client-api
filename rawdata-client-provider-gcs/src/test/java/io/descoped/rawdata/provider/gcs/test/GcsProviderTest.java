package io.descoped.rawdata.provider.gcs.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider's internals, which the TCK cannot see: the objects in the bucket, and the configuration.
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
        Map<String, String> objects = FakeGcs.objects(prefix);
        String checkpoint = "k" + HexFormat.of().formatHex("checkpoint".getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of(
                prefix + "t/metadata/" + checkpoint,
                prefix + "t/segments/0000000000000000000.avro"), List.copyOf(objects.keySet()), "two publishes, one window, one object");
        assertEquals("avro/binary", objects.get(prefix + "t/segments/0000000000000000000.avro"));
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
        unknown.put("gcs.credential-provider", "magic");
        IllegalArgumentException kind = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(unknown));
        assertTrue(kind.getMessage().contains("gcs.credential-provider"), kind.getMessage());

        Map<String, String> computeEngine = FakeGcs.configuration("x/");
        computeEngine.put("gcs.credential-provider", "Compute_Engine"); // as dynamic-configuration reads enums
        RawdataClient.create(computeEngine).close(); // no token is asked for until a request

        Map<String, String> noKey = FakeGcs.configuration("x/");
        noKey.put("gcs.credential-provider", "service-account");
        IllegalArgumentException key = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noKey));
        assertTrue(key.getMessage().contains("gcs.service-account.key-file"), key.getMessage());

        Map<String, String> longPrefix = FakeGcs.configuration("p".repeat(659));
        IllegalArgumentException tooLong = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(longPrefix));
        assertTrue(tooLong.getMessage().contains("gcs.prefix"), tooLong.getMessage());

        Map<String, String> missingKey = FakeGcs.configuration("x/");
        missingKey.put("gcs.credential-provider", "service-account");
        missingKey.put("gcs.service-account.key-file", "/no/such/key.json");
        assertThrows(UncheckedIOException.class, () -> RawdataClient.create(missingKey));

        Map<String, String> absolute = FakeGcs.configuration("/x/");
        IllegalArgumentException prefix = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(absolute));
        assertTrue(prefix.getMessage().contains("gcs.prefix"), prefix.getMessage());

        Map<String, String> noSlash = FakeGcs.configuration("folder");
        try (RawdataClient client = RawdataClient.create(noSlash)) {
            String topic = "t-" + UUID.randomUUID();
            try (RawdataProducer producer = client.producer(topic)) {
                producer.publish(message("a")); // closing uploads the window
            }
            assertTrue(FakeGcs.objects("folder/").containsKey("folder/" + topic + "/segments/0000000000000000000.avro"));
        }
    }
}
