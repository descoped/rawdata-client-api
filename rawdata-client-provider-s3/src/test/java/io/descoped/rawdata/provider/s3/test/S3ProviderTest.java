package io.descoped.rawdata.provider.s3.test;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider's internals, which the TCK cannot see, against S3Mock: the objects in the bucket, encryption, the
 * configuration and the credential kinds.
 */
@Timeout(120)
class S3ProviderTest {

    private static RawdataClient client(String prefix) {
        return RawdataClient.create(S3Mock.configuration(prefix));
    }

    private static String prefix() {
        return "provider-" + UUID.randomUUID() + "/";
    }

    static RawdataMessage message(String position) {
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
        Map<String, String> objects = S3Mock.objects(prefix);
        String checkpoint = "k" + HexFormat.of().formatHex("checkpoint".getBytes(StandardCharsets.UTF_8));
        assertEquals(List.of(
                prefix + "t/metadata/" + checkpoint,
                prefix + "t/segments/0000000000000000000.avro"), List.copyOf(objects.keySet()), "two publishes, one window, one object");
        assertEquals("avro/binary", objects.get(prefix + "t/segments/0000000000000000000.avro"));
    }

    /**
     * The bucket is required; the credentials, path style and encryption must be known values; static credentials
     * need both keys, a profile its name, and a KMS key aws:kms; the prefix is a relative folder.
     */
    @Test
    void configurationIsChecked() {
        Map<String, String> noBucket = Map.of(RawdataClient.PROVIDER_KEY, "s3");
        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(noBucket));
        assertTrue(missing.getMessage().contains("s3.bucket-name"), missing.getMessage());

        refused("s3.credentials", Map.of("s3.credentials", "magic"));
        refused("s3.secret-access-key", Map.of("s3.secret-access-key", ""));
        refused("s3.profile", Map.of("s3.credentials", "profile"));
        refused("s3.path-style-access", Map.of("s3.path-style-access", "yes"));
        refused("s3.sse", Map.of("s3.sse", "rot13"));
        refused("s3.sse-kms-key-id", Map.of("s3.sse", "AES256", "s3.sse-kms-key-id", "alias/x"));
        refused("s3.prefix", Map.of("s3.prefix", "/x/"));
        refused("s3.prefix", Map.of("s3.prefix", "p".repeat(659)));

        Map<String, String> noSlash = S3Mock.configuration("folder");
        try (RawdataClient client = RawdataClient.create(noSlash)) {
            String topic = "t-" + UUID.randomUUID();
            try (RawdataProducer producer = client.producer(topic)) {
                producer.publish(message("a")); // closing uploads the window
            }
            assertTrue(S3Mock.objects("folder/").containsKey("folder/" + topic + "/segments/0000000000000000000.avro"));
        }
    }

    /**
     * Server-side encryption is asked for on every put when configured.
     */
    @Test
    void serverSideEncryptionIsAskedFor() {
        String prefix = prefix();
        Map<String, String> configuration = S3Mock.configuration(prefix);
        configuration.put("s3.sse", "AES256");
        try (RawdataClient client = RawdataClient.create(configuration)) {
            client.producer("t").publish(message("a"));
            client.metadata("t").put("k", new byte[]{1});
        }
        try (software.amazon.awssdk.services.s3.S3Client s3 = S3Mock.client()) {
            for (String key : S3Mock.objects(prefix).keySet()) {
                assertEquals("AES256", s3.headObject(h -> h.bucket(S3Mock.BUCKET).key(key)).serverSideEncryptionAsString(), key);
            }
        }
    }

    /**
     * The other credential kinds build a client without asking AWS anything until the first call.
     */
    @Test
    void everyCredentialKindBuildsAClient() {
        for (Map<String, String> change : List.of(
                Map.of("s3.credentials", "anonymous"),
                Map.of("s3.credentials", "default"),
                Map.of("s3.credentials", "profile", "s3.profile", "no-such-profile"),
                Map.of("s3.sse", "aws:kms", "s3.sse-kms-key-id", "alias/rawdata"))) {
            Map<String, String> configuration = S3Mock.configuration("x/");
            configuration.putAll(change);
            RawdataClient.create(configuration).close();
        }
        Map<String, String> noRegionOrEndpoint = S3Mock.configuration("x/");
        noRegionOrEndpoint.remove("s3.endpoint");
        noRegionOrEndpoint.put("s3.region", "eu-north-1");
        RawdataClient.create(noRegionOrEndpoint).close();
    }

    private static void refused(String key, Map<String, String> change) {
        Map<String, String> configuration = S3Mock.configuration("x/");
        change.forEach((k, v) -> {
            if (v.isEmpty()) {
                configuration.remove(k);
            } else {
                configuration.put(k, v);
            }
        });
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> RawdataClient.create(configuration));
        assertTrue(refused.getMessage().contains(key), refused.getMessage());
    }
}
