package io.descoped.rawdata.smoke;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataClientInitializer;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.service.provider.api.ProviderName;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Lists the providers the service loader finds, and writes and reads a topic through each store it is given:
 * filesystem always, postgres, gcs and s3 when their system properties are set (smoke.postgres.port,
 * smoke.gcs.endpoint, smoke.s3.endpoint). Exits with 1 on the first failure.
 */
public final class Smoke {

    private static final List<String> PROVIDERS = List.of("discard", "filesystem", "gcs", "memory", "postgres", "s3");

    private Smoke() {
    }

    public static void main(String[] args) throws Exception {
        String layout = args.length > 0 ? args[0] : "?";
        Module module = Smoke.class.getModule();
        System.out.println("[" + layout + "] smoke app in " + (module.isNamed() ? "module " + module.getName() : "the unnamed module"));

        TreeSet<String> found = new TreeSet<>();
        ServiceLoader.load(RawdataClientInitializer.class).stream()
                .map(provider -> provider.type().getAnnotation(ProviderName.class))
                .forEach(name -> found.add(name == null ? "?" : name.value()));
        check(found.equals(new TreeSet<>(PROVIDERS)), "providers " + found + ", expected " + PROVIDERS);
        System.out.println("[" + layout + "] providers: " + found);

        roundTrip(layout, Map.of(
                RawdataClient.PROVIDER_KEY, "filesystem",
                "filesystem.storage-folder", Files.createTempDirectory("rawdata-smoke").toString(),
                "filesystem.poll-interval-ms", "50"));
        String postgresPort = System.getProperty("smoke.postgres.port");
        if (postgresPort != null) {
            roundTrip(layout, Map.of(
                    RawdataClient.PROVIDER_KEY, "postgres",
                    "postgres.driver.host", "localhost",
                    "postgres.driver.port", postgresPort,
                    "postgres.driver.user", "rdc",
                    "postgres.driver.password", "rdc",
                    "postgres.driver.database", "rdc",
                    "rawdata.postgres.consumer.prefetch-poll-interval-when-empty", "50"));
        }
        String gcsEndpoint = System.getProperty("smoke.gcs.endpoint");
        if (gcsEndpoint != null) {
            Map<String, String> gcs = new HashMap<>();
            gcs.put(RawdataClient.PROVIDER_KEY, "gcs");
            gcs.put("gcs.bucket-name", "rawdata");
            gcs.put("gcs.credentials", "none");
            gcs.put("gcs.endpoint", gcsEndpoint);
            gcs.put("gcs.prefix", layout + "/");
            gcs.put("gcs.poll-interval-ms", "50");
            roundTrip(layout, gcs);
        }
        String s3Endpoint = System.getProperty("smoke.s3.endpoint");
        if (s3Endpoint != null) {
            Map<String, String> s3 = new HashMap<>();
            s3.put(RawdataClient.PROVIDER_KEY, "s3");
            s3.put("s3.bucket-name", "rawdata");
            s3.put("s3.region", "us-east-1");
            s3.put("s3.endpoint", s3Endpoint);
            s3.put("s3.path-style-access", "true");
            s3.put("s3.credentials", "static");
            s3.put("s3.access-key-id", "smoke");
            s3.put("s3.secret-access-key", "smoke");
            s3.put("s3.prefix", layout + "/");
            s3.put("s3.poll-interval-ms", "50");
            roundTrip(layout, s3);
        }
    }

    private static void roundTrip(String layout, Map<String, String> configuration) throws InterruptedException {
        String provider = configuration.get(RawdataClient.PROVIDER_KEY);
        String topic = "smoke-" + layout + "-" + UUID.randomUUID();
        try (RawdataClient client = RawdataClient.create(configuration)) {
            try (RawdataProducer producer = client.producer(topic)) {
                producer.publish(message("a"), message("b"));
                producer.publish(message("c"));
            }
            client.metadata(topic).put("checkpoint", new byte[]{42});
            List<RawdataMessage> read;
            try (RawdataConsumer consumer = client.consumer(topic)) {
                read = consumer.receiveUpTo(10, Duration.ofSeconds(5));
            }
            check(read.stream().map(RawdataMessage::position).toList().equals(List.of("a", "b", "c")), provider + ": read " + read);
            for (int i = 1; i < read.size(); i++) {
                check(RawdataIds.ORDER.compare(read.get(i - 1).id(), read.get(i).id()) < 0, provider + ": ids out of order");
            }
            check(Arrays.equals(read.get(1).get("payload"), "b".getBytes()), provider + ": payload");
            check(read.get(2).equals(client.lastMessage(topic)), provider + ": last message");
            check(Arrays.equals(client.metadata(topic).get("checkpoint"), new byte[]{42}), provider + ": metadata");
        }
        System.out.println("[" + layout + "] " + provider + ": OK");
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", position.getBytes()).build();
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            System.err.println("SMOKE TEST FAILED: " + what);
            System.exit(1);
        }
    }
}
