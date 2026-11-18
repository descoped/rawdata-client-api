package io.descoped.rawdata.provider.gcs.test;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.descoped.rawdata.api.RawdataClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.HashMap;
import java.util.Map;

/**
 * A fake GCS server (fsouza/fake-gcs-server) in Docker, shared by the tests of a run, with one bucket.
 */
public final class FakeGcs {

    /**
     * The bucket the tests use.
     */
    public static final String BUCKET = "rawdata";

    private static final int PORT = 4443;

    @SuppressWarnings("resource") // stopped by Testcontainers when the JVM exits
    private static final GenericContainer<?> SERVER = new GenericContainer<>("fsouza/fake-gcs-server:1.56.1")
            .withExposedPorts(PORT)
            .withCommand("-scheme", "http", "-port", String.valueOf(PORT), "-backend", "memory")
            .waitingFor(Wait.forHttp("/storage/v1/b").forPort(PORT));

    static {
        SERVER.start();
        Storage storage = storage();
        try {
            storage.create(BucketInfo.of(BUCKET));
        } finally {
            close(storage);
        }
    }

    private FakeGcs() {
    }

    static String endpoint() {
        return "http://" + SERVER.getHost() + ":" + SERVER.getMappedPort(PORT);
    }

    /**
     * A storage client of the fake server, for checking what the provider wrote.
     *
     * @return a new client; close it with {@link #close(Storage)}
     */
    public static Storage storage() {
        return StorageOptions.newBuilder().setHost(endpoint()).setProjectId("test").setCredentials(NoCredentials.getInstance())
                .build().getService();
    }

    /**
     * Closes a storage client.
     *
     * @param storage the client
     */
    public static void close(Storage storage) {
        try {
            storage.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static Map<String, String> configuration(String prefix) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RawdataClient.PROVIDER_KEY, "gcs");
        configuration.put("gcs.bucket-name", BUCKET);
        configuration.put("gcs.prefix", prefix);
        configuration.put("gcs.credentials", "none");
        configuration.put("gcs.project-id", "test");
        configuration.put("gcs.endpoint", endpoint());
        configuration.put("gcs.poll-interval-ms", "50");
        return configuration;
    }
}
