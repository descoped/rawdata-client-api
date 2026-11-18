package io.descoped.rawdata.provider.gcs.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.descoped.rawdata.api.RawdataClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * A fake GCS server (fsouza/fake-gcs-server) in Docker, shared by the tests of a run, with one bucket.
 */
public final class FakeGcs {

    /**
     * The bucket the tests use.
     */
    public static final String BUCKET = "rawdata";

    private static final int PORT = 4443;
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final ObjectMapper JSON = new ObjectMapper();

    @SuppressWarnings("resource") // stopped by Testcontainers when the JVM exits
    private static final GenericContainer<?> SERVER = new GenericContainer<>("fsouza/fake-gcs-server:1.56.1")
            .withExposedPorts(PORT)
            .withCommand("-scheme", "http", "-port", String.valueOf(PORT), "-backend", "memory")
            .waitingFor(Wait.forHttp("/storage/v1/b").forPort(PORT));

    static {
        SERVER.start();
        HttpResponse<String> created = send(HttpRequest.newBuilder(URI.create(endpoint() + "/storage/v1/b?project=test"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"" + BUCKET + "\"}")));
        if (created.statusCode() != 200) {
            throw new IllegalStateException("Cannot create the bucket: HTTP " + created.statusCode() + " " + created.body());
        }
    }

    private FakeGcs() {
    }

    /**
     * The fake server's endpoint.
     *
     * @return {@code http://host:port}
     */
    public static String endpoint() {
        return "http://" + SERVER.getHost() + ":" + SERVER.getMappedPort(PORT);
    }

    /**
     * The objects under a prefix and their content types, for checking what the provider wrote.
     *
     * @param prefix the prefix
     * @return object name to content type, sorted by name
     */
    public static Map<String, String> objects(String prefix) {
        HttpResponse<String> listed = send(HttpRequest.newBuilder(URI.create(endpoint() + "/storage/v1/b/" + BUCKET + "/o?prefix="
                + URLEncoder.encode(prefix, StandardCharsets.UTF_8))).GET());
        try {
            Map<String, String> objects = new TreeMap<>();
            for (JsonNode item : JSON.readTree(listed.body()).path("items")) {
                objects.put(item.path("name").asText(), item.path("contentType").asText());
            }
            return objects;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static HttpResponse<String> send(HttpRequest.Builder request) {
        try {
            return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    static Map<String, String> configuration(String prefix) {
        Map<String, String> configuration = new HashMap<>();
        configuration.put(RawdataClient.PROVIDER_KEY, "gcs");
        configuration.put("gcs.bucket-name", BUCKET);
        configuration.put("gcs.prefix", prefix);
        configuration.put("gcs.credentials", "none");
        configuration.put("gcs.endpoint", endpoint());
        configuration.put("gcs.poll-interval-ms", "50");
        return configuration;
    }
}
