package io.descoped.rawdata.provider.gcs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.http.HttpTimeoutException;
import java.util.zip.CRC32C;
import java.util.Base64;
import java.nio.ByteBuffer;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The few calls of the GCS JSON API (v1) that the store needs, on the JDK's {@link HttpClient}: a media upload with a
 * generation precondition, a download, an object's generation, a listing and a delete.
 * <p>
 * Every call is retried on an I/O error and on 408, 429 and 5xx, with exponential backoff and jitter. That is safe for
 * all of them: reads and deletes are idempotent, and every upload carries a generation precondition, so a retry of an
 * upload that succeeded fails its precondition instead of writing twice; the caller handles that.
 */
final class GcsClient {

    static final String DEFAULT_ENDPOINT = "https://storage.googleapis.com";

    private static final int OK = 200;
    private static final int NO_CONTENT = 204;
    private static final int PARTIAL_CONTENT = 206;
    private static final int UNAUTHORIZED = 401;
    private static final int NOT_FOUND = 404;
    private static final int PRECONDITION_FAILED = 412;
    private static final int TOO_MANY_REQUESTS = 429;
    private static final int SERVICE_UNAVAILABLE = 503;
    private static final int MAX_ATTEMPTS = 6;
    private static final long FIRST_BACKOFF_MILLIS = 200;
    private static final long MAX_BACKOFF_MILLIS = 5_000;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(5);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient http;
    private final String endpoint;
    private final GcsCredentials credentials;

    /**
     * Creates a client.
     *
     * @param endpoint    the API's root, {@link #DEFAULT_ENDPOINT} or an emulator's, without a trailing {@code /}
     * @param credentials what authorizes the requests
     */
    GcsClient(String endpoint, GcsCredentials credentials) {
        this(HttpClient.newBuilder().connectTimeout(CONNECT_TIMEOUT).build(), endpoint, credentials);
    }

    private volatile Runnable throttled = () -> { };

    GcsClient(HttpClient http, String endpoint, GcsCredentials credentials) {
        this.http = http;
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.credentials = credentials;
    }

    /**
     * Uploads an object in one request, if its generation is the given one.
     *
     * @param generation the object's current generation, or 0 for "only if there is no such object"
     * @return true if it was written; false if the precondition failed
     */
    boolean upload(String bucket, String object, String contentType, byte[] content, long generation) throws IOException {
        URI uri = URI.create(endpoint + "/upload/storage/v1/b/" + encode(bucket) + "/o?uploadType=media&name=" + encode(object)
                + "&ifGenerationMatch=" + generation);
        String hash = "crc32c=" + crc32c(content); // GCS refuses the upload (400) if the bytes it got differ
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri)
                .header("Content-Type", contentType)
                .header("X-Goog-Hash", hash)
                .POST(HttpRequest.BodyPublishers.ofByteArray(content)));
        if (response.statusCode() == PRECONDITION_FAILED) {
            return false;
        }
        expect(response, "Uploading " + object);
        return true;
    }

    /**
     * Downloads an object.
     *
     * @return its content, or empty if there is no such object
     */
    Optional<byte[]> download(String bucket, String object) throws IOException {
        URI uri = URI.create(objectUri(bucket, object) + "?alt=media");
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).GET());
        if (response.statusCode() == NOT_FOUND) {
            return Optional.empty();
        }
        expect(response, "Downloading " + object);
        return Optional.of(response.body());
    }

    /**
     * An object's generation, size and checksum.
     *
     * @param generation the generation
     * @param size       the size in bytes
     * @param crc32c     the CRC32C as GCS gives it (base64 of the 4 bytes, big-endian), or null if it gave none
     */
    record ObjectInfo(long generation, long size, String crc32c) {
    }

    /**
     * The CRC32C of bytes as GCS writes it: base64 of the 4 bytes, big-endian.
     */
    static String crc32c(byte[] content) {
        CRC32C crc = new CRC32C();
        crc.update(content);
        return Base64.getEncoder().encodeToString(ByteBuffer.allocate(4).putInt((int) crc.getValue()).array());
    }

    /**
     * Registers what to do when GCS throttles (429, 503) or a request times out, on any attempt.
     */
    void onThrottled(Runnable listener) {
        this.throttled = listener;
    }

    /**
     * The object's generation and size.
     *
     * @return them, or empty if there is no such object
     */
    Optional<ObjectInfo> info(String bucket, String object) throws IOException {
        URI uri = URI.create(objectUri(bucket, object) + "?fields=generation,size,crc32c");
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).GET());
        if (response.statusCode() == NOT_FOUND) {
            return Optional.empty();
        }
        expect(response, "Reading the metadata of " + object);
        JsonNode json = JSON.readTree(response.body());
        JsonNode crc32c = json.path("crc32c");
        return Optional.of(new ObjectInfo(number(json, "generation", object), number(json, "size", object),
                crc32c.isTextual() ? crc32c.asText() : null));
    }

    private static long number(JsonNode json, String field, String object) throws IOException {
        JsonNode value = json.path(field);
        if (value.isMissingNode() || value.isNull()) {
            throw new IOException("GCS returned no " + field + " for " + object);
        }
        return value.isNumber() ? value.asLong() : Long.parseLong(value.asText()); // GCS sends int64 as a string
    }

    /**
     * Downloads a range of one generation of an object.
     *
     * @param generation the generation; another one, or none, fails
     * @param offset     the first byte
     * @param length     how many bytes, 1 or more, within the object
     * @return the bytes
     * @throws IOException if GCS fails, or the generation is gone
     */
    byte[] downloadRange(String bucket, String object, long generation, long offset, int length) throws IOException {
        URI uri = URI.create(objectUri(bucket, object) + "?alt=media&generation=" + generation);
        String range = "bytes=" + offset + "-" + (offset + length - 1);
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).header("Range", range).GET());
        if (response.statusCode() == NOT_FOUND) {
            throw new IOException("Generation " + generation + " of " + object + " is gone: it was replaced or deleted");
        }
        if (response.statusCode() != PARTIAL_CONTENT) {
            expect(response, "Downloading " + range + " of " + object);
        }
        byte[] body = response.body();
        if (response.statusCode() == OK && (offset > 0 || body.length > length)) {
            throw new IOException("GCS sent the whole of " + object + " for " + range); // the Range header was ignored
        }
        if (body.length != length) {
            throw new IOException("GCS sent " + body.length + " bytes of " + object + " for " + range);
        }
        return body;
    }

    /**
     * The object's generation.
     *
     * @return the generation, or empty if there is no such object
     */
    OptionalLong generation(String bucket, String object) throws IOException {
        URI uri = URI.create(objectUri(bucket, object) + "?fields=generation");
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).GET());
        if (response.statusCode() == NOT_FOUND) {
            return OptionalLong.empty();
        }
        expect(response, "Reading the metadata of " + object);
        JsonNode generation = JSON.readTree(response.body()).path("generation");
        if (generation.isMissingNode() || generation.isNull()) {
            throw new IOException("GCS returned no generation for " + object);
        }
        return OptionalLong.of(generation.isNumber() ? generation.asLong() : Long.parseLong(generation.asText()));
    }

    /**
     * Lists the names of the objects directly under a prefix (not in its sub-folders).
     *
     * @param prefix the folder, ending with {@code /}
     * @return the full names, in the order GCS lists them
     */
    List<String> list(String bucket, String prefix) throws IOException {
        List<String> names = new ArrayList<>();
        String pageToken = null;
        do {
            String query = "?prefix=" + encode(prefix) + "&delimiter=" + encode("/") + "&fields=" + encode("items(name),nextPageToken")
                    + (pageToken == null ? "" : "&pageToken=" + encode(pageToken));
            URI uri = URI.create(bucketUri(bucket) + "/o" + query);
            HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).GET());
            expect(response, "Listing " + prefix);
            JsonNode page = JSON.readTree(response.body());
            for (JsonNode item : page.path("items")) {
                names.add(item.path("name").asText());
            }
            JsonNode next = page.path("nextPageToken");
            pageToken = next.isTextual() && !next.asText().isEmpty() ? next.asText() : null;
        } while (pageToken != null);
        return names;
    }

    /**
     * Fails if the bucket cannot be listed under the prefix, for example because it does not exist (404): a cheap
     * listing of at most one object, which needs only the permission the store needs anyway.
     */
    void checkBucket(String bucket, String prefix) throws IOException {
        URI uri = URI.create(bucketUri(bucket) + "/o?prefix=" + encode(prefix) + "&maxResults=1&fields="
                + encode("nextPageToken"));
        expect(send(() -> HttpRequest.newBuilder(uri).GET()), "Listing the bucket " + bucket);
    }

    /**
     * Deletes an object; does nothing if there is none.
     */
    void delete(String bucket, String object) throws IOException {
        URI uri = URI.create(objectUri(bucket, object));
        HttpResponse<byte[]> response = send(() -> HttpRequest.newBuilder(uri).DELETE());
        if (response.statusCode() != NOT_FOUND) {
            expect(response, "Deleting " + object);
        }
    }

    private String bucketUri(String bucket) {
        return endpoint + "/storage/v1/b/" + encode(bucket);
    }

    private String objectUri(String bucket, String object) {
        return bucketUri(bucket) + "/o/" + encode(object);
    }

    private HttpResponse<byte[]> send(Supplier<HttpRequest.Builder> request) throws IOException {
        HttpResponse<byte[]> response = sendWithRetries(request);
        if (response.statusCode() == UNAUTHORIZED && credentials.authorization().isPresent()) {
            credentials.invalidate(); // revoked, or the clock is off: once more with a new token
            response = sendWithRetries(request);
        }
        return response;
    }

    private HttpResponse<byte[]> sendWithRetries(Supplier<HttpRequest.Builder> request) throws IOException {
        for (int attempt = 1; ; attempt++) {
            try {
                HttpResponse<byte[]> response = sendOnce(request);
                if (response.statusCode() == TOO_MANY_REQUESTS || response.statusCode() == SERVICE_UNAVAILABLE) {
                    throttled.run();
                }
                if (!retryable(response.statusCode()) || attempt == MAX_ATTEMPTS) {
                    return response;
                }
            } catch (GcsCredentials.RefusedException e) {
                throw e;
            } catch (IOException e) {
                if (e instanceof HttpTimeoutException) {
                    throttled.run();
                }
                if (attempt == MAX_ATTEMPTS) {
                    throw e;
                }
            }
            backOff(attempt);
        }
    }

    private HttpResponse<byte[]> sendOnce(Supplier<HttpRequest.Builder> request) throws IOException {
        HttpRequest.Builder builder = request.get().timeout(REQUEST_TIMEOUT);
        credentials.authorization().ifPresent(value -> builder.header("Authorization", value)); // a token request is retried like the call
        try {
            return http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw interrupted(e);
        }
    }

    static boolean retryable(int status) {
        return status == 408 || status == 429 || status >= 500;
    }

    private static void backOff(int attempt) throws InterruptedIOException {
        long ceiling = Math.min(MAX_BACKOFF_MILLIS, FIRST_BACKOFF_MILLIS << (attempt - 1));
        try {
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextLong(ceiling / 2, ceiling + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw interrupted(e);
        }
    }

    private static InterruptedIOException interrupted(InterruptedException e) {
        InterruptedIOException interrupted = new InterruptedIOException("Interrupted while calling GCS");
        interrupted.initCause(e);
        return interrupted;
    }

    private static void expect(HttpResponse<byte[]> response, String what) throws IOException {
        int status = response.statusCode();
        if (status != OK && status != NO_CONTENT) {
            throw new IOException(what + " failed: HTTP " + status + " " + message(response.body()));
        }
    }

    /**
     * The error message of a GCS error response, or the start of the body.
     */
    private static String message(byte[] body) {
        try {
            JsonNode message = JSON.readTree(body).path("error").path("message");
            if (message.isTextual()) {
                return message.asText();
            }
        } catch (IOException e) {
            // not JSON; the body itself follows
        }
        String text = new String(body, StandardCharsets.UTF_8);
        return text.length() > 200 ? text.substring(0, 200) + "…" : text;
    }

    /**
     * Percent-encodes a path segment or query value: everything but the unreserved characters of RFC 3986, so
     * {@code /} in an object name is encoded as GCS requires.
     */
    static String encode(String value) {
        StringBuilder encoded = new StringBuilder();
        for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
            int c = b & 0xff;
            if (c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~') {
                encoded.append((char) c);
            } else {
                encoded.append('%').append(Character.toUpperCase(Character.forDigit(c >> 4, 16)))
                        .append(Character.toUpperCase(Character.forDigit(c & 0xf, 16)));
            }
        }
        return encoded.toString();
    }

    @Override
    public String toString() {
        return "GcsClient{endpoint=" + endpoint + ", credentials=" + credentials + '}';
    }
}
