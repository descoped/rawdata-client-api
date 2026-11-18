package io.descoped.rawdata.provider.gcs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The client's retries, error messages and name encoding, against a local server.
 */
class GcsClientTest {

    /**
     * 503 and 429 are retried until the call succeeds; the token goes with every attempt.
     */
    @Test
    void retryableResponsesAreRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (LocalHttp gcs = new LocalHttp(request -> switch (calls.incrementAndGet()) {
            case 1 -> LocalHttp.Response.json(503, "{}");
            case 2 -> LocalHttp.Response.json(429, "{}");
            default -> LocalHttp.Response.json(200, "{\"generation\":\"42\"}");
        })) {
            GcsClient client = new GcsClient(gcs.root(), token("t"));
            assertEquals(42, client.generation("b", "o").getAsLong());
            assertEquals(3, gcs.requests.size());
            assertTrue(gcs.requests.stream().allMatch(r -> "Bearer t".equals(r.header("Authorization"))));
        }
    }

    /**
     * 404 means absent, 412 a failed precondition; other errors are not retried and carry GCS's message.
     */
    @Test
    void statusesMeanWhatTheySay() throws Exception {
        try (LocalHttp gcs = new LocalHttp(request -> switch (request.method()) {
            case "GET" -> LocalHttp.Response.json(404, "{}");
            case "POST" -> LocalHttp.Response.json(412, "{}");
            default -> LocalHttp.Response.json(403, "{\"error\":{\"message\":\"robot has no access\"}}");
        })) {
            GcsClient client = new GcsClient(gcs.root(), GcsCredentials.NONE);
            assertTrue(client.download("b", "o").isEmpty());
            assertTrue(client.generation("b", "o").isEmpty());
            assertFalse(client.upload("b", "o", "avro/binary", new byte[]{1}, 0));
            IOException denied = assertThrows(IOException.class, () -> client.delete("b", "o"));
            assertTrue(denied.getMessage().contains("HTTP 403") && denied.getMessage().contains("robot has no access"), denied.getMessage());
            assertEquals(4, gcs.requests.size(), "nothing was retried");
        }
    }

    /**
     * Object names are percent-encoded, '/' included; the upload names the generation it requires.
     */
    @Test
    void namesAreEncodedAndThePreconditionIsSent() throws Exception {
        try (LocalHttp gcs = new LocalHttp(request -> LocalHttp.Response.json(200, "{}"))) {
            GcsClient client = new GcsClient(gcs.root() + "/", GcsCredentials.NONE);
            client.upload("bucket", "p/t é/segments/1.avro", "avro/binary", new byte[]{1}, 0);
            client.delete("bucket", "p/t/metadata/k6b");
            LocalHttp.Request upload = gcs.requests.get(0);
            assertEquals("/upload/storage/v1/b/bucket/o", upload.path());
            assertTrue(upload.query().contains("name=p%2Ft%20%C3%A9%2Fsegments%2F1.avro"), upload.query());
            assertTrue(upload.query().contains("ifGenerationMatch=0"), upload.query());
            assertEquals("/storage/v1/b/bucket/o/p%2Ft%2Fmetadata%2Fk6b", gcs.requests.get(1).path());
        }
        assertEquals("a-b_c.d~%2F%20", GcsClient.encode("a-b_c.d~/ "));
    }

    /**
     * A listing follows nextPageToken until the last page.
     */
    @Test
    void aListingFollowsItsPages() throws Exception {
        try (LocalHttp gcs = new LocalHttp(request -> request.query().contains("pageToken=p2")
                ? LocalHttp.Response.json(200, "{\"items\":[{\"name\":\"f/c\"}]}")
                : LocalHttp.Response.json(200, "{\"items\":[{\"name\":\"f/a\"},{\"name\":\"f/b\"}],\"nextPageToken\":\"p2\"}"))) {
            assertEquals(java.util.List.of("f/a", "f/b", "f/c"), new GcsClient(gcs.root(), GcsCredentials.NONE).list("b", "f/"));
            assertEquals(2, gcs.requests.size());
        }
    }

    /**
     * A failed token request (an I/O error or a 5xx from Google) is retried like the call; a refused one is not.
     */
    @Test
    void aFailedTokenRequestIsRetried() throws Exception {
        AtomicInteger tokens = new AtomicInteger();
        try (LocalHttp google = new LocalHttp(request -> request.path().endsWith("/token")
                ? LocalHttp.Response.json(tokens.incrementAndGet() == 1 ? 503 : 200, "{\"access_token\":\"t\",\"expires_in\":3600}")
                : LocalHttp.Response.json(200, "{\"generation\":\"1\"}"))) {
            GcsCredentials metadata = new GcsCredentials.MetadataServer(google.root(), java.net.http.HttpClient.newHttpClient(), java.time.Clock.systemUTC());
            assertEquals(1, new GcsClient(google.root(), metadata).generation("b", "o").getAsLong());
            assertEquals(2, tokens.get());
        }
        try (LocalHttp refusing = new LocalHttp(request -> LocalHttp.Response.json(400, "{\"error\":\"invalid_grant\"}"))) {
            GcsCredentials metadata = new GcsCredentials.MetadataServer(refusing.root(), java.net.http.HttpClient.newHttpClient(), java.time.Clock.systemUTC());
            assertThrows(GcsCredentials.RefusedException.class, () -> new GcsClient(refusing.root(), metadata).generation("b", "o"));
            assertEquals(1, refusing.requests.size(), "a refused token is not asked for again");
        }
    }

    /**
     * A 401 drops the kept token and tries once more with a new one; a second 401 is the caller's.
     */
    @Test
    void anUnauthorizedResponseRefreshesTheTokenOnce() throws Exception {
        AtomicInteger invalidated = new AtomicInteger();
        GcsCredentials credentials = new GcsCredentials() {
            @Override
            java.util.Optional<String> authorization() {
                return java.util.Optional.of("Bearer " + invalidated.get());
            }

            @Override
            void invalidate() {
                invalidated.incrementAndGet();
            }
        };
        try (LocalHttp gcs = new LocalHttp(request -> "Bearer 0".equals(request.header("Authorization"))
                ? LocalHttp.Response.json(401, "{}") : LocalHttp.Response.json(200, "{\"generation\":\"7\"}"))) {
            assertEquals(7, new GcsClient(gcs.root(), credentials).generation("b", "o").getAsLong());
            assertEquals(1, invalidated.get());
        }
        invalidated.set(0);
        try (LocalHttp gcs = new LocalHttp(request -> LocalHttp.Response.json(401, "{\"error\":{\"message\":\"no\"}}"))) {
            IOException denied = assertThrows(IOException.class, () -> new GcsClient(gcs.root(), credentials).generation("b", "o"));
            assertTrue(denied.getMessage().contains("HTTP 401"), denied.getMessage());
            assertEquals(2, gcs.requests.size());
        }
    }

    private static GcsCredentials token(String token) {
        return new GcsCredentials() {
            @Override
            java.util.Optional<String> authorization() {
                return java.util.Optional.of("Bearer " + token);
            }
        };
    }
}
