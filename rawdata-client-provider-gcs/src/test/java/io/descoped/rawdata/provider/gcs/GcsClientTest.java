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

    private static GcsCredentials token(String token) {
        return new GcsCredentials() {
            @Override
            java.util.Optional<String> authorization() {
                return java.util.Optional.of("Bearer " + token);
            }
        };
    }
}
