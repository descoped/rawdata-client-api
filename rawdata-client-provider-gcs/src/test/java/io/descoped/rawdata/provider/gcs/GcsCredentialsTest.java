package io.descoped.rawdata.provider.gcs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The token sources against a local server playing Google's token endpoint and metadata server.
 */
class GcsCredentialsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    Path folder;

    private static Map<String, String> form(String body) {
        return Arrays.stream(body.split("&")).map(field -> field.split("=", 2))
                .collect(Collectors.toMap(f -> URLDecoder.decode(f[0], StandardCharsets.UTF_8), f -> URLDecoder.decode(f[1], StandardCharsets.UTF_8)));
    }

    private static String pem(byte[] pkcs8) {
        return "-----BEGIN PRIVATE KEY-----\n" + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(pkcs8)
                + "\n-----END PRIVATE KEY-----\n";
    }

    private Path write(String name, Map<String, String> json) throws IOException {
        return Files.write(folder.resolve(name), JSON.writeValueAsBytes(json));
    }

    /**
     * A service account signs a JWT with its key; the token endpoint gets it as a jwt-bearer grant with the storage
     * scope, the key id, and the token URI as audience, and the token is kept until shortly before it expires.
     */
    @Test
    void aServiceAccountExchangesASignedJwt() throws Exception {
        KeyPair keys = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        try (LocalHttp google = new LocalHttp(request -> {
            Map<String, String> form = form(request.body());
            String[] jwt = form.get("assertion").split("\\.");
            try {
                Signature signature = Signature.getInstance("SHA256withRSA");
                signature.initVerify(keys.getPublic());
                signature.update((jwt[0] + "." + jwt[1]).getBytes(StandardCharsets.US_ASCII));
                boolean signed = signature.verify(Base64.getUrlDecoder().decode(jwt[2]));
                JsonNode header = JSON.readTree(Base64.getUrlDecoder().decode(jwt[0]));
                JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(jwt[1]));
                boolean valid = signed && "urn:ietf:params:oauth:grant-type:jwt-bearer".equals(form.get("grant_type"))
                        && "RS256".equals(header.path("alg").asText()) && "key-1".equals(header.path("kid").asText())
                        && "robot@project.iam.gserviceaccount.com".equals(claims.path("iss").asText())
                        && GcsCredentials.SCOPE.equals(claims.path("scope").asText())
                        && claims.path("aud").asText().endsWith("/token")
                        && claims.path("exp").asLong() - claims.path("iat").asLong() == 3600;
                return valid ? LocalHttp.Response.json(200, "{\"access_token\":\"sa-token\",\"expires_in\":3600}")
                        : LocalHttp.Response.json(400, "{\"error\":\"invalid_grant\"}");
            } catch (Exception e) {
                return LocalHttp.Response.json(500, "{}");
            }
        })) {
            Map<String, String> key = new HashMap<>();
            key.put("type", "service_account");
            key.put("client_email", "robot@project.iam.gserviceaccount.com");
            key.put("private_key_id", "key-1");
            key.put("private_key", pem(keys.getPrivate().getEncoded()));
            key.put("token_uri", google.root() + "/token");
            GcsCredentials credentials = GcsCredentials.fromFile(write("key.json", key), HTTP);

            assertEquals(Optional.of("Bearer sa-token"), credentials.authorization());
            assertEquals(Optional.of("Bearer sa-token"), credentials.authorization());
            assertEquals(1, google.requests.size(), "the token is kept");
            assertFalse(credentials.toString().contains("PRIVATE"), credentials.toString());
        }
    }

    /**
     * A token is fetched again once it is within five minutes of expiring.
     */
    @Test
    void aTokenIsRefreshedBeforeItExpires() throws Exception {
        try (LocalHttp google = new LocalHttp(request -> LocalHttp.Response.json(200, "{\"access_token\":\"t\",\"expires_in\":3600}"))) {
            MutableClock clock = new MutableClock();
            GcsCredentials credentials = new GcsCredentials.MetadataServer(google.root(), HTTP, clock);
            credentials.authorization();
            clock.now = clock.now.plusSeconds(3600 - 301);
            credentials.authorization();
            assertEquals(1, google.requests.size(), "still more than five minutes left");
            clock.now = clock.now.plusSeconds(2);
            credentials.authorization();
            assertEquals(2, google.requests.size(), "refreshed within five minutes of expiry");
        }
    }

    /**
     * A short-lived token is used for half its lifetime, not fetched on every call; invalidate drops it.
     */
    @Test
    void aShortLivedTokenIsKeptForHalfItsLifetime() throws Exception {
        assertEquals(java.time.Duration.ofMinutes(55), GcsCredentials.Token.lifetime(java.time.Duration.ofHours(1)));
        assertEquals(java.time.Duration.ofSeconds(120), GcsCredentials.Token.lifetime(java.time.Duration.ofSeconds(240)));
        assertEquals(java.time.Duration.ZERO, GcsCredentials.Token.lifetime(java.time.Duration.ZERO));
        try (LocalHttp google = new LocalHttp(request -> LocalHttp.Response.json(200, "{\"access_token\":\"t\",\"expires_in\":240}"))) {
            MutableClock clock = new MutableClock();
            GcsCredentials credentials = new GcsCredentials.MetadataServer(google.root(), HTTP, clock);
            credentials.authorization();
            clock.now = clock.now.plusSeconds(119);
            credentials.authorization();
            assertEquals(1, google.requests.size());
            credentials.invalidate();
            credentials.authorization();
            assertEquals(2, google.requests.size());
        }
    }

    /**
     * gcloud's user credentials exchange the refresh token, with the client id and secret.
     */
    @Test
    void aGcloudUserExchangesItsRefreshToken() throws Exception {
        try (LocalHttp google = new LocalHttp(request -> {
            Map<String, String> form = form(request.body());
            boolean valid = "refresh_token".equals(form.get("grant_type")) && "refresh-1".equals(form.get("refresh_token"))
                    && "client-1".equals(form.get("client_id")) && "secret-1".equals(form.get("client_secret"));
            return valid ? LocalHttp.Response.json(200, "{\"access_token\":\"user-token\",\"expires_in\":3600}")
                    : LocalHttp.Response.json(400, "{\"error\":\"invalid_grant\"}");
        })) {
            Map<String, String> user = new HashMap<>();
            user.put("type", "authorized_user");
            user.put("client_id", "client-1");
            user.put("client_secret", "secret-1");
            user.put("refresh_token", "refresh-1");
            user.put("token_uri", google.root() + "/token");
            GcsCredentials credentials = GcsCredentials.fromFile(write("adc.json", user), HTTP);
            assertEquals(Optional.of("Bearer user-token"), credentials.authorization());
            assertFalse(credentials.toString().contains("secret"), credentials.toString());
        }
    }

    /**
     * The metadata server is asked with Metadata-Flavor: Google; a token error says what failed without secrets.
     */
    @Test
    void theMetadataServerIsAskedWithItsHeader() throws Exception {
        try (LocalHttp metadata = new LocalHttp(request -> "Google".equals(request.header("Metadata-Flavor"))
                && request.path().equals("/computeMetadata/v1/instance/service-accounts/default/token")
                ? LocalHttp.Response.json(200, "{\"access_token\":\"vm-token\",\"expires_in\":3600}")
                : LocalHttp.Response.json(403, "{\"error\":\"missing header\"}"))) {
            assertEquals(Optional.of("Bearer vm-token"), new GcsCredentials.MetadataServer(metadata.root(), HTTP, Clock.systemUTC()).authorization());
        }
        try (LocalHttp refusing = new LocalHttp(request -> LocalHttp.Response.json(400, "{\"error\":\"invalid_grant\"}"))) {
            IOException failed = assertThrows(IOException.class,
                    () -> new GcsCredentials.MetadataServer(refusing.root(), HTTP, Clock.systemUTC()).authorization());
            assertTrue(failed.getMessage().contains("HTTP 400") && failed.getMessage().contains("invalid_grant"), failed.getMessage());
        }
    }

    /**
     * Application Default Credentials: GOOGLE_APPLICATION_CREDENTIALS first, then gcloud's file, then a metadata
     * server that answers, else a clear error.
     */
    @Test
    void applicationDefaultCredentialsAreFoundInGooglesOrder() throws Exception {
        Map<String, String> user = new HashMap<>();
        user.put("type", "authorized_user");
        user.put("client_id", "c");
        user.put("client_secret", "s");
        user.put("refresh_token", "r");
        Path named = write("named.json", user);
        Path gcloud = Files.createDirectories(folder.resolve("gcloud"));
        Files.write(gcloud.resolve("application_default_credentials.json"), JSON.writeValueAsBytes(user));
        Path home = Files.createDirectories(folder.resolve("home"));

        assertInstanceOf(GcsCredentials.AuthorizedUser.class,
                GcsCredentials.applicationDefault(Map.of("GOOGLE_APPLICATION_CREDENTIALS", named.toString()), home, HTTP));
        assertInstanceOf(GcsCredentials.AuthorizedUser.class,
                GcsCredentials.applicationDefault(Map.of("CLOUDSDK_CONFIG", gcloud.toString()), home, HTTP));

        try (LocalHttp metadata = new LocalHttp(request -> new LocalHttp.Response(200, "", Map.of("Metadata-Flavor", "Google")))) {
            String host = metadata.root().substring("http://".length());
            assertInstanceOf(GcsCredentials.MetadataServer.class,
                    GcsCredentials.applicationDefault(Map.of("GCE_METADATA_HOST", host), home, HTTP));
        }
        try (LocalHttp other = new LocalHttp(request -> new LocalHttp.Response(200, "", Map.of()))) {
            String host = other.root().substring("http://".length());
            IOException none = assertThrows(IOException.class,
                    () -> GcsCredentials.applicationDefault(Map.of("GCE_METADATA_HOST", host), home, HTTP));
            assertTrue(none.getMessage().contains("GOOGLE_APPLICATION_CREDENTIALS"), none.getMessage());
        }
    }

    /**
     * An external_account file (workload identity federation) is refused with a message that says what is supported.
     */
    @Test
    void anUnsupportedCredentialsFileIsRefused() throws Exception {
        Path external = write("external.json", Map.of("type", "external_account"));
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> GcsCredentials.fromFile(external, HTTP));
        assertTrue(refused.getMessage().contains("external_account") && refused.getMessage().contains("service_account"), refused.getMessage());
        Path broken = write("broken.json", Map.of("type", "service_account", "client_email", "a@b", "private_key", "not a key"));
        assertThrows(IllegalArgumentException.class, () -> GcsCredentials.fromFile(broken, HTTP));
    }

    private static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-18T12:00:00Z");

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
