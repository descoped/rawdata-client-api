package io.descoped.rawdata.provider.gcs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * What authorizes the requests to GCS: an OAuth 2.0 access token from Google, or nothing for an emulator.
 * <p>
 * The sources are Google's own: a service account key (a JWT signed with the key and exchanged for a token), a
 * gcloud user (a refresh token), and the metadata server of a Google Cloud runtime (GCE, GKE, Cloud Run, Cloud
 * Functions). {@link #applicationDefault} finds them in the order of Google's Application Default Credentials.
 * Tokens are kept until five minutes before they expire. A credential's {@code toString} never shows a secret.
 */
abstract class GcsCredentials {

    /**
     * The scope of the tokens: reading and writing objects.
     */
    static final String SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";
    static final String GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token";
    static final String METADATA_HOST = "metadata.google.internal";

    private static final String REFRESH_TOKEN = "refresh_token";
    private static final String GCLOUD_FILE = "application_default_credentials.json";
    private static final String METADATA_FLAVOR = "Metadata-Flavor";
    private static final String GOOGLE = "Google";

    private static final Duration REFRESH_MARGIN = Duration.ofMinutes(5);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * No credentials: requests go without authorization, as an emulator accepts.
     */
    static final GcsCredentials NONE = new GcsCredentials() {
        @Override
        Optional<String> authorization() {
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "none";
        }
    };

    /**
     * The value of the {@code Authorization} header, fetching or refreshing the token if needed.
     *
     * @return {@code Bearer <token>}, or empty for no authorization
     * @throws IOException if no token can be had
     */
    abstract Optional<String> authorization() throws IOException;

    /**
     * Forgets a kept token, so that the next {@link #authorization()} fetches a new one: after GCS refused the
     * token (401), for example because it was revoked or the clock is off.
     */
    void invalidate() {
        // nothing is kept by default
    }

    /**
     * Google refused to give a token (a 4xx from the token endpoint, such as {@code invalid_grant}): retrying does
     * not help.
     */
    static final class RefusedException extends IOException {

        private static final long serialVersionUID = 1L;

        RefusedException(String message) {
            super(message);
        }
    }

    /**
     * Reads a credentials file: a service account key or a gcloud user's application default credentials.
     *
     * @param file the JSON file
     * @param http the client for the token endpoint
     * @return the credentials
     * @throws IOException              if the file cannot be read
     * @throws IllegalArgumentException if it is not such a file, or of a type this provider does not support
     */
    static GcsCredentials fromFile(Path file, HttpClient http) throws IOException {
        JsonNode json = JSON.readTree(Files.readAllBytes(file));
        String type = json.path("type").asText("");
        switch (type) {
            case "service_account":
                return new ServiceAccount(text(json, "client_email", file), privateKey(text(json, "private_key", file), file),
                        json.path("private_key_id").asText(null), json.path("token_uri").asText(GOOGLE_TOKEN_URI), http, Clock.systemUTC());
            case "authorized_user":
                return new AuthorizedUser(text(json, "client_id", file), text(json, "client_secret", file),
                        text(json, REFRESH_TOKEN, file), json.path("token_uri").asText(GOOGLE_TOKEN_URI), http, Clock.systemUTC());
            default:
                throw new IllegalArgumentException("Credentials file " + file + " has type '" + type
                        + "'; the gcs provider supports service_account and authorized_user files, and the metadata server");
        }
    }

    /**
     * Finds the Application Default Credentials, in Google's order: the file in {@code GOOGLE_APPLICATION_CREDENTIALS},
     * gcloud's {@code application_default_credentials.json} (in {@code CLOUDSDK_CONFIG}, or {@code ~/.config/gcloud},
     * or {@code %APPDATA%\gcloud} on Windows), and the metadata server ({@code GCE_METADATA_HOST} or
     * {@code metadata.google.internal}) if it answers.
     *
     * @param environment the environment variables
     * @param home        the user's home folder
     * @param http        the client for the token endpoints
     * @return the credentials
     * @throws IOException if none are found, or a file cannot be read
     */
    static GcsCredentials applicationDefault(Map<String, String> environment, Path home, HttpClient http) throws IOException {
        String named = environment.get("GOOGLE_APPLICATION_CREDENTIALS");
        if (named != null && !named.isBlank()) {
            return fromFile(Path.of(named), http);
        }
        Path gcloud = gcloudFile(environment, home);
        if (Files.isRegularFile(gcloud)) {
            return fromFile(gcloud, http);
        }
        String host = environment.getOrDefault("GCE_METADATA_HOST", METADATA_HOST);
        MetadataServer metadata = new MetadataServer("http://" + host, http, Clock.systemUTC());
        if (metadata.answers()) {
            return metadata;
        }
        throw new IOException("No Application Default Credentials: set GOOGLE_APPLICATION_CREDENTIALS to a key file, run "
                + "'gcloud auth application-default login', or run on Google Cloud (no metadata server at " + host + ")");
    }

    private static Path gcloudFile(Map<String, String> environment, Path home) {
        String config = environment.get("CLOUDSDK_CONFIG");
        if (config != null && !config.isBlank()) {
            return Path.of(config, GCLOUD_FILE);
        }
        String appData = environment.get("APPDATA");
        if (appData != null && System.getProperty("os.name", "").startsWith("Windows")) {
            return Path.of(appData, "gcloud", GCLOUD_FILE);
        }
        return home.resolve(".config").resolve("gcloud").resolve(GCLOUD_FILE);
    }

    private static String text(JsonNode json, String field, Path file) {
        String value = json.path(field).asText("");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Credentials file " + file + " has no " + field);
        }
        return value;
    }

    static PrivateKey privateKey(String pem, Path file) {
        String base64 = pem.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replaceAll("\\s", "");
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Credentials file " + file + " has no RSA private key in PKCS#8 PEM", e);
        }
    }

    /**
     * Credentials that fetch an access token and keep it until shortly before it expires.
     */
    abstract static class Token extends GcsCredentials {

        private final HttpClient http;
        private final Clock clock;
        private String accessToken; // guarded by this
        private Instant refreshAt = Instant.MIN; // guarded by this

        Token(HttpClient http, Clock clock) {
            this.http = http;
            this.clock = clock;
        }

        /**
         * Builds the request for a new token, whose response is Google's JSON with {@code access_token} and
         * {@code expires_in}.
         */
        abstract HttpRequest tokenRequest() throws IOException;

        @Override
        final synchronized Optional<String> authorization() throws IOException {
            if (accessToken == null || !clock.instant().isBefore(refreshAt)) {
                Instant asked = clock.instant();
                JsonNode response = call(tokenRequest());
                String fetched = response.path("access_token").asText("");
                if (fetched.isEmpty()) {
                    throw new IOException("The token response of " + this + " has no access_token");
                }
                accessToken = fetched;
                refreshAt = asked.plus(lifetime(Duration.ofSeconds(Math.max(0, response.path("expires_in").asLong(0)))));
            }
            return Optional.of("Bearer " + accessToken);
        }

        /**
         * How long a token with this lifetime is used: until five minutes before it expires, or until half its
         * lifetime is over if that comes first, so a token with little time left is not fetched on every call.
         */
        static Duration lifetime(Duration expiresIn) {
            Duration half = expiresIn.dividedBy(2);
            Duration beforeExpiry = expiresIn.minus(REFRESH_MARGIN);
            return beforeExpiry.compareTo(half) > 0 ? beforeExpiry : half;
        }

        @Override
        final synchronized void invalidate() {
            accessToken = null;
        }

        final Clock clock() {
            return clock;
        }

        final HttpClient http() {
            return http;
        }

        JsonNode call(HttpRequest request) throws IOException {
            try {
                HttpResponse<byte[]> response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
                int status = response.statusCode();
                if (status != 200) {
                    // the body of an OAuth error names the problem (invalid_grant, …) and holds no secret
                    String message = "Getting an access token for " + this + " failed: HTTP " + status + " "
                            + new String(response.body(), StandardCharsets.UTF_8);
                    throw GcsClient.retryable(status) ? new IOException(message) : new RefusedException(message);
                }
                return JSON.readTree(response.body());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                InterruptedIOException interrupted = new InterruptedIOException("Interrupted while getting an access token");
                interrupted.initCause(e);
                throw interrupted;
            }
        }

        static HttpRequest form(String uri, Map<String, String> fields) {
            String body = fields.entrySet().stream()
                    .map(field -> URLEncoder.encode(field.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(field.getValue(), StandardCharsets.UTF_8))
                    .collect(Collectors.joining("&"));
            return HttpRequest.newBuilder(URI.create(uri)).timeout(TIMEOUT)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        }
    }

    /**
     * A service account key: a JWT signed with the key (RS256) is exchanged for an access token at the token URI.
     */
    static final class ServiceAccount extends Token {

        private final String email;
        private final PrivateKey key;
        private final String keyId;
        private final String tokenUri;

        ServiceAccount(String email, PrivateKey key, String keyId, String tokenUri, HttpClient http, Clock clock) {
            super(http, clock);
            this.email = email;
            this.key = key;
            this.keyId = keyId;
            this.tokenUri = tokenUri;
        }

        @Override
        HttpRequest tokenRequest() throws IOException {
            long now = clock().instant().getEpochSecond();
            Map<String, Object> header = new LinkedHashMap<>();
            header.put("alg", "RS256");
            header.put("typ", "JWT");
            if (keyId != null) {
                header.put("kid", keyId);
            }
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("iss", email);
            claims.put("scope", SCOPE);
            claims.put("aud", tokenUri);
            claims.put("iat", now);
            claims.put("exp", now + 3600);
            String unsigned = base64Url(JSON.writeValueAsBytes(header)) + "." + base64Url(JSON.writeValueAsBytes(claims));
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
            fields.put("assertion", unsigned + "." + base64Url(sign(unsigned)));
            return form(tokenUri, fields);
        }

        private byte[] sign(String content) throws IOException {
            try {
                Signature signature = Signature.getInstance("SHA256withRSA");
                signature.initSign(key);
                signature.update(content.getBytes(StandardCharsets.US_ASCII));
                return signature.sign();
            } catch (GeneralSecurityException e) {
                throw new IOException("Cannot sign the token request of " + this, e);
            }
        }

        private static String base64Url(byte[] bytes) {
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        }

        @Override
        public String toString() {
            return "service account " + email;
        }
    }

    /**
     * A gcloud user's application default credentials: a refresh token is exchanged for an access token.
     */
    static final class AuthorizedUser extends Token {

        private final String clientId;
        private final String clientSecret;
        private final String refreshToken;
        private final String tokenUri;

        AuthorizedUser(String clientId, String clientSecret, String refreshToken, String tokenUri, HttpClient http, Clock clock) {
            super(http, clock);
            this.clientId = clientId;
            this.clientSecret = clientSecret;
            this.refreshToken = refreshToken;
            this.tokenUri = tokenUri;
        }

        @Override
        HttpRequest tokenRequest() {
            Map<String, String> fields = new LinkedHashMap<>();
            fields.put("grant_type", REFRESH_TOKEN);
            fields.put("client_id", clientId);
            fields.put("client_secret", clientSecret);
            fields.put(REFRESH_TOKEN, refreshToken);
            return form(tokenUri, fields);
        }

        @Override
        public String toString() {
            return "gcloud user credentials";
        }
    }

    /**
     * The metadata server of a Google Cloud runtime, which gives tokens for the runtime's service account.
     */
    static final class MetadataServer extends Token {

        private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(2);

        private final String root;

        MetadataServer(String root, HttpClient http, Clock clock) {
            super(http, clock);
            this.root = root;
        }

        @Override
        HttpRequest tokenRequest() {
            return HttpRequest.newBuilder(URI.create(root + "/computeMetadata/v1/instance/service-accounts/default/token"))
                    .timeout(TIMEOUT).header(METADATA_FLAVOR, GOOGLE).GET().build();
        }

        /**
         * Whether a metadata server answers here.
         */
        boolean answers() {
            HttpRequest probe = HttpRequest.newBuilder(URI.create(root + "/computeMetadata/v1/")).timeout(PROBE_TIMEOUT)
                    .header(METADATA_FLAVOR, GOOGLE).GET().build();
            try {
                HttpResponse<Void> response = http().send(probe, HttpResponse.BodyHandlers.discarding());
                return GOOGLE.equals(response.headers().firstValue(METADATA_FLAVOR).orElse(null));
            } catch (IOException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        @Override
        public String toString() {
            return "metadata server " + root;
        }
    }
}
