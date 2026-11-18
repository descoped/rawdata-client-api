package io.descoped.rawdata.provider.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * A local HTTP server for the tests: each request is recorded and answered by a function of it.
 */
final class LocalHttp implements AutoCloseable {

    /**
     * A recorded request.
     */
    record Request(String method, String path, String query, Map<String, List<String>> headers, String body) {
        String header(String name) {
            return headers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> e.getValue().get(0)).findFirst().orElse(null);
        }
    }

    /**
     * An answer.
     */
    record Response(int status, String body, Map<String, String> headers) {
        static Response json(int status, String body) {
            return new Response(status, body, Map.of("Content-Type", "application/json"));
        }
    }

    private final HttpServer server;
    final List<Request> requests = new CopyOnWriteArrayList<>();

    LocalHttp(Function<Request, Response> handler) {
        try {
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", exchange -> answer(exchange, handler));
        server.start();
    }

    private void answer(HttpExchange exchange, Function<Request, Response> handler) throws IOException {
        Request request = new Request(exchange.getRequestMethod(), exchange.getRequestURI().getRawPath(),
                exchange.getRequestURI().getRawQuery(), exchange.getRequestHeaders(),
                new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        requests.add(request);
        Response response = handler.apply(request);
        response.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        byte[] body = response.body().getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(response.status(), body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }

    String root() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
