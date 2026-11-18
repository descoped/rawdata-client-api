package io.descoped.rawdata.copy;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCopy;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataTopics;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Copies topics from one rawdata store to another, keeping the ids and the order: a backup, or a move from one
 * provider or cloud to another. A copy continues where an earlier one stopped: when the target topic has messages, it
 * starts after the target's last id.
 * <pre>
 * rawdata-copy --config copy.properties [options] topic[:target-topic]...
 * </pre>
 * The configuration file holds two provider configurations, one with keys that start with {@code source.} and one
 * with {@code target.}, each with {@code rawdata.client.provider}. Run with {@code --help} for the options.
 * <p>
 * A command-line tool: it reports on standard output and standard error, not through a logger.
 */
public final class RawdataCopyTool {

    /**
     * Exit status: every topic was copied.
     */
    public static final int OK = 0;

    /**
     * Exit status: a copy failed; the topics before it were copied, and a new run continues where it stopped.
     */
    public static final int FAILED = 1;

    /**
     * Exit status: the arguments or the configuration are wrong, or a store cannot be opened (a provider that refuses
     * its configuration, a database that does not answer); nothing was copied.
     */
    public static final int USAGE = 2;

    static final String USAGE_TEXT = """
            Usage: rawdata-copy --config <file> [options] <topic>[:<target-topic>]...

            Copies topics from the source store to the target store, keeping ids and order. When a target topic has
            messages, the copy starts after its last id, so a copy that stopped can be run again.

              --config <file>          properties with source.* and target.* provider configurations (required)
              --metadata               also copy each topic's metadata entries, overwriting target entries with the
                                       same key; the target's other entries stay
              --from-id <uuid>         start at this id (inclusive) instead of the beginning
              --from-time <time>       start at this time: an ISO-8601 instant or epoch milliseconds
              --batch-size <n>         messages per publish to the target (default 1000)
              --idle-timeout-ms <ms>   stop when the source has had nothing new for this long (default 1000)
              --help                   show this text

            Exit status: 0 copied; 1 a copy failed (run again to continue); 2 wrong arguments or configuration, or a
            store that cannot be opened (nothing copied).
            """;

    private static final String PREFIX = "rawdata-copy: ";

    private RawdataCopyTool() {
    }

    /**
     * Runs the tool and exits with its status.
     *
     * @param args the arguments
     */
    @SuppressWarnings("java:S106") // a command-line tool writes to the console
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Runs the tool. The first run sends java.util.logging to slf4j.
     *
     * @param args the arguments
     * @param out  where the progress goes
     * @param err  where errors go
     * @return {@link #OK}, {@link #FAILED} or {@link #USAGE}
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        routeJavaUtilLoggingToSlf4j();
        Options options;
        Map<String, String> configuration;
        try {
            options = Options.parse(args);
            if (options.help) {
                out.print(USAGE_TEXT);
                return OK;
            }
            configuration = load(options.config);
        } catch (IllegalArgumentException e) {
            return usage(err, e.getMessage());
        }
        RawdataClient from = null;
        RawdataClient to = null;
        try {
            from = client(configuration, "source.");
            to = client(configuration, "target.");
            return copyAll(from, to, options, out, err);
        } catch (IllegalArgumentException | UncheckedConfigurationException e) {
            return usage(err, e.getMessage());
        } finally {
            close(to);
            close(from);
        }
    }

    /**
     * Sends java.util.logging, which the PostgreSQL driver uses, to slf4j, so that all library logging comes out
     * one way (System.Logger goes there through slf4j-jdk-platform-logging). Once per JVM.
     */
    static synchronized void routeJavaUtilLoggingToSlf4j() {
        if (!SLF4JBridgeHandler.isInstalled()) {
            SLF4JBridgeHandler.removeHandlersForRootLogger();
            SLF4JBridgeHandler.install();
        }
    }

    private static int copyAll(RawdataClient from, RawdataClient to, Options options, PrintStream out, PrintStream err) {
        for (Options.Topic topic : options.topics) {
            try {
                out.println(copy(from, to, topic, options));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                err.println(PREFIX + topic + ": interrupted; run again to continue");
                return FAILED;
            } catch (RuntimeException e) {
                err.println(PREFIX + topic + ": copy failed; run again to continue: " + e);
                return FAILED;
            }
        }
        return OK;
    }

    private static String copy(RawdataClient from, RawdataClient to, Options.Topic topic, Options options) throws InterruptedException {
        UUID start = options.fromId;
        if (start == null && options.fromTime != null) {
            start = RawdataIds.beginningOf(options.fromTime);
        }
        RawdataMessage last = to.lastMessage(topic.target());
        boolean resume = last != null && (start == null || RawdataIds.ORDER.compare(last.id(), start) >= 0);
        long messages;
        try (RawdataConsumer consumer = consumer(from, topic.source(), resume ? last.id() : start, !resume);
             RawdataProducer producer = to.producer(topic.target())) {
            messages = RawdataCopy.copy(consumer, producer, options.batchSize, options.idleTimeout);
        }
        StringBuilder report = new StringBuilder().append(topic).append(": ").append(plural(messages, "message", "messages"));
        if (resume) {
            report.append(" after ").append(last.id());
        } else if (start != null) {
            report.append(" from ").append(start);
        }
        if (options.metadata) {
            report.append(", ").append(plural(copyMetadata(from.metadata(topic.source()), to.metadata(topic.target())),
                    "metadata entry", "metadata entries"));
        }
        return report.toString();
    }

    private static RawdataConsumer consumer(RawdataClient from, String topic, UUID start, boolean inclusive) {
        return start == null ? from.consumer(topic) : from.consumer(topic, start, inclusive);
    }

    private static long copyMetadata(RawdataMetadataClient source, RawdataMetadataClient target) {
        long entries = 0;
        for (String key : source.keys()) {
            byte[] value = source.get(key);
            if (value != null) { // removed since the listing
                target.put(key, value);
                entries++;
            }
        }
        return entries;
    }

    private static String plural(long count, String one, String many) {
        return count + " " + (count == 1 ? one : many);
    }

    private static int usage(PrintStream err, String message) {
        err.println(PREFIX + message);
        err.println();
        err.print(USAGE_TEXT);
        return USAGE;
    }

    /**
     * Creates the client of one part of the configuration. A store that cannot be opened, because the provider
     * refuses its configuration or the store does not answer, is a usage error, whatever exception it throws.
     */
    private static RawdataClient client(Map<String, String> configuration, String prefix) {
        Map<String, String> part = part(configuration, prefix);
        try {
            return RawdataClient.create(part);
        } catch (RuntimeException e) {
            throw new UncheckedConfigurationException("Cannot open the " + prefix.substring(0, prefix.length() - 1)
                    + " store: " + e.getMessage(), e);
        }
    }

    private static void close(RawdataClient client) {
        if (client != null) {
            client.close();
        }
    }

    private static Map<String, String> load(Path file) {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read the configuration " + file + ": " + e.getMessage(), e);
        }
        Map<String, String> configuration = new HashMap<>();
        for (String key : properties.stringPropertyNames()) {
            configuration.put(key, properties.getProperty(key));
        }
        return configuration;
    }

    /**
     * The keys with a prefix, without it.
     */
    static Map<String, String> part(Map<String, String> configuration, String prefix) {
        Map<String, String> part = new HashMap<>();
        configuration.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                part.put(key.substring(prefix.length()), value);
            }
        });
        String provider = part.get(RawdataClient.PROVIDER_KEY);
        if (provider == null || provider.isBlank()) {
            throw new IllegalArgumentException("The configuration has no " + prefix + RawdataClient.PROVIDER_KEY);
        }
        return part;
    }

    /**
     * A store cannot be opened.
     */
    static final class UncheckedConfigurationException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        UncheckedConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The parsed arguments.
     */
    static final class Options {

        record Topic(String source, String target) {
            @Override
            public String toString() {
                return source + " -> " + target;
            }
        }

        Path config;
        boolean help;
        boolean metadata;
        UUID fromId;
        Long fromTime;
        int batchSize = 1000;
        Duration idleTimeout = Duration.ofSeconds(1);
        final List<Topic> topics = new ArrayList<>();

        static Options parse(String[] args) {
            Options options = new Options();
            Deque<String> remaining = new ArrayDeque<>(Arrays.asList(args));
            while (!remaining.isEmpty()) {
                options.take(remaining.removeFirst(), remaining);
            }
            if (!options.help) {
                options.check();
            }
            return options;
        }

        private void take(String arg, Deque<String> remaining) {
            switch (arg) {
                case "--help", "-h" -> help = true;
                case "--metadata" -> metadata = true;
                case "--config" -> config = Path.of(value(remaining, arg));
                case "--from-id" -> fromId = id(value(remaining, arg));
                case "--from-time" -> fromTime = time(value(remaining, arg));
                case "--batch-size" -> batchSize = (int) positive(value(remaining, arg), arg);
                case "--idle-timeout-ms" -> idleTimeout = Duration.ofMillis(positive(value(remaining, arg), arg));
                default -> {
                    if (arg.startsWith("-")) {
                        throw new IllegalArgumentException("Unknown option " + arg);
                    }
                    topics.add(topic(arg));
                }
            }
        }

        private void check() {
            if (config == null) {
                throw new IllegalArgumentException("--config is required");
            }
            if (topics.isEmpty()) {
                throw new IllegalArgumentException("Name at least one topic");
            }
            if (fromId != null && fromTime != null) {
                throw new IllegalArgumentException("Give --from-id or --from-time, not both");
            }
        }

        private static String value(Deque<String> remaining, String option) {
            String value = remaining.pollFirst();
            if (value == null || value.startsWith("--")) {
                throw new IllegalArgumentException(option + " needs a value");
            }
            return value;
        }

        private static Topic topic(String arg) {
            int colon = arg.indexOf(':');
            String source = colon < 0 ? arg : arg.substring(0, colon);
            String target = colon < 0 ? arg : arg.substring(colon + 1);
            if (!RawdataTopics.isValid(source) || !RawdataTopics.isValid(target)) {
                throw new IllegalArgumentException("Not a topic name, or source:target: " + arg);
            }
            return new Topic(source, target);
        }

        private static UUID id(String value) {
            try {
                return RawdataIds.requireValid(UUID.fromString(value));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("--from-id is not a UUIDv7: " + value, e);
            }
        }

        private static long time(String value) {
            try {
                return value.chars().allMatch(Character::isDigit) ? Long.parseLong(value) : Instant.parse(value).toEpochMilli();
            } catch (NumberFormatException | DateTimeParseException e) {
                throw new IllegalArgumentException("--from-time is neither an ISO-8601 instant nor epoch milliseconds: " + value, e);
            }
        }

        private static long positive(String value, String option) {
            try {
                long number = Long.parseLong(value);
                if (number > 0 && number <= Integer.MAX_VALUE) {
                    return number;
                }
            } catch (NumberFormatException e) {
                // reported below
            }
            throw new IllegalArgumentException(option + " must be a positive number: " + value);
        }
    }
}
