package io.descoped.rawdata.copy;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataCopy;
import io.descoped.rawdata.api.RawdataIds;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataMetadataClient;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataTopics;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Copies topics from one rawdata store to another, keeping the ids and the order: a backup, or a move from one
 * provider to another. A copy continues where an earlier one stopped: when the target topic has messages, it starts
 * after the target's last id.
 * <pre>
 * rawdata-copy --config copy.properties [options] topic[:target-topic]...
 * </pre>
 * The configuration file holds two provider configurations, one with keys that start with {@code source.} and one
 * with {@code target.}, each with {@code rawdata.client.provider}. Run with {@code --help} for the options.
 */
public final class RawdataCopyTool {

    /**
     * Exit status: every topic was copied.
     */
    public static final int OK = 0;

    /**
     * Exit status: a copy failed; topics before it were copied.
     */
    public static final int FAILED = 1;

    /**
     * Exit status: the arguments or the configuration are wrong; nothing was copied.
     */
    public static final int USAGE = 2;

    static final String USAGE_TEXT = """
            Usage: rawdata-copy --config <file> [options] <topic>[:<target-topic>]...

            Copies topics from the source store to the target store, keeping ids and order. When a target topic has
            messages, the copy starts after its last id, so a copy that stopped can be run again.

              --config <file>          properties with source.* and target.* provider configurations (required)
              --metadata               also copy each topic's metadata entries, replacing the target's
              --from-id <uuid>         start at this id (inclusive) instead of the beginning
              --from-time <time>       start at this time: an ISO-8601 instant or epoch milliseconds
              --batch-size <n>         messages per publish to the target (default 1000)
              --idle-timeout-ms <ms>   stop when the source has had nothing new for this long (default 1000)
              --help                   show this text
            """;

    private RawdataCopyTool() {
    }

    /**
     * Runs the tool and exits with its status.
     *
     * @param args the arguments
     */
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Runs the tool.
     *
     * @param args the arguments
     * @param out  where the progress goes
     * @param err  where errors go
     * @return {@link #OK}, {@link #FAILED} or {@link #USAGE}
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        Options options;
        Map<String, String> source;
        Map<String, String> target;
        try {
            options = Options.parse(args);
            if (options.help) {
                out.print(USAGE_TEXT);
                return OK;
            }
            Map<String, String> configuration = load(options.config);
            source = part(configuration, "source.");
            target = part(configuration, "target.");
        } catch (IllegalArgumentException e) {
            err.println("rawdata-copy: " + e.getMessage());
            err.println();
            err.print(USAGE_TEXT);
            return USAGE;
        }
        try (RawdataClient from = RawdataClient.create(source); RawdataClient to = RawdataClient.create(target)) {
            for (Options.Topic topic : options.topics) {
                copy(from, to, topic, options, out);
            }
            return OK;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("rawdata-copy: interrupted");
            return FAILED;
        } catch (RuntimeException e) {
            err.println("rawdata-copy: copy failed: " + e);
            return FAILED;
        }
    }

    private static void copy(RawdataClient from, RawdataClient to, Options.Topic topic, Options options, PrintStream out) throws InterruptedException {
        UUID start = options.fromId;
        boolean inclusive = true;
        if (start == null && options.fromTime != null) {
            start = RawdataIds.beginningOf(options.fromTime);
        }
        RawdataMessage last = to.lastMessage(topic.target);
        if (last != null && (start == null || RawdataIds.ORDER.compare(last.id(), start) >= 0)) {
            start = last.id(); // continue after what the target has
            inclusive = false;
        }
        long messages;
        try (RawdataConsumer consumer = start == null ? from.consumer(topic.source) : from.consumer(topic.source, start, inclusive);
             RawdataProducer producer = to.producer(topic.target)) {
            messages = RawdataCopy.copy(consumer, producer, options.batchSize, options.idleTimeout);
        }
        StringBuilder report = new StringBuilder(topic.source).append(" -> ").append(topic.target).append(": ")
                .append(messages).append(messages == 1 ? " message" : " messages");
        if (last != null) {
            report.append(" after ").append(last.id());
        }
        if (options.metadata) {
            RawdataMetadataClient sourceMetadata = from.metadata(topic.source);
            RawdataMetadataClient targetMetadata = to.metadata(topic.target);
            int entries = 0;
            for (String key : sourceMetadata.keys()) {
                byte[] value = sourceMetadata.get(key);
                if (value != null) { // removed since the listing
                    targetMetadata.put(key, value);
                    entries++;
                }
            }
            report.append(", ").append(entries).append(entries == 1 ? " metadata entry" : " metadata entries");
        }
        out.println(report);
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
     * The parsed arguments.
     */
    static final class Options {

        record Topic(String source, String target) {
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
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                switch (arg) {
                    case "--help", "-h" -> options.help = true;
                    case "--metadata" -> options.metadata = true;
                    case "--config" -> options.config = Path.of(value(args, ++i, arg));
                    case "--from-id" -> options.fromId = id(value(args, ++i, arg));
                    case "--from-time" -> options.fromTime = time(value(args, ++i, arg));
                    case "--batch-size" -> options.batchSize = (int) positive(value(args, ++i, arg), arg);
                    case "--idle-timeout-ms" -> options.idleTimeout = Duration.ofMillis(positive(value(args, ++i, arg), arg));
                    default -> {
                        if (arg.startsWith("-")) {
                            throw new IllegalArgumentException("Unknown option " + arg);
                        }
                        options.topics.add(topic(arg));
                    }
                }
            }
            if (options.help) {
                return options;
            }
            if (options.config == null) {
                throw new IllegalArgumentException("--config is required");
            }
            if (options.topics.isEmpty()) {
                throw new IllegalArgumentException("Name at least one topic");
            }
            if (options.fromId != null && options.fromTime != null) {
                throw new IllegalArgumentException("Give --from-id or --from-time, not both");
            }
            return options;
        }

        private static String value(String[] args, int i, String option) {
            if (i >= args.length) {
                throw new IllegalArgumentException(option + " needs a value");
            }
            return args[i];
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
