package io.descoped.rawdata.tck.avro;

import io.descoped.rawdata.api.RawdataClient;
import io.descoped.rawdata.api.RawdataConsumer;
import io.descoped.rawdata.api.RawdataException;
import io.descoped.rawdata.api.RawdataMessage;
import io.descoped.rawdata.api.RawdataProducer;
import io.descoped.rawdata.api.RawdataWriterConflictException;
import io.descoped.rawdata.provider.avro.AvroRawdataClient;
import io.descoped.rawdata.provider.avro.AvroTopicFolders;
import io.descoped.rawdata.provider.avro.AvroUploadWindow;
import io.descoped.rawdata.tck.RawdataClientContractTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The provider contract for providers on the Avro engine, plus the contract of the upload window. Producers buffer on
 * a local disk and upload one Avro file per window; a message is visible once its window is uploaded, so
 * {@link #awaitPublished} closes the topic's window and waits for the upload ({@link AvroRawdataClient#flush}).
 * <p>
 * {@link #configuration()} must give each call a {@code local-temp-folder} of its own, as each process has its own
 * disk.
 */
public abstract class AvroClientContractTest extends RawdataClientContractTest {

    /**
     * Creates the test; subclasses are instantiated by JUnit.
     */
    protected AvroClientContractTest() {
    }

    @Override
    protected void awaitPublished(RawdataClient client, String topic) {
        ((AvroRawdataClient) client).flush(topic);
    }

    private Map<String, String> with(String... keysAndValues) {
        Map<String, String> configuration = new HashMap<>(configuration());
        for (int i = 0; i < keysAndValues.length; i += 2) {
            configuration.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return configuration;
    }

    private static RawdataMessage message(String position) {
        return RawdataMessage.builder().position(position).put("payload", position.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static List<String> readAll(RawdataConsumer consumer) throws InterruptedException {
        List<String> positions = new ArrayList<>();
        RawdataMessage message;
        while ((message = consumer.receive(1, TimeUnit.SECONDS)) != null) {
            positions.add(message.position());
        }
        return positions;
    }

    /**
     * A message is not visible before its window is uploaded, and is once it is.
     */
    @Test
    public void aMessageIsVisibleOnceItsWindowIsUploaded() {
        try (RawdataClient writer = RawdataClient.create(with(AvroUploadWindow.MAX_SECONDS, "3600"))) {
            writer.producer(topic).publish(message("a"));
            assertNull(client.lastMessage(topic), "buffered, not yet uploaded");
            awaitPublished(writer, topic);
            assertEquals("a", client.lastMessage(topic).position());
        }
    }

    /**
     * A window closes when its time is up, also when nothing more is published.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void aWindowClosesOnTimeWithoutNewPublishes() throws InterruptedException {
        try (RawdataClient writer = RawdataClient.create(with(AvroUploadWindow.MAX_SECONDS, "1"));
             RawdataConsumer consumer = client.consumer(topic)) {
            writer.producer(topic).publish(message("a"));
            RawdataMessage received = consumer.receive(30, TimeUnit.SECONDS);
            assertNotNull(received, "the timer uploads the window");
            assertEquals("a", received.position());
        }
    }

    /**
     * A window closes when it is larger than its size limit.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void aWindowClosesOnSize() throws InterruptedException {
        try (RawdataClient writer = RawdataClient.create(with(AvroUploadWindow.MAX_SECONDS, "3600", AvroUploadWindow.MAX_BYTES, "1"));
             RawdataConsumer consumer = client.consumer(topic)) {
            RawdataProducer producer = writer.producer(topic);
            producer.publish(message("a"));
            producer.publish(message("b"));
            assertEquals("a", consumer.receive(30, TimeUnit.SECONDS).position());
            assertEquals("b", consumer.receive(30, TimeUnit.SECONDS).position());
        }
    }

    /**
     * Closing a producer uploads its topic's open window and waits for the upload, as in 2.x.
     */
    @Test
    public void closingTheProducerUploadsTheOpenWindow() {
        try (RawdataClient writer = RawdataClient.create(with(AvroUploadWindow.MAX_SECONDS, "3600"))) {
            try (RawdataProducer producer = writer.producer(topic)) {
                producer.publish(message("a"), message("b"));
            }
            assertEquals("b", client.lastMessage(topic).position());
        }
    }

    /**
     * Many publishes in one window are one segment, read back in order.
     *
     * @throws InterruptedException if interrupted while waiting
     */
    @Test
    public void manyPublishesInOneWindowAreReadInOrder() throws InterruptedException {
        List<String> published = new ArrayList<>();
        try (RawdataClient writer = RawdataClient.create(with(AvroUploadWindow.MAX_SECONDS, "3600"));
             RawdataProducer producer = writer.producer(topic)) {
            for (int page = 0; page < 20; page++) {
                List<RawdataMessage> messages = new ArrayList<>();
                for (int i = 0; i < 25; i++) {
                    String position = "p" + page + "-" + i;
                    messages.add(message(position));
                    published.add(position);
                }
                producer.publish(messages);
            }
        }
        try (RawdataConsumer consumer = client.consumer(topic)) {
            assertEquals(published, readAll(consumer));
        }
    }

    /**
     * After a crash, the next writer of the topic uploads the window the crashed one left, cut back to its complete
     * blocks, which hold whole publish calls, and goes on after it. The crashed writer, should it still run, can no longer write the topic.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    public void aWindowACrashLeftIsUploadedWhenTheWriterStartsAgain() throws Exception {
        Map<String, String> crashed = with(AvroUploadWindow.MAX_SECONDS, "3600", AvroUploadWindow.SYNC_INTERVAL, "64");
        Map<String, String> restarted = with(AvroUploadWindow.MAX_SECONDS, "3600");
        List<String> published = new ArrayList<>();
        RawdataClient lost = RawdataClient.create(crashed);
        try {
            RawdataProducer producer = lost.producer(topic);
            for (int call = 0; call < 20; call++) {
                List<RawdataMessage> messages = new ArrayList<>();
                for (int i = 0; i < 3; i++) {
                    String position = "c" + call + "-" + i;
                    messages.add(message(position));
                    published.add(position);
                }
                producer.publish(messages); // a call is written whole or not at all, also through a crash
            }
            // The crash: the window file as the disk holds it, with a torn block at its end, in the restarted folder.
            Path window = onlyWindow(crashed);
            Path copy = buffer(restarted).resolve(window.getFileName());
            Files.createDirectories(copy.getParent());
            Files.copy(window, copy);
            Files.write(copy, new byte[]{1, 2, 3, 4, 5, 6, 7}, StandardOpenOption.APPEND);

            try (RawdataClient writer = RawdataClient.create(restarted)) {
                writer.producer(topic).publish(message("after"));
                awaitPublished(writer, topic);
            }
            try (RawdataConsumer consumer = client.consumer(topic)) {
                List<String> read = readAll(consumer);
                assertEquals("after", read.get(read.size() - 1));
                List<String> recovered = read.subList(0, read.size() - 1);
                assertFalse(recovered.isEmpty(), "the complete blocks are uploaded");
                assertEquals(published.subList(0, recovered.size()), recovered, "a prefix, in order, without gaps");
                assertEquals(0, recovered.size() % 3, "whole calls only: " + recovered.size());
            }
            assertThrows(RawdataWriterConflictException.class, () -> awaitPublished(lost, topic),
                    "the crashed writer's segment is taken");
        } finally {
            lost.close();
        }
    }

    /**
     * A window a crash left that no longer follows the topic's end (another writer went on) is set aside, never
     * uploaded: the writer fails fast once, and the next publish starts from the topic's end.
     *
     * @throws Exception if the test fails unexpectedly
     */
    @Test
    public void aLeftWindowThatNoLongerFitsIsSetAside() throws Exception {
        Map<String, String> crashed = with(AvroUploadWindow.MAX_SECONDS, "3600", AvroUploadWindow.SYNC_INTERVAL, "64");
        Map<String, String> restarted = with(AvroUploadWindow.MAX_SECONDS, "3600");
        try (RawdataClient lost = RawdataClient.create(crashed)) {
            RawdataProducer producer = lost.producer(topic);
            for (int i = 0; i < 20; i++) {
                producer.publish(message("m" + i));
            }
            Path window = onlyWindow(crashed);
            Path copy = buffer(restarted).resolve(window.getFileName());
            Files.createDirectories(copy.getParent());
            Files.copy(window, copy);
            awaitPublished(lost, topic); // the topic goes on without the restarted writer: the leftover no longer fits

            try (RawdataClient writer = RawdataClient.create(restarted)) {
                RawdataProducer next = writer.producer(topic);
                RawdataMessage after = message("after");
                assertThrows(RawdataWriterConflictException.class, () -> next.publish(after));
                assertTrue(Files.exists(copy.resolveSibling(copy.getFileName() + ".invalid")), "kept, set aside");
                next.publish(after);
                awaitPublished(writer, topic);
            }
            assertEquals("after", client.lastMessage(topic).position());
        }
    }

    /**
     * Only one client at a time may use a topic's buffer folder.
     */
    @Test
    public void aBufferFolderIsUsedByOneClient() {
        Map<String, String> shared = with(AvroUploadWindow.MAX_SECONDS, "3600");
        try (RawdataClient first = RawdataClient.create(shared); RawdataClient second = RawdataClient.create(shared)) {
            first.producer(topic).publish(message("a"));
            RawdataProducer producer = second.producer(topic);
            RawdataMessage b = message("b");
            RawdataException refused = assertThrows(RawdataException.class, () -> producer.publish(b));
            assertTrue(refused.getMessage().contains("in use"), refused.getMessage());
        }
    }

    private Path buffer(Map<String, String> configuration) {
        return Path.of(configuration.get(AvroUploadWindow.LOCAL_TEMP_FOLDER)).resolve(AvroTopicFolders.folderName(topic));
    }

    private Path onlyWindow(Map<String, String> configuration) throws IOException {
        try (Stream<Path> files = Files.list(buffer(configuration))) {
            List<Path> windows = files.filter(file -> file.getFileName().toString().endsWith(".window.avro")).toList();
            assertEquals(1, windows.size(), "one open window: " + windows);
            return windows.get(0);
        }
    }
}
