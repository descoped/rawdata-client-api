package io.descoped.rawdata.provider.avro;

import io.descoped.rawdata.api.RawdataMetadataClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * A topic's metadata entries, each stored under {@code k} and the lowercase hexadecimal of its key's UTF-8 bytes: a
 * name that is never empty, and safe and distinct on every file system and object store, case-insensitive ones
 * included.
 */
final class AvroRawdataMetadataClient implements RawdataMetadataClient {

    private static final HexFormat HEX = HexFormat.of();

    private final AvroRawdataClient client;
    private final String topic;

    AvroRawdataMetadataClient(AvroRawdataClient client, String topic) {
        this.client = client;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public Set<String> keys() {
        client.checkOpen();
        try {
            Set<String> keys = new TreeSet<>();
            for (String name : client.store().metadataNames(topic)) {
                if (isName(name)) {
                    keys.add(new String(HEX.parseHex(name, 1, name.length()), StandardCharsets.UTF_8));
                }
            }
            return Collections.unmodifiableSet(keys);
        } catch (IOException e) {
            throw client.failure("Listing the metadata of topic " + topic, e);
        }
    }

    @Override
    public byte[] get(String key) {
        String name = name(key);
        client.checkOpen();
        try {
            return client.store().readMetadata(topic, name);
        } catch (IOException e) {
            throw client.failure("Reading metadata " + key + " of topic " + topic, e);
        }
    }

    @Override
    public RawdataMetadataClient put(String key, byte[] value) {
        String name = name(key);
        Objects.requireNonNull(value, "value");
        client.checkOpen();
        try {
            client.store().writeMetadata(topic, name, value.clone());
        } catch (IOException e) {
            throw client.failure("Writing metadata " + key + " of topic " + topic, e);
        }
        return this;
    }

    @Override
    public RawdataMetadataClient remove(String key) {
        String name = name(key);
        client.checkOpen();
        try {
            client.store().deleteMetadata(topic, name);
        } catch (IOException e) {
            throw client.failure("Removing metadata " + key + " of topic " + topic, e);
        }
        return this;
    }

    private static String name(String key) {
        return "k" + HEX.formatHex(Objects.requireNonNull(key, "key").getBytes(StandardCharsets.UTF_8));
    }

    private static boolean isName(String name) {
        if (name.isEmpty() || name.charAt(0) != 'k' || name.length() % 2 != 1) {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            char c = name.charAt(i);
            if (!(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "AvroRawdataMetadataClient{topic=" + topic + '}';
    }
}
