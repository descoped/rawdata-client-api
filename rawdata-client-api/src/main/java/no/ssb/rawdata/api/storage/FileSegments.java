package no.ssb.rawdata.api.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class FileSegments {

    static final ObjectMapper OBJECT_MAPPER;

    static {
        OBJECT_MAPPER = new ObjectMapper();
    }

    private final ArrayNode jsonFileArray;

    public static ObjectMapper objectMapper() {
        return OBJECT_MAPPER;
    }

    public static <T> T fromJson(InputStream source, Class<T> clazz) {
        try {
            String json = new String(source.readAllBytes(), StandardCharsets.UTF_8);
            return objectMapper().readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJSON(Object value) {
        try {
            return objectMapper().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toPrettyJSON(Object value) {
        try {
            return objectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public FileSegments() {
        jsonFileArray = objectMapper().createArrayNode();
    }

    public FileSegments write(String filename, byte[] data) {
        ObjectNode jsonFileNode = objectMapper().createObjectNode();
        String base64encodedData = new String(Base64.getEncoder().encode(data), StandardCharsets.UTF_8);
        jsonFileNode.put(filename, base64encodedData);
        jsonFileArray.add(jsonFileNode);
        return this;
    }

    public byte[] read(byte[] data, String filename) {
        ArrayNode metadataArrayNode = fromJson(new ByteArrayInputStream(data), ArrayNode.class);
        byte[] result = null;
        for(int n = 0; n < metadataArrayNode.size(); n++) {
            JsonNode fileEntry = metadataArrayNode.get(n);
            JsonNode jsonNode = fileEntry.get(filename);
            if (jsonNode != null) {
                result = Base64.getDecoder().decode(jsonNode.asText().getBytes());
            }
        }
        return result;
    }

    public List<String> list(byte[] data) {
        ArrayNode metadataArrayNode = fromJson(new ByteArrayInputStream(data), ArrayNode.class);
        List<String> result = new ArrayList<>();
        for(int n = 0; n < metadataArrayNode.size(); n++) {
            JsonNode fileEntry = metadataArrayNode.get(n);
            result.add(fileEntry.fieldNames().next());
        }
        return result;
    }

    public String asJson() {
        return toPrettyJSON(jsonFileArray);
    }

    public byte[] toByteArray() {
        return toJSON(jsonFileArray).getBytes();
    }
}
