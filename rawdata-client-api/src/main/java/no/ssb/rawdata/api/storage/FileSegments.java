package no.ssb.rawdata.api.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class FileSegments {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ArrayNode jsonFileArray;

    private FileSegments() {
        jsonFileArray = OBJECT_MAPPER.createArrayNode();
    }

    private FileSegments(byte[] data) {
        try {
            jsonFileArray = OBJECT_MAPPER.readValue(data, ArrayNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FileSegments create() {
        return new FileSegments();
    }

    public static FileSegments parse(byte[] data) {
        return new FileSegments(data);
    }

    public FileSegments write(String filename, byte[] data) {
        ObjectNode jsonFileNode = OBJECT_MAPPER.createObjectNode();
        String base64encodedData = new String(Base64.getEncoder().encode(data), StandardCharsets.UTF_8);
        jsonFileNode.put(filename, base64encodedData);
        jsonFileArray.add(jsonFileNode);
        return this;
    }

    public byte[] read(String filename) {
        byte[] result = null;
        for (int n = 0; n < jsonFileArray.size(); n++) {
            JsonNode fileEntry = jsonFileArray.get(n);
            JsonNode jsonNode = fileEntry.get(filename);
            if (jsonNode != null) {
                result = Base64.getDecoder().decode(jsonNode.asText().getBytes());
                break;
            }
        }
        return result;
    }

    public List<String> list() {
        List<String> result = new ArrayList<>();
        for (int n = 0; n < jsonFileArray.size(); n++) {
            JsonNode fileEntry = jsonFileArray.get(n);
            result.add(fileEntry.fieldNames().next());
        }
        return result;
    }

    public String asJson() {
        try {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonFileArray);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toByteArray() {
        try {
            return OBJECT_MAPPER.writeValueAsString(jsonFileArray).getBytes();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
