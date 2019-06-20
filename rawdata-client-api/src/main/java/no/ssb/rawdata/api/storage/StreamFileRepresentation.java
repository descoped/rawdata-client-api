package no.ssb.rawdata.api.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StreamFileRepresentation {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final ArrayNode jsonFileArray;

    private StreamFileRepresentation() {
        jsonFileArray = OBJECT_MAPPER.createArrayNode();
    }

    private StreamFileRepresentation(byte[] data) {
        try {
            jsonFileArray = OBJECT_MAPPER.readValue(data, ArrayNode.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamFileRepresentation create() {
        return new StreamFileRepresentation();
    }

    public static StreamFileRepresentation parse(byte[] data) {
        return new StreamFileRepresentation(data);
    }

    public StreamFileRepresentation write(String filename, byte[] data) {
        ObjectNode jsonFileNode = OBJECT_MAPPER.createObjectNode();
        jsonFileNode.put(filename, data);
        jsonFileArray.add(jsonFileNode);
        return this;
    }

    public byte[] read(String filename) {
        byte[] result = null;
        for (int n = 0; n < jsonFileArray.size(); n++) {
            JsonNode fileEntry = jsonFileArray.get(n);
            JsonNode jsonNode = fileEntry.get(filename);
            if (jsonNode != null) {
                try {
                    result = jsonNode.binaryValue();
                } catch (IOException e) {
                    throw new RuntimeException();
                }
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
