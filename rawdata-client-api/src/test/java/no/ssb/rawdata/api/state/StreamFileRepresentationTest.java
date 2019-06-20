package no.ssb.rawdata.api.state;

import no.ssb.rawdata.api.storage.StreamFileRepresentation;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class StreamFileRepresentationTest {

    byte[] file1 = "abc".getBytes();
    byte[] file2 = "def".getBytes();
    byte[] file3 = "ghi".getBytes();

    @Test
    public void testName() {
        StreamFileRepresentation streamFileRepresentation = StreamFileRepresentation.create();
        streamFileRepresentation.write("file1.txt", file1);
        streamFileRepresentation.write("file2.txt", file2);
        streamFileRepresentation.write("file3.txt", file3);

        String json = streamFileRepresentation.asJson();
        System.out.printf("%s%n", json);

        byte[] actualfile1 = streamFileRepresentation.read("file1.txt");
        assertEquals(actualfile1, file1);

        List<String> files = streamFileRepresentation.list();
        System.out.printf("%s%n", files.stream().collect(Collectors.joining(",")));

        StreamFileRepresentation parsed = StreamFileRepresentation.parse(streamFileRepresentation.toByteArray());
        assertEquals(streamFileRepresentation.toByteArray(), parsed.toByteArray());
    }
}
