package no.ssb.rawdata.api.state;

import no.ssb.rawdata.api.storage.FileSegments;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;

public class FileSegmentsTest {

    byte[] file1 = "abc".getBytes();
    byte[] file2 = "def".getBytes();
    byte[] file3 = "ghi".getBytes();

    @Test
    public void testName() {
        FileSegments fileSegments = new FileSegments();
        fileSegments.write("file1.txt", file1);
        fileSegments.write("file2.txt", file2);
        fileSegments.write("file3.txt", file3);

        String json = fileSegments.asJson();
        System.out.printf("%s%n", json);

        byte[] actualfile1 = fileSegments.read(json.getBytes(), "file1.txt");
        assertEquals(actualfile1, file1);

        List<String> files = fileSegments.list(json.getBytes());
        System.out.printf("%s%n", files.stream().collect(Collectors.joining(",")));
    }
}
