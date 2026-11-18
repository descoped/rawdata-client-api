package io.descoped.rawdata.provider.avro.filesystem;

import io.descoped.rawdata.provider.avro.AvroStore;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Topics in a folder: {@code <folder>/<topic>/segments/<number>.avro}, the number in 19 digits, and
 * {@code <folder>/<topic>/metadata/<name>}. A file is written under a temporary name in the topic's folder first; a
 * segment is then hard-linked to its name, which fails if the name exists, and a metadata entry is moved over its
 * name. Readers so never see part of a file, and two writers never both create a segment. The folder must be on a
 * file system with hard links and atomic moves, as local file systems on Linux, macOS and Windows are.
 */
final class FilesystemAvroStore implements AvroStore {

    private static final int MAX_NAME_LENGTH = 255;

    private final Path folder;

    FilesystemAvroStore(Path folder) {
        this.folder = folder;
    }

    @Override
    public boolean create(String topic, long segment, byte[] content) throws IOException {
        Path target = segment(topic, segment);
        Files.createDirectories(target.getParent());
        Path temporary = temporaryFile(topic, content);
        try {
            Files.createLink(target, temporary);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    @Override
    public byte[] read(String topic, long segment) throws IOException {
        try {
            return Files.readAllBytes(segment(topic, segment));
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    public boolean exists(String topic, long segment) {
        return Files.exists(segment(topic, segment));
    }

    @Override
    public Set<String> metadataNames(String topic) throws IOException {
        Set<String> names = new TreeSet<>();
        try (Stream<Path> files = Files.list(metadata(topic))) {
            files.forEach(file -> names.add(file.getFileName().toString()));
        } catch (NoSuchFileException e) {
            // no metadata yet
        }
        return names;
    }

    @Override
    public byte[] readMetadata(String topic, String name) throws IOException {
        try {
            return Files.readAllBytes(metadata(topic, name));
        } catch (NoSuchFileException e) {
            return null;
        }
    }

    @Override
    public void writeMetadata(String topic, String name, byte[] value) throws IOException {
        Path target = metadata(topic, name);
        Files.createDirectories(target.getParent());
        Path temporary = temporaryFile(topic, value);
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    @Override
    public void deleteMetadata(String topic, String name) throws IOException {
        Files.deleteIfExists(metadata(topic, name));
    }

    @Override
    public void close() {
        // nothing to release
    }

    private Path temporaryFile(String topic, byte[] content) throws IOException {
        Path topicFolder = folder.resolve(topic);
        Files.createDirectories(topicFolder);
        Path temporary = Files.createTempFile(topicFolder, ".", ".tmp");
        try {
            Files.write(temporary, content);
        } catch (IOException | RuntimeException e) {
            Files.deleteIfExists(temporary);
            throw e;
        }
        return temporary;
    }

    private Path segment(String topic, long segment) {
        return folder.resolve(topic).resolve("segments").resolve(String.format("%019d.avro", segment));
    }

    private Path metadata(String topic) {
        return folder.resolve(topic).resolve("metadata");
    }

    private Path metadata(String topic, String name) {
        if (name.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException("A metadata key of the filesystem provider is at most "
                    + (MAX_NAME_LENGTH - 1) / 2 + " bytes in UTF-8");
        }
        return metadata(topic).resolve(name);
    }

    @Override
    public String toString() {
        return "FilesystemAvroStore{folder=" + folder + '}';
    }
}
