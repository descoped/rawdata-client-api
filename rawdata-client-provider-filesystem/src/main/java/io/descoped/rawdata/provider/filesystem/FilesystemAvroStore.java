package io.descoped.rawdata.provider.filesystem;

import io.descoped.rawdata.provider.avro.AvroStore;
import io.descoped.rawdata.provider.avro.AvroTopicFolders;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Topics in a folder: {@code <folder>/<topic folder>/segments/<number>.avro}, the number in 19 digits, and
 * {@code <folder>/<topic folder>/metadata/<name>}. A file is written under a temporary name ({@code .*.tmp}) in the
 * topic's folder first; a segment is then hard-linked to its name, which fails if the name exists, and a metadata
 * entry is moved over its name. Readers so never see part of a file, and two writers never both create a segment;
 * creating an existing segment again with the same bytes counts as created, as in every store.
 * A writer that crashes between the two steps leaves its temporary file behind; nothing reads it, and it may be
 * deleted. The folder must be on a file system with hard links and atomic moves, as local file systems on Linux,
 * macOS and Windows are.
 * <p>
 * The topic folder is the topic's name, kept apart from others on case-insensitive file systems (macOS, Windows) and
 * valid on Windows: an upper-case letter is written as {@code ^} and the letter in lower case, a trailing {@code .}
 * gets a {@code ^} after it, and a Windows device name ({@code con}, {@code nul}, {@code com1}...) gets {@code ~}
 * before it. {@code ^} and {@code ~} are not in topic names, so different topics never share a folder.
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
            // as in every store: the same bytes are this writer's segment (an Avro file has a random sync marker)
            return readIfExists(target).map(stored -> Arrays.equals(content, stored)).orElse(false);
        } finally {
            Files.deleteIfExists(temporary);
        }
    }

    @Override
    public Optional<byte[]> read(String topic, long segment) throws IOException {
        return readIfExists(segment(topic, segment));
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
    public Optional<byte[]> readMetadata(String topic, String name) throws IOException {
        return readIfExists(metadata(topic, name));
    }

    private static Optional<byte[]> readIfExists(Path file) throws IOException {
        try {
            return Optional.of(Files.readAllBytes(file));
        } catch (NoSuchFileException e) {
            return Optional.empty();
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
        Path topicFolder = topicFolder(topic);
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

    private Path topicFolder(String topic) {
        return folder.resolve(AvroTopicFolders.folderName(topic));
    }

    private Path segment(String topic, long segment) {
        return topicFolder(topic).resolve("segments").resolve(String.format("%019d.avro", segment));
    }

    private Path metadata(String topic) {
        return topicFolder(topic).resolve("metadata");
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
