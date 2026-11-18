package io.descoped.rawdata.provider.avro;

import java.util.Locale;
import java.util.Set;

/**
 * The name of a topic's folder on a local file system, used for the upload window's buffer and by the
 * {@code filesystem} store. It keeps topics apart on case-insensitive file systems (macOS, Windows) and is valid on
 * Windows: an upper-case letter is written as {@code ^} and the letter in lower case, a trailing {@code .} gets a
 * {@code ^} after it, and a Windows device name ({@code con}, {@code nul}, {@code com1}...) gets {@code ~} before it.
 * {@code ^} and {@code ~} are not in topic names, so different topics never share a folder.
 */
public final class AvroTopicFolders {

    private static final Set<String> WINDOWS_DEVICES = Set.of("con", "prn", "aux", "nul",
            "com0", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
            "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9");

    private AvroTopicFolders() {
    }

    /**
     * The folder name of a topic.
     *
     * @param topic a valid topic name
     * @return the folder name
     */
    public static String folderName(String topic) {
        StringBuilder name = new StringBuilder(topic.length() + 4);
        String base = topic.contains(".") ? topic.substring(0, topic.indexOf('.')) : topic;
        if (WINDOWS_DEVICES.contains(base.toLowerCase(Locale.ROOT))) {
            name.append('~');
        }
        for (int i = 0; i < topic.length(); i++) {
            char c = topic.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                name.append('^').append(Character.toLowerCase(c));
            } else {
                name.append(c);
            }
        }
        if (topic.endsWith(".")) {
            name.append('^');
        }
        return name.toString();
    }
}
