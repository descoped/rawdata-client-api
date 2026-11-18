/**
 * The {@code filesystem} provider: topics as Avro files in a local folder, on the Avro engine. Exports nothing; the
 * provider is found through the API module's {@code uses}.
 */
module io.descoped.rawdata.provider.filesystem {
    requires io.descoped.rawdata.provider.avro;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.provider.filesystem.FilesystemRawdataClientInitializer;
}
