/**
 * The Avro engine and the {@code filesystem} provider: topics as Avro files, one per publish, in a local folder.
 * Exports the engine, {@link io.descoped.rawdata.provider.avro.AvroRawdataClient} and the
 * {@link io.descoped.rawdata.provider.avro.AvroStore} it writes to, for providers that keep the files elsewhere, such
 * as {@code gcs}.
 */
module io.descoped.rawdata.provider.avro {
    requires transitive io.descoped.rawdata.api;
    requires org.apache.avro;

    exports io.descoped.rawdata.provider.avro;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.provider.avro.filesystem.FilesystemRawdataClientInitializer;
}
