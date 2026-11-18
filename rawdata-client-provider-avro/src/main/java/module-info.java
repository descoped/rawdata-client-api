/**
 * The Avro engine: topics as Avro files, one per publish, in an {@link io.descoped.rawdata.provider.avro.AvroStore}.
 * Exports the engine, {@link io.descoped.rawdata.provider.avro.AvroRawdataClient}, and the store it writes to, for
 * the providers built on it: {@code filesystem}, {@code gcs} and {@code s3}. It is not a provider itself.
 */
module io.descoped.rawdata.provider.avro {
    requires transitive io.descoped.rawdata.api;
    requires org.apache.avro;

    exports io.descoped.rawdata.provider.avro;
}
