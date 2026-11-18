/**
 * The {@code gcs} provider: topics as Avro files in a Google Cloud Storage bucket, on the Avro engine, through the
 * GCS JSON API on the JDK's HTTP client. Exports nothing; the provider is found through the API module's
 * {@code uses}.
 */
module io.descoped.rawdata.provider.gcs {
    requires io.descoped.rawdata.provider.avro;
    requires java.net.http;
    requires com.fasterxml.jackson.databind;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.provider.gcs.GcsRawdataClientInitializer;
}
