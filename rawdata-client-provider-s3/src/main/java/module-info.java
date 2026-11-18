/**
 * The {@code s3} provider: topics as Avro files in an Amazon S3 bucket, on the Avro engine, through the AWS SDK for
 * Java 2 with its JDK-based HTTP client. Exports nothing; the provider is found through the API module's
 * {@code uses}.
 * <p>
 * The name ends in a digit, as the SDK's own {@code software.amazon.awssdk.services.s3} does: {@code s3} is the
 * service's name.
 */
@SuppressWarnings("module") // "s3 should avoid terminal digits"
module io.descoped.rawdata.provider.s3 {
    requires io.descoped.rawdata.provider.avro;
    requires software.amazon.awssdk.services.s3;
    requires software.amazon.awssdk.http.urlconnection;
    requires software.amazon.awssdk.core;
    requires software.amazon.awssdk.awscore;
    requires software.amazon.awssdk.auth;
    requires software.amazon.awssdk.regions;
    requires software.amazon.awssdk.http;
    requires software.amazon.awssdk.identity.spi;
    requires software.amazon.awssdk.utils;
    requires software.amazon.awssdk.retries.api;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.provider.s3.S3RawdataClientInitializer;
}
