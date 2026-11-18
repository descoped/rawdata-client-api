/**
 * The contract tests every rawdata provider must pass: extend
 * {@link io.descoped.rawdata.tck.RawdataClientContractTest} in the provider's tests and give it the provider's
 * configuration. A provider built on the Avro engine also extends
 * {@link io.descoped.rawdata.tck.avro.AvroStoreContractTest} with its store. Open, so that JUnit can run the
 * inherited tests.
 */
open module io.descoped.rawdata.tck {
    requires transitive io.descoped.rawdata.api;
    requires transitive io.descoped.rawdata.provider.avro;
    requires transitive org.junit.jupiter.api;
    requires transitive org.junit.jupiter.params;

    exports io.descoped.rawdata.tck;
    exports io.descoped.rawdata.tck.avro;
}
