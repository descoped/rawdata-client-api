/**
 * The tests of the Avro engine and the {@code filesystem} provider, as a module of their own that uses the provider
 * through {@code RawdataClient.create}, as an application does. Open, so that JUnit can reach the test classes.
 */
open module io.descoped.rawdata.provider.avro.test {
    requires io.descoped.rawdata.provider.avro;
    requires io.descoped.rawdata.tck;
    requires org.apache.avro;
}
