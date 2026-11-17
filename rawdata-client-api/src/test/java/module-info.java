/**
 * The API tests, as a module of their own that uses the API the way an application does: through
 * {@code RawdataClient.create} and the built-in providers found by the API module's {@code uses}. Open, so that JUnit
 * can reach the test classes.
 */
open module io.descoped.rawdata.api.test {
    requires io.descoped.rawdata.api;
    requires org.junit.jupiter.api;
    requires org.junit.jupiter.params;
}
