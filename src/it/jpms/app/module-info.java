/**
 * The JPMS smoke test: an application that requires only the rawdata API, as a user's would, and finds the providers
 * through the service loader.
 */
module io.descoped.rawdata.smoke {
    requires io.descoped.rawdata.api;

    uses io.descoped.rawdata.api.RawdataClientInitializer;
}
