/**
 * The copy tool: copies topics from one rawdata store to another. The providers are found through the API module's
 * {@code uses}; run it with {@code io.descoped.rawdata.copy.RawdataCopyTool} as the main class.
 */
module io.descoped.rawdata.copy {
    requires io.descoped.rawdata.api;
}
