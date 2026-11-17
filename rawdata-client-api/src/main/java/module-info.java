/**
 * The rawdata client API: {@link io.descoped.rawdata.api.RawdataClient} and its provider service
 * {@link io.descoped.rawdata.api.RawdataClientInitializer}, with the built-in {@code memory} and {@code discard}
 * providers.
 */
module io.descoped.rawdata.api {
    requires transitive io.descoped.service.provider.api;

    exports io.descoped.rawdata.api;

    // Resolves provider modules on the module path that no module requires.
    uses io.descoped.rawdata.api.RawdataClientInitializer;

    provides io.descoped.rawdata.api.RawdataClientInitializer with
            io.descoped.rawdata.memory.MemoryRawdataClientInitializer,
            io.descoped.rawdata.discard.DiscardingRawdataClientInitializer;
}
