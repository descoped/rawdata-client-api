package io.descoped.rawdata.api;

import io.descoped.service.provider.api.ProviderInitializer;

/**
 * The provider service: a provider implements it, names itself with
 * {@link io.descoped.service.provider.api.ProviderName}, and registers it with {@code provides} and
 * {@code META-INF/services}.
 */
public interface RawdataClientInitializer extends ProviderInitializer<io.descoped.rawdata.api.RawdataClient> {
}
