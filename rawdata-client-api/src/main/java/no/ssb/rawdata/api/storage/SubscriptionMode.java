package no.ssb.rawdata.api.storage;

public enum  SubscriptionMode {

    // queueing - out of order queueing consumption. Many consumer allowed per subscription.
    SHARED,

    // in-order streaming. only one consumer per subscription.
    EXCLUSIVE;

}
