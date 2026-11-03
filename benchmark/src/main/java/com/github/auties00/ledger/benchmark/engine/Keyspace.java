package com.github.auties00.ledger.benchmark.engine;

/// The named key spaces every engine creates when it opens.
///
/// Keys order by unsigned byte comparison in every key space and every engine.
public enum Keyspace {
    /// Messages keyed by chat number (4 bytes), timestamp (8 bytes) and message id.
    MESSAGES,
    /// Message timestamps keyed by chat number and message id, for lookups by id.
    MESSAGE_IDS,
    /// Signal sessions keyed by address, overwritten on every message.
    SESSIONS,
    /// Chat metadata keyed by chat number, overwritten on every message.
    CHATS,
    /// App-state index entries keyed by a 32-byte MAC.
    APP_STATE,
    /// Pending webhook deliveries keyed by sequence number.
    INBOX,
    /// Values of the workloads that are not modelled on Cobalt.
    GENERIC
}
