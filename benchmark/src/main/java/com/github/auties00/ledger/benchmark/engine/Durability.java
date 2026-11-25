package com.github.auties00.ledger.benchmark.engine;

/// When a write reaches stable storage.
///
/// Both levels are atomic and immediately visible to readers; they differ in what survives a crash.
public enum Durability {
    /// The write is not necessarily durable: the engine may keep it in its own buffers, so a crash of the process can
    /// lose it, or in the OS page cache, so a power loss can. Each engine's adapter says which.
    DEFERRED,
    /// The write, and every write before it, is on stable storage when the call returns.
    DURABLE
}
