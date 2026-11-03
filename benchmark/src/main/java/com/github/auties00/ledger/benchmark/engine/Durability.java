package com.github.auties00.ledger.benchmark.engine;

/// When a write reaches stable storage.
///
/// Both levels are atomic and immediately visible to readers; they differ only in what survives a power loss.
public enum Durability {
    /// The write survives a crash of the process, but not necessarily a power loss: the engine may keep it in its
    /// own buffers or the OS page cache.
    DEFERRED,
    /// The write, and every write before it, is on stable storage when the call returns.
    DURABLE
}
