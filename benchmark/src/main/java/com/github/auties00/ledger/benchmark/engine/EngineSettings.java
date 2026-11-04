package com.github.auties00.ledger.benchmark.engine;

/// Settings every engine applies the same way, so the comparison stays fair.
///
/// @param cacheBytes the memory each engine may use to cache its data, applied to whatever cache the engine has
///                   (MVStore cache, RocksDB block cache, SQLite page cache, Berkeley DB cache); engines that read
///                   through the OS page cache ignore it
/// @param expectedBytes an estimate of the data the workload stores, which engines that must size their file up
///                      front (LMDB) use to reserve room
/// @param largeValues the size from which Ledger writes a value once, to its file of large values; the other engines
///                    have no such choice and ignore it
public record EngineSettings(long cacheBytes, long expectedBytes, long largeValues) {
    /// The settings of every workload that does not choose its own.
    public static final EngineSettings DEFAULT = new EngineSettings(256L << 20, 256L << 20, 1L << 20);

    /// {@return these settings, expecting `bytes` of data}
    ///
    /// @param bytes the expected data size
    public EngineSettings withExpectedBytes(long bytes) {
        return new EngineSettings(cacheBytes, bytes, largeValues);
    }

    /// {@return these settings, with Ledger writing values of `bytes` or more to its file of large values}
    ///
    /// @param bytes the threshold, at least 4 KiB
    public EngineSettings withLargeValues(long bytes) {
        return new EngineSettings(cacheBytes, expectedBytes, bytes);
    }
}
