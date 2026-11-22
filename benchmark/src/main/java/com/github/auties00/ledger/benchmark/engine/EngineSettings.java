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
/// @param checkpointEvery the log growth after which Ledger starts a checkpoint on its own thread, or 0 for Ledger's
///                        default; the other engines ignore it. The template builder sets it far above the explicit
///                        checkpoints it makes, so that no checkpoint starts at a point thread timing chooses
public record EngineSettings(long cacheBytes, long expectedBytes, long largeValues, long checkpointEvery) {
    /// The settings of every workload that does not choose its own.
    public static final EngineSettings DEFAULT = new EngineSettings(256L << 20, 256L << 20, 1L << 20, 0);

    /// {@return these settings, expecting `bytes` of data}
    ///
    /// @param bytes the expected data size
    public EngineSettings withExpectedBytes(long bytes) {
        return new EngineSettings(cacheBytes, bytes, largeValues, checkpointEvery);
    }

    /// {@return these settings, with Ledger writing values of `bytes` or more to its file of large values}
    ///
    /// @param bytes the threshold, at least 4 KiB
    public EngineSettings withLargeValues(long bytes) {
        return new EngineSettings(cacheBytes, expectedBytes, bytes, checkpointEvery);
    }

    /// {@return these settings, with Ledger starting a checkpoint of its own after `bytes` of log, or after its default
    /// if `bytes` is 0}
    ///
    /// @param bytes the interval, at least 1 MiB, or 0
    public EngineSettings withCheckpointEvery(long bytes) {
        return new EngineSettings(cacheBytes, expectedBytes, largeValues, bytes);
    }
}
