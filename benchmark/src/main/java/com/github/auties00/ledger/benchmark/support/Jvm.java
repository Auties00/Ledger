package com.github.auties00.ledger.benchmark.support;

/// The options every forked benchmark JVM gets.
public final class Jvm {
    /// RocksDB, SQLite and LMDB load native libraries, and the process probe calls the OS.
    public static final String NATIVE_ACCESS = "--enable-native-access=ALL-UNNAMED";
    /// lmdbjava and its JNR binding use `sun.misc.Unsafe`.
    public static final String UNSAFE = "--sun-misc-unsafe-memory-access=allow";
    /// A fixed heap, so collections do not resize it during a measurement.
    public static final String HEAP_MIN = "-Xms4g";
    /// The same bound as [#HEAP_MIN].
    public static final String HEAP_MAX = "-Xmx4g";
    /// A heap that starts small and grows up to [#HEAP_MAX], for the workloads that measure memory: the heap the JVM
    /// commits, and so its resident memory, then follow what the engine holds, as in an application left to the JVM's
    /// defaults. The maximum stays that of the other workloads, from which Ledger sizes its defaults.
    public static final String HEAP_GROWING = "-Xms64m";

    private Jvm() {
    }
}
