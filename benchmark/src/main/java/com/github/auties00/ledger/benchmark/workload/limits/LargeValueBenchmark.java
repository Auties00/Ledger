package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/// Values of megabytes: media bodies or backups kept in the store rather than beside it.
///
/// [#writeValue()] overwrites one of [#SLOTS] values durably, so the store stays the same size and the engine must
/// reclaim the space of the value it replaces; [#readValue()] reads one back. Engines that split values across fixed
/// pages, or rewrite them during compaction, pay per byte here more than engines that store a value contiguously.
///
/// Not a Cobalt workload: Cobalt keeps media bodies in files of their own.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class LargeValueBenchmark {
    /// The values the workload cycles over.
    public static final int SLOTS = 8;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    /// The size of each value.
    @Param({"1048576", "16777216"})
    public int valueSize;

    /// The size from which Ledger writes a value once, to its file of large values, rather than to its log and again
    /// in a checkpoint; [Long#MAX_VALUE] keeps every value in the log. Sweeping it against [#valueSize] finds the size
    /// from which writing once pays.
    @Param({"262144"})
    public long largeValues;

    private Corpus corpus;
    private Stores.Store store;
    private int next;

    /// Opens an empty store holding [#SLOTS] values.
    ///
    /// @throws IOException if the engine fails to open or write
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        var settings = EngineSettings.DEFAULT.withExpectedBytes((long) SLOTS * valueSize * 4).withLargeValues(largeValues);
        store = Stores.empty(EngineKind.of(engine), settings, false);
        for (var slot = 0; slot < SLOTS; slot++) {
            store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.longKey(slot), corpus.slice(slot, valueSize)), Durability.DURABLE);
        }
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Overwrites one value durably.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void writeValue() throws IOException {
        var slot = next++ % SLOTS;
        store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.longKey(slot), corpus.slice(next, valueSize)), Durability.DURABLE);
    }

    /// Reads one value.
    ///
    /// @return the value
    /// @throws IOException if the engine fails to read
    @Benchmark
    public byte[] readValue() throws IOException {
        return store.engine().get(Keyspace.GENERIC, Corpus.longKey(next++ % SLOTS));
    }
}
