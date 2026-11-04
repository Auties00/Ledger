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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/// Eight threads writing small entries as fast as they can, none of them waiting for stable storage.
///
/// Engines whose writers run concurrently (RocksDB's memtable, Berkeley DB's record locks) scale with the threads;
/// single-writer engines (MVStore, LMDB, SQLite, Ledger) run one write at a time however many threads ask. Pass `-t`
/// to change the threads.
///
/// Not a Cobalt workload: Cobalt writes to one session from one place at a time.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Threads(8)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ParallelWritersBenchmark {
    private static final int KEYS = 1 << 20;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    private Corpus corpus;
    private Stores.Store store;

    /// Opens an empty store.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, false);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Writes one 128-byte entry under a random key.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void write() throws IOException {
        var key = ThreadLocalRandom.current().nextInt(KEYS);
        store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(key, 128)), Durability.DEFERRED);
    }
}
