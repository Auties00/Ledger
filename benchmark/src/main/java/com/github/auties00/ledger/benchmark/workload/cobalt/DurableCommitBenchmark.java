package com.github.auties00.ledger.benchmark.workload.cobalt;

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

/// One durable write at a time: the Signal session a client stores before it sends a message, or the record it
/// stores before it acknowledges one.
///
/// Each operation overwrites one 1.5 KB session and returns once it is on stable storage, so the latency
/// distribution is the price of every send and every acknowledgement. Nothing runs concurrently, so no engine can
/// share a force.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class DurableCommitBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    private Stores.Store store;
    private Corpus corpus;
    private int next;

    /// Opens an empty store holding the sessions the operations overwrite.
    ///
    /// @throws IOException if the engine fails to open or write
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, sealed);
        var batch = new WriteBatch();
        for (var session = 0; session < Stores.SESSIONS; session++) {
            batch.put(Keyspace.SESSIONS, Corpus.sessionKey(session), corpus.slice(session, Stores.SESSION_BYTES));
        }
        store.engine().write(batch, Durability.DURABLE);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Overwrites one session durably.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void storeSession() throws IOException {
        var session = next++ % Stores.SESSIONS;
        var batch = new WriteBatch().put(Keyspace.SESSIONS, Corpus.sessionKey(session), corpus.slice(next, Stores.SESSION_BYTES));
        store.engine().write(batch, Durability.DURABLE);
    }
}
