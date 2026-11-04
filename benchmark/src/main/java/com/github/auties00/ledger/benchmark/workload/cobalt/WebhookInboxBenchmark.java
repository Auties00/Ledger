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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/// The Cloud API webhook inbox: every delivery is stored durably before Meta gets its `200`, and removed once the
/// listeners it reached have returned.
///
/// Each operation stores one delivery of 1 to 8 KB durably and removes the one stored [#IN_FLIGHT] deliveries
/// earlier without waiting for stable storage. Four JMH threads deliver concurrently, as concurrent webhook requests
/// do; pass `-t` to change that.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Threads(4)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class WebhookInboxBenchmark {
    /// The deliveries stored and not yet removed.
    public static final int IN_FLIGHT = 64;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    private Corpus corpus;
    private Stores.Store store;
    private final AtomicLong sequence = new AtomicLong();

    /// Opens an empty store.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, sealed);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Stores one delivery durably and removes an older one.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void deliver() throws IOException {
        var number = sequence.getAndIncrement();
        var size = 1_024 + (int) Math.floorMod(number * 7_919, 7_169);
        store.engine().write(new WriteBatch().put(Keyspace.INBOX, Corpus.longKey(number), corpus.slice(number, size)), Durability.DURABLE);
        if (number >= IN_FLIGHT) {
            store.engine().write(new WriteBatch().delete(Keyspace.INBOX, Corpus.longKey(number - IN_FLIGHT)), Durability.DEFERRED);
        }
    }
}
