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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/// Many virtual threads committing durably at once: several sessions in one process, or a burst of webhook
/// deliveries, each waiting for its own write to reach stable storage.
///
/// An operation is one durable 1 KB write; each invocation runs [#COMMITS] of them, split evenly over `committers`
/// virtual threads, each writing its own keys. An engine with group commit serves concurrent committers with fewer
/// forces than writes, so its throughput grows with `committers`; one without it stays where a single committer is.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class GroupCommitBenchmark {
    /// The durable writes of one invocation.
    public static final int COMMITS = 512;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    /// The virtual threads committing concurrently; divides [#COMMITS].
    @Param({"1", "8", "64", "256"})
    public int committers;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    private Stores.Store store;
    private Corpus corpus;
    private final AtomicLong sequence = new AtomicLong();

    /// Opens an empty store.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        if (COMMITS % committers != 0) {
            throw new IllegalArgumentException("committers must divide " + COMMITS + ": " + committers);
        }
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

    /// Runs [#COMMITS] durable writes over `committers` virtual threads and waits for all of them.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(COMMITS)
    public void commitConcurrently() throws Exception {
        var perCommitter = COMMITS / committers;
        var results = new ArrayList<Future<?>>(committers);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var committer = 0; committer < committers; committer++) {
                results.add(executor.submit(() -> {
                    for (var i = 0; i < perCommitter; i++) {
                        var id = sequence.getAndIncrement();
                        // Keys cycle over a bounded set, so the store does not grow without bound
                        var batch = new WriteBatch().put(Keyspace.INBOX, Corpus.longKey(id % 65_536), corpus.slice(id, 1_024));
                        store.engine().write(batch, Durability.DURABLE);
                    }
                    return null;
                }));
            }
        }
        for (var result : results) {
            result.get();
        }
    }
}
