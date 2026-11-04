package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.IoCounters;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.ProcessProbe;
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
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/// Overwriting random keys of a data set larger than the cache: the update-heavy pattern log-structured merge trees
/// are built for and copy-on-write B-trees pay the most for.
///
/// The store holds `keys` uniformly spread 256-byte entries, more than the 256 MiB every engine may cache. Each
/// invocation overwrites [#BATCH] random entries in one deferred batch. A B-tree rewrites a page, or a path of pages
/// under copy-on-write, per scattered update; an LSM tree appends and merges later. `bytesWritten` against
/// `payloadBytes` shows the difference.
///
/// Not a Cobalt workload: Cobalt's updates concentrate on a small, hot set of records.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 3, time = 20)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class RandomOverwriteBenchmark {
    /// The overwrites of one invocation.
    public static final int BATCH = 100;
    private static final int VALUE_SIZE = 256;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// The entries the store holds.
    @Param({"2000000"})
    public int keys;

    private Corpus corpus;
    private Stores.Store store;
    private SplittableRandom random;
    private long writtenBefore;
    private int invocations;

    /// Opens a copy of the loaded store.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        random = new SplittableRandom(3);
        var settings = EngineSettings.DEFAULT.withExpectedBytes((long) keys * (VALUE_SIZE + 64));
        store = Stores.loadedGeneric(EngineKind.of(engine), settings, keys, VALUE_SIZE);
    }

    /// Notes the bytes written so far, so the iteration reports its own.
    @Setup(Level.Iteration)
    public void startIteration() {
        writtenBefore = ProcessProbe.bytesWritten();
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Overwrites [#BATCH] random entries.
    ///
    /// @param counters receives what the overwrites cost on disk
    /// @throws IOException if the engine fails to write
    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void overwrite(IoCounters counters) throws IOException {
        var batch = new WriteBatch();
        for (var i = 0; i < BATCH; i++) {
            var key = random.nextInt(keys);
            batch.put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(random.nextLong(), VALUE_SIZE));
        }
        store.engine().write(batch, Durability.DEFERRED);
        counters.payloadBytes += BATCH * (16L + VALUE_SIZE);
        counters.bytesWritten = ProcessProbe.bytesWritten() - writtenBefore;
        if ((++invocations & 1023) == 0) {
            counters.sizeOnDisk = store.engine().sizeOnDisk();
        }
    }
}
