package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
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

/// Writing while a long reader holds an old version of the store open, as an export or a backup would.
///
/// The store holds [#KEYS] 512-byte entries and each iteration overwrites [#WRITES] of them durably, one at a time;
/// the score is the time per write. With `pinned` set, a read view opened before the first write stays open
/// throughout. A copy-on-write engine cannot reuse the pages that view still sees, so its files grow with every
/// write; `sizeOnDisk` at the end of each iteration, with and without the view, shows by how much. The writes are
/// counted rather than timed so the growth stays bounded: LMDB, whose map is sized up front, would otherwise fill it.
///
/// Not a Cobalt workload: Cobalt holds no read open for long. Ledger pins versions the same way, so the cost applies
/// to it as well.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class PinnedReaderBenchmark {
    /// The durable overwrites of one iteration.
    public static final int WRITES = 5_000;
    private static final int KEYS = 20_000;
    private static final int VALUE_SIZE = 512;

    /// The engine, by [EngineKind#id()]; Berkeley DB and the floor have no read views.
    @Param({"ledger", "ledger-plain", "mvstore", "rocksdb", "lmdb", "sqlite"})
    public String engine;

    /// Whether a read view stays open while the writes run.
    @Param({"false", "true"})
    public boolean pinned;

    private Corpus corpus;
    private Stores.Store store;
    private Engine.ReadView view;
    private SplittableRandom random;
    private long writtenBefore;

    /// Opens an empty store, fills it and opens the read view.
    ///
    /// @throws IOException if the engine fails to open, write or read
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        random = new SplittableRandom(5);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT.withExpectedBytes(1L << 30), false);
        var batch = new WriteBatch();
        for (var key = 0; key < KEYS; key++) {
            batch.put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(key, VALUE_SIZE));
        }
        store.engine().write(batch, Durability.DURABLE);
        if (pinned) {
            view = store.engine().openReadView();
            if (view.get(Keyspace.GENERIC, Corpus.hashedKey(0, 16)) == null) {
                throw new IllegalStateException("The read view does not see the data");
            }
        }
    }

    /// Notes the bytes written so far, so the iteration reports its own.
    @Setup(Level.Iteration)
    public void startIteration() {
        writtenBefore = ProcessProbe.bytesWritten();
    }

    /// Closes the read view, the store, and deletes the store.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        if (view != null) {
            view.close();
        }
        store.close();
    }

    /// Overwrites [#WRITES] entries durably, one at a time.
    ///
    /// @param counters receives what the writes cost on disk
    /// @throws IOException if the engine fails to write
    @Benchmark
    @OperationsPerInvocation(WRITES)
    public void overwrite(IoCounters counters) throws IOException {
        for (var i = 0; i < WRITES; i++) {
            var key = random.nextInt(KEYS);
            store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(random.nextLong(), VALUE_SIZE)), Durability.DURABLE);
        }
        counters.payloadBytes = WRITES * (16L + VALUE_SIZE);
        counters.bytesWritten = ProcessProbe.bytesWritten() - writtenBefore;
        counters.sizeOnDisk = store.engine().sizeOnDisk();
    }
}
