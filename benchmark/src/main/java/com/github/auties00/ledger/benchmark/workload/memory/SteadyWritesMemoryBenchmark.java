package com.github.auties00.ledger.benchmark.workload.memory;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Arrivals;
import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import com.github.auties00.ledger.benchmark.support.WriteMemoryCounters;
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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/// The memory a store holds while messages keep arriving: what the writes it has not checkpointed yet hold, between and
/// across its checkpoints.
///
/// [#write(WriteMemoryCounters)] opens a copy of a store holding `messages` messages, then stores half as many received
/// messages again, 100 per batch, every tenth batch and the last durable and the others deferred, as a client catching
/// up after a long time offline does. Ten times along the way, at fixed message counts, it collects the heap and keeps
/// the largest live heap (`heapBytesLive`), which for an engine that keeps its recent writes in memory until a
/// checkpoint is what they hold; `heapBytes` is what is left after the last write. With 200,000 messages the writes put
/// about 340 MB of keys and sealed values through the engine, more than Ledger's default checkpoint interval for a 4
/// GiB heap (256 MiB of log), so the samples straddle at least one of its checkpoints. Its time is not the point.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(value = 3, warmups = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_GROWING, Jvm.HEAP_MAX})
public class SteadyWritesMemoryBenchmark {
    private static final int MESSAGES_PER_BATCH = 100;
    private static final int DURABLE_EVERY = 10;
    private static final int SAMPLES = 10;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds before the writes; the workload stores half as many again.
    @Param({"200000"})
    public int messages;

    private Corpus corpus;
    private Path directory;
    private Engine opened;

    /// Generates the corpus.
    @Setup(Level.Trial)
    public void generate() {
        corpus = new Corpus(messages);
    }

    /// Copies the loaded store for the iteration.
    ///
    /// @throws IOException if the store cannot be built or copied
    @Setup(Level.Iteration)
    public void copy() throws IOException {
        directory = Stores.copyOf(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
    }

    /// Closes the iteration's store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Iteration)
    public void close() throws IOException {
        try {
            if (opened != null) {
                opened.close();
                opened = null;
            }
        } finally {
            Directories.delete(directory);
        }
    }

    /// Opens the store, stores the received messages and samples the memory while it does.
    ///
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open or write
    @Benchmark
    public void write(WriteMemoryCounters counters) throws IOException {
        counters.begin();
        opened = Stores.openAt(EngineKind.of(engine), directory, Stores.settingsFor(corpus), sealed);
        var batches = Math.max(SAMPLES, messages / 2 / MESSAGES_PER_BATCH);
        var arrivals = new Arrivals(corpus, 0, 13);
        var batch = new WriteBatch();
        for (var number = 1; number <= batches; number++) {
            batch.clear();
            for (var i = 0; i < MESSAGES_PER_BATCH; i++) {
                arrivals.receive(batch);
            }
            opened.write(batch, number % DURABLE_EVERY == 0 || number == batches ? Durability.DURABLE : Durability.DEFERRED);
            if (number % (batches / SAMPLES) == 0) {
                batch.clear();
                counters.sample();
            }
        }
        batch.clear();
        counters.end();
    }
}
