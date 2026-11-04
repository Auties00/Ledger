package com.github.auties00.ledger.benchmark.workload.cobalt;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/// Storing the history a new companion receives right after pairing: every message, in the order a history sync
/// delivers them, in batches that need not be durable on their own, followed by one durable write.
///
/// The score is the time to store `messages` messages into an empty store. The secondary results say what that
/// cost on disk: `bytesWritten` against `payloadBytes` is the engine's write amplification, and `sizeOnDisk`
/// against `payloadBytes` its space amplification.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class HistoryIngestBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages stored.
    @Param({"200000"})
    public int messages;

    /// The messages per batch.
    @Param({"1000", "10000"})
    public int batchSize;

    private Corpus corpus;
    private long payloadBytes;
    private Stores.Store store;

    /// Generates the corpus and measures its payload.
    @Setup(Level.Trial)
    public void generate() {
        corpus = new Corpus(messages);
        var total = 0L;
        for (var index = 0; index < corpus.messages(); index++) {
            var chat = corpus.chat(index);
            var sequence = corpus.sequence(index);
            // The message under its key, and the key under the id index key
            total += Corpus.MESSAGE_KEY_LENGTH * 2L + corpus.body(chat, sequence).length + 4 + Corpus.ID_LENGTH;
        }
        payloadBytes = total;
    }

    /// Opens an empty store for the iteration.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Iteration)
    public void open() throws IOException {
        store = Stores.empty(EngineKind.of(engine), Stores.settingsFor(corpus), sealed);
    }

    /// Closes the iteration's store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Iteration)
    public void close() throws IOException {
        store.close();
    }

    /// Stores the whole corpus.
    ///
    /// @param counters receives what the ingest cost on disk
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void ingest(IoCounters counters) throws IOException {
        var writtenBefore = ProcessProbe.bytesWritten();
        Stores.ingest(store.engine(), corpus, batchSize);
        counters.bytesWritten = ProcessProbe.bytesWritten() - writtenBefore;
        counters.payloadBytes = payloadBytes;
        counters.sizeOnDisk = store.engine().sizeOnDisk();
    }
}
