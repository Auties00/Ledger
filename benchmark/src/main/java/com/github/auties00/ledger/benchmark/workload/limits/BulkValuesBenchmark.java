package com.github.auties00.ledger.benchmark.workload.limits;

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

/// The history sync of `cobalt.HistoryIngestBenchmark`, on Ledger alone, sweeping the bytes from which a batch writes its
/// values too large for a leaf to the file of large values rather than to the log and again in a checkpoint.
///
/// The corpus's values stay far below every threshold here, so [#largeValues] moves that rule of a batch only. Against
/// `batchSize`, it finds the bytes from which writing once pays, on each file system: a batch of 1,000 messages holds
/// about 1.3 MiB of units, never less than 1.1 MiB, one of 250 about 0.33 MiB. Above what a batch holds, it keeps
/// them in the log and shows what writing once saves; `bytesWritten` says the same in bytes.
///
/// Its own class, so the results of `cobalt.HistoryIngestBenchmark` keep comparing with those of earlier versions.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class BulkValuesBenchmark {
    /// The messages stored.
    @Param({"200000"})
    public int messages;

    /// The messages per batch.
    @Param({"250", "1000", "10000"})
    public int batchSize;

    /// The bytes of units from which a batch writes them to the file of large values.
    @Param({"262144", "524288", "1048576", "2097152"})
    public long largeValues;

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
        store = Stores.empty(EngineKind.of("ledger"), Stores.settingsFor(corpus).withLargeValues(largeValues), true);
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
