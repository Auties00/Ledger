package com.github.auties00.ledger.benchmark.workload.cobalt;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
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

/// Clearing a chat: deleting every message of the most active chat, about a sixth of the history, and its id index
/// entries, durably.
///
/// An engine with range deletes records the deletion in constant space; one without visits and removes every key.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ChatDeleteBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds.
    @Param({"200000"})
    public int messages;

    private Corpus corpus;
    private Stores.Store store;

    /// Generates the corpus.
    @Setup(Level.Trial)
    public void generate() {
        corpus = new Corpus(messages);
    }

    /// Opens a copy of the loaded store for the iteration.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Iteration)
    public void open() throws IOException {
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
    }

    /// Closes the iteration's store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Iteration)
    public void close() throws IOException {
        store.close();
    }

    /// Deletes the most active chat.
    ///
    /// @param counters receives what the deletion cost on disk
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void clearChat(IoCounters counters) throws IOException {
        var writtenBefore = ProcessProbe.bytesWritten();
        store.engine().deleteRange(Keyspace.MESSAGES, Corpus.chatStart(0), Corpus.chatEnd(0), Durability.DEFERRED);
        store.engine().deleteRange(Keyspace.MESSAGE_IDS, Corpus.chatStart(0), Corpus.chatEnd(0), Durability.DURABLE);
        counters.bytesWritten = ProcessProbe.bytesWritten() - writtenBefore;
        counters.sizeOnDisk = store.engine().sizeOnDisk();
    }
}
