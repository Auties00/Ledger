package com.github.auties00.ledger.benchmark.workload.cobalt;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/// Finding a message by its id, as receipts, reactions, edits and quoted replies do.
///
/// Each operation picks a message uniformly from the whole history, so most hits fall outside any cache of recent
/// data, then reads its key from the id index and its body by that key.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class MessageLookupBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds.
    @Param({"200000"})
    public int messages;

    private Corpus corpus;
    private Stores.Store store;

    /// Opens a copy of the loaded store.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(messages);
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Reads one message by id.
    ///
    /// @return the message body
    /// @throws IOException if the engine fails to read
    @Benchmark
    public byte[] findById() throws IOException {
        var index = ThreadLocalRandom.current().nextInt(corpus.messages());
        var chat = corpus.chat(index);
        var key = store.engine().get(Keyspace.MESSAGE_IDS, corpus.idKey(chat, corpus.sequence(index)));
        return store.engine().get(Keyspace.MESSAGES, key);
    }
}
