package com.github.auties00.ledger.benchmark.workload.cobalt;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.MemoryCounters;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/// Starting a client over a store that already holds a history.
///
/// [#openAndShowFirstChat(Blackhole)] is the startup: it opens the store, reads the 100 sessions the first sends
/// need and the newest page of the most active chat. [#footprint(MemoryCounters)] opens the store, reads 1,000
/// pages of chats picked by activity, then records the memory the warm store holds; its time is not the point.
///
/// The store's files were just copied, so they sit in the OS page cache: this is a warm start. A cold start needs
/// the cache dropped between iterations, which takes administrator rights.
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ReopenBenchmark {
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

    private Engine open() throws IOException {
        opened = Stores.openAt(EngineKind.of(engine), directory, Stores.settingsFor(corpus), sealed);
        return opened;
    }

    /// Opens the store and reads what the first screen and the first sends need.
    ///
    /// @param blackhole consumes what is read
    /// @throws IOException if the engine fails to open or read
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void openAndShowFirstChat(Blackhole blackhole) throws IOException {
        var engine = open();
        for (var session = 0; session < 100; session++) {
            blackhole.consume(engine.get(Keyspace.SESSIONS, Corpus.sessionKey(session)));
        }
        engine.scan(Keyspace.MESSAGES, Corpus.chatStart(0), Corpus.chatEnd(0), true, 50, (key, value) -> {
            blackhole.consume(value);
            return true;
        });
    }

    /// Opens the store, warms it with 1,000 chat pages and records the memory it holds.
    ///
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open or read
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    public void footprint(MemoryCounters counters) throws IOException {
        var engine = open();
        var random = new SplittableRandom(11);
        var visited = new long[1];
        for (var page = 0; page < 1_000; page++) {
            var chat = corpus.chat(random.nextInt(corpus.messages()));
            engine.scan(Keyspace.MESSAGES, Corpus.chatStart(chat), Corpus.chatEnd(chat), true, 50, (key, value) -> {
                visited[0] += value.length;
                return true;
            });
        }
        counters.record();
        if (visited[0] == 0) {
            throw new IllegalStateException("The store returned no messages");
        }
    }
}
