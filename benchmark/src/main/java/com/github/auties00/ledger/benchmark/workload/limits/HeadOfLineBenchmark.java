package com.github.auties00.ledger.benchmark.workload.limits;

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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/// The latency of blind commits of their own keys while other threads keep updating one hot key, against the same
/// commits with the hot threads idle.
///
/// Eight threads commit 128-byte values under random keys of their own, which is what is measured. Beside them,
/// `hotThreads` virtual threads update one hot counter as fast as they can: `compute` through the engine's
/// read-modify-write, Ledger's `compute` or, for `ledger-exclusive`, a transaction that runs alone; `none` not at all.
/// A compute whose key a write ahead of it holds runs its function in the order of the commits, which the commits
/// behind it wait to become visible for; a transaction stops every commit while it runs. None of the writes waits for
/// stable storage.
///
/// `disjoint` is the fair baseline of `compute`: the same threads compute as fast, each a counter of its own, so the
/// store and the processors carry the same load with no key shared. The ratio of `compute` to `disjoint` is what the
/// hot key costs the measured commits; the ratio of `disjoint` to `none` is what the extra writers cost them.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Threads(8)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class HeadOfLineBenchmark {
    private static final int KEYS = 1 << 20;
    private static final byte[] HOT = Corpus.hashedKey(-1, 16);

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-exclusive"})
    public String engine;

    /// How the hot key is updated: `none`, `compute`, `put`, a blind write of the key, which orders behind the
    /// writes of the key ahead of it as a compute does, or `disjoint`, computes of a key of each thread's own.
    @Param({"none", "compute", "put", "disjoint"})
    public String hot;

    /// The threads updating the hot key.
    @Param({"1", "4"})
    public int hotThreads;

    private Corpus corpus;
    private Stores.Store store;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ArrayList<Thread> hammers = new ArrayList<>();

    /// Opens an empty store and starts the threads updating the hot key.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, false);
        if (hot.equals("none")) {
            return;
        }
        for (var i = 0; i < hotThreads; i++) {
            var key = hot.equals("disjoint") ? Corpus.hashedKey(-2 - i, 16) : HOT;
            hammers.add(Thread.ofVirtual().name("hot-key").start(() -> {
                while (!stopped.get()) {
                    try {
                        if (hot.equals("put")) {
                            store.engine().write(new WriteBatch().put(Keyspace.CHATS, key, corpus.slice(0, 128)), Durability.DEFERRED);
                            continue;
                        }
                        store.engine().compute(Keyspace.CHATS, key, value -> {
                            var next = value == null ? corpus.slice(0, 128) : value.clone();
                            var buffer = ByteBuffer.wrap(next);
                            buffer.putLong(0, value == null ? 1 : buffer.getLong(0) + 1);
                            return next;
                        }, Durability.DEFERRED);
                    } catch (IOException exception) {
                        throw new RuntimeException(exception);
                    }
                }
            }));
        }
    }

    /// Stops the hot threads, closes the store and deletes it.
    ///
    /// @throws Exception if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws Exception {
        stopped.set(true);
        for (var hammer : hammers) {
            hammer.join();
        }
        store.close();
    }

    /// Commits one 128-byte value under a random key of the thread's own.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void blindCommit() throws IOException {
        var key = ThreadLocalRandom.current().nextInt(KEYS);
        store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(key, 128)), Durability.DEFERRED);
    }
}
