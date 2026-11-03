package com.github.auties00.ledger.benchmark.workload.limits;

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
import java.nio.ByteBuffer;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/// A question the schema did not plan for: the 50 newest starred messages across every chat.
///
/// No key space indexes stars, so a key-value engine, Ledger included, reads and opens every message and keeps the
/// newest 50 starred ones. [SqlIndexQueryBenchmark] asks SQLite the same question with an index it creates on the
/// spot; the gap between the two is what a schema fixed in advance costs. Cobalt avoids the gap by maintaining the
/// indexes it needs as it writes.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class AdHocQueryBenchmark {
    private static final int RESULTS = 50;

    /// The engine, by [EngineKind#id()].
    @Param({"mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds.
    @Param({"200000"})
    public int messages;

    private Stores.Store store;
    private int expected;

    /// Opens a copy of the loaded store and counts the answer it must give.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        var corpus = new Corpus(messages);
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
        var starred = 0;
        for (var index = 0; index < corpus.messages(); index++) {
            if (Corpus.starred(corpus.body(corpus.chat(index), corpus.sequence(index)))) {
                starred++;
            }
        }
        expected = Math.min(RESULTS, starred);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Scans every message for the newest starred ones.
    ///
    /// @return the keys of the newest starred messages, newest last
    /// @throws IOException if the engine fails to read
    @Benchmark
    public PriorityQueue<byte[]> newestStarred() throws IOException {
        var newest = new PriorityQueue<byte[]>(RESULTS + 1, (left, right) ->
                Long.compare(ByteBuffer.wrap(left, 4, 8).getLong(), ByteBuffer.wrap(right, 4, 8).getLong()));
        store.engine().scan(Keyspace.MESSAGES, Corpus.chatStart(0), new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
                false, Integer.MAX_VALUE, (key, value) -> {
                    if (Corpus.starred(value)) {
                        newest.add(key.clone());
                        if (newest.size() > RESULTS) {
                            newest.poll();
                        }
                    }
                    return true;
                });
        if (newest.size() != expected) {
            throw new IllegalStateException("Found " + newest.size() + " starred messages, expected " + expected);
        }
        return newest;
    }
}
