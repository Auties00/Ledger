package com.github.auties00.ledger.benchmark.workload.cobalt;

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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/// Applying app-state (syncd) data: a collection snapshot, then the patches that follow it.
///
/// [#applySnapshot(SnapshotState)] writes one snapshot of `entries` index entries (32-byte MAC keys, 100 to 300
/// byte values) into an empty store as one atomic, durable batch. [#applyPatch(PatchState)] applies one patch of 50
/// mutations, half removing old entries and half adding new ones, to a store holding `entries` entries.
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class AppStateSyncBenchmark {
    private static final int PATCH = 50;

    /// Adds `count` index entries, numbered from `first`, to `batch`.
    private static void entries(Corpus corpus, WriteBatch batch, long first, int count) {
        for (var entry = first; entry < first + count; entry++) {
            batch.put(Keyspace.APP_STATE, Corpus.hashedKey(entry, 32), corpus.slice(entry, 100 + (int) Math.floorMod(entry * 31, 201)));
        }
    }

    /// Applies one snapshot to an empty store.
    ///
    /// @param state the store and the snapshot
    /// @throws IOException if the engine fails to write
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 2)
    @Measurement(iterations = 5)
    public void applySnapshot(SnapshotState state) throws IOException {
        state.store.engine().write(state.snapshot, Durability.DURABLE);
    }

    /// Applies one patch durably.
    ///
    /// @param state the store
    /// @throws IOException if the engine fails to write
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 2, time = 5)
    @Measurement(iterations = 3, time = 10)
    public void applyPatch(PatchState state) throws IOException {
        var batch = new WriteBatch();
        for (var i = 0; i < PATCH / 2; i++) {
            batch.delete(Keyspace.APP_STATE, Corpus.hashedKey(state.oldest++, 32));
        }
        entries(state.corpus, batch, state.newest, PATCH / 2);
        state.newest += PATCH / 2;
        state.store.engine().write(batch, Durability.DURABLE);
    }

    /// An empty store and the snapshot to apply to it.
    @State(Scope.Benchmark)
    public static class SnapshotState {
        /// The engine, by [EngineKind#id()].
        @Param({"mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The index entries of the snapshot.
        @Param({"20000"})
        public int entries;

        Stores.Store store;
        WriteBatch snapshot;

        /// Opens an empty store and builds the snapshot.
        ///
        /// @throws IOException if the engine fails to open
        @Setup(Level.Iteration)
        public void open() throws IOException {
            store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, sealed);
            snapshot = new WriteBatch();
            entries(new Corpus(1), snapshot, 0, entries);
        }

        /// Closes the store and deletes it.
        ///
        /// @throws IOException if the engine fails to close
        @TearDown(Level.Iteration)
        public void close() throws IOException {
            store.close();
        }
    }

    /// A store holding `entries` index entries.
    @State(Scope.Benchmark)
    public static class PatchState {
        /// The engine, by [EngineKind#id()].
        @Param({"mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je", "floor"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The index entries the store holds.
        @Param({"20000"})
        public int entries;

        Corpus corpus;
        Stores.Store store;
        long oldest;
        long newest;

        /// Opens an empty store and applies a snapshot of `entries` entries.
        ///
        /// @throws IOException if the engine fails to open or write
        @Setup(Level.Trial)
        public void open() throws IOException {
            corpus = new Corpus(1);
            store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, sealed);
            var snapshot = new WriteBatch();
            entries(corpus, snapshot, 0, entries);
            store.engine().write(snapshot, Durability.DURABLE);
            oldest = 0;
            newest = entries;
        }

        /// Closes the store and deletes it.
        ///
        /// @throws IOException if the engine fails to close
        @TearDown(Level.Trial)
        public void close() throws IOException {
            store.close();
        }
    }
}
