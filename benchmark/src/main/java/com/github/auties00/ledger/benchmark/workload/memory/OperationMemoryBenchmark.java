package com.github.auties00.ledger.benchmark.workload.memory;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Arrivals;
import com.github.auties00.ledger.benchmark.support.Directories;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.PeakCounters;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/// The memory the heavy operations of a client's store take while they run: the peak, against what the process held
/// before and what is left once they are done.
///
/// Each operation opens its store itself, so [PeakCounters] measures its floor with no store open (the JVM, the
/// corpus and the operation's input), and records what is left with the store still open:
///
/// - [#ingest(IngestState, PeakCounters)] stores a whole history sync of `messages` messages into an empty store, in
///   batches of `batchSize` messages, the last one durable;
/// - [#checkpointUnderLoad(LoadedState, PeakCounters)] opens a copy of a store holding `messages` messages, lets four
///   writers store received messages durably, ten per batch, until they have stored a tenth as many, then checkpoints
///   (see [Engine#checkpoint()]) while they go on, and stops them once the checkpoint is done;
/// - [#applySnapshot(SnapshotState, PeakCounters)] applies an app-state snapshot of `entries` index entries to an empty
///   store, as one durable batch;
/// - [#recoverAfterCrash(RecoveryState, PeakCounters, Blackhole)] opens a store whose last writer crashed after
///   storing a quarter as many received messages again ([CrashingWriter]), and reads what the first screen needs;
/// - [#largeValues(LargeValueState, PeakCounters, Blackhole)] stores 128 MiB of values of `valueSize` bytes durably,
///   one per batch, and reads them back.
///
/// The heap peak is that of the collections the JVM ran during the operation (see `support.MemoryWatch`), an upper
/// bound of the live heap at those moments. Their time is not the point.
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_GROWING, Jvm.HEAP_MAX})
public class OperationMemoryBenchmark {
    private static final int LOAD_WRITERS = 4;
    private static final int LOAD_MESSAGES_PER_BATCH = 10;
    private static final long LARGE_VALUE_BYTES = 128L << 20;

    /// Stores a whole history into an empty store.
    ///
    /// @param state the corpus
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open or write
    @Benchmark
    public void ingest(IngestState state, PeakCounters counters) throws IOException {
        counters.begin();
        state.store = Stores.empty(EngineKind.of(state.engine), Stores.settingsFor(state.corpus), state.sealed);
        Stores.ingest(state.store.engine(), state.corpus, state.batchSize);
        counters.end();
    }

    /// Checkpoints a loaded store while writers store received messages.
    ///
    /// @param state the copy of the loaded store
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open, write or checkpoint
    /// @throws InterruptedException if interrupted while waiting for the writers
    @Benchmark
    public void checkpointUnderLoad(LoadedState state, PeakCounters counters) throws IOException, InterruptedException {
        counters.begin();
        var engine = state.open();
        var written = new AtomicLong();
        var stopped = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var writers = new ArrayList<Thread>();
        for (var writer = 0; writer < LOAD_WRITERS; writer++) {
            var arrivals = new Arrivals(state.corpus, (long) writer << 40, writer);
            writers.add(Thread.ofVirtual().start(() -> {
                var batch = new WriteBatch();
                try {
                    while (!stopped.get()) {
                        batch.clear();
                        for (var i = 0; i < LOAD_MESSAGES_PER_BATCH; i++) {
                            arrivals.receive(batch);
                        }
                        engine.write(batch, Durability.DURABLE);
                        written.addAndGet(LOAD_MESSAGES_PER_BATCH);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                    stopped.set(true);
                }
            }));
        }
        try {
            var target = Math.max(LOAD_MESSAGES_PER_BATCH, state.messages / 10);
            while (written.get() < target && !stopped.get()) {
                Thread.sleep(1);
            }
            if (failure.get() == null) {
                engine.checkpoint();
            }
        } finally {
            stopped.set(true);
            for (var writer : writers) {
                writer.join();
            }
        }
        if (failure.get() != null) {
            throw new IOException("A writer failed", failure.get());
        }
        counters.end();
    }

    /// Applies one app-state snapshot to an empty store.
    ///
    /// @param state the snapshot
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open or write
    @Benchmark
    public void applySnapshot(SnapshotState state, PeakCounters counters) throws IOException {
        counters.begin();
        state.store = Stores.empty(EngineKind.of(state.engine), EngineSettings.DEFAULT, state.sealed);
        state.store.engine().write(state.snapshot, Durability.DURABLE);
        counters.end();
    }

    /// Opens a store its last writer left without closing it and reads what the first screen needs.
    ///
    /// @param state the crashed store
    /// @param counters receives the memory
    /// @param blackhole consumes what is read
    /// @throws IOException if the engine fails to open or read
    @Benchmark
    public void recoverAfterCrash(RecoveryState state, PeakCounters counters, Blackhole blackhole) throws IOException {
        counters.begin();
        var engine = state.open();
        for (var session = 0; session < 100; session++) {
            blackhole.consume(engine.get(Keyspace.SESSIONS, Corpus.sessionKey(session)));
        }
        var page = engine.scan(Keyspace.MESSAGES, Corpus.chatStart(0), Corpus.chatEnd(0), true, 50, (key, value) -> {
            blackhole.consume(value);
            return true;
        });
        counters.end();
        if (page == 0) {
            throw new IllegalStateException("The recovered store returned no messages");
        }
    }

    /// Stores 128 MiB of large values durably and reads them back.
    ///
    /// @param state the value size
    /// @param counters receives the memory
    /// @param blackhole consumes what is read
    /// @throws IOException if the engine fails to open, write or read, or a value is missing
    @Benchmark
    public void largeValues(LargeValueState state, PeakCounters counters, Blackhole blackhole) throws IOException {
        counters.begin();
        var values = state.values();
        var settings = EngineSettings.DEFAULT.withExpectedBytes(LARGE_VALUE_BYTES * 4);
        state.store = Stores.empty(EngineKind.of(state.engine), settings, false);
        var engine = state.store.engine();
        for (var value = 0; value < values; value++) {
            engine.write(new WriteBatch().put(Keyspace.GENERIC, Corpus.longKey(value), state.corpus.slice(value, state.valueSize)), Durability.DURABLE);
        }
        for (var value = 0; value < values; value++) {
            var read = engine.get(Keyspace.GENERIC, Corpus.longKey(value));
            if (read == null || read.length != state.valueSize) {
                throw new IllegalStateException("Value " + value + " is missing");
            }
            blackhole.consume(read);
        }
        counters.end();
    }

    /// The corpus of a history sync and the store it goes into.
    @State(Scope.Benchmark)
    public static class IngestState {
        /// The engine, by [EngineKind#id()].
        @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The messages stored.
        @Param({"200000"})
        public int messages;

        /// The messages per batch.
        @Param({"10000"})
        public int batchSize;

        Corpus corpus;
        Stores.Store store;

        /// Generates the corpus.
        @Setup(Level.Trial)
        public void generate() {
            corpus = new Corpus(messages);
        }

        /// Closes the iteration's store and deletes it.
        ///
        /// @throws IOException if the engine fails to close
        @TearDown(Level.Iteration)
        public void close() throws IOException {
            if (store != null) {
                store.close();
                store = null;
            }
        }
    }

    /// A copy of a loaded store, opened by the operation.
    @State(Scope.Benchmark)
    public static class LoadedState {
        /// The engine, by [EngineKind#id()].
        @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The messages the store holds.
        @Param({"200000"})
        public int messages;

        Corpus corpus;
        Path directory;
        Engine opened;

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

        Engine open() throws IOException {
            opened = Stores.openAt(EngineKind.of(engine), directory, Stores.settingsFor(corpus), sealed);
            return opened;
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
    }

    /// A copy of a loaded store whose last writer stored a quarter as many messages again and crashed.
    @State(Scope.Benchmark)
    public static class RecoveryState {
        /// The engine, by [EngineKind#id()].
        @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The messages the store holds before the crashed writer's.
        @Param({"200000"})
        public int messages;

        Corpus corpus;
        Path directory;
        Engine opened;

        /// Generates the corpus.
        @Setup(Level.Trial)
        public void generate() {
            corpus = new Corpus(messages);
        }

        /// Copies the loaded store and has a [CrashingWriter] process write to it and halt.
        ///
        /// @throws IOException if the store cannot be built or copied, or the writer fails
        /// @throws InterruptedException if interrupted while waiting for the writer
        @Setup(Level.Iteration)
        public void crash() throws IOException, InterruptedException {
            directory = Stores.copyOf(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
            var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            var process = new ProcessBuilder(java, Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MAX, "-cp", System.getProperty("java.class.path"),
                    CrashingWriter.class.getName(), engine, directory.toString(), String.valueOf(sealed),
                    String.valueOf(messages), String.valueOf(Math.max(100, messages / 4)))
                    .redirectErrorStream(true)
                    .start();
            var output = new StringBuilder();
            try (var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append('\n');
                }
            }
            if (process.waitFor() != 0 || output.indexOf("written ") < 0) {
                throw new IOException("The crashing writer failed with " + process.exitValue() + ":\n" + output);
            }
        }

        Engine open() throws IOException {
            opened = Stores.openAt(EngineKind.of(engine), directory, Stores.settingsFor(corpus), sealed);
            return opened;
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
    }

    /// An app-state snapshot and the store it goes into.
    @State(Scope.Benchmark)
    public static class SnapshotState {
        /// The engine, by [EngineKind#id()].
        @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
        public String engine;

        /// Whether values are sealed with AES-GCM before they reach the engine.
        @Param({"true"})
        public boolean sealed;

        /// The index entries of the snapshot: 32-byte MAC keys, 100 to 300 byte values.
        @Param({"100000"})
        public int entries;

        WriteBatch snapshot;
        Stores.Store store;

        /// Builds the snapshot.
        @Setup(Level.Trial)
        public void build() {
            var corpus = new Corpus(1);
            snapshot = new WriteBatch();
            for (var entry = 0L; entry < entries; entry++) {
                snapshot.put(Keyspace.APP_STATE, Corpus.hashedKey(entry, 32), corpus.slice(entry, 100 + (int) Math.floorMod(entry * 31, 201)));
            }
        }

        /// Closes the iteration's store and deletes it.
        ///
        /// @throws IOException if the engine fails to close
        @TearDown(Level.Iteration)
        public void close() throws IOException {
            if (store != null) {
                store.close();
                store = null;
            }
        }
    }

    /// The size of the large values and the store they go into.
    @State(Scope.Benchmark)
    public static class LargeValueState {
        /// The engine, by [EngineKind#id()].
        @Param({"ledger", "ledger-plain", "mvstore", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
        public String engine;

        /// The size of each value: Ledger writes one below 1 MiB to its log, then into its tree at the next
        /// checkpoint, and one of 1 MiB or more once, to a file of its own.
        @Param({"262144", "16777216"})
        public int valueSize;

        Corpus corpus;
        Stores.Store store;

        /// Generates the pool the values come from.
        @Setup(Level.Trial)
        public void generate() {
            corpus = new Corpus(1);
        }

        int values() {
            return (int) Math.max(1, LARGE_VALUE_BYTES / valueSize);
        }

        /// Closes the iteration's store and deletes it.
        ///
        /// @throws IOException if the engine fails to close
        @TearDown(Level.Iteration)
        public void close() throws IOException {
            if (store != null) {
                store.close();
                store = null;
            }
        }
    }
}
