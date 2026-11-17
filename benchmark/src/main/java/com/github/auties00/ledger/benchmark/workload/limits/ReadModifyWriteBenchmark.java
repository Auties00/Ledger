package com.github.auties00.ledger.benchmark.workload.limits;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
import com.github.auties00.ledger.benchmark.engine.EngineSettings;
import com.github.auties00.ledger.benchmark.engine.Keyspace;
import com.github.auties00.ledger.benchmark.engine.WriteBatch;
import com.github.auties00.ledger.benchmark.support.Jvm;
import com.github.auties00.ledger.benchmark.support.Stores;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/// Read-modify-writes of counters from 1 to 256 virtual threads: each operation reads a 128-byte value, increments the
/// counter in its first eight bytes and writes it back, so that no increment is lost.
///
/// `keys` picks what the threads contend for: one hot key, 64 keys, or a million keys drawn from a Zipf distribution
/// or uniformly, which the threads rarely share. `residency` puts them in a key space Ledger holds in memory or in one
/// it pages. Each invocation runs [#OPERATIONS] operations, split over `threads` virtual threads, none of them waiting
/// for stable storage.
///
/// `compute` is the engine's read-modify-write: Ledger's `compute`, which runs beside the other commits and runs the
/// function again in order when another write of the key came first, `ledger-exclusive`'s transaction, which runs
/// alone, and for `ledger-striped` and the other engines a lock of 256 per key over a read and a write, which every
/// writer of the key must take. `getThenPut` reads and writes without either, which loses increments, and `put` writes blind: they are what a
/// compute costs against. The `functionRuns` counter is the function's runs over the iteration, which over the
/// operations is its runs per operation.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ReadModifyWriteBenchmark {
    /// The operations of one invocation.
    public static final int OPERATIONS = 1_024;
    private static final int VALUE_BYTES = 128;
    private static final int MANY_KEYS = 1 << 20;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-exclusive", "ledger-striped", "mvstore"})
    public String engine;

    /// The keys the operations draw from: `hot`, `64`, `zipf` or `uniform`, the last two over a million keys.
    @Param({"hot", "64", "zipf", "uniform"})
    public String keys;

    /// The virtual threads; divides [#OPERATIONS].
    @Param({"1", "8", "64", "256"})
    public int threads;

    /// Whether the key space is held in memory (`resident`) or paged.
    @Param({"resident", "paged"})
    public String residency;

    private Stores.Store store;
    private Keyspace keyspace;
    private Corpus corpus;
    private double[] zipf;

    /// The function's runs, summed over an iteration.
    @State(Scope.Thread)
    @AuxCounters(AuxCounters.Type.EVENTS)
    public static class Runs {
        /// The runs of the compute functions in the iteration.
        public long functionRuns;
    }

    /// Opens an empty store.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        if (OPERATIONS % threads != 0) {
            throw new IllegalArgumentException("threads must divide " + OPERATIONS + ": " + threads);
        }
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, false);
        // Ledger holds the chats in memory and pages the rest
        keyspace = residency.equals("resident") ? Keyspace.CHATS : Keyspace.GENERIC;
        if (keys.equals("zipf")) {
            zipf = new double[MANY_KEYS];
            var total = 0.0;
            for (var i = 0; i < MANY_KEYS; i++) {
                total += 1.0 / (i + 1);
                zipf[i] = total;
            }
            for (var i = 0; i < MANY_KEYS; i++) {
                zipf[i] /= total;
            }
        }
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    private byte[] key() {
        var random = ThreadLocalRandom.current();
        var index = switch (keys) {
            case "hot" -> 0;
            case "64" -> random.nextInt(64);
            case "zipf" -> {
                var found = Arrays.binarySearch(zipf, random.nextDouble());
                yield Math.min(MANY_KEYS - 1, found < 0 ? -found - 1 : found);
            }
            default -> random.nextInt(MANY_KEYS);
        };
        return Corpus.hashedKey(index, 16);
    }

    private byte[] incremented(byte[] value) {
        var next = value == null ? corpus.slice(0, VALUE_BYTES) : value.clone();
        var buffer = ByteBuffer.wrap(next);
        buffer.putLong(0, value == null ? 1 : buffer.getLong(0) + 1);
        return next;
    }

    /// Runs `operation` [#OPERATIONS] times over `threads` virtual threads and waits for all of them.
    private void run(Operation operation) throws Exception {
        var perThread = OPERATIONS / threads;
        if (threads == 1) {
            for (var i = 0; i < perThread; i++) {
                operation.run();
            }
            return;
        }
        var results = new ArrayList<Future<?>>(threads);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var thread = 0; thread < threads; thread++) {
                results.add(executor.submit(() -> {
                    for (var i = 0; i < perThread; i++) {
                        operation.run();
                    }
                    return null;
                }));
            }
        }
        for (var result : results) {
            result.get();
        }
    }

    @FunctionalInterface
    private interface Operation {
        void run() throws IOException;
    }

    /// Increments a counter through the engine's read-modify-write.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void compute(Runs runs) throws Exception {
        var counted = new LongAdder();
        run(() -> store.engine().compute(keyspace, key(), value -> {
            counted.increment();
            return incremented(value);
        }, Durability.DEFERRED));
        runs.functionRuns += counted.sum();
    }

    /// Increments a counter with a read and a write that nothing holds together.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void getThenPut() throws Exception {
        run(() -> {
            var key = key();
            store.engine().write(new WriteBatch().put(keyspace, key, incremented(store.engine().get(keyspace, key))), Durability.DEFERRED);
        });
    }

    /// Writes a value without reading it.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(OPERATIONS)
    public void put() throws Exception {
        run(() -> store.engine().write(new WriteBatch().put(keyspace, key(), incremented(null)), Durability.DEFERRED));
    }
}
