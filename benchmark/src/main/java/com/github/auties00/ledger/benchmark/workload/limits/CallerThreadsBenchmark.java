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
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/// The same writes from platform threads and from virtual threads, to tell what the kind of the calling thread costs.
///
/// An invocation splits its operations evenly over `threads` callers: platform threads of a pool made once, or
/// virtual threads made for the invocation, as an application on virtual threads makes them.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class CallerThreadsBenchmark {
    /// The durable writes of one invocation.
    public static final int DURABLE = 512;
    /// The deferred writes of one invocation.
    public static final int DEFERRED = 32_768;
    private static final int KEYS = 1 << 20;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger"})
    public String engine;

    /// The kind of the calling threads: `platform` or `virtual`.
    @Param({"platform", "virtual"})
    public String callers;

    /// The calling threads.
    @Param({"1", "8"})
    public int threads;

    private Stores.Store store;
    private Corpus corpus;
    private ExecutorService platform;
    private final AtomicLong sequence = new AtomicLong();

    /// Opens an empty store.
    ///
    /// @throws IOException if the engine fails to open
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(1);
        store = Stores.empty(EngineKind.of(engine), EngineSettings.DEFAULT, true);
        platform = callers.equals("platform") ? Executors.newFixedThreadPool(threads) : null;
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        if (platform != null) {
            platform.shutdown();
        }
        store.close();
    }

    /// Runs [#DURABLE] durable 1 KB writes.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(DURABLE)
    public void durable() throws Exception {
        var each = DURABLE / threads;
        run(() -> {
            for (var i = 0; i < each; i++) {
                var id = sequence.getAndIncrement();
                var batch = new WriteBatch().put(Keyspace.INBOX, Corpus.longKey(id % 65_536), corpus.slice(id, 1_024));
                store.engine().write(batch, Durability.DURABLE);
            }
            return null;
        });
    }

    /// Runs [#DEFERRED] writes of 128 bytes under random keys, none waiting for stable storage.
    ///
    /// @throws Exception if a write fails
    @Benchmark
    @OperationsPerInvocation(DEFERRED)
    public void deferred() throws Exception {
        var each = DEFERRED / threads;
        run(() -> {
            var random = ThreadLocalRandom.current();
            for (var i = 0; i < each; i++) {
                var key = random.nextInt(KEYS);
                store.engine().write(new WriteBatch().put(Keyspace.GENERIC, Corpus.hashedKey(key, 16), corpus.slice(key, 128)), Durability.DEFERRED);
            }
            return null;
        });
    }

    private void run(Callable<Void> work) throws Exception {
        var results = new ArrayList<Future<Void>>(threads);
        if (platform != null) {
            for (var thread = 0; thread < threads; thread++) {
                results.add(platform.submit(work));
            }
        } else {
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (var thread = 0; thread < threads; thread++) {
                    results.add(executor.submit(work));
                }
            }
        }
        for (var result : results) {
            result.get();
        }
    }
}
