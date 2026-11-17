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

/// The same lookups from platform threads and from virtual threads, to tell what the kind of the calling thread costs
/// a read.
///
/// An invocation splits its lookups evenly over `threads` callers: platform threads of a pool made once, or virtual
/// threads made for the invocation.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class CallerThreadsReadBenchmark {
    /// The lookups of one invocation.
    public static final int LOOKUPS = 16_384;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger"})
    public String engine;

    /// The kind of the calling threads: `platform` or `virtual`.
    @Param({"platform", "virtual"})
    public String callers;

    /// The calling threads.
    @Param({"8"})
    public int threads;

    /// The messages the store holds.
    @Param({"200000"})
    public int messages;

    private Stores.Store store;
    private Corpus corpus;
    private ExecutorService platform;

    /// Opens a copy of the loaded store.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(messages);
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), true, corpus);
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

    /// Reads [#LOOKUPS] messages by id.
    ///
    /// @throws Exception if a read fails
    @Benchmark
    @OperationsPerInvocation(LOOKUPS)
    public void findById() throws Exception {
        var each = LOOKUPS / threads;
        Callable<Void> work = () -> {
            var random = ThreadLocalRandom.current();
            for (var i = 0; i < each; i++) {
                var index = random.nextInt(corpus.messages());
                var chat = corpus.chat(index);
                var key = store.engine().get(Keyspace.MESSAGE_IDS, corpus.idKey(chat, corpus.sequence(index)));
                var body = key == null ? null : store.engine().get(Keyspace.MESSAGES, key);
                if (body == null) {
                    throw new IllegalStateException("Message " + index + " of the corpus is missing");
                }
            }
            return null;
        };
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
