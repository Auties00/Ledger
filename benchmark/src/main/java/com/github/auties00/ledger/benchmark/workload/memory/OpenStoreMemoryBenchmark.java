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
import com.github.auties00.ledger.benchmark.support.StoreCostCounters;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/// What an open store costs the process before it holds any data to speak of: a client that keeps several sessions,
/// one store each, pays it once per session.
///
/// Each iteration first creates `stores` stores and closes them: `empty` ones, or `tiny` ones holding 100 received
/// messages. [#open(StoreCostCounters)] then opens all of them at once and uses each as a client does right after
/// it starts (one session read, the newest page of a chat, one received message stored durably), and records the
/// memory the process holds against what it held before (`heapBytesFloor`, the JVM and the corpus), in total and
/// per store. Its time is not the point.
///
/// Each store gets the shared settings of every workload, its cache limit included, sized for 16 MiB of data.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1)
@Measurement(iterations = 3)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_GROWING, Jvm.HEAP_MAX})
public class OpenStoreMemoryBenchmark {
    private static final EngineSettings SETTINGS = EngineSettings.DEFAULT.withExpectedBytes(16L << 20);
    private static final int TINY_MESSAGES = 100;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The stores open at once.
    @Param({"1", "8"})
    public int stores;

    /// What each store holds: `empty`, or `tiny` for 100 received messages.
    @Param({"empty", "tiny"})
    public String contents;

    private Corpus corpus;
    private final List<Path> directories = new ArrayList<>();
    private final List<Engine> opened = new ArrayList<>();

    /// Generates the corpus the messages come from.
    @Setup(Level.Trial)
    public void generate() {
        corpus = new Corpus(20_000);
    }

    /// Creates the iteration's stores and closes them.
    ///
    /// @throws IOException if a store cannot be created or written
    @Setup(Level.Iteration)
    public void create() throws IOException {
        var tiny = switch (contents) {
            case "empty" -> false;
            case "tiny" -> true;
            default -> throw new IllegalArgumentException("contents must be empty or tiny: " + contents);
        };
        for (var store = 0; store < stores; store++) {
            var created = Stores.empty(EngineKind.of(engine), SETTINGS, sealed);
            directories.add(created.directory());
            try (var engine = created.engine()) {
                if (tiny) {
                    var arrivals = new Arrivals(corpus, (long) store << 32, store);
                    var batch = new WriteBatch();
                    for (var message = 0; message < TINY_MESSAGES; message++) {
                        arrivals.receive(batch);
                    }
                    engine.write(batch, Durability.DURABLE);
                }
            }
        }
    }

    /// Closes the iteration's stores and deletes them.
    ///
    /// @throws IOException if an engine fails to close or a directory cannot be deleted
    @TearDown(Level.Iteration)
    public void close() throws IOException {
        try {
            for (var engine : opened) {
                engine.close();
            }
        } finally {
            opened.clear();
            for (var directory : directories) {
                Directories.delete(directory);
            }
            directories.clear();
        }
    }

    /// Opens every store, uses each once and records what they hold.
    ///
    /// @param counters receives the memory
    /// @param blackhole consumes what is read
    /// @throws IOException if an engine fails to open, read or write
    @Benchmark
    public void open(StoreCostCounters counters, Blackhole blackhole) throws IOException {
        counters.begin();
        for (var store = 0; store < directories.size(); store++) {
            var engine = Stores.openAt(EngineKind.of(this.engine), directories.get(store), SETTINGS, sealed);
            opened.add(engine);
            blackhole.consume(engine.get(Keyspace.SESSIONS, Corpus.sessionKey(store)));
            engine.scan(Keyspace.MESSAGES, Corpus.chatStart(0), Corpus.chatEnd(0), true, 50, (key, value) -> {
                blackhole.consume(value);
                return true;
            });
            engine.write(new Arrivals(corpus, ((long) store << 32) + TINY_MESSAGES, ~store).receive(new WriteBatch()), Durability.DURABLE);
        }
        counters.end(directories.size());
    }
}
