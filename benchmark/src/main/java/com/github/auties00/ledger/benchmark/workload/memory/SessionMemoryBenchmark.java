package com.github.auties00.ledger.benchmark.workload.memory;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.Engine;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/// The memory a client's store holds once it has been in use for a while: the steady state of a session over a large
/// history.
///
/// [#session(PeakCounters)] opens a copy of a store holding `messages` messages and runs one session over it: the
/// startup reads (the first 100 sessions, the newest page of the most active chat), the chat list (every chat and
/// session), 1,000 pages of chats picked by activity, 1,000 messages found by id, and 2,000 received messages stored
/// durably ten at a time. It then records what the open store holds after a full collection (`heapBytes`,
/// `residentBytes`) next to what the process held before it opened (`heapBytesFloor`, the JVM and the corpus) and the
/// peaks of the session. Its time is not the point.
///
/// The session's writes are a fixed 2,000 messages whatever the history's size, so the two sizes show how the steady
/// state grows with the history alone.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 0)
@Measurement(iterations = 1)
@Fork(value = 3, warmups = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_GROWING, Jvm.HEAP_MAX})
public class SessionMemoryBenchmark {
    private static final int RECEIVED = 2_000;
    private static final int RECEIVED_PER_BATCH = 10;

    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds.
    @Param({"200000", "1000000"})
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

    /// Opens the store, runs one session over it and records the memory the open store holds.
    ///
    /// @param counters receives the memory
    /// @throws IOException if the engine fails to open, read or write, or a read finds less than the store holds
    @Benchmark
    public void session(PeakCounters counters) throws IOException {
        counters.begin();
        var engine = open();
        var read = new long[1];
        Engine.EntryVisitor visitor = (key, value) -> {
            read[0] += value.length;
            return true;
        };
        for (var session = 0; session < 100; session++) {
            read[0] += engine.get(Keyspace.SESSIONS, Corpus.sessionKey(session)).length;
        }
        engine.scan(Keyspace.MESSAGES, Corpus.chatStart(0), Corpus.chatEnd(0), true, 50, visitor);
        var chats = engine.scan(Keyspace.CHATS, Corpus.chatStart(0), Corpus.chatEnd(corpus.chats() - 1), false, Integer.MAX_VALUE, visitor);
        // Every session's address starts with the same digit
        var sessions = engine.scan(Keyspace.SESSIONS, new byte[]{'3'}, new byte[]{'4'}, false, Integer.MAX_VALUE, visitor);
        if (chats != corpus.chats() || sessions != Stores.SESSIONS) {
            throw new IllegalStateException("The store returned " + chats + " chats and " + sessions + " sessions");
        }
        var random = new SplittableRandom(11);
        for (var page = 0; page < 1_000; page++) {
            var chat = corpus.chat(random.nextInt(corpus.messages()));
            engine.scan(Keyspace.MESSAGES, Corpus.chatStart(chat), Corpus.chatEnd(chat), true, 50, visitor);
        }
        for (var lookup = 0; lookup < 1_000; lookup++) {
            var index = random.nextInt(corpus.messages());
            var key = engine.get(Keyspace.MESSAGE_IDS, corpus.idKey(corpus.chat(index), corpus.sequence(index)));
            var body = key == null ? null : engine.get(Keyspace.MESSAGES, key);
            if (body == null) {
                throw new IllegalStateException("Message " + index + " of the corpus is missing");
            }
            read[0] += body.length;
        }
        var arrivals = new Arrivals(corpus, 0, 7);
        var batch = new WriteBatch();
        for (var received = 0; received < RECEIVED; received += RECEIVED_PER_BATCH) {
            batch.clear();
            for (var i = 0; i < RECEIVED_PER_BATCH; i++) {
                arrivals.receive(batch);
            }
            engine.write(batch, Durability.DURABLE);
        }
        batch.clear();
        counters.end();
        if (read[0] == 0) {
            throw new IllegalStateException("The store returned no data");
        }
    }
}
