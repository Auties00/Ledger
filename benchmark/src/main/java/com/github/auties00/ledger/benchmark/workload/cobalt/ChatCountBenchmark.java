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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/// Counting the messages of a chat of about 5,000 messages in a history of `messages`, as a client shows a chat's
/// message count or sums it over its chats.
///
/// Ledger counts a range from the counts its tree's branches keep, reading the nodes on the paths to the range's
/// bounds, and looks up each key of the range written since the last checkpoint; the other engines scan the range.
/// `recent` messages are written to the chat, without waiting for stable storage, after the store opens and before
/// the first count, so they are in memory and not yet in the trees. `cold` reopens the store before each count, so
/// the count reads the files rather than the cache; a reopen of Ledger checkpoints the recent messages as it closes,
/// so only `warm` measures them.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ChatCountBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "mvstore-encrypted", "rocksdb"})
    public String engine;

    /// The messages the store holds before the first count.
    @Param({"200000"})
    public int messages;

    /// The messages written to the counted chat since the last checkpoint.
    @Param({"0", "10000"})
    public int recent;

    /// Whether the counts find the store's pages cached (`warm`) or reopen it first (`cold`).
    @Param({"warm", "cold"})
    public String cache;

    private EngineKind kind;
    private EngineSettings settings;
    private Corpus corpus;
    private Stores.Store store;
    private com.github.auties00.ledger.benchmark.engine.Engine engineInstance;
    private int chat;
    private long expected;

    /// Opens a copy of the loaded store and writes the recent messages.
    ///
    /// @throws IOException if the store cannot be built, opened or written
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(messages);
        kind = EngineKind.of(engine);
        settings = Stores.settingsFor(corpus);
        store = Stores.loaded(kind, settings, false, corpus);
        engineInstance = store.engine();
        chat = 0;
        for (var candidate = 0; candidate < corpus.chats(); candidate++) {
            if (Math.abs(corpus.chatSize(candidate) - 5_000) < Math.abs(corpus.chatSize(chat) - 5_000)) {
                chat = candidate;
            }
        }
        var batch = new WriteBatch();
        for (var i = 0; i < recent; i++) {
            var id = corpus.id(chat, 0x800000 + i);
            var key = ByteBuffer.allocate(Corpus.MESSAGE_KEY_LENGTH).putInt(chat).putLong(Corpus.LATEST + i).put(id).array();
            batch.put(Keyspace.MESSAGES, key, corpus.slice(i, 200));
            if (batch.size() == 1_000) {
                engineInstance.write(batch, Durability.DEFERRED);
                batch = new WriteBatch();
            }
        }
        if (batch.size() > 0) {
            engineInstance.write(batch, Durability.DEFERRED);
        }
        expected = corpus.chatSize(chat) + recent;
    }

    /// Reopens the store before each count of a `cold` run.
    ///
    /// @throws IOException if the engine fails to close or open
    @Setup(Level.Invocation)
    public void reopen() throws IOException {
        if (cache.equals("cold")) {
            engineInstance.close();
            engineInstance = Stores.openAt(kind, store.directory(), settings, false);
        }
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        // The engine open now, which a cold run reopened over the store's directory
        new Stores.Store(engineInstance, store.directory()).close();
    }

    /// Counts the messages of the chat.
    ///
    /// @return the count
    /// @throws IOException if the engine fails to read
    @Benchmark
    public long countChat() throws IOException {
        var count = engineInstance.count(Keyspace.MESSAGES, Corpus.chatStart(chat), Corpus.chatEnd(chat));
        if (count != expected) {
            throw new IllegalStateException("Counted " + count + " messages, expected " + expected);
        }
        return count;
    }
}
