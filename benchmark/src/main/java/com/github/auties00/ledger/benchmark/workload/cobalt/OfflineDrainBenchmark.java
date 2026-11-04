package com.github.auties00.ledger.benchmark.workload.cobalt;

import com.github.auties00.ledger.benchmark.data.Corpus;
import com.github.auties00.ledger.benchmark.engine.Durability;
import com.github.auties00.ledger.benchmark.engine.EngineKind;
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
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/// Receiving the messages queued while the client was offline, one at a time.
///
/// Each operation is one received message: its body, its id index entry, the sender's Signal session and the chat's
/// metadata, written as one durable batch, which must complete before the client acknowledges the message. The store
/// already holds a history of `messages` messages, and the chat of each new message follows the history's activity.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class OfflineDrainBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds before the first operation.
    @Param({"200000"})
    public int messages;

    private Corpus corpus;
    private Stores.Store store;
    private SplittableRandom random;
    private long received;

    /// Opens a copy of the loaded store.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(messages);
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
        random = new SplittableRandom(7);
    }

    /// Closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    @TearDown(Level.Trial)
    public void close() throws IOException {
        store.close();
    }

    /// Stores one received message durably.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void receiveMessage() throws IOException {
        var template = random.nextInt(corpus.messages());
        var chat = corpus.chat(template);
        var number = received++;
        var id = corpus.id(chat, (int) (number % 0xFFFFFF));
        var key = ByteBuffer.allocate(Corpus.MESSAGE_KEY_LENGTH)
                .putInt(chat)
                .putLong(Corpus.LATEST + number)
                .put(id)
                .array();
        var idKey = ByteBuffer.allocate(4 + Corpus.ID_LENGTH)
                .putInt(chat)
                .put(id)
                .array();
        var batch = new WriteBatch()
                .put(Keyspace.MESSAGES, key, corpus.body(chat, corpus.sequence(template)))
                .put(Keyspace.MESSAGE_IDS, idKey, key)
                .put(Keyspace.SESSIONS, Corpus.sessionKey(random.nextInt(Stores.SESSIONS)), corpus.slice(number, Stores.SESSION_BYTES))
                .put(Keyspace.CHATS, Corpus.chatStart(chat), corpus.slice(~number, Stores.CHAT_BYTES));
        store.engine().write(batch, Durability.DURABLE);
    }
}
