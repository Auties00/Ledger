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

/// Receiving the messages queued while the client was offline, with the chat updated from what it holds.
///
/// Each operation is one received message, written as one durable batch: its body, its id index entry and the
/// sender's Signal session, put blind, and the chat's metadata read and rebuilt, its unread count incremented and its
/// last message timestamp advanced, which a store must not lose to a concurrent update of the same chat. `computed`
/// rebuilds the chat inside the batch, through the engine's read-modify-write (Ledger's `compute`, or a lock per key
/// over a read and the write for the others); `blind` puts the chat without reading it, as [OfflineDrainBenchmark]
/// does, which is what the read-modify-write costs against. The store already holds a history of `messages` messages.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ChatUpdateBenchmark {
    /// The engine, by [EngineKind#id()].
    @Param({"ledger", "ledger-plain", "ledger-exclusive", "mvstore-encrypted", "rocksdb", "sqlite"})
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

    /// Stores one received message durably, rebuilding its chat from what the store holds.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void computed() throws IOException {
        var number = received++;
        var chat = next(number);
        var timestamp = Corpus.LATEST + number;
        store.engine().computeIn(batch(chat, number), Keyspace.CHATS, Corpus.chatStart(chat),
                current -> updated(current, timestamp, number), Durability.DURABLE);
    }

    /// Stores one received message durably, putting its chat without reading it.
    ///
    /// @throws IOException if the engine fails to write
    @Benchmark
    public void blind() throws IOException {
        var number = received++;
        var chat = next(number);
        var batch = batch(chat, number).put(Keyspace.CHATS, Corpus.chatStart(chat), updated(null, Corpus.LATEST + number, number));
        store.engine().write(batch, Durability.DURABLE);
    }

    // The chat of the next message, which follows the history's activity
    private int next(long number) {
        return corpus.chat(random.nextInt(corpus.messages()));
    }

    private WriteBatch batch(int chat, long number) {
        var template = random.nextInt(corpus.messages());
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
        return new WriteBatch()
                .put(Keyspace.MESSAGES, key, corpus.body(chat, corpus.sequence(template)))
                .put(Keyspace.MESSAGE_IDS, idKey, key)
                .put(Keyspace.SESSIONS, Corpus.sessionKey(random.nextInt(Stores.SESSIONS)), corpus.slice(number, Stores.SESSION_BYTES));
    }

    /// {@return the chat `current` holds, or a new one, with one more unread message and `timestamp` as its last}
    ///
    /// The unread count is the chat's first four bytes and the timestamp the next eight, the rest of its
    /// [Stores#CHAT_BYTES] as they were.
    private byte[] updated(byte[] current, long timestamp, long number) {
        var chat = current == null || current.length < 12 ? corpus.slice(~number, Stores.CHAT_BYTES) : current.clone();
        var buffer = ByteBuffer.wrap(chat);
        var unread = current == null || current.length < 12 ? 0 : buffer.getInt(0);
        buffer.putInt(0, unread + 1).putLong(4, timestamp);
        return chat;
    }
}
