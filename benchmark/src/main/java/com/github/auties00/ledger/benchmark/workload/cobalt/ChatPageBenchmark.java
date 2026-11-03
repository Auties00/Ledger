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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/// Opening a chat and scrolling back through it.
///
/// [#newestPage(Blackhole)] reads the 50 newest messages of a chat, [#olderPage(Blackhole)] the 50 messages before a
/// random point of a chat's history. Chats are picked in proportion to their activity. With `backgroundWrites` above
/// zero, a virtual thread receives that many messages per second while the pages are read, each written durably as
/// [OfflineDrainBenchmark] writes them.
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 10)
@Fork(value = 1, jvmArgsAppend = {Jvm.NATIVE_ACCESS, Jvm.UNSAFE, Jvm.HEAP_MIN, Jvm.HEAP_MAX})
public class ChatPageBenchmark {
    private static final int PAGE = 50;

    /// The engine, by [EngineKind#id()].
    @Param({"mvstore", "mvstore-encrypted", "rocksdb", "lmdb", "sqlite", "berkeley-je"})
    public String engine;

    /// Whether values are sealed with AES-GCM before they reach the engine.
    @Param({"true"})
    public boolean sealed;

    /// The messages the store holds.
    @Param({"200000"})
    public int messages;

    /// The messages received per second while the pages are read.
    @Param({"0", "100"})
    public int backgroundWrites;

    private Corpus corpus;
    private Stores.Store store;
    private Thread writer;
    private volatile boolean writing;

    /// Opens a copy of the loaded store and starts the background writer.
    ///
    /// @throws IOException if the store cannot be built or opened
    @Setup(Level.Trial)
    public void open() throws IOException {
        corpus = new Corpus(messages);
        store = Stores.loaded(EngineKind.of(engine), Stores.settingsFor(corpus), sealed, corpus);
        if (backgroundWrites > 0) {
            writing = true;
            writer = Thread.ofVirtual().name("background-writer").start(this::receive);
        }
    }

    private void receive() {
        var interval = TimeUnit.SECONDS.toNanos(1) / backgroundWrites;
        var next = System.nanoTime();
        var number = 0L;
        while (writing) {
            var template = ThreadLocalRandom.current().nextInt(corpus.messages());
            var chat = corpus.chat(template);
            var id = corpus.id(chat, (int) (number % 0xFFFFFF));
            var key = ByteBuffer.allocate(Corpus.MESSAGE_KEY_LENGTH).putInt(chat).putLong(Corpus.LATEST + number).put(id).array();
            var batch = new WriteBatch()
                    .put(Keyspace.MESSAGES, key, corpus.body(chat, corpus.sequence(template)))
                    .put(Keyspace.MESSAGE_IDS, ByteBuffer.allocate(4 + Corpus.ID_LENGTH).putInt(chat).put(id).array(), key)
                    .put(Keyspace.CHATS, Corpus.chatStart(chat), corpus.slice(number, Stores.CHAT_BYTES));
            try {
                store.engine().write(batch, Durability.DURABLE);
            } catch (IOException exception) {
                System.err.println("background writer failed: " + exception);
                return;
            }
            number++;
            next += interval;
            var wait = next - System.nanoTime();
            if (wait > 0) {
                LockSupport.parkNanos(wait);
            }
        }
    }

    /// Stops the background writer, closes the store and deletes it.
    ///
    /// @throws IOException if the engine fails to close
    /// @throws InterruptedException if interrupted while waiting for the writer
    @TearDown(Level.Trial)
    public void close() throws IOException, InterruptedException {
        writing = false;
        if (writer != null) {
            writer.join();
        }
        store.close();
    }

    /// Reads the newest page of a chat.
    ///
    /// @param blackhole consumes the messages
    /// @throws IOException if the engine fails to read
    @Benchmark
    public void newestPage(Blackhole blackhole) throws IOException {
        var chat = corpus.chat(ThreadLocalRandom.current().nextInt(corpus.messages()));
        store.engine().scan(Keyspace.MESSAGES, Corpus.chatStart(chat), Corpus.chatEnd(chat), true, PAGE, (key, value) -> {
            blackhole.consume(value);
            return true;
        });
    }

    /// Reads the page before a random message of a chat.
    ///
    /// @param blackhole consumes the messages
    /// @throws IOException if the engine fails to read
    @Benchmark
    public void olderPage(Blackhole blackhole) throws IOException {
        var index = ThreadLocalRandom.current().nextInt(corpus.messages());
        var chat = corpus.chat(index);
        var before = corpus.messageKey(chat, corpus.sequence(index));
        store.engine().scan(Keyspace.MESSAGES, Corpus.chatStart(chat), before, true, PAGE, (key, value) -> {
            blackhole.consume(value);
            return true;
        });
    }
}
