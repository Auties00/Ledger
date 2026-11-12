package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Stores whose commits have frame bodies on chunks of the process's pool, which the log hands back as it seals them
/// while committers take the same chunks for the ropes their batches and values live on: the values read before and
/// after a checkpoint and after a power loss, commits taken back, and a log that stops with such frames queued.
///
/// The force of the log is held while committers queue frames behind it, so the frames the flush then seals give their
/// chunks back while the next batches take them; with assertions on, a released chunk is overwritten with a pattern
/// first, which a frame or a value still reading it would show.
class FramePoolStressTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // A resident key space whose decoder refuses a value starting with 0x7F, which takes its batch back as it applies
    private static final Keyspace<String, String> STRICT = Keyspace.resident("strict", KeyCodec.utf8(), ValueCodec.of(
            value -> value.getBytes(StandardCharsets.UTF_8).length,
            (value, target) -> MemorySegment.copy(MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)), 0, target, 0, target.byteSize()),
            source -> {
                if (source.byteSize() > 0 && source.get(ValueLayout.JAVA_BYTE, 0) == 0x7F) {
                    throw new IllegalArgumentException("refused");
                }
                return new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
            }));
    private static final int THREADS = 6;
    private static final int LARGE_COMMITS = 8;
    // Writes of a large commit, whose body of about 330 KiB spans two chunks and whose batch is on a rope
    private static final int LARGE_WRITES = 130;

    private static Ledger open(Path directory, Encryption encryption) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).keyspace(CHATS)
                .keyspace(STRICT).commitWindow(Duration.ZERO))
                .segmentSize(1L << 20)
                .build();
    }

    /// {@return the value of write `write` of commit `commit` of committer `thread`, about 2.5 KiB}
    private static String value(int thread, int commit, int write) {
        var seed = thread * 1_000_003L + commit * 7_919L + write;
        var random = new Random(seed);
        var builder = new StringBuilder(2_600);
        builder.append(thread).append('/').append(commit).append('/').append(write).append(':');
        var length = 2_000 + random.nextInt(1_000);
        for (var i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }

    private static String key(int thread, int write) {
        return "t%d-%04d".formatted(thread, write);
    }

    /// Checks that the process's pool holds no chunk free twice.
    private static void assertNoChunkFreeTwice() {
        var free = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
        for (var array : HeapPool.process().freeForTests(FrameBody.CHUNK_BYTES)) {
            assertTrue(free.add(array), "a chunk is free twice");
        }
    }

    /// Checks that `ledger` holds `expected` in [#NAMES] and each committer's last chat.
    private static void assertHolds(Ledger ledger, Map<String, String> expected, Map<String, String> chats) {
        var names = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            names.put(key, value);
            return true;
        });
        assertEquals(new TreeMap<>(expected).keySet(), names.keySet());
        for (var entry : expected.entrySet()) {
            if (!entry.getValue().equals(names.get(entry.getKey()))) {
                fail("the value of " + entry.getKey() + " differs");
            }
        }
        for (var entry : chats.entrySet()) {
            assertEquals(entry.getValue(), ledger.get(CHATS, entry.getKey()).orElseThrow());
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("keeps every value while frames queued behind a held force give their chunks back to the next batches")
    void queuedFrames(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var blocked = new AtomicBoolean();
        var forcing = new CountDownLatch(1);
        var released = new CountDownLatch(1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && blocked.get()) {
                forcing.countDown();
                try {
                    released.await();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        var expected = new ConcurrentHashMap<String, String>();
        var chats = new ConcurrentHashMap<String, String>();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var ledger = open(disk.getPath("/store"), encryption);
        try {
            ledger.commit(ledger.newBatch().put(NAMES, "warm", "x")).awaitDurable();
            expected.put("warm", "x");
            blocked.set(true);
            ledger.commit(ledger.newBatch().put(NAMES, "first", "blocked"));
            expected.put("first", "blocked");
            assertTrue(forcing.await(10, TimeUnit.SECONDS), "the first group never reached its force");
            // Two phases: frames queue behind the held force, then the flush seals them while the committers go on
            var queued = new CountDownLatch(THREADS);
            var go = new CountDownLatch(1);
            var committers = new ArrayList<Thread>();
            for (var t = 0; t < THREADS; t++) {
                var thread = t;
                committers.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var commit = 0; commit < 2 * LARGE_COMMITS; commit++) {
                            if (commit == LARGE_COMMITS) {
                                queued.countDown();
                                go.await();
                            }
                            var batch = ledger.newBatch();
                            // Later commits overwrite earlier ones' keys, or write new ones
                            var first = commit * (LARGE_WRITES / 2);
                            var values = new String[LARGE_WRITES];
                            for (var write = 0; write < LARGE_WRITES; write++) {
                                values[write] = value(thread, commit, write);
                                batch.put(NAMES, key(thread, first + write), values[write]);
                            }
                            batch.put(CHATS, "chat" + thread, "commit " + commit);
                            ledger.commit(batch);
                            for (var write = 0; write < LARGE_WRITES; write++) {
                                expected.put(key(thread, first + write), values[write]);
                            }
                            chats.put("chat" + thread, "commit " + commit);
                            // A small commit beside, whose frame a run takes
                            ledger.commit(ledger.newBatch().put(NAMES, "small" + thread, "v" + commit));
                            expected.put("small" + thread, "v" + commit);
                            // The values just written, and some of an earlier commit, on the arrays they live on
                            var random = new Random(thread * 31L + commit);
                            for (var check = 0; check < 20; check++) {
                                var write = random.nextInt(LARGE_WRITES);
                                assertEquals(values[write], ledger.get(NAMES, key(thread, first + write)).orElseThrow());
                                var older = random.nextInt(first + LARGE_WRITES);
                                assertEquals(expected.get(key(thread, older)), ledger.get(NAMES, key(thread, older)).orElseThrow());
                            }
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                        queued.countDown();
                    }
                }));
            }
            assertTrue(queued.await(1, TimeUnit.MINUTES), "the committers never queued their first frames");
            blocked.set(false);
            released.countDown();
            go.countDown();
            // A checkpoint while they go on, which takes the values from the arrays they live on
            ledger.checkpoint();
            for (var committer : committers) {
                committer.join();
            }
            if (!failures.isEmpty()) {
                var failure = new AssertionError(failures.size() + " committers failed");
                failures.forEach(failure::addSuppressed);
                throw failure;
            }
            ledger.awaitDurable();
            assertNoChunkFreeTwice();
            assertHolds(ledger, expected, chats);
            ledger.checkpoint();
            assertHolds(ledger, expected, chats);
            // Written after the last checkpoint, so recovery replays them from the log
            for (var t = 0; t < THREADS; t++) {
                var batch = ledger.newBatch();
                for (var write = 0; write < LARGE_WRITES; write++) {
                    var value = value(t, 100, write);
                    batch.put(NAMES, key(t, write), value);
                    expected.put(key(t, write), value);
                }
                ledger.commit(batch);
            }
            ledger.awaitDurable();
            assertNoChunkFreeTwice();
        } finally {
            released.countDown();
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        var survivor = disk.afterPowerLoss(new Random(encrypted ? 3 : 4));
        try (var reopened = open(survivor.getPath("/store"), encryption)) {
            assertHolds(reopened, expected, chats);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a commit taken back gives its frame's chunks back once, and the store goes on")
    void takenBack(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        try (var ledger = open(disk.getPath("/store"), encryption)) {
            var expected = new TreeMap<String, String>();
            for (var round = 0; round < 4; round++) {
                var commit = round;
                // A transaction of about 1.5 MiB of frame that throws
                assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                    for (var write = 0; write < 600; write++) {
                        transaction.put(NAMES, key(9, write), value(9, commit, write));
                    }
                    throw new IllegalStateException("abort");
                }));
                assertNoChunkFreeTwice();
                // A batch that runs alone, for its range removal, and whose last write's decoder throws
                var batch = ledger.newBatch().deleteRange(NAMES, KeyRange.between("a", "b"));
                for (var write = 0; write < 300; write++) {
                    batch.put(NAMES, key(8, write), value(8, commit, write));
                }
                batch.putEncoded(STRICT, MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8)), MemorySegment.ofArray(new byte[]{0x7F}));
                assertThrows(IllegalArgumentException.class, () -> ledger.commit(batch));
                assertNoChunkFreeTwice();
                // A transaction and a batch of the same size that commit
                ledger.update(transaction -> {
                    for (var write = 0; write < 300; write++) {
                        transaction.put(NAMES, key(7, write), value(7, commit, write));
                    }
                });
                var committed = ledger.newBatch();
                for (var write = 0; write < 300; write++) {
                    committed.put(NAMES, key(6, write), value(6, commit, write));
                    expected.put(key(6, write), value(6, commit, write));
                    expected.put(key(7, write), value(7, commit, write));
                }
                ledger.commit(committed).awaitDurable();
                assertNoChunkFreeTwice();
            }
            assertEquals(List.of(), ledger.range(NAMES, KeyRange.between(key(8, 0), key(9, 9_999)), 10));
            assertHolds(ledger, expected, Map.of());
        }
        try (var reopened = open(disk.afterPowerLoss(new Random(5)).getPath("/store"), encryption)) {
            assertEquals(0, reopened.size(STRICT));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("recovers a state between the last durable commit and the last visible one when the log stops with frames on chunks queued")
    void stoppedWithQueuedFrames(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var dry = CrashFileSystem.create();
        new Workload(encryption).run(dry.getPath("/store"));
        var forces = dry.forces();
        for (var force = 0; force <= forces; force += Math.max(1, forces / 12)) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            var workload = new Workload(encryption);
            workload.run(disk.getPath("/store"));
            assertNoChunkFreeTwice();
            var survivor = force % 2 == 0 ? disk.afterPowerLoss(new Random(force)) : disk.afterProcessCrash();
            try (var ledger = open(survivor.getPath("/store"), encryption)) {
                var recovered = new TreeMap<String, String>();
                ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
                    recovered.put(key, value);
                    return true;
                });
                var match = false;
                for (var commit = workload.durable; commit <= workload.visible && !match; commit++) {
                    match = workload.history.get(commit).equals(recovered);
                }
                if (!match) {
                    fail("At force " + force + ", the store recovered a state no commit from " + workload.durable + " to "
                            + workload.visible + " left, of " + recovered.size() + " keys");
                }
                ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
            }
        }
    }

    /// Commits of frames on chunks that do not wait for durability, a commit window apart so they queue, then one that
    /// waits; and the state of [#NAMES] after each commit.
    private static final class Workload {
        private final Encryption encryption;
        final List<TreeMap<String, String>> history;
        int visible;
        int durable;

        Workload(Encryption encryption) {
            this.encryption = encryption;
            this.history = new ArrayList<>();
            history.add(new TreeMap<>());
        }

        /// Runs until it ends or the store fails, which a stopped file system makes it do.
        void run(Path directory) {
            Ledger ledger = null;
            try {
                ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES)
                        .keyspace(CHATS).keyspace(STRICT).commitWindow(Duration.ofMillis(2)))
                        .segmentSize(1L << 20)
                        .build();
                var state = new TreeMap<String, String>();
                for (var round = 0; round < 3; round++) {
                    for (var commit = 0; commit < 4; commit++) {
                        var batch = ledger.newBatch();
                        for (var write = 0; write < LARGE_WRITES; write++) {
                            var key = key(round, commit * 40 + write);
                            var value = value(round, commit, write);
                            batch.put(NAMES, key, value);
                            state.put(key, value);
                        }
                        var pending = ledger.commit(batch);
                        visible++;
                        history.add(new TreeMap<>(state));
                        if (commit == 3) {
                            pending.awaitDurable();
                            durable = visible;
                        }
                    }
                }
                ledger.close();
                durable = visible;
            } catch (RuntimeException | InterruptedException failure) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }
    }
}
