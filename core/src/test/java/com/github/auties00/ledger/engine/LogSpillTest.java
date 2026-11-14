package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The paged values that leave the heap for the log once the queue of a store holds more than its budget
/// ([SpillQueue]): every read returns them as written, through every path a read takes, before and after a checkpoint
/// and a reopen; a damaged log is reported; the segments they are in stay while a reader of an older tree may need
/// them, and a reader of a newer tree reads them from it once they retire; and a store that crashes recovers, moving
/// the values it replays off the heap the same way. The stores run with a budget of 0, so every value of a durable
/// commit moves, a cold slab, so the values of small batches lie on arrays of their own commit, and, but for the tests
/// of that verdict, key spaces whose values may leave the heap from their first commit rather than once samples of
/// thousands of puts show that few are replaced; a key space that removes what it writes lists no value at all.
class LogSpillTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Below LOG_VALUE_MIN, at it, and well above, in a frame of runs and in one of its own
    private static final int[] LENGTHS = {40, 200, SpillQueue.LOG_VALUE_MIN, 700, 1_500, 5_000};

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        return open(directory, encrypted, budget, Duration.ZERO);
    }

    private static LiveLedger open(Path directory, boolean encrypted, long budget, Duration window) {
        var ledger = openJudged(directory, encrypted, budget, window);
        ledger.state(MESSAGES).overwrites.forceSpillsForTests();
        ledger.state(TEXTS).overwrites.forceSpillsForTests();
        return ledger;
    }

    /// Opens a store whose key spaces let their values leave the heap only once their samples show few are replaced.
    private static LiveLedger openJudged(Path directory, boolean encrypted, long budget, Duration window) {
        return openJudged(directory, encrypted, budget, window, 64 << 10);
    }

    /// Opens a store as [#open(Path, boolean, long, Duration)] does, with segments of `segmentSize` bytes.
    private static LiveLedger openSized(Path directory, boolean encrypted, long budget, Duration window, long segmentSize) {
        var ledger = openJudged(directory, encrypted, budget, window, segmentSize);
        ledger.state(MESSAGES).overwrites.forceSpillsForTests();
        ledger.state(TEXTS).overwrites.forceSpillsForTests();
        return ledger;
    }

    private static LiveLedger openJudged(Path directory, boolean encrypted, long budget, Duration window, long segmentSize) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(window))
                .segmentSize(segmentSize)
                .spillBudget(budget)
                // Values of a group written through stay on the heap, and whether a group goes through depends on
                // whether a spare segment is ready; writtenThroughValuesStay covers them
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    /// {@return whether `value` is the value `logged` holds the tag of, of the version stamped `stamp`}
    private static boolean verifies(SpillQueue.Crypto crypto, byte[] value, long stamp, LogValue logged) {
        return crypto.verifies(value, 0, value.length, stamp, logged);
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static byte[] value(int message, int generation) {
        var length = LENGTHS[Math.floorMod(message * 7 + generation, LENGTHS.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i);
        }
        return value;
    }

    private static String text(int message, int generation) {
        return "t" + message + "/" + generation + "-" + "x".repeat(LENGTHS[Math.floorMod(message + generation, LENGTHS.length)]);
    }

    /// What a store should hold, by key space.
    private static final class Expected {
        final TreeMap<Integer, byte[]> messages = new TreeMap<>();
        final TreeMap<String, String> texts = new TreeMap<>();
        final TreeMap<String, String> chats = new TreeMap<>();

        Expected copy() {
            var copy = new Expected();
            copy.messages.putAll(messages);
            copy.texts.putAll(texts);
            copy.chats.putAll(chats);
            return copy;
        }
    }

    /// Commits `count` messages from `first`, in one batch, and records them in `expected`.
    private static long write(Ledger ledger, Expected expected, int first, int count, int generation, boolean durable) throws Exception {
        var batch = ledger.newBatch();
        for (var message = first; message < first + count; message++) {
            var value = value(message, generation);
            batch.put(MESSAGES, key(message), value);
            expected.messages.put(message, value);
            var text = text(message, generation);
            batch.put(TEXTS, "t" + message, text);
            expected.texts.put("t" + message, text);
            batch.put(CHATS, "c" + message % 5, "chat " + message);
            expected.chats.put("c" + message % 5, "chat " + message);
        }
        var commit = ledger.commit(batch);
        if (durable) {
            commit.awaitDurable();
        }
        return commit.sequence();
    }

    /// Moves every value of the durable commits, on this thread and the queue's.
    private static void spillNow(LiveLedger ledger) throws InterruptedException {
        var spills = ledger.spills();
        spills.drain(ledger.log()::durableSequence);
        spills.awaitIdleForTests();
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        var chain = ledger.state(keyspace).chain(MemorySegment.ofArray(key));
        return chain == null ? null : chain.head;
    }

    /// Checks every way a read returns the values of `expected`.
    private static void verify(Ledger ledger, Expected expected) {
        verifyView(ledger, expected);
        for (var entry : expected.messages.entrySet()) {
            var read = ledger.readEncoded(MESSAGES, key(entry.getKey()), segment -> segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
            assertArrayEquals(entry.getValue(), read.orElseThrow(), "message " + entry.getKey() + " read encoded");
        }
        var visited = new TreeMap<String, String>();
        ledger.forEach(TEXTS, KeyRange.<String>all().reversed(), (key, value) -> {
            visited.put(key, value);
            return true;
        });
        assertEquals(expected.texts, visited, "texts scanned descending");
        assertEquals(expected.messages.size(), ledger.size(MESSAGES));
    }

    private static void verifyView(View view, Expected expected) {
        for (var entry : expected.messages.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, key(entry.getKey())).orElseThrow(), "message " + entry.getKey());
        }
        for (var entry : expected.texts.entrySet()) {
            assertEquals(entry.getValue(), view.get(TEXTS, entry.getKey()).orElseThrow(), "text " + entry.getKey());
        }
        for (var entry : expected.chats.entrySet()) {
            assertEquals(entry.getValue(), view.get(CHATS, entry.getKey()).orElseThrow(), "chat " + entry.getKey());
        }
        var messages = view.range(MESSAGES, KeyRange.all(), Integer.MAX_VALUE);
        assertEquals(expected.messages.size(), messages.size(), "messages in a range of " + view);
        var index = 0;
        for (var entry : expected.messages.entrySet()) {
            var found = messages.get(index++);
            assertArrayEquals(key(entry.getKey()), found.key());
            assertArrayEquals(entry.getValue(), found.value(), "message " + entry.getKey() + " in a range");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("values moved to the log, and to arrays of their own, read back as written, through a checkpoint and a reopen")
    void readsBack(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, 0)) {
            for (var batch = 0; batch < 6; batch++) {
                // Batches of 1 message fit a run, those of 30 take frames of their own
                write(ledger, expected, batch * 40, batch % 2 == 0 ? 1 : 30, 0, true);
            }
            var before = ledger.snapshot();
            spillNow(ledger);
            var spills = ledger.spills();
            assertTrue(spills.loggedValuesForTests() > 0, "no value moved to the log");
            assertTrue(spills.copiedValuesForTests() > 0, "no value moved to an array of its own");
            assertEquals(0, spills.heldForTests());
            for (var message : expected.messages.keySet()) {
                var source = head(ledger, MESSAGES, key(message)).sourceForTests();
                if (message / 40 % 2 == 0 && message % 40 == 0) {
                    // A commit of small values alone, made while the queue held nothing, lists none
                    var small = expected.messages.get(message).length < SpillQueue.LOG_VALUE_MIN
                            && text(message, 0).length() < SpillQueue.LOG_VALUE_MIN;
                    if (small) {
                        assertInstanceOf(MemorySegment.class, source, "message " + message);
                        continue;
                    }
                }
                if (expected.messages.get(message).length >= SpillQueue.LOG_VALUE_MIN) {
                    assertInstanceOf(LogValue.class, source, "message " + message);
                } else {
                    assertInstanceOf(byte[].class, source, "message " + message);
                }
            }
            // A resident value never moves
            assertFalse(head(ledger, CHATS, "c0".getBytes(StandardCharsets.UTF_8)).sourceForTests() instanceof LogValue);
            verify(ledger, expected);
            try (before) {
                verifyView(before, expected);
            }
            ledger.update(transaction -> {
                for (var entry : expected.messages.entrySet()) {
                    assertArrayEquals(entry.getValue(), transaction.get(MESSAGES, key(entry.getKey())).orElseThrow());
                }
            });
            ledger.checkpoint();
            verify(ledger, expected);
            // A key written again moves again
            write(ledger, expected, 0, 30, 1, true);
            spillNow(ledger);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("the values of commits the log wrote as runs of several commits read back from where each commit lies in the run")
    void readsBackFromRuns(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, 0, Duration.ofMillis(20))) {
            long last = 0;
            for (var message = 0; message < 60; message++) {
                last = write(ledger, expected, message, 1, 0, false);
            }
            ledger.awaitDurable();
            assertTrue(ledger.log().durableSequence() >= last);
            spillNow(ledger);
            assertTrue(ledger.spills().loggedValuesForTests() > 0);
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a value whose bytes in the log changed is reported, never returned")
    void reportsDamage(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, 0)) {
            write(ledger, expected, 0, 30, 0, true);
            spillNow(ledger);
            var damaged = -1;
            LogValue logged = null;
            for (var message : expected.messages.keySet()) {
                if (head(ledger, MESSAGES, key(message)).sourceForTests() instanceof LogValue found) {
                    damaged = message;
                    logged = found;
                    break;
                }
            }
            assertTrue(damaged >= 0);
            var position = logged.frameOffset() + LogWriter.FRAME_HEADER + logged.bodyIndex() + 3;
            try (var channel = FileChannel.open(logged.segment().path(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                var one = ByteBuffer.allocate(1);
                channel.read(one, position);
                one.put(0, (byte) (one.get(0) ^ 0x5A)).rewind();
                channel.write(one, position);
            }
            var message = damaged;
            assertThrows(LedgerCorruptedException.class, () -> ledger.get(MESSAGES, key(message)));
            // The others read on
            for (var other : expected.messages.keySet()) {
                if (other != message) {
                    assertArrayEquals(expected.messages.get(other), ledger.get(MESSAGES, key(other)).orElseThrow());
                }
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("values read back in turn from several segments into a caller's array allocate nothing per value, as a checkpoint reads them")
    void readsBackInPlace(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, 0)) {
            // Segments of 64 KiB: the values of 1,800 messages fill several
            for (var batch = 0; batch < 60; batch++) {
                write(ledger, expected, batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            var logged = new ArrayList<Version>();
            var lengths = new ArrayList<byte[]>();
            var segments = new java.util.HashSet<Long>();
            for (var entry : expected.messages.entrySet()) {
                var version = head(ledger, MESSAGES, key(entry.getKey()));
                if (version.sourceForTests() instanceof LogValue value) {
                    logged.add(version);
                    lengths.add(entry.getValue());
                    segments.add(value.segment().number());
                }
            }
            assertTrue(segments.size() >= 3, "values in " + segments.size() + " segments");
            // The first half and the second in turn, as a checkpoint's key order interleaves the segments
            var order = new ArrayList<Integer>();
            var half = logged.size() / 2;
            for (var i = 0; i < half; i++) {
                order.add(i);
                order.add(half + i);
            }
            var target = new byte[8_192];
            var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            var perValue = 0L;
            for (var round = 0; round < 4; round++) {
                var before = threads.getCurrentThreadAllocatedBytes();
                for (var index : order) {
                    logged.get(index).readLogged(target, 16);
                }
                perValue = (threads.getCurrentThreadAllocatedBytes() - before) / order.size();
            }
            for (var index : order) {
                var value = lengths.get(index);
                logged.get(index).readLogged(target, 16);
                assertArrayEquals(value, Arrays.copyOfRange(target, 16, 16 + value.length));
            }
            // Before, each read wrapped its array in a buffer and, switching segments, initialized a key schedule
            assertTrue(perValue < 32, "a value read back allocated " + perValue + " bytes");
        }
    }

    @Test
    @DisplayName("the segments of moved values stay while a snapshot of the tree before the checkpoint reads them, and retire once it closes")
    void keepsSegmentsForOlderTrees(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, 0)) {
            for (var batch = 0; batch < 8; batch++) {
                write(ledger, expected, batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            var held = ledger.spills().segmentsForTests();
            assertTrue(held.length > 1, "the values moved to " + held.length + " segments");
            var snapshot = ledger.snapshot();
            var seen = expected.copy();
            write(ledger, expected, 1_000, 30, 0, true);
            ledger.checkpoint();
            // The snapshot reads the tree before the checkpoint, and the values of the segments it made obsolete
            var kept = ledger.spills().segmentsForTests();
            assertTrue(kept.length > 0 && kept[0] == held[0], "the checkpoint retired a segment a snapshot needs");
            assertTrue(Files.exists(segmentPath(directory, held[0])));
            verifyView(snapshot, seen);
            verify(ledger, expected);
            snapshot.close();
            // A tick retires them, as no checkpoint follows
            ledger.checkpointer().tick();
            assertTrue(Arrays.stream(ledger.spills().segmentsForTests()).noneMatch(number -> number == held[0]));
            assertFalse(Files.exists(segmentPath(directory, held[0])));
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true, 0)) {
            verify(ledger, expected);
        }
    }

    private static Path segmentPath(Path directory, long number) {
        return directory.resolve("log").resolve(LogFiles.segmentName(number));
    }

    @Test
    @DisplayName("a snapshot from before the moved values holds no segment back, and reads at the latest commit find those values in the tree once their segments retire")
    void readsRetiredValuesFromTheTree(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, 0)) {
            // A resident value alone, which never moves. A value of the snapshot's commit that moved to the log would
            // make the snapshot a reader of its segment, which keeps that segment and every later one, and whether it
            // moves depends on whether the device takes the commit's group written through
            var first = ledger.newBatch();
            first.put(CHATS, "c-old", "before the moved values");
            ledger.commit(first).awaitDurable();
            expected.chats.put("c-old", "before the moved values");
            var old = ledger.snapshot();
            var seen = expected.copy();
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            write(ledger, expected, 2_000, 30, 0, true);
            ledger.checkpoint();
            // The snapshot keeps the versions in memory, so reads find a version whose segment is gone
            var retired = 0;
            for (var message = 0; message < 180; message++) {
                var version = head(ledger, MESSAGES, key(message));
                if (version != null && version.sourceForTests() instanceof LogValue logged && logged.segment().retired()) {
                    retired++;
                }
            }
            assertTrue(retired > 0, "no version in memory has its value in a retired segment");
            verify(ledger, expected);
            ledger.update(transaction -> {
                for (var entry : expected.texts.entrySet()) {
                    assertEquals(entry.getValue(), transaction.get(TEXTS, entry.getKey()).orElseThrow());
                }
            });
            try (var later = ledger.snapshot()) {
                verifyView(later, expected);
            }
            verifyView(old, seen);
            // A checkpoint with the versions still in memory writes their keys as the tree holds them
            write(ledger, expected, 3_000, 5, 0, true);
            ledger.checkpoint();
            verify(ledger, expected);
            old.close();
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true, 0)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a reader whose interrupt closes the channel of a segment reads its value, keeps its interrupt, and leaves the segment readable")
    void survivesInterrupts(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, 0)) {
            write(ledger, expected, 0, 30, 0, true);
            spillNow(ledger);
            var failure = new AtomicReference<Throwable>();
            var reader = Thread.ofPlatform().start(() -> {
                try {
                    for (var round = 0; round < 3; round++) {
                        Thread.currentThread().interrupt();
                        for (var entry : expected.messages.entrySet()) {
                            assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
                        }
                        assertTrue(Thread.interrupted(), "the read lost the interrupt");
                    }
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            reader.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            verify(ledger, expected);
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("readers of every kind read the right values while writers move them to the log and checkpoints retire their segments")
    void readersRaceRetirement(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true, 0)) {
            var written = new AtomicInteger();
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var readers = new ArrayList<Thread>();
            for (var thread = 0; thread < 4; thread++) {
                var seed = thread;
                readers.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    try {
                        while (!stop.get() && failure.get() == null) {
                            var count = written.get();
                            if (count == 0) {
                                Thread.onSpinWait();
                                continue;
                            }
                            switch (random.nextInt(3)) {
                                case 0 -> {
                                    var message = random.nextInt(count);
                                    // Written once, in generation 0, before written counted it
                                    assertArrayEquals(value(message, 0), ledger.get(MESSAGES, key(message)).orElseThrow());
                                }
                                case 1 -> {
                                    var from = random.nextInt(count);
                                    var entries = ledger.range(MESSAGES, KeyRange.atLeast(key(from)), 20);
                                    for (var entry : entries) {
                                        var message = ByteBuffer.wrap(entry.key()).getInt();
                                        assertArrayEquals(value(message, 0), entry.value());
                                    }
                                }
                                default -> {
                                    try (var snapshot = ledger.snapshot()) {
                                        var message = random.nextInt(count);
                                        assertArrayEquals(value(message, 0), snapshot.get(MESSAGES, key(message)).orElseThrow());
                                        Thread.sleep(random.nextInt(3));
                                        assertEquals(text(message, 0), snapshot.get(TEXTS, "t" + message).orElseThrow());
                                    }
                                }
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            var expected = new Expected();
            try {
                for (var batch = 0; batch < 120 && failure.get() == null; batch++) {
                    write(ledger, expected, batch * 20, 20, 0, batch % 3 == 0);
                    written.set((batch + 1) * 20);
                    if (batch % 15 == 14) {
                        ledger.checkpoint();
                    }
                }
            } finally {
                stop.set(true);
                for (var reader : readers) {
                    reader.join();
                }
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            ledger.awaitDurable();
            spillNow(ledger);
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a recovery moves the values it replays off the heap, those too short for the log onto chunks, reads them back, and checkpoints them")
    void recoveryMovesReplayedValues(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, batch * 40, batch % 2 == 0 ? 1 : 30, 0, true);
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            assertTrue(ledger.spills().loggedValuesForTests() > 0, "the recovery moved no value to the log");
            for (var entry : expected.messages.entrySet()) {
                var source = head(ledger, MESSAGES, key(entry.getKey())).sourceForTests();
                if (entry.getValue().length >= SpillQueue.LOG_VALUE_MIN) {
                    assertInstanceOf(LogValue.class, source, "message " + entry.getKey());
                } else {
                    var chunk = assertInstanceOf(MemorySegment.class, source, "message " + entry.getKey());
                    assertEquals(ReplayValues.CHUNK_BYTES, chunk.byteSize(), "message " + entry.getKey() + " stayed on its body");
                }
            }
            verify(ledger, expected);
            write(ledger, expected, 5_000, 10, 0, true);
            var logStart = ledger.log().currentSegment();
            ledger.checkpoint();
            verify(ledger, expected);
            assertTrue(Arrays.stream(ledger.spills().segmentsForTests()).allMatch(number -> number >= logStart),
                    "a segment of the recovery stayed");
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a recovery moves to the log the values it replays onto arrays of their own, too long to share a chunk")
    void recoveryMovesOwnReplayedValues(boolean encrypted, @TempDir Path directory) throws Exception {
        var lengths = new int[]{ReplayValues.SHARED_BYTES + 1, 20_000, 100_000};
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            for (var message = 0; message < lengths.length * 2; message++) {
                var value = new byte[lengths[message % lengths.length]];
                for (var i = 0; i < value.length; i++) {
                    value[i] = (byte) (message * 13 + i);
                }
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            for (var entry : expected.entrySet()) {
                var source = head(ledger, MESSAGES, key(entry.getKey())).sourceForTests();
                assertInstanceOf(LogValue.class, source, "message " + entry.getKey() + " stayed in the heap");
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
            }
            ledger.checkpoint();
            for (var entry : expected.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
            }
        }
        try (var ledger = open(directory, encrypted, 0)) {
            for (var entry : expected.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("a key space that removes each value 64 commits after writing it lists none, so the arrays of removed values go to the collector")
    void removalsListNoValues(@TempDir Path directory) throws Exception {
        // The shape of a webhook inbox: one delivery of 1 to 2 KB a commit, removed 64 deliveries later
        var inFlight = 64;
        var homes = new ArrayList<WeakReference<Object>>();
        try (var ledger = openJudged(directory, true, 64L << 20, Duration.ZERO)) {
            var spills = ledger.spills();
            var overwrites = ledger.state(MESSAGES).overwrites;
            // Past the 64 sampled puts, about 4,096 of them, of the first judgement
            for (var message = 0; message < 5_000; message++) {
                var value = new byte[1_024 + message % 1_024];
                Arrays.fill(value, (byte) message);
                var commit = ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
                if (message % 512 == 0) {
                    commit.awaitDurable();
                }
                if (message < 200) {
                    var source = head(ledger, MESSAGES, key(message)).sourceForTests();
                    assertInstanceOf(MemorySegment.class, source, "the value of message " + message + " is on an array of its commit");
                    homes.add(new WeakReference<>(source));
                }
                if (message >= inFlight) {
                    ledger.commit(ledger.newBatch().delete(MESSAGES, key(message - inFlight)));
                }
                assertEquals(0, spills.heldForTests(), "a commit listed values at message " + message);
            }
            ledger.awaitDurable();
            assertFalse(overwrites.worthwhile(), "the samples were never judged");
            assertFalse(overwrites.spills());
            assertEquals(0, spills.loggedValuesForTests() + spills.copiedValuesForTests());
            var retained = homes.size();
            for (var attempt = 0; attempt < 20 && retained != 0; attempt++) {
                System.gc();
                Thread.sleep(20);
                retained = (int) homes.stream().filter(home -> home.get() != null).count();
            }
            assertEquals(0, retained, "arrays of removed values are still reachable");
        }
    }

    @Test
    @DisplayName("a key space that keeps what it writes lets its values leave the heap once its samples are judged")
    void keptValuesSpillOnceJudged(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = openJudged(directory, true, 0, Duration.ZERO)) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            assertFalse(overwrites.spills(), "the values may leave the heap before any sample");
            var batches = 0;
            // A batch of 64 messages judges its key spaces whenever its commit is sampled, one in 64
            while (!overwrites.spills()) {
                assertTrue(batches < 1_000, "the samples of values written once never let them leave the heap");
                write(ledger, expected, batches * 64, 64, 0, false);
                batches++;
            }
            assertEquals(0, ledger.spills().loggedValuesForTests() + ledger.spills().copiedValuesForTests());
            write(ledger, expected, batches * 64, 64, 0, true);
            spillNow(ledger);
            assertTrue(ledger.spills().loggedValuesForTests() > 0, "no value moved to the log once judged");
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("tagging a value, decrypting it back and checking its tag allocate the tag's place and the value read back, never a copy of the value per step")
    void cryptoAllocatesOnlyWhatItKeeps(boolean encrypted) {
        var queue = new SpillQueue(0, encrypted);
        var crypto = queue.takeCrypto();
        var key = encrypted ? new SecretKeySpec(new byte[32], "AES") : null;
        var segment = queue.segment(1, Path.of("unused"), key);
        var value = new byte[5_000];
        new Random(1).nextBytes(value);
        var home = MemorySegment.ofArray(value).asReadOnly();
        var sealed = new byte[SpillQueue.BLOCK + value.length];
        var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        // Compiled first, so the counts are those of the code the store runs
        for (var round = 0; round < 3; round++) {
            var rounds = round < 2 ? 5_000 : 1_000;
            var before = threads.getCurrentThreadAllocatedBytes();
            LogValue logged = null;
            for (var i = 0; i < rounds; i++) {
                logged = crypto.logValue(segment, 64, 3, i, home, 0, value.length);
            }
            var tagging = (threads.getCurrentThreadAllocatedBytes() - before) / rounds;
            before = threads.getCurrentThreadAllocatedBytes();
            var verified = true;
            for (var i = 0; i < rounds; i++) {
                verified &= verifies(crypto, value, rounds - 1, logged);
            }
            var verifying = (threads.getCurrentThreadAllocatedBytes() - before) / rounds;
            assertTrue(verified);
            var decrypting = 0L;
            if (encrypted) {
                before = threads.getCurrentThreadAllocatedBytes();
                for (var i = 0; i < rounds; i++) {
                    crypto.decrypt(key, 64, 3, sealed, 3, value.length, new byte[value.length], 0);
                }
                decrypting = (threads.getCurrentThreadAllocatedBytes() - before) / rounds;
            }
            if (round == 2) {
                assertTrue(tagging < 256, "tagging allocated " + tagging + " bytes per value");
                assertTrue(verifying < 256, "verifying allocated " + verifying + " bytes per value");
                assertTrue(decrypting < value.length + 256, "decrypting allocated " + decrypting + " bytes per value");
            }
        }
        var logged = crypto.logValue(segment, 64, 3, 7, home, 0, value.length);
        var changed = value.clone();
        changed[100] ^= 1;
        assertFalse(verifies(crypto, changed, 7, logged));
        if (encrypted) {
            // The tag binds the version it was made for
            assertFalse(verifies(crypto, value, 8, logged));
        }
        queue.giveBack(crypto);
    }

    @Test
    @DisplayName("a thread that started moving values during a recovery stops at the log's durable sequence once the store attaches its log")
    void recoveryThreadFollowsTheLog(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(0, true);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        // A replayed commit whose value takes the thread a while to tag, queued before the store has a log
        var replayed = queue.record(1, 1);
        replayed.add(spillable(1L << 20, 64 << 20), 0, 64 << 20);
        replayed.locate(view, 64, 0, false);
        queue.add(replayed);
        // The thread took the record, so it runs with the supplier it read at its start
        while (queue.heldForTests() != 0) {
            Thread.onSpinWait();
        }
        // The store attaches its log, which holds commits up to 1 durably, while the thread still tags
        queue.useDurable(() -> 1);
        // A live commit 2, published, its frame sealed but not durable
        var live = spillable(2L << 20, 4_000);
        var record = queue.record(2, 1);
        record.add(live, 0, 4_000);
        record.locate(view, 1 << 20, 0, false);
        queue.add(record);
        queue.awaitIdleForTests();
        assertInstanceOf(MemorySegment.class, live.sourceForTests(), "the value of a commit the log does not hold durably");
        assertEquals(1, queue.loggedValuesForTests(), "values moved to the log");
    }

    @Test
    @DisplayName("the values of a commit whose group the log wrote through stay on the heap, and its record leaves the queue")
    void writtenThroughValuesStay(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(0, true);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        var through = spillable(1L << 20, 4_000);
        var record = queue.record(1, 1);
        record.add(through, 0, 4_000);
        record.locate(view, 64, 0, true);
        queue.add(record);
        queue.drain(() -> 1);
        queue.awaitIdleForTests();
        assertInstanceOf(MemorySegment.class, through.sourceForTests(), "a value of a group written through");
        assertEquals(0, queue.loggedValuesForTests(), "values moved to the log");
        assertEquals(0, queue.heldForTests());
    }

    @ParameterizedTest(name = "encrypted: {0}, segment of {1} bytes")
    @CsvSource({"true,65536", "false,65536", "true,1048576", "false,1048576"})
    @DisplayName("a recovery over its budget moves the values it replays from runs and frames of their own before the log's last budget to the log at once, and keeps those of the last budget, as far as the last segment's frames reach, on chunks the queue lists, never on a body, without draining the queue")
    void recoveryKeepsTheLastBudgetOnChunks(boolean encrypted, int segmentSize, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        // The messages of the last commits, long enough for the log: 1,500 and 5,000 bytes
        var newest = List.of(16_000, 16_001);
        try (var ledger = openSized(directory, encrypted, Long.MAX_VALUE, Duration.ofMillis(20), segmentSize)) {
            long last = 0;
            for (var message = 0; message < 400; message++) {
                // Commits of one message share runs, those of 30 take frames of their own
                last = write(ledger, expected, message * 40, message % 10 == 0 ? 30 : 1, 0, false);
            }
            for (var message : newest) {
                assertTrue(value(message, 0).length >= SpillQueue.LOG_VALUE_MIN, "message " + message);
                last = write(ledger, expected, message, 1, 0, false);
            }
            ledger.awaitDurable();
            assertTrue(ledger.log().durableSequence() >= last);
            ledger.closeWithoutCheckpoint();
        }
        var budget = 64L << 10;
        try (var ledger = openSized(directory, encrypted, budget, Duration.ZERO, segmentSize)) {
            // The values of the last budget are listed, and never more than the budget, so no drain started: one that
            // listed every value would have drained the excess, one that listed none would hold nothing
            var held = ledger.spills().heldForTests();
            ledger.spills().awaitIdleForTests();
            assertTrue(held > 0, "the recovery listed no value");
            assertTrue(held <= budget, "the queue holds " + held + " bytes");
            assertEquals(0, ledger.spills().drainedRecordsForTests(), "records drained");
            var logged = 0;
            for (var message : expected.messages.keySet()) {
                var source = head(ledger, MESSAGES, key(message)).sourceForTests();
                switch (source) {
                    case LogValue _ -> logged++;
                    case MemorySegment chunk -> assertEquals(ReplayValues.CHUNK_BYTES, chunk.byteSize(),
                            "message " + message + " is on its frame body");
                    case byte[] _ -> {
                    }
                    default -> fail("message " + message + " is on " + source);
                }
            }
            assertTrue(logged > 0, "the recovery moved no value to the log");
            // The last segment is longer than the budget when it is 1 MiB, and only partly written
            for (var message : newest) {
                assertInstanceOf(MemorySegment.class, head(ledger, MESSAGES, key(message)).sourceForTests(),
                        "message " + message + " of the last budget");
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = openSized(directory, encrypted, budget, Duration.ZERO, segmentSize)) {
            verify(ledger, expected);
        }
    }

    /// {@return a version of a value of `length` bytes on an array of its own, stamped `stamp`}
    private static Version spillable(long stamp, int length) {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++) {
            bytes[i] = (byte) i;
        }
        return Version.of(stamp, MemorySegment.ofArray(bytes).asReadOnly(), 0, length, null, null, null);
    }

    @Test
    @DisplayName("the product modulo 2^61 - 1 of the tags is the exact one for operands up to 2^62")
    void multipliesModuloThePrime() {
        var prime = java.math.BigInteger.ONE.shiftLeft(61).subtract(java.math.BigInteger.ONE);
        var random = new Random(3);
        long[] edges = {0, 1, 2, (1L << 61) - 2, (1L << 61) - 1, 1L << 61, (1L << 62) - 1};
        for (var i = 0; i < 20_000; i++) {
            var a = i < edges.length * edges.length ? edges[i / edges.length] : random.nextLong() >>> 2;
            var b = i < edges.length * edges.length ? edges[i % edges.length] : random.nextLong() >>> 2;
            var expected = java.math.BigInteger.valueOf(a).multiply(java.math.BigInteger.valueOf(b)).mod(prime).longValueExact();
            assertEquals(expected, SpillQueue.Crypto.multiply(a, b), a + " * " + b);
        }
    }

    @Test
    @DisplayName("the tag of a value rejects every value that differs from it in one byte, in its length or in the version it belongs to, at every length up to several words")
    void tagsRejectEveryChange() {
        var queue = new SpillQueue(0, true);
        var crypto = queue.takeCrypto();
        var segment = queue.segment(1, Path.of("unused"), new SecretKeySpec(new byte[32], "AES"));
        var random = new Random(4);
        for (var length = 0; length <= 64; length++) {
            var value = new byte[length];
            random.nextBytes(value);
            var logged = crypto.logValue(segment, 0, 0, 1_000L + length, MemorySegment.ofArray(value), 0, length);
            assertTrue(verifies(crypto, value, 1_000L + length, logged));
            assertFalse(verifies(crypto, value, 1_001L + length, logged), "another version, length " + length);
            for (var at = 0; at < length; at++) {
                for (var bit = 0; bit < 8; bit++) {
                    var changed = value.clone();
                    changed[at] ^= (byte) (1 << bit);
                    assertFalse(verifies(crypto, changed, 1_000L + length, logged), "bit " + bit + " of byte " + at + " of " + length);
                }
            }
            assertFalse(verifies(crypto, Arrays.copyOf(value, length + 1), 1_000L + length, logged), "a zero byte more after " + length);
            if (length > 0) {
                assertFalse(verifies(crypto, Arrays.copyOf(value, length - 1), 1_000L + length, logged), "a byte less than " + length);
            }
        }
        queue.giveBack(crypto);
    }

    @Test
    @DisplayName("a value decrypted from the block that holds its first byte is the value AES-CTR from the body's first counter block decrypts, wherever it starts and however long it is")
    void decryptsAsCounterMode() throws Exception {
        var queue = new SpillQueue(0, true);
        var crypto = queue.takeCrypto();
        var random = new Random(2);
        var bytes = new byte[32];
        random.nextBytes(bytes);
        var key = new SecretKeySpec(bytes, "AES");
        var body = new byte[20_000];
        random.nextBytes(body);
        for (var frameOffset : new long[]{0, 4_096, 0x1_0000_1000L}) {
            var counter = new byte[SpillQueue.BLOCK];
            System.arraycopy(LogWriter.nonce(frameOffset), 0, counter, 0, 12);
            Bytes.i32(counter, 12, 2);
            var ctr = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            ctr.init(javax.crypto.Cipher.ENCRYPT_MODE, key, new javax.crypto.spec.IvParameterSpec(counter));
            var sealedBody = ctr.doFinal(body);
            for (var trial = 0; trial < 200; trial++) {
                var length = 1 + random.nextInt(trial % 2 == 0 ? 64 : 9_000);
                var bodyIndex = random.nextInt(body.length - length);
                var skip = bodyIndex & (SpillQueue.BLOCK - 1);
                var sealed = Arrays.copyOfRange(sealedBody, bodyIndex - skip, bodyIndex + length);
                var decrypted = new byte[length];
                crypto.decrypt(key, frameOffset, bodyIndex, sealed, skip, length, decrypted, 0);
                assertArrayEquals(Arrays.copyOfRange(body, bodyIndex, bodyIndex + length), decrypted,
                        "at " + bodyIndex + " for " + length + " bytes of the frame at " + frameOffset);
            }
        }
        queue.giveBack(crypto);
    }

    @Test
    @DisplayName("a segment a snapshot held past the checkpoint that made it obsolete is ignored and removed by the next open")
    void ignoresSegmentsLeftBehind(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        long first;
        try (var ledger = open(directory, true, 0)) {
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            first = ledger.spills().segmentsForTests()[0];
            var snapshot = ledger.snapshot();
            write(ledger, expected, 1_000, 30, 0, true);
            ledger.checkpoint();
            assertTrue(Files.exists(segmentPath(directory, first)));
            // As a process that stops with the snapshot open leaves it
            ledger.closeWithoutCheckpoint();
            snapshot.close();
        }
        try (var ledger = open(directory, true, 0)) {
            assertFalse(Files.exists(segmentPath(directory, first)));
            verify(ledger, expected);
        }
    }

    static IntStream seeds() {
        return IntStream.range(0, 48);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    @DisplayName("a store that moves every value it can recovers a state between its last durable commit and its last visible one, whatever point a crash or a loss of power stops it at")
    void recoversAfterCrash(int seed) throws Exception {
        var encrypted = seed % 2 == 0;
        var dry = CrashFileSystem.create();
        new Workload(seed, encrypted).run(dry.getPath("/store"));
        var disk = CrashFileSystem.create();
        var random = new Random(seed * 7919L + 3);
        disk.stopAfter(random.nextLong(dry.changes() + 1));
        var workload = new Workload(seed, encrypted);
        workload.run(disk.getPath("/store"));
        var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
        try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
            Expected match = null;
            for (var commit = workload.durable; commit <= workload.visible; commit++) {
                if (matches(ledger, workload.history.get(commit))) {
                    match = workload.history.get(commit);
                    break;
                }
            }
            if (match == null) {
                fail("The store recovered a state no commit from " + workload.durable + " to " + workload.visible + " left");
            }
            verify(ledger, match);
            var after = match.copy();
            write(ledger, after, 50_000, 10, 0, true);
            spillNow(ledger);
            ledger.checkpoint();
            verify(ledger, after);
            ledger.close();
            try (var reopened = open(survivor.getPath("/store"), encrypted, 0)) {
                verify(reopened, after);
            }
        }
    }

    private static boolean matches(Ledger ledger, Expected expected) {
        var messages = new TreeMap<Integer, byte[]>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            messages.put(ByteBuffer.wrap(key).getInt(), value);
            return true;
        });
        if (messages.size() != expected.messages.size()) {
            return false;
        }
        for (var entry : expected.messages.entrySet()) {
            if (!Arrays.equals(entry.getValue(), messages.get(entry.getKey()))) {
                return false;
            }
        }
        var texts = new TreeMap<String, String>();
        ledger.forEach(TEXTS, KeyRange.all(), (key, value) -> {
            texts.put(key, value);
            return true;
        });
        var chats = new TreeMap<String, String>();
        ledger.forEach(CHATS, KeyRange.all(), (key, value) -> {
            chats.put(key, value);
            return true;
        });
        return texts.equals(expected.texts) && chats.equals(expected.chats);
    }

    /// A workload drawn from a seed: batches, some durable, keys written again, removals, checkpoints and reopens, the
    /// queue moving every durable value; and the state after each commit, the last it made visible and the last it
    /// knows durable.
    private static final class Workload {
        private final Random random;
        private final boolean encrypted;
        final List<Expected> history;
        Expected state;
        int visible;
        int durable;

        Workload(int seed, boolean encrypted) {
            this.random = new Random(seed);
            this.encrypted = encrypted;
            this.history = new ArrayList<>();
            this.state = new Expected();
            history.add(state.copy());
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted, 0);
                for (var step = 0; step < 40; step++) {
                    var choice = random.nextInt(20);
                    if (choice == 0) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (choice == 1) {
                        ledger.close();
                        durable = visible;
                        ledger = open(directory, encrypted, 0);
                    } else if (choice == 2 && !state.messages.isEmpty()) {
                        var next = state.copy();
                        var removed = next.messages.firstKey();
                        next.messages.remove(removed);
                        next.texts.remove("t" + removed);
                        var commit = ledger.commit(ledger.newBatch().delete(MESSAGES, key(removed)).delete(TEXTS, "t" + removed));
                        commit(next);
                        if (random.nextBoolean()) {
                            commit.awaitDurable();
                            durable = visible;
                        }
                    } else {
                        var next = state.copy();
                        var awaited = random.nextInt(3) == 0;
                        write(ledger, next, random.nextInt(60), 1 + random.nextInt(25), step, false);
                        commit(next);
                        if (awaited) {
                            ledger.awaitDurable();
                            durable = visible;
                        }
                    }
                }
                ledger.close();
                durable = visible;
            } catch (Exception | AssertionError failure) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }

        private void commit(Expected next) {
            state = next;
            visible++;
            history.add(next.copy());
        }
    }
}
