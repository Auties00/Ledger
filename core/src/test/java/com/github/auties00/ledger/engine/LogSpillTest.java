package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
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
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
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
/// thousands of puts, or the first 64 puts of a commit of 64 writes or more, show that few are replaced; a key space
/// that removes what it writes in commits of under 64 writes lists no value at all.
class LogSpillTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Below LOG_VALUE_MIN, at it, between it and PAYING_VALUE_MIN, and well above, in a frame of runs and in one of its
    // own
    private static final int[] LENGTHS = {40, SpillQueue.LOG_VALUE_MIN, 200, 700, 1_500, 5_000};

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

    /// Opens a store as [#openJudged(Path, boolean, long, Duration, long)] does, with segments of 4 MiB, that keeps
    /// no value in the file of large values, so a value of any length may move to the log.
    private static LiveLedger openUnsplit(Path directory, boolean encrypted) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).largeValues(Long.MAX_VALUE))
                .segmentSize(4L << 20)
                .spillBudget(0)
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

    /// {@return where the value of `key` lies: on its newest version in memory, or in the run the open replayed it
    /// into, as [PackedRun#sourceForTests(int)] gives it}
    private static Object source(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        var version = head(ledger, keyspace, key);
        if (version != null) {
            return version.sourceForTests();
        }
        var run = ledger.state(keyspace).run;
        assertNotNull(run, "no version nor run holds the key");
        var entry = run.find(MemorySegment.ofArray(key));
        assertTrue(entry >= 0, "the run lacks the key");
        return run.sourceForTests(entry);
    }

    /// {@return whether the value of `key` lies on its batch's array: a version's segment, or a home of a bulk commit's
    /// run}
    private static boolean onHome(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        var version = head(ledger, keyspace, key);
        if (version != null) {
            return version.sourceForTests() instanceof MemorySegment;
        }
        var run = ledger.state(keyspace).run;
        return run != null && run.onHomeForTests(run.find(MemorySegment.ofArray(key)));
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
                    // A commit of values too short to pay for their move alone, made while the queue held nothing,
                    // lists none
                    var small = expected.messages.get(message).length < SpillQueue.PAYING_VALUE_MIN
                            && text(message, 0).length() < SpillQueue.PAYING_VALUE_MIN;
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

    @ParameterizedTest(name = "{0} bytes of heap: {1}")
    @CsvSource({
            "0, 4194304",
            "1073741824, 4194304",
            "4294967296, 4194304",
            "8589934592, 8388608",
            "17179869184, 16777216",
            "68719476736, 67108864",
            "137438953472, 67108864",
            "9223372036854775807, 67108864"
    })
    @DisplayName("the default budget is a 1,024th of the heap, never under 4 MiB nor over 64 MiB")
    void defaultBudget(long maxMemory, long budget) {
        assertEquals(budget, SpillQueue.defaultBudget(maxMemory));
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
    @DisplayName("values read back in turn from several segments into a caller's array allocate no copy and no key schedule per value, as a checkpoint reads them")
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
            // Without encryption nothing; with it the counter block and the key bytes the cipher copies to start at the
            // value, never a buffer over the target or the key schedule of a segment switched to (about 300 bytes)
            var allowed = encrypted ? 256 : 32;
            assertTrue(perValue < allowed, "a value read back allocated " + perValue + " bytes");
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

    @ParameterizedTest(name = "encrypted: {0}, crash: {1}")
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    @DisplayName("values moved to segments after a checkpoint's cut stay in place while a tick retires the segments a snapshot held back and rollovers prepare the next segments from those spares, and a close checkpoints them, or a crash replays them")
    void pendingValuesOutliveRecycledSpares(boolean encrypted, boolean crash, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, 0)) {
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            var held = ledger.spills().segmentsForTests();
            var snapshot = ledger.snapshot();
            var seen = expected.copy();
            ledger.checkpoint();
            var logStart = ledger.log().currentSegment();
            assertTrue(held[0] < logStart && Files.exists(segmentPath(directory, held[0])),
                    "the checkpoint retired a segment a snapshot needs");
            // Values no checkpoint holds, moved to the segments after the cut, over several rollovers
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, 1_000 + batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            var pending = pendingSegments(ledger, 1_000, 1_180);
            assertTrue(pending.length > 1, "the pending values moved to " + pending.length + " segments");
            assertTrue(pending[0] >= logStart, "a pending value moved to segment " + pending[0] + " before the cut's " + logStart);
            verifyView(snapshot, seen);
            snapshot.close();
            // The tick retires the held segments into spares, keeping those of the pending values
            ledger.checkpointer().tick();
            assertTrue(Arrays.stream(ledger.spills().segmentsForTests()).noneMatch(number -> number == held[0]));
            assertFalse(Files.exists(segmentPath(directory, held[0])));
            var spares = spares(directory);
            assertFalse(spares.isEmpty(), "the tick kept no spare");
            assertPendingInPlace(directory, ledger, 1_000, 1_180);
            // Rollovers prepare their segments from those spares, while the pending values stay where they moved
            for (var batch = 0; batch < 6; batch++) {
                write(ledger, expected, 2_000 + batch * 30, 30, 0, true);
            }
            spillNow(ledger);
            assertTrue(spares.stream().anyMatch(spare -> !Files.exists(spare)), "no rollover took a spare");
            assertTrue(pendingSegments(ledger, 2_000, 2_180).length > 1, "the later values moved to one segment");
            ledger.checkpointer().tick();
            assertPendingInPlace(directory, ledger, 1_000, 1_180);
            assertPendingInPlace(directory, ledger, 2_000, 2_180);
            verify(ledger, expected);
            if (crash) {
                ledger.closeWithoutCheckpoint();
            }
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    /// {@return the segments, ascending and distinct, that the newest values of the messages from `from` to `to`,
    /// exclusive, moved to and that no checkpoint retired}
    private static long[] pendingSegments(LiveLedger ledger, int from, int to) {
        return IntStream.range(from, to)
                .mapToObj(message -> head(ledger, MESSAGES, key(message)))
                .filter(version -> version != null && version.sourceForTests() instanceof LogValue logged && !logged.segment().retired())
                .mapToLong(version -> ((LogValue) version.sourceForTests()).segment().number())
                .distinct().sorted().toArray();
    }

    /// Checks that the file of every segment the newest values of the messages from `from` to `to`, exclusive, moved to
    /// is in place, unless a checkpoint retired it, and that each such value reads back.
    private static void assertPendingInPlace(Path directory, LiveLedger ledger, int from, int to) {
        for (var message = from; message < to; message++) {
            var version = head(ledger, MESSAGES, key(message));
            if (version != null && version.sourceForTests() instanceof LogValue logged && !logged.segment().retired()) {
                assertTrue(Files.exists(segmentPath(directory, logged.segment().number())),
                        "the segment " + logged.segment().number() + " of message " + message + " is gone");
                assertNotNull(version.copyBytes(), "message " + message);
            }
        }
    }

    /// {@return the spare segments of the log of the store in `directory`}
    private static List<Path> spares(Path directory) throws java.io.IOException {
        try (var files = Files.list(directory.resolve("log"))) {
            return files.filter(file -> file.getFileName().toString().endsWith(".spare")).toList();
        }
    }

    private static Path segmentPath(Path directory, long number) {
        return directory.resolve("log").resolve(LogFiles.segmentName(number));
    }

    @Test
    @Timeout(120)
    @DisplayName("values moved to segments beside checkpoints that leave them pending keep those segments through idle ticks that retire the segments before them, and a close checkpoints them from the log")
    void pendingValuesKeepTheirSegments(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        var message = 0;
        try (var ledger = openSized(directory, true, 0, Duration.ZERO, 256 << 10)) {
            var pending = 0;
            for (var round = 0; round < 4; round++) {
                // Values moved before the checkpoint, which takes them
                for (var batch = 0; batch < 6; batch++, message += 30) {
                    write(ledger, expected, message, 30, 0, true);
                }
                spillNow(ledger);
                // A snapshot of the tree before the checkpoint holds the segments it makes obsolete back, so the idle
                // ticks after it closes retire them
                var snapshot = ledger.snapshot();
                var seen = expected.copy();
                // Commits moved while the checkpoint runs, which it may cut before or after
                var first = message;
                var failure = new AtomicReference<Throwable>();
                var writer = Thread.ofPlatform().start(() -> {
                    try {
                        for (var batch = 0; batch < 6; batch++) {
                            write(ledger, expected, first + batch * 30, 30, 0, true);
                            spillNow(ledger);
                        }
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                });
                ledger.checkpoint();
                writer.join();
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                message += 6 * 30;
                // Commits moved after the checkpoint, which it leaves pending
                for (var batch = 0; batch < 3; batch++, message += 30) {
                    write(ledger, expected, message, 30, 0, true);
                }
                spillNow(ledger);
                verifyView(snapshot, seen);
                snapshot.close();
                for (var tick = 0; tick < 3; tick++) {
                    ledger.checkpointer().tick();
                    pending = Math.max(pending, assertSegmentsKept(ledger, directory, expected));
                }
                verify(ledger, expected);
            }
            assertTrue(pending > 0, "no value moved to the log was left pending");
        }
        try (var ledger = openSized(directory, true, 0, Duration.ZERO, 256 << 10)) {
            verify(ledger, expected);
        }
    }

    /// Checks that the segment of every version of `expected`'s keys whose value moved to the log is in place or
    /// retired, never gone while a reader may still read the value from it, and returns how many such versions have
    /// their segment in place.
    private static int assertSegmentsKept(LiveLedger ledger, Path directory, Expected expected) {
        var kept = 0;
        var keys = new ArrayList<Map.Entry<Keyspace<?, ?>, byte[]>>();
        for (var message : expected.messages.keySet()) {
            keys.add(Map.entry(MESSAGES, key(message)));
        }
        for (var text : expected.texts.keySet()) {
            keys.add(Map.entry(TEXTS, text.getBytes(StandardCharsets.UTF_8)));
        }
        for (var key : keys) {
            for (var version = head(ledger, key.getKey(), key.getValue()); version != null; version = version.older) {
                if (version.sourceForTests() instanceof LogValue logged) {
                    // A segment retires before its file goes, so a file found gone was retired first
                    var present = Files.exists(segmentPath(directory, logged.segment().number()));
                    assertTrue(present || logged.segment().retired(),
                            "segment " + logged.segment().number() + " is gone but not retired");
                    if (present && !logged.segment().retired()) {
                        kept++;
                    }
                }
            }
        }
        return kept;
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
                var source = source(ledger, MESSAGES, key(entry.getKey()));
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
    @DisplayName("a recovery keeps in the log the values it replays to a run and to chains from the queue's threshold, whatever the verdict a live commit of them would take, and reports a run's value whose bytes in the log changed")
    void recoveryLogsReplayedValuesFromTheThreshold(boolean encrypted, @TempDir Path directory) throws Exception {
        int[] lengths = {SpillQueue.LOG_VALUE_MIN - 1, SpillQueue.LOG_VALUE_MIN, SpillQueue.PAYING_VALUE_MIN - 1,
                SpillQueue.PAYING_VALUE_MIN};
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            // Messages from 0 replay to the run, and those from 100, after the range removal, to chains
            for (var first : new int[]{0, 100}) {
                if (first > 0) {
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(50), key(60))))
                            .awaitDurable();
                }
                var batch = ledger.newBatch();
                for (var i = 0; i < lengths.length; i++) {
                    var value = new byte[lengths[i]];
                    for (var j = 0; j < value.length; j++) {
                        value[j] = (byte) (first * 13 + i * 7 + j);
                    }
                    batch.put(MESSAGES, key(first + i), value);
                    expected.put(first + i, value);
                }
                ledger.commit(batch).awaitDurable();
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            assertNotNull(ledger.state(MESSAGES).run, "no run of the writes before the range removal");
            var damaged = -1;
            LogValue logged = null;
            for (var entry : expected.entrySet()) {
                var message = entry.getKey();
                var length = entry.getValue().length;
                var chained = head(ledger, MESSAGES, key(message)) != null;
                assertEquals(message >= 100, chained, "message " + message + " replayed to a chain");
                var source = source(ledger, MESSAGES, key(message));
                if (length >= SpillQueue.LOG_VALUE_MIN) {
                    var found = assertInstanceOf(LogValue.class, source, "message " + message + " of " + length + " bytes");
                    if (!chained && length == SpillQueue.LOG_VALUE_MIN) {
                        damaged = message;
                        logged = found;
                    }
                } else {
                    assertFalse(source instanceof LogValue, "message " + message + " of " + length + " bytes is in the log");
                }
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(message)).orElseThrow(), "message " + message);
            }
            assertTrue(damaged >= 0, "no value of the run lies in the log at the threshold");
            var position = logged.frameOffset() + LogWriter.FRAME_HEADER + logged.bodyIndex() + 3;
            try (var channel = FileChannel.open(logged.segment().path(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                var one = ByteBuffer.allocate(1);
                channel.read(one, position);
                one.put(0, (byte) (one.get(0) ^ 0x5A)).rewind();
                channel.write(one, position);
            }
            var message = damaged;
            assertThrows(LedgerCorruptedException.class, () -> ledger.get(MESSAGES, key(message)));
            for (var other : expected.keySet()) {
                if (other != message) {
                    assertArrayEquals(expected.get(other), ledger.get(MESSAGES, key(other)).orElseThrow(), "message " + other);
                }
            }
            ledger.closeWithoutCheckpoint();
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
                var source = source(ledger, MESSAGES, key(entry.getKey()));
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
    @DisplayName("a key space that keeps what it writes in commits of under 64 writes lets its values leave the heap once its samples are judged")
    void keptValuesSpillOnceJudged(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        var count = 20;
        try (var ledger = openJudged(directory, true, 0, Duration.ZERO)) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            assertFalse(overwrites.spills(), "the values may leave the heap before any sample");
            var batches = 0;
            // A batch of 20 messages is 60 writes, under the 64 that sample every put of a key space not yet judged,
            // so one put in 64 is sampled, and the first judgement comes after about 4,096 messages
            while (!overwrites.spills()) {
                assertTrue(batches < 1_000, "the samples of values written once never let them leave the heap");
                write(ledger, expected, batches * count, count, 0, false);
                batches++;
            }
            assertTrue(batches > 20, "judged after " + batches + " batches, as if every put were sampled");
            ledger.awaitDurable();
            spillNow(ledger);
            // The key space of texts may be judged first, so only the messages before the last batch are checked
            for (var message = 0; message < (batches - 1) * count; message++) {
                assertInstanceOf(MemorySegment.class, head(ledger, MESSAGES, key(message)).sourceForTests(),
                        "message " + message + " moved before its key space was judged");
            }
            write(ledger, expected, batches * count, count, 0, true);
            spillNow(ledger);
            assertTrue(ledger.spills().loggedValuesForTests() > 0, "no value moved to the log once judged");
            verify(ledger, expected);
        }
    }

    /// Commits `count` messages from `first` to [#MESSAGES] in one batch, each of one of `lengths`, after `residents`
    /// writes of [#CHATS], and records them in `expected`.
    private static void writeBulk(Ledger ledger, Expected expected, int first, int count, int[] lengths, int residents) throws Exception {
        var batch = ledger.newBatch();
        for (var i = 0; i < residents; i++) {
            batch.put(CHATS, "bulk" + i, "resident " + first + "/" + i);
            expected.chats.put("bulk" + i, "resident " + first + "/" + i);
        }
        for (var message = first; message < first + count; message++) {
            var value = new byte[lengths[message % lengths.length]];
            for (var i = 0; i < value.length; i++) {
                value[i] = (byte) (message * 13 + i * 7);
            }
            batch.put(MESSAGES, key(message), value);
            expected.messages.put(message, value);
        }
        ledger.commit(batch).awaitDurable();
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("one commit of 20,000 puts to a key space not yet judged judges it at its 64th put and lists the values from there, which leave the heap and read back from a frame of millions of bytes before and after a checkpoint and a reopen")
    void bulkCommitListsOnceJudged(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var count = 20_000;
        // About 4.7 MB of values, so the commit's frame is far above the largest body of a run and close to a segment;
        // the values of 600 bytes make the move pay, and those of 96 and 200 bytes go to the log beside them
        int[] lengths = {40, 200, SpillQueue.LOG_VALUE_MIN, 600};
        var segmentSize = 6L << 20;
        try (var ledger = openJudged(directory, encrypted, 0, Duration.ZERO, segmentSize)) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            assertFalse(overwrites.judged());
            // A commit of resident writes only, past the bound of a bulk commit, which no key space samples
            var residents = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                // Long enough for the log, so the commit may list values
                var chat = "chat " + i + "x".repeat(SpillQueue.LOG_VALUE_MIN);
                residents.put(CHATS, "only" + i, chat);
                expected.chats.put("only" + i, chat);
            }
            ledger.commit(residents).awaitDurable();
            assertEquals(0, overwrites.sampledPutsForTests());
            writeBulk(ledger, expected, 0, count, lengths, 100);
            assertTrue(overwrites.judged(), "the bulk commit did not judge its key space");
            assertTrue(overwrites.spills(), "values written once may not leave the heap");
            var before = ledger.snapshot();
            spillNow(ledger);
            var spills = ledger.spills();
            assertEquals(0, spills.heldForTests());
            var logged = 0;
            var copied = 0;
            for (var message = 0; message < count; message++) {
                // The bulk commit keeps the values in its run: a short one moves to a chunk of the run's own
                var source = source(ledger, MESSAGES, key(message));
                if (message < ValueSlab.Overwrites.BULK_WRITES - 1) {
                    assertTrue(onHome(ledger, MESSAGES, key(message)), "message " + message + " was listed before the judgement");
                } else if (expected.messages.get(message).length >= SpillQueue.LOG_VALUE_MIN) {
                    assertInstanceOf(LogValue.class, source, "message " + message);
                    logged++;
                } else {
                    assertInstanceOf(MemorySegment.class, source, "message " + message);
                    assertFalse(onHome(ledger, MESSAGES, key(message)), "message " + message);
                    copied++;
                }
            }
            assertEquals(logged, spills.loggedValuesForTests());
            assertEquals(copied, spills.copiedValuesForTests());
            for (var i = 0; i < 100; i++) {
                assertFalse(head(ledger, CHATS, ("bulk" + i).getBytes(StandardCharsets.UTF_8)).sourceForTests() instanceof LogValue);
            }
            verify(ledger, expected);
            try (before) {
                verifyView(before, expected);
            }
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = openJudged(directory, encrypted, 0, Duration.ZERO, segmentSize)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a commit of values of 100 to 300 bytes, like an app-state snapshot, whose move pays only through values under 256 bytes, stays off the queue on its batch's arrays, while one whose longer values pay moves its values of 96 to 255 bytes to the log beside them, and both read back as written before and after a checkpoint and a reopen")
    void shortValuesStayOffTheQueue(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var count = 20_000;
        // Two in three values count at the arrays of their own they would take, 16 bytes more than the value, though
        // they would go to the log
        int[] lengths = {100, 200, 300};
        try (var ledger = openJudged(directory, encrypted, 0, Duration.ZERO, 6L << 20)) {
            writeBulk(ledger, expected, 0, count, lengths, 0);
            assertTrue(ledger.state(MESSAGES).overwrites.spills(), "values written once may not leave the heap");
            var spills = ledger.spills();
            assertEquals(1, spills.unpaidRecordsForTests(), "the commit's record was queued");
            assertEquals(0, spills.heldForTests());
            spillNow(ledger);
            assertEquals(0, spills.loggedValuesForTests() + spills.copiedValuesForTests());
            for (var message = 0; message < count; message += 97) {
                assertTrue(onHome(ledger, MESSAGES, key(message)), "message " + message);
            }
            // Values of 700 bytes pay for the move of the rest: those of 120 bytes go to the log, those of 40 to arrays
            // of their own
            var paying = 1_200;
            writeBulk(ledger, expected, count, paying, new int[]{40, 120, 700}, 0);
            spillNow(ledger);
            assertEquals(1, spills.unpaidRecordsForTests(), "the commit of long values was left off the queue");
            assertEquals(2 * paying / 3, spills.loggedValuesForTests());
            assertEquals(paying / 3, spills.copiedValuesForTests());
            for (var message = count; message < count + paying; message++) {
                var source = head(ledger, MESSAGES, key(message)).sourceForTests();
                if (expected.messages.get(message).length < SpillQueue.LOG_VALUE_MIN) {
                    assertInstanceOf(byte[].class, source, "message " + message);
                } else {
                    assertInstanceOf(LogValue.class, source, "message " + message);
                }
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = openJudged(directory, encrypted, 0, Duration.ZERO, 6L << 20)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "{0} bytes")
    @ValueSource(ints = {160 << 10, 256 << 10})
    @DisplayName("a value on an array of its own leaves the heap for the log beside thousands of short values, which stay on their batch's arrays")
    void ownValueBesideShortValues(int ownLength, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var shorts = 3_000;
        var own = 1_000_000;
        try (var ledger = openSized(directory, true, 0, Duration.ZERO, 16L << 20)) {
            var batch = ledger.newBatch();
            for (var message = 0; message < shorts; message++) {
                var value = new byte[message % 2 == 0 ? 100 : 200];
                Arrays.fill(value, (byte) message);
                batch.put(MESSAGES, key(message), value);
                expected.messages.put(message, value);
            }
            var large = new byte[ownLength];
            new Random(ownLength).nextBytes(large);
            batch.put(MESSAGES, key(own), large);
            expected.messages.put(own, large);
            ledger.commit(batch).awaitDurable();
            spillNow(ledger);
            var spills = ledger.spills();
            assertEquals(1, spills.unpaidRecordsForTests(), "the short values' move was judged to pay");
            assertEquals(1, spills.loggedValuesForTests());
            assertEquals(0, spills.copiedValuesForTests());
            assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(own)).sourceForTests());
            for (var message = 0; message < shorts; message += 97) {
                assertTrue(onHome(ledger, MESSAGES, key(message)), "message " + message);
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = openSized(directory, true, 0, Duration.ZERO, 16L << 20)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "chained: {0}, lengths: {1}")
    @CsvSource({
            "true, 100 200 300, false",
            "false, 100 200 300, false",
            "true, 255 255 255 255 255 255 255 255 255 256, false",
            "false, 255 255 255 255 255 255 255 255 255 256, false",
            "true, 256, true",
            "false, 256, true",
            "true, 90 90 90 90 90 90 90 300 300 300, false",
            "false, 90 90 90 90 90 90 90 300 300 300, true"
    })
    @DisplayName("a commit's verdict counts a value from 256 bytes at its LogValue and a shorter one at the array of its own it would take, but a bulk run's value under 96 bytes at its length in the run's chunk, whether the commit goes to chains over a tree or to a bulk run, encrypted or not")
    void verdictCountsShortValues(boolean chained, String lengthList, boolean moves, @TempDir Path directory) throws Exception {
        // Every list holds a value of at least 256 bytes, without which a commit lists no value while the queue is
        // within its budget
        var lengths = Arrays.stream(lengthList.split(" ")).mapToInt(Integer::parseInt).toArray();
        var count = 4_000;
        for (var encrypted : new boolean[]{true, false}) {
            var expected = new Expected();
            var store = directory.resolve(encrypted ? "encrypted" : "plain");
            try (var ledger = openSized(store, encrypted, 0, Duration.ZERO, 6L << 20)) {
                if (chained) {
                    // A tree that holds the key space, so the batch goes to chains rather than to a bulk run
                    var seed = new byte[1_000];
                    ledger.commit(ledger.newBatch().put(MESSAGES, key(1_000_000), seed)).awaitDurable();
                    expected.messages.put(1_000_000, seed);
                    ledger.checkpoint();
                }
                var spills = ledger.spills();
                spillNow(ledger);
                var unpaid = spills.unpaidRecordsForTests();
                var logged = spills.loggedValuesForTests();
                var copied = spills.copiedValuesForTests();
                writeBulk(ledger, expected, 0, count, lengths, 0);
                var run = ledger.state(MESSAGES).run;
                if (chained) {
                    assertNull(run, "a bulk run over a tree that holds the key space");
                } else {
                    assertNotNull(run, "no bulk run in an empty key space");
                    assertTrue(run.committed());
                }
                assertEquals(unpaid + (moves ? 0 : 1), spills.unpaidRecordsForTests(), "records judged not to pay");
                spillNow(ledger);
                var longValues = 0;
                for (var message = 0; message < count; message++) {
                    if (lengths[message % lengths.length] >= SpillQueue.LOG_VALUE_MIN) {
                        longValues++;
                    }
                }
                if (moves) {
                    assertEquals(logged + longValues, spills.loggedValuesForTests());
                    assertEquals(copied + count - longValues, spills.copiedValuesForTests());
                } else {
                    assertEquals(logged, spills.loggedValuesForTests());
                    assertEquals(copied, spills.copiedValuesForTests());
                }
                for (var message = 0; message < count; message++) {
                    assertEquals(!moves, onHome(ledger, MESSAGES, key(message)), "message " + message);
                }
                verify(ledger, expected);
                ledger.checkpoint();
                verify(ledger, expected);
            }
            try (var ledger = openSized(store, encrypted, 0, Duration.ZERO, 6L << 20)) {
                verify(ledger, expected);
            }
        }
    }

    @Test
    @DisplayName("a commit of thousands of removals from a key space not yet judged samples them one in 64, like any removal, and judges nothing")
    void bulkRemovalsJudgeNothing(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        int[] lengths = {300};
        try (var ledger = openJudged(directory, true, 64L << 20, Duration.ZERO)) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            // Commits of 40 writes, sampled one put in 64, too few to judge
            for (var batch = 0; batch < 50; batch++) {
                writeBulk(ledger, expected, batch * 40, 40, lengths, 0);
            }
            assertFalse(overwrites.judged(), "2,000 puts sampled one in 64 judged the key space");
            var puts = overwrites.sampledPutsForTests();
            assertTrue(puts > 0 && puts < 64, puts + " sampled puts");
            var removals = ledger.newBatch();
            for (var message = 0; message < 2_000; message++) {
                removals.delete(MESSAGES, key(message));
            }
            ledger.commit(removals).awaitDurable();
            assertFalse(overwrites.judged(), "the removals judged the key space");
            assertFalse(overwrites.spills());
            assertEquals(puts, overwrites.sampledPutsForTests(), "the removals counted as puts");
            assertEquals(0, ledger.size(MESSAGES));
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
        replayed.locate(view, 64, 0, 0, false);
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
        record.locate(view, 1 << 20, 0, 0, false);
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
        record.locate(view, 64, 0, 0, true);
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
                var source = source(ledger, MESSAGES, key(message));
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

    /// {@return a value of `length` bytes for `message`, long enough to take an array of its own in its batch}
    private static byte[] large(int message, int length) {
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 101 + i * 7 + (i >>> 9));
        }
        return value;
    }

    /// Checks every way a read returns the values of `expected` from `view`.
    private static void verifyLarge(View view, TreeMap<Integer, byte[]> expected) {
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, key(entry.getKey())).orElseThrow(), "message " + entry.getKey());
        }
        var found = view.range(MESSAGES, KeyRange.all(), Integer.MAX_VALUE);
        assertEquals(expected.size(), found.size(), "messages in a range of " + view);
        var index = 0;
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), found.get(index++).value(), "message " + entry.getKey() + " in a range");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("values on arrays of their own leave the heap for the log before any verdict on their key space, and read back as written from a snapshot, a transaction, a checkpoint and a recovery")
    void ownValuesMove(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        // From the shortest value an array of its own takes to one past the slice of a read of the channel
        var lengths = new int[]{ArrayWriteBatch.CHUNK_BYTES / 2, 256 << 10, LogSegmentView.SLICE + 3_000};
        try (var ledger = openUnsplit(directory, encrypted)) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            for (var message = 0; message < 6; message++) {
                var value = large(message, lengths[message % lengths.length]);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            assertFalse(overwrites.spills(), "the key space was judged");
            var spills = ledger.spills();
            assertEquals(6, spills.loggedValuesForTests(), "values moved to the log");
            assertEquals(0, spills.heldForTests());
            for (var message : expected.keySet()) {
                assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(message)).sourceForTests(), "message " + message);
                var read = ledger.readEncoded(MESSAGES, key(message), segment -> segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
                assertArrayEquals(expected.get(message), read.orElseThrow(), "message " + message + " read encoded");
            }
            verifyLarge(ledger, expected);
            var before = ledger.snapshot();
            var seen = new TreeMap<>(expected);
            // Overwritten, the moved value stays the snapshot's
            var replaced = large(100, 256 << 10);
            ledger.commit(ledger.newBatch().put(MESSAGES, key(0), replaced)).awaitDurable();
            expected.put(0, replaced);
            spillNow(ledger);
            ledger.update(transaction -> {
                for (var entry : expected.entrySet()) {
                    assertArrayEquals(entry.getValue(), transaction.get(MESSAGES, key(entry.getKey())).orElseThrow());
                }
            });
            // The checkpoint reads the values it writes back from the log
            ledger.checkpoint();
            verifyLarge(ledger, expected);
            try (before) {
                verifyLarge(before, seen);
            }
            // Moved after the checkpoint, then replayed by the recovery
            for (var message = 10; message < 13; message++) {
                var value = large(message, lengths[message % lengths.length]);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(10)).sourceForTests());
            verifyLarge(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = openUnsplit(directory, encrypted)) {
            verifyLarge(ledger, expected);
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
        try (var ledger = openUnsplit(directory, encrypted)) {
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a value that takes nearly all of its frame leaves the heap without a tag of its own, and reads back as written through its frame, from a get, a range, a checkpoint and a recovery, while values that share their frame or exceed the pool keep their tags")
    void framedValuesMove(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        // Framed: from the shortest framed value to one near the longest frame the pool recycles
        var framed = new int[]{SpillQueue.FRAMED_VALUE_MIN, ArrayWriteBatch.CHUNK_BYTES / 2, 256 << 10, 900 << 10};
        try (var ledger = openUnsplit(directory, true)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            for (var message = 0; message < framed.length; message++) {
                var value = large(message, framed[message]);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            // Two values of half a frame each, and one whose frame is past the pool's arrays
            var shared = ledger.newBatch();
            for (var message = 10; message < 12; message++) {
                var value = large(message, 200 << 10);
                shared.put(MESSAGES, key(message), value);
                expected.put(message, value);
            }
            ledger.commit(shared).awaitDurable();
            var long_ = large(20, HeapPool.MAX_POOLED);
            ledger.commit(ledger.newBatch().put(MESSAGES, key(20), long_)).awaitDurable();
            expected.put(20, long_);
            spillNow(ledger);
            for (var message : expected.keySet()) {
                var logged = assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(message)).sourceForTests(), "message " + message);
                assertEquals(message < framed.length, logged.framed(), "message " + message + " framed");
            }
            verifyLarge(ledger, expected);
            // The checkpoint reads the framed values back into its units
            ledger.checkpoint();
            verifyLarge(ledger, expected);
            for (var message = 30; message < 32; message++) {
                var value = large(message, 300 << 10);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            assertTrue(((LogValue) head(ledger, MESSAGES, key(30)).sourceForTests()).framed());
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = openUnsplit(directory, true)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            verifyLarge(ledger, expected);
        }
    }

    @ParameterizedTest(name = "byte {0} of the frame")
    @ValueSource(strings = {"header", "value", "tag"})
    @DisplayName("a framed value whose frame changed in its header, its value or its tag is reported, never returned, and the other values read on")
    void reportsDamagedFrames(String where, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openUnsplit(directory, true)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            for (var message = 0; message < 4; message++) {
                var value = large(message, 100 << 10);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            var logged = (LogValue) head(ledger, MESSAGES, key(2)).sourceForTests();
            assertTrue(logged.framed());
            var frame = logged.frameOffset();
            var position = switch (where) {
                case "header" -> frame + 9;
                case "value" -> frame + LogWriter.FRAME_HEADER + logged.bodyIndex() + 50_000;
                default -> frame + LogWriter.FRAME_HEADER + logged.frameBody() + 5;
            };
            try (var channel = FileChannel.open(logged.segment().path(), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                var one = ByteBuffer.allocate(1);
                channel.read(one, position);
                one.put(0, (byte) (one.get(0) ^ 0x21)).rewind();
                channel.write(one, position);
            }
            assertThrows(LedgerCorruptedException.class, () -> ledger.get(MESSAGES, key(2)));
            var target = new byte[(100 << 10) + 8];
            assertThrows(LedgerCorruptedException.class, () -> head(ledger, MESSAGES, key(2)).readLogged(target, 8));
            for (var message : expected.keySet()) {
                if (message != 2) {
                    assertArrayEquals(expected.get(message), ledger.get(MESSAGES, key(message)).orElseThrow());
                }
            }
            ledger.closeWithoutCheckpoint();
        }
    }

    @Test
    @DisplayName("a get of a framed value allocates the value it returns and little else, the frame's arrays coming back to the pool")
    void framedReadsAllocateTheValue(@TempDir Path directory) throws Exception {
        var length = 256 << 10;
        try (var ledger = openUnsplit(directory, true)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            for (var message = 0; message < 8; message++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), large(message, length))).awaitDurable();
            }
            spillNow(ledger);
            assertTrue(((LogValue) head(ledger, MESSAGES, key(3)).sourceForTests()).framed());
            var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            var least = Long.MAX_VALUE;
            for (var round = 0; round < 6; round++) {
                var before = threads.getCurrentThreadAllocatedBytes();
                for (var message = 0; message < 8; message++) {
                    ledger.get(MESSAGES, key(message)).orElseThrow();
                }
                least = Math.min(least, (threads.getCurrentThreadAllocatedBytes() - before) / 8);
            }
            assertTrue(least < length + (4 << 10), "a get allocated " + least + " bytes");
            ledger.closeWithoutCheckpoint();
        }
    }

    @Test
    @DisplayName("moved values on arrays of their own keep their segments for a snapshot of an older tree, and once the segments retire a reader whose versions point there reads them from its tree")
    void ownValuesOfRetiredSegments(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openJudged(directory, true, 0, Duration.ZERO, 1L << 20)) {
            // A resident value alone, which never moves, so the oldest snapshot holds no segment back
            ledger.commit(ledger.newBatch().put(CHATS, "c-old", "before the moved values")).awaitDurable();
            var old = ledger.snapshot();
            for (var message = 0; message < 8; message++) {
                var value = large(message, 200_000 + message);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            var held = ledger.spills().segmentsForTests();
            assertTrue(held.length > 1, "the values moved to " + held.length + " segments");
            var middle = ledger.snapshot();
            var seen = new TreeMap<>(expected);
            for (var message = 0; message < 8; message += 2) {
                var value = large(message + 50, 200_000);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
            }
            spillNow(ledger);
            ledger.checkpoint();
            var kept = ledger.spills().segmentsForTests();
            assertTrue(kept.length > 0 && kept[0] == held[0], "the checkpoint retired a segment a snapshot needs");
            verifyLarge(middle, seen);
            verifyLarge(ledger, expected);
            middle.close();
            ledger.checkpointer().tick();
            assertTrue(Arrays.stream(ledger.spills().segmentsForTests()).noneMatch(number -> number == held[0]));
            // The oldest snapshot keeps the versions in memory, whose values are in the retired segments now
            var retired = 0;
            for (var message = 1; message < 8; message += 2) {
                if (head(ledger, MESSAGES, key(message)).sourceForTests() instanceof LogValue logged && logged.segment().retired()) {
                    retired++;
                }
            }
            assertTrue(retired > 0, "no version in memory has its value in a retired segment");
            verifyLarge(ledger, expected);
            try (var later = ledger.snapshot()) {
                verifyLarge(later, expected);
            }
            assertEquals("before the moved values", old.get(CHATS, "c-old").orElseThrow());
            assertTrue(old.get(MESSAGES, key(1)).isEmpty());
            old.close();
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
        try (var ledger = openJudged(directory, true, 0, Duration.ZERO, 1L << 20)) {
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("in a batch a hot slab takes beside a value on an array of its own, only that value leaves the heap: the others stay on the slab's chunk")
    void slabValuesStayBesideOwnValues(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        // Key spaces whose values may leave the heap from the first commit, so only the slab keeps them where they are
        try (var ledger = open(directory, true, 0)) {
            ledger.valueSlab().forceHotForTests(1L << 20);
            for (var batch = 0; batch < 3; batch++) {
                var writes = ledger.newBatch();
                // Fifteen short values and one long one fill the writes a slab takes
                for (var i = 0; i < 15; i++) {
                    var message = batch * 100 + i;
                    var value = large(message, 1_000 + i);
                    writes.put(MESSAGES, key(message), value);
                    expected.put(message, value);
                }
                var own = large(batch * 100 + 99, 256 << 10);
                writes.put(MESSAGES, key(batch * 100 + 99), own);
                expected.put(batch * 100 + 99, own);
                ledger.commit(writes).awaitDurable();
            }
            spillNow(ledger);
            var spills = ledger.spills();
            assertEquals(3, spills.loggedValuesForTests(), "values moved to the log");
            assertEquals(0, spills.copiedValuesForTests(), "values moved to arrays of their own");
            for (var entry : expected.entrySet()) {
                var version = head(ledger, MESSAGES, key(entry.getKey()));
                if (entry.getKey() % 100 == 99) {
                    assertInstanceOf(LogValue.class, version.sourceForTests(), "message " + entry.getKey());
                } else {
                    assertInstanceOf(MemorySegment.class, version.sourceForTests(), "message " + entry.getKey());
                    assertTrue(version.sourceBytesForTests() > 15 * 1_000, "message " + entry.getKey() + " is not on a chunk");
                }
            }
            verifyLarge(ledger, expected);
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a batch a hot slab takes beside values on arrays of their own lists them in a record with room for those values only, not for every write")
    void ownValueRecordsHaveOwnRoom(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        // A budget no commit here passes, which the records of values of 256 KiB on arrays of their own leave outside
        try (var ledger = open(directory, true, 64L << 20)) {
            ledger.valueSlab().forceHotForTests(1L << 20);
            com.github.auties00.ledger.Commit last = null;
            for (var batch = 0; batch < 3; batch++) {
                var writes = ledger.newBatch();
                for (var i = 0; i < 14; i++) {
                    var message = batch * 100 + i;
                    var value = large(message, 1_000 + i);
                    writes.put(MESSAGES, key(message), value);
                    expected.put(message, value);
                }
                for (var own = 98; own < 100; own++) {
                    var value = large(batch * 100 + own, 256 << 10);
                    writes.put(MESSAGES, key(batch * 100 + own), value);
                    expected.put(batch * 100 + own, value);
                }
                last = ledger.commit(writes);
            }
            last.awaitDurable();
            spillNow(ledger);
            var spills = ledger.spills();
            assertEquals(3, spills.drainedRecordsForTests(), "records drained");
            assertEquals(6, spills.drainedRoomForTests(), "the room of the drained records");
            assertEquals(6, spills.loggedValuesForTests(), "values moved to the log");
            assertEquals(0, spills.heldForTests(), "bytes the budget counts");
            verifyLarge(ledger, expected);
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("the values of a group the log wrote through move to the log when each is long enough to read back from the device, and stay when one is not")
    void writtenThroughLongValuesMove(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(0, true);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        var length = SpillQueue.THROUGH_VALUE_MIN;
        var alone = Version.ofOwn(1L << 20, large(1, length), null, null, null);
        var record = queue.record(1, 1);
        record.add(alone, 0, length);
        record.locate(view, 64, 0, 0, true);
        // Outside the budget, durable as a queue without a log takes every commit, so it moves at once
        queue.add(record);
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, alone.sourceForTests(), "a long value alone in a group written through");
        assertEquals(0, queue.heldForTests());
        var beside = Version.ofOwn(2L << 20, large(2, length), null, null, null);
        var small = spillable(2L << 20 | 1, 4_000);
        var mixed = queue.record(2, 2);
        mixed.add(beside, 0, length);
        mixed.add(small, length, 4_000);
        mixed.locate(view, 1 << 20, 0, 0, true);
        queue.add(mixed);
        var shorter = Version.ofOwn(3L << 20, large(3, length - 1), null, null, null);
        var under = queue.record(3, 1);
        under.add(shorter, 0, length - 1);
        under.locate(view, 2 << 20, 0, 0, true);
        queue.add(under);
        queue.drain(() -> 3);
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, alone.sourceForTests(), "a long value alone in a group written through");
        assertInstanceOf(byte[].class, beside.sourceForTests(), "a long value beside a short one");
        assertInstanceOf(MemorySegment.class, small.sourceForTests(), "a short value beside a long one");
        assertInstanceOf(byte[].class, shorter.sourceForTests(), "a value just short of the bound");
        assertEquals(1, queue.loggedValuesForTests(), "values moved to the log");
        assertEquals(0, queue.heldForTests());
    }

    @Test
    @DisplayName("a commit of a value on an array of its own that the log writes through leaves the heap for the log, and reads back as written")
    void writtenThroughOwnValuesMove() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var expected = new TreeMap<Integer, byte[]>();
        var moved = -1;
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS))
                .segmentSize(1L << 20)
                .spillBudget(0)
                .writeThrough(WriteThrough.Mode.DSYNC)
                .build()) {
            var log = ledger.log();
            var checkpointed = false;
            for (var message = 0; message < 200 && moved < 0; message++) {
                if (!checkpointed && log.currentSegment() >= 12) {
                    // Segments enough for a checkpoint to leave spares, which the log writes through into
                    ledger.checkpoint();
                    checkpointed = true;
                }
                var value = large(message, 200_000);
                var through = log.writtenThrough();
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value)).awaitDurable();
                expected.put(message, value);
                if (log.writtenThrough() > through) {
                    moved = message;
                }
            }
            assertTrue(moved >= 0, "no commit was written through");
            spillNow(ledger);
            assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(moved)).sourceForTests(), "message " + moved);
            verifyLarge(ledger, expected);
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).build()) {
            verifyLarge(ledger, expected);
        }
    }

    /// {@return a record of the commit at `sequence` whose frame is at `frameOffset` of `view`, listing `version`, a
    /// value of `length` bytes}
    private static SpillQueue.Record listed(SpillQueue queue, long sequence, LogSegmentView view, long frameOffset,
                                            Version version, int length) {
        var record = queue.record(sequence, 1);
        record.add(version, 0, length);
        record.locate(view, frameOffset, 0, 0, false);
        return record;
    }

    @Test
    @DisplayName("records of values of 128 KiB or more on arrays of their own leave the heap once durable outside the budget, interleaved in commit order with the records the budget counts, which alone count in the held bytes")
    void ownLongValuesSkipTheBudget(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(10_000, true);
        var durable = new java.util.concurrent.atomic.AtomicLong();
        queue.useDurable(durable::get);
        var own = large(0, SpillQueue.THROUGH_VALUE_MIN);
        var versions = new Version[7];
        var views = new LogSegmentView[7];
        for (var sequence = 1; sequence <= 6; sequence++) {
            views[sequence] = queue.segment(sequence, directory.resolve(sequence + ".log"), null);
            // Even sequences list a long value on an array of its own, odd ones a value of a shared array
            var eager = sequence % 2 == 0;
            var length = eager ? own.length : 4_000;
            versions[sequence] = eager ? Version.ofOwn((long) sequence << 20, own, null, null, null) : spillable((long) sequence << 20, length);
            queue.add(listed(queue, sequence, views[sequence], 64, versions[sequence], length));
        }
        queue.awaitIdleForTests();
        assertEquals(12_000, queue.heldForTests(), "bytes the budget counts");
        assertArrayEquals(new long[]{2, 4, 6}, queue.eagerSequencesForTests(), "records outside the budget");
        assertEquals(0, queue.drainedRecordsForTests(), "records drained before any commit is durable");
        durable.set(4);
        queue.durableAdvanced();
        queue.awaitIdleForTests();
        for (var sequence = 1; sequence <= 4; sequence++) {
            assertInstanceOf(LogValue.class, versions[sequence].sourceForTests(), "commit " + sequence);
        }
        assertInstanceOf(MemorySegment.class, versions[5].sourceForTests(), "a durable commit's value under the budget");
        assertInstanceOf(byte[].class, versions[6].sourceForTests(), "a commit not durable yet");
        assertEquals(4_000, queue.heldForTests(), "bytes the budget counts");
        assertArrayEquals(new long[]{6}, queue.eagerSequencesForTests(), "records outside the budget");
        assertArrayEquals(new long[]{1, 2, 3, 4}, queue.segmentsForTests());
        assertEquals(4, queue.drainedRecordsForTests());
        // A checkpoint holding commit 5 drops its record, not the later one
        queue.published(5, 1, 6);
        assertEquals(0, queue.heldForTests());
        assertArrayEquals(new long[]{6}, queue.eagerSequencesForTests());
        queue.published(6, 1, 6);
        assertArrayEquals(new long[0], queue.eagerSequencesForTests());
        durable.set(6);
        queue.durableAdvanced();
        queue.awaitIdleForTests();
        assertInstanceOf(byte[].class, versions[6].sourceForTests(), "a value a checkpoint holds before it moved");
        assertEquals(4, queue.drainedRecordsForTests());
    }

    @Test
    @DisplayName("a durable record outside the budget moves at once, but the drain it starts moves no record the budget counts while the queue holds no more than its budget, even above three quarters of it")
    void eagerRecordsKeepTheBudget(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(10_000, true);
        queue.useDurable(() -> 3);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        // Under the budget, over its three quarters
        var counted = spillable(1L << 20, 8_000);
        queue.add(listed(queue, 1, view, 64, counted, 8_000));
        queue.awaitIdleForTests();
        var own = Version.ofOwn(2L << 20, large(2, SpillQueue.THROUGH_VALUE_MIN), null, null, null);
        queue.add(listed(queue, 2, view, 1 << 20, own, SpillQueue.THROUGH_VALUE_MIN));
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, own.sourceForTests(), "a durable value outside the budget");
        assertInstanceOf(MemorySegment.class, counted.sourceForTests(), "a value within the budget moved");
        assertEquals(8_000, queue.heldForTests());
        assertEquals(1, queue.drainedRecordsForTests());
        // Over the budget, the drain moves the oldest records down to three quarters of it
        var newer = spillable(3L << 20, 4_000);
        queue.add(listed(queue, 3, view, 2 << 20, newer, 4_000));
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, counted.sourceForTests(), "the oldest value over the budget");
        assertInstanceOf(MemorySegment.class, newer.sourceForTests(), "a value within three quarters of the budget");
        assertEquals(4_000, queue.heldForTests());
    }

    @Test
    @DisplayName("a record the budget kept queued in an older segment while records outside it moved to a newer one notes its segment in order, so a checkpoint covers it and keeps it for a reader between its commit and the checkpoint")
    void segmentsStayOrdered(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(10_000, true);
        queue.useDurable(() -> 2);
        var first = queue.segment(1, directory.resolve("1.log"), null);
        var second = queue.segment(2, directory.resolve("2.log"), null);
        var counted = spillable(1L << 20, 4_000);
        queue.add(listed(queue, 1, first, 64, counted, 4_000));
        var own = Version.ofOwn(2L << 20, large(2, SpillQueue.THROUGH_VALUE_MIN), null, null, null);
        queue.add(listed(queue, 2, second, 64, own, SpillQueue.THROUGH_VALUE_MIN));
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, own.sourceForTests(), "a durable value outside the budget");
        assertInstanceOf(MemorySegment.class, counted.sourceForTests(), "a value under the budget");
        assertArrayEquals(new long[]{2}, queue.segmentsForTests());
        // Over the budget: commit 1 moves, commit 3 is not durable
        queue.add(listed(queue, 3, second, 1 << 20, spillable(3L << 20, 8_000), 8_000));
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, counted.sourceForTests(), "a durable value over the budget");
        assertArrayEquals(new long[]{1, 2}, queue.segmentsForTests(), "the segments by number");
        var latest = new java.util.concurrent.atomic.AtomicLong(1);
        var readers = new ReaderTable(16, latest::get);
        var reader = readers.register();
        latest.set(3);
        // A checkpoint holding commit 2 that keeps the log from segment 2
        queue.published(2, 2, 2);
        assertEquals(1, queue.retirableBefore(2, readers), "the first segment kept for a reader at commit 1");
        assertFalse(first.retired());
        readers.unregister(reader);
        assertEquals(2, queue.retirableBefore(2, readers));
        assertTrue(first.retired());
        assertFalse(second.retired());
        assertArrayEquals(new long[]{2}, queue.segmentsForTests());
    }

    @Test
    @DisplayName("a record outside the budget queued after its commit became durable moves without another durable group, one queued before moves at the next, and a checkpoint or a close drops one still queued")
    void ownLongValuesMoveOnceDurable(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(64L << 20, true);
        var durable = new java.util.concurrent.atomic.AtomicLong(1);
        queue.useDurable(durable::get);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        var own = large(0, SpillQueue.THROUGH_VALUE_MIN);
        var versions = new Version[5];
        for (var sequence = 1; sequence < versions.length; sequence++) {
            versions[sequence] = Version.ofOwn((long) sequence << 20, own, null, null, null);
        }
        queue.add(listed(queue, 1, view, 64, versions[1], own.length));
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, versions[1].sourceForTests(), "a record queued once durable");
        queue.add(listed(queue, 2, view, 1 << 20, versions[2], own.length));
        queue.awaitIdleForTests();
        assertInstanceOf(byte[].class, versions[2].sourceForTests(), "a record not durable yet");
        assertArrayEquals(new long[]{2}, queue.eagerSequencesForTests());
        durable.set(2);
        queue.durableAdvanced();
        queue.awaitIdleForTests();
        assertInstanceOf(LogValue.class, versions[2].sourceForTests(), "a record durable at the next group");
        assertArrayEquals(new long[0], queue.eagerSequencesForTests());
        queue.add(listed(queue, 3, view, 2 << 20, versions[3], own.length));
        queue.published(3, 0, 3);
        assertArrayEquals(new long[0], queue.eagerSequencesForTests(), "records a checkpoint holds");
        queue.add(listed(queue, 4, view, 3 << 20, versions[4], own.length));
        queue.close();
        assertArrayEquals(new long[0], queue.eagerSequencesForTests(), "records of a closed queue");
        durable.set(4);
        queue.durableAdvanced();
        queue.awaitIdleForTests();
        assertInstanceOf(byte[].class, versions[3].sourceForTests());
        assertInstanceOf(byte[].class, versions[4].sourceForTests());
        assertEquals(2, queue.loggedValuesForTests(), "values moved to the log");
    }

    @Test
    @Timeout(60)
    @DisplayName("a record outside the budget queued while the log makes its commit durable moves, whichever of the two comes first")
    void ownLongValuesRaceDurability(@TempDir Path directory) throws Exception {
        var queue = new SpillQueue(64L << 20, true);
        var durable = new java.util.concurrent.atomic.AtomicLong();
        queue.useDurable(durable::get);
        var view = queue.segment(1, directory.resolve("1.log"), null);
        var own = large(0, SpillQueue.THROUGH_VALUE_MIN);
        var barrier = new java.util.concurrent.CyclicBarrier(2);
        var commits = 500;
        var versions = new Version[commits + 1];
        for (var sequence = 1; sequence <= commits; sequence++) {
            versions[sequence] = Version.ofOwn((long) sequence << 20, own, null, null, null);
        }
        var log = Thread.ofPlatform().start(() -> {
            try {
                for (var sequence = 1; sequence <= commits; sequence++) {
                    barrier.await();
                    durable.set(sequence);
                    queue.durableAdvanced();
                }
            } catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        });
        for (var sequence = 1; sequence <= commits; sequence++) {
            var record = listed(queue, sequence, view, 64L + ((long) sequence << 20), versions[sequence], own.length);
            barrier.await();
            queue.add(record);
        }
        log.join();
        for (var sequence = 1; sequence <= commits; sequence++) {
            while (!(versions[sequence].sourceForTests() instanceof LogValue)) {
                Thread.sleep(1);
            }
        }
        assertEquals(commits, queue.loggedValuesForTests(), "values moved to the log");
    }

    @Test
    @Timeout(120)
    @DisplayName("a commit under the budget whose segment no value moved to yet, then a value of 256 KiB that moves at once to a newer segment, keeps the older segment for a snapshot from between its commit and a checkpoint that keeps the log from the newer one")
    void budgetedOlderSegmentKeptForReaders(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 64 << 10, Duration.ZERO, 1L << 20)) {
            var log = ledger.log();
            // Ten values of 1,500 bytes on their batch's arrays, a record the budget counts and keeps
            var batch = ledger.newBatch();
            for (var message = 0; message < 10; message++) {
                var value = large(message, 1_500);
                batch.put(MESSAGES, key(message), value);
                expected.put(message, value);
            }
            ledger.commit(batch).awaitDurable();
            var older = log.currentSegment();
            var old = ledger.snapshot();
            var seen = new TreeMap<>(expected);
            // Resident values, which never move, until the log rolls to a newer segment
            for (var filler = 0; log.currentSegment() == older; filler++) {
                ledger.commit(ledger.newBatch().put(CHATS, "f" + filler, "x".repeat(100_000))).awaitDurable();
            }
            var newer = log.currentSegment();
            var own = large(50, 256 << 10);
            ledger.commit(ledger.newBatch().put(MESSAGES, key(50), own)).awaitDurable();
            expected.put(50, own);
            var spills = ledger.spills();
            while (!(head(ledger, MESSAGES, key(50)).sourceForTests() instanceof LogValue)) {
                Thread.sleep(1);
            }
            assertInstanceOf(MemorySegment.class, head(ledger, MESSAGES, key(0)).sourceForTests(), "a value under the budget");
            // Commits of short values until the queue passes its budget and the older commit's values move
            for (var message = 100; !(head(ledger, MESSAGES, key(0)).sourceForTests() instanceof LogValue); message += 10) {
                assertTrue(message < 1_000, "the older commit's values never moved");
                var more = ledger.newBatch();
                for (var i = message; i < message + 10; i++) {
                    var value = large(i, 1_500);
                    more.put(MESSAGES, key(i), value);
                    expected.put(i, value);
                }
                ledger.commit(more).awaitDurable();
                spills.awaitIdleForTests();
            }
            assertEquals(newer, log.currentSegment(), "the log did not roll again");
            assertArrayEquals(new long[]{older, newer}, spills.segmentsForTests(), "the segments by number");
            ledger.checkpoint();
            var kept = spills.segmentsForTests();
            assertTrue(kept.length > 0 && kept[0] == older, "the checkpoint retired a segment a snapshot needs");
            verifyLarge(old, seen);
            old.close();
            ledger.checkpointer().tick();
            assertTrue(Arrays.stream(spills.segmentsForTests()).noneMatch(number -> number == older));
            verifyLarge(ledger, expected);
        }
        try (var ledger = openSized(directory, true, 64 << 10, Duration.ZERO, 1L << 20)) {
            verifyLarge(ledger, expected);
        }
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
    @DisplayName("a value decrypted in place is the value AES-CTR from the body's first counter block decrypts, wherever it starts and however long it is, and the decryption allocates a few hundred bytes whatever the value's length")
    void decryptsInPlaceAsCounterMode() throws Exception {
        var queue = new SpillQueue(0, true);
        var crypto = queue.takeCrypto();
        var random = new Random(3);
        var bytes = new byte[32];
        random.nextBytes(bytes);
        var key = new SecretKeySpec(bytes, "AES");
        var body = new byte[700_000];
        random.nextBytes(body);
        for (var frameOffset : new long[]{0, 4_096, 0x1_0000_1000L}) {
            var counter = new byte[SpillQueue.BLOCK];
            System.arraycopy(LogWriter.nonce(frameOffset), 0, counter, 0, 12);
            Bytes.i32(counter, 12, 2);
            var ctr = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
            ctr.init(javax.crypto.Cipher.ENCRYPT_MODE, key, new javax.crypto.spec.IvParameterSpec(counter));
            var sealedBody = ctr.doFinal(body);
            // Lengths around the pieces of the scratch, and up to several of them
            var lengths = new int[]{1, 15, 16, 17, SpillQueue.KEPT_SCRATCH - 1, SpillQueue.KEPT_SCRATCH,
                    SpillQueue.KEPT_SCRATCH + 1, 3 * SpillQueue.KEPT_SCRATCH + 5, 256 << 10, 600_000};
            for (var length : lengths) {
                for (var trial = 0; trial < 4; trial++) {
                    var bodyIndex = random.nextInt(body.length - length);
                    var at = random.nextInt(40);
                    var value = new byte[at + length + random.nextInt(40)];
                    System.arraycopy(sealedBody, bodyIndex, value, at, length);
                    crypto.decryptInPlace(key, frameOffset, bodyIndex, value, at, length);
                    assertArrayEquals(Arrays.copyOfRange(body, bodyIndex, bodyIndex + length),
                            Arrays.copyOfRange(value, at, at + length),
                            "at " + bodyIndex + " for " + length + " bytes of the frame at " + frameOffset);
                }
            }
        }
        var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        var value = new byte[256 << 10];
        var allocated = 0L;
        for (var round = 0; round < 3; round++) {
            var before = threads.getCurrentThreadAllocatedBytes();
            for (var i = 0; i < 200; i++) {
                crypto.decryptInPlace(key, 4_096, 3 + 16 * i, value, 0, value.length);
            }
            allocated = (threads.getCurrentThreadAllocatedBytes() - before) / 200;
        }
        assertTrue(allocated < 512, "decrypting in place allocated " + allocated + " bytes per value");
        queue.giveBack(crypto);
    }

    @Test
    @DisplayName("the polynomials of a tag, four words at a time, are those Horner's rule gives a word at a time, whatever the value's length and offset")
    void tagsAsHornersRule() {
        var queue = new SpillQueue(0, true);
        var crypto = queue.takeCrypto();
        var random = new Random(4);
        var value = new byte[2_000];
        random.nextBytes(value);
        // Every byte set, so a word is as large as one gets
        Arrays.fill(value, 900, 1_000, (byte) 0xFF);
        for (var length = 0; length < 300; length++) {
            for (var offset : new int[]{0, 1, 7, 900 - length / 2}) {
                var stamp = random.nextLong();
                var tag = crypto.tagForTests(value, offset, length, stamp);
                for (var i = 0; i < 2; i++) {
                    var point = tag[2 + i];
                    var expected = point;
                    expected = SpillQueue.Crypto.multiply(expected + (stamp >>> 8), point);
                    expected = SpillQueue.Crypto.multiply(expected + (stamp & 0xFF), point);
                    for (var at = offset; at < offset + length; at += 7) {
                        var word = 0L;
                        for (var j = at; j < Math.min(at + 7, offset + length); j++) {
                            word = (word << 8) | (value[j] & 0xFF);
                        }
                        expected = SpillQueue.Crypto.multiply(expected + word, point);
                    }
                    expected = SpillQueue.Crypto.multiply(expected + length, point);
                    assertEquals(expected, tag[i], "polynomial " + i + " of " + length + " bytes at " + offset);
                }
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

    /// A workload drawn from a seed: batches, some durable and some of one value on an array of its own, keys written
    /// again, removals, checkpoints and reopens, the queue moving every durable value; and the state after each commit,
    /// the last it made visible and the last it knows durable.
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
                    } else if (choice == 3) {
                        // A value on an array of its own, which moves to the log whatever the key space's samples
                        var next = state.copy();
                        var message = random.nextInt(60);
                        var value = large(message + step * 1_000, 140_000 + random.nextInt(160_000));
                        var commit = ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
                        next.messages.put(message, value);
                        commit(next);
                        if (random.nextBoolean()) {
                            commit.awaitDurable();
                            durable = visible;
                        }
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

    /// Commits a rope batch of `count` messages from `first`, each of one of `lengths`, with a value of `ownLength`
    /// bytes on an array of its own among them and a chat, and records them in `expected`.
    private static void writeSplit(Ledger ledger, Expected expected, int first, int count, int[] lengths, int ownLength)
            throws Exception {
        var batch = ledger.newBatch();
        for (var message = first; message < first + count; message++) {
            var value = message == first + count / 2 ? large(message, ownLength) : new byte[lengths[message % lengths.length]];
            if (value.length != ownLength) {
                for (var i = 0; i < value.length; i++) {
                    value[i] = (byte) (message * 11 + i * 3);
                }
            }
            batch.put(MESSAGES, key(message), value);
            expected.messages.put(message, value);
            if (message % 500 == 0) {
                batch.put(CHATS, "split" + message, "chat " + message);
                expected.chats.put("split" + message, "chat " + message);
            }
        }
        ledger.commit(batch).awaitDurable();
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("the values a split frame leaves to its section, a value of 200 KiB on an array of its own among them, leave the heap from where the section put them, and read back as written before and after a checkpoint and a recovery")
    void splitSectionValuesMove(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var count = 2_000;
        int[] lengths = {SpillQueue.LOG_VALUE_MIN, 700, 1_500};
        var own = 200 << 10;
        try (var ledger = openSized(directory, encrypted, 0, Duration.ZERO, 16L << 20)) {
            writeSplit(ledger, expected, 0, count, lengths, own);
            spillNow(ledger);
            var spills = ledger.spills();
            assertEquals(count, spills.loggedValuesForTests(), "values moved to the log");
            for (var message = 0; message < count; message++) {
                assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(message)).sourceForTests(), "message " + message);
            }
            verify(ledger, expected);
            // The checkpoint reads them back from the section
            ledger.checkpoint();
            verify(ledger, expected);
            // Replayed by the recovery, which moves them from the section again
            writeSplit(ledger, expected, count, count, lengths, own);
            spillNow(ledger);
            assertInstanceOf(LogValue.class, head(ledger, MESSAGES, key(count + count / 2)).sourceForTests());
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = openSized(directory, encrypted, 0, Duration.ZERO, 16L << 20)) {
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = openSized(directory, encrypted, 0, Duration.ZERO, 16L << 20)) {
            verify(ledger, expected);
        }
    }

    /// Commits a bulk batch of `count` messages from `first` of 100 to 300 bytes, as an app-state snapshot's: short
    /// values whose move one by one would not pay, but some long enough that the commit lists them, about 200 bytes
    /// each, so a split frame's value frames hold the homes of the run the commit keeps them in; records them in
    /// `expected`.
    ///
    /// @return the commit, not waited for
    private static Commit writeBulk(Ledger ledger, TreeMap<Integer, byte[]> expected, int first, int count) {
        var batch = ledger.newBatch();
        for (var message = first; message < first + count; message++) {
            var value = large(message, 100 + message % 201);
            batch.put(MESSAGES, key(message), value);
            expected.put(message, value);
        }
        return ledger.commit(batch);
    }

    /// {@return the run of the bulk commit of `MESSAGES`}
    private static PackedRun bulkRun(LiveLedger ledger) {
        var run = ledger.state(MESSAGES).run;
        assertNotNull(run, "the bulk commit made no run");
        assertTrue(run.committed());
        return run;
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a bulk commit of 10 MB of short values lets go of every home of its run once durable, before its wait returns, and reads each back from its value frame at the first read, before and after a checkpoint and a reopen")
    void framedHomes(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, encrypted, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var run = bulkRun(ledger);
            var homes = run.framedHomesForTests();
            assertTrue(homes > 30, homes + " homes let go of");
            assertEquals(0, run.homesForTests(), "homes the run still holds once its commit's wait returned");
            assertTrue(ledger.spills().droppedHomesForTests() >= SpillQueue.FRAMED_HOMES_MIN);
            assertEquals(0, ledger.spills().eagerSequencesForTests().length, "a record of the dropped homes still queued");
            // The first read of a value reads its home back, and the home stays
            assertArrayEquals(expected.get(7), ledger.get(MESSAGES, key(7)).orElseThrow());
            assertEquals(1, run.homesForTests());
            verifyLarge(ledger, expected);
            assertEquals(homes, run.homesForTests(), "every home read back");
            ledger.checkpoint();
            verifyLarge(ledger, expected);
            // A second bulk commit, over a tree that holds the first: its puts go to chains, whose versions keep their
            // arrays as they did
            var more = ledger.newBatch();
            for (var message = 50_000; message < 50_100; message++) {
                var value = large(message, 300);
                more.put(MESSAGES, key(message), value);
                expected.put(message, value);
            }
            ledger.commit(more).awaitDurable();
            verifyLarge(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = openSized(directory, encrypted, 1 << 20, Duration.ZERO, 64L << 20)) {
            verifyLarge(ledger, expected);
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a bulk commit that appends to the run of an earlier one lets go of its own homes once durable, by their indexes in the arrays the views share, the earlier commit's homes staying as its drop left them, and every value of both reads back, before and after a checkpoint and a reopen")
    void framedHomesAppended(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, encrypted, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var first = bulkRun(ledger);
            var firstHomes = first.framedHomesForTests();
            assertTrue(firstHomes > 30, firstHomes + " homes let go of");
            // One read of the first commit's homes back, which the second commit's drop leaves as it is
            assertArrayEquals(expected.get(7), ledger.get(MESSAGES, key(7)).orElseThrow());
            assertEquals(1, first.homesForTests());
            writeBulk(ledger, expected, 50_000, 50_000).awaitDurable();
            var second = bulkRun(ledger);
            assertNotSame(first, second);
            assertTrue(second.sharesArraysForTests(first), "the second bulk commit did not append to the run");
            assertEquals(100_000, second.size(), "the appending commit's view does not hold both commits' entries");
            var homes = second.framedHomesForTests();
            assertTrue(homes > firstHomes + 30, homes + " homes let go of after the append, " + firstHomes + " before");
            assertEquals(1, second.homesForTests(), "homes the run still holds once the appending commit's wait returned");
            assertEquals(0, ledger.spills().eagerSequencesForTests().length, "a record of the dropped homes still queued");
            assertArrayEquals(expected.get(60_007), ledger.get(MESSAGES, key(60_007)).orElseThrow());
            assertEquals(2, second.homesForTests());
            verifyLarge(ledger, expected);
            assertEquals(homes, second.homesForTests(), "every home read back");
            ledger.checkpoint();
            verifyLarge(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = openSized(directory, encrypted, 1 << 20, Duration.ZERO, 64L << 20)) {
            verifyLarge(ledger, expected);
            ledger.checkpoint();
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit's checkpoint reads back every home its run let go of before it merges the run, and the close's checkpoint does too")
    void framedHomesCheckpoint(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var run = bulkRun(ledger);
            assertEquals(0, run.homesForTests());
            ledger.checkpoint();
            assertEquals(run.framedHomesForTests(), run.homesForTests(), "homes the checkpoint read back");
            verifyLarge(ledger, expected);
        }
        // The close's checkpoint, of a store whose tree is empty before the bulk commit, as a bulk commit needs
        var second = directory.resolve("second");
        var written = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(second, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, written, 100_000, 50_000).awaitDurable();
            assertEquals(0, bulkRun(ledger).homesForTests());
        }
        try (var ledger = openSized(second, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            verifyLarge(ledger, written);
        }
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            verifyLarge(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a checkpoint's read-back of the homes a run let go of allocates the homes and one frame's array per reading thread, not an array per home")
    void framedHomesReadBackAllocatesTheHomes(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, encrypted, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var run = bulkRun(ledger);
            var homes = run.framedHomesForTests();
            assertTrue(homes > 30, homes + " homes let go of");
            assertEquals(0, run.homesForTests());
            var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            var before = threads.getTotalThreadAllocatedBytes();
            run.readBackHomes();
            var allocated = threads.getTotalThreadAllocatedBytes() - before;
            assertEquals(homes, run.homesForTests(), "homes read back");
            // Each reading thread's array holds the longest frame, 256 KiB of body at most; 1 MiB covers the threads,
            // the ciphers and the other threads of the store
            var bound = run.framedBytesForTests() + (long) Runtime.getRuntime().availableProcessors() * ((256 << 10) + 64) + (1 << 20);
            assertTrue(allocated < bound, "the read-back of " + homes + " homes of " + run.framedBytesForTests()
                    + " bytes allocated " + allocated + " bytes, over " + bound);
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit of 4 MB of short values, under the homes a run lets go of, keeps them, and its record leaves the queue")
    void smallBulkKeepsHomes(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 20_000).awaitDurable();
            var run = bulkRun(ledger);
            assertEquals(0, run.framedHomesForTests());
            assertTrue(run.homesForTests() > 10);
            assertEquals(0, ledger.spills().eagerSequencesForTests().length);
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit no one waits for lets go of its homes on the thread that moves values")
    void framedHomesUnwaited(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            var commit = writeBulk(ledger, expected, 0, 50_000);
            var run = bulkRun(ledger);
            // Durable once the log's flush is done; the thread that moves values then lets go of the homes
            while (ledger.log().durableSequence() < commit.sequence()) {
                Thread.sleep(1);
            }
            var spills = ledger.spills();
            for (var waited = 0; run.framedHomesForTests() == 0; waited++) {
                assertTrue(waited < 10_000, "the homes never left");
                spills.awaitIdleForTests();
                Thread.sleep(1);
            }
            spills.awaitIdleForTests();
            assertEquals(0, spills.eagerSequencesForTests().length);
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("the homes a run let go of leave the heap: nothing but the run held them")
    void framedHomesUnreachable() throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        var disk = CrashFileSystem.create();
        try (var ledger = openSized(disk.getPath("/store"), true, 1 << 20, Duration.ZERO, 64L << 20)) {
            ledger.commit(ledger.newBatch().put(CHATS, "warm", "x")).awaitDurable();
            var reached = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".log")) {
                    reached.countDown();
                    try {
                        assertTrue(release.await(2, TimeUnit.MINUTES));
                    } catch (InterruptedException exception) {
                        throw new java.io.IOException(exception);
                    }
                }
            });
            Commit commit;
            List<WeakReference<Object>> weak;
            try {
                // A first commit whose group's write holds the writer, so the bulk commit's frames wait in the queue
                ledger.commit(ledger.newBatch().put(CHATS, "first", "x"));
                assertTrue(reached.await(2, TimeUnit.MINUTES));
                commit = writeBulk(ledger, expected, 0, 50_000);
                weak = homesOf(bulkRun(ledger));
                assertTrue(weak.size() > 30);
            } finally {
                // Every later write passes the interceptor at once
                release.countDown();
            }
            commit.awaitDurable();
            for (var round = 0; round < 50 && weak.stream().anyMatch(reference -> reference.get() != null); round++) {
                System.gc();
                Thread.sleep(20);
            }
            assertTrue(weak.stream().allMatch(reference -> reference.get() == null), "a home let go of is still reachable");
            verifyLarge(ledger, expected);
        }
    }

    /// {@return a weak reference to each home `run` holds}
    private static List<WeakReference<Object>> homesOf(PackedRun run) {
        var homes = new ArrayList<WeakReference<Object>>();
        for (var entry = 0; entry < run.size(); entry++) {
            var home = run.sourceForTests(entry);
            if (home != null && homes.stream().noneMatch(reference -> reference.get() == home)) {
                homes.add(new WeakReference<>(home));
            }
        }
        return homes;
    }

    @RepeatedTest(5)
    @DisplayName("readers of a run's homes let go of read every value at once, each home read back by one of them, while a checkpoint merges the run")
    void framedHomesConcurrentReads(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var run = bulkRun(ledger);
            var homes = run.framedHomesForTests();
            assertEquals(0, run.homesForTests());
            var start = new CountDownLatch(1);
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var readers = new ArrayList<Thread>();
            for (var reader = 0; reader < 8; reader++) {
                var seed = reader;
                readers.add(Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        var random = new Random(seed);
                        for (var i = 0; i < 5_000; i++) {
                            var message = random.nextInt(50_000);
                            assertArrayEquals(expected.get(message), ledger.get(MESSAGES, key(message)).orElseThrow(), "message " + message);
                        }
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                }));
            }
            var checkpoint = Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    ledger.checkpoint();
                } catch (Throwable failure) {
                    failures.add(failure);
                }
            });
            start.countDown();
            for (var reader : readers) {
                reader.join();
            }
            checkpoint.join();
            assertTrue(failures.isEmpty(), () -> failures.peek().toString());
            assertEquals(homes, run.homesForTests());
            verifyLarge(ledger, expected);
        }
    }

    @ParameterizedTest(name = "checkpoint before the wait: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a snapshot from before a checkpoint that takes a bulk commit's run reads every value of the run, the homes let go of included, while the checkpoint keeps the segment of their value frames, which retires once the snapshot closes")
    void framedHomesSegmentKeptForSnapshot(boolean early, @TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 16L << 20)) {
            var log = ledger.log();
            var commit = writeBulk(ledger, expected, 0, 50_000);
            var run = bulkRun(ledger);
            var older = log.currentSegment();
            var snapshot = ledger.snapshot();
            var seen = new TreeMap<>(expected);
            if (early) {
                // A checkpoint whose publication may come before or after the run lets go of its homes
                ledger.checkpoint();
            }
            commit.awaitDurable();
            // Resident values, which never move, until the log rolls to a newer segment
            for (var filler = 0; log.currentSegment() == older; filler++) {
                ledger.commit(ledger.newBatch().put(CHATS, "f" + filler, "x".repeat(100_000))).awaitDurable();
            }
            ledger.checkpoint();
            var spills = ledger.spills();
            if (run.framedHomesForTests() > 0) {
                assertTrue(Arrays.stream(spills.segmentsForTests()).anyMatch(number -> number == older),
                        "the checkpoint retired the segment of the value frames a snapshot reads");
            }
            verifyLarge(snapshot, seen);
            snapshot.close();
            ledger.checkpointer().tick();
            assertTrue(Arrays.stream(spills.segmentsForTests()).noneMatch(number -> number == older));
            verifyLarge(ledger, expected);
        }
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 16L << 20)) {
            verifyLarge(ledger, expected);
        }
    }

    @Test
    @DisplayName("a read of a home let go of whose value frame is damaged reports the damage, and the home stays let go of")
    void framedHomeDamaged(@TempDir Path directory) throws Exception {
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = openSized(directory, true, 1 << 20, Duration.ZERO, 64L << 20)) {
            writeBulk(ledger, expected, 0, 50_000).awaitDurable();
            var run = bulkRun(ledger);
            assertTrue(run.framedHomesForTests() > 0);
            var name = LogWriter.segmentName(ledger.log().currentSegment());
            Path path;
            try (var files = Files.walk(directory)) {
                path = files.filter(file -> file.getFileName().toString().equals(name)).findFirst().orElseThrow();
            }
            // A byte of every value frame's body: the frames sit past the head, about 256 KiB each
            try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                var size = channel.size();
                var one = ByteBuffer.allocate(1);
                for (var at = (long) LogWriter.SEGMENT_HEADER + 4_096; at < size; at += 8_192) {
                    one.clear();
                    channel.read(one, at);
                    one.flip();
                    var flipped = (byte) (one.get(0) ^ 0x40);
                    channel.write(ByteBuffer.wrap(new byte[]{flipped}), at);
                }
            }
            assertThrows(LedgerCorruptedException.class, () -> ledger.get(MESSAGES, key(7)));
            assertEquals(0, run.homesForTests());
            ledger.closeWithoutCheckpoint();
        }
    }

    static IntStream framedSeeds() {
        return IntStream.range(0, 12);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("framedSeeds")
    @DisplayName("a store whose bulk commit's run let go of its homes recovers that commit and a state between its last durable commit and its last visible one, whether a crash or a loss of power stops it as it reads the homes back, writes after them, checkpoints them or closes")
    void framedHomesRecoverAfterCrash(int seed) throws Exception {
        var encrypted = seed % 2 == 0;
        var dry = CrashFileSystem.create();
        var counted = new FramedWorkload(seed, encrypted);
        counted.run(dry.getPath("/store"));
        assertTrue(counted.dropped, "the bulk commit's run let go of no home");
        var disk = CrashFileSystem.create();
        var random = new Random(seed * 7919L + 5);
        // Past the bulk commit, durable before the workload goes on
        disk.stopAfter(counted.bulkChanges + random.nextLong(dry.changes() - counted.bulkChanges + 1));
        var workload = new FramedWorkload(seed, encrypted);
        workload.run(disk.getPath("/store"));
        var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
        try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
            var read = new TreeMap<Integer, byte[]>();
            ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                read.put(ByteBuffer.wrap(key).getInt(), value);
                return true;
            });
            if (workload.bulkChanges == 0) {
                // This run took more changes to make the bulk commit durable than the dry run, and stopped before it was
                assertTrue(read.isEmpty() || read.size() == workload.bulk.size(), read.size() + " messages");
                return;
            }
            for (var entry : workload.bulk.entrySet()) {
                assertArrayEquals(entry.getValue(), read.remove(entry.getKey()), "bulk message " + entry.getKey());
            }
            var matched = false;
            for (var commit = workload.durable; commit <= workload.visible && !matched; commit++) {
                matched = sameMessages(read, workload.history.get(commit));
            }
            assertTrue(matched, "the store recovered a state no commit from " + workload.durable + " to " + workload.visible + " left");
            for (var message = 100_000; message < 150_000; message += 97) {
                assertArrayEquals(workload.bulk.get(message), ledger.get(MESSAGES, key(message)).orElseThrow(), "bulk message " + message);
            }
        }
    }

    /// {@return whether `read` holds exactly the messages of `expected`}
    private static boolean sameMessages(TreeMap<Integer, byte[]> read, TreeMap<Integer, byte[]> expected) {
        if (!read.keySet().equals(expected.keySet())) {
            return false;
        }
        for (var entry : expected.entrySet()) {
            if (!Arrays.equals(entry.getValue(), read.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /// A workload drawn from a seed whose first commit is a durable bulk commit of 50,000 short messages, whose run lets
    /// go of its homes, followed by reads of them, small batches of other keys, some durable, checkpoints and a close;
    /// and the small batches' state after each commit, the last it made visible and the last it knows durable.
    private static final class FramedWorkload {
        private final Random random;
        private final boolean encrypted;
        final TreeMap<Integer, byte[]> bulk = new TreeMap<>();
        final List<TreeMap<Integer, byte[]>> history = new ArrayList<>(List.of(new TreeMap<>()));
        int visible;
        int durable;
        // The file system's changes once the bulk commit is durable, and whether its run let go of its homes
        long bulkChanges;
        boolean dropped;

        FramedWorkload(int seed, boolean encrypted) {
            this.random = new Random(seed);
            this.encrypted = encrypted;
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted, 0);
                writeBulk(ledger, bulk, 100_000, 50_000).awaitDurable();
                dropped = ledger.state(MESSAGES).run.framedHomesForTests() > 0;
                bulkChanges = changes(directory);
                for (var step = 0; step < 20; step++) {
                    var choice = random.nextInt(10);
                    if (choice == 0) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (choice == 1) {
                        for (var i = 0; i < 200; i++) {
                            var message = 100_000 + random.nextInt(50_000);
                            assertArrayEquals(bulk.get(message), ledger.get(MESSAGES, key(message)).orElseThrow());
                        }
                    } else {
                        var next = new TreeMap<>(history.getLast());
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 1 + random.nextInt(20); i++) {
                            var message = random.nextInt(500);
                            var value = large(message + step * 1_000, 100 + random.nextInt(2_000));
                            batch.put(MESSAGES, key(message), value);
                            next.put(message, value);
                        }
                        var commit = ledger.commit(batch);
                        history.add(next);
                        visible++;
                        if (random.nextBoolean()) {
                            commit.awaitDurable();
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

        /// {@return the changes the file system of `directory` made so far}
        private static long changes(Path directory) {
            return ((CrashFileSystem) directory.getFileSystem()).changes();
        }
    }
}
