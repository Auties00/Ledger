package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.WriteBatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The runs a bulk commit keeps its puts of new keys in ([PackedRun#committed()]): a batch of at least
/// [LiveLedger#BULK_RUN_WRITES] puts to a paged key space whose latest tree holds none of its entries runs alone and
/// keeps those puts in arrays, which every read, scan, count, later write, range removal, checkpoint, snapshot, spill
/// and crash sees as the chains they replaced showed them. The expected state of every test is kept beside the store and
/// checked through every read path.
class BulkCommitRunTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int BULK = LiveLedger.BULK_RUN_WRITES + 500;
    // Empty, short, at the log's threshold, between it and the verdict's paying length, and above
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 200, 700, 1_500};
    // Values that move to the log once their record drains: mostly long, so the record's verdict lets all move
    private static final int[] LONG_LENGTHS = {40, SpillQueue.LOG_VALUE_MIN, 700, 1_500, 3_000};
    private static final int LARGE_VALUES = 64 << 10;

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO)
                .largeValues(MESSAGES, LARGE_VALUES))
                .segmentSize(4L << 20)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static byte[] value(int message, int generation, int[] lengths) {
        var length = lengths[Math.floorMod(message * 7 + generation, lengths.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i);
        }
        return value;
    }

    /// What a store should hold.
    private static final class Expected {
        final TreeMap<Integer, byte[]> messages = new TreeMap<>();
        final TreeMap<String, String> texts = new TreeMap<>();

        Expected copy() {
            var copy = new Expected();
            copy.messages.putAll(messages);
            copy.texts.putAll(texts);
            return copy;
        }
    }

    /// Adds to `batch` puts of messages `first` to `first + count`, excluded, and records them in `expected`.
    private static void puts(WriteBatch batch, Expected expected, int first, int count, int generation, int[] lengths) {
        for (var message = first; message < first + count; message++) {
            var value = value(message, generation, lengths);
            batch.put(MESSAGES, key(message), value);
            expected.messages.put(message, value);
        }
    }

    /// Checks every way a view reads `expected`: point reads, encoded reads, scans both ways, ranges and counts.
    private static void verify(View view, Expected expected) {
        for (var entry : expected.messages.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, key(entry.getKey())).orElseThrow(),
                    () -> "message " + entry.getKey());
            assertArrayEquals(entry.getValue(), view.readEncoded(MESSAGES, key(entry.getKey()),
                    segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        }
        for (var entry : expected.texts.entrySet()) {
            assertEquals(entry.getValue(), view.get(TEXTS, entry.getKey()).orElseThrow(), "text " + entry.getKey());
        }
        assertEquals(expected.messages.size(), view.size(MESSAGES), "messages counted");
        assertEquals(expected.texts.size(), view.size(TEXTS), "texts counted");
        var keys = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            var message = ByteBuffer.wrap(key).getInt();
            assertArrayEquals(expected.messages.get(message), value, "message " + message + " scanned");
            keys.add(message);
            return true;
        });
        assertEquals(new ArrayList<>(expected.messages.keySet()), keys, "messages scanned");
        var descending = new ArrayList<Integer>();
        view.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (key, _) -> {
            descending.add(ByteBuffer.wrap(key).getInt());
            return true;
        });
        assertEquals(new ArrayList<>(expected.messages.descendingKeySet()), descending, "messages scanned descending");
        for (var from = 0; from < BULK + 600; from += 777) {
            var to = from + 300;
            var range = view.range(MESSAGES, KeyRange.between(key(from), key(to)), Integer.MAX_VALUE);
            var wanted = expected.messages.subMap(from, to);
            assertEquals(wanted.size(), range.size(), "messages from " + from);
            var index = 0;
            for (var entry : wanted.entrySet()) {
                assertArrayEquals(key(entry.getKey()), range.get(index).key());
                assertArrayEquals(entry.getValue(), range.get(index++).value());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a bulk commit into an empty key space keeps its puts in a run that every read sees from the commit on, sorts it at the first scan, and leaves memory with the checkpoint that takes it")
    void bulkCommitFormsRun(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            try (var before = ledger.snapshot()) {
                var batch = ledger.newBatch();
                puts(batch, expected, 0, BULK, 0, LENGTHS);
                // Below the threshold: chains
                for (var i = 0; i < 100; i++) {
                    batch.put(TEXTS, "t" + i, "text " + i);
                    expected.texts.put("t" + i, "text " + i);
                }
                batch.put(CHATS, "c", "chat");
                ledger.commit(batch).awaitDurable();
                var run = ledger.state(MESSAGES).run;
                assertNotNull(run, "the bulk commit made no run");
                assertTrue(run.committed());
                assertEquals(BULK, run.size());
                assertEquals(0, ledger.state(MESSAGES).chains(), "a put of a new key took a chain");
                assertNull(ledger.state(TEXTS).run, "a run below the threshold");
                assertFalse(run.orderedForTests(), "the commit sorted its run");
                // A snapshot before the commit sees none of it
                assertEquals(0, before.size(MESSAGES));
                assertTrue(before.get(MESSAGES, key(5)).isEmpty());
                assertTrue(before.range(MESSAGES, KeyRange.all(), 10).isEmpty());
                verify(ledger, expected);
                assertTrue(run.orderedForTests(), "a scan left the run unsorted");
            }
            // Later commits overwrite and remove keys of the run, blind and alone
            var batch = ledger.newBatch();
            for (var message = 0; message < BULK; message += 9) {
                var value = value(message, 1, LENGTHS);
                batch.put(MESSAGES, key(message), value);
                expected.messages.put(message, value);
                batch.delete(MESSAGES, key(message + 4));
                expected.messages.remove(message + 4);
            }
            batch.put(MESSAGES, key(BULK + 1), new byte[]{9});
            expected.messages.put(BULK + 1, new byte[]{9});
            ledger.commit(batch).awaitDurable();
            verify(ledger, expected);
            ledger.update(transaction -> {
                transaction.delete(MESSAGES, key(7));
                transaction.put(MESSAGES, key(8), new byte[]{8});
                assertEquals(expected.messages.size() - 1, transaction.size(MESSAGES));
            });
            expected.messages.remove(7);
            expected.messages.put(8, new byte[]{8});
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the run outlived the checkpoint that took it");
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            assertNull(ledger.state(MESSAGES).run, "a clean open replayed writes");
            verify(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a key the bulk commit writes again, removes, or gives a value the run cannot hold goes to a chain over the run's entry, counted exactly")
    void repeatedKeysInBulk(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LENGTHS);
            // Put twice, put then removed, put then removed then put, removed never put, and a value on an array of
            // its own and one in the file of large values over a key of the run
            batch.put(MESSAGES, key(3), new byte[]{3, 3});
            expected.messages.put(3, new byte[]{3, 3});
            batch.delete(MESSAGES, key(4));
            expected.messages.remove(4);
            batch.delete(MESSAGES, key(5));
            batch.put(MESSAGES, key(5), new byte[]{5});
            expected.messages.put(5, new byte[]{5});
            batch.delete(MESSAGES, key(BULK + 50));
            var own = value(6, 9, new int[]{200 << 10});
            batch.put(MESSAGES, key(6), own);
            expected.messages.put(6, own);
            var large = value(10, 9, new int[]{LARGE_VALUES + 10});
            batch.put(MESSAGES, key(10), large);
            expected.messages.put(10, large);
            // New keys whose values the run cannot hold, then put again with ones it can
            batch.put(MESSAGES, key(BULK + 2), own);
            batch.put(MESSAGES, key(BULK + 2), new byte[]{2});
            expected.messages.put(BULK + 2, new byte[]{2});
            batch.put(MESSAGES, key(BULK + 3), large);
            expected.messages.put(BULK + 3, large);
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            assertTrue(run.committed());
            assertEquals(BULK, run.size());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a bulk commit's run is not made where a tree holds the key space, a range removal is newer than the tree, the batch removes a range of it, or another run is in memory")
    void ineligibleKeyspaces(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            // A range removal before any tree
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(0), key(1)))).awaitDurable();
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LENGTHS);
            ledger.commit(batch).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "a run under a range removal newer than the tree");
            verify(ledger, expected);
            ledger.checkpoint();
            // The tree holds the key space
            batch = ledger.newBatch();
            puts(batch, expected, BULK, BULK, 0, LENGTHS);
            ledger.commit(batch).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "a run over a tree that holds the key space");
            verify(ledger, expected);
        }
        var other = directory.resolve("other");
        var otherExpected = new Expected();
        try (var ledger = open(other, encrypted, Long.MAX_VALUE)) {
            // The batch removes a range of the key space
            var batch = ledger.newBatch();
            puts(batch, otherExpected, 0, BULK, 0, LENGTHS);
            batch.deleteRange(MESSAGES, KeyRange.between(key(10), key(20)));
            otherExpected.messages.subMap(10, 20).clear();
            ledger.commit(batch).awaitDurable();
            assertNull(ledger.state(MESSAGES).run, "a run in a batch that removes a range of its key space");
            verify(ledger, otherExpected);
            // A range removal of another key space leaves the run to this one; a second bulk commit finds it there
            batch = ledger.newBatch();
            for (var i = 0; i < BULK; i++) {
                batch.put(TEXTS, "t" + i, "text " + i);
                otherExpected.texts.put("t" + i, "text " + i);
            }
            batch.deleteRange(MESSAGES, KeyRange.between(key(30), key(40)));
            otherExpected.messages.subMap(30, 40).clear();
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(TEXTS).run;
            assertNotNull(run, "no run beside a range removal of another key space");
            batch = ledger.newBatch();
            for (var i = BULK; i < 2 * BULK; i++) {
                batch.put(TEXTS, "t" + i, "text " + i);
                otherExpected.texts.put("t" + i, "text " + i);
            }
            ledger.commit(batch).awaitDurable();
            assertTrue(ledger.state(TEXTS).run == run, "a second run beside the first");
            verify(ledger, otherExpected);
            ledger.checkpoint();
            verify(ledger, otherExpected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("range removals after a bulk commit, counted later and at once, count the run's entries they hide, through a checkpoint and a crash")
    void rangeRemovalsAfterBulk(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LENGTHS);
            ledger.commit(batch).awaitDurable();
            assertNotNull(ledger.state(MESSAGES).run);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(100), key(400)))).awaitDurable();
            expected.messages.subMap(100, 400).clear();
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            ledger.update(transaction -> {
                transaction.deleteRange(MESSAGES, KeyRange.between(key(1_000), key(1_100)));
                transaction.put(MESSAGES, key(1_050), new byte[]{1});
            });
            expected.messages.subMap(1_000, 1_100).clear();
            expected.messages.put(1_050, new byte[]{1});
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("the values of a bulk commit's run leave the heap for the log and the run's chunks, read back as written, and the run lets go of its homes")
    void runValuesSpill(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        // A budget no commit reaches, so the values move only when the test drains the queue
        try (var ledger = open(directory, encrypted, 1L << 30)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LONG_LENGTHS);
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            assertTrue(run.homesForTests() > 0);
            assertTrue(run.onHomeForTests(run.find(MemorySegment.ofArray(key(1)))));
            // A reader that holds a slice of a home while the spill lets go of the home reads it unchanged
            var held = new AtomicReference<byte[]>();
            ledger.readEncoded(MESSAGES, key(1), segment -> {
                ledger.spills().drainAllForTests(ledger.log()::durableSequence);
                held.set(segment.toArray(ValueLayout.JAVA_BYTE));
                return segment;
            });
            assertArrayEquals(expected.messages.get(1), held.get());
            assertEquals(0, run.homesForTests(), "the run kept a home every value left");
            var logged = 0;
            var chunked = 0;
            for (var message = 0; message < BULK; message++) {
                var source = run.sourceForTests(run.find(MemorySegment.ofArray(key(message))));
                if (expected.messages.get(message).length >= SpillQueue.LOG_VALUE_MIN) {
                    assertInstanceOf(LogValue.class, source, "message " + message);
                    logged++;
                } else {
                    assertInstanceOf(MemorySegment.class, source, "message " + message);
                    assertFalse(run.onHomeForTests(run.find(MemorySegment.ofArray(key(message)))));
                    chunked++;
                }
            }
            assertTrue(logged > 0 && chunked > 0);
            assertTrue(ledger.spills().loggedValuesForTests() >= logged);
            verify(ledger, expected);
            // Overwrites over values in the log, then a checkpoint that reads them back
            batch = ledger.newBatch();
            for (var message = 0; message < BULK; message += 5) {
                var value = value(message, 1, LONG_LENGTHS);
                batch.put(MESSAGES, key(message), value);
                expected.messages.put(message, value);
            }
            ledger.commit(batch).awaitDurable();
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    @ParameterizedTest(name = "first written by a bulk commit: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an overwrite of a key a bulk commit's run holds counts as one of a value in memory, so the overwrites of a key space a bulk commit filled stop listing their values for the log as those of one filled by small commits do")
    void overwritesOfRunKeys(boolean bulk, @TempDir Path directory) throws Exception {
        var keys = 6_000;
        var value = new byte[512];
        // A budget no commit reaches, so every record listed stays queued
        try (var ledger = open(directory, false, 1L << 30)) {
            var state = ledger.state(MESSAGES);
            var size = bulk ? keys : 1_000;
            for (var first = 0; first < keys; first += size) {
                var batch = ledger.newBatch();
                for (var message = first; message < first + size; message++) {
                    batch.put(MESSAGES, key(message), value);
                }
                ledger.commit(batch).awaitDurable();
            }
            assertEquals(bulk, state.run != null);
            assertTrue(state.overwrites.spills(), "values written once may not leave the heap");
            // Each key overwritten once by a commit of its own, one in 64 of them sampled, every one over a value no
            // checkpoint took: about 94 samples against the 157 of the first writes, past the eighth that stops the
            // values leaving the heap at the judgement of the 192nd
            for (var message = 0; message < keys; message++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
            }
            assertFalse(state.overwrites.spills(), "the overwrites counted as puts of new keys");
            // An overwrite then lists no value for the log, and its commit queues no record
            var records = ledger.spills().recordRoomsForTests().length;
            for (var message = 0; message < 1_000; message++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
            }
            assertEquals(records, ledger.spills().recordRoomsForTests().length, "an overwrite queued a record of its value");
            assertEquals(bulk, state.run != null, "a checkpoint merged the run");
        }
    }

    /// Fills `keys` keys of [#MESSAGES] with `value`, by one bulk commit or by commits of 1,000 puts, checks that the
    /// key space's values may then leave the heap and returns the key space's state.
    private static KeyspaceState fill(LiveLedger ledger, boolean bulk, int keys, byte[] value)
            throws InterruptedException {
        var state = ledger.state(MESSAGES);
        var size = bulk ? keys : 1_000;
        for (var first = 0; first < keys; first += size) {
            var batch = ledger.newBatch();
            for (var message = first; message < first + size; message++) {
                batch.put(MESSAGES, key(message), value);
            }
            ledger.commit(batch).awaitDurable();
        }
        assertEquals(bulk, state.run != null);
        assertTrue(state.overwrites.spills(), "values written once may not leave the heap");
        return state;
    }

    @ParameterizedTest(name = "first written by a bulk commit: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a removal of a key a bulk commit's run holds counts as one of a value in memory, so removals of a key space a bulk commit filled stop its values leaving the heap as those of one filled by small commits do")
    void removalsOfRunKeys(boolean bulk, @TempDir Path directory) throws Exception {
        var keys = 6_000;
        var value = new byte[512];
        try (var ledger = open(directory, false, 1L << 30)) {
            var state = fill(ledger, bulk, keys, value);
            // Each key removed by a commit of its own beside a put of a new key, one in 64 of each sampled: the
            // removals over values no checkpoint took are past the eighth of the puts that stops the values leaving
            for (var message = 0; message < keys; message++) {
                ledger.commit(ledger.newBatch().delete(MESSAGES, key(message)));
                ledger.commit(ledger.newBatch().put(MESSAGES, key(keys + message), value));
            }
            assertFalse(state.overwrites.spills(), "the removals counted as removals of keys memory did not hold");
            assertEquals(bulk, state.run != null, "a checkpoint merged the run");
            assertEquals(keys, ledger.size(MESSAGES));
        }
    }

    @ParameterizedTest(name = "first written by a bulk commit: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a put of a key a range removal hid after a bulk commit's run or a chain held it counts as a put of a new key, so the values of a key space refilled after a range removal still leave the heap")
    void putsAfterRangeRemoval(boolean bulk, @TempDir Path directory) throws Exception {
        var keys = 6_000;
        var value = new byte[512];
        try (var ledger = open(directory, false, 1L << 30)) {
            var state = fill(ledger, bulk, keys, value);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.all())).awaitDurable();
            assertEquals(0, ledger.size(MESSAGES));
            for (var message = 0; message < keys; message++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
            }
            assertTrue(state.overwrites.spills(), "puts of keys a range removal hid counted as overwrites");
            assertEquals(keys, ledger.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("an overwrite of a key the run of an open's replay holds counts as one of a value in memory until the first checkpoint takes the run")
    void overwritesOfReplayRunKeys(@TempDir Path directory) throws Exception {
        var keys = 6_000;
        var value = new byte[512];
        try (var ledger = open(directory, false, 1L << 30)) {
            fill(ledger, false, keys, value);
            ledger.closeWithoutCheckpoint();
        }
        // A budget of 0 replays every put into a run
        try (var ledger = open(directory, false, 0)) {
            var state = ledger.state(MESSAGES);
            var run = state.run;
            assertNotNull(run, "the replay packed no run");
            assertFalse(run.committed());
            assertEquals(0, state.chains());
            // Puts of new keys, all sampled in a commit of 1,000 until the first judgement, let the values leave the
            // heap
            var batch = ledger.newBatch();
            for (var message = keys; message < keys + 1_000; message++) {
                batch.put(MESSAGES, key(message), value);
            }
            ledger.commit(batch).awaitDurable();
            assertTrue(state.overwrites.spills(), "values written once may not leave the heap");
            for (var message = 0; message < keys; message++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(message), value));
            }
            assertFalse(state.overwrites.spills(), "the overwrites counted as puts of new keys");
            assertSame(run, state.run, "a checkpoint merged the run");
            assertEquals(keys + 1_000, ledger.size(MESSAGES));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store that crashes after a bulk commit, its values moved or not, recovers it from the log")
    void crashAfterBulk(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        Expected afterSecond;
        try (var ledger = open(directory, encrypted, 0)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LONG_LENGTHS);
            ledger.commit(batch).awaitDurable();
            spillNow(ledger);
            afterSecond = expected.copy();
            batch = ledger.newBatch();
            for (var message = 0; message < BULK; message += 3) {
                batch.delete(MESSAGES, key(message));
                afterSecond.messages.remove(message);
            }
            ledger.commit(batch).awaitDurable();
            verify(ledger, afterSecond);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, afterSecond);
            var batch = ledger.newBatch();
            puts(batch, afterSecond, BULK, 100, 2, LENGTHS);
            ledger.commit(batch).awaitDurable();
            verify(ledger, afterSecond);
            ledger.checkpoint();
            verify(ledger, afterSecond);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a checkpoint whose cut is below a bulk commit that published while it built leaves the run out of its trees, and the next one takes it")
    void checkpointCutBelowBulk(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            var first = ledger.newBatch();
            puts(first, expected, BULK, 20, 0, LENGTHS);
            ledger.commit(first).awaitDurable();
            var ran = new AtomicBoolean();
            ledger.checkpointer().hookCutForTests(() -> {
                if (ran.compareAndSet(false, true)) {
                    var batch = ledger.newBatch();
                    puts(batch, expected, 0, BULK, 0, LENGTHS);
                    ledger.commit(batch);
                    assertNotNull(ledger.state(MESSAGES).run, "the bulk commit made no run");
                }
            });
            ledger.checkpoint();
            assertTrue(ran.get());
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "a checkpoint below the bulk commit released its run");
            assertTrue(ledger.tree().sequence < run.minStamp() >>> Reads.OP_BITS);
            verify(ledger, expected);
            ledger.checkpointer().hookCutForTests(null);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit after a replayed run was counted and released counts exactly, the replay's correction kept")
    void bulkAfterReplayedRun(@TempDir Path directory) throws Exception {
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            var batch = ledger.newBatch();
            for (var message = 0; message < 50; message++) {
                batch.put(MESSAGES, key(1_000_000 + message), new byte[]{1});
            }
            ledger.commit(batch).awaitDurable();
            ledger.closeWithoutCheckpoint();
        }
        var expected = new Expected();
        try (var ledger = open(directory, true, 0)) {
            assertNotNull(ledger.state(MESSAGES).run, "the replay built no run");
            assertEquals(50, ledger.size(MESSAGES));
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            // Emptied, then checkpointed, so the latest tree holds none of the key space
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.all())).awaitDurable();
            assertEquals(0, ledger.size(MESSAGES));
            ledger.checkpoint();
            ledger.checkpoint();
            var batch = ledger.newBatch();
            puts(batch, expected, 0, BULK, 0, LENGTHS);
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "no run over an emptied key space");
            assertTrue(run.committed());
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true, 0)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a bulk commit taken back by a decoder that throws leaves no run, no count and no entry, and the next one makes its run")
    void rollbackLeavesNoRun(@TempDir Path directory) throws Exception {
        ValueCodec<String> refusing = ValueCodec.of(value -> value.getBytes(StandardCharsets.UTF_8).length,
                (value, target) -> MemorySegment.copy(MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)), 0,
                        target, 0, target.byteSize()),
                source -> {
                    var text = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
                    if (text.equals("fail")) {
                        throw new IllegalArgumentException("refused");
                    }
                    return text;
                });
        var failing = Keyspace.resident("failing", KeyCodec.utf8(), refusing);
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.key(KEY)).keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).keyspace(failing))
                .writeThrough(WriteThrough.Mode.OFF)
                .build()) {
            var expected = new Expected();
            var batch = ledger.newBatch();
            puts(batch, new Expected(), 0, BULK, 0, LENGTHS);
            // Decoded as the batch applies, after the run took every put
            batch.putEncoded(failing, MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8)),
                    MemorySegment.ofArray("fail".getBytes(StandardCharsets.UTF_8)));
            assertThrows(IllegalArgumentException.class, () -> ledger.commit(batch));
            assertNull(ledger.state(MESSAGES).run, "a run of a commit taken back");
            assertEquals(0, ledger.size(MESSAGES));
            verify(ledger, expected);
            var next = ledger.newBatch();
            puts(next, expected, 0, BULK, 1, LENGTHS);
            ledger.commit(next).awaitDurable();
            assertNotNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "round {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("pipelined commits and checkpoints racing a bulk commit on the same keys keep the counts exact and every checkpoint's count check passing")
    void racingWriters(int round, @TempDir Path directory) throws Exception {
        // Every commit's writes, by sequence, applied in that order once all finished
        var commits = new ConcurrentHashMap<Long, List<Object[]>>();
        try (var ledger = open(directory, true, round == 2 ? 0 : Long.MAX_VALUE)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            var start = new CountDownLatch(1);
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 4; t++) {
                var seed = round * 10 + t;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    try {
                        start.await();
                        while (!stop.get()) {
                            var batch = ledger.newBatch();
                            var writes = new ArrayList<Object[]>();
                            for (var i = 0; i < 1 + random.nextInt(4); i++) {
                                var message = random.nextInt(BULK + 200);
                                if (random.nextInt(3) == 0) {
                                    batch.delete(MESSAGES, key(message));
                                    writes.add(new Object[]{message, null});
                                } else {
                                    var value = value(message, 100 + random.nextInt(50), LENGTHS);
                                    batch.put(MESSAGES, key(message), value);
                                    writes.add(new Object[]{message, value});
                                }
                            }
                            commits.put(ledger.commit(batch).sequence(), writes);
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            // Round 0 races the commits alone; the others checkpoint all along, before and after the bulk commit
            var checkpoints = Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    while (round > 0 && !stop.get()) {
                        ledger.checkpoint();
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            start.countDown();
            var batch = ledger.newBatch();
            var writes = new ArrayList<Object[]>();
            for (var message = 0; message < BULK; message++) {
                var value = value(message, 0, LENGTHS);
                batch.put(MESSAGES, key(message), value);
                writes.add(new Object[]{message, value});
            }
            commits.put(ledger.commit(batch).sequence(), writes);
            if (round == 0) {
                // No checkpoint ran: the keys the writers wrote first took chains, and the run took the others
                var run = ledger.state(MESSAGES).run;
                assertNotNull(run, "no run beside the racing writers");
                assertTrue(run.size() > 0 && run.size() <= BULK);
            }
            Thread.sleep(50);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            checkpoints.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            var expected = new Expected();
            for (var sequence : commits.keySet().stream().sorted().toList()) {
                for (var write : commits.get(sequence)) {
                    if (write[1] == null) {
                        expected.messages.remove((Integer) write[0]);
                    } else {
                        expected.messages.put((Integer) write[0], (byte[]) write[1]);
                    }
                }
            }
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    /// Checks every way a view reads `expected`, of keys of any length: point reads, encoded reads, scans both ways,
    /// ranges and counts.
    private static void verifyKeys(View view, TreeMap<byte[], byte[]> expected) {
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, entry.getKey()).orElseThrow(),
                    () -> "key of " + entry.getKey().length + " bytes");
            assertArrayEquals(entry.getValue(), view.readEncoded(MESSAGES, entry.getKey(),
                    segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        }
        assertEquals(expected.size(), view.size(MESSAGES), "keys counted");
        var keys = new ArrayList<byte[]>();
        view.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            assertArrayEquals(expected.get(key), value, "scanned value");
            keys.add(key);
            return true;
        });
        assertEquals(expected.size(), keys.size(), "keys scanned");
        var ascending = expected.keySet().iterator();
        for (var key : keys) {
            assertArrayEquals(ascending.next(), key, "keys scanned out of order");
        }
        var descending = new ArrayList<byte[]>();
        view.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (key, _) -> {
            descending.add(key);
            return true;
        });
        assertEquals(expected.size(), descending.size(), "keys scanned descending");
        var reversed = expected.descendingKeySet().iterator();
        for (var key : descending) {
            assertArrayEquals(reversed.next(), key, "keys scanned descending out of order");
        }
        var sorted = new ArrayList<>(expected.keySet());
        for (var from = 0; from + 1 < sorted.size(); from += 397) {
            var to = Math.min(from + 150, sorted.size() - 1);
            var wanted = expected.subMap(sorted.get(from), sorted.get(to));
            var range = view.range(MESSAGES, KeyRange.between(sorted.get(from), sorted.get(to)), Integer.MAX_VALUE);
            assertEquals(wanted.size(), range.size(), "keys from " + from);
            var index = 0;
            for (var entry : wanted.entrySet()) {
                assertArrayEquals(entry.getKey(), range.get(index).key());
                assertArrayEquals(entry.getValue(), range.get(index++).value());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a bulk commit of keys of 4 to 1,024 bytes keeps them in whole chunks of 256 KiB but the last, finds every one, and reads them through a spill under a snapshot, removals, a crash, a checkpoint and a reopen")
    void keysSpanChunks(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(42);
        var expected = new TreeMap<byte[], byte[]>(Arrays::compareUnsigned);
        // Budget no commit reaches, so the values move only when the test drains the queue
        try (var ledger = open(directory, encrypted, 1L << 30)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            var batch = ledger.newBatch();
            for (var i = 0; i < 3_000; i++) {
                // A run of keys of 1,024 bytes first, so some key does not fit the rest of its chunk and starts the next
                var key = new byte[i < 300 ? KeyCodec.MAX_LENGTH : 4 + random.nextInt(KeyCodec.MAX_LENGTH - 3)];
                random.nextBytes(key);
                ByteBuffer.wrap(key).putInt(i * 7_919);
                var value = new byte[LONG_LENGTHS[random.nextInt(LONG_LENGTHS.length)]];
                random.nextBytes(value);
                batch.put(MESSAGES, key, value);
                expected.put(key, value);
            }
            ledger.commit(batch).awaitDurable();
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "the bulk commit made no run");
            assertTrue(run.committed());
            assertEquals(3_000, run.size());
            var chunks = run.keyChunkLengthsForTests();
            assertTrue(chunks.length > 4, () -> "keys in " + Arrays.toString(chunks));
            for (var i = 0; i < chunks.length - 1; i++) {
                assertEquals(PackedRun.KEY_CHUNK, chunks[i], "chunk " + i + " of " + Arrays.toString(chunks));
            }
            assertTrue(chunks[chunks.length - 1] <= PackedRun.KEY_CHUNK);
            for (var key : expected.keySet()) {
                assertTrue(run.find(MemorySegment.ofArray(key)) >= 0, "the run lacks a key of " + key.length + " bytes");
            }
            verifyKeys(ledger, expected);
            try (var before = ledger.snapshot()) {
                ledger.spills().drainAllForTests(ledger.log()::durableSequence);
                ledger.spills().awaitIdleForTests();
                assertEquals(0, run.homesForTests(), "the run kept a home every value left");
                verifyKeys(before, expected);
            }
            verifyKeys(ledger, expected);
            batch = ledger.newBatch();
            var index = 0;
            for (var key : new ArrayList<>(expected.keySet())) {
                if (index++ % 4 == 0) {
                    batch.delete(MESSAGES, key);
                    expected.remove(key);
                }
            }
            ledger.commit(batch).awaitDurable();
            verifyKeys(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verifyKeys(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verifyKeys(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verifyKeys(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    private static void spillNow(LiveLedger ledger) throws InterruptedException {
        var spills = ledger.spills();
        spills.drain(ledger.log()::durableSequence);
        spills.awaitIdleForTests();
    }
}
