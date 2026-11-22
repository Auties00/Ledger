package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The runs an open replays the paged writes before the log's last budget into ([PackedRun]): most stores run with a
/// budget of 0, so every replayed paged write of a key memory holds no chain of goes to a run, and every read, scan,
/// count, later write, range removal, checkpoint, snapshot and further crash sees the state the log holds, as the
/// chains it replaced showed it.
class PackedRunTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // Empty, short, on both sides of the threshold for the log, on both sides of the verdict's paying length, and above
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN - 1, SpillQueue.LOG_VALUE_MIN, 200,
            SpillQueue.PAYING_VALUE_MIN, 700, 1_500};
    // The least value of MESSAGES that goes to the file of large values: longer than every value of LENGTHS
    private static final int LARGE_VALUES = 4 << 10;

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO)
                .largeValues(MESSAGES, LARGE_VALUES))
                .segmentSize(64 << 10)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    /// {@return the key of message `id` of chat `chat`}
    private static byte[] key(int chat, int id) {
        return ByteBuffer.allocate(5).put((byte) chat).putInt(id).array();
    }

    private static byte[] value(int seed, int generation) {
        var length = LENGTHS[Math.floorMod(seed * 7 + generation, LENGTHS.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (seed * 31 + generation * 17 + i);
        }
        return value;
    }

    /// {@return a text key longer than a chain holds in its words}
    private static String textKey(int id) {
        return "text-key-longer-than-thirty-two-bytes-" + id;
    }

    private static String text(int id, int generation) {
        return "t" + id + "/" + generation + "x".repeat(LENGTHS[Math.floorMod(id + generation, LENGTHS.length)]);
    }

    /// What a store should hold.
    private static final class Expected {
        final TreeMap<ByteBuffer, byte[]> messages = new TreeMap<>(Expected::compare);
        final TreeMap<String, String> texts = new TreeMap<>();

        private static int compare(ByteBuffer left, ByteBuffer right) {
            return Arrays.compareUnsigned(left.array(), right.array());
        }

        Expected copy() {
            var copy = new Expected();
            copy.messages.putAll(messages);
            copy.texts.putAll(texts);
            return copy;
        }

        void put(byte[] key, byte[] value) {
            messages.put(ByteBuffer.wrap(key), value);
        }

        void delete(byte[] key) {
            messages.remove(ByteBuffer.wrap(key));
        }

        void deleteChat(int chat) {
            messages.keySet().removeIf(key -> key.array()[0] == (byte) chat);
        }
    }

    /// Checks every way a view reads `expected`: point reads, encoded reads, scans both ways, ranges of one chat and
    /// counts.
    private static void verify(View view, Expected expected) {
        for (var entry : expected.messages.entrySet()) {
            assertArrayEquals(entry.getValue(), view.get(MESSAGES, entry.getKey().array()).orElseThrow(),
                    () -> "message " + Arrays.toString(entry.getKey().array()));
            assertArrayEquals(entry.getValue(), view.readEncoded(MESSAGES, entry.getKey().array(),
                    segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        }
        for (var entry : expected.texts.entrySet()) {
            assertEquals(entry.getValue(), view.get(TEXTS, entry.getKey()).orElseThrow(), "text " + entry.getKey());
        }
        assertEquals(expected.messages.size(), view.size(MESSAGES), "messages counted");
        assertEquals(expected.texts.size(), view.size(TEXTS), "texts counted");
        assertEquals(new ArrayList<>(expected.messages.keySet()), scan(view, KeyRange.all()), "messages scanned");
        assertEquals(new ArrayList<>(expected.messages.descendingKeySet()), scan(view, KeyRange.<byte[]>all().reversed()),
                "messages scanned descending");
        for (var chat = 0; chat < 4; chat++) {
            var from = new byte[]{(byte) chat};
            var to = new byte[]{(byte) (chat + 1)};
            var range = new ArrayList<>(expected.messages.subMap(ByteBuffer.wrap(from), true, ByteBuffer.wrap(to), false).descendingKeySet());
            assertEquals(range, scan(view, KeyRange.between(from, to).reversed()), "chat " + chat + " descending");
            var page = view.range(MESSAGES, KeyRange.between(from, to).reversed(), 7);
            assertEquals(Math.min(7, range.size()), page.size());
            for (var i = 0; i < page.size(); i++) {
                assertArrayEquals(range.get(i).array(), page.get(i).key());
                assertArrayEquals(expected.messages.get(range.get(i)), page.get(i).value());
            }
        }
        var texts = new TreeMap<String, String>();
        view.forEach(TEXTS, KeyRange.<String>all().reversed(), (key, value) -> {
            texts.put(key, value);
            return true;
        });
        assertEquals(expected.texts, texts, "texts scanned");
    }

    private static List<ByteBuffer> scan(View view, KeyRange<byte[]> range) {
        var keys = new ArrayList<ByteBuffer>();
        view.forEach(MESSAGES, range, (key, _) -> {
            keys.add(ByteBuffer.wrap(key));
            return true;
        });
        return keys;
    }

    /// Writes `count` messages of `chat` from `first`, with a text each, in one batch.
    private static void write(Ledger ledger, Expected expected, int chat, int first, int count, int generation)
            throws InterruptedException {
        var batch = ledger.newBatch();
        for (var id = first; id < first + count; id++) {
            var value = value(chat * 1_000 + id, generation);
            batch.put(MESSAGES, key(chat, id), value);
            expected.put(key(chat, id), value);
            var text = text(id, generation);
            batch.put(TEXTS, textKey(id), text);
            expected.texts.put(textKey(id), text);
        }
        batch.put(CHATS, "c" + chat, "chat " + generation);
        ledger.commit(batch).awaitDurable();
    }

    /// Writes a checkpointed history and a crashed tail over it: overwrites and removals of checkpointed keys, removals
    /// of keys never written, new keys, and keys written twice in the tail.
    private static Expected crashedStore(Path directory, boolean encrypted, boolean checkpointed) throws InterruptedException {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            if (checkpointed) {
                for (var chat = 0; chat < 4; chat++) {
                    write(ledger, expected, chat, 0, 40, 0);
                }
                ledger.checkpoint();
            }
            for (var chat = 0; chat < 4; chat++) {
                write(ledger, expected, chat, 30, 30, 1);
            }
            var batch = ledger.newBatch();
            for (var id = 0; id < 80; id += 3) {
                batch.delete(MESSAGES, key(1, id));
                expected.delete(key(1, id));
                batch.delete(TEXTS, textKey(id));
                expected.texts.remove(textKey(id));
            }
            ledger.commit(batch).awaitDurable();
            write(ledger, expected, 2, 50, 20, 2);
            ledger.closeWithoutCheckpoint();
        }
        return expected;
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("the writes a recovery replays go to runs, read back as the log holds them, merge with later writes and the tree, and leave memory with the first checkpoint")
    void replayedWritesFormRuns(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, encrypted, true);
        try (var ledger = open(directory, encrypted, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "the replay built no run");
            assertNotNull(ledger.state(TEXTS).run, "the replay built no run of the longer keys");
            assertEquals(0, ledger.state(MESSAGES).chains(), "a replayed write took a chain");
            verify(ledger, expected);
            ledger.update(transaction -> {
                assertEquals(expected.messages.size(), transaction.size(MESSAGES));
                verify(transaction, expected);
            });
            // Later writes over the run's keys, the tree's and new ones, read with the run under them
            var batch = ledger.newBatch();
            for (var id = 0; id < 80; id += 4) {
                var value = value(id, 9);
                batch.put(MESSAGES, key(2, id), value);
                expected.put(key(2, id), value);
                batch.delete(MESSAGES, key(3, id + 1));
                expected.delete(key(3, id + 1));
            }
            ledger.commit(batch).awaitDurable();
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the run outlived the checkpoint that took it");
            assertNull(ledger.state(TEXTS).run);
            verify(ledger, expected);
            write(ledger, expected, 0, 100, 5, 3);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            assertNull(ledger.state(MESSAGES).run, "a clean open replayed writes");
            verify(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store that crashed before its first checkpoint reads its runs alone, without a tree, through every read")
    void runsWithoutTrees(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, encrypted, false);
        try (var ledger = open(directory, encrypted, 0)) {
            assertNotNull(ledger.state(MESSAGES).run);
            assertNull(ledger.tree(), "a tree before the first checkpoint");
            verify(ledger, expected);
            ledger.update(transaction -> {
                transaction.delete(MESSAGES, key(0, 31));
                transaction.put(MESSAGES, key(0, 1_000), new byte[]{1});
                assertEquals(expected.messages.size(), transaction.size(MESSAGES));
            });
            expected.delete(key(0, 31));
            expected.put(key(0, 1_000), new byte[]{1});
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a range removal in the replayed log publishes its key space's run before it counts, and the writes after it replay to chains")
    void replayedRangeRemoval(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            for (var chat = 0; chat < 4; chat++) {
                write(ledger, expected, chat, 0, 20, 0);
            }
            ledger.checkpoint();
            write(ledger, expected, 1, 10, 30, 1);
            write(ledger, expected, 2, 10, 30, 1);
            // A clear of chat 1 in the middle of the log, then writes to it and over the run's keys of chat 2
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(new byte[]{1}, new byte[]{2})))
                    .awaitDurable();
            expected.deleteChat(1);
            write(ledger, expected, 1, 15, 5, 2);
            write(ledger, expected, 2, 25, 10, 2);
            write(ledger, expected, 3, 40, 10, 2);
            ledger.closeWithoutCheckpoint();
        }
        for (var round = 0; round < 2; round++) {
            try (var ledger = open(directory, encrypted, 0)) {
                if (round == 0) {
                    assertNotNull(ledger.state(MESSAGES).run, "no run of the writes before the removal");
                    assertTrue(ledger.state(MESSAGES).chains() > 0, "the writes after the removal took no chain");
                }
                verify(ledger, expected);
                if (round == 0) {
                    // A crash again before any checkpoint replays the same log
                    ledger.closeWithoutCheckpoint();
                }
            }
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a range removal as the first write of the replayed log leaves its key space no run")
    void replayedRangeRemovalFirst(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            write(ledger, expected, 0, 0, 20, 0);
            ledger.checkpoint();
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(new byte[]{0}, new byte[]{0, 0, 0, 0, 10})))
                    .awaitDurable();
            for (var id = 0; id < 10; id++) {
                expected.delete(key(0, id));
            }
            write(ledger, expected, 0, 5, 10, 1);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            assertNull(ledger.state(MESSAGES).run, "a run after the key space's first range removal");
            assertNotNull(ledger.state(TEXTS).run);
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("range removals after the open, of batches counted later and of transactions counted at once, count the run's entries they remove")
    void liveRangeRemovals(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, encrypted, true);
        try (var ledger = open(directory, encrypted, 0)) {
            assertNotNull(ledger.state(MESSAGES).run);
            // Counted later, on a thread of the store's own, against the trees and the run
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(new byte[]{1}, new byte[]{2})))
                    .awaitDurable();
            expected.deleteChat(1);
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            verify(ledger, expected);
            ledger.update(transaction -> {
                transaction.deleteRange(MESSAGES, KeyRange.between(key(2, 35), key(2, 55)));
                assertEquals(expected.messages.size() - expected.messages.subMap(ByteBuffer.wrap(key(2, 35)),
                        ByteBuffer.wrap(key(2, 55))).size(), transaction.size(MESSAGES));
            });
            expected.messages.subMap(ByteBuffer.wrap(key(2, 35)), ByteBuffer.wrap(key(2, 55))).clear();
            // A write after the removal of a key the run holds
            ledger.commit(ledger.newBatch().put(MESSAGES, key(1, 40), new byte[]{4, 2})).awaitDurable();
            expected.put(key(1, 40), new byte[]{4, 2});
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("writes of the run's keys, in batches, transactions and batches that run alone, count exactly whether the run puts or removes the key")
    void writesOfRunKeys(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, encrypted, true);
        try (var ledger = open(directory, encrypted, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            // Replayed puts of new and of checkpointed keys, and replayed removals of checkpointed and unknown keys
            var put = key(1, 31);
            var removed = key(1, 30);
            var overwritten = key(0, 35);
            assertTrue(run.find(MemorySegment.ofArray(put)) >= 0 && run.find(MemorySegment.ofArray(removed)) >= 0);
            ledger.update(transaction -> {
                transaction.put(MESSAGES, put, new byte[]{1});
                transaction.delete(MESSAGES, removed);
                transaction.put(MESSAGES, overwritten, new byte[]{2});
                transaction.delete(MESSAGES, key(1, 33));
            });
            expected.put(put, new byte[]{1});
            expected.put(overwritten, new byte[]{2});
            expected.delete(key(1, 33));
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            verify(ledger, expected);
            // Pipelined commits write blind but for the keys the run holds
            ledger.commit(ledger.newBatch().put(MESSAGES, removed, new byte[]{3}).delete(MESSAGES, key(1, 36))
                    .put(MESSAGES, key(1, 39), new byte[]{4})).awaitDurable();
            expected.put(removed, new byte[]{3});
            expected.delete(key(1, 36));
            expected.put(key(1, 39), new byte[]{4});
            assertEquals(expected.messages.size(), ledger.size(MESSAGES));
            // A batch with a range removal runs alone and looks its other keys up before its turn
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(3, 0), key(3, 5)))
                    .put(MESSAGES, key(3, 31), new byte[]{5}).delete(MESSAGES, key(3, 32)).put(MESSAGES, key(1, 42), new byte[]{6}))
                    .awaitDurable();
            expected.messages.subMap(ByteBuffer.wrap(key(3, 0)), ByteBuffer.wrap(key(3, 5))).clear();
            expected.put(key(3, 31), new byte[]{5});
            expected.delete(key(3, 32));
            expected.put(key(1, 42), new byte[]{6});
            verify(ledger, expected);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    /// The tree a store holds of MESSAGES before the log the runs are replayed from.
    enum Base {
        /// A checkpointed tree of the key space.
        CHECKPOINTED,
        /// A checkpoint taken while the key space was empty, which leaves it no tree.
        EMPTY,
        /// No checkpoint at all.
        NONE
    }

    static List<Arguments> lastBudgetCases() {
        var cases = new ArrayList<Arguments>();
        for (var base : Base.values()) {
            cases.add(Arguments.of(base, true));
            cases.add(Arguments.of(base, false));
        }
        return cases;
    }

    @ParameterizedTest(name = "{0}, encrypted: {1}")
    @MethodSource("lastBudgetCases")
    @DisplayName("writes the replay sends to chains over keys of the run, of the last budget and of the file of large values, take the run's entry as the key's state, with or without a tree under it")
    void lastBudgetOverRun(Base base, boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            switch (base) {
                case CHECKPOINTED -> {
                    write(ledger, expected, 0, 0, 50, 0);
                    ledger.checkpoint();
                }
                case EMPTY -> {
                    ledger.commit(ledger.newBatch().put(CHATS, "c", "x")).awaitDurable();
                    ledger.checkpoint();
                }
                case NONE -> {
                }
            }
            for (var round = 0; round < 40; round++) {
                write(ledger, expected, 1 + round % 3, round * 10, 10, 1);
                if (round == 20) {
                    // Before the last budget: values of the file of large values, two over keys the run holds, one
                    // removed after; and a removal the run takes of a key it holds
                    var batch = ledger.newBatch();
                    for (var id = 1; id < 30; id += 4) {
                        var large = new byte[LARGE_VALUES + id];
                        large[id] = (byte) id;
                        batch.put(MESSAGES, key(2, id), large);
                        expected.put(key(2, id), large);
                    }
                    ledger.commit(batch).awaitDurable();
                    ledger.commit(ledger.newBatch().delete(MESSAGES, key(2, 5)).delete(MESSAGES, key(3, 22))).awaitDurable();
                    expected.delete(key(2, 5));
                    expected.delete(key(3, 22));
                }
            }
            // The last commits, within the last budget, write keys the run holds, a checkpointed one and new ones
            var batch = ledger.newBatch();
            for (var id = 0; id < 40; id += 2) {
                batch.delete(MESSAGES, key(1, id));
                expected.delete(key(1, id));
                var value = value(id, 7);
                batch.put(MESSAGES, key(2, id + 1), value);
                expected.put(key(2, id + 1), value);
            }
            batch.delete(MESSAGES, key(0, 3));
            expected.delete(key(0, 3));
            batch.put(MESSAGES, key(3, 5_000), new byte[]{9});
            expected.put(key(3, 5_000), new byte[]{9});
            // A large value over a key of the run, and removals of keys the run removed and of a large value
            var large = new byte[LARGE_VALUES];
            large[0] = 1;
            batch.put(MESSAGES, key(3, 25), large);
            expected.put(key(3, 25), large);
            batch.delete(MESSAGES, key(3, 22));
            batch.delete(MESSAGES, key(2, 9));
            expected.delete(key(2, 9));
            ledger.commit(batch).awaitDurable();
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 16 << 10)) {
            assertNotNull(ledger.state(MESSAGES).run, "no run before the last budget");
            assertTrue(ledger.state(MESSAGES).chains() > 0, "the last budget took no chain");
            verify(ledger, expected);
            for (var id = 0; id < 40; id += 2) {
                assertFalse(ledger.get(MESSAGES, key(1, id)).isPresent(), "a removal of the last budget was lost");
            }
            ledger.checkpoint();
            verify(ledger, expected);
            // Every later checkpoint finds the count it expects
            write(ledger, expected, 1, 0, 5, 9);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 16 << 10)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a checkpoint that fails leaves the run to the next one, which merges it again")
    void failedCheckpointKeepsTheRun() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var expected = crashedStore(directory, true, true);
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                    && path.getFileName().toString().equals(PageFile.FILE)) {
                throw new java.io.IOException("No space left on device");
            }
        });
        try (var ledger = open(directory, true, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            write(ledger, expected, 2, 31, 4, 5);
            failing.set(true);
            assertThrows(LedgerIoException.class, ledger::checkpoint);
            assertTrue(ledger.state(MESSAGES).run == run, "a failed checkpoint dropped the run");
            verify(ledger, expected);
            write(ledger, expected, 1, 31, 4, 6);
            failing.set(false);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, true, 0)) {
            verify(ledger, expected);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a snapshot of the tree the store opened from keeps reading the run, and its values in the log, after the checkpoint that takes it, which lets go of the run once the snapshot closes")
    void snapshotAcrossCheckpoint(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, encrypted, true);
        try (var ledger = open(directory, encrypted, 0)) {
            var before = expected.copy();
            try (var snapshot = ledger.snapshot()) {
                var batch = ledger.newBatch();
                for (var id = 30; id < 60; id += 2) {
                    batch.delete(MESSAGES, key(0, id));
                    expected.delete(key(0, id));
                }
                ledger.commit(batch).awaitDurable();
                ledger.checkpoint();
                assertNotNull(ledger.state(MESSAGES).run, "the run left while a snapshot of the tree before reads it");
                verify(snapshot, before);
                verify(ledger, expected);
                write(ledger, expected, 3, 200, 5, 4);
                ledger.checkpoint();
                verify(snapshot, before);
            }
            // Released by the next maintenance, or the next checkpoint's prune
            write(ledger, expected, 3, 300, 5, 5);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
            // The segments the run's values were in retire, and the tree holds the values
            write(ledger, expected, 3, 400, 5, 6);
            ledger.checkpoint();
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
    }

    @Test
    @DisplayName("a run keeps the newest write of each key, its values in chunks and in the log, and finds every key by hash and in order")
    void runLayout(@TempDir Path directory) throws Exception {
        var expected = crashedStore(directory, true, true);
        try (var ledger = open(directory, true, 0)) {
            var run = ledger.state(MESSAGES).run;
            var keys = new ArrayList<ByteBuffer>();
            for (var position = 0; position < run.size(); position++) {
                var entry = run.entryAt(position);
                keys.add(ByteBuffer.wrap(run.key(entry).toArray(ValueLayout.JAVA_BYTE)));
                assertEquals(entry, run.find(run.key(entry)));
                if (position > 0) {
                    assertTrue(Arrays.compareUnsigned(keys.get(position - 1).array(), keys.get(position).array()) < 0,
                            "the run is out of order at " + position);
                }
            }
            for (var entry : expected.messages.entrySet()) {
                var found = run.find(MemorySegment.ofArray(entry.getKey().array()));
                if (found < 0) {
                    continue;
                }
                assertFalse(run.removes(found));
                var source = run.sourceForTests(found);
                var length = entry.getValue().length;
                if (length >= SpillQueue.LOG_VALUE_MIN) {
                    assertTrue(source instanceof LogValue, "a value of " + length + " bytes is not in the log");
                } else if (length > 0) {
                    assertTrue(source instanceof MemorySegment chunk && chunk.byteSize() == ReplayValues.CHUNK_BYTES,
                            "a value of " + length + " bytes is not in a chunk");
                }
                assertArrayEquals(entry.getValue(), run.valueBytes(found));
            }
            // Removals of checkpointed keys and of keys never written
            assertTrue(run.removes(run.find(MemorySegment.ofArray(key(1, 0)))));
            assertTrue(run.removes(run.find(MemorySegment.ofArray(key(1, 78)))));
            assertEquals(-1, run.find(MemorySegment.ofArray(key(9, 9))));
            assertEquals(-1, run.find(key(9, 9)));
        }
    }

    /// {@return a key of chat `chat` of `length` bytes, from 5 to [KeyCodec#MAX_LENGTH], unique by its first five}
    private static byte[] longKey(int chat, int id, int length) {
        var key = new byte[length];
        ByteBuffer.wrap(key).put((byte) chat).putInt(id);
        for (var i = 5; i < length; i++) {
            key[i] = (byte) (id * 13 + i);
        }
        return key;
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a replay of more than a megabyte of long keys into one key space keeps them in several chunks of the run, read, scanned and counted as the log holds them, before and after the checkpoint that takes them")
    void replayedKeysSpanChunks(boolean encrypted, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var random = new Random(11);
        var lengths = new int[2_400];
        for (var id = 0; id < lengths.length; id++) {
            lengths[id] = 5 + random.nextInt(KeyCodec.MAX_LENGTH - 4);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            for (var first = 0; first < lengths.length; first += 200) {
                var batch = ledger.newBatch();
                for (var id = first; id < first + 200; id++) {
                    var key = longKey(id % 4, id, lengths[id]);
                    var value = value(id, 0);
                    batch.put(MESSAGES, key, value);
                    expected.put(key, value);
                }
                ledger.commit(batch).awaitDurable();
            }
            // Overwrites and removals of keys the run takes, and removals of keys never written
            var batch = ledger.newBatch();
            for (var id = 0; id < lengths.length; id += 7) {
                var key = longKey(id % 4, id, lengths[id]);
                if (id % 2 == 0) {
                    var value = value(id, 5);
                    batch.put(MESSAGES, key, value);
                    expected.put(key, value);
                } else {
                    batch.delete(MESSAGES, key);
                    expected.delete(key);
                }
                batch.delete(MESSAGES, longKey(id % 4, -id - 1, KeyCodec.MAX_LENGTH));
            }
            ledger.commit(batch).awaitDurable();
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "the replay built no run");
            assertTrue(run.keyChunkLengthsForTests().length >= 5, "the run's keys lie in fewer than five chunks");
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        try (var ledger = open(directory, encrypted, 0)) {
            verify(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    static Stream<Arguments> keyLayouts() {
        return Stream.of(Arguments.of(1, false), Arguments.of(2, true), Arguments.of(3, false), Arguments.of(4, true));
    }

    @ParameterizedTest(name = "seed {0}, last chunk full: {1}")
    @MethodSource("keyLayouts")
    @DisplayName("a run of keys of 1 to 1,024 bytes over many chunks places each key whole in one chunk, trims only the last, and finds, compares, orders and scans every key as a sorted map of them does")
    void keysAcrossChunks(int seed, boolean fillLast) {
        var random = new Random(seed);
        var valueBytes = new byte[64 << 10];
        random.nextBytes(valueBytes);
        var valueChunk = MemorySegment.ofArray(valueBytes).asReadOnly();
        // The newest write of each key: its value, or null for a removal
        var model = new TreeMap<ByteBuffer, byte[]>(Expected::compare);
        var builder = new PackedRun.Builder();
        var stamp = new long[1];
        // Where the keys end and how many started a chunk early, as the builder places them
        var placed = new int[2];
        // Each key reaches the builder at an offset of a larger array, as a replayed key lies in its frame body
        var offsets = new Random(seed ^ 0x0FF5E7L);
        Consumer<byte[]> write = key -> {
            var wrapped = ByteBuffer.wrap(key);
            if (!model.containsKey(wrapped)) {
                var start = placed[0];
                if (start % PackedRun.KEY_CHUNK + key.length > PackedRun.KEY_CHUNK) {
                    start = (start / PackedRun.KEY_CHUNK + 1) * PackedRun.KEY_CHUNK;
                    placed[1]++;
                }
                placed[0] = start + key.length;
            }
            assertTrue(builder.takes(MemorySegment.ofArray(key), null, 100));
            var from = offsets.nextInt(16);
            var body = new byte[from + key.length + offsets.nextInt(16)];
            offsets.nextBytes(body);
            System.arraycopy(key, 0, body, from, key.length);
            var hash = Segments.hash(body, from, key.length);
            var kind = random.nextInt(8);
            if (kind == 0) {
                builder.delete(body, from, key.length, hash, stamp[0]++);
                model.put(wrapped, null);
            } else if (kind == 1) {
                builder.put(body, from, key.length, hash, stamp[0]++, null, 0, 0);
                model.put(wrapped, new byte[0]);
            } else {
                var length = 1 + random.nextInt(100);
                var offset = random.nextInt(valueBytes.length - length);
                builder.put(body, from, key.length, hash, stamp[0]++, valueChunk, offset, length);
                model.put(wrapped, Arrays.copyOfRange(valueBytes, offset, offset + length));
            }
        };
        // A whole first chunk of keys of the longest length, the last of which ends at the chunk's end
        for (var id = 0; id < PackedRun.KEY_CHUNK / KeyCodec.MAX_LENGTH; id++) {
            write.accept(longKey(0, id, KeyCodec.MAX_LENGTH));
        }
        assertEquals(PackedRun.KEY_CHUNK, placed[0]);
        checkBuilder(builder, model, random);
        // Keys of every length, new ones and writes of earlier ones
        var keys = new ArrayList<>(model.keySet());
        while (placed[0] < 6 * PackedRun.KEY_CHUNK) {
            if (random.nextInt(5) == 0) {
                write.accept(keys.get(random.nextInt(keys.size())).array());
                continue;
            }
            var length = random.nextInt(4) == 0 ? KeyCodec.MAX_LENGTH - random.nextInt(8) : 1 + random.nextInt(KeyCodec.MAX_LENGTH);
            var key = new byte[length];
            random.nextBytes(key);
            write.accept(key);
            keys.add(ByteBuffer.wrap(key));
        }
        if (fillLast) {
            // New keys that end exactly at the end of the last chunk
            var counter = 0;
            while (placed[0] % PackedRun.KEY_CHUNK != 0) {
                var rest = PackedRun.KEY_CHUNK - placed[0] % PackedRun.KEY_CHUNK;
                var length = Math.min(rest, KeyCodec.MAX_LENGTH);
                byte[] key;
                if (length >= 5) {
                    key = longKey(9, counter++, length);
                } else {
                    key = new byte[length];
                    do {
                        random.nextBytes(key);
                    } while (model.containsKey(ByteBuffer.wrap(key)));
                }
                write.accept(key);
            }
        }
        assertTrue(placed[1] > 0, "no key started a chunk early");
        checkBuilder(builder, model, random);
        var run = builder.build(null, null);

        var chunks = run.keyChunkLengthsForTests();
        var last = chunks.length - 1;
        assertEquals((placed[0] - 1) / PackedRun.KEY_CHUNK, last, "chunks of the keys");
        for (var i = 0; i < last; i++) {
            assertEquals(PackedRun.KEY_CHUNK, chunks[i], "chunk " + i + " is not whole");
        }
        assertEquals(placed[0] - last * PackedRun.KEY_CHUNK, chunks[last], "the last chunk is not trimmed to its keys");
        assertEquals(0, run.keyLengthForTests(), "a run of keys of many lengths did not make their ends");
        checkRun(run, model, random);
    }

    /// Checks that `run`, built over no tree, finds, compares, orders and scans every key of `model`, as a removal if
    /// its value is `null`, and no other key.
    private static void checkRun(PackedRun run, TreeMap<ByteBuffer, byte[]> model, Random random) {
        var sorted = new ArrayList<>(model.keySet());
        assertEquals(sorted.size(), run.size());
        for (var position = 0; position < run.size(); position++) {
            var entry = run.entryAt(position);
            var key = sorted.get(position).array();
            assertArrayEquals(key, run.key(entry).toArray(ValueLayout.JAVA_BYTE), "key at " + position);
            assertEquals(entry, run.find(MemorySegment.ofArray(key)));
            assertEquals(entry, run.find(key));
            var value = model.get(sorted.get(position));
            assertEquals(value == null, run.removes(entry));
            if (value != null) {
                assertArrayEquals(value, run.valueBytes(entry));
                assertArrayEquals(value, run.value(entry).toArray(ValueLayout.JAVA_BYTE));
            }
        }
        assertEquals(model.values().stream().filter(Objects::nonNull).count(), run.correction(),
                "every put of a run over no tree adds a key");
        // Probes: keys of the run, keys next to them, and keys of no write
        var probes = new ArrayList<byte[]>();
        for (var i = 0; i < 400; i++) {
            var key = sorted.get(random.nextInt(sorted.size())).array();
            probes.add(key);
            var near = Arrays.copyOf(key, Math.max(1, Math.min(KeyCodec.MAX_LENGTH, key.length + random.nextInt(3) - 1)));
            near[near.length - 1] += (byte) (random.nextInt(3) - 1);
            probes.add(near);
            var absent = new byte[1 + random.nextInt(KeyCodec.MAX_LENGTH)];
            random.nextBytes(absent);
            probes.add(absent);
        }
        for (var probe : probes) {
            var wrapped = ByteBuffer.wrap(probe);
            var below = model.headMap(wrapped).size();
            var expectedEntry = model.containsKey(wrapped) ? run.entryAt(below) : -1;
            assertEquals(expectedEntry, run.find(MemorySegment.ofArray(probe)));
            assertEquals(expectedEntry, run.find(probe));
            assertEquals(below, run.lowerPosition(MemorySegment.ofArray(probe)));
            var entry = run.entryAt(random.nextInt(run.size()));
            var key = run.key(entry).toArray(ValueLayout.JAVA_BYTE);
            assertEquals(Integer.signum(Arrays.compareUnsigned(key, probe)),
                    Integer.signum(run.compareKey(entry, MemorySegment.ofArray(probe))));
        }
        // Scans over no tree, whole and between two probes, both ways
        assertEquals(live(model), scanRun(run, null, null, false));
        assertEquals(live(model).reversed(), scanRun(run, null, null, true));
        for (var i = 0; i < 20; i++) {
            var from = probes.get(random.nextInt(probes.size()));
            var to = probes.get(random.nextInt(probes.size()));
            if (Arrays.compareUnsigned(from, to) > 0) {
                var swap = from;
                from = to;
                to = swap;
            }
            var range = live(model.subMap(ByteBuffer.wrap(from), true, ByteBuffer.wrap(to), false));
            assertEquals(range, scanRun(run, from, to, false));
            assertEquals(range.reversed(), scanRun(run, from, to, true));
        }
    }

    @Test
    @DisplayName("the layout of keys of one length divides every entry index below 2^24 by the keys a chunk holds exactly, at and around each multiple of it and at random, for every key length")
    void keyLayoutDivides() {
        var random = new Random(7);
        var bound = 1 << 24;
        var checked = new BitSet();
        for (var length = 1; length <= PackedRun.KEY_CHUNK; length = length < KeyCodec.MAX_LENGTH ? length + 1 : 2 * length) {
            var layout = PackedRun.KeyLayout.of(length);
            var perChunk = PackedRun.KEY_CHUNK / length;
            assertEquals(perChunk, layout.perChunk());
            if (!checked.get(perChunk)) {
                checked.set(perChunk);
                for (var multiple = (long) perChunk; multiple - 1 < bound; multiple += perChunk) {
                    for (var entry = (int) multiple - 1; entry <= multiple + 1 && entry < bound; entry++) {
                        if (layout.chunk(entry) != entry / perChunk) {
                            fail("keys of " + length + " bytes: entry " + entry + " in chunk " + layout.chunk(entry));
                        }
                    }
                }
            }
            for (var i = 0; i < 2_000; i++) {
                var entry = random.nextInt(bound);
                assertEquals(entry / perChunk, layout.chunk(entry), "keys of " + length + " bytes: entry " + entry);
                assertEquals(entry / perChunk * PackedRun.KEY_CHUNK + entry % perChunk * length, layout.start(entry));
            }
        }
    }

    static Stream<Arguments> oneLengthConversions() {
        return Stream.of(Arguments.of(24, 0), Arguments.of(24, 1), Arguments.of(24, 10_922), Arguments.of(24, 10_923),
                Arguments.of(24, 25_000), Arguments.of(32, 8_192), Arguments.of(32, 16_385), Arguments.of(1_024, 256),
                Arguments.of(1_024, 300));
    }

    @ParameterizedTest(name = "{1} keys of {0} bytes")
    @MethodSource("oneLengthConversions")
    @DisplayName("a builder of keys of one length keeps no ends of them and places as many in each chunk as fit; at the first key of another length it makes the ends of every key where they lie, and either run finds, compares, orders and scans every key as a sorted map of them does")
    void keysOfOneLength(int length, int before) {
        var random = new Random(length * 31L + before);
        var valueBytes = new byte[64 << 10];
        random.nextBytes(valueBytes);
        var valueChunk = MemorySegment.ofArray(valueBytes).asReadOnly();
        var model = new TreeMap<ByteBuffer, byte[]>(Expected::compare);
        // Two builders given the same writes: the first sees keys of one length alone, the second keys of others after
        var oneLength = new PackedRun.Builder();
        var mixed = new PackedRun.Builder();
        var stamp = new long[1];
        var keys = new ArrayList<byte[]>();
        var built = new AtomicBoolean();
        Consumer<byte[]> write = key -> {
            var hash = Segments.hash(key, 0, key.length);
            var removes = random.nextInt(8) == 0;
            var size = random.nextInt(100);
            var offset = random.nextInt(valueBytes.length - size);
            for (var builder : built.get() ? List.of(mixed) : List.of(oneLength, mixed)) {
                if (removes) {
                    builder.delete(key, 0, key.length, hash, stamp[0]);
                } else {
                    builder.put(key, 0, key.length, hash, stamp[0], valueChunk, offset, size);
                }
            }
            stamp[0]++;
            model.put(ByteBuffer.wrap(key), removes ? null : Arrays.copyOfRange(valueBytes, offset, offset + size));
        };
        for (var id = 0; id < before; id++) {
            var key = longKey(id % 4, id, length);
            write.accept(key);
            keys.add(key);
            if (random.nextInt(10) == 0) {
                // A write of a key the builders hold, which places none
                write.accept(keys.get(random.nextInt(keys.size())));
            }
        }
        checkBuilder(oneLength, model, random);
        var run = oneLength.build(null, null);
        built.set(true);
        assertEquals(before, run.size());
        if (before > 0) {
            assertEquals(length, run.keyLengthForTests());
            assertEquals(0, run.arrayLengthsForTests()[0], "a run of keys of one length kept their ends");
            var perChunk = PackedRun.KEY_CHUNK / length;
            var chunks = run.keyChunkLengthsForTests();
            assertEquals((before + perChunk - 1) / perChunk, chunks.length, "chunks of the keys");
            assertEquals(((before - 1) % perChunk + 1) * length, chunks[chunks.length - 1], "the last chunk is not trimmed to its keys");
            checkRun(run, new TreeMap<>(model), random);
        }
        // A key of another length, then keys of that length and of the first
        var other = longKey(5, -1, length == 24 ? 7 : 24);
        write.accept(other);
        keys.add(other);
        for (var id = before; id < before + 3_000; id++) {
            var key = longKey(id % 4, id, random.nextInt(3) == 0 ? 5 + random.nextInt(KeyCodec.MAX_LENGTH - 4) : length);
            write.accept(key);
            keys.add(key);
        }
        checkBuilder(mixed, model, random);
        var converted = mixed.build(null, null);
        assertEquals(0, converted.keyLengthForTests());
        assertEquals(converted.size(), converted.arrayLengthsForTests()[0]);
        checkRun(converted, model, random);
    }

    /// Checks that `builder` finds every key of `model`, as a removal if its value is `null`, and no other key.
    private static void checkBuilder(PackedRun.Builder builder, TreeMap<ByteBuffer, byte[]> model, Random random) {
        for (var entry : model.entrySet()) {
            var found = builder.find(MemorySegment.ofArray(entry.getKey().array()));
            assertTrue(found >= 0, "the builder lost a key");
            assertEquals(entry.getValue() == null, builder.removes(found));
        }
        for (var i = 0; i < 100; i++) {
            var absent = new byte[1 + random.nextInt(KeyCodec.MAX_LENGTH)];
            random.nextBytes(absent);
            if (!model.containsKey(ByteBuffer.wrap(absent))) {
                assertEquals(-1, builder.find(MemorySegment.ofArray(absent)));
            }
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {1, 2, 3})
    @DisplayName("a builder given keys at unaligned offsets of larger arrays hashes and places each as a bulk commit's builder places it given as a segment, and finds a key it holds rather than adding it again")
    void keysFromArraysAsSegments(int seed) {
        var random = new Random(seed);
        var fromArrays = new PackedRun.Builder();
        var fromSegments = new PackedRun.Builder();
        var keys = new ArrayList<byte[]>();
        // The newest write of each key: whether it removes it
        var removed = new TreeMap<ByteBuffer, Boolean>(Expected::compare);
        var stamp = 0L;
        while (fromArrays.size() < 30_000) {
            byte[] key;
            if (!keys.isEmpty() && random.nextInt(6) == 0) {
                key = keys.get(random.nextInt(keys.size()));
            } else {
                // Short keys repeat by chance too, long ones cross the ends of the key chunks
                key = new byte[random.nextInt(8) == 0 ? 1 + random.nextInt(KeyCodec.MAX_LENGTH) : 1 + random.nextInt(40)];
                random.nextBytes(key);
            }
            var from = random.nextInt(24);
            var body = new byte[from + key.length + random.nextInt(9)];
            random.nextBytes(body);
            System.arraycopy(key, 0, body, from, key.length);
            var hash = Segments.hash(body, from, key.length);
            var segment = MemorySegment.ofArray(key.clone()).asReadOnly();
            assertEquals(Segments.hash(segment), hash);
            assertTrue(fromArrays.takes(key.length, null, 0));
            var held = fromArrays.find(segment);
            var size = fromArrays.size();
            var removes = random.nextInt(4) == 0;
            if (removes) {
                fromArrays.delete(body, from, key.length, hash, stamp);
            } else {
                fromArrays.put(body, from, key.length, hash, stamp, null, 0, 0);
            }
            stamp++;
            removed.put(ByteBuffer.wrap(key), removes);
            var entry = fromArrays.find(segment, hash);
            assertEquals(removes, fromArrays.removes(entry));
            if (held >= 0) {
                assertEquals(held, entry);
                assertEquals(size, fromArrays.size());
                continue;
            }
            keys.add(key);
            assertEquals(size, entry);
            assertEquals(entry, fromSegments.putHome(segment, Segments.hash(segment), stamp, null, 0, 0));
        }
        var arrays = fromArrays.build(null, null);
        var segments = fromSegments.build(null, null);
        assertEquals(segments.size(), arrays.size());
        assertArrayEquals(segments.keyChunkLengthsForTests(), arrays.keyChunkLengthsForTests());
        for (var entry = 0; entry < arrays.size(); entry++) {
            assertEquals(0, Segments.compare(segments.key(entry), arrays.key(entry)));
        }
        for (var i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            assertEquals(i, arrays.find(MemorySegment.ofArray(key)));
            assertEquals(i, arrays.find(key));
            assertEquals(i, segments.find(MemorySegment.ofArray(key)));
            assertEquals(removed.get(ByteBuffer.wrap(key)), arrays.removes(i));
        }
    }

    /// {@return the keys `model` puts, in order, each with its value, in hex}
    private static List<String> live(SortedMap<ByteBuffer, byte[]> model) {
        var live = new ArrayList<String>();
        model.forEach((key, value) -> {
            if (value != null) {
                live.add(HexFormat.of().formatHex(key.array()) + "=" + HexFormat.of().formatHex(value));
            }
        });
        return live;
    }

    /// {@return the entries a cursor over `run` alone returns from `from` to `to`, either `null` for no bound, in hex}
    private static List<String> scanRun(PackedRun run, byte[] from, byte[] to, boolean descending) {
        var cursor = new PackedRun.Cursor(run, null, from == null ? null : MemorySegment.ofArray(from),
                to == null ? null : MemorySegment.ofArray(to), descending);
        var entries = new ArrayList<String>();
        while (cursor.next()) {
            var key = cursor.key().toArray(ValueLayout.JAVA_BYTE);
            assertEquals(0, cursor.compareKey(MemorySegment.ofArray(key)));
            entries.add(HexFormat.of().formatHex(key) + "=" + HexFormat.of().formatHex(cursor.value().toArray(ValueLayout.JAVA_BYTE)));
        }
        cursor.release();
        return entries;
    }

    static IntStream seeds() {
        return IntStream.range(0, 32);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    @DisplayName("a store that recovers into runs recovers a state between its last durable commit and its last visible one, whatever point a crash or a loss of power stops it at, and again after the recovery")
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
        // Some seeds keep a last budget of the log on chains, over the runs of the writes before it
        var budget = Workload.budget(seed);
        Expected after;
        try (var ledger = open(survivor.getPath("/store"), encrypted, budget)) {
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
            after = match.copy();
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(new byte[]{2}, new byte[]{3})))
                    .awaitDurable();
            after.deleteChat(2);
            write(ledger, after, 0, 500, 8, 0);
            verify(ledger, after);
            // A second crash, before the first checkpoint after the recovery
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, budget)) {
            verify(ledger, after);
            ledger.checkpoint();
            verify(ledger, after);
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, budget)) {
            verify(ledger, after);
        }
    }

    private static boolean matches(Ledger ledger, Expected expected) {
        var messages = new TreeMap<ByteBuffer, byte[]>(Expected::compare);
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            messages.put(ByteBuffer.wrap(key), value);
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
        return texts.equals(expected.texts);
    }

    /// A workload drawn from a seed: batches over a few chats, removals of keys and of chats, transactions,
    /// checkpoints and reopens; and the state after each commit, the last it made visible and the last it knows
    /// durable.
    private static final class Workload {
        private final Random random;
        private final boolean encrypted;
        private final long budget;
        final List<Expected> history;
        Expected state;
        int visible;
        int durable;

        Workload(int seed, boolean encrypted) {
            this.random = new Random(seed);
            this.encrypted = encrypted;
            this.budget = budget(seed);
            this.history = new ArrayList<>();
            this.state = new Expected();
            history.add(state.copy());
        }

        /// {@return the bytes of the log's last budget of the stores of `seed`: none, or a few commits}
        static long budget(int seed) {
            return seed % 4 == 1 ? 4 << 10 : 0;
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted, budget);
                for (var step = 0; step < 50; step++) {
                    var choice = random.nextInt(24);
                    if (choice == 0) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (choice == 1) {
                        ledger.closeWithoutCheckpoint();
                        durable = visible;
                        ledger = open(directory, encrypted, budget);
                    } else if (choice == 2) {
                        var next = state.copy();
                        var chat = random.nextInt(4);
                        var commit = ledger.commit(ledger.newBatch()
                                .deleteRange(MESSAGES, KeyRange.between(new byte[]{(byte) chat}, new byte[]{(byte) (chat + 1)})));
                        next.deleteChat(chat);
                        commit(next);
                        commit.awaitDurable();
                        durable = visible;
                    } else if (choice < 6 && !state.messages.isEmpty()) {
                        var next = state.copy();
                        var keys = new ArrayList<>(next.messages.keySet());
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 5; i++) {
                            var removed = keys.get(random.nextInt(keys.size())).array();
                            batch.delete(MESSAGES, removed);
                            next.delete(removed);
                        }
                        var commit = ledger.commit(batch);
                        commit(next);
                        if (random.nextBoolean()) {
                            commit.awaitDurable();
                            durable = visible;
                        }
                    } else {
                        var next = state.copy();
                        var chat = random.nextInt(4);
                        var first = random.nextInt(40);
                        var count = 1 + random.nextInt(15);
                        var batch = ledger.newBatch();
                        for (var id = first; id < first + count; id++) {
                            var value = value(chat * 1_000 + id, step);
                            batch.put(MESSAGES, key(chat, id), value);
                            next.put(key(chat, id), value);
                            var text = text(id, step);
                            batch.put(TEXTS, textKey(id), text);
                            next.texts.put(textKey(id), text);
                        }
                        var commit = ledger.commit(batch);
                        commit(next);
                        if (random.nextInt(3) == 0) {
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

        private void commit(Expected next) {
            state = next;
            visible++;
            history.add(next.copy());
        }
    }

    @Test
    @DisplayName("the run of a key space no write of the replay reached is absent, and one of a store without a queue of values is never built")
    void noRunWithoutMovedWrites(@TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            write(ledger, expected, 0, 0, 20, 0);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            assertNull(ledger.state(MESSAGES).run, "a store that moves no value built a run");
            verify(ledger, expected);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true, 0)) {
            assertNotNull(ledger.state(MESSAGES).run);
            assertNull(ledger.state(CHATS).run, "a resident key space built a run");
            verify(ledger, expected);
        }
    }

    /// Puts message `id` into `builder`, stamped `stamp`: in the log for three of every four ids, at a body index of
    /// `id` plus `generation` so an overwrite is told apart, and in `chunk` for the rest.
    private static void putReserved(PackedRun.Builder builder, int id, long stamp, int generation, MemorySegment chunk) {
        var key = key(id % 4, id);
        if (id % 4 == 3) {
            put(builder, key, stamp, chunk, id % 32, 8);
        } else {
            putLogged(builder, key, stamp, 64, id + generation, id, generation);
        }
    }

    /// Puts `key` into `builder` as the replay does, a value of `length` bytes at `offset` of `chunk`, or none.
    private static void put(PackedRun.Builder builder, byte[] key, long stamp, MemorySegment chunk, int offset, int length) {
        builder.put(key, 0, key.length, Segments.hash(key, 0, key.length), stamp, chunk, offset, length);
    }

    /// Puts `key` into `builder` as the replay does, a value of 300 bytes in the log, at `bodyIndex` of the frame at
    /// `frameOffset` of no segment, with the tag `tagHigh` and `tagLow`.
    private static void putLogged(PackedRun.Builder builder, byte[] key, long stamp, long frameOffset, int bodyIndex,
                                  long tagHigh, int tagLow) {
        builder.putLogged(key, 0, key.length, Segments.hash(key, 0, key.length), stamp, null, frameOffset, bodyIndex,
                tagHigh, tagLow, 300);
    }

    @Test
    @DisplayName("a builder asked for room grows the ends of its keys to it when they are next full, keeps the places of its logged values in whole chunks that take no room in advance, doubles after, never shrinks an array, and builds a run trimmed to its entries")
    void builderReserves() {
        var chunk = MemorySegment.ofArray(new byte[64]).asReadOnly();
        var builder = new PackedRun.Builder();
        var stamp = 0L;
        for (var id = 0; id < 4_096; id++) {
            putReserved(builder, id, stamp++, 0, chunk);
        }
        assertEquals(4_096, builder.capacity());
        // Three quarters of the entries are logged, in a first chunk grown to a whole one and a second made whole
        assertArrayEquals(new int[]{2_048, 2_048}, builder.loggedLengthsForTests());
        builder.reserve(10_000);
        // Overwrites take no room, so the arrays wait for the next new key
        putReserved(builder, 5, stamp++, 1, chunk);
        assertEquals(4_096, builder.capacity());
        putReserved(builder, 4_096, stamp++, 0, chunk);
        assertEquals(10_000, builder.capacity());
        assertArrayEquals(new int[]{2_048, 2_048}, builder.loggedLengthsForTests(), "the logged values took room in advance");
        for (var id = 4_097; id < 10_001; id++) {
            putReserved(builder, id, stamp++, 0, chunk);
        }
        assertEquals(20_000, builder.capacity(), "the growth after the reserved one did not double");
        for (var id = 10_001; id < 20_000; id++) {
            putReserved(builder, id, stamp++, 0, chunk);
        }
        builder.reserve(5_000);
        putReserved(builder, 20_000, stamp++, 0, chunk);
        assertEquals(40_000, builder.capacity(), "room below the entries held did not leave the arrays to double");
        var run = builder.build(null, null);
        assertEquals(20_001, run.size());
        assertEquals(5, run.keyLengthForTests());
        var lengths = run.arrayLengthsForTests();
        assertEquals(0, lengths[0], "a run of keys of one length kept their ends");
        assertEquals(20_001, lengths[1]);
        for (var i = 2; i < lengths.length; i++) {
            assertEquals(15_001, lengths[i], "logged values trimmed");
        }
        for (var id = 0; id <= 20_000; id++) {
            var entry = run.find(MemorySegment.ofArray(key(id % 4, id)));
            assertTrue(entry >= 0, "lost key " + id);
            var source = run.sourceForTests(entry);
            if (id % 4 == 3) {
                assertSame(chunk, source);
            } else {
                var logged = assertInstanceOf(LogValue.class, source);
                var generation = id == 5 ? 1 : 0;
                assertEquals(id + generation, logged.bodyIndex(), "body of " + id);
                assertEquals(id, logged.tagHigh());
                assertEquals(generation, logged.tagLow());
            }
        }
    }

    @Test
    @DisplayName("room for fewer logged values than a builder holds, left by overwrites of logged values with short ones, never shrinks their arrays")
    void reserveKeepsOrphanedLoggedValues() {
        var chunk = MemorySegment.ofArray(new byte[64]).asReadOnly();
        var builder = new PackedRun.Builder();
        var stamp = 0L;
        for (var id = 0; id < 4_096; id++) {
            putLogged(builder, key(id % 4, id), stamp++, 0, id, 0, 0);
        }
        // Each overwrite by a short value and then by a logged one leaves the first logged place unused
        for (var id = 0; id < 4_096; id++) {
            var key = key(id % 4, id);
            put(builder, key, stamp++, chunk, 0, 8);
            putLogged(builder, key, stamp++, 0, -id, 0, 0);
        }
        assertArrayEquals(new int[]{2_048, 2_048, 2_048, 2_048}, builder.loggedLengthsForTests());
        builder.reserve(6_000);
        put(builder, key(0, 4_096), stamp, chunk, 0, 8);
        assertEquals(6_000, builder.capacity());
        assertArrayEquals(new int[]{2_048, 2_048, 2_048, 2_048}, builder.loggedLengthsForTests());
        var run = builder.build(null, null);
        var lengths = run.arrayLengthsForTests();
        assertEquals(0, lengths[0], "a run of keys of one length kept their ends");
        assertEquals(4_097, lengths[1]);
        for (var i = 2; i < lengths.length; i++) {
            assertEquals(8_192, lengths[i]);
        }
        for (var id = 0; id < 4_096; id++) {
            var logged = assertInstanceOf(LogValue.class, run.sourceForTests(run.find(MemorySegment.ofArray(key(id % 4, id)))));
            assertEquals(-id, logged.bodyIndex());
        }
    }

    /// A replay drawn as frames of the same length: the new keys and the overwrites each frame gives one run, the frames
    /// the last budget holds, which give it none, and the most growths of its arrays from [ReplayValues#RESERVE_AT]
    /// entries on.
    record Reservation(String name, int frames, int budgetFrames, IntUnaryOperator fresh, IntUnaryOperator overwrites,
                       int growths) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Reservation> reservations() {
        return Stream.of(
                new Reservation("new keys at an even rate", 100, 0, _ -> 500, _ -> 0, 1),
                new Reservation("new keys before the last budget", 100, 30, _ -> 500, _ -> 50, 1),
                new Reservation("overwrites after a dense start", 100, 0, frame -> frame < 10 ? 1_000 : 0,
                        frame -> frame < 10 ? 0 : 500, 1),
                new Reservation("a key space first written half way", 100, 0, frame -> frame < 50 ? 0 : 1_000, _ -> 0, 1),
                new Reservation("a sparse start before dense frames", 100, 0, frame -> frame < 50 ? 10 : 1_000, _ -> 0, 8),
                new Reservation("one frame denser than the rest", 100, 0, frame -> frame == 0 ? 20_000 : 100, _ -> 0, 3),
                new Reservation("past the entries a reservation multiplies by sixteen", 1_000, 0, _ -> 600, _ -> 0, 4));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("reservations")
    @DisplayName("a run's builder, full from 4,096 entries on, grows to the entries the rest of the log is expected to give it at the rate of its frames so far, at least a quarter more and at most sixteen times up to the reservation's bound, and builds a run trimmed to its entries")
    void replayReservesRuns(Reservation reservation) {
        var frameBytes = 1L << 16;
        var total = reservation.frames() * frameBytes;
        var budget = reservation.budgetFrames() * frameBytes;
        var values = new ReplayValues();
        var state = new KeyspaceState(MESSAGES);
        var random = new Random(reservation.frames());
        var keys = 0;
        var stamp = 0L;
        var growths = 0;
        for (var frame = 0; frame < reservation.frames(); frame++) {
            var logFrom = total - frame * frameBytes;
            if (logFrom <= budget) {
                break;
            }
            values.reached(logFrom);
            var fresh = reservation.fresh().applyAsInt(frame);
            var writes = fresh + (keys == 0 ? 0 : reservation.overwrites().applyAsInt(frame));
            for (var i = 0; i < writes; i++) {
                var run = values.run(state);
                var size = run.size();
                var capacity = run.capacity();
                var id = i < fresh ? keys++ : random.nextInt(keys);
                var key = key(id % 4, id);
                if (random.nextInt(10) == 0) {
                    run.delete(key, 0, key.length, Segments.hash(key, 0, key.length), stamp++);
                } else {
                    put(run, key, stamp++, null, 0, 0);
                }
                if (run.capacity() != capacity) {
                    assertEquals(capacity, size, "the arrays grew before they were full");
                    var grown = run.capacity();
                    assertTrue(grown >= capacity + capacity / 4, "grew from " + capacity + " to " + grown);
                    if (capacity >= ReplayValues.RESERVE_AT) {
                        growths++;
                        assertTrue(grown <= Math.max(2L * capacity, Math.min(16L * capacity, ReplayValues.RESERVE_ENTRIES)),
                                "grew from " + capacity + " to " + grown);
                    } else {
                        assertEquals(2 * capacity, grown, "a builder below the reservation's entries did not double");
                    }
                }
                values.reserve(run, budget);
            }
        }
        assertTrue(growths <= reservation.growths(), growths + " growths");
        var run = values.stopRun(state).build(null, null);
        assertEquals(keys, run.size());
        var lengths = run.arrayLengthsForTests();
        assertEquals(0, lengths[0], "a run of keys of one length kept their ends");
        assertEquals(keys, lengths[1]);
        for (var id = 0; id < keys; id += 97) {
            assertTrue(run.find(MemorySegment.ofArray(key(id % 4, id))) >= 0, "lost key " + id);
        }
    }

    static Stream<Arguments> reservedRecoveries() {
        return Stream.of(Arguments.of(true, "plateau"), Arguments.of(false, "plateau"), Arguments.of(true, "late"),
                Arguments.of(false, "late"), Arguments.of(true, "sparse"), Arguments.of(false, "sparse"));
    }

    @ParameterizedTest(name = "encrypted: {0}, {1}")
    @MethodSource("reservedRecoveries")
    @DisplayName("a store that loses power while its log holds runs of more than 4,096 entries, written densely then overwritten, late in the log or after a sparse start, recovers the state of a commit it made durable, and again after a second crash and a checkpoint")
    void reservedRunsRecover(boolean encrypted, String shape) throws Exception {
        var seed = shape.hashCode() + (encrypted ? 1 : 0);
        var dry = CrashFileSystem.create();
        var history = new ArrayList<Expected>();
        var dense = new long[1];
        var durable = reservedWorkload(dry.getPath("/store"), encrypted, shape, seed, history, dense);
        assertEquals(history.size() - 1, durable, "the workload stopped on a file system that never stops");
        var disk = CrashFileSystem.create();
        var random = new Random(seed);
        // A stop after the dense commits, once the log holds more than RESERVE_AT keys of the run
        disk.stopAfter(dense[0] + random.nextLong(dry.changes() - dense[0] + 1));
        history.clear();
        durable = reservedWorkload(disk.getPath("/store"), encrypted, shape, seed, history, dense);
        var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
        Expected after;
        try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run, "the replay built no run");
            assertTrue(run.size() > ReplayValues.RESERVE_AT, "the run holds " + run.size() + " entries");
            assertTrue(ledger.replayReservationsForTests() > 0, "the replay grew its run by doubling alone");
            var lengths = run.arrayLengthsForTests();
            assertEquals(0, lengths[0], "a run of keys of one length kept their ends");
            assertEquals(run.size(), lengths[1]);
            Expected match = null;
            for (var commit = durable; commit < history.size() && match == null; commit++) {
                if (matches(ledger, history.get(commit))) {
                    match = history.get(commit);
                }
            }
            if (match == null) {
                fail("The store recovered a state no commit from " + durable + " to " + (history.size() - 1) + " left");
            }
            verify(ledger, match);
            after = match.copy();
            write(ledger, after, 1, 90_000, 20, 7);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
            verify(ledger, after);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, after);
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
            verify(ledger, after);
        }
    }

    /// Runs the workload of `shape` on `directory`, adding the state after each commit to `history`, which starts with
    /// the empty state, and closes the store without a checkpoint; stops at the first failure of the file system.
    ///
    /// @param dense where the changes of the file system stood once the dense commits were durable
    /// @return the last commit the store made durable
    private static int reservedWorkload(Path directory, boolean encrypted, String shape, int seed, List<Expected> history,
                                        long[] dense) {
        var random = new Random(seed);
        var state = new Expected();
        history.add(state.copy());
        var durable = 0;
        LiveLedger ledger = null;
        try {
            ledger = open(directory, encrypted, Long.MAX_VALUE);
            var next = 0;
            // The start: no commit, commits of texts alone, or commits of a few messages
            for (var commit = 0; commit < 30 && !shape.equals("plateau"); commit++) {
                var batch = ledger.newBatch();
                var expected = state.copy();
                for (var i = 0; i < 20; i++) {
                    if (shape.equals("late")) {
                        var text = text(commit * 20 + i, 0);
                        batch.put(TEXTS, textKey(commit * 20 + i), text);
                        expected.texts.put(textKey(commit * 20 + i), text);
                    } else {
                        var id = next++;
                        var value = value(id, 0);
                        batch.put(MESSAGES, key(id % 4, id), value);
                        expected.put(key(id % 4, id), value);
                    }
                }
                history.add(expected.copy());
                ledger.commit(batch).awaitDurable();
                state = expected;
                durable = history.size() - 1;
            }
            // Dense commits of new keys
            for (var commit = 0; commit < 16; commit++) {
                var batch = ledger.newBatch();
                var expected = state.copy();
                for (var i = 0; i < 400; i++) {
                    var id = next++;
                    var value = value(id, 1);
                    batch.put(MESSAGES, key(id % 4, id), value);
                    expected.put(key(id % 4, id), value);
                }
                history.add(expected.copy());
                ledger.commit(batch).awaitDurable();
                state = expected;
                durable = history.size() - 1;
            }
            dense[0] = ((CrashFileSystem) directory.getFileSystem()).changes();
            // Overwrites and removals of the keys written, with no new key
            for (var commit = 0; commit < (shape.equals("plateau") ? 24 : 4); commit++) {
                var batch = ledger.newBatch();
                var expected = state.copy();
                for (var i = 0; i < 200; i++) {
                    var id = random.nextInt(next);
                    if (random.nextInt(8) == 0) {
                        batch.delete(MESSAGES, key(id % 4, id));
                        expected.delete(key(id % 4, id));
                    } else {
                        var value = value(id, 2 + commit);
                        batch.put(MESSAGES, key(id % 4, id), value);
                        expected.put(key(id % 4, id), value);
                    }
                }
                history.add(expected.copy());
                ledger.commit(batch).awaitDurable();
                state = expected;
                durable = history.size() - 1;
            }
            ledger.closeWithoutCheckpoint();
        } catch (Exception | AssertionError failure) {
            if (ledger != null) {
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // The file system stopped: closing only lets go of the threads
                }
            }
        }
        return durable;
    }

    /// {@return a key of the tests of a bulk commit's hash table, `id` after a fixed prefix}
    private static byte[] tableKey(int id) {
        return ByteBuffer.allocate(8).putInt(0x7AB1E000).putInt(id).array();
    }

    /// {@return the keys `first` to `first + count`, excluded}
    private static List<byte[]> tableKeys(int first, int count) {
        var keys = new ArrayList<byte[]>(count);
        for (var id = first; id < first + count; id++) {
            keys.add(tableKey(id));
        }
        return keys;
    }

    /// Puts `keys`, which the run holds no entry of, into `builder` as a bulk commit puts empty values.
    private static void putEmpty(PackedRun.Builder builder, List<byte[]> keys) {
        var stamp = 0L;
        for (var key : keys) {
            var segment = MemorySegment.ofArray(key);
            assertTrue(builder.fits(segment, null, 0, 0));
            builder.putHome(segment, Segments.hash(segment), stamp++, null, 0, 0);
        }
    }

    /// Checks that `run` finds each key of `present` through both of its finds, and none of `absent`.
    private static void assertFinds(PackedRun run, List<byte[]> present, List<byte[]> absent) {
        for (var key : present) {
            var entry = run.find(key);
            assertTrue(entry >= 0 && entry < run.size(), () -> "lost " + HexFormat.of().formatHex(key));
            assertEquals(entry, run.find(MemorySegment.ofArray(key)));
            assertArrayEquals(key, run.key(entry).toArray(ValueLayout.JAVA_BYTE));
        }
        for (var key : absent) {
            assertEquals(-1, run.find(key), () -> "found " + HexFormat.of().formatHex(key));
            assertEquals(-1, run.find(MemorySegment.ofArray(key)));
        }
    }

    /// {@return whether `value` is a power of two}
    private static boolean powerOfTwo(int value) {
        return value > 0 && (value & (value - 1)) == 0;
    }

    @ParameterizedTest(name = "{0} entries")
    @ValueSource(ints = {1, 2, 3, 5, 7, 9, 13, 100, 2_548, 666_667})
    @DisplayName("a bulk commit's new run takes a hash table two thirds full of its entries, of any length, and finds every key through both finds and no other")
    void commitTableTwoThirds(int entries) {
        var builder = PackedRun.Builder.forCommit(entries, 8L * entries, null, 0);
        var keys = tableKeys(0, entries);
        putEmpty(builder, keys);
        var run = builder.buildCommitted(null);
        assertEquals(Math.max(2, entries + (entries + 1) / 2), run.slotsForTests());
        assertFinds(run, keys, tableKeys(entries, Math.min(entries, 1_000) + 10));
    }

    @ParameterizedTest(name = "{0} entries")
    @ValueSource(ints = {2, 3, 6, 7, 10, 11, 666_667})
    @DisplayName("keys whose probes start at the last slot of a table of odd length wrap to its first slots, and a find of a key of no entry that starts there wraps to an empty slot")
    void probesWrap(int entries) {
        var slots = entries + (entries + 1) / 2;
        assertEquals(1, slots & 1, "the table's length is even");
        // Keys whose probe starts at the last slot, up to half the entries and at least two, then keys of no entry
        // whose probe starts there too
        var wanted = Math.min(Math.max(2, entries / 2 + 1), 64);
        var last = new ArrayList<byte[]>();
        var absent = new ArrayList<byte[]>();
        var probe = ByteBuffer.wrap(tableKey(0));
        for (var id = 0; last.size() < wanted || absent.size() < 8; id++) {
            var key = probe.putInt(4, id).array();
            if (PackedRun.home(Segments.hash(key), slots) == slots - 1) {
                if (last.size() < wanted) {
                    last.add(key.clone());
                } else {
                    absent.add(key.clone());
                }
            }
        }
        var keys = new ArrayList<>(last.subList(0, Math.min(entries, wanted)));
        for (var id = -1; keys.size() < entries; id--) {
            keys.add(tableKey(id));
        }
        var builder = PackedRun.Builder.forCommit(entries, 8L * entries, null, 0);
        putEmpty(builder, keys);
        var run = builder.buildCommitted(null);
        assertEquals(slots, run.slotsForTests());
        assertFinds(run, keys, absent);
    }

    @Test
    @DisplayName("the probe of a table of any length starts in it, and spreads hashes over it evenly")
    void homeInTable() {
        var random = new Random(7);
        for (var length : new int[]{2, 3, 5, 11, 1_500, 150_000, 1_000_001, 1 << 20, PackedRun.MAX_ENTRIES + PackedRun.MAX_ENTRIES / 2}) {
            for (var i = 0; i < 10_000; i++) {
                var home = PackedRun.home(random.nextInt(), length);
                assertTrue(home >= 0 && home < length, "home " + home + " of " + length);
            }
            assertEquals(0, PackedRun.home(0, length));
            assertEquals(length - 1, PackedRun.home(-1, length));
        }
        // 300,000 random hashes over 30 equal parts of a table of 3,000 slots: each part within 10% of the mean
        var parts = new int[30];
        for (var i = 0; i < 300_000; i++) {
            parts[PackedRun.home(random.nextInt(), 3_000) / 100]++;
        }
        for (var part : parts) {
            assertTrue(Math.abs(part - 10_000) < 1_000, "part of " + part);
        }
    }

    @Test
    @DisplayName("a new run whose commit took fewer entries than it planned keeps its table while they fill half of it, and takes one two thirds full of them below")
    void newRunGivesBackTable() {
        // 1,000 planned puts: 1,500 slots, kept down to 750 entries
        var kept = PackedRun.Builder.forCommit(1_000, 8_000, null, 0);
        var keys = tableKeys(0, 750);
        putEmpty(kept, keys);
        var run = kept.buildCommitted(null);
        assertEquals(1_500, run.slotsForTests());
        assertFinds(run, keys, tableKeys(750, 300));
        var shrunk = PackedRun.Builder.forCommit(1_000, 8_000, null, 0);
        keys = tableKeys(0, 749);
        putEmpty(shrunk, keys);
        run = shrunk.buildCommitted(null);
        assertEquals(749 + 375, run.slotsForTests());
        assertFinds(run, keys, tableKeys(749, 300));
    }

    @Test
    @DisplayName("an append to a run of a table two thirds full keeps the table while the entries fill at most three quarters of it, and makes a power of two past that, where older views and the newest find their own keys")
    void appendsToCommitTable() {
        var builder = PackedRun.Builder.forCommit(1_000, 8_000, null, 0);
        var first = tableKeys(0, 1_000);
        putEmpty(builder, first);
        var run = builder.buildCommitted(null);
        assertEquals(1_500, run.slotsForTests());
        // 125 more fill three quarters of the table exactly
        var appending = PackedRun.Builder.appending(run, 125, 1_000);
        var second = tableKeys(1_000, 125);
        putEmpty(appending, second);
        var appended = appending.buildCommitted(null);
        assertEquals(1_500, appended.slotsForTests(), "an append within three quarters made a table");
        var both = new ArrayList<>(first);
        both.addAll(second);
        assertFinds(run, first, second);
        assertFinds(appended, both, tableKeys(1_125, 100));
        // One more key is past three quarters: a power of two
        appending = PackedRun.Builder.appending(appended, 1, 8);
        var third = tableKeys(1_125, 1);
        putEmpty(appending, third);
        var grown = appending.buildCommitted(null);
        assertEquals(2_048, grown.slotsForTests());
        var all = new ArrayList<>(both);
        all.addAll(third);
        assertFinds(run, first, tableKeys(1_000, 126));
        assertFinds(appended, both, third);
        assertFinds(grown, all, tableKeys(1_126, 100));
    }

    @Test
    @DisplayName("readers of the first view of a run of a table two thirds full find its keys and none of the later commits' while appends grow the table past three quarters")
    void readersAcrossAppendRehash() throws Exception {
        var builder = PackedRun.Builder.forCommit(3_000, 24_000, null, 0);
        var first = tableKeys(0, 3_000);
        putEmpty(builder, first);
        var run = builder.buildCommitted(null);
        assertEquals(4_500, run.slotsForTests());
        // Appends of 375 keys: the first fills three quarters of the table, the second makes a power of two
        var later = tableKeys(3_000, 18_750);
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var readers = new ArrayList<Thread>();
        for (var r = 0; r < 2; r++) {
            var seed = r;
            readers.add(Thread.ofPlatform().start(() -> {
                var random = new Random(seed);
                try {
                    while (!stop.get()) {
                        var key = first.get(random.nextInt(first.size()));
                        var entry = run.find(key);
                        if (entry < 0 || entry >= run.size() || run.find(MemorySegment.ofArray(key)) != entry) {
                            throw new AssertionError("the first view lost " + HexFormat.of().formatHex(key));
                        }
                        var absent = later.get(random.nextInt(later.size()));
                        if (run.find(absent) != -1 || run.find(MemorySegment.ofArray(absent)) != -1) {
                            throw new AssertionError("the first view found a later key");
                        }
                    }
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            }));
        }
        var newest = run;
        var held = new ArrayList<>(first);
        var tables = new ArrayList<Integer>();
        for (var from = 0; from < later.size(); from += 375) {
            var keys = later.subList(from, from + 375);
            var appending = PackedRun.Builder.appending(newest, keys.size(), 8L * keys.size());
            putEmpty(appending, keys);
            newest = appending.buildCommitted(null);
            held.addAll(keys);
            tables.add(newest.slotsForTests());
        }
        stop.set(true);
        for (var reader : readers) {
            reader.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertEquals(4_500, tables.getFirst(), "an append within three quarters made a table");
        assertTrue(powerOfTwo(tables.get(1)) && powerOfTwo(tables.getLast()), "the table past three quarters is not a power of two");
        assertFinds(run, first, later.subList(0, 200));
        assertFinds(newest, held, tableKeys(21_750, 200));
    }

    @Test
    @DisplayName("a commit taken back as it appends to a run of a table two thirds full gives back the table of exactly its earlier length, which finds every key of the newest view, and the next append takes it")
    void rollbackRestoresCommitTable() {
        var builder = PackedRun.Builder.forCommit(1_000, 8_000, null, 0);
        var first = tableKeys(0, 1_000);
        putEmpty(builder, first);
        var run = builder.buildCommitted(null);
        assertEquals(1_500, run.slotsForTests());
        var taken = PackedRun.Builder.appending(run, 4_000, 32_000);
        var takenKeys = tableKeys(1_000, 4_000);
        putEmpty(taken, takenKeys);
        assertTrue(powerOfTwo(run.slotsForTests()), "the append made no larger table");
        taken.rollback();
        assertEquals(1_500, run.slotsForTests(), "the rollback kept the larger table");
        assertFinds(run, first, takenKeys);
        var next = PackedRun.Builder.appending(run, 100, 800);
        var nextKeys = tableKeys(5_000, 100);
        putEmpty(next, nextKeys);
        var appended = next.buildCommitted(null);
        assertEquals(1_500, appended.slotsForTests());
        var both = new ArrayList<>(first);
        both.addAll(nextKeys);
        assertFinds(appended, both, takenKeys);
        assertFinds(run, first, nextKeys);
    }
}
