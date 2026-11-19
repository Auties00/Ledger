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
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            // Commits of tens of messages each: the run places their logged values once per frame
            assertTrue(run.framePairsForTests() > 0, "the run keeps a place per value of commits of many values");
        }
    }

    /// Records in `builder` a put of `key`, write `op` of commit `sequence`, of a value of 200 bytes in the frame at
    /// `frameOffset` of `segment`, and in `model` its place.
    private static void putLogged(PackedRun.Builder builder, TreeMap<ByteBuffer, LogValue> model, String key,
                                  LogSegmentView segment, long frameOffset, long sequence, int op) {
        // A body index and a tag of each write's own, so a value placed by another write's place differs
        var logged = new LogValue(segment, frameOffset, op * 1_000 + (int) sequence, sequence, op);
        builder.putLogged(MemorySegment.ofArray(key.getBytes()), Reads.stamp(sequence, op), logged, 200);
        model.put(ByteBuffer.wrap(key.getBytes()), logged);
    }

    /// Checks that `run` places every logged value of `model` where its write put it, and finds a chunk for the
    /// others, mapped to `null`.
    private static void checkPlaces(PackedRun run, TreeMap<ByteBuffer, LogValue> model) {
        for (var entry : model.entrySet()) {
            var name = new String(entry.getKey().array());
            var found = run.find(MemorySegment.ofArray(entry.getKey().array()));
            assertTrue(found >= 0, () -> "no entry of " + name);
            var source = run.sourceForTests(found);
            if (entry.getValue() == null) {
                assertInstanceOf(MemorySegment.class, source, name);
            } else {
                assertEquals(entry.getValue(), source, () -> "the place of " + name);
            }
        }
    }

    private static LogSegmentView segment(long number) {
        return new LogSegmentView(number, Path.of("segment-" + number), null, null);
    }

    @Test
    @DisplayName("a run places the logged values of commits that share frames, overwritten across commits, twice in one commit, after a short value and in two segments, by one pair per frame")
    void framesTable() {
        var first = segment(1);
        var second = segment(2);
        var chunk = MemorySegment.ofArray(new byte[64]);
        var builder = new PackedRun.Builder();
        var model = new TreeMap<ByteBuffer, LogValue>();
        for (long sequence = 1; sequence <= 40; sequence++) {
            // Three commits per frame, the first seven frames in one segment and the rest in the next
            var frame = (sequence - 1) / 3;
            var segment = frame < 7 ? first : second;
            var frameOffset = 4_096 * frame;
            var op = 0;
            for (var i = 0; i < 4; i++) {
                putLogged(builder, model, "new-" + sequence + "-" + i, segment, frameOffset, sequence, op++);
            }
            if (sequence > 1) {
                putLogged(builder, model, "new-" + (sequence - 1) + "-0", segment, frameOffset, sequence, op++);
            }
            putLogged(builder, model, "twice-" + sequence, segment, frameOffset, sequence, op++);
            putLogged(builder, model, "twice-" + sequence, segment, frameOffset, sequence, op++);
            var key = "short-then-logged".getBytes();
            if (sequence % 3 == 0) {
                builder.put(MemorySegment.ofArray(key), Reads.stamp(sequence, op++), chunk, 0, 40);
                model.put(ByteBuffer.wrap(key), null);
            } else {
                putLogged(builder, model, "short-then-logged", segment, frameOffset, sequence, op++);
            }
            putLogged(builder, model, "often-" + sequence % 5, segment, frameOffset, sequence, op);
        }
        var run = builder.build(null, null);
        assertEquals(14, run.framePairsForTests(), "a pair per frame");
        checkPlaces(run, model);
    }

    @Test
    @DisplayName("a run of commits of one logged value each keeps a place per value when each commit has a frame of its own, and a pair per frame when commits share frames")
    void oneValuePerCommit() {
        var segment = segment(1);
        var own = new PackedRun.Builder();
        var ownModel = new TreeMap<ByteBuffer, LogValue>();
        for (long sequence = 1; sequence <= 50; sequence++) {
            putLogged(own, ownModel, "own-" + sequence, segment, 512 * sequence, sequence, 0);
        }
        var ownRun = own.build(null, null);
        assertEquals(-1, ownRun.framePairsForTests(), "a pair per value costs more than a place per value");
        checkPlaces(ownRun, ownModel);
        var shared = new PackedRun.Builder();
        var sharedModel = new TreeMap<ByteBuffer, LogValue>();
        for (long sequence = 1; sequence <= 60; sequence++) {
            putLogged(shared, sharedModel, "shared-" + sequence, segment, 4_096 * ((sequence - 1) / 6), sequence, 0);
        }
        var sharedRun = shared.build(null, null);
        assertEquals(10, sharedRun.framePairsForTests());
        checkPlaces(sharedRun, sharedModel);
    }

    @ParameterizedTest(name = "case {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("a value of an older commit in that commit's frame keeps the frames' table, and one in another frame, of an older commit or of the newest, makes the run keep a place per value")
    void valuesOutOfOrder(int outOfOrder) {
        var segment = segment(1);
        var builder = new PackedRun.Builder();
        var model = new TreeMap<ByteBuffer, LogValue>();
        for (long sequence = 1; sequence <= 10; sequence++) {
            for (var op = 0; op < 5; op++) {
                putLogged(builder, model, "k-" + sequence + "-" + op, segment, 4_096 * sequence, sequence, op);
            }
        }
        switch (outOfOrder) {
            // Commit 4 again, in its own frame
            case 0 -> putLogged(builder, model, "k-2-0", segment, 4_096 * 4, 4, 7);
            // Commit 4 again, in the frame of commit 6
            case 1 -> putLogged(builder, model, "k-2-0", segment, 4_096 * 6, 4, 7);
            // The newest commit, in a frame after its own
            default -> putLogged(builder, model, "k-9-1", segment, 4_096 * 11, 10, 7);
        }
        for (long sequence = 11; sequence <= 12; sequence++) {
            for (var op = 0; op < 5; op++) {
                putLogged(builder, model, "k-" + sequence + "-" + op, segment, 4_096 * (sequence + 1), sequence, op);
            }
        }
        var run = builder.build(null, null);
        assertEquals(outOfOrder == 0 ? 12 : -1, run.framePairsForTests());
        checkPlaces(run, model);
    }

    @ParameterizedTest(name = "encrypted: {0}, commits awaited one by one: {1}")
    @MethodSource("singleCommits")
    @DisplayName("a replay of commits of one logged message each, each in a frame of its own or several in a frame, reads every value back authenticated, before and after the checkpoint that takes the run")
    void replayedSingleValueCommits(boolean encrypted, boolean awaited, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        var commits = 300;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            for (var id = 0; id < commits; id++) {
                var value = new byte[200];
                Arrays.fill(value, (byte) id);
                value[0] = (byte) (id >>> 8);
                var commit = ledger.commit(ledger.newBatch().put(MESSAGES, key(id & 3, id), value));
                expected.put(key(id & 3, id), value);
                if (awaited || id % 50 == 49) {
                    commit.awaitDurable();
                }
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 0)) {
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            if (awaited) {
                assertEquals(-1, run.framePairsForTests(), "commits of a frame each kept a pair per value");
            } else {
                // Groups of up to 50 commits, sealed as runs of commits in shared frames
                var pairs = run.framePairsForTests();
                assertTrue(pairs > 0 && pairs < commits / 2, "the run kept " + pairs + " pairs for " + commits + " values");
            }
            for (var id = 0; id < commits; id++) {
                var entry = run.find(MemorySegment.ofArray(key(id & 3, id)));
                assertInstanceOf(LogValue.class, run.sourceForTests(entry));
                assertArrayEquals(expected.messages.get(ByteBuffer.wrap(key(id & 3, id))), run.valueBytes(entry));
            }
            verify(ledger, expected);
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run);
            verify(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    private static Stream<Arguments> singleCommits() {
        return Stream.of(Arguments.of(true, true), Arguments.of(true, false), Arguments.of(false, true),
                Arguments.of(false, false));
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
            var segment = MemorySegment.ofArray(key);
            assertTrue(builder.takes(segment, null, 100));
            var kind = random.nextInt(8);
            if (kind == 0) {
                builder.delete(segment, stamp[0]++);
                model.put(wrapped, null);
            } else if (kind == 1) {
                builder.put(segment, stamp[0]++, null, 0, 0);
                model.put(wrapped, new byte[0]);
            } else {
                var length = 1 + random.nextInt(100);
                var offset = random.nextInt(valueBytes.length - length);
                builder.put(segment, stamp[0]++, valueChunk, offset, length);
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
}
