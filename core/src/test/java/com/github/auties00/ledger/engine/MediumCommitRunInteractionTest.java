package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Uncounted runs, which idle commits of a hundred puts over a populated tree append to, beside range removals
/// counted at the commit and later, transactions, a commit that writes a counted and an uncounted run at once, a
/// checkpoint that fails over such a run, and a racing mix of them all with pipelined writers, checkpoints and counting
/// snapshots. The expected state is kept beside the store and checked through point reads, counts, scans and ranges.
class MediumCommitRunInteractionTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> TEXTS = Keyspace.paged("texts", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int POPULATED = 3_000;
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 200, 700, 1_500};

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO))
                .segmentSize(1L << 20)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
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

    private static void put(WriteBatch batch, Keyspace<byte[], byte[]> keyspace, TreeMap<Integer, byte[]> expected,
                            int message, int generation) {
        var value = value(message, generation);
        batch.put(keyspace, key(message), value);
        expected.put(message, value);
    }

    private static TreeMap<Integer, byte[]> populate(LiveLedger ledger) throws InterruptedException {
        var expected = new TreeMap<Integer, byte[]>();
        var batch = ledger.newBatch();
        for (var message = 0; message < POPULATED; message++) {
            put(batch, MESSAGES, expected, message, 0);
        }
        ledger.commit(batch).awaitDurable();
        ledger.checkpoint();
        return expected;
    }

    private static void verifyPoints(View view, Keyspace<byte[], byte[]> keyspace, TreeMap<Integer, byte[]> expected,
                                     int last) {
        assertEquals(expected.size(), view.size(keyspace), keyspace.name() + " counted");
        for (var message = 0; message <= last; message++) {
            var wanted = expected.get(message);
            var found = view.get(keyspace, key(message));
            if (wanted == null) {
                assertTrue(found.isEmpty(), keyspace.name() + " " + message + " read present");
            } else {
                assertTrue(found.isPresent(), keyspace.name() + " " + message + " absent");
                assertArrayEquals(wanted, found.get(), keyspace.name() + " " + message);
            }
        }
    }

    private static void verify(View view, Keyspace<byte[], byte[]> keyspace, TreeMap<Integer, byte[]> expected,
                               int last) {
        verifyPoints(view, keyspace, expected, last);
        var keys = new ArrayList<Integer>();
        view.forEach(keyspace, KeyRange.all(), (key, value) -> {
            var message = ByteBuffer.wrap(key).getInt();
            assertArrayEquals(expected.get(message), value, keyspace.name() + " " + message + " scanned");
            keys.add(message);
            return true;
        });
        assertEquals(new ArrayList<>(expected.keySet()), keys, keyspace.name() + " scanned");
        for (var from = 0; from < last; from += 977) {
            var to = from + 400;
            var range = view.range(keyspace, KeyRange.between(key(from), key(to)), Integer.MAX_VALUE);
            assertEquals(expected.subMap(from, to).size(), range.size(), keyspace.name() + " from " + from);
        }
    }

    // Idle commits of 100 puts appending to an uncounted run, half new keys and half keys of the tree
    private static void medium(LiveLedger ledger, TreeMap<Integer, byte[]> expected, int commit, Random random) {
        var batch = ledger.newBatch();
        for (var i = 0; i < 60; i++) {
            put(batch, MESSAGES, expected, POPULATED + commit * 100 + i, commit);
        }
        for (var i = 0; i < 40; i++) {
            put(batch, MESSAGES, expected, random.nextInt(POPULATED), commit + 1);
        }
        var removed = random.nextInt(POPULATED + commit * 100 + 60);
        batch.delete(MESSAGES, key(removed));
        expected.remove(removed);
        ledger.commit(batch);
    }

    private static void deleteRange(LiveLedger ledger, TreeMap<Integer, byte[]> expected, int from, int to,
                                    boolean withPut) {
        var batch = ledger.newBatch();
        if (withPut) {
            // An earlier write of the key space makes the commit count the removal itself
            put(batch, MESSAGES, expected, from, 77);
        }
        batch.deleteRange(MESSAGES, KeyRange.between(key(from), key(to)));
        expected.subMap(from, to).clear();
        ledger.commit(batch);
    }
    @ParameterizedTest(name = "encrypted {0}, counted inline {1}")
    @CsvSource({"true,false", "false,true", "true,true"})
    @DisplayName("a range removal over an uncounted run's keys and its tree's, snapshots before it, refused appends after it, a checkpoint and a reopen count exactly")
    void rangeRemovalOverUncountedRun(boolean encrypted, boolean inline, @TempDir Path directory) throws Exception {
        var random = new Random(3);
        var last = POPULATED + 3_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            expected = populate(ledger);
            for (var commit = 0; commit < 10; commit++) {
                medium(ledger, expected, commit, random);
            }
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            assertFalse(run.counted());
            var before = ledger.snapshot();
            var beforeSeen = new TreeMap<>(expected);
            deleteRange(ledger, expected, POPULATED - 500, POPULATED + 450, inline);
            verifyPoints(ledger, MESSAGES, expected, last);
            for (var commit = 10; commit < 15; commit++) {
                medium(ledger, expected, commit, random);
            }
            verifyPoints(ledger, MESSAGES, expected, last);
            verifyPoints(before, MESSAGES, beforeSeen, last);
            var middle = ledger.snapshot();
            var middleSeen = new TreeMap<>(expected);
            ledger.checkpoint();
            verify(ledger, MESSAGES, expected, last);
            verify(before, MESSAGES, beforeSeen, last);
            verify(middle, MESSAGES, middleSeen, last);
            before.close();
            middle.close();
            for (var commit = 15; commit < 20; commit++) {
                medium(ledger, expected, commit, random);
            }
            deleteRange(ledger, expected, POPULATED + 1_520, POPULATED + 1_730, !inline);
            verify(ledger, MESSAGES, expected, last);
            ledger.checkpoint();
            verify(ledger, MESSAGES, expected, last);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, MESSAGES, expected, last);
        }
    }

    @Test
    @DisplayName("transactions read, overwrite and remove the keys of an uncounted run and of its tree, counted exactly")
    void transactionsOverUncountedRun(@TempDir Path directory) throws Exception {
        var random = new Random(5);
        var last = POPULATED + 2_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            expected = populate(ledger);
            for (var commit = 0; commit < 8; commit++) {
                medium(ledger, expected, commit, random);
            }
            var current = expected;
            for (var t = 0; t < 30; t++) {
                var message = t % 2 == 0 ? POPULATED + random.nextInt(800) : random.nextInt(POPULATED);
                var remove = t % 3 == 0;
                ledger.update(transaction -> {
                    var found = transaction.get(MESSAGES, key(message));
                    assertEquals(current.containsKey(message), found.isPresent(), "transaction read " + message);
                    assertEquals(current.size(), transaction.size(MESSAGES), "transaction count");
                    if (remove) {
                        transaction.delete(MESSAGES, key(message));
                    } else {
                        transaction.put(MESSAGES, key(message), value(message, 500));
                    }
                });
                if (remove) {
                    expected.remove(message);
                } else {
                    expected.put(message, value(message, 500));
                }
                assertEquals(expected.size(), ledger.size(MESSAGES));
                if (t == 15) {
                    medium(ledger, expected, 8, random);
                }
            }
            verifyPoints(ledger, MESSAGES, expected, last);
            ledger.checkpoint();
            verify(ledger, MESSAGES, expected, last);
        }
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            verify(ledger, MESSAGES, expected, last);
        }
    }

    @ParameterizedTest(name = "encrypted {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("one idle commit makes a counted run over an empty key space and appends to an uncounted one over a populated one, counted exactly through checkpoints")
    void countedAndUncountedInOneCommit(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(9);
        var texts = new TreeMap<Integer, byte[]>();
        var last = POPULATED + 3_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            expected = populate(ledger);
            medium(ledger, expected, 0, random);
            for (var commit = 1; commit < 4; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < LiveLedger.BULK_RUN_WRITES + 10; i++) {
                    put(batch, TEXTS, texts, commit * 10_000 + i, commit);
                }
                for (var i = 0; i < 70; i++) {
                    put(batch, MESSAGES, expected, POPULATED + commit * 100 + i, commit);
                }
                for (var i = 0; i < 30; i++) {
                    put(batch, MESSAGES, expected, random.nextInt(POPULATED), commit);
                }
                ledger.commit(batch);
                var textRun = ledger.state(TEXTS).run;
                assertNotNull(textRun, "no counted run");
                assertTrue(textRun.counted());
                var messageRun = ledger.state(MESSAGES).run;
                assertNotNull(messageRun);
                assertFalse(messageRun.counted());
                verifyPoints(ledger, TEXTS, texts, 40_000);
                verifyPoints(ledger, MESSAGES, expected, last);
            }
            ledger.checkpoint();
            verify(ledger, TEXTS, texts, 40_000);
            verify(ledger, MESSAGES, expected, last);
            for (var commit = 4; commit < 8; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 100; i++) {
                    put(batch, TEXTS, texts, commit * 10_000 + i, commit);
                }
                for (var i = 0; i < 100; i++) {
                    put(batch, MESSAGES, expected, POPULATED + commit * 100 + i, commit);
                }
                ledger.commit(batch);
            }
            verify(ledger, TEXTS, texts, 80_000);
            verify(ledger, MESSAGES, expected, last);
            ledger.checkpoint();
            verify(ledger, TEXTS, texts, 80_000);
            verify(ledger, MESSAGES, expected, last);
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE)) {
            verify(ledger, TEXTS, texts, 80_000);
            verify(ledger, MESSAGES, expected, last);
        }
    }

    @Test
    @DisplayName("a checkpoint that fails over an uncounted run leaves it closed and counted, later idle commits take chains, and the next checkpoint takes the run")
    void failedCheckpointOverUncountedRun() throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName().toString().endsWith(".data")) {
                throw new IOException("No space left on device");
            }
        });
        var directory = disk.getPath("/store");
        var random = new Random(11);
        var last = POPULATED + 2_000;
        TreeMap<Integer, byte[]> expected;
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            expected = populate(ledger);
            for (var commit = 0; commit < 5; commit++) {
                medium(ledger, expected, commit, random);
            }
            var run = ledger.state(MESSAGES).run;
            assertNotNull(run);
            failing.set(true);
            assertThrows(LedgerException.class, ledger::checkpoint);
            failing.set(false);
            assertFalse(run.appendable());
            verifyPoints(ledger, MESSAGES, expected, last);
            for (var commit = 5; commit < 10; commit++) {
                medium(ledger, expected, commit, random);
            }
            verifyPoints(ledger, MESSAGES, expected, last);
            ledger.checkpoint();
            verify(ledger, MESSAGES, expected, last);
            // A run over the new tree once the old one is released
            for (var commit = 10; commit < 13; commit++) {
                medium(ledger, expected, commit, random);
            }
            assertNotNull(ledger.state(MESSAGES).run, "no run after the failed checkpoint's successor");
            verify(ledger, MESSAGES, expected, last);
            ledger.checkpoint();
            verify(ledger, MESSAGES, expected, last);
        }
        try (var ledger = open(directory, true, Long.MAX_VALUE)) {
            verify(ledger, MESSAGES, expected, last);
        }
    }

    @ParameterizedTest(name = "round {0}")
    @ValueSource(ints = {0, 1, 2, 3, 4, 5})
    @DisplayName("medium and bulk commits beside pipelined writers, range removals, transactions, checkpoints and counting snapshots keep counts exact")
    void racingWithRanges(int round, @TempDir Path directory) throws Exception {
        var commits = new java.util.concurrent.ConcurrentHashMap<Long, List<Object[]>>();
        try (var ledger = open(directory, round % 2 == 0, round >= 3 ? 0 : Long.MAX_VALUE)) {
            var populated = populate(ledger);
            commits.put(0L, populated.entrySet().stream().map(e -> new Object[]{MESSAGES, e.getKey(), e.getValue()}).toList());
            var stop = new AtomicBoolean();
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var quiet = new java.util.concurrent.locks.ReentrantReadWriteLock(true);
            var threads = new ArrayList<Thread>();
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(round);
                try {
                    while (!stop.get()) {
                        var batch = ledger.newBatch();
                        var writes = new ArrayList<Object[]>();
                        for (var i = 0; i < 1 + random.nextInt(4); i++) {
                            var keyspace = random.nextInt(3) == 0 ? TEXTS : MESSAGES;
                            var message = random.nextInt(POPULATED + 20_000);
                            if (random.nextInt(3) == 0) {
                                batch.delete(keyspace, key(message));
                                writes.add(new Object[]{keyspace, message, null});
                            } else {
                                var value = value(message, 100 + random.nextInt(50));
                                batch.put(keyspace, key(message), value);
                                writes.add(new Object[]{keyspace, message, value});
                            }
                        }
                        quiet.readLock().lock();
                        try {
                            commits.put(ledger.commit(batch).sequence(), writes);
                        } finally {
                            quiet.readLock().unlock();
                        }
                        Thread.sleep(0, 300_000);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(round + 50);
                try {
                    while (!stop.get()) {
                        quiet.readLock().lock();
                        try {
                            var keyspace = random.nextBoolean() ? TEXTS : MESSAGES;
                            var from = random.nextInt(POPULATED + 18_000);
                            var to = from + 1 + random.nextInt(120);
                            if (random.nextInt(3) == 0) {
                                var message = random.nextInt(POPULATED + 20_000);
                                var value = value(message, 300);
                                var sequence = ledger.update(transaction -> {
                                    transaction.get(keyspace, key(message));
                                    transaction.size(keyspace);
                                    transaction.put(keyspace, key(message), value);
                                }).sequence();
                                commits.put(sequence, List.<Object[]>of(new Object[]{keyspace, message, value}));
                            } else {
                                var batch = ledger.newBatch();
                                var writes = new ArrayList<Object[]>();
                                if (random.nextBoolean()) {
                                    var value = value(from, 200);
                                    batch.put(keyspace, key(from), value);
                                    writes.add(new Object[]{keyspace, from, value});
                                }
                                batch.deleteRange(keyspace, KeyRange.between(key(from), key(to)));
                                writes.add(new Object[]{keyspace, from, to});
                                commits.put(ledger.commit(batch).sequence(), writes);
                            }
                        } finally {
                            quiet.readLock().unlock();
                        }
                        Thread.sleep(2);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    var iteration = 0;
                    while (!stop.get()) {
                        quiet.readLock().lock();
                        try (var snapshot = ledger.snapshot()) {
                            for (var keyspace : List.of(MESSAGES, TEXTS)) {
                                var size = snapshot.size(keyspace);
                                if (++iteration % 7 == 0) {
                                    var scanned = snapshot.forEach(keyspace, KeyRange.all(), (k, v) -> true);
                                    assertEquals(size, scanned, keyspace.name() + ": a snapshot's count and scan disagree");
                                }
                                assertEquals(size, snapshot.size(keyspace), "a snapshot counted twice differently");
                            }
                        } finally {
                            quiet.readLock().unlock();
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            var checkpoints = Thread.ofPlatform().start(() -> {
                try {
                    while (round > 0 && !stop.get()) {
                        quiet.readLock().lock();
                        try {
                            ledger.checkpoint();
                        } finally {
                            quiet.readLock().unlock();
                        }
                        Thread.sleep(4);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            var random = new Random(round + 100);
            for (var commit = 0; commit < 160 && failure.get() == null; commit++) {
                var still = commit % 4 == 3;
                if (still) {
                    quiet.writeLock().lock();
                }
                try {
                    if (still && commit % 16 == 15) {
                        ledger.checkpoint();
                    }
                    var batch = ledger.newBatch();
                    var writes = new ArrayList<Object[]>();
                    var bulkTexts = commit % 20 == 7;
                    for (var i = 0; i < (bulkTexts ? LiveLedger.BULK_RUN_WRITES + 5 : 100); i++) {
                        var keyspace = bulkTexts || commit % 3 == 0 ? TEXTS : MESSAGES;
                        var message = random.nextInt(4) == 0 ? random.nextInt(POPULATED) : POPULATED + (commit * 131 + i) % 18_000;
                        var value = value(message, commit);
                        batch.put(keyspace, key(message), value);
                        writes.add(new Object[]{keyspace, message, value});
                    }
                    for (var i = 0; i < 5; i++) {
                        var message = random.nextInt(POPULATED + 18_000);
                        batch.delete(MESSAGES, key(message));
                        writes.add(new Object[]{MESSAGES, message, null});
                    }
                    commits.put(ledger.commit(batch).sequence(), writes);
                } finally {
                    if (still) {
                        quiet.writeLock().unlock();
                    }
                }
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            checkpoints.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            var messages = new TreeMap<Integer, byte[]>();
            var texts = new TreeMap<Integer, byte[]>();
            for (var sequence : commits.keySet().stream().sorted().toList()) {
                for (var write : commits.get(sequence)) {
                    var target = write[0] == TEXTS ? texts : messages;
                    var message = (Integer) write[1];
                    if (write[2] == null) {
                        target.remove(message);
                    } else if (write[2] instanceof Integer to) {
                        target.subMap(message, to).clear();
                    } else {
                        target.put(message, (byte[]) write[2]);
                    }
                }
            }
            verify(ledger, MESSAGES, messages, POPULATED + 20_000);
            verify(ledger, TEXTS, texts, POPULATED + 20_000);
            ledger.checkpoint();
            verify(ledger, MESSAGES, messages, POPULATED + 20_000);
            verify(ledger, TEXTS, texts, POPULATED + 20_000);
        }
    }
}
