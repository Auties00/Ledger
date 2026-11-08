package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Range removals of a batch counted after their commit ([PendingRanges]): the counts every kind of reader sees before
/// and after the count, with the counts held back, delayed, blocked or failing, across checkpoints, closes, crashes and
/// concurrent commits. A store's background counts are held back by giving it an executor that never runs them, so a
/// count of the key space, a transaction or a checkpoint is the first to need each one.
class PendingRangesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> IDS = Keyspace.paged("ids", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    // A resident key space whose decoder refuses a value starting with 0x7F, to fail a batch after its range removal
    private static final Keyspace<String, String> STRICT = Keyspace.resident("strict", KeyCodec.utf8(), ValueCodec.of(
            value -> value.getBytes(StandardCharsets.UTF_8).length,
            (value, target) -> MemorySegment.copy(MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)), 0, target, 0, target.byteSize()),
            source -> {
                if (source.byteSize() > 0 && source.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0) == 0x7F) {
                    throw new IllegalArgumentException("refused");
                }
                return new String(source.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
            }));
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, IDS, CHATS);
    private static final Executor HELD = _ -> {
    };

    /// How the background counts of a store run.
    enum Counts {
        HELD_BACK, VIRTUAL, DELAYED;

        Executor executor() {
            return switch (this) {
                case HELD_BACK -> HELD;
                case VIRTUAL -> null;
                case DELAYED -> task -> Thread.ofVirtual().start(() -> {
                    LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(2_000_000));
                    task.run();
                });
            };
        }
    }

    private static LiveLedger open(Path directory, Executor counts) {
        var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(IDS).keyspace(CHATS).keyspace(STRICT).build();
        if (counts != null) {
            ledger.rangeCountsForTests(counts);
        }
        return ledger;
    }

    private static String message(int i) {
        return "m%05d".formatted(i);
    }

    /// Puts `count` messages into the two paged key spaces and a chat each tenth, checkpoints and closes, so the next
    /// open finds every entry in the trees and none in memory or in the page cache.
    private static void seed(Path directory, int count) {
        try (var ledger = open(directory, null)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < count; i++) {
                batch.put(MESSAGES, message(i), "message " + i).put(IDS, message(i), "id " + i);
                if (i % 10 == 0) {
                    batch.put(CHATS, message(i), "chat " + i);
                }
            }
            ledger.commit(batch);
        }
    }

    private static int pending(LiveLedger ledger, Keyspace<?, ?> keyspace) {
        return ledger.state(keyspace).ranges.listed();
    }

    private static long scanned(com.github.auties00.ledger.View view, Keyspace<String, String> keyspace) {
        return view.forEach(keyspace, KeyRange.all(), (_, _) -> true);
    }

    @Test
    @DisplayName("a batch's removal of a paged key space with a tree publishes without its count, which each kind of count then takes in")
    void everyCountTakesItIn(@TempDir Path directory) {
        seed(directory, 3_000);
        try (var ledger = open(directory, HELD)) {
            var before = ledger.snapshot();
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(100), message(600))));
            assertEquals(1, pending(ledger, MESSAGES));
            // A transaction's count is the first to need it
            var counted = new long[1];
            ledger.update(transaction -> counted[0] = transaction.size(MESSAGES));
            assertEquals(2_500, counted[0]);

            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(1_000), message(1_100))));
            // A snapshot's, then the store's
            try (var after = ledger.snapshot()) {
                assertEquals(2_400, after.size(MESSAGES));
            }
            assertEquals(2_400, ledger.size(MESSAGES));

            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.atLeast(message(2_900))));
            assertEquals(2_300, ledger.size(MESSAGES));

            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.below(message(50))));
            // The checkpoint's
            ledger.checkpoint();
            assertEquals(2_250, ledger.size(MESSAGES));
            assertEquals(2_250, scanned(ledger, MESSAGES));

            // A snapshot from before every removal counts none of them
            assertEquals(3_000, before.size(MESSAGES));
            assertEquals(3_000, scanned(before, MESSAGES));
            before.close();
            // The next maintenance folds the counted removals, whose reads have gone
            ledger.update(transaction -> transaction.put(IDS, "after", "x"));
            assertEquals(0, pending(ledger, MESSAGES));
            assertTrue(ledger.readers().idle(), "a counted removal kept its read");
            assertEquals(3_001, ledger.size(IDS));
            assertEquals(300, ledger.size(CHATS));
        }
        try (var ledger = open(directory, HELD)) {
            assertEquals(2_250, ledger.size(MESSAGES));
            assertEquals(2_250, scanned(ledger, MESSAGES));
        }
    }

    @Test
    @DisplayName("a removal of a resident key space, or of one without a tree, counts as it applies")
    void countsAtOnceWithoutPages(@TempDir Path directory) {
        try (var ledger = open(directory, HELD)) {
            ledger.commit(ledger.newBatch().put(MESSAGES, message(1), "a").put(MESSAGES, message(2), "b"));
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.all()));
            assertEquals(0, pending(ledger, MESSAGES));
            assertEquals(0, ledger.size(MESSAGES));
        }
        seed(directory, 200);
        try (var ledger = open(directory, HELD)) {
            ledger.commit(ledger.newBatch().deleteRange(CHATS, KeyRange.below(message(100))));
            assertEquals(0, pending(ledger, CHATS));
            assertEquals(10, ledger.size(CHATS));
            assertTrue(ledger.readers().idle());
        }
    }

    @ParameterizedTest(name = "overlapping: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("two removals of one key space in one batch each count what the other left")
    void twoInOneBatch(boolean overlapping, @TempDir Path directory) {
        seed(directory, 2_000);
        var model = new TreeMap<String, String>();
        for (var i = 0; i < 2_000; i++) {
            model.put(message(i), "message " + i);
        }
        var first = KeyRange.between(message(100), message(800));
        var second = overlapping ? KeyRange.between(message(500), message(1_200)) : KeyRange.between(message(1_500), message(1_700));
        model.subMap(message(100), message(800)).clear();
        if (overlapping) {
            model.subMap(message(500), message(1_200)).clear();
        } else {
            model.subMap(message(1_500), message(1_700)).clear();
        }
        try (var ledger = open(directory, HELD)) {
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, first).deleteRange(IDS, first).deleteRange(MESSAGES, second));
            assertEquals(2, pending(ledger, MESSAGES));
            assertEquals(1, pending(ledger, IDS));
            assertEquals(model.size(), ledger.size(MESSAGES));
            assertEquals(1_300, ledger.size(IDS));
            ledger.checkpoint();
            assertEquals(model.size(), ledger.size(MESSAGES));
            assertEquals(model.size(), scanned(ledger, MESSAGES));
        }
        try (var ledger = open(directory, HELD)) {
            assertEquals(model.size(), ledger.size(MESSAGES));
            assertEquals(1_300, ledger.size(IDS));
        }
    }

    @ParameterizedTest
    @EnumSource(Counts.class)
    @DisplayName("puts and removals beside a removal in its batch, or in the batches after, leave exact counts")
    void writesBesideRemovals(Counts counts, @TempDir Path directory) {
        seed(directory, 1_000);
        var model = new TreeMap<String, String>();
        for (var i = 0; i < 1_000; i++) {
            model.put(message(i), "message " + i);
        }
        try (var ledger = open(directory, counts.executor())) {
            // A put before the removal in its batch: the removal counts at once, the put included
            ledger.commit(ledger.newBatch().put(MESSAGES, message(150), "new").put(MESSAGES, "m00150x", "new")
                    .deleteRange(MESSAGES, KeyRange.between(message(100), message(200))));
            model.subMap(message(100), message(200)).clear();
            // A later write of a key the batch wrote: its prune drops the version the removal saw
            ledger.commit(ledger.newBatch().put(MESSAGES, "m00150x", "again"));
            model.put("m00150x", "again");
            assertEquals(model.size(), ledger.size(MESSAGES));
            // A put and a removal after the removal in its batch, into its range: the removal counts later
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(300), message(400)))
                    .put(MESSAGES, message(350), "back").put(MESSAGES, "m00350x", "added").delete(MESSAGES, message(399))
                    .delete(MESSAGES, message(450)));
            model.subMap(message(300), message(400)).clear();
            model.put(message(350), "back");
            model.put("m00350x", "added");
            model.remove(message(450));
            assertEquals(model.size(), ledger.size(MESSAGES));
            // A removal, then batches that write into its range before its count runs
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(500), message(700))));
            model.subMap(message(500), message(700)).clear();
            ledger.commit(ledger.newBatch().put(MESSAGES, message(600), "again").put(MESSAGES, message(800), "over"));
            model.put(message(600), "again");
            model.put(message(800), "over");
            ledger.commit(ledger.newBatch().delete(MESSAGES, message(600)).delete(MESSAGES, message(900)));
            model.remove(message(600));
            model.remove(message(900));
            assertEquals(model.size(), ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(model.size(), ledger.size(MESSAGES));
            assertEquals(model.size(), scanned(ledger, MESSAGES));
        }
        try (var ledger = open(directory, counts.executor())) {
            assertEquals(model.size(), ledger.size(MESSAGES));
            assertEquals(model.size(), scanned(ledger, MESSAGES));
        }
    }

    @ParameterizedTest
    @EnumSource(Counts.class)
    @DisplayName("random batches of removals, puts and deletes beside snapshots, transactions, checkpoints, closes and crashes keep every count exact")
    void randomWorkload(Counts counts, @TempDir Path directory) {
        var random = new Random(counts.ordinal() * 31L + 7);
        var models = new TreeMap<String, TreeMap<String, String>>();
        for (var keyspace : KEYSPACES) {
            models.put(keyspace.name(), new TreeMap<>());
        }
        var ledger = open(directory, counts.executor());
        try {
            for (var step = 0; step < 400; step++) {
                var choice = random.nextInt(40);
                if (choice == 0) {
                    ledger.checkpoint();
                } else if (choice == 1) {
                    ledger.close();
                    ledger = open(directory, counts.executor());
                } else if (choice == 2) {
                    ledger.closeWithoutCheckpoint();
                    ledger = open(directory, counts.executor());
                } else if (choice < 6) {
                    // A snapshot from before a removal counts without it, one after with it
                    var before = ledger.snapshot();
                    var expected = models.get(MESSAGES.name()).size();
                    var range = range(random);
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, range));
                    subMap(models.get(MESSAGES.name()), range).clear();
                    try (var after = ledger.snapshot()) {
                        assertEquals(models.get(MESSAGES.name()).size(), after.size(MESSAGES));
                    }
                    assertEquals(expected, before.size(MESSAGES));
                    assertEquals(expected, scanned(before, MESSAGES));
                    before.close();
                } else if (choice < 8) {
                    var key = key(random);
                    var sizes = new long[1];
                    ledger.update(transaction -> {
                        transaction.put(IDS, key, "t");
                        sizes[0] = transaction.size(MESSAGES);
                    });
                    models.get(IDS.name()).put(key, "t");
                    assertEquals(models.get(MESSAGES.name()).size(), sizes[0]);
                } else {
                    batch(ledger, models, random);
                }
                for (var keyspace : KEYSPACES) {
                    assertEquals(models.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " at step " + step);
                }
            }
            for (var keyspace : KEYSPACES) {
                assertEquals(models.get(keyspace.name()).size(), scanned(ledger, keyspace));
            }
            ledger.checkpoint();
        } finally {
            ledger.close();
        }
        try (var reopened = open(directory, counts.executor())) {
            for (var keyspace : KEYSPACES) {
                assertEquals(models.get(keyspace.name()).size(), reopened.size(keyspace));
                assertEquals(models.get(keyspace.name()).size(), scanned(reopened, keyspace));
            }
        }
    }

    private static String key(Random random) {
        return "k%03d".formatted(random.nextInt(400));
    }

    private static KeyRange<String> range(Random random) {
        var a = key(random);
        var b = key(random);
        var from = a.compareTo(b) <= 0 ? a : b;
        var to = from.equals(a) ? b : a;
        return switch (random.nextInt(6)) {
            case 0 -> KeyRange.atLeast(from);
            case 1 -> KeyRange.below(to);
            default -> KeyRange.between(from, to);
        };
    }

    private static NavigableMap<String, String> subMap(TreeMap<String, String> model, KeyRange<String> range) {
        var from = range.from().orElse(null);
        var to = range.to().orElse(null);
        if (from != null && to != null) {
            return from.compareTo(to) >= 0 ? new TreeMap<>() : model.subMap(from, true, to, false);
        }
        return from != null ? model.tailMap(from, true) : to != null ? model.headMap(to, false) : model;
    }

    private static void batch(Ledger ledger, TreeMap<String, TreeMap<String, String>> models, Random random) {
        var batch = ledger.newBatch();
        var writes = 1 + random.nextInt(random.nextInt(4) == 0 ? 200 : 8);
        for (var i = 0; i < writes; i++) {
            var keyspace = KEYSPACES.get(random.nextInt(KEYSPACES.size()));
            var model = models.get(keyspace.name());
            var kind = random.nextInt(writes > 20 ? 60 : 8);
            if (kind == 0) {
                var range = range(random);
                batch.deleteRange(keyspace, range);
                subMap(model, range).clear();
            } else if (kind < 3) {
                var key = key(random);
                batch.delete(keyspace, key);
                model.remove(key);
            } else {
                var key = key(random);
                var value = "v" + random.nextInt(1_000);
                batch.put(keyspace, key, value);
                model.put(key, value);
            }
        }
        ledger.commit(batch);
    }

    @Test
    @DisplayName("a checkpoint that comes before the removal's own count counts it, and the count that comes later finds it counted")
    void checkpointBeforeCount(@TempDir Path directory) throws Exception {
        seed(directory, 3_000);
        var tasks = new ConcurrentLinkedQueue<Runnable>();
        try (var ledger = open(directory, tasks::add)) {
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(0), message(1_000)))
                    .deleteRange(IDS, KeyRange.between(message(0), message(1_000)))).awaitDurable();
            assertEquals(2, tasks.size());
            var messages = firstPending(ledger, MESSAGES);
            var ids = firstPending(ledger, IDS);
            // Overwrites that make the next checkpoints rewrite and free the pages of the trees the removals count in
            for (var round = 0; round < 3; round++) {
                var batch = ledger.newBatch();
                for (var i = 1_000; i < 3_000; i += 3) {
                    batch.put(MESSAGES, message(i), "round " + round).put(IDS, message(i), "round " + round);
                }
                ledger.commit(batch);
                ledger.checkpoint();
                // The first checkpoint counted them
                assertTrue(messages.settled() && ids.settled());
            }
            assertEquals(2_000, ledger.size(MESSAGES));
            assertEquals(2_000, ledger.size(IDS));
            // The held-back counts run now, against a store that moved on: they find their removals counted
            for (var task : tasks) {
                task.run();
            }
            assertEquals(2_000, ledger.size(MESSAGES));
            assertTrue(ledger.readers().idle(), "a counted removal kept its read");
        }
        CheckpointTest.verifyPages(directory, true);
        try (var ledger = open(directory, HELD)) {
            assertEquals(2_000, ledger.size(MESSAGES));
            assertEquals(2_000, scanned(ledger, IDS));
        }
    }

    @ParameterizedTest(name = "final checkpoint: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a close with a count never run leaves a store that reopens with the exact count")
    void closeWithPendingCount(boolean checkpoint, @TempDir Path directory) throws Exception {
        seed(directory, 2_000);
        try (var ledger = open(directory, HELD)) {
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(10), message(1_510)))).awaitDurable();
            if (checkpoint) {
                ledger.close();
            } else {
                ledger.closeWithoutCheckpoint();
            }
        }
        CheckpointTest.verifyPages(directory, true);
        try (var ledger = open(directory, HELD)) {
            assertEquals(500, ledger.size(MESSAGES));
            assertEquals(500, scanned(ledger, MESSAGES));
            assertEquals(2_000, ledger.size(IDS));
        }
    }

    @Test
    @DisplayName("a close waits for a count running on its own thread before it closes the files the count reads")
    void closeWaitsForRunningCount(@TempDir Path directory) throws Exception {
        seed(directory, 2_000);
        var tasks = new ConcurrentLinkedQueue<Runnable>();
        var ledger = open(directory, tasks::add);
        ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(0), message(700)))).awaitDurable();
        var removal = firstPending(ledger, MESSAGES);
        var counting = counting(removal);
        // Holding the removal's lock, so its count waits inside, once it started
        counting.lock();
        var counter = Thread.ofVirtual().start(tasks.poll());
        Thread closer;
        var closeFailure = new AtomicBoolean();
        try {
            while (!counting.hasQueuedThreads()) {
                Thread.onSpinWait();
            }
            closer = Thread.ofPlatform().start(() -> {
                try {
                    ledger.closeWithoutCheckpoint();
                } catch (RuntimeException failure) {
                    closeFailure.set(true);
                }
            });
            closer.join(300);
            assertTrue(closer.isAlive(), "the close did not wait for the count");
        } finally {
            counting.unlock();
        }
        closer.join();
        counter.join();
        assertFalse(closeFailure.get());
        // The count ran with the files open
        assertTrue(removal.settled());
        try (var reopened = open(directory, HELD)) {
            assertEquals(1_300, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("a count that starts after the close began finds the store closed and reads nothing")
    void countAfterClose(@TempDir Path directory) {
        seed(directory, 500);
        var tasks = new ConcurrentLinkedQueue<Runnable>();
        var ledger = open(directory, tasks::add);
        ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(0), message(100))));
        var removal = firstPending(ledger, MESSAGES);
        ledger.closeWithoutCheckpoint();
        tasks.poll().run();
        assertFalse(removal.settled());
        try (var reopened = open(directory, HELD)) {
            assertEquals(400, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("a page the count reads that fails authentication fails the counts and the checkpoint, not the commit, until it reads again")
    void failedCountIsRetried(@TempDir Path directory) throws Exception {
        seed(directory, 4_000);
        var file = directory.resolve(PageFile.FILE);
        try (var ledger = open(directory, null)) {
            var original = Files.readAllBytes(file);
            var damaged = original.clone();
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < original.length / PageFormat.SIZE; page++) {
                damaged[page * PageFormat.SIZE + PageFormat.HEADER + 5] ^= 1;
            }
            write(file, damaged);
            ledger.clearPageCacheForTests();
            // The removal reads no page as it commits
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(500), message(1_500)))).awaitDurable();
            assertThrows(LedgerCorruptedException.class, () -> ledger.size(MESSAGES));
            assertThrows(LedgerCorruptedException.class, ledger::checkpoint);
            // The failure is not kept: the removal holds its read for the next count
            assertThrows(LedgerCorruptedException.class, () -> ledger.size(MESSAGES));
            assertFalse(ledger.readers().idle(), "a removal whose count failed let its read go");
            write(file, original);
            ledger.clearPageCacheForTests();
            assertEquals(3_000, ledger.size(MESSAGES));
            assertTrue(ledger.readers().idle(), "a counted removal kept its read");
            ledger.checkpoint();
            assertEquals(3_000, scanned(ledger, MESSAGES));
        }
        CheckpointTest.verifyPages(directory, true);
        try (var ledger = open(directory, HELD)) {
            assertEquals(3_000, ledger.size(MESSAGES));
        }
    }

    private static void write(Path file, byte[] bytes) throws Exception {
        try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            var buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer, buffer.position());
            }
            channel.force(false);
        }
    }

    @Test
    @DisplayName("a batch taken back after its removal lets the removal's read go and counts nothing")
    void rollbackReleasesTheRead(@TempDir Path directory) {
        seed(directory, 1_000);
        try (var ledger = open(directory, HELD)) {
            var batch = ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(0), message(500)))
                    .putEncoded(STRICT, MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8)), MemorySegment.ofArray(new byte[]{0x7F}));
            assertThrows(IllegalArgumentException.class, () -> ledger.commit(batch));
            assertTrue(ledger.readers().idle(), "the batch taken back kept its removal's read");
            assertEquals(0, pending(ledger, MESSAGES));
            assertEquals(1_000, ledger.size(MESSAGES));
            assertEquals(1_000, scanned(ledger, MESSAGES));
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(0), message(500))));
            assertEquals(500, ledger.size(MESSAGES));
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"process crash", "power loss"})
    @DisplayName("a crash before the count ran recovers the removal and counts it exactly")
    void crashBeforeCount(String failure) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        seed(directory, 2_000);
        CrashFileSystem survivor;
        try (var ledger = open(directory, HELD)) {
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(100), message(900)))
                    .deleteRange(IDS, KeyRange.atLeast(message(1_000)))).awaitDurable();
            // Not waited for: a loss of power may drop it, a crash of the process keeps it
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.below(message(50))));
            survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(3));
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(survivor.getPath("/store"), HELD)) {
            // The removals waited for are recovered, the other may be
            var messages = scanned(ledger, MESSAGES);
            assertTrue(messages == 1_200 || (messages == 1_150 && ledger.get(MESSAGES, message(0)).isEmpty()), "recovered " + messages);
            assertTrue(ledger.get(MESSAGES, message(500)).isEmpty());
            assertEquals(messages, ledger.size(MESSAGES));
            assertEquals(1_000, ledger.size(IDS));
            ledger.checkpoint();
            assertEquals(messages, ledger.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("pipelined commits, removals counted later, snapshots, transactions and checkpoints at once keep every count exact")
    void stress(@TempDir Path directory) throws Exception {
        seed(directory, 1_000);
        try (var ledger = open(directory, null)) {
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<String>();
            var removals = new AtomicLong();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 12; t++) {
                var id = t;
                Runnable task = () -> {
                    var random = new Random(id);
                    try {
                        while (!stop.get()) {
                            var choice = random.nextInt(100);
                            if (choice < 6) {
                                var from = random.nextInt(1_100);
                                ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(from), message(from + random.nextInt(60)))));
                                removals.incrementAndGet();
                            } else if (choice < 10) {
                                try (var snapshot = ledger.snapshot()) {
                                    var seen = scanned(snapshot, MESSAGES);
                                    var counted = snapshot.size(MESSAGES);
                                    if (seen != counted) {
                                        failures.add("snapshot read " + seen + " entries where it counts " + counted);
                                    }
                                }
                            } else if (choice < 12) {
                                ledger.update(transaction -> {
                                    var seen = scanned(transaction, MESSAGES);
                                    var counted = transaction.size(MESSAGES);
                                    if (seen != counted) {
                                        failures.add("transaction read " + seen + " entries where it counts " + counted);
                                    }
                                });
                            } else {
                                var batch = ledger.newBatch();
                                for (var i = 0; i < 1 + random.nextInt(4); i++) {
                                    var key = message(random.nextInt(1_100));
                                    if (random.nextInt(3) == 0) {
                                        batch.delete(MESSAGES, key);
                                    } else {
                                        batch.put(MESSAGES, key, "v" + id);
                                    }
                                }
                                ledger.commit(batch);
                            }
                        }
                    } catch (RuntimeException | Error failure) {
                        if (failures.isEmpty()) {
                            failure.printStackTrace();
                        }
                        failures.add(failure.toString());
                    }
                };
                threads.add(id < 4 ? Thread.ofPlatform().start(task) : Thread.ofVirtual().start(task));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        ledger.checkpoint();
                        Thread.sleep(20);
                    }
                } catch (RuntimeException | Error | InterruptedException failure) {
                    failures.add("checkpoint: " + failure);
                }
            }));
            Thread.sleep(3_000);
            stop.set(true);
            for (var thread : threads) {
                thread.join(TimeUnit.SECONDS.toMillis(60));
                assertFalse(thread.isAlive(), "a thread did not finish");
            }
            assertTrue(failures.isEmpty(), () -> failures.size() + " failures, first: " + failures.peek());
            assertTrue(removals.get() > 20, () -> removals + " removals");
            assertEquals(scanned(ledger, MESSAGES), ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(scanned(ledger, MESSAGES), ledger.size(MESSAGES));
        }
        try (var ledger = open(directory, null)) {
            assertEquals(scanned(ledger, MESSAGES), ledger.size(MESSAGES));
        }
    }

    /// {@return the first removal of `keyspace` whose count is listed}
    private static PendingRanges.Removal firstPending(LiveLedger ledger, Keyspace<?, ?> keyspace) {
        try {
            var runField = PendingRanges.class.getDeclaredField("run");
            runField.setAccessible(true);
            var run = runField.get(ledger.state(keyspace).ranges);
            var removals = run.getClass().getDeclaredField("removals");
            removals.setAccessible(true);
            var start = run.getClass().getDeclaredField("start");
            start.setAccessible(true);
            return ((PendingRanges.Removal[]) removals.get(run))[start.getInt(run)];
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    private static ReentrantLock counting(PendingRanges.Removal removal) {
        try {
            var field = PendingRanges.Removal.class.getDeclaredField("counting");
            field.setAccessible(true);
            return (ReentrantLock) field.get(removal);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    @Test
    @DisplayName("a count held back past commits that write into its range counts what the range held at its commit")
    void heldCountIgnoresLaterWrites(@TempDir Path directory) {
        seed(directory, 3_000);
        var tasks = new ConcurrentLinkedQueue<Runnable>();
        try (var ledger = open(directory, tasks::add)) {
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(message(200), message(2_200))));
            for (var round = 0; round < 3; round++) {
                var batch = ledger.newBatch();
                for (var i = 200; i < 2_200; i += 2) {
                    batch.put(MESSAGES, message(i), "round " + round);
                }
                batch.delete(MESSAGES, message(2_500 + round));
                ledger.commit(batch);
            }
            assertEquals(1, tasks.size());
            // Counted here: 2,000 removed, 1,000 written back, 3 deleted
            assertEquals(1_997, ledger.size(MESSAGES));
            assertEquals(1_997, scanned(ledger, MESSAGES));
            tasks.poll().run();
            ledger.update(transaction -> transaction.put(IDS, "x", "y"));
            assertTrue(ledger.readers().idle(), "a counted removal kept its read");
            assertEquals(0, pending(ledger, MESSAGES));
            ledger.checkpoint();
            assertEquals(1_997, ledger.size(MESSAGES));
        }
        try (var ledger = open(directory, HELD)) {
            assertEquals(1_997, ledger.size(MESSAGES));
        }
    }
}
