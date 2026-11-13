package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The log's replay of paged key spaces as blind writes, as the pipeline wrote them: an open after a crash looks no
/// replayed key up in the trees, and every count after it stays exact, before and after the first checkpoint, through
/// a second crash before one, with puts and removals of keys the trees hold and lack, large values and range removals
/// over the keys written blind, in their own commit and in a later one.
class ReplayBlindWritesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> IDS = Keyspace.paged("ids", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, IDS, CHATS);
    private static final int SEEDED = 2_000;
    private static final long LARGE_VALUES = 4_096;

    private static LiveLedger open(Path directory) {
        return (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(IDS).keyspace(CHATS).largeValues(LARGE_VALUES).build();
    }

    private static String message(int i) {
        return "m%05d".formatted(i);
    }

    /// Puts [#SEEDED] messages into the paged key spaces and a chat each tenth, then closes with a checkpoint, so the
    /// next open finds them in the trees and none in memory.
    private static TreeMap<String, TreeMap<String, String>> seed(Path directory) throws InterruptedException {
        var model = new TreeMap<String, TreeMap<String, String>>();
        for (var keyspace : KEYSPACES) {
            model.put(keyspace.name(), new TreeMap<>());
        }
        try (var ledger = open(directory)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < SEEDED; i++) {
                put(batch, model, MESSAGES, message(i), "message " + i);
                put(batch, model, IDS, message(i), "id " + i);
                if (i % 10 == 0) {
                    put(batch, model, CHATS, message(i), "chat " + i);
                }
            }
            ledger.commit(batch).awaitDurable();
        }
        return model;
    }

    private static void put(WriteBatch batch, TreeMap<String, TreeMap<String, String>> model,
                            Keyspace<String, String> keyspace, String key, String value) {
        batch.put(keyspace, key, value);
        model.get(keyspace.name()).put(key, value);
    }

    private static void delete(WriteBatch batch, TreeMap<String, TreeMap<String, String>> model,
                               Keyspace<String, String> keyspace, String key) {
        batch.delete(keyspace, key);
        model.get(keyspace.name()).remove(key);
    }

    private static void deleteRange(WriteBatch batch, TreeMap<String, TreeMap<String, String>> model,
                                    Keyspace<String, String> keyspace, String from, String to) {
        batch.deleteRange(keyspace, KeyRange.between(from, to));
        model.get(keyspace.name()).subMap(from, to).clear();
    }

    /// Commits, durably, writes of every kind the replay applies blind, to a store whose trees hold the seeded entries.
    private static void writeLog(LiveLedger ledger, TreeMap<String, TreeMap<String, String>> model) throws InterruptedException {
        var large = "L".repeat((int) LARGE_VALUES * 2);
        var first = ledger.newBatch();
        for (var i = 0; i < 200; i++) {
            // Keys the tree holds, overwritten and removed
            put(first, model, MESSAGES, message(i), "overwritten " + i);
            delete(first, model, IDS, message(200 + i));
            // Keys the tree lacks, put and removed
            put(first, model, MESSAGES, "n%05d".formatted(i), "new " + i);
            delete(first, model, IDS, "absent%05d".formatted(i));
        }
        // Large values, to a key the tree holds and one it lacks
        put(first, model, MESSAGES, message(500), large);
        put(first, model, IDS, "large", large);
        put(first, model, CHATS, message(10), "chat overwritten");
        put(first, model, CHATS, "new chat", "chat");
        ledger.commit(first).awaitDurable();
        // A range removal over keys written blind by the same commit
        var second = ledger.newBatch();
        put(second, model, MESSAGES, message(1_000), "before the removal");
        put(second, model, MESSAGES, "n%05d".formatted(1_000), "before the removal");
        deleteRange(second, model, MESSAGES, message(990), message(1_010));
        deleteRange(second, model, MESSAGES, "n00995", "n01005");
        ledger.commit(second).awaitDurable();
        // A range removal over keys written blind by an earlier commit, the tree's and new ones
        var third = ledger.newBatch();
        deleteRange(third, model, MESSAGES, message(100), message(150));
        deleteRange(third, model, MESSAGES, "n00050", "n00080");
        deleteRange(third, model, IDS, message(150), message(250));
        put(third, model, IDS, message(220), "after the removal");
        ledger.commit(third).awaitDurable();
        // Writes after the removals, of keys they hid
        var fourth = ledger.newBatch();
        put(fourth, model, MESSAGES, message(120), "again");
        delete(fourth, model, MESSAGES, message(130));
        delete(fourth, model, MESSAGES, "n00060");
        put(fourth, model, MESSAGES, "n00070", "again");
        ledger.commit(fourth).awaitDurable();
    }

    private static void assertContents(Ledger ledger, TreeMap<String, TreeMap<String, String>> model) {
        for (var keyspace : KEYSPACES) {
            var entries = new TreeMap<String, String>();
            ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                entries.put(key, value);
                return true;
            });
            assertEquals(model.get(keyspace.name()), entries, keyspace.name());
        }
    }

    private static void assertCounts(Ledger ledger, TreeMap<String, TreeMap<String, String>> model, String when) {
        for (var keyspace : KEYSPACES) {
            assertEquals(model.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " count " + when);
        }
    }

    /// Writes the log over the seeded trees and crashes, as `failure` says, once its commits are durable.
    private static CrashFileSystem crashedWithLog(String failure, TreeMap<String, TreeMap<String, String>> model) throws InterruptedException {
        var disk = CrashFileSystem.create();
        model.putAll(seed(disk.getPath("/store")));
        CrashFileSystem survivor;
        try (var ledger = open(disk.getPath("/store"))) {
            writeLog(ledger, model);
            survivor = crash(disk, failure, 1);
            ledger.closeWithoutCheckpoint();
        }
        return survivor;
    }

    private static CrashFileSystem crash(CrashFileSystem disk, String failure, long seed) {
        return failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(seed));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"process crash", "power loss"})
    @DisplayName("the replay looks up no paged key in the trees and the counts after it are exact, through a second crash and a checkpoint")
    void exactCounts(String failure) throws Exception {
        var model = new TreeMap<String, TreeMap<String, String>>();
        var survivor = crashedWithLog(failure, model);
        CrashFileSystem second;
        try (var ledger = open(survivor.getPath("/store"))) {
            // Every paged write replayed blind, and none settled yet
            assertTrue(ledger.state(MESSAGES).blind.listed() >= 200, "messages written blind: " + ledger.state(MESSAGES).blind.listed());
            assertTrue(ledger.state(IDS).blind.listed() >= 200, "ids written blind: " + ledger.state(IDS).blind.listed());
            assertEquals(0, ledger.state(CHATS).blind.listed(), "a resident key space wrote blind");
            assertContents(ledger, model);
            assertCounts(ledger, model, "before a checkpoint");
            // A second crash before any checkpoint replays the same log again
            second = crash(survivor, failure, 2);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(second.getPath("/store"))) {
            assertCounts(ledger, model, "after a second crash");
            ledger.checkpoint();
            assertCounts(ledger, model, "after the first checkpoint");
            assertContents(ledger, model);
            ledger.commit(ledger.newBatch().put(MESSAGES, message(0), "after recovery").delete(MESSAGES, message(1))
                    .put(MESSAGES, "after", "recovery")).awaitDurable();
            model.get(MESSAGES.name()).put(message(0), "after recovery");
            model.get(MESSAGES.name()).remove(message(1));
            model.get(MESSAGES.name()).put("after", "recovery");
            assertCounts(ledger, model, "after a commit");
        }
        try (var ledger = open(second.getPath("/store"))) {
            assertCounts(ledger, model, "after a clean reopen");
            assertContents(ledger, model);
        }
        CheckpointTest.verifyPages(second.getPath("/store"), true);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"process crash", "power loss"})
    @DisplayName("a checkpoint that is the first to settle the replayed blind writes counts them exactly")
    void checkpointSettles(String failure) throws Exception {
        var model = new TreeMap<String, TreeMap<String, String>>();
        var survivor = crashedWithLog(failure, model);
        try (var ledger = open(survivor.getPath("/store"))) {
            // No count runs before it: the checkpoint settles every blind write from the leaves it merges
            ledger.checkpoint();
            assertCounts(ledger, model, "after the first checkpoint");
            assertContents(ledger, model);
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            assertCounts(ledger, model, "after a clean reopen");
        }
    }

    @Test
    @DisplayName("a replay of paged writes only leaves the page cache without the leaves of their keys")
    void noLeavesRead() throws Exception {
        var disk = CrashFileSystem.create();
        var model = seed(disk.getPath("/store"));
        long cachedAfterOpen;
        CrashFileSystem survivor;
        try (var ledger = open(disk.getPath("/store"))) {
            cachedAfterOpen = ledger.pageCacheBytesForTests();
            var batch = ledger.newBatch();
            // One key of every leaf, and more, of both paged key spaces
            for (var i = 0; i < SEEDED; i += 3) {
                put(batch, model, MESSAGES, message(i), "overwritten " + i);
                delete(batch, model, IDS, message(i));
            }
            ledger.commit(batch).awaitDurable();
            survivor = disk.afterProcessCrash();
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            assertEquals(cachedAfterOpen, ledger.pageCacheBytesForTests(), "the replay read leaves");
            assertCounts(ledger, model, "after the replay");
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"process crash", "power loss"})
    @DisplayName("commits, counts and checkpoints at once over the keys the replay wrote blind keep every count exact")
    void concurrentAfterRecovery(String failure) throws Exception {
        var model = new TreeMap<String, TreeMap<String, String>>();
        var survivor = crashedWithLog(failure, model);
        var writers = 4;
        // Each writer owns the messages whose number it is modulo the writers, so it keeps its own model of them
        var owned = new ArrayList<TreeMap<String, String>>();
        for (var t = 0; t < writers; t++) {
            var mine = new TreeMap<String, String>();
            for (var entry : model.get(MESSAGES.name()).entrySet()) {
                if (owner(entry.getKey(), writers) == t) {
                    mine.put(entry.getKey(), entry.getValue());
                }
            }
            owned.add(mine);
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var stop = new AtomicBoolean();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < writers; t++) {
                var id = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new Random(id);
                        var mine = owned.get(id);
                        for (var commit = 0; commit < 300; commit++) {
                            var number = random.nextInt(SEEDED / writers) * writers + id;
                            var key = random.nextBoolean() ? message(number) : "n%05d".formatted(number);
                            if (random.nextInt(3) == 0) {
                                ledger.commit(ledger.newBatch().delete(MESSAGES, key));
                                mine.remove(key);
                            } else {
                                ledger.commit(ledger.newBatch().put(MESSAGES, key, "w" + commit));
                                mine.put(key, "w" + commit);
                            }
                        }
                    } catch (Throwable failed) {
                        failures.add(failed);
                    }
                }));
            }
            var observer = Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        ledger.size(MESSAGES);
                        ledger.size(IDS);
                        ledger.checkpoint();
                    }
                } catch (Throwable failed) {
                    failures.add(failed);
                }
            });
            for (var thread : threads) {
                thread.join();
            }
            stop.set(true);
            observer.join();
            assertTrue(failures.isEmpty(), () -> "failed: " + failures);
            var messages = new TreeMap<String, String>();
            owned.forEach(messages::putAll);
            model.put(MESSAGES.name(), messages);
            assertCounts(ledger, model, "after the commits");
            ledger.checkpoint();
            assertCounts(ledger, model, "after the last checkpoint");
            assertContents(ledger, model);
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            assertCounts(ledger, model, "after a clean reopen");
        }
    }

    /// {@return the writer of [#concurrentAfterRecovery(String)] that owns `key`, by its number modulo `writers`}
    private static int owner(String key, int writers) {
        var digits = key.replaceAll("\\D", "");
        return digits.isEmpty() ? -1 : Integer.parseInt(digits) % writers;
    }

    static IntStream seeds() {
        return IntStream.range(0, 12);
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    @DisplayName("random puts, removals and range removals replayed blind count exactly, settled by a count or by a checkpoint")
    void randomWorkload(int seed) throws Exception {
        var random = new Random(seed);
        var disk = CrashFileSystem.create();
        var model = seed(disk.getPath("/store"));
        CrashFileSystem survivor;
        try (var ledger = open(disk.getPath("/store"))) {
            for (var commit = 0; commit < 30; commit++) {
                var batch = ledger.newBatch();
                var writes = 1 + random.nextInt(40);
                for (var i = 0; i < writes; i++) {
                    var keyspace = KEYSPACES.get(random.nextInt(KEYSPACES.size()));
                    var key = random.nextBoolean() ? message(random.nextInt(SEEDED + 200)) : "n%05d".formatted(random.nextInt(300));
                    var kind = random.nextInt(20);
                    if (kind == 0) {
                        var to = key.charAt(0) + "%05d".formatted(Integer.parseInt(key.substring(1)) + random.nextInt(60));
                        deleteRange(batch, model, keyspace, key, to);
                    } else if (kind < 7) {
                        delete(batch, model, keyspace, key);
                    } else {
                        var value = random.nextInt(15) == 0 ? "L".repeat(5_000 + random.nextInt(3_000)) : "v" + random.nextInt(1_000);
                        put(batch, model, keyspace, key, value);
                    }
                }
                ledger.commit(batch).awaitDurable();
            }
            survivor = crash(disk, "process crash", seed);
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            if (random.nextBoolean()) {
                assertCounts(ledger, model, "before a checkpoint");
            }
            ledger.checkpoint();
            assertCounts(ledger, model, "after the first checkpoint");
            assertContents(ledger, model);
        }
    }
}
