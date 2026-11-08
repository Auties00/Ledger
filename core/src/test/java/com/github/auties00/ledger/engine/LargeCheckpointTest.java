package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Checkpoints of enough keys that their chains are sorted and resolved on every processor: what they write against a
/// model, beside commits that go on, and what a crash in one leaves.
class LargeCheckpointTest {
    private static final byte[] KEY = new byte[32];
    // Above the dirty chains from which a checkpoint runs on every processor
    private static final int KEYS = 90_000;
    private static final int BATCH = 5_000;
    private static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.utf8());

    /// {@return the key of number `i`, whose first 8 bytes tell it from the others, in no order of the numbers}
    private static String scattered(int i) {
        return "%016x:%d".formatted((i + 1) * 0x9E3779B97F4A7C15L, i);
    }

    /// {@return the key of number `i`, whose first 16 bytes it shares with the keys of its chat}
    private static String chatted(int i) {
        return "393331234567-%03d:%016x".formatted(i % 7, (i + 1) * 0x9E3779B97F4A7C15L);
    }

    static Stream<Arguments> shapes() {
        return Stream.of(
                Arguments.of("scattered keys in a paged key space", PAGED, (IntFunction<String>) LargeCheckpointTest::scattered),
                Arguments.of("keys of chats in a paged key space", PAGED, (IntFunction<String>) LargeCheckpointTest::chatted),
                Arguments.of("scattered keys in a resident key space", RESIDENT, (IntFunction<String>) LargeCheckpointTest::scattered));
    }

    private static Ledger open(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(PAGED).keyspace(RESIDENT)
                .largeValues(4_096).checkpointEvery(1L << 40).build();
    }

    private static void commit(Ledger ledger, Keyspace<String, String> keyspace, TreeMap<String, String> model, List<String> keys, String value) {
        for (var from = 0; from < keys.size(); from += BATCH) {
            var batch = ledger.newBatch();
            for (var key : keys.subList(from, Math.min(keys.size(), from + BATCH))) {
                batch.put(keyspace, key, value);
                model.put(key, value);
            }
            ledger.commit(batch);
        }
    }

    private static void assertHolds(Ledger ledger, Keyspace<String, String> keyspace, TreeMap<String, String> model) {
        assertEquals(model.size(), ledger.size(keyspace));
        var expected = model.entrySet().iterator();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            assertTrue(expected.hasNext(), "more entries than the model holds");
            var entry = expected.next();
            assertEquals(entry.getKey(), key);
            assertEquals(entry.getValue(), value);
            return true;
        });
        assertFalse(expected.hasNext(), "fewer entries than the model holds");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    @DisplayName("hold overwrites, removals, new keys, large values, range removals and rollbacks, through a reopen")
    void mixedWrites(String shape, Keyspace<String, String> keyspace, IntFunction<String> keyOf, @TempDir Path directory) throws Exception {
        var random = new Random(shape.hashCode());
        var model = new TreeMap<String, String>();
        var keys = new ArrayList<String>();
        for (var i = 0; i < KEYS; i++) {
            keys.add(keyOf.apply(i));
        }
        try (var ledger = open(directory)) {
            commit(ledger, keyspace, model, keys, "first");
            ledger.checkpoint();
            assertHolds(ledger, keyspace, model);

            // Each overwrite finds its key only in the tree, in a paged key space: a blind write
            commit(ledger, keyspace, model, keys.subList(0, KEYS - KEYS / 5), "second");
            for (var from = KEYS / 2; from < KEYS / 2 + 10_000; from += BATCH) {
                var batch = ledger.newBatch();
                for (var i = from; i < from + BATCH; i++) {
                    batch.delete(keyspace, keys.get(i));
                    model.remove(keys.get(i));
                }
                ledger.commit(batch);
            }
            var fresh = new ArrayList<String>();
            for (var i = KEYS; i < KEYS + 10_000; i++) {
                fresh.add(keyOf.apply(i));
            }
            commit(ledger, keyspace, model, fresh, "fresh");
            var large = "large".repeat(1_000);
            for (var i = 0; i < 300; i++) {
                var key = keys.get(random.nextInt(KEYS));
                ledger.commit(ledger.newBatch().put(keyspace, key, large));
                model.put(key, large);
            }
            // Rolled back, leaving empty chains for keys of the tree and for new ones; some of them written again after
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                for (var i = 0; i < 2_000; i++) {
                    transaction.put(keyspace, keys.get(i * 7), "rolled back");
                    transaction.put(keyspace, keyOf.apply(KEYS * 2 + i), "rolled back");
                }
                throw new IllegalStateException("abort");
            }));
            var again = new ArrayList<String>();
            for (var i = 0; i < 500; i++) {
                again.add(keyOf.apply(KEYS * 2 + i));
                again.add(keys.get(i * 7));
            }
            commit(ledger, keyspace, model, again, "again");
            var low = keys.get(random.nextInt(KEYS));
            var high = keys.get(random.nextInt(KEYS));
            if (low.compareTo(high) > 0) {
                var swap = low;
                low = high;
                high = swap;
            }
            ledger.commit(ledger.newBatch().deleteRange(keyspace, KeyRange.between(low, high)));
            model.subMap(low, high).clear();
            ledger.checkpoint();
            assertHolds(ledger, keyspace, model);
        }
        CheckpointTest.verifyPages(directory, true);
        try (var ledger = open(directory)) {
            assertHolds(ledger, keyspace, model);
            commit(ledger, keyspace, model, keys.subList(1_000, 2_000), "third");
            ledger.checkpoint();
            assertHolds(ledger, keyspace, model);
        }
    }

    @ParameterizedTest(name = "resident: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("run beside commits on their keys, which later checkpoints and a reopen hold")
    void besideCommits(boolean resident, @TempDir Path directory) throws Exception {
        var keyspace = resident ? RESIDENT : PAGED;
        var writers = 4;
        var models = new ArrayList<TreeMap<String, String>>();
        var keys = new ArrayList<List<String>>();
        for (var writer = 0; writer < writers; writer++) {
            models.add(new TreeMap<>());
            keys.add(new ArrayList<>());
        }
        for (var i = 0; i < KEYS; i++) {
            keys.get(i % writers).add(scattered(i));
        }
        try (var ledger = open(directory)) {
            var failures = new ConcurrentLinkedQueue<Throwable>();
            for (var round = 0; round < 3; round++) {
                // Every key dirty before each checkpoint, so it runs on every processor
                for (var writer = 0; writer < writers; writer++) {
                    commit(ledger, keyspace, models.get(writer), keys.get(writer), "round " + round);
                }
                var start = new CountDownLatch(1);
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < writers; writer++) {
                        var model = models.get(writer);
                        var own = keys.get(writer);
                        var random = new Random(round * 31L + writer);
                        var value = "writer " + writer + " round " + round;
                        executor.execute(() -> {
                            try {
                                start.await();
                                write(ledger, keyspace, model, own, random, value);
                            } catch (Throwable failure) {
                                failures.add(failure);
                            }
                        });
                    }
                    start.countDown();
                    ledger.checkpoint();
                }
                assertTrue(failures.isEmpty(), () -> failures.toString());
                var all = new TreeMap<String, String>();
                models.forEach(all::putAll);
                assertHolds(ledger, keyspace, all);
            }
            ledger.checkpoint();
        }
        CheckpointTest.verifyPages(directory, true);
        var all = new TreeMap<String, String>();
        models.forEach(all::putAll);
        try (var ledger = open(directory)) {
            assertHolds(ledger, keyspace, all);
        }
    }

    private static void write(Ledger ledger, Keyspace<String, String> keyspace, TreeMap<String, String> model, List<String> keys, Random random, String value) {
        for (var step = 0; step < 1_500; step++) {
            var key = keys.get(random.nextInt(keys.size()));
            switch (random.nextInt(8)) {
                case 0 -> {
                    ledger.commit(ledger.newBatch().delete(keyspace, key));
                    model.remove(key);
                }
                case 1 -> assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                    transaction.put(keyspace, key, "rolled back");
                    throw new IllegalStateException("abort");
                }));
                default -> {
                    ledger.commit(ledger.newBatch().put(keyspace, key, value));
                    model.put(key, value);
                }
            }
        }
    }

    @Test
    @DisplayName("that a crash stops at any force leave the commits acknowledged durable, in a store that goes on")
    void crash() throws Exception {
        var dry = CrashFileSystem.create();
        var before = new long[1];
        new CrashWorkload(dry.getPath("/store")).run(() -> before[0] = dry.forces());
        var forces = dry.forces();
        assertTrue(forces > before[0], "the checkpoints force nothing");
        var random = new Random(29);
        for (var stop = before[0] - 2; stop <= forces; stop += Math.max(1, (forces - before[0]) / 8)) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(stop);
            var workload = new CrashWorkload(disk.getPath("/store"));
            workload.run(() -> { });
            var survivor = random.nextBoolean() ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = open(survivor.getPath("/store"))) {
                var recovered = new TreeMap<String, String>();
                ledger.forEach(PAGED, KeyRange.all(), (key, value) -> {
                    recovered.put(key, value);
                    return true;
                });
                workload.assertRecovered(recovered);
                assertEquals(recovered.size(), ledger.size(PAGED));
                ledger.commit(ledger.newBatch().put(PAGED, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
            }
            try (var ledger = open(survivor.getPath("/store"))) {
                assertEquals("recovery", ledger.get(PAGED, "after").orElseThrow());
            }
            CheckpointTest.verifyPages(survivor.getPath("/store"), true);
        }
    }

    /// Fills a store and overwrites every key, a batch at a time, with a large checkpoint after each pass, until it ends
    /// or the file system stops it, and knows which commits it acknowledged durable.
    private static final class CrashWorkload {
        private static final int COMMITS = KEYS / BATCH;
        private final Path directory;
        // The batches of the two passes acknowledged durable and made visible
        private int durable;
        private int visible;

        CrashWorkload(Path directory) {
            this.directory = directory;
        }

        void run(Runnable beforeLastCheckpoint) throws InterruptedException {
            try (var ledger = open(directory)) {
                pass(ledger, "first");
                ledger.checkpoint();
                pass(ledger, "second");
                beforeLastCheckpoint.run();
                ledger.checkpoint();
            } catch (RuntimeException _) {
                // The file system stopped
            }
        }

        private void pass(Ledger ledger, String value) throws InterruptedException {
            for (var commit = 0; commit < COMMITS; commit++) {
                var batch = ledger.newBatch();
                for (var i = commit * BATCH; i < (commit + 1) * BATCH; i++) {
                    batch.put(PAGED, scattered(i), value);
                }
                var committed = ledger.commit(batch);
                visible++;
                committed.awaitDurable();
                durable = visible;
            }
        }

        /// Checks that `recovered` is the state after one commit between the last one durable and the last one visible.
        void assertRecovered(Map<String, String> recovered) {
            var second = 0;
            for (var value : recovered.values()) {
                if (value.equals("second")) {
                    second++;
                }
            }
            var commits = recovered.size() < KEYS ? recovered.size() / BATCH : COMMITS + second / BATCH;
            assertTrue(commits >= durable && commits <= visible,
                    () -> "the store holds " + recovered.size() + " keys, where commits " + durable + " to " + visible + " were at risk");
            for (var i = 0; i < KEYS; i++) {
                var expected = commits <= COMMITS
                        ? (i < commits * BATCH ? "first" : null)
                        : (i < (commits - COMMITS) * BATCH ? "second" : "first");
                assertEquals(expected, recovered.get(scattered(i)), "key " + i);
            }
        }
    }

    @Test
    @DisplayName("resolving on every processor gives what the rules say, in key order, a state per key")
    void resolvesEachKeyOnce() {
        var state = new KeyspaceState(PAGED);
        var bound = 1_000L;
        var chains = new ArrayList<Chain>();
        var expected = new TreeMap<String, Version>();
        var external = new HashSet<PageRef>();
        var random = new Random(3);
        for (var i = 0; i < KEYS; i++) {
            var name = scattered(i);
            var key = Segments.encodeKey(KeyCodec.utf8(), name);
            var kind = random.nextInt(16);
            var chain = state.open(key);
            chains.add(chain);
            switch (kind) {
                // Past the cut only: a paged key space leaves the key to the tree
                case 0 -> chain.attach(new Version(bound + 1, value(), null, null));
                // Dropped: the state is that of the chain that replaced it, listed too, and only once
                case 1 -> {
                    assertTrue(state.dropRolledBack(chain, 0));
                    var replacement = state.open(key);
                    var version = new Version(bound, value(), null, null);
                    replacement.attach(version);
                    chains.add(replacement);
                    expected.put(name, version);
                }
                // Dropped, with nothing that replaced it: a rolled-back write
                case 2 -> assertTrue(state.dropRolledBack(chain, 0));
                case 3 -> {
                    var unit = new PageRef(i + 1, 1, 0, 0);
                    var version = Version.ofOwn(bound - 1, new byte[8], null, null, unit);
                    chain.attach(version);
                    external.add(unit);
                    expected.put(name, version);
                }
                case 4 -> {
                    chain.attach(new Version(bound - 1, (MemorySegment) null, null, null));
                    expected.put(name, null);
                }
                default -> {
                    var older = new Version(bound - 1, value(), null, null);
                    chain.attach(new Version(bound + 5, value(), null, older));
                    expected.put(name, older);
                }
            }
        }
        var sorted = chains.toArray(Chain[]::new);
        var words = ChainSort.sort(sorted);
        var adopted = new HashSet<PageRef>();
        var ops = Checkpointer.resolve(sorted, words, state, RangeRemoval.NONE, bound, adopted);
        assertEquals(expected.size(), ops.size());
        assertEquals(external, adopted);
        var index = 0;
        for (var entry : expected.entrySet()) {
            var op = ops.get(index);
            var key = op.key();
            assertArrayEquals(Segments.toBytes(Segments.encodeKey(KeyCodec.utf8(), entry.getKey())), Segments.toBytes(key));
            assertEquals(0, ops.compare(index, Segments.word(key, 0, key.byteSize()), key));
            if (entry.getValue() == null) {
                assertNull(op.version(), entry.getKey());
            } else {
                assertSame(entry.getValue(), op.version(), entry.getKey());
            }
            index++;
        }
    }

    @Test
    @DisplayName("a failure on a worker that resolves chains reaches the caller")
    void workerFailure() {
        var state = new KeyspaceState(PAGED);
        var chains = new Chain[KEYS];
        for (var i = 0; i < KEYS; i++) {
            chains[i] = state.open(Segments.encodeKey(KeyCodec.utf8(), scattered(i)));
        }
        var words = ChainSort.sort(chains);
        chains[KEYS / 2] = null;
        assertThrows(NullPointerException.class, () -> Checkpointer.resolve(chains, words, state, RangeRemoval.NONE, 0, new HashSet<>()));
    }

    private static MemorySegment value() {
        return MemorySegment.ofArray(new byte[8]).asReadOnly();
    }
}
