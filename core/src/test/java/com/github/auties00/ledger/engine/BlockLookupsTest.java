package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Commits whose chains are looked up a block at a time: keys a batch writes more than once, key spaces mixed in one
/// block, chains dropped between a block's lookup and the opening of its chains, and batches of every size beside
/// checkpoints, commits on other threads and reopens, against a model of the entries.
class BlockLookupsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.utf8());
    // Sizes on both sides of the block threshold, of a block, of the bound of the touched heads, and many blocks
    private static final int[] SIZES = {1, 2, 7, 8, 9, 15, 16, 17, 31, 33, 100, 255, 256, 257, 300, 700};

    private static Ledger directory(Path directory, long checkpointEvery) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(PAGED).keyspace(RESIDENT)
                .checkpointEvery(checkpointEvery).build();
    }

    private static Ledger memory() {
        return Ledger.builder().memory().keyspace(PAGED).keyspace(RESIDENT).build();
    }

    private static MemorySegment key(String key) {
        return MemorySegment.ofArray(key.getBytes(StandardCharsets.UTF_8));
    }

    // One model per key space: the value of each key, absent once removed
    private static void verify(Ledger ledger, Map<String, String> paged, Map<String, String> resident, int keys) {
        for (var entry : List.of(Map.entry(PAGED, paged), Map.entry(RESIDENT, resident))) {
            var model = entry.getValue();
            for (var i = 0; i < keys; i++) {
                var name = "key" + i;
                assertEquals(Optional.ofNullable(model.get(name)), ledger.get(entry.getKey(), name), entry.getKey().name() + " " + name);
            }
            assertEquals(model.size(), ledger.size(entry.getKey()), "the size of " + entry.getKey().name());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("a batch that puts, removes and puts again one key across its blocks, in both key spaces, leaves the last write and an exact count")
    void repeatedKeys(boolean inDirectory, @TempDir Path directory) {
        try (var ledger = inDirectory ? directory(directory, 1L << 30) : memory()) {
            var paged = new HashMap<String, String>();
            var resident = new HashMap<String, String>();
            for (var round = 0; round < 3; round++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 40; i++) {
                    // key0 at the start, the end and across the boundary of the first block; key1 in every write of
                    // the second block, alternating key spaces
                    var keyspace = i % 2 == 0 ? PAGED : RESIDENT;
                    var model = keyspace == PAGED ? paged : resident;
                    var name = i % 3 == 0 ? "key0" : (i >= 16 && i < 32 ? "key1" : "key" + (2 + i));
                    if (i % 7 == 6) {
                        batch.delete(keyspace, name);
                        model.remove(name);
                    } else {
                        var value = round + ":" + i;
                        batch.put(keyspace, name, value);
                        model.put(name, value);
                    }
                }
                ledger.commit(batch);
                verify(ledger, paged, resident, 50);
                if (inDirectory) {
                    ledger.checkpoint();
                    verify(ledger, paged, resident, 50);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("a batch of many writes of one new key copies the key into the slab once")
    void oneKeyCopy(boolean inDirectory, @TempDir Path directory) {
        try (var ledger = (LiveLedger) (inDirectory ? directory(directory, 1L << 30) : memory())) {
            for (var keyspace : List.of(PAGED, RESIDENT)) {
                var state = ledger.state(keyspace);
                var before = state.keyBytesForTests();
                var batch = ledger.newBatch();
                for (var i = 0; i < 40; i++) {
                    batch.put(keyspace, "a-key-of-16-byte", "value" + i);
                }
                ledger.commit(batch);
                assertEquals(before + 16, state.keyBytesForTests(), "the slab of " + keyspace.name());
                // Two new keys, interleaved, and the one already held
                batch = ledger.newBatch();
                for (var i = 0; i < 40; i++) {
                    batch.put(keyspace, i % 3 == 0 ? "first-new-key" : i % 3 == 1 ? "second-new-key" : "a-key-of-16-byte", "value" + i);
                }
                ledger.commit(batch);
                assertEquals(before + 16 + 13 + 14, state.keyBytesForTests(), "the slab of " + keyspace.name());
                assertEquals(Optional.of("value39"), ledger.get(keyspace, "first-new-key"));
                assertEquals(Optional.of("value38"), ledger.get(keyspace, "a-key-of-16-byte"));
                assertEquals(3, ledger.size(keyspace));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @DisplayName("a batch whose runs of new keys open blocks one write at a time, between runs of held keys looked up a block at a time, with keys repeated across both, matches a model")
    void newKeysBetweenHeldKeys(boolean inDirectory, @TempDir Path directory) {
        var paged = new HashMap<String, String>();
        var resident = new HashMap<String, String>();
        var held = 400;
        try (var ledger = inDirectory ? directory(directory, 1L << 30) : memory()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < held; i++) {
                for (var keyspace : List.of(PAGED, RESIDENT)) {
                    batch.put(keyspace, "key" + i, "held" + i);
                    (keyspace == PAGED ? paged : resident).put("key" + i, "held" + i);
                }
            }
            ledger.commit(batch);
            for (var round = 0; round < 3; round++) {
                batch = ledger.newBatch();
                var write = 0;
                // Runs long enough that the blocks of new keys skip up to the most blocks, each run of held keys
                // ending the skips; every new key is written again in the next run of held keys
                for (var run = 0; run < 6; run++) {
                    var fresh = run % 2 == 0;
                    var length = fresh ? 16 * (40 + 13 * run) + 5 : 16 * 9 + 3;
                    for (var i = 0; i < length; i++, write++) {
                        var keyspace = i % 5 == 0 ? RESIDENT : PAGED;
                        var model = keyspace == PAGED ? paged : resident;
                        var name = fresh ? "key" + (held + round * 10_000 + write)
                                : i % 4 == 0 ? "key" + (held + round * 10_000 + write - length) : "key" + ((write * 7) % held);
                        var value = round + ":" + write;
                        if (!fresh && i % 11 == 0) {
                            batch.delete(keyspace, name);
                            model.remove(name);
                        } else {
                            batch.put(keyspace, name, value);
                            model.put(name, value);
                        }
                    }
                }
                ledger.commit(batch);
                verify(ledger, paged, resident, held + round * 10_000 + write);
                if (inDirectory) {
                    ledger.checkpoint();
                    verify(ledger, paged, resident, held + round * 10_000 + write);
                }
            }
        }
    }

    @Test
    @DisplayName("a chain dropped after a block looked it up, or between the lookup's steps, gives way to a chain of its own")
    void droppedAfterLookup() {
        for (var stage = 0; stage < 3; stage++) {
            var state = new KeyspaceState(PAGED);
            var key = key("k");
            var hash = Segments.hash(key);
            var dropped = state.open(key);
            var other = state.open(key("other"));
            var lookups = new ChainIndex.Lookups(true);
            state.lookUp(lookups, key, hash);
            state.lookUp(lookups, key("other"), Segments.hash(key("other")));
            switch (stage) {
                case 0 -> {
                    lookups.run();
                    assertSame(dropped, lookups.found(0));
                    assertTrue(state.drop(dropped, null, () -> Long.MAX_VALUE));
                }
                case 1 -> lookups.runForTests(() -> assertTrue(state.drop(dropped, null, () -> Long.MAX_VALUE)));
                default -> {
                    lookups.run();
                    // Released and still in the index, as the publisher leaves it between its two steps of a drop
                    assertTrue(dropped.release(dropped.claimed()));
                }
            }
            var opened = state.open(key, hash, lookups.found(0));
            assertNotSame(dropped, opened, "stage " + stage);
            assertFalse(opened.released(), "stage " + stage);
            assertSame(opened, state.chain(key), "stage " + stage);
            assertSame(opened, state.open(key), "stage " + stage);
            assertSame(other, state.open(key("other"), Segments.hash(key("other")), lookups.found(1)));
        }
    }

    @Test
    @DisplayName("batches of every size with repeated keys and removals match a model through checkpoints, a crash and a reopen")
    void fuzzAgainstModel(@TempDir Path directory) {
        var random = new SplittableRandom(17);
        var paged = new HashMap<String, String>();
        var resident = new HashMap<String, String>();
        var keys = 500;
        var ledger = directory(directory, 1 << 20);
        try {
            for (var round = 0; round < 120; round++) {
                var batch = ledger.newBatch();
                var size = SIZES[random.nextInt(SIZES.length)];
                for (var i = 0; i < size; i++) {
                    var keyspace = random.nextInt(3) == 0 ? RESIDENT : PAGED;
                    var model = keyspace == PAGED ? paged : resident;
                    var name = "key" + random.nextInt(keys);
                    if (random.nextInt(5) == 0) {
                        batch.delete(keyspace, name);
                        model.remove(name);
                    } else {
                        var value = round + ":" + i;
                        batch.put(keyspace, name, value);
                        model.put(name, value);
                    }
                }
                ledger.commit(batch);
                if (round % 9 == 0) {
                    ledger.checkpoint();
                }
                if (round % 10 == 0) {
                    verify(ledger, paged, resident, keys);
                }
            }
            verify(ledger, paged, resident, keys);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            ledger = directory(directory, 1 << 20);
            verify(ledger, paged, resident, keys);
        } finally {
            ledger.close();
        }
    }

    @Test
    @DisplayName("blocks committed on several threads beside checkpoints that drop their chains match a model, before and after a reopen")
    void concurrentBesideCheckpoints(@TempDir Path directory) throws Exception {
        var threads = 4;
        var keysPerThread = 300;
        var models = new ArrayList<Map<String, String>>();
        var residentModels = new ArrayList<Map<String, String>>();
        var failure = new AtomicReference<Throwable>();
        var stop = new AtomicBoolean();
        try (var ledger = directory(directory, 1 << 20)) {
            var checkpoints = Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        ledger.checkpoint();
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            var writers = new ArrayList<Thread>();
            for (var t = 0; t < threads; t++) {
                var paged = new HashMap<String, String>();
                var resident = new HashMap<String, String>();
                models.add(paged);
                residentModels.add(resident);
                var thread = t;
                writers.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new SplittableRandom(thread);
                        for (var round = 0; round < 1_500; round++) {
                            var batch = ledger.newBatch();
                            var size = 8 + random.nextInt(120);
                            for (var i = 0; i < size; i++) {
                                var keyspace = random.nextInt(4) == 0 ? RESIDENT : PAGED;
                                var model = keyspace == PAGED ? paged : resident;
                                // Each thread's keys are its own, interleaved with the others' in every segment
                                var name = "key" + (random.nextInt(keysPerThread) * threads + thread);
                                if (random.nextInt(6) == 0) {
                                    batch.delete(keyspace, name);
                                    model.remove(name);
                                } else {
                                    var value = thread + ":" + round + ":" + i;
                                    batch.put(keyspace, name, value);
                                    model.put(name, value);
                                }
                            }
                            ledger.commit(batch);
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            for (var writer : writers) {
                writer.join();
            }
            stop.set(true);
            checkpoints.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            verify(ledger, merge(models), merge(residentModels), threads * keysPerThread);
        }
        try (var ledger = directory(directory, 1 << 20)) {
            verify(ledger, merge(models), merge(residentModels), threads * keysPerThread);
        }
    }

    private static Map<String, String> merge(List<Map<String, String>> models) {
        var merged = new HashMap<String, String>();
        for (var model : models) {
            merged.putAll(model);
        }
        return merged;
    }
}
