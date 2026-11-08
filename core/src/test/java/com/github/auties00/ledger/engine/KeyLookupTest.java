package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Reads by key over a store's real trees and chains: the array a key is looked up by agrees with the segments the chains
/// were written with, and a key space whose chains are gone answers from its tree.
class KeyLookupTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> PAGED = Keyspace.paged("paged", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<byte[], byte[]> RESIDENT = Keyspace.resident("resident", KeyCodec.bytes(), ValueCodec.bytes());
    // Across word sizes and past the point where a comparison falls back to a mismatch
    private static final int[] KEY_LENGTHS = {1, 7, 8, 9, 20, 32, 63, 64, 65, 200, KeyCodec.MAX_LENGTH};

    private static Ledger open(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(PAGED).keyspace(NAMES).keyspace(RESIDENT).build();
    }

    private static byte[] key(int length, int id) {
        var key = new byte[length];
        Arrays.fill(key, (byte) 0xC3);
        key[length - 1] = (byte) id;
        return key;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void assertValue(String expected, Optional<byte[]> actual) {
        assertTrue(actual.isPresent(), "no value, expected " + expected);
        assertEquals(expected, new String(actual.get(), StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("a key with a version in memory newer than the tree reads that version, in every key length and through a snapshot taken before it")
    void memoryOverTree(@TempDir Path directory) {
        try (var ledger = open(directory)) {
            for (var length : KEY_LENGTHS) {
                ledger.commit(ledger.newBatch().put(PAGED, key(length, 1), bytes("tree" + length)));
            }
            ledger.commit(ledger.newBatch().put(NAMES, "name", "tree"));
            ledger.checkpoint();
            try (var before = ledger.snapshot()) {
                for (var length : KEY_LENGTHS) {
                    ledger.commit(ledger.newBatch().put(PAGED, key(length, 1), bytes("memory" + length)));
                }
                ledger.commit(ledger.newBatch().put(NAMES, "name", "memory"));
                for (var length : KEY_LENGTHS) {
                    assertValue("memory" + length, ledger.get(PAGED, key(length, 1)));
                    assertValue("memory" + length, ledger.readEncoded(PAGED, key(length, 1), Segments::toBytes));
                    assertValue("tree" + length, before.get(PAGED, key(length, 1)));
                    assertEquals(Optional.empty(), ledger.get(PAGED, key(length, 2)));
                }
                assertEquals(Optional.of("memory"), ledger.get(NAMES, "name"));
                assertEquals(Optional.of("tree"), before.get(NAMES, "name"));
            }
        }
    }

    @Test
    @DisplayName("a key space with no chain answers from its tree, and sees the first chain written after")
    void noChains(@TempDir Path directory) {
        try (var ledger = open(directory)) {
            for (var length : KEY_LENGTHS) {
                ledger.commit(ledger.newBatch().put(PAGED, key(length, 1), bytes("tree" + length)));
            }
        }
        try (var ledger = (LiveLedger) open(directory)) {
            var state = ledger.state(PAGED);
            assertEquals(0, state.chains());
            for (var length : KEY_LENGTHS) {
                assertValue("tree" + length, ledger.get(PAGED, key(length, 1)));
                assertEquals(Optional.empty(), ledger.get(PAGED, key(length, 2)));
            }
            var written = key(32, 3);
            ledger.commit(ledger.newBatch().put(PAGED, written, bytes("first")));
            assertTrue(state.chains() > 0);
            assertNull(state.head(key(32, 2)));
            assertValue("first", ledger.get(PAGED, written));
            assertValue("tree32", ledger.get(PAGED, key(32, 1)));
        }
    }

    // The bytes the calling thread allocated so far, by reflection: this module reads no management module
    private static long allocatedBytes() throws ReflectiveOperationException {
        var threads = Class.forName("java.lang.management.ManagementFactory").getMethod("getThreadMXBean").invoke(null);
        return (long) Class.forName("com.sun.management.ThreadMXBean").getMethod("getCurrentThreadAllocatedBytes").invoke(threads);
    }

    // The fewest bytes a get of every key allocated in one of several rounds: the first rounds also pay for what the
    // JVM has not linked or compiled yet
    private static long bytesPerGet(Ledger ledger, byte[][] keys) throws ReflectiveOperationException {
        var fewest = Long.MAX_VALUE;
        for (var round = 0; round < 40; round++) {
            var before = allocatedBytes();
            for (var key : keys) {
                assertTrue(ledger.get(PAGED, key).isPresent());
            }
            fewest = Math.min(fewest, (allocatedBytes() - before) / keys.length);
        }
        return fewest;
    }

    private static byte[][] keys(int length, int firstId, int count) {
        var keys = new byte[count][];
        for (var i = 0; i < count; i++) {
            keys[i] = key(length, firstId + i);
        }
        return keys;
    }

    private static byte[] filled(int length, int last) {
        var bound = new byte[length];
        Arrays.fill(bound, (byte) 0xC3);
        bound[length - 1] = (byte) last;
        return bound;
    }

    @ParameterizedTest
    @CsvSource({"32, 4", "32, 31", "100, 99"})
    @DisplayName("what a read by key allocates does not grow with the range removals no checkpoint has made durable, from the tree or from memory")
    void readsAllocateNothingPerRemoval(int keyLength, int shared, @TempDir Path directory) throws ReflectiveOperationException {
        var removals = 1_024;
        var inTree = keys(keyLength, 0, 100);
        var inMemory = keys(keyLength, 100, 100);
        try (var ledger = (LiveLedger) open(directory)) {
            var batch = ledger.newBatch();
            for (var key : inTree) {
                batch.put(PAGED, key, bytes("tree"));
            }
            ledger.commit(batch);
            ledger.checkpoint();
            batch = ledger.newBatch();
            for (var key : inMemory) {
                batch.put(PAGED, key, bytes("memory"));
            }
            ledger.commit(batch);
            var treeWithout = bytesPerGet(ledger, inTree);
            var memoryWithout = bytesPerGet(ledger, inMemory);

            // Every bound shares `shared` bytes with the keys and none covers a key: half end above them and decide at
            // the lower bound, half end below them and decide at the upper bound
            var above = KeyRange.between(filled(shared + 1, 0xFF), filled(shared + 2, 0xFF));
            var below = KeyRange.between(Arrays.copyOf(filled(shared + 1, 0xC3), shared), filled(shared + 1, 0x00));
            batch = ledger.newBatch();
            for (var i = 0; i < removals; i++) {
                batch.deleteRange(PAGED, i % 2 == 0 ? above : below);
            }
            ledger.commit(batch);

            var state = ledger.state(PAGED);
            assertEquals(removals, state.removals.length);
            var treeWith = bytesPerGet(ledger, inTree);
            var memoryWith = bytesPerGet(ledger, inMemory);
            assertEquals(removals, state.removals.length);
            // A segment per removal costs 72 bytes; the bound is far under that and far over what compiling changes
            assertTrue(treeWith - treeWithout < removals * 8L, "from the tree: " + treeWithout + " bytes, then " + treeWith);
            assertTrue(memoryWith - memoryWithout < removals * 8L, "from memory: " + memoryWithout + " bytes, then " + memoryWith);
        }
    }

    @Test
    @DisplayName("a range removal hides the tree's keys it covers from later readers, and no version written after it")
    void removals(@TempDir Path directory) {
        try (var ledger = open(directory)) {
            for (var id = 0; id < 10; id++) {
                ledger.commit(ledger.newBatch().put(PAGED, key(40, id), bytes("v" + id)));
            }
            ledger.checkpoint();
            try (var before = ledger.snapshot()) {
                ledger.commit(ledger.newBatch().deleteRange(PAGED, KeyRange.between(key(40, 3), key(40, 6))));
                ledger.commit(ledger.newBatch().put(PAGED, key(40, 4), bytes("again")));
                for (var id = 0; id < 10; id++) {
                    assertValue("v" + id, before.get(PAGED, key(40, id)));
                    if (id == 4) {
                        assertValue("again", ledger.get(PAGED, key(40, id)));
                    } else if (id == 3 || id == 5) {
                        assertEquals(Optional.empty(), ledger.get(PAGED, key(40, id)));
                        assertEquals(Optional.empty(), ledger.readEncoded(PAGED, key(40, id), Segments::toBytes));
                    } else {
                        assertValue("v" + id, ledger.get(PAGED, key(40, id)));
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("a value of bytes read from a leaf, inline or not, is the caller's own copy, of the exact length")
    void byteValues(@TempDir Path directory) {
        var sizes = new int[]{0, 1, 8, 100, 1_000, 3_000, 40_000, 1 << 20};
        var random = new SplittableRandom(3);
        var values = new byte[sizes.length][];
        try (var ledger = open(directory)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < sizes.length; i++) {
                values[i] = new byte[sizes[i]];
                random.nextBytes(values[i]);
                batch.put(PAGED, key(20, i), values[i]);
            }
            ledger.commit(batch);
            ledger.checkpoint();
            for (var i = 0; i < sizes.length; i++) {
                var read = ledger.get(PAGED, key(20, i)).orElseThrow();
                assertArrayEquals(values[i], read);
                assertNotSame(values[i], read);
                if (read.length > 0) {
                    read[0] ^= 0x55;
                    assertArrayEquals(values[i], ledger.get(PAGED, key(20, i)).orElseThrow(), "the read shared its array with the leaf");
                }
                assertArrayEquals(values[i], ledger.readEncoded(PAGED, key(20, i), Segments::toBytes).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("a key of bytes is checked as its encoding was, reads of every kind")
    void checkedKeys(@TempDir Path directory) {
        try (var ledger = open(directory)) {
            for (var keyspace : new Keyspace[]{PAGED, RESIDENT}) {
                @SuppressWarnings("unchecked")
                var typed = (Keyspace<byte[], byte[]>) keyspace;
                assertThrows(IllegalArgumentException.class, () -> ledger.get(typed, new byte[0]));
                assertThrows(IllegalArgumentException.class, () -> ledger.get(typed, new byte[KeyCodec.MAX_LENGTH + 1]));
                assertThrows(IllegalArgumentException.class, () -> ledger.readEncoded(typed, new byte[0], Segments::toBytes));
                assertThrows(IllegalArgumentException.class, () -> ledger.readEncoded(typed, new byte[KeyCodec.MAX_LENGTH + 1], Segments::toBytes));
                assertThrows(NullPointerException.class, () -> ledger.get(typed, null));
            }
        }
    }

    @Test
    @DisplayName("a key the caller changes after a read, or a bound it changes during a scan, changes nothing the store holds or walks")
    void callerArrays(@TempDir Path directory) {
        try (var ledger = open(directory)) {
            for (var id = 0; id < 100; id++) {
                ledger.commit(ledger.newBatch().put(PAGED, key(30, id), bytes("v" + id)));
            }
            ledger.checkpoint();
            var key = key(30, 7);
            assertValue("v7", ledger.get(PAGED, key));
            Arrays.fill(key, (byte) 0);
            assertValue("v7", ledger.get(PAGED, key(30, 7)));

            for (var range : new boolean[]{false, true}) {
                var from = key(30, 10);
                var to = key(30, 20);
                var seen = new ArrayList<Integer>();
                var bounds = KeyRange.between(from, to);
                ledger.forEach(PAGED, range ? bounds.reversed() : bounds, (found, _) -> {
                    seen.add(found[29] & 0xFF);
                    // The visit moves the array the caller gave as a bound: the scan keeps its own
                    Arrays.fill(from, (byte) 0);
                    Arrays.fill(to, (byte) 0xFF);
                    return true;
                });
                assertEquals(10, seen.size());
                assertEquals(range ? 19 : 10, seen.getFirst());
                assertEquals(range ? 10 : 19, seen.getLast());
            }
        }
    }

    @Test
    @DisplayName("a store that crashed finds the versions its log holds over its tree")
    void afterCrash(@TempDir Path directory) throws Exception {
        var ledger = open(directory);
        for (var length : KEY_LENGTHS) {
            ledger.commit(ledger.newBatch().put(PAGED, key(length, 1), bytes("tree" + length)));
        }
        ledger.checkpoint();
        for (var length : KEY_LENGTHS) {
            ledger.commit(ledger.newBatch().put(PAGED, key(length, 1), bytes("log" + length)));
        }
        ledger.awaitDurable();
        ((LiveLedger) ledger).closeWithoutCheckpoint();
        try (var reopened = open(directory)) {
            for (var length : KEY_LENGTHS) {
                assertValue("log" + length, reopened.get(PAGED, key(length, 1)));
                assertEquals(Optional.empty(), reopened.get(PAGED, key(length, 2)));
            }
        }
    }

    @Test
    @DisplayName("a reader finds at least the version of every commit it synchronized with while checkpoints empty the key space's chains")
    void readersBesideCheckpoints(@TempDir Path directory) throws Exception {
        var keys = 16;
        try (var ledger = (LiveLedger) open(directory)) {
            var published = new AtomicLong();
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var threads = new ArrayList<Thread>();
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    for (var n = 1L; !stop.get(); n++) {
                        ledger.commit(ledger.newBatch()
                                .put(PAGED, longKey(n % keys), ByteBuffer.allocate(Long.BYTES).putLong(n).array())
                                .put(NAMES, "n" + n % keys, Long.toString(n)));
                        published.setRelease(n);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        ledger.checkpoint();
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            for (var r = 0; r < 3; r++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get()) {
                            var n = published.getAcquire();
                            if (n == 0) {
                                continue;
                            }
                            var value = ledger.get(PAGED, longKey(n % keys));
                            if (value.isEmpty() || ByteBuffer.wrap(value.get()).getLong() < n) {
                                throw new AssertionError("the bytes key of " + n + " read " + value.map(found -> ByteBuffer.wrap(found).getLong()));
                            }
                            var name = ledger.get(NAMES, "n" + n % keys);
                            if (name.isEmpty() || Long.parseLong(name.get()) < n) {
                                throw new AssertionError("the name of " + n + " read " + name);
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                        stop.set(true);
                    }
                }));
            }
            Thread.sleep(3_000);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertFalse(published.get() == 0, "the writer committed nothing");
        }
    }

    private static byte[] longKey(long id) {
        return ByteBuffer.allocate(24).putLong(0x0102030405060708L).putLong(0x1112131415161718L).putLong(id).array();
    }
}
