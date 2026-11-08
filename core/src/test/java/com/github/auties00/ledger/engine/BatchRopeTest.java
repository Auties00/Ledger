package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Batches past a chunk, which move their encodings to a rope: paged values on chunks the store's versions keep, the
/// rest on chunks that return to the pool. Tests of a batch alone, of a store, of commits that fail and of crashes.
class BatchRopeTest {
    private static final byte[] KEY = new byte[32];
    private static final int CHUNK = ArrayWriteBatch.CHUNK_BYTES;
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());

    private static Ledger.Options options(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES);
    }

    private static ArrayWriteBatch batch(HeapPool pool) {
        return new ArrayWriteBatch(new Object(), pool, Map.of(MESSAGES.name(), MESSAGES, SESSIONS.name(), SESSIONS, NAMES.name(), NAMES));
    }

    private static HeapPool pool() {
        return new HeapPool(1L << 26, false);
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static byte[] key(int number) {
        return new byte[]{1, (byte) (number >>> 16), (byte) (number >>> 8), (byte) number};
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    @Nested
    @DisplayName("a batch")
    class Spans {
        @Test
        @DisplayName("stays on its array up to a chunk and moves to the rope past it, one write at the boundary split nowhere")
        void boundary() {
            var batch = batch(pool());
            // Each write takes 4 + 1,000 bytes, so the 262nd crosses
            for (var i = 0; i < 300; i++) {
                batch.put(i % 3 == 0 ? SESSIONS : MESSAGES, key(i), value(i, 1_000));
                assertEquals(i >= 261, batch.chunksForTests() > 0, "write " + i);
                assertEquals((i + 1) * 1_004L, batch.encodedBytes());
            }
            for (var i = 0; i < 300; i++) {
                assertArrayEquals(key(i), bytes(batch.key(i)), "key " + i);
                assertArrayEquals(value(i, 1_000), bytes(batch.value(i)), "value " + i);
                assertEquals(1_004, batch.spanBytes(i));
                assertEquals(1_000, batch.valueBytes(i));
                assertTrue(batch.value(i).isReadOnly());
                assertArrayEquals(value(i, 1_000), Arrays.copyOfRange(batch.valueArray(i), batch.valueArrayOffset(i), batch.valueArrayOffset(i) + 1_000));
            }
        }

        @ParameterizedTest(name = "lower bound {0}, upper bound {1}, room {2}")
        @CsvSource({"true,true,1", "true,false,1", "false,true,1", "false,false,1", "true,true,3", "true,true,4", "true,true,200"})
        @DisplayName("moves a range removal whose bounds cross the boundary, or that an earlier write left behind, with absent bounds absent")
        void rangeAcrossBoundary(boolean lower, boolean upper, int room) {
            var batch = batch(pool());
            var filler = new byte[120_000];
            batch.put(MESSAGES, new byte[]{1}, filler).put(MESSAGES, new byte[]{2}, filler);
            batch.put(MESSAGES, new byte[]{3}, new byte[CHUNK - 240_002 - 1 - room]);
            assertEquals(CHUNK - room, batch.encodedBytes());
            var low = new byte[]{4, 4};
            var high = new byte[]{5, 5};
            var range = lower ? upper ? KeyRange.between(low, high) : KeyRange.atLeast(low) : upper ? KeyRange.below(high) : KeyRange.<byte[]>all();
            batch.deleteRange(MESSAGES, range);
            batch.put(MESSAGES, new byte[]{6}, value(6, 5_000));
            assertTrue(batch.chunksForTests() > 0);
            if (lower) {
                assertArrayEquals(low, bytes(batch.key(3)));
            } else {
                assertNull(batch.key(3));
            }
            if (upper) {
                assertArrayEquals(high, bytes(batch.value(3)));
            } else {
                assertNull(batch.value(3));
            }
            assertEquals((lower ? 2 : 0) + (upper ? 2 : 0), batch.spanBytes(3));
            assertArrayEquals(new byte[]{3}, bytes(batch.key(2)));
            assertArrayEquals(value(6, 5_000), bytes(batch.value(4)));
        }

        @Test
        @DisplayName("keeps a removal's and a range's spans through the move, and takes them after it")
        void removals() {
            var batch = batch(pool());
            batch.delete(MESSAGES, new byte[]{1});
            batch.deleteRange(MESSAGES, KeyRange.atLeast(new byte[]{2}));
            batch.put(MESSAGES, new byte[]{3}, value(1, 130_000 - 1));
            batch.put(MESSAGES, new byte[]{4}, value(2, 130_000 - 1));
            batch.put(MESSAGES, new byte[]{5}, value(3, 3_000));
            assertTrue(batch.chunksForTests() > 0);
            batch.delete(MESSAGES, new byte[]{6});
            batch.deleteRange(MESSAGES, KeyRange.below(new byte[]{7}));
            assertArrayEquals(new byte[]{1}, bytes(batch.key(0)));
            assertNull(batch.value(0));
            assertArrayEquals(new byte[]{2}, bytes(batch.key(1)));
            assertNull(batch.value(1));
            assertArrayEquals(value(2, 130_000 - 1), bytes(batch.value(3)));
            assertArrayEquals(new byte[]{6}, bytes(batch.key(5)));
            assertNull(batch.key(6));
            assertArrayEquals(new byte[]{7}, bytes(batch.value(6)));
        }

        @Test
        @DisplayName("leaves values on arrays of their own where they are, before the move and after it")
        void ownValues() {
            var batch = batch(pool());
            batch.put(MESSAGES, new byte[]{1}, value(1, CHUNK));
            var first = batch.ownValue(0);
            batch.put(MESSAGES, new byte[]{2}, value(2, 100_000)).put(MESSAGES, new byte[]{3}, value(3, 100_000)).put(MESSAGES, new byte[]{4}, value(4, 100_000));
            assertTrue(batch.chunksForTests() > 0);
            batch.put(SESSIONS, new byte[]{5}, value(5, CHUNK / 2));
            batch.putEncoded(MESSAGES, MemorySegment.ofArray(new byte[]{6}), MemorySegment.ofArray(value(6, CHUNK / 2)));
            assertSame(first, batch.ownValue(0));
            assertArrayEquals(new byte[]{1}, bytes(batch.key(0)));
            assertArrayEquals(value(1, CHUNK), bytes(batch.value(0)));
            assertArrayEquals(new byte[]{5}, bytes(batch.key(4)));
            assertArrayEquals(value(5, CHUNK / 2), bytes(batch.value(4)));
            assertArrayEquals(value(6, CHUNK / 2), bytes(batch.value(5)));
            assertEquals(CHUNK + 300_000 + CHUNK + 6, batch.encodedBytes());
            batch.prepare(false);
            assertNull(batch.home(0));
            assertNull(batch.home(4));
            assertNull(batch.home(5));
            assertNotNull(batch.home(1));
        }

        @Test
        @DisplayName("refuses to detach, and compacts nothing, on a rope")
        void detachAndCompact() {
            var batch = batch(pool());
            batch.put(MESSAGES, new byte[]{1}, new byte[100_000]).put(MESSAGES, new byte[]{2}, new byte[100_000]).put(MESSAGES, new byte[]{3}, new byte[100_000]);
            batch.compact();
            assertThrows(IllegalStateException.class, batch::detach);
            assertArrayEquals(new byte[]{3}, bytes(batch.key(2)));
        }

        @Test
        @DisplayName("gives the prepared home of each paged value its bytes, and none to a resident value")
        void homes() {
            var batch = batch(pool());
            for (var i = 0; i < 500; i++) {
                batch.put(i % 5 == 0 ? SESSIONS : MESSAGES, key(i), value(i, 1_000 + i));
            }
            batch.prepare(false);
            for (var i = 0; i < 500; i++) {
                var home = batch.home(i);
                if (i % 5 == 0) {
                    assertNull(home);
                } else {
                    assertTrue(home.isReadOnly());
                    assertTrue(home.byteSize() <= CHUNK);
                    assertArrayEquals(value(i, 1_000 + i), bytes(home.asSlice(batch.homeOffset(i), 1_000 + i)), "write " + i);
                }
            }
        }

        @Test
        @DisplayName("a batch whose store copies every value has no homes, and all its chunks return to the pool")
        void copies() {
            var pool = pool();
            var batch = batch(pool);
            for (var i = 0; i < 100; i++) {
                batch.put(MESSAGES, key(i), new byte[10_000]);
            }
            var chunks = batch.chunksForTests();
            batch.prepare(true);
            assertNull(batch.home(0));
            var before = pool.pooledBytesForTests();
            batch.finish();
            assertEquals(before + (long) chunks * CHUNK, pool.pooledBytesForTests());
        }

        @ParameterizedTest(name = "{0} writes on the last chunk")
        @CsvSource({"1,9000", "27,243000", "28,262144", "29,262144"})
        @DisplayName("copies the last value chunk to an array of its bytes when it leaves more than a sixteenth unused")
        void tail(int last, int home) {
            var batch = batch(pool());
            // 29 values of 9,000 bytes fill the first chunk, whose remainder the next does not fit in
            for (var i = 0; i < 29 + last; i++) {
                batch.put(MESSAGES, key(i), value(i, 9_000));
            }
            batch.prepare(false);
            assertEquals(CHUNK, batch.home(0).byteSize());
            var lastWrite = 29 + last - 1;
            assertEquals(home, batch.home(lastWrite).byteSize());
            assertArrayEquals(value(lastWrite, 9_000), bytes(batch.home(lastWrite).asSlice(batch.homeOffset(lastWrite), 9_000)));
        }

        @Test
        @DisplayName("prepares again as it did, a write added after it starting a new chunk")
        void preparesTwice() {
            var batch = batch(pool());
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, key(i), value(i, 9_000));
            }
            batch.prepare(false);
            var homes = new MemorySegment[40];
            for (var i = 0; i < 40; i++) {
                homes[i] = batch.home(i);
            }
            var chunks = batch.chunksForTests();
            batch.prepare(false);
            for (var i = 0; i < 40; i++) {
                assertSame(homes[i], batch.home(i));
            }
            assertEquals(chunks, batch.chunksForTests());
            batch.put(MESSAGES, key(40), value(40, 9_000));
            assertEquals(chunks + 1, batch.chunksForTests());
            batch.prepare(false);
            for (var i = 0; i < 40; i++) {
                assertSame(homes[i], batch.home(i));
                assertArrayEquals(value(i, 9_000), bytes(homes[i].asSlice(batch.homeOffset(i), 9_000)));
            }
            assertEquals(9_000, batch.home(40).byteSize());
        }

        @Test
        @DisplayName("hands over the chunks of values at the end of a commit, and returns the others to the pool, including a last chunk it copied")
        void finishes() {
            var pool = pool();
            var batch = batch(pool);
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, key(i), value(i, 9_000));
            }
            batch.prepare(false);
            var home = batch.home(0);
            var last = batch.home(39);
            assertNotEquals(CHUNK, last.byteSize());
            var before = pool.pooledBytesForTests();
            batch.finish();
            // The chunk of the keys and the last value chunk, which versions do not slice
            assertEquals(before + 2L * CHUNK, pool.pooledBytesForTests());
            // No later batch gets an array the versions slice
            for (var round = 0; round < 8; round++) {
                var other = batch(pool);
                for (var i = 0; i < 40; i++) {
                    other.put(MESSAGES, key(i), new byte[9_000]);
                    Arrays.fill(other.valueArray(i), other.valueArrayOffset(i), other.valueArrayOffset(i) + 9_000, (byte) 0x7F);
                }
                other.prepare(true);
                other.finish();
            }
            assertArrayEquals(value(0, 9_000), bytes(home.asSlice(0, 9_000)));
            assertArrayEquals(value(39, 9_000), bytes(last.asSlice(last.byteSize() - 9_000, 9_000)));
        }

        @Test
        @DisplayName("forgets the rope when cleared, keeping out of the pool what a prepare exposed")
        void clears() {
            var pool = pool();
            var batch = batch(pool);
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, key(i), value(i, 9_000));
            }
            batch.prepare(false);
            var before = pool.pooledBytesForTests();
            batch.clear();
            assertEquals(before + 2L * CHUNK, pool.pooledBytesForTests());
            assertEquals(0, batch.encodedBytes());
            assertEquals(0, batch.chunksForTests());
            batch.put(MESSAGES, key(1), new byte[]{1});
            assertEquals(0, batch.chunksForTests());
            assertArrayEquals(new byte[]{1}, bytes(batch.value(0)));

            // Value chunks no prepare saw go back too
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, key(i), value(i, 9_000));
            }
            var chunks = batch.chunksForTests();
            before = pool.pooledBytesForTests();
            batch.clear();
            assertEquals(before + (long) chunks * CHUNK, pool.pooledBytesForTests());
        }

        @Test
        @DisplayName("still counts its writes against the store's limit")
        void maxOps(@TempDir Path directory) throws Exception {
            // About a gigabyte for the writes' tables
            Assumptions.assumeTrue(Runtime.getRuntime().maxMemory() >= 3L << 30);
            try (var ledger = options(directory).build()) {
                var batch = ledger.newBatch();
                var key = new byte[]{1};
                for (var i = 0; i <= Reads.MAX_OPS; i++) {
                    batch.delete(MESSAGES, key);
                }
                assertTrue(((ArrayWriteBatch) batch).chunksForTests() > 0);
                assertThrows(IllegalArgumentException.class, () -> ledger.commit(batch));
                batch.clear();
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        private static WriteBatch mixed(Ledger ledger, int count) {
            var batch = ledger.newBatch();
            for (var i = 0; i < count; i++) {
                switch (i % 7) {
                    case 0 -> batch.put(SESSIONS, key(i), value(i, 700));
                    case 1 -> batch.put(NAMES, key(i), "n".repeat(300 + i % 50));
                    case 2 -> batch.putEncoded(MESSAGES, MemorySegment.ofArray(key(i)), MemorySegment.ofArray(value(i, messageLength(i))));
                    default -> batch.put(MESSAGES, key(i), value(i, messageLength(i)));
                }
            }
            return batch;
        }

        // Some empty, some past the least threshold of the file of large values
        private static int messageLength(int i) {
            return i % 100 == 2 ? 0 : i % 7 == 3 ? 5_000 : 2_000;
        }

        private static void verifyMixed(Ledger ledger, int count) {
            for (var i = 0; i < count; i++) {
                switch (i % 7) {
                    case 0 -> assertArrayEquals(value(i, 700), ledger.get(SESSIONS, key(i)).orElseThrow(), "session " + i);
                    case 1 -> assertEquals("n".repeat(300 + i % 50), ledger.get(NAMES, key(i)).orElseThrow(), "name " + i);
                    default -> assertArrayEquals(value(i, messageLength(i)), ledger.get(MESSAGES, key(i)).orElseThrow(), "message " + i);
                }
            }
        }

        @Test
        @DisplayName("reads back a large batch of every kind of write, past a checkpoint and a reopen")
        void roundTrip(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                ledger.commit(mixed(ledger, 2_000)).awaitDurable();
                verifyMixed(ledger, 2_000);
                ledger.checkpoint();
                verifyMixed(ledger, 2_000);
            }
            try (var ledger = options(directory).build()) {
                verifyMixed(ledger, 2_000);
            }
        }

        @Test
        @DisplayName("applies a large batch with range removals in its order, whichever side of the move they are on")
        void rangeRemovals(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                for (var i = 0; i < 10; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{5, (byte) i}, new byte[]{(byte) i})).awaitDurable();
                }
                ledger.checkpoint();
                var batch = ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(new byte[]{5, 2}, new byte[]{5, 4}));
                for (var i = 0; i < 200; i++) {
                    batch.put(MESSAGES, key(i), value(i, 2_000));
                }
                batch.deleteRange(MESSAGES, KeyRange.between(new byte[]{5, 6}, new byte[]{5, 8})).deleteRange(MESSAGES, KeyRange.atLeast(new byte[]{5, 9}));
                ledger.commit(batch).awaitDurable();
                for (var i = 0; i < 10; i++) {
                    assertEquals(i < 2 || i >= 4 && i < 6 || i == 8, ledger.get(MESSAGES, new byte[]{5, (byte) i}).isPresent(), "key 5," + i);
                }
                for (var i = 0; i < 200; i++) {
                    assertArrayEquals(value(i, 2_000), ledger.get(MESSAGES, key(i)).orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("keeps a paged value on a chunk of values only, at most a chunk, and a resident value on its own array")
        void retention(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) options(directory).build()) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 400; i++) {
                    batch.put(MESSAGES, key(i), value(i, 2_000));
                    if (i % 40 == 0) {
                        batch.put(SESSIONS, key(i), value(i, 1_500));
                        batch.put(NAMES, key(i), "n".repeat(1_500));
                    }
                }
                ledger.commit(batch).awaitDurable();
                var arrays = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
                var total = 0L;
                for (var i = 0; i < 400; i++) {
                    var version = head(ledger, MESSAGES, key(i));
                    assertTrue(version.sourceBytesForTests() <= CHUNK);
                    if (arrays.add(version.sourceForTests())) {
                        total += version.sourceBytesForTests();
                    }
                }
                // 3 chunks of 131 values of 2,000 bytes, and one of the last 7, cut to their bytes
                assertEquals(4, arrays.size());
                assertEquals(3L * CHUNK + 14_000, total);
                assertEquals(1_500, head(ledger, SESSIONS, key(40)).sourceBytesForTests());
                assertEquals(1_500, head(ledger, NAMES, key(40)).sourceBytesForTests());
            }
        }

        @Test
        @DisplayName("what a commit keeps, range bounds included, survives other large batches taking arrays from the pool")
        void reusedArrays(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                for (var i = 0; i < 10; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{5, (byte) i}, new byte[]{(byte) i})).awaitDurable();
                }
                ledger.checkpoint();
                var first = mixed(ledger, 1_000).deleteRange(MESSAGES, KeyRange.between(new byte[]{5, 2}, new byte[]{5, 6}));
                ledger.commit(first).awaitDurable();
                for (var round = 0; round < 8; round++) {
                    var other = ledger.newBatch();
                    for (var i = 0; i < 400; i++) {
                        other.put(SESSIONS, new byte[]{7, (byte) round, (byte) (i >>> 8), (byte) i}, filled(1_500, 0x7F));
                        other.put(MESSAGES, new byte[]{7, (byte) round, (byte) (i >>> 8), (byte) i}, filled(1_500, 0x55));
                    }
                    ledger.commit(other).awaitDurable();
                    verifyMixed(ledger, 1_000);
                }
                for (var i = 0; i < 10; i++) {
                    assertEquals(i < 2 || i >= 6, ledger.get(MESSAGES, new byte[]{5, (byte) i}).isPresent(), "key 5," + i);
                }
            }
        }

        private static byte[] filled(int length, int value) {
            var bytes = new byte[length];
            Arrays.fill(bytes, (byte) value);
            return bytes;
        }

        @Test
        @DisplayName("a store in memory copies a large batch's values and leaves their chunks to the pool")
        void memoryStore() throws Exception {
            try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES).build()) {
                ledger.commit(mixed(ledger, 700)).awaitDurable();
                verifyMixed(ledger, 700);
                assertEquals(2_000, head(ledger, MESSAGES, key(4)).sourceBytesForTests());
            }
        }

        @ParameterizedTest(name = "values from {0} bytes in their units")
        @ValueSource(longs = {4_096, 1L << 20})
        @DisplayName("seals values on the rope into the file of large values")
        void units(long largeValues, @TempDir Path directory) throws Exception {
            try (var ledger = options(directory).largeValues(largeValues).build()) {
                ledger.commit(mixed(ledger, 700)).awaitDurable();
                verifyMixed(ledger, 700);
                ledger.checkpoint();
                verifyMixed(ledger, 700);
            }
            try (var ledger = options(directory).largeValues(largeValues).build()) {
                verifyMixed(ledger, 700);
            }
        }

        @ParameterizedTest(name = "alone: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a commit whose decoder throws leaves a large batch whole, to retry, to extend or to clear")
        void failedCommit(boolean alone, @TempDir Path directory) throws Exception {
            var fails = new AtomicBoolean(true);
            var picky = Keyspace.resident("picky", KeyCodec.bytes(), ValueCodec.<byte[]>of(value -> value.length,
                    (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
                    source -> {
                        if (fails.get()) {
                            throw new IllegalStateException("no");
                        }
                        return source.toArray(ValueLayout.JAVA_BYTE);
                    }));
            try (var ledger = options(directory).keyspace(picky).build()) {
                var batch = mixed(ledger, 700);
                if (alone) {
                    batch.deleteRange(MESSAGES, KeyRange.atLeast(new byte[]{9}));
                }
                batch.putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(MESSAGES, key(3)).isEmpty());
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                fails.set(false);
                batch.put(MESSAGES, new byte[]{8}, value(8, 2_000));
                ledger.commit(batch).awaitDurable();
                verifyMixed(ledger, 700);
                assertArrayEquals(value(8, 2_000), ledger.get(MESSAGES, new byte[]{8}).orElseThrow());
                assertArrayEquals(new byte[]{1, 2, 3}, ledger.get(picky, new byte[]{3}).orElseThrow());

                fails.set(true);
                var cleared = ledger.newBatch();
                for (var i = 0; i < 200; i++) {
                    cleared.put(MESSAGES, new byte[]{10, (byte) i}, value(i, 2_000));
                }
                cleared.putEncoded(picky, MemorySegment.ofArray(new byte[]{4}), MemorySegment.ofArray(new byte[]{4}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(cleared));
                fails.set(false);
                cleared.clear();
                for (var i = 0; i < 200; i++) {
                    cleared.put(MESSAGES, new byte[]{11, (byte) i}, value(i + 1, 2_000));
                }
                ledger.commit(cleared).awaitDurable();
                assertTrue(ledger.get(MESSAGES, new byte[]{10, 0}).isEmpty());
                for (var i = 0; i < 200; i++) {
                    assertArrayEquals(value(i + 1, 2_000), ledger.get(MESSAGES, new byte[]{11, (byte) i}).orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("a commit whose write to the file of large values fails can be retried, with the same batch, after the failure")
        void failedUnit() throws Exception {
            try (var disk = CrashFileSystem.create()) {
                var armed = new AtomicBoolean();
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(BlobFile.FILE) && armed.compareAndSet(true, false)) {
                        throw new java.io.IOException("no room");
                    }
                });
                try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                        .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES).largeValues(4_096).build()) {
                    var batch = mixed(ledger, 700);
                    armed.set(true);
                    assertThrows(LedgerIoException.class, () -> ledger.commit(batch));
                    ledger.commit(batch).awaitDurable();
                    verifyMixed(ledger, 700);
                }
            }
        }

        @Test
        @DisplayName("concurrent committers of large batches read back their values, beside checkpoints, and after a reopen")
        void concurrent(@TempDir Path directory) throws Exception {
            var threads = 4;
            var rounds = Integer.getInteger("stress.rounds", 25);
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var models = new ArrayList<TreeMap<Integer, byte[]>>();
            try (var ledger = options(directory).checkpointEvery(1L << 22).largeValues(64 << 10).build()) {
                var running = new ArrayList<Thread>();
                for (var t = 0; t < threads; t++) {
                    var model = new TreeMap<Integer, byte[]>();
                    models.add(model);
                    var id = t;
                    running.add(Thread.ofPlatform().start(() -> {
                        try {
                            var random = new Random(id);
                            var batch = ledger.newBatch();
                            for (var round = 0; round < rounds; round++) {
                                var pending = new TreeMap<Integer, byte[]>();
                                var writes = 100 + random.nextInt(400);
                                for (var w = 0; w < writes; w++) {
                                    var number = id << 16 | random.nextInt(5_000);
                                    var length = switch (random.nextInt(40)) {
                                        case 0 -> 0;
                                        case 1 -> 70_000 + random.nextInt(10_000);
                                        case 2 -> CHUNK / 2 + random.nextInt(1_000);
                                        default -> random.nextInt(4_000);
                                    };
                                    var bytes = value(random.nextInt(), length);
                                    batch.put(MESSAGES, key(number), bytes);
                                    pending.put(number, bytes);
                                    if (w % 50 == 0) {
                                        batch.put(SESSIONS, key(number), bytes);
                                    }
                                }
                                ledger.commit(batch);
                                model.putAll(pending);
                                var keys = new ArrayList<>(model.keySet());
                                for (var i = 0; i < 16; i++) {
                                    var number = keys.get(random.nextInt(keys.size()));
                                    assertArrayEquals(model.get(number), ledger.get(MESSAGES, key(number)).orElseThrow());
                                }
                            }
                        } catch (Throwable failure) {
                            failures.add(failure);
                        }
                    }));
                }
                for (var thread : running) {
                    thread.join();
                }
                if (!failures.isEmpty()) {
                    throw new AssertionError(failures.size() + " failures", failures.peek());
                }
                for (var model : models) {
                    for (var entry : model.entrySet()) {
                        assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
                    }
                }
            }
            try (var ledger = options(directory).largeValues(64 << 10).build()) {
                for (var model : models) {
                    for (var entry : model.entrySet()) {
                        assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key(entry.getKey())).orElseThrow());
                    }
                }
            }
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
        @DisplayName("recovers a large batch whole or not at all, wherever the power fails, and whole once it was durable")
        void crash(int seed) throws Exception {
            var large = seed % 2 == 0 ? 4_096L : 1L << 20;
            var dry = CrashFileSystem.create();
            writeForCrash(dry, large);
            var random = new Random(seed);
            for (var attempt = 0; attempt < 6; attempt++) {
                var disk = CrashFileSystem.create();
                if (random.nextBoolean()) {
                    disk.stopAfter(random.nextLong(dry.changes() + 1));
                } else {
                    disk.stopAtForce(random.nextLong(dry.forces() + 1));
                }
                var durable = writeForCrash(disk, large);
                var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
                try (var ledger = Ledger.builder().directory(survivor.getPath("/store")).encryption(Encryption.key(KEY))
                        .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES).largeValues(large).build()) {
                    var whole = ledger.get(SESSIONS, key(0)).isPresent();
                    assertTrue(whole || !durable, "a durable commit lost");
                    if (whole) {
                        verifyMixed(ledger, 700);
                    } else {
                        for (var i = 0; i < 700; i++) {
                            assertFalse(ledger.get(MESSAGES, key(i)).isPresent(), "part of a batch: " + i);
                        }
                    }
                }
            }
        }

        /// {@return whether the commit of the large batch was acknowledged durable before the file system stopped}
        private static boolean writeForCrash(CrashFileSystem disk, long large) {
            var durable = false;
            Ledger ledger = null;
            try {
                ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                        .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES).largeValues(large).build();
                ledger.commit(mixed(ledger, 700)).awaitDurable();
                durable = true;
                ledger.checkpoint();
            } catch (RuntimeException | InterruptedException _) {
                // The file system stopped where it was told to
            }
            if (ledger != null) {
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // Likewise
                }
            }
            return durable;
        }
    }
}
