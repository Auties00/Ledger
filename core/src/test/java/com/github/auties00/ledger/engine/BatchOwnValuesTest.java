package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Values of half a chunk or more, which a batch keeps on arrays of their own from the put on: what the store's versions
/// hold, how a commit that failed leaves them, and what recovery reads back.
class BatchOwnValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final int OWN = ArrayWriteBatch.CHUNK_BYTES / 2;
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());
    private static final Keyspace<byte[], String> TEXTS = Keyspace.paged("texts", KeyCodec.bytes(), ValueCodec.utf8());

    private static Ledger.Options options(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES).keyspace(TEXTS);
    }

    private static ArrayWriteBatch batch() {
        return new ArrayWriteBatch(new Object(), new HeapPool(1 << 24, false), Map.of(
                MESSAGES.name(), MESSAGES, SESSIONS.name(), SESSIONS, NAMES.name(), NAMES, TEXTS.name(), TEXTS));
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    @Nested
    @DisplayName("a batch")
    class Accessors {
        @Test
        @DisplayName("keeps the key on its array and the value on one of its own, read-only through the accessors")
        void spans() {
            var batch = batch();
            var caller = value(1, OWN);
            batch.put(MESSAGES, new byte[]{1, 2}, caller);
            assertEquals(2 + OWN, batch.encodedBytes());
            assertEquals(2 + OWN, batch.key(0).byteSize() + batch.valueBytes(0));
            assertEquals(OWN, batch.valueBytes(0));
            assertArrayEquals(new byte[]{1, 2}, batch.key(0).toArray(ValueLayout.JAVA_BYTE));
            var read = batch.value(0);
            assertTrue(read.isReadOnly());
            assertArrayEquals(caller, read.toArray(ValueLayout.JAVA_BYTE));
            var own = batch.ownValue(0);
            assertNotSame(caller, own);
            assertSame(own, batch.valueArray(0));
            assertEquals(0, batch.valueArrayOffset(0));
            caller[0]++;
            assertEquals((byte) (caller[0] - 1), own[0]);
        }

        @Test
        @DisplayName("leaves its own values out of what prepare keeps: alone they make no homes, beside a small value it keeps the array")
        void prepares() {
            var alone = batch();
            alone.put(MESSAGES, new byte[]{1}, value(1, OWN));
            alone.prepare(false);
            assertNull(alone.home(0));
            assertEquals(0, alone.homeOffset(0));
            var beside = batch();
            beside.put(MESSAGES, new byte[]{1}, value(1, OWN)).put(MESSAGES, new byte[]{2}, value(2, 2_000));
            beside.prepare(false);
            assertNull(beside.home(0));
            assertEquals(0, beside.homeOffset(0));
            assertNull(beside.ownValue(1));
            var home = beside.home(1);
            assertTrue(home.isReadOnly());
            assertArrayEquals(value(2, 2_000), home.asSlice(beside.homeOffset(1), 2_000).toArray(ValueLayout.JAVA_BYTE));
        }

        @Test
        @DisplayName("holds a range bound or a value below half a chunk on its array, and sends the store to it for the file of large values")
        void valueArrays() {
            var batch = batch();
            batch.put(MESSAGES, new byte[]{1}, value(1, OWN - 1)).deleteRange(MESSAGES, KeyRange.below(new byte[]{9})).delete(MESSAGES, new byte[]{8});
            assertNull(batch.ownValue(0));
            assertEquals(1, batch.valueArrayOffset(0));
            assertArrayEquals(value(1, OWN - 1), java.util.Arrays.copyOfRange(batch.valueArray(0), 1, OWN));
            assertNull(batch.key(1));
            assertArrayEquals(new byte[]{9}, batch.value(1).toArray(ValueLayout.JAVA_BYTE));
            assertNull(batch.value(2));
            assertEquals(1, batch.key(2).byteSize() + batch.valueBytes(2));
            assertEquals(0, batch.valueBytes(2));
        }

        @Test
        @DisplayName("refuses to detach encodings that are not on one array, and forgets its own values when cleared")
        void detachAndClear() {
            var batch = batch();
            batch.put(MESSAGES, new byte[]{1}, value(1, OWN));
            assertThrows(IllegalStateException.class, batch::detach);
            batch.clear();
            assertEquals(0, batch.encodedBytes());
            assertTrue(batch.isEmpty());
            batch.put(MESSAGES, new byte[]{1}, new byte[3]);
            assertNull(batch.ownValue(0));
            assertEquals(4, batch.detach().length);
        }

        @Test
        @DisplayName("keeps its own values through the growth of its writes")
        void growth() {
            var batch = batch();
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, new byte[]{(byte) i}, i % 10 == 0 ? new byte[OWN] : new byte[3]);
            }
            for (var i = 0; i < 40; i++) {
                assertEquals(i % 10 == 0 ? OWN : 3, batch.valueBytes(i));
                assertEquals(i % 10 == 0, batch.ownValue(i) != null);
            }
        }

        @ParameterizedTest(name = "held {0}, more {1}")
        @CsvSource({"0,2130706423", "2130706422,1", "1073741824,1056964599"})
        @DisplayName("has room up to what a log frame's array can hold")
        void room(long held, long more) {
            ArrayWriteBatch.checkRoom(held, more);
            assertThrows(IllegalArgumentException.class, () -> ArrayWriteBatch.checkRoom(held, more + 1));
        }

        static Stream<Arguments> segments() {
            var arena = Arena.global();
            return Stream.of(
                    Arguments.of("a byte array", (java.util.function.Function<byte[], MemorySegment>) MemorySegment::ofArray),
                    Arguments.of("a slice of a byte array", (java.util.function.Function<byte[], MemorySegment>) bytes -> {
                        var wider = new byte[bytes.length + 11];
                        System.arraycopy(bytes, 0, wider, 5, bytes.length);
                        return MemorySegment.ofArray(wider).asSlice(5, bytes.length);
                    }),
                    Arguments.of("a read-only byte array", (java.util.function.Function<byte[], MemorySegment>) bytes -> MemorySegment.ofArray(bytes).asReadOnly()),
                    Arguments.of("native memory", (java.util.function.Function<byte[], MemorySegment>) bytes -> {
                        var memory = arena.allocate(bytes.length);
                        MemorySegment.copy(bytes, 0, memory, ValueLayout.JAVA_BYTE, 0, bytes.length);
                        return memory;
                    }),
                    Arguments.of("a long array", (java.util.function.Function<byte[], MemorySegment>) bytes -> {
                        var memory = MemorySegment.ofArray(new long[bytes.length / Long.BYTES]);
                        MemorySegment.copy(bytes, 0, memory, ValueLayout.JAVA_BYTE, 0, bytes.length);
                        return memory;
                    }));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("segments")
        @DisplayName("copies an encoded value, whichever memory it is on")
        void encoded(String name, java.util.function.Function<byte[], MemorySegment> make) {
            var bytes = value(5, 2 * OWN);
            var expected = bytes.clone();
            var segment = make.apply(bytes);
            var batch = batch();
            batch.putEncoded(MESSAGES, MemorySegment.ofArray(new byte[]{3}), segment);
            assertArrayEquals(expected, batch.ownValue(0));
            if (!segment.isReadOnly()) {
                segment.set(ValueLayout.JAVA_BYTE, 0, (byte) (expected[0] + 1));
            }
            assertArrayEquals(expected, batch.value(0).toArray(ValueLayout.JAVA_BYTE));
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        @ParameterizedTest(name = "{0} bytes")
        @ValueSource(ints = {OWN - 1, OWN, 1 << 20, (1 << 20) + 1})
        @DisplayName("keeps a paged value on an array of the value's length from half a chunk on, past a commit, a checkpoint and a reopen")
        void paged(int length, @TempDir Path directory) throws Exception {
            var caller = value(length, length);
            var expected = caller.clone();
            try (var ledger = (LiveLedger) options(directory).build()) {
                // A session beside it, so a value below half a chunk has to leave the batch's array
                var batch = ledger.newBatch().put(SESSIONS, new byte[]{9}, new byte[16_384]).put(MESSAGES, new byte[]{1}, caller);
                // The batch copied the value at the put
                caller[0]++;
                ledger.commit(batch).awaitDurable();
                var version = head(ledger, MESSAGES, new byte[]{1});
                assertEquals(length, version.sourceBytesForTests());
                assertEquals(length >= OWN, version.sourceForTests() instanceof byte[]);
                assertArrayEquals(expected, ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
                ledger.checkpoint();
                assertArrayEquals(expected, ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
            }
            try (var ledger = options(directory).build()) {
                assertArrayEquals(expected, ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
            }
        }

        @Test
        @DisplayName("holds a resident byte array once, on the batch's own copy, which reads return")
        void residentBytes(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) options(directory).build()) {
                var caller = value(1, OWN);
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, caller)).awaitDurable();
                var version = head(ledger, SESSIONS, new byte[]{1});
                assertSame(version.sourceForTests(), version.decoded);
                assertNotSame(caller, version.decoded);
                assertSame(version.decoded, ledger.get(SESSIONS, new byte[]{1}).orElseThrow());
                assertArrayEquals(caller, ledger.get(SESSIONS, new byte[]{1}).orElseThrow());
            }
        }

        @Test
        @DisplayName("keeps the decoded form of a resident value of another codec beside its own encoding, and encodes a paged one once")
        void otherCodecs(@TempDir Path directory) throws Exception {
            var text = "x".repeat(OWN + 3);
            try (var ledger = (LiveLedger) options(directory).build()) {
                ledger.commit(ledger.newBatch().put(NAMES, new byte[]{1}, text).put(TEXTS, new byte[]{2}, text)).awaitDurable();
                var resident = head(ledger, NAMES, new byte[]{1});
                assertSame(text, resident.decoded);
                assertInstanceOf(byte[].class, resident.sourceForTests());
                assertEquals(text.length(), resident.sourceBytesForTests());
                assertEquals(text, ledger.get(NAMES, new byte[]{1}).orElseThrow());
                assertInstanceOf(byte[].class, head(ledger, TEXTS, new byte[]{2}).sourceForTests());
                assertEquals(text, ledger.get(TEXTS, new byte[]{2}).orElseThrow());
            }
        }

        @Test
        @DisplayName("a store in memory keeps the batch's array without a copy")
        void memoryStore() {
            try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(MESSAGES).build()) {
                var value = value(1, OWN);
                var batch = (ArrayWriteBatch) ledger.newBatch().put(MESSAGES, new byte[]{1}, value);
                var own = batch.ownValue(0);
                ledger.commit(batch);
                assertSame(own, head(ledger, MESSAGES, new byte[]{1}).sourceForTests());
                assertArrayEquals(value, ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
            }
        }

        @Test
        @DisplayName("a commit whose decoder throws leaves the batch whole, to retry, to extend or to clear")
        void failedCommit(@TempDir Path directory) throws Exception {
            var fails = new AtomicBoolean(true);
            var picky = Keyspace.resident("picky", KeyCodec.bytes(), ValueCodec.<byte[]>of(value -> value.length,
                    (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
                    source -> {
                        if (fails.get()) {
                            throw new IllegalStateException("no");
                        }
                        return source.toArray(ValueLayout.JAVA_BYTE);
                    }));
            var large = value(1, OWN);
            var small = value(2, 500);
            try (var ledger = options(directory).keyspace(picky).build()) {
                var batch = ledger.newBatch().put(MESSAGES, new byte[]{1}, large).put(MESSAGES, new byte[]{2}, small)
                        .putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(MESSAGES, new byte[]{1}).isEmpty());
                // Prepared twice, on the arrays the first prepare left
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                fails.set(false);
                batch.put(MESSAGES, new byte[]{4}, value(4, OWN));
                ledger.commit(batch).awaitDurable();
                assertArrayEquals(large, ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
                assertArrayEquals(small, ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                assertArrayEquals(value(4, OWN), ledger.get(MESSAGES, new byte[]{4}).orElseThrow());
                assertArrayEquals(new byte[]{1, 2, 3}, ledger.get(picky, new byte[]{3}).orElseThrow());

                fails.set(true);
                var cleared = ledger.newBatch().put(MESSAGES, new byte[]{5}, large).deleteRange(MESSAGES, KeyRange.all())
                        .putEncoded(picky, MemorySegment.ofArray(new byte[]{6}), MemorySegment.ofArray(new byte[]{1}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(cleared));
                assertArrayEquals(small, ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                cleared.clear();
                fails.set(false);
                ledger.commit(cleared.put(MESSAGES, new byte[]{7}, new byte[]{7})).awaitDurable();
                assertTrue(ledger.get(MESSAGES, new byte[]{5}).isEmpty());
                assertArrayEquals(new byte[]{7}, ledger.get(MESSAGES, new byte[]{7}).orElseThrow());
            }
        }

        static Stream<Arguments> recoveries() {
            return Stream.of(true, false).flatMap(encrypted -> Stream.of(
                    Arguments.of(encrypted, 4_096L, List.of(10_000, 30_000, OWN, 1 << 20)),
                    Arguments.of(encrypted, 1L << 30, List.of(10_000, OWN, (1 << 20) + 5))));
        }

        @ParameterizedTest(name = "encrypted: {0}, values from {1} bytes in their units")
        @MethodSource("recoveries")
        @DisplayName("recovers after a power loss every value a durable commit wrote, from the batch's array or their own, in its frame or its unit")
        void recovers(boolean encrypted, long largeValues, List<Integer> lengths) throws Exception {
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            try (var disk = CrashFileSystem.create()) {
                var path = disk.getPath("/store");
                var ledger = Ledger.builder().directory(path).encryption(encryption).keyspace(MESSAGES).keyspace(SESSIONS)
                        .largeValues(largeValues).build();
                var batch = ledger.newBatch();
                for (var i = 0; i < lengths.size(); i++) {
                    batch.put(i % 2 == 0 ? MESSAGES : SESSIONS, new byte[]{(byte) i}, value(i, lengths.get(i)));
                }
                ledger.commit(batch).awaitDurable();
                disk.stopAfter(0);
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // The file system stopped where it was told to
                }
                var survivor = disk.afterPowerLoss(new Random(7));
                try (var reopened = Ledger.builder().directory(survivor.getPath("/store")).encryption(encryption).keyspace(MESSAGES).keyspace(SESSIONS)
                        .largeValues(largeValues).build()) {
                    for (var i = 0; i < lengths.size(); i++) {
                        var read = reopened.get(i % 2 == 0 ? MESSAGES : SESSIONS, new byte[]{(byte) i});
                        assertFalse(read.isEmpty(), "write " + i);
                        assertArrayEquals(value(i, lengths.get(i)), read.get(), "write " + i);
                    }
                }
            }
        }
    }
}
