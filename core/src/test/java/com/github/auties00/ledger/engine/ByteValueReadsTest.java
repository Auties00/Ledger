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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
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
import static org.junit.jupiter.api.Assertions.fail;

/// Reads of byte array values, which copy each value out of the array that holds it (a version's own, the segment of
/// a chunk or a log frame, a leaf, a blob of the data file, a unit of the file of large values) rather than decoding
/// it, while a resident key space returns the instance it holds and any other codec decodes.
class ByteValueReadsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    // Byte arrays under a codec of the caller's: the reads must decode them
    private static final Keyspace<String, byte[]> MASKED = Keyspace.paged("masked", KeyCodec.utf8(), ValueCodec.ofTrusted(
            value -> value.length,
            (value, target) -> {
                for (var i = 0; i < value.length; i++) {
                    target.set(ValueLayout.JAVA_BYTE, i, (byte) (value[i] ^ 0x5A));
                }
            },
            source -> {
                var bytes = source.toArray(ValueLayout.JAVA_BYTE);
                for (var i = 0; i < bytes.length; i++) {
                    bytes[i] ^= 0x5A;
                }
                return bytes;
            }));
    // Above half a chunk, so it takes an array of its own at the put
    private static final int LARGE = ArrayWriteBatch.CHUNK_BYTES / 2 + 1_000;
    // Above a chunk, whatever else its batch holds
    private static final int HUGE = ArrayWriteBatch.CHUNK_BYTES + 50_000;

    private static Ledger open(Path directory, long largeValues) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(FILES).keyspace(SESSIONS).keyspace(MASKED).largeValues(largeValues).build();
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static Version head(Ledger ledger, Keyspace<String, ?> keyspace, String key) {
        var chain = ((LiveLedger) ledger).state(keyspace).chain(Segments.encodeKey(KeyCodec.utf8(), key));
        return chain == null ? null : chain.head;
    }

    private static boolean ownsArray(Version version) {
        return version.sourceForTests() instanceof byte[];
    }

    /// Checks that the store, a snapshot of it and a transaction read `expected` under `key`, each read in an array of
    /// its own, as a decode of what [Ledger#readEncoded] reads gives it.
    private static void assertReads(Ledger ledger, Keyspace<String, byte[]> keyspace, String key, byte[] expected) {
        var first = ledger.get(keyspace, key).orElseThrow();
        assertArrayEquals(expected, first);
        var second = ledger.get(keyspace, key).orElseThrow();
        assertNotSame(first, second);
        if (first.length > 0) {
            first[first.length - 1] ^= 1;
            assertArrayEquals(expected, ledger.get(keyspace, key).orElseThrow(), "a read returned the store's array");
        }
        assertArrayEquals(expected, ledger.readEncoded(keyspace, key, keyspace.values()::decode).orElseThrow());
        try (var snapshot = ledger.snapshot()) {
            assertArrayEquals(expected, snapshot.get(keyspace, key).orElseThrow());
        }
        var read = new byte[1][];
        ledger.update(transaction -> read[0] = transaction.get(keyspace, key).orElseThrow());
        assertArrayEquals(expected, read[0]);
    }

    @Nested
    @DisplayName("paged values")
    class Paged {
        @ParameterizedTest(name = "large values from {0} bytes")
        @ValueSource(longs = {4_096, Long.MAX_VALUE})
        @DisplayName("read the same bytes from versions in memory, after a checkpoint, and from the tree alone after a reopen")
        void memoryThenTree(long largeValues, @TempDir Path directory) {
            var huge = value(1, HUGE);
            var large = value(2, LARGE);
            var small = value(3, 100);
            var empty = new byte[0];
            var alone = value(4, LARGE);
            var expected = Map.of("huge", huge, "large", large, "small", small, "empty", empty, "alone", alone);
            try (var ledger = open(directory, largeValues)) {
                ledger.commit(ledger.newBatch().put(FILES, "huge", huge).put(FILES, "large", large)
                        .put(FILES, "small", small).put(FILES, "empty", empty));
                ledger.commit(ledger.newBatch().put(FILES, "alone", alone));
                for (var key : List.of("huge", "large", "alone")) {
                    assertTrue(ownsArray(head(ledger, FILES, key)), "a value of over half a chunk is on a segment");
                }
                assertFalse(ownsArray(head(ledger, FILES, "small")), "a small value took an array of its own");
                expected.forEach((key, value) -> assertReads(ledger, FILES, key, value));
                ledger.checkpoint();
                expected.forEach((key, value) -> assertReads(ledger, FILES, key, value));
            }
            try (var ledger = open(directory, largeValues)) {
                for (var entry : expected.entrySet()) {
                    assertNull(head(ledger, FILES, entry.getKey()), "a paged value is in memory after a reopen");
                    assertReads(ledger, FILES, entry.getKey(), entry.getValue());
                }
                assertEquals(Optional.empty(), ledger.get(FILES, "absent"));
                // A transaction reads its own write, on the segment the transaction encoded it into
                var written = value(5, HUGE);
                var read = new byte[1][];
                ledger.update(transaction -> {
                    transaction.put(FILES, "written", written);
                    read[0] = transaction.get(FILES, "written").orElseThrow();
                });
                assertArrayEquals(written, read[0]);
                assertReads(ledger, FILES, "written", written);
            }
        }

        @Test
        @DisplayName("read inline values of the tree each in an array of its own")
        void inline(@TempDir Path directory) {
            var values = new TreeMap<String, byte[]>();
            for (var i = 0; i < 300; i++) {
                values.put("k%03d".formatted(i), value(100 + i, i % 50));
            }
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                var batch = ledger.newBatch();
                values.forEach((key, value) -> batch.put(FILES, key, value));
                ledger.commit(batch);
                ledger.checkpoint();
            }
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                values.forEach((key, value) -> {
                    var first = ledger.get(FILES, key).orElseThrow();
                    assertArrayEquals(value, first);
                    assertNotSame(first, ledger.get(FILES, key).orElseThrow());
                });
            }
        }

        @ParameterizedTest(name = "large values from {0} bytes")
        @ValueSource(longs = {4_096, Long.MAX_VALUE})
        @DisplayName("hide a value of the tree that a range removal since covers, and keep it for a snapshot from before")
        void rangeRemoval(long largeValues, @TempDir Path directory) {
            var huge = value(6, HUGE);
            var small = value(7, 10);
            try (var ledger = open(directory, largeValues)) {
                ledger.commit(ledger.newBatch().put(FILES, "b-huge", huge).put(FILES, "b-small", small).put(FILES, "d", small));
                ledger.checkpoint();
            }
            try (var ledger = open(directory, largeValues)) {
                try (var before = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().deleteRange(FILES, KeyRange.between("b", "c")));
                    assertEquals(Optional.empty(), ledger.get(FILES, "b-huge"));
                    assertEquals(Optional.empty(), ledger.get(FILES, "b-small"));
                    assertArrayEquals(small, ledger.get(FILES, "d").orElseThrow());
                    assertArrayEquals(huge, before.get(FILES, "b-huge").orElseThrow());
                    assertArrayEquals(small, before.get(FILES, "b-small").orElseThrow());
                    ledger.update(transaction -> assertEquals(Optional.empty(), transaction.get(FILES, "b-huge")));
                }
                ledger.checkpoint();
                assertEquals(Optional.empty(), ledger.get(FILES, "b-huge"));
            }
        }

        @Test
        @DisplayName("a value of a batch used again is the one it carries, not the array a large value of the batch's last commit took")
        void reusedBatch(@TempDir Path directory) {
            var huge = value(8, HUGE);
            var large = value(9, LARGE);
            var small = value(10, 200);
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                var batch = ledger.newBatch();
                ledger.commit(batch.put(FILES, "huge", huge).put(FILES, "large", large));
                ledger.commit(batch.put(FILES, "small", small));
                assertFalse(ownsArray(head(ledger, FILES, "small")));
                assertTrue(head(ledger, FILES, "small").sourceBytesForTests() < LARGE, "a small value kept a large one's array");
                assertReads(ledger, FILES, "small", small);
                assertReads(ledger, FILES, "huge", huge);
                assertReads(ledger, FILES, "large", large);
                // A batch whose array goes back to the pool: the next one that takes it writes other bytes in it
                for (var round = 0; round < 4; round++) {
                    ledger.commit(batch.put(FILES, "other" + round, value(20 + round, HUGE)).put(FILES, "s" + round, small));
                }
                assertReads(ledger, FILES, "huge", huge);
                assertReads(ledger, FILES, "large", large);
            }
        }

        @Test
        @DisplayName("a batch cleared after it was prepared gives no write the array a large value took")
        void clearedBatch(@TempDir Path directory) {
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                var batch = (ArrayWriteBatch) ledger.newBatch();
                batch.put(FILES, "huge", value(11, HUGE)).put(FILES, "small", value(12, 10));
                batch.prepare(false);
                assertEquals(HUGE, batch.ownValue(0).length, "the large value has no array of its own");
                assertNull(batch.home(0), "the large value has a home besides its array");
                assertInstanceOf(MemorySegment.class, batch.home(1), "the small value has no home");
                batch.clear();
                batch.put(FILES, "next", value(13, 10));
                assertNull(batch.ownValue(0), "a cleared batch kept the array of a large value");
                assertNull(batch.home(0), "a cleared batch kept the homes of its last preparation");
                batch.finish();
            }
        }
    }

    @Nested
    @DisplayName("other key spaces")
    class Others {
        @Test
        @DisplayName("a resident key space returns the array it holds, before and after a reopen")
        void resident(@TempDir Path directory) {
            var huge = value(14, HUGE);
            var small = value(15, 40);
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                ledger.commit(ledger.newBatch().put(SESSIONS, "huge", huge).put(SESSIONS, "small", small).put(FILES, "f", small));
                for (var key : List.of("huge", "small")) {
                    var held = ledger.get(SESSIONS, key).orElseThrow();
                    assertSame(held, ledger.get(SESSIONS, key).orElseThrow());
                    assertSame(head(ledger, SESSIONS, key).decoded, held);
                }
                assertArrayEquals(huge, ledger.get(SESSIONS, "huge").orElseThrow());
                ledger.checkpoint();
            }
            try (var ledger = open(directory, Long.MAX_VALUE)) {
                // The first read starts the load, and reads the key from the tree unless the load has passed it
                assertArrayEquals(huge, ledger.get(SESSIONS, "huge").orElseThrow());
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!((LiveLedger) ledger).residentLoadedForTests(SESSIONS)) {
                    assertTrue(System.nanoTime() < deadline, "the load did not end");
                    Thread.onSpinWait();
                }
                var held = ledger.get(SESSIONS, "huge").orElseThrow();
                assertArrayEquals(huge, held);
                assertSame(held, ledger.get(SESSIONS, "huge").orElseThrow());
                assertArrayEquals(small, ledger.get(SESSIONS, "small").orElseThrow());
            }
        }

        @ParameterizedTest(name = "large values from {0} bytes")
        @ValueSource(longs = {4_096, Long.MAX_VALUE})
        @DisplayName("a codec of byte arrays other than the store's decodes every read")
        void otherCodec(long largeValues, @TempDir Path directory) {
            var huge = value(16, HUGE);
            var small = value(17, 30);
            try (var ledger = open(directory, largeValues)) {
                ledger.commit(ledger.newBatch().put(MASKED, "huge", huge).put(MASKED, "small", small).put(FILES, "f", small));
                assertReads(ledger, MASKED, "huge", huge);
                assertReads(ledger, MASKED, "small", small);
                ledger.checkpoint();
            }
            try (var ledger = open(directory, largeValues)) {
                assertReads(ledger, MASKED, "huge", huge);
                assertReads(ledger, MASKED, "small", small);
            }
        }
    }

    @Nested
    @DisplayName("copies")
    class Copies {
        @Test
        @DisplayName("copy the bytes of a version on an array of its own or on part of a segment, and nothing for a removal")
        void versions() {
            var bytes = value(18, 1_000);
            var own = new Version(1, bytes, null, null);
            assertArrayEquals(bytes, own.copyBytes());
            assertNotSame(bytes, own.copyBytes());
            var segment = MemorySegment.ofArray(bytes).asReadOnly();
            var part = Version.of(1, segment, 100, 300, null, null, null);
            assertArrayEquals(Arrays.copyOfRange(bytes, 100, 400), part.copyBytes());
            var external = Version.ofOwn(1, bytes, null, null, new PageRef(1, 1, 2, 3));
            assertArrayEquals(bytes, external.copyBytes());
            assertNull(new Version(1, (MemorySegment) null, null, null).copyBytes());
        }

        @Test
        @DisplayName("refuse a copy past the end of its array rather than pad it with zeros")
        void bounds() {
            var bytes = value(19, 10);
            assertArrayEquals(Arrays.copyOfRange(bytes, 2, 5), Segments.toBytes(bytes, 2, 3));
            assertEquals(0, Segments.toBytes(bytes, 10, 0).length);
            assertThrows(IndexOutOfBoundsException.class, () -> Segments.toBytes(bytes, 5, 6));
            assertThrows(IndexOutOfBoundsException.class, () -> Segments.toBytes(bytes, -1, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> Segments.toBytes(MemorySegment.ofArray(bytes), 5, 6));
        }
    }

    @ParameterizedTest(name = "large values from {0} bytes")
    @ValueSource(longs = {4_096, Long.MAX_VALUE})
    @DisplayName("readers see every large value whole while a writer replaces them and checkpoints cut their versions")
    void concurrentReads(long largeValues, @TempDir Path directory) throws Exception {
        var keys = List.of("a", "b", "c", "d");
        try (var ledger = open(directory, largeValues)) {
            for (var i = 0; i < keys.size(); i++) {
                ledger.commit(ledger.newBatch().put(FILES, keys.get(i), stamped(i, LARGE)));
            }
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var done = new AtomicBoolean();
            var readers = new ArrayList<Thread>();
            for (var r = 0; r < 3; r++) {
                var reader = r;
                readers.add(Thread.ofPlatform().start(() -> {
                    var random = ThreadLocalRandom.current();
                    try {
                        while (!done.get()) {
                            var key = keys.get(random.nextInt(keys.size()));
                            var read = switch (reader) {
                                case 0 -> ledger.get(FILES, key);
                                case 1 -> {
                                    try (var snapshot = ledger.snapshot()) {
                                        yield snapshot.get(FILES, key);
                                    }
                                }
                                default -> {
                                    var found = new AtomicReference<Optional<byte[]>>();
                                    ledger.update(transaction -> found.set(transaction.get(FILES, key)));
                                    yield found.get();
                                }
                            };
                            checkStamped(read.orElseThrow(() -> new AssertionError("no value under " + key)));
                        }
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                }));
            }
            var random = new Random(largeValues);
            for (var generation = keys.size(); generation < 160 && failures.isEmpty(); generation++) {
                var key = keys.get(random.nextInt(keys.size()));
                var length = LARGE + random.nextInt(HUGE);
                var batch = ledger.newBatch().put(FILES, key, stamped(generation, length));
                if (random.nextBoolean()) {
                    // Beside small values, which versions find on a segment
                    batch.put(FILES, "small" + random.nextInt(8), value(generation, 64));
                }
                ledger.commit(batch);
                if (generation % 20 == 0) {
                    ledger.checkpoint();
                }
            }
            done.set(true);
            for (var reader : readers) {
                reader.join();
            }
            if (!failures.isEmpty()) {
                var failure = new AssertionError("A reader failed");
                failures.forEach(failure::addSuppressed);
                throw failure;
            }
        }
    }

    /// {@return a value of `length` bytes whose bytes all follow from its generation, written in its first four}
    private static byte[] stamped(int generation, int length) {
        var value = new byte[length];
        MemorySegment.ofArray(value).set(ValueLayout.JAVA_INT_UNALIGNED, 0, generation);
        MemorySegment.ofArray(value).set(ValueLayout.JAVA_INT_UNALIGNED, 4, length);
        for (var i = 8; i < length; i++) {
            value[i] = (byte) (generation * 131 + i);
        }
        return value;
    }

    private static void checkStamped(byte[] value) {
        var generation = MemorySegment.ofArray(value).get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        var length = MemorySegment.ofArray(value).get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        if (length != value.length) {
            throw new AssertionError("generation " + generation + " read " + value.length + " bytes of " + length);
        }
        for (var i = 8; i < length; i++) {
            if (value[i] != (byte) (generation * 131 + i)) {
                throw new AssertionError("generation " + generation + " differs at byte " + i);
            }
        }
    }

    static Stream<Arguments> crashes() {
        return IntStream.range(0, 24).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0 ? 4_096L : Long.MAX_VALUE,
                seed % 3 == 0 ? "process crash" : "power loss"));
    }

    @ParameterizedTest(name = "seed {0}, large values from {1} bytes, {2}")
    @MethodSource("crashes")
    @DisplayName("reads after a crash return the values of a state between the last durable commit and the last visible one")
    void crash(int seed, long largeValues, String failure) throws Exception {
        var dry = CrashFileSystem.create();
        new CrashWorkload(seed, largeValues).run(dry.getPath("/store"));
        var disk = CrashFileSystem.create();
        var random = new Random(seed * 7919L + 3);
        if (random.nextBoolean()) {
            disk.stopAfter(random.nextLong(dry.changes() + 1));
        } else {
            disk.stopAtForce(random.nextLong(dry.forces() + 1));
        }
        var workload = new CrashWorkload(seed, largeValues);
        workload.run(disk.getPath("/store"));
        var survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
        int match;
        try (var ledger = CrashWorkload.open(survivor.getPath("/store"), largeValues)) {
            match = workload.match(ledger);
            if (match < 0) {
                fail("The reads match no state from commit " + workload.durable + " to " + workload.visible);
            }
            ledger.checkpoint();
            assertTrue(workload.matches(ledger, workload.history.get(match)), "a checkpoint changed what the reads return");
        }
        try (var ledger = CrashWorkload.open(survivor.getPath("/store"), largeValues)) {
            assertTrue(workload.matches(ledger, workload.history.get(match)), "the tree reads return other values");
        }
    }

    /// A workload of byte array values of every size, drawn from a seed, and the state after each of its commits.
    private static final class CrashWorkload {
        private static final int[] LENGTHS = {0, 60, 5_000, LARGE, HUGE};
        private static final int KEYS = 6;
        private final Random random;
        private final long largeValues;
        // The value of each key after each commit, by the seed of its bytes
        final List<Map<String, Integer>> history;
        int visible;
        int durable;

        CrashWorkload(int seed, long largeValues) {
            this.random = new Random(seed);
            this.largeValues = largeValues;
            this.history = new ArrayList<>();
            history.add(Map.of());
        }

        static Ledger open(Path directory, long largeValues) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(FILES)
                    .largeValues(largeValues))
                    .segmentSize(1 << 20)
                    .build();
        }

        static byte[] valueOf(int seed) {
            return value(seed, LENGTHS[Math.floorMod(seed, LENGTHS.length)]);
        }

        void run(Path directory) {
            Ledger ledger = null;
            try {
                ledger = open(directory, largeValues);
                for (var step = 0; step < 24; step++) {
                    var choice = random.nextInt(12);
                    if (choice == 0) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (choice == 1) {
                        ledger.close();
                        durable = visible;
                        ledger = open(directory, largeValues);
                    } else {
                        var next = new HashMap<>(history.get(visible));
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 1 + random.nextInt(3); i++) {
                            var key = "k" + random.nextInt(KEYS);
                            if (random.nextInt(6) == 0) {
                                batch.delete(FILES, key);
                                next.remove(key);
                            } else {
                                var seed = random.nextInt(1_000_000);
                                batch.put(FILES, key, valueOf(seed));
                                next.put(key, seed);
                            }
                        }
                        var commit = ledger.commit(batch);
                        history.add(next);
                        visible = history.size() - 1;
                        if (choice < 6) {
                            commit.awaitDurable();
                            durable = visible;
                        }
                    }
                }
                ledger.close();
                durable = visible;
            } catch (RuntimeException | InterruptedException _) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }

        /// {@return the first commit from the last durable one to the last visible one whose state the reads of
        /// `ledger` return, or -1}
        int match(Ledger ledger) {
            for (var commit = durable; commit <= visible; commit++) {
                if (matches(ledger, history.get(commit))) {
                    return commit;
                }
            }
            return -1;
        }

        boolean matches(Ledger ledger, Map<String, Integer> state) {
            if (ledger.size(FILES) != state.size()) {
                return false;
            }
            for (var i = 0; i < KEYS; i++) {
                var key = "k" + i;
                var read = ledger.get(FILES, key);
                var seed = state.get(key);
                if (seed == null ? read.isPresent() : read.isEmpty() || !Arrays.equals(valueOf(seed), read.get())) {
                    return false;
                }
            }
            return true;
        }
    }
}
