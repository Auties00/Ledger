package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Values of a G1 region or more that a batch keeps in two arrays, a head of whole regions and a short tail, until a
/// checkpoint: which values split, what reads, checkpoints, recovery and a commit that failed see of them, and how
/// concurrent writers, readers and checkpoints share them. Every test forces a region of 1 MiB, whatever the JVM runs.
class SplitValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final long REGION = 1L << 20;
    private static final int EXACT = 2 << 20;
    private static final int ODD = (3 << 20) + 1_000;
    // Its tail would be half a region: kept on one array
    private static final int WIDE_TAIL = (3 << 20) / 2;
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    // Values below 8 MiB stay in the log, so none is split
    private static final Keyspace<String, byte[]> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());

    @BeforeEach
    void forceRegion() {
        SplitValue.forceRegionForTests(REGION);
    }

    @AfterEach
    void releaseRegion() {
        SplitValue.forceRegionForTests(-1);
    }

    private static Ledger.Options options(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(FILES).keyspace(NOTES).keyspace(SESSIONS).keyspace(TEXTS).largeValues(NOTES, 8L << 20);
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static Version head(Ledger ledger, Keyspace<?, ?> keyspace, String key) {
        return ((LiveLedger) ledger).state(keyspace).chain(MemorySegment.ofArray(key.getBytes())).head;
    }

    private static void assertSplit(Version version, int length) {
        var split = assertInstanceOf(SplitValue.class, version.sourceForTests());
        assertEquals(SplitValue.headLength(length, REGION), split.head.length);
        assertEquals(length, split.length());
        assertEquals(length, version.sourceBytesForTests());
        assertEquals(length, version.valueLength());
        // The head and its header fill whole regions, and the tail is an ordinary object
        assertEquals(0, (split.head.length + ValueSlab.HEADROOM) % REGION);
        assertTrue(split.tail.length < REGION / 2);
    }

    private static void assertReads(Ledger ledger, String key, byte[] expected) {
        assertArrayEquals(expected, ledger.get(FILES, key).orElseThrow());
        assertArrayEquals(expected, ledger.readEncoded(FILES, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        var ranged = ledger.range(FILES, KeyRange.between(key, key + "\0"), 2);
        assertEquals(1, ranged.size());
        assertArrayEquals(expected, ranged.getFirst().value());
        var encoded = new ArrayList<byte[]>();
        ledger.forEachEncoded(FILES, KeyRange.between(key, key + "\0"), (_, value) -> encoded.add(value.toArray(ValueLayout.JAVA_BYTE)));
        assertEquals(1, encoded.size());
        assertArrayEquals(expected, encoded.getFirst());
    }

    @Nested
    @DisplayName("the head of a split value")
    class Heads {
        @ParameterizedTest(name = "{0} bytes in regions of {1}: head of {2}")
        @CsvSource({
                // A value of whole regions: the head leaves its header and headroom to the tail
                "2097152, 1048576, 2097088",
                "16777216, 2097152, 16777152",
                "3146728, 1048576, 3145664",
                // A value whose one array fills its last region already gains nothing
                "2097088, 1048576, 0",
                // Tails of half a region or more, or of more than a thirty-second of the value
                "1572864, 1048576, 0",
                "1114112, 1048576, 0",
                "1081344, 1048576, 1048512",
                // Below a region, or no region known
                "1048575, 1048576, 0",
                "16777216, 0, 0",
                "16777216, 64, 0"})
        @DisplayName("fills whole regions, and is 0 where splitting gains nothing")
        void lengths(long length, long region, int head) {
            assertEquals(head, SplitValue.headLength(length, region));
        }

        @Test
        @DisplayName("joins back into the value")
        void joins() {
            var value = value(1, ODD);
            var head = SplitValue.headLength(ODD, REGION);
            var split = new SplitValue(Arrays.copyOf(value, head), Arrays.copyOfRange(value, head, ODD));
            assertArrayEquals(value, split.join());
            assertEquals(ODD, split.length());
        }
    }

    @Nested
    @DisplayName("a batch")
    class Batch {
        private ArrayWriteBatch batch(long splitFrom) {
            return new ArrayWriteBatch(new Object(), new HeapPool(1 << 24, false), Map.of(FILES.name(), FILES, TEXTS.name(), TEXTS),
                    keyspace -> keyspace == FILES ? splitFrom : Long.MAX_VALUE);
        }

        @Test
        @DisplayName("splits a large value into arrays it alone holds, refuses a view of it, and counts it whole")
        void splits() {
            var batch = batch(1L << 20);
            var caller = value(2, ODD);
            var expected = caller.clone();
            batch.put(FILES, "a", caller);
            caller[0]++;
            caller[ODD - 1]++;
            assertEquals(1 + ODD, batch.encodedBytes());
            assertEquals(ODD, batch.valueBytes(0));
            assertEquals(ODD, batch.largestValueBytes());
            var head = batch.ownValue(0);
            var tail = batch.ownTail(0);
            assertEquals(SplitValue.headLength(ODD, REGION), head.length);
            assertArrayEquals(expected, SplitValue.join(head, tail));
            assertThrows(IllegalStateException.class, () -> batch.value(0));
            assertThrows(IllegalStateException.class, () -> batch.valueArray(0));
            batch.clear();
            batch.put(FILES, "b", value(3, 100));
            assertNull(batch.ownTail(0));
        }

        @Test
        @DisplayName("splits an encoded value from whichever memory it is on")
        void splitsEncoded() {
            var expected = value(4, EXACT);
            try (var arena = Arena.ofConfined()) {
                var native_ = arena.allocate(EXACT);
                MemorySegment.copy(expected, 0, native_, ValueLayout.JAVA_BYTE, 0, EXACT);
                var padded = new byte[EXACT + 10];
                System.arraycopy(expected, 0, padded, 5, EXACT);
                var sources = List.of(MemorySegment.ofArray(expected), MemorySegment.ofArray(expected).asReadOnly(), native_,
                        MemorySegment.ofArray(padded).asSlice(5, EXACT));
                for (var source : sources) {
                    var batch = batch(1L << 20);
                    batch.putEncoded(FILES, MemorySegment.ofArray(new byte[]{1}), source);
                    assertArrayEquals(expected, SplitValue.join(batch.ownValue(0), batch.ownTail(0)), source.toString());
                }
            }
        }

        @Test
        @DisplayName("keeps one array below the threshold, for a key space that never splits, or without a region")
        void keepsWhole() {
            var below = batch(4L << 20);
            below.put(FILES, "a", value(5, ODD));
            assertNull(below.ownTail(0));
            var other = batch(1L << 20);
            other.put(TEXTS, "a", "x".repeat(ODD));
            assertNull(other.ownTail(0));
            SplitValue.forceRegionForTests(0);
            var none = batch(1L << 20);
            none.put(FILES, "a", value(6, ODD));
            assertNull(none.ownTail(0));
            assertEquals(ODD, none.valueArray(0).length);
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("splits a large value of a paged key space of byte arrays, which every read, a snapshot, a checkpoint and a reopen return whole")
        void splits(boolean encrypted, @TempDir Path directory) throws Exception {
            var exact = value(10, EXACT);
            var odd = value(11, ODD);
            try (var ledger = options(directory, encrypted).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "exact", exact)).awaitDurable();
                ledger.commit(ledger.newBatch().putEncoded(FILES, MemorySegment.ofArray("odd".getBytes()), MemorySegment.ofArray(odd)))
                        .awaitDurable();
                assertSplit(head(ledger, FILES, "exact"), EXACT);
                assertSplit(head(ledger, FILES, "odd"), ODD);
                assertReads(ledger, "exact", exact);
                assertReads(ledger, "odd", odd);
                try (var snapshot = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().put(FILES, "exact", odd)).awaitDurable();
                    assertArrayEquals(exact, snapshot.get(FILES, "exact").orElseThrow());
                    ledger.checkpoint();
                    assertArrayEquals(exact, snapshot.get(FILES, "exact").orElseThrow());
                }
                assertReads(ledger, "exact", odd);
                assertReads(ledger, "odd", odd);
                ledger.commit(ledger.newBatch().put(FILES, "late", exact)).awaitDurable();
            }
            CheckpointTest.verifyPages(directory, encrypted);
            try (var ledger = options(directory, encrypted).build()) {
                assertReads(ledger, "exact", odd);
                assertReads(ledger, "odd", odd);
                assertReads(ledger, "late", exact);
            }
        }

        @Test
        @DisplayName("replays a split value from its unit after a close without a checkpoint")
        void replays(@TempDir Path directory) throws Exception {
            var exact = value(12, EXACT);
            var ledger = options(directory, true).build();
            ledger.commit(ledger.newBatch().put(FILES, "exact", exact).put(FILES, "small", new byte[]{1})).awaitDurable();
            assertSplit(head(ledger, FILES, "exact"), EXACT);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            try (var reopened = options(directory, true).build()) {
                assertReads(reopened, "exact", exact);
                assertArrayEquals(new byte[]{1}, reopened.get(FILES, "small").orElseThrow());
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("applies a split value in a batch that removes a range, which runs alone")
        void alone(@TempDir Path directory) throws Exception {
            var exact = value(13, EXACT);
            try (var ledger = options(directory, true).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "a", new byte[]{1}).put(FILES, "b", new byte[]{2})).awaitDurable();
                ledger.commit(ledger.newBatch().deleteRange(FILES, KeyRange.between("a", "c")).put(FILES, "c", exact)).awaitDurable();
                assertSplit(head(ledger, FILES, "c"), EXACT);
                assertTrue(ledger.get(FILES, "a").isEmpty());
                assertReads(ledger, "c", exact);
                ledger.checkpoint();
                assertReads(ledger, "c", exact);
            }
        }

        @Test
        @DisplayName("keeps one array for a value it does not split: a wide tail, the log, a resident or decoded key space, a transaction")
        void keepsWhole(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory, true).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "wide", value(14, WIDE_TAIL)).put(NOTES, "note", value(15, ODD))
                        .put(SESSIONS, "session", value(16, ODD)).put(TEXTS, "text", "y".repeat(ODD))).awaitDurable();
                ledger.update(transaction -> transaction.put(FILES, "transaction", value(17, ODD))).awaitDurable();
                for (var keyspace : List.<Keyspace<String, ?>>of(FILES, NOTES, SESSIONS, TEXTS)) {
                    for (var key : List.of("wide", "note", "session", "text", "transaction")) {
                        var chain = ((LiveLedger) ledger).state(keyspace).chain(MemorySegment.ofArray(key.getBytes()));
                        if (chain != null && chain.head != null) {
                            assertFalse(chain.head.sourceForTests() instanceof SplitValue, keyspace.name() + " " + key);
                        }
                    }
                }
                assertArrayEquals(value(14, WIDE_TAIL), ledger.get(FILES, "wide").orElseThrow());
                assertArrayEquals(value(15, ODD), ledger.get(NOTES, "note").orElseThrow());
                assertArrayEquals(value(16, ODD), ledger.get(SESSIONS, "session").orElseThrow());
                assertEquals("y".repeat(ODD), ledger.get(TEXTS, "text").orElseThrow());
                assertArrayEquals(value(17, ODD), ledger.get(FILES, "transaction").orElseThrow());
            }
            try (var memory = Ledger.builder().memory().keyspace(FILES).build()) {
                memory.commit(memory.newBatch().put(FILES, "memory", value(18, ODD))).awaitDurable();
                assertArrayEquals(value(18, ODD), memory.get(FILES, "memory").orElseThrow());
                assertFalse(head(memory, FILES, "memory").sourceForTests() instanceof SplitValue);
            }
        }

        @Test
        @DisplayName("a commit that fails after writing the unit of a split value leaves the batch to retry, and its pages free")
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
            var odd = value(19, ODD);
            try (var ledger = options(directory, true).keyspace(picky).build()) {
                var batch = ledger.newBatch().put(FILES, "odd", odd)
                        .putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(FILES, "odd").isEmpty());
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                fails.set(false);
                ledger.commit(batch).awaitDurable();
                assertSplit(head(ledger, FILES, "odd"), ODD);
                assertReads(ledger, "odd", odd);
                ledger.checkpoint();
                assertReads(ledger, "odd", odd);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("recovers, wherever the power fails, the split values of the commits from the last durable one to the last visible one")
        void crashes(boolean encrypted) throws Exception {
            var dry = CrashFileSystem.create();
            new CrashWorkload(encrypted).run(dry.getPath("/store"));
            for (var force = 0; force <= dry.forces(); force++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(force);
                var workload = new CrashWorkload(encrypted);
                workload.run(disk.getPath("/store"));
                var survivor = disk.afterPowerLoss(new Random(force));
                try (var ledger = options(survivor.getPath("/store"), encrypted).build()) {
                    var recovered = 0;
                    while (recovered < CrashWorkload.COMMITS && ledger.get(FILES, "k" + recovered).isPresent()) {
                        assertArrayEquals(CrashWorkload.value(recovered), ledger.get(FILES, "k" + recovered).orElseThrow(), "k" + recovered);
                        recovered++;
                    }
                    for (var later = recovered; later < CrashWorkload.COMMITS; later++) {
                        assertTrue(ledger.get(FILES, "k" + later).isEmpty(), "k" + later + " after a gap at force " + force);
                    }
                    assertTrue(recovered >= workload.durable && recovered <= workload.visible,
                            recovered + " recovered, durable " + workload.durable + ", visible " + workload.visible + ", force " + force);
                    // The store keeps working on what it recovered
                    ledger.commit(ledger.newBatch().put(FILES, "after", value(20, EXACT))).awaitDurable();
                    ledger.checkpoint();
                    assertArrayEquals(value(20, EXACT), ledger.get(FILES, "after").orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("serves concurrent writers, readers and checkpoints whole values, split or not")
        void concurrent(@TempDir Path directory) throws Exception {
            var writers = 4;
            var rounds = 6;
            var lengths = new int[]{EXACT, EXACT + 3_000, WIDE_TAIL, 200_000};
            // Each value by its first 16 bytes, which a reader checks the rest against
            var known = new ConcurrentHashMap<String, byte[]>();
            var failure = new AtomicReference<Throwable>();
            try (var ledger = options(directory, true).build()) {
                var start = new CountDownLatch(1);
                var running = new AtomicBoolean(true);
                var threads = new ArrayList<Thread>();
                for (var writer = 0; writer < writers; writer++) {
                    var id = writer;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var round = 0; round < rounds; round++) {
                                var seed = id * 1_000 + round;
                                var written = value(seed, lengths[(id + round) % lengths.length]);
                                known.put(Arrays.toString(Arrays.copyOf(written, 16)), written);
                                var commit = ledger.commit(ledger.newBatch().put(FILES, "w" + id + "-" + round % 2, written));
                                if (round % 2 == 0) {
                                    commit.awaitDurable();
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
                for (var reader = 0; reader < 2; reader++) {
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            var random = new Random();
                            while (running.get()) {
                                var key = "w" + random.nextInt(writers) + "-" + random.nextInt(2);
                                var read = random.nextBoolean()
                                        ? ledger.get(FILES, key).orElse(null)
                                        : ledger.readEncoded(FILES, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElse(null);
                                if (read != null) {
                                    var expected = known.get(Arrays.toString(Arrays.copyOf(read, 16)));
                                    if (expected == null || !Arrays.equals(expected, read)) {
                                        throw new AssertionError("a read of " + key + " returned a value no writer wrote");
                                    }
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
                var checkpoints = Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        while (running.get()) {
                            ledger.checkpoint();
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                });
                start.countDown();
                for (var writer = 0; writer < writers; writer++) {
                    threads.get(writer).join();
                }
                running.set(false);
                for (var thread : threads) {
                    thread.join();
                }
                checkpoints.join();
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                var expected = new HashMap<String, byte[]>();
                for (var writer = 0; writer < writers; writer++) {
                    for (var round = 0; round < rounds; round++) {
                        expected.put("w" + writer + "-" + round % 2, value(writer * 1_000 + round, lengths[(writer + round) % lengths.length]));
                    }
                }
                for (var entry : expected.entrySet()) {
                    assertArrayEquals(entry.getValue(), ledger.get(FILES, entry.getKey()).orElseThrow(), entry.getKey());
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    /// Commits a split value per commit, the second half of them without waiting for durability, until the file
    /// system stops.
    private static final class CrashWorkload {
        static final int COMMITS = 4;
        private final boolean encrypted;
        // The commits acknowledged durable, and those made visible
        int durable;
        int visible;

        CrashWorkload(boolean encrypted) {
            this.encrypted = encrypted;
        }

        static byte[] value(int index) {
            return SplitValuesTest.value(100 + index, index % 2 == 0 ? EXACT : ODD);
        }

        void run(Path directory) throws InterruptedException {
            Ledger ledger;
            try {
                ledger = options(directory, encrypted).build();
            } catch (RuntimeException _) {
                return;
            }
            try {
                for (var index = 0; index < COMMITS; index++) {
                    // A commit that throws may still have reached the log
                    visible = index + 1;
                    var commit = ledger.commit(ledger.newBatch().put(FILES, "k" + index, value(index)));
                    if (index < COMMITS / 2) {
                        commit.awaitDurable();
                        durable = index + 1;
                    }
                }
                ledger.close();
                durable = visible;
            } catch (RuntimeException _) {
                // The file system stopped where it was told to
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // It stays stopped
                }
            }
        }
    }
}
