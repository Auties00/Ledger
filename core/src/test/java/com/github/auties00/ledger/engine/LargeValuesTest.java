package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The file of large values: values a batch writes once, at commit, which checkpoints take without a copy and
/// recovery reads back with the commits it replays. A store without encryption lets the tests find where a value's
/// bytes went.
class LargeValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.bytes());

    private static Ledger.Options options(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(FILES).keyspace(NOTES).largeValues(4_096);
    }

    private static Ledger open(Path directory, boolean encrypted) {
        return options(directory, encrypted).build();
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    /// {@return whether a file under `directory` named like `name` holds the first 64 bytes of `value`}
    private static boolean holds(Path directory, String name, byte[] value) throws IOException {
        var pattern = java.util.Arrays.copyOf(value, 64);
        try (var files = Files.walk(directory)) {
            for (var file : files.filter(Files::isRegularFile).filter(path -> path.toString().contains(name)).toList()) {
                var bytes = Files.readAllBytes(file);
                for (var i = 0; i + pattern.length <= bytes.length; i++) {
                    if (java.util.Arrays.equals(bytes, i, i + pattern.length, pattern, 0, pattern.length)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Nested
    @DisplayName("writes")
    class Writes {
        @Test
        @DisplayName("write a large value once, to its own file: neither the log nor a checkpoint copies it")
        void once(@TempDir Path directory) throws Exception {
            var value = value(1, 1 << 20);
            try (var ledger = open(directory, false)) {
                ledger.commit(ledger.newBatch().put(FILES, "big", value)).awaitDurable();
                ledger.checkpoint();
            }
            assertTrue(holds(directory, BlobFile.FILE, value), "the file of large values does not hold the value");
            assertFalse(holds(directory, ".log", value), "the log holds the value");
            assertFalse(holds(directory, PageFile.FILE, value), "the data file holds the value");
            try (var ledger = open(directory, false)) {
                assertArrayEquals(value, ledger.get(FILES, "big").orElseThrow());
            }
            CheckpointTest.verifyPages(directory, false);
        }

        @Test
        @DisplayName("keep values below the threshold, and those of a key space with a threshold of its own, in the log")
        void thresholds(@TempDir Path directory) throws Exception {
            var below = value(2, 4_095);
            var excluded = value(3, 100_000);
            try (var ledger = options(directory, false).largeValues(NOTES, Long.MAX_VALUE).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "below", below).put(NOTES, "excluded", excluded)).awaitDurable();
            }
            assertFalse(holds(directory, BlobFile.FILE, below));
            assertFalse(holds(directory, BlobFile.FILE, excluded));
            try (var ledger = open(directory, false)) {
                assertArrayEquals(below, ledger.get(FILES, "below").orElseThrow());
                assertArrayEquals(excluded, ledger.get(NOTES, "excluded").orElseThrow());
            }
        }

        @Test
        @DisplayName("refuse a threshold below a page")
        void smallThreshold(@TempDir Path directory) {
            assertThrows(IllegalArgumentException.class, () -> Ledger.builder().directory(directory).encryption(Encryption.none()).largeValues(4_095));
            assertThrows(IllegalArgumentException.class, () -> Ledger.builder().directory(directory).encryption(Encryption.none()).largeValues(FILES, 100));
        }
    }

    @Nested
    @DisplayName("space")
    class Space {
        @Test
        @DisplayName("free the units of values replaced or removed before a checkpoint took them")
        void superseded(@TempDir Path directory) throws Exception {
            for (var encrypted : new boolean[]{false, true}) {
                var store = directory.resolve(encrypted ? "encrypted" : "plain");
                var kept = value(4, 50_000);
                try (var ledger = open(store, encrypted)) {
                    ledger.commit(ledger.newBatch().put(FILES, "a", value(5, 60_000)));
                    ledger.commit(ledger.newBatch().put(FILES, "a", kept).put(FILES, "b", value(6, 70_000)));
                    ledger.commit(ledger.newBatch().delete(FILES, "b").put(FILES, "c", value(7, 9_000)));
                    ledger.commit(ledger.newBatch().deleteRange(FILES, com.github.auties00.ledger.KeyRange.between("c", "d")));
                    ledger.checkpoint();
                    var blobs = ((LiveLedger) ledger).blobsForTests();
                    assertEquals(0, blobs.unadoptedForTests(), "units in flight after the checkpoint");
                }
                CheckpointTest.verifyPages(store, encrypted);
                try (var ledger = open(store, encrypted)) {
                    assertArrayEquals(kept, ledger.get(FILES, "a").orElseThrow());
                    assertEquals(Optional.empty(), ledger.get(FILES, "b"));
                    assertEquals(Optional.empty(), ledger.get(FILES, "c"));
                    assertEquals(1, ledger.size(FILES));
                }
            }
        }

        @Test
        @DisplayName("keep the unit a snapshot's tree points to until the snapshot closes")
        void snapshots(@TempDir Path directory) throws Exception {
            var first = value(8, 40_000);
            try (var ledger = open(directory, true)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", first));
                ledger.checkpoint();
                try (var snapshot = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().put(FILES, "a", value(9, 40_000)));
                    ledger.checkpoint();
                    // Values that would land in the first one's pages, were they free
                    for (var round = 0; round < 6; round++) {
                        ledger.commit(ledger.newBatch().put(FILES, "x" + round, value(10 + round, 40_000)));
                        ledger.checkpoint();
                    }
                    assertArrayEquals(first, snapshot.get(FILES, "a").orElseThrow());
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        @Test
        @DisplayName("reads back the values of the commits it replays, and takes their pages again")
        void replays(@TempDir Path directory) throws Exception {
            var values = new ArrayList<byte[]>();
            var ledger = open(directory, true);
            ledger.commit(ledger.newBatch().put(FILES, "checkpointed", value(20, 30_000))).awaitDurable();
            ledger.checkpoint();
            for (var i = 0; i < 5; i++) {
                values.add(value(21 + i, 10_000 + 7_000 * i));
                ledger.commit(ledger.newBatch().put(FILES, "k" + i, values.getLast())).awaitDurable();
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            try (var reopened = open(directory, true)) {
                for (var i = 0; i < 5; i++) {
                    assertArrayEquals(values.get(i), reopened.get(FILES, "k" + i).orElseThrow(), "k" + i);
                }
                // The claimed pages are not handed to new values
                reopened.commit(reopened.newBatch().put(FILES, "new", value(30, 50_000))).awaitDurable();
                for (var i = 0; i < 5; i++) {
                    assertArrayEquals(values.get(i), reopened.get(FILES, "k" + i).orElseThrow(), "k" + i);
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("ends the log at a last commit whose unit the file does not hold, as at a torn frame")
        void lostUnit(@TempDir Path directory) throws Exception {
            var lost = value(40, 20_000);
            var ledger = open(directory, false);
            ledger.commit(ledger.newBatch().put(NOTES, "kept", "before".getBytes())).awaitDurable();
            ledger.commit(ledger.newBatch().put(FILES, "lost", lost)).awaitDurable();
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            damageUnits(directory);
            try (var reopened = open(directory, false)) {
                assertArrayEquals("before".getBytes(), reopened.get(NOTES, "kept").orElseThrow());
                assertEquals(Optional.empty(), reopened.get(FILES, "lost"));
                assertEquals(0, reopened.size(FILES));
                reopened.commit(reopened.newBatch().put(FILES, "after", value(41, 30_000))).awaitDurable();
            }
            CheckpointTest.verifyPages(directory, false);
        }

        @Test
        @DisplayName("refuses a log whose missing unit a later group follows: that commit was acknowledged")
        void damagedUnit(@TempDir Path directory) throws Exception {
            var ledger = open(directory, false);
            ledger.commit(ledger.newBatch().put(FILES, "damaged", value(50, 20_000))).awaitDurable();
            ledger.commit(ledger.newBatch().put(NOTES, "later", "after".getBytes())).awaitDurable();
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            damageUnits(directory);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, false));
        }

        @Test
        @DisplayName("gives every open an epoch above the earlier ones, across crashes and checkpoints")
        void epochs(@TempDir Path directory) {
            var last = -1;
            for (var round = 0; round < 6; round++) {
                var ledger = (LiveLedger) open(directory, true);
                var epoch = ledger.blobsForTests().epoch();
                assertTrue(Integer.compareUnsigned(epoch, last) > 0 || last == -1, "epoch " + epoch + " after " + last);
                last = epoch;
                ledger.commit(ledger.newBatch().put(FILES, "r" + round, value(60 + round, 5_000)));
                if (round % 3 == 2) {
                    ledger.close();
                } else {
                    ledger.closeWithoutCheckpoint();
                }
            }
            try (var ledger = open(directory, true)) {
                for (var round = 0; round < 6; round++) {
                    assertArrayEquals(value(60 + round, 5_000), ledger.get(FILES, "r" + round).orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("gives the next opens a new epoch after a unit was written without a commit and nothing was checkpointed")
        void epochOfLostUnit(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, true)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", value(1, 10))).awaitDurable();
            }
            for (var round = 0; round < 2; round++) {
                var ledger = (LiveLedger) open(directory, true);
                int used;
                try {
                    // What a commit that failed after writing its units, or one a crash cut off, leaves
                    used = ledger.blobsForTests().epoch();
                    var unit = ledger.blobsForTests().write(value(2, 5_000), 0, 5_000);
                    ledger.blobsForTests().release(unit);
                } finally {
                    if (round == 0) {
                        ledger.close();
                    } else {
                        ledger.closeWithoutCheckpoint();
                    }
                }
                // An open in between that commits nothing must not forget the epoch either
                open(directory, true).close();
                try (var reopened = (LiveLedger) open(directory, true)) {
                    var epoch = reopened.blobsForTests().epoch();
                    assertTrue(Integer.compareUnsigned(epoch, used) > 0, "epoch " + epoch + " after " + used);
                    assertArrayEquals(value(1, 10), reopened.get(FILES, "a").orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("an interrupted reader or committer reads and writes its large value, and leaves the file usable for the others")
        void interruptedCallers(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) open(directory, true)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", value(1, 50_000))).awaitDurable();
                ledger.clearPageCacheForTests();
                // An interrupted thread's file operations close their channel at once
                Thread.currentThread().interrupt();
                assertArrayEquals(value(1, 50_000), ledger.get(FILES, "a").orElseThrow());
                assertTrue(Thread.interrupted(), "the reader lost its interrupt");
                Thread.currentThread().interrupt();
                var commit = ledger.commit(ledger.newBatch().put(FILES, "b", value(2, 50_000)));
                assertTrue(Thread.interrupted(), "the committer lost its interrupt");
                commit.awaitDurable();
                // A checkpoint forces the file through a channel no caller's interrupt closed
                Thread.currentThread().interrupt();
                ledger.checkpoint();
                assertTrue(Thread.interrupted(), "the checkpoint lost the interrupt");
                ledger.clearPageCacheForTests();
                assertArrayEquals(value(2, 50_000), ledger.get(FILES, "b").orElseThrow());
            }
            try (var ledger = open(directory, true)) {
                assertArrayEquals(value(1, 50_000), ledger.get(FILES, "a").orElseThrow());
                assertArrayEquals(value(2, 50_000), ledger.get(FILES, "b").orElseThrow());
            }
        }

        /// Overwrites every unit page of the file of large values with zeros.
        private static void damageUnits(Path directory) throws IOException {
            try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.WRITE)) {
                var zeros = ByteBuffer.allocate((int) Math.max(0, channel.size() - PageFormat.offset(PageFormat.FIRST_UNIT_PAGE)));
                channel.write(zeros, PageFormat.offset(PageFormat.FIRST_UNIT_PAGE));
                channel.force(true);
            }
        }
    }

    @Nested
    @DisplayName("values no checkpoint took")
    class Pending {
        private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
        // Above the pooled sizes: their units take three, four and two regions of 1 MiB, which the tests force
        private static final int LARGE = (2 << 20) + 100;
        private static final int LARGER = (3 << 20) + 100;
        private static final int SMALL_LARGE = (1 << 20) + 100;

        private static Version head(Ledger ledger, String key) {
            return ((LiveLedger) ledger).state(FILES).chain(MemorySegment.ofArray(key.getBytes())).head;
        }

        private static PageRef unit(Ledger ledger, String key) {
            return assertInstanceOf(Version.External.class, head(ledger, key)).unit;
        }

        @ParameterizedTest(name = "{0} bytes")
        @ValueSource(ints = {10_000, LARGE})
        @DisplayName("keep no copy in memory, and read the same before a checkpoint, after it and after a reopen")
        void noCopy(int length, @TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(1 << 20);
            try {
                var expected = value(90, length);
                try (var ledger = open(directory, true)) {
                    ledger.commit(ledger.newBatch().put(FILES, "v", expected)).awaitDurable();
                    assertInstanceOf(BlobFile.class, head(ledger, "v").sourceForTests());
                    assertEquals(0, head(ledger, "v").sourceBytesForTests());
                    for (var read = 0; read < 3; read++) {
                        assertArrayEquals(expected, ledger.get(FILES, "v").orElseThrow());
                        assertArrayEquals(expected, ledger.readEncoded(FILES, "v", segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
                        var visited = new ArrayList<byte[]>();
                        ledger.forEachEncoded(FILES, KeyRange.all(), (_, value) -> visited.add(value.toArray(ValueLayout.JAVA_BYTE)));
                        assertEquals(1, visited.size());
                        assertArrayEquals(expected, visited.getFirst());
                    }
                    ledger.checkpoint();
                    assertArrayEquals(expected, ledger.get(FILES, "v").orElseThrow());
                }
                try (var ledger = open(directory, true)) {
                    assertArrayEquals(expected, ledger.get(FILES, "v").orElseThrow());
                }
                CheckpointTest.verifyPages(directory, true);
            } finally {
                HeapPool.forceRegionForTests(-1);
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a snapshot reads a value a later commit replaced, after the checkpoint of that commit and new values that would reuse free pages")
        void snapshotAcrossOverwrite(boolean encrypted, @TempDir Path directory) throws Exception {
            var first = value(91, 40_000);
            try (var ledger = open(directory, encrypted)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", first)).awaitDurable();
                try (var snapshot = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().put(FILES, "a", value(92, 40_000))).awaitDurable();
                    ledger.checkpoint();
                    for (var round = 0; round < 6; round++) {
                        ledger.commit(ledger.newBatch().put(FILES, "x" + round, value(93 + round, 40_000))).awaitDurable();
                        ledger.checkpoint();
                    }
                    assertArrayEquals(first, snapshot.get(FILES, "a").orElseThrow());
                }
                assertArrayEquals(value(92, 40_000), ledger.get(FILES, "a").orElseThrow());
                // Once the snapshot closed, the replaced unit leaves the units in flight and its pages free
                ledger.commit(ledger.newBatch().put(FILES, "y", value(99, 40_000))).awaitDurable();
                ledger.checkpoint();
                assertEquals(0, ((LiveLedger) ledger).blobsForTests().unadoptedForTests());
            }
            CheckpointTest.verifyPages(directory, encrypted);
        }

        @Test
        @DisplayName("a snapshot reads a value a later range removal covered, after the checkpoint of the removal and new values")
        void snapshotAcrossRangeRemoval(@TempDir Path directory) throws Exception {
            var removed = value(100, 30_000);
            try (var ledger = open(directory, true)) {
                ledger.commit(ledger.newBatch().put(FILES, "r1", removed).put(FILES, "s", value(101, 30_000))).awaitDurable();
                try (var snapshot = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().deleteRange(FILES, KeyRange.between("r", "s"))).awaitDurable();
                    ledger.checkpoint();
                    for (var round = 0; round < 6; round++) {
                        ledger.commit(ledger.newBatch().put(FILES, "x" + round, value(102 + round, 30_000))).awaitDurable();
                        ledger.checkpoint();
                    }
                    assertArrayEquals(removed, snapshot.get(FILES, "r1").orElseThrow());
                    assertEquals(Optional.empty(), ledger.get(FILES, "r1"));
                }
                ledger.commit(ledger.newBatch().put(FILES, "y", value(110, 30_000))).awaitDurable();
                ledger.checkpoint();
                assertEquals(0, ((LiveLedger) ledger).blobsForTests().unadoptedForTests());
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("a read that found a version before a checkpoint replaced its unit opens the unit whole, though new values are written meanwhile")
        void readAcrossPublish(@TempDir Path directory) throws Exception {
            var first = value(120, 40_000);
            var second = value(121, 40_000);
            try (var ledger = (LiveLedger) open(directory, true)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", first)).awaitDurable();
                ledger.clearPageCacheForTests();
                var raced = new AtomicBoolean();
                var failure = new AtomicReference<Throwable>();
                ledger.blobsForTests().readHookForTests(() -> {
                    if (!raced.compareAndSet(false, true)) {
                        return;
                    }
                    // Between the reader's choice of the version and its read of the unit
                    var racer = Thread.ofPlatform().start(() -> {
                        try {
                            ledger.commit(ledger.newBatch().put(FILES, "a", second)).awaitDurable();
                            ledger.checkpoint();
                            for (var round = 0; round < 6; round++) {
                                ledger.commit(ledger.newBatch().put(FILES, "x" + round, value(122 + round, 40_000))).awaitDurable();
                                ledger.checkpoint();
                            }
                        } catch (Throwable throwable) {
                            failure.set(throwable);
                        }
                    });
                    try {
                        racer.join();
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException(exception);
                    }
                });
                try {
                    assertArrayEquals(first, ledger.get(FILES, "a").orElseThrow());
                } finally {
                    ledger.blobsForTests().readHookForTests(null);
                }
                assertTrue(raced.get(), "the read never went to the file");
                assertNull(failure.get(), () -> "the racing writer failed: " + failure.get());
                assertArrayEquals(second, ledger.get(FILES, "a").orElseThrow());
                ledger.commit(ledger.newBatch().put(FILES, "y", value(130, 40_000))).awaitDurable();
                ledger.checkpoint();
                assertEquals(0, ledger.blobsForTests().unadoptedForTests());
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("reads of large values the cache refuses, then takes, never cache an array the pool may hand out again, nor one of more regions than the unit needs")
        void spares(@TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(1 << 20);
            var pool = HeapPool.process();
            // The arrays above the pooled sizes the pool holds free, by identity
            var free = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>()));
            pool.watchForTests(new HeapPool.Watcher() {
                @Override
                public void acquired(byte[] array) {
                    free.remove(array);
                }

                @Override
                public void released(byte[] array) {
                    if (array.length > HeapPool.MAX_POOLED) {
                        free.add(array);
                    }
                }
            });
            try (var ledger = (LiveLedger) options(directory, true).pageCache(256L << 20).build()) {
                var values = new LinkedHashMap<String, byte[]>();
                values.put("larger", value(140, LARGER));
                values.put("large", value(141, LARGE));
                values.put("other", value(142, LARGE));
                values.put("small", value(143, SMALL_LARGE));
                var batch = ledger.newBatch();
                values.forEach((key, value) -> batch.put(FILES, key, value));
                ledger.commit(batch).awaitDurable();
                var blobs = ledger.blobsForTests();
                // The larger unit first, refused, so the spares hold arrays of more regions than the others need
                for (var round = 0; round < 4; round++) {
                    for (var entry : values.entrySet()) {
                        var key = entry.getKey();
                        assertArrayEquals(entry.getValue(), ledger.get(FILES, key).orElseThrow(), key + " at read " + round);
                        var ref = unit(ledger, key);
                        var cached = blobs.cachedForTests(ref);
                        if (cached != null) {
                            assertFalse(free.contains(cached), "the cache holds a spare of the pool for " + key);
                            var unitBytes = ref.pages() * PageFormat.SIZE;
                            assertEquals(HeapPool.largeLength(unitBytes), cached.length, key);
                            assertEquals(unitBytes, PageCache.charge(cached), key);
                        }
                    }
                }
                for (var key : values.keySet()) {
                    assertNotNull(blobs.cachedForTests(unit(ledger, key)), key + " is not cached after four reads");
                }
                assertTrue(pool.largeSparesForTests() <= HeapPool.LARGE_SPARES);
            } finally {
                pool.watchForTests(null);
                HeapPool.forceRegionForTests(-1);
            }
        }

        @Test
        @DisplayName("with regions of 8 MiB, a large value below half a region is cached in an array of its unit's length, charged as its length")
        void belowHalfRegion(@TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(8 << 20);
            try (var ledger = (LiveLedger) options(directory, true).pageCache(256L << 20).build()) {
                var value = value(144, SMALL_LARGE);
                ledger.commit(ledger.newBatch().put(FILES, "a", value)).awaitDurable();
                for (var round = 0; round < 3; round++) {
                    assertArrayEquals(value, ledger.get(FILES, "a").orElseThrow(), "read " + round);
                }
                var ref = unit(ledger, "a");
                var cached = ledger.blobsForTests().cachedForTests(ref);
                assertNotNull(cached, "not cached after three reads");
                assertEquals(ref.pages() * PageFormat.SIZE, cached.length, "an array longer than its unit");
                assertEquals(cached.length, PageCache.charge(cached));
            } finally {
                HeapPool.forceRegionForTests(-1);
            }
        }

        @Test
        @DisplayName("concurrent reads of large values the cache cannot hold copy each out of arrays of their own, whole")
        void uncached(@TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(1 << 20);
            // A quarter of the limit is below each unit, so the cache holds none
            try (var ledger = (LiveLedger) options(directory, true).pageCache(4L << 20).build()) {
                var values = List.of(value(150, LARGE), value(151, LARGER), value(152, LARGE));
                for (var i = 0; i < values.size(); i++) {
                    ledger.commit(ledger.newBatch().put(FILES, "k" + i, values.get(i))).awaitDurable();
                }
                var readers = new ArrayList<Thread>();
                var failure = new AtomicReference<Throwable>();
                for (var reader = 0; reader < 4; reader++) {
                    readers.add(Thread.ofPlatform().start(() -> {
                        try {
                            for (var round = 0; round < 6; round++) {
                                for (var i = 0; i < values.size(); i++) {
                                    assertArrayEquals(values.get(i), ledger.get(FILES, "k" + i).orElseThrow());
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.set(throwable);
                        }
                    }));
                }
                for (var reader : readers) {
                    reader.join();
                }
                assertNull(failure.get(), () -> "a reader failed: " + failure.get());
                for (var i = 0; i < values.size(); i++) {
                    assertNull(ledger.blobsForTests().cachedForTests(unit(ledger, "k" + i)), "k" + i + " is cached");
                }
            } finally {
                HeapPool.forceRegionForTests(-1);
            }
        }

        @Test
        @DisplayName("a get of a large value the cache cannot hold allocates little but the caller's copy, before a checkpoint takes its unit and after")
        void copiesOutOfSpares(@TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(1 << 20);
            var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            var length = 16 << 20;
            // A quarter of the limit is below the unit, so the cache never holds it
            try (var ledger = (LiveLedger) options(directory, true).pageCache(4L << 20).build()) {
                var expected = value(160, length);
                ledger.commit(ledger.newBatch().put(FILES, "v", expected)).awaitDurable();
                for (var checkpointed : new boolean[]{false, true}) {
                    if (checkpointed) {
                        ledger.checkpoint();
                        // The reads go to the tree's leaf, which points to the unit
                        assertNull(ledger.state(FILES).head(MemorySegment.ofArray("v".getBytes())), "a version in memory");
                    }
                    // The first read may make the pool's spares
                    for (var read = 0; read < 4; read++) {
                        var before = threads.getCurrentThreadAllocatedBytes();
                        var value = ledger.get(FILES, "v").orElseThrow();
                        var allocated = threads.getCurrentThreadAllocatedBytes() - before;
                        assertArrayEquals(expected, value);
                        if (read > 0) {
                            assertTrue(allocated - length < length / 4, allocated + " bytes allocated by read " + read
                                    + (checkpointed ? " after the checkpoint" : " before the checkpoint"));
                        }
                    }
                }
            } finally {
                HeapPool.forceRegionForTests(-1);
            }
        }

        @Test
        @DisplayName("a read of a value of a closed store fails rather than opening the file again")
        void closed(@TempDir Path directory) throws Exception {
            var ledger = (LiveLedger) open(directory, true);
            ledger.commit(ledger.newBatch().put(FILES, "a", value(160, 20_000))).awaitDurable();
            var blobs = ledger.blobsForTests();
            var ref = unit(ledger, "a");
            ledger.clearPageCacheForTests();
            ledger.close();
            assertThrows(IllegalStateException.class, () -> blobs.copyValue(ref, 20_000, PageCache.POINT));
            assertThrows(IllegalStateException.class, () -> ledger.get(FILES, "a"));
        }

        @Test
        @DisplayName("recovery applies no write of a commit one of whose units does not authenticate, and replays the earlier ones without a copy")
        void recoveryAuthenticatesEveryUnit(@TempDir Path directory) throws Exception {
            HeapPool.forceRegionForTests(1 << 20);
            try {
                var ledger = (LiveLedger) options(directory, true).keyspace(SESSIONS).build();
                var kept = value(170, LARGE);
                ledger.commit(ledger.newBatch().put(FILES, "kept", kept).put(SESSIONS, "session", value(171, 20_000))).awaitDurable();
                ledger.commit(ledger.newBatch().put(FILES, "first", value(172, LARGE)).put(FILES, "second", value(173, 30_000))
                        .put(SESSIONS, "late", value(174, 20_000))).awaitDurable();
                var damaged = unit(ledger, "second");
                ledger.closeWithoutCheckpoint();
                try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.WRITE)) {
                    channel.write(ByteBuffer.wrap(new byte[64]), PageFormat.offset(damaged.page()) + PageFormat.HEADER);
                    channel.force(true);
                }
                try (var reopened = (LiveLedger) options(directory, true).keyspace(SESSIONS).build()) {
                    assertArrayEquals(kept, reopened.get(FILES, "kept").orElseThrow());
                    assertArrayEquals(value(171, 20_000), reopened.get(SESSIONS, "session").orElseThrow());
                    assertInstanceOf(BlobFile.class, head(reopened, "kept").sourceForTests());
                    assertEquals(Optional.empty(), reopened.get(FILES, "first"));
                    assertEquals(Optional.empty(), reopened.get(FILES, "second"));
                    assertEquals(Optional.empty(), reopened.get(SESSIONS, "late"));
                    reopened.commit(reopened.newBatch().put(FILES, "after", value(175, LARGE))).awaitDurable();
                    assertArrayEquals(kept, reopened.get(FILES, "kept").orElseThrow());
                }
                CheckpointTest.verifyPages(directory, true);
            } finally {
                HeapPool.forceRegionForTests(-1);
            }
        }

        @Test
        @DisplayName("a store that overwrites its large values keeps the file of large values at a steady size")
        void steadySize(@TempDir Path directory) throws Exception {
            var slots = 8;
            var length = 64 << 10;
            try (var ledger = (LiveLedger) open(directory, true)) {
                for (var write = 0; write < 400; write++) {
                    ledger.commit(ledger.newBatch().put(FILES, "slot" + write % slots, value(write, length)));
                    if (write % 16 == 15) {
                        ledger.checkpoint();
                    }
                }
                ledger.checkpoint();
                // The live units, those of the writes since the last checkpoint, and those waiting for readers
                var unitPages = PageFormat.pagesFor(length);
                assertTrue(ledger.blobsForTests().pageCount() <= PageFormat.FIRST_UNIT_PAGE + 4L * (slots + 16) * unitPages,
                        "the file grew to " + ledger.blobsForTests().pageCount() + " pages");
                for (var slot = 0; slot < slots; slot++) {
                    assertArrayEquals(value(400 - slots + slot, length), ledger.get(FILES, "slot" + slot).orElseThrow());
                }
            }
        }
    }
}
