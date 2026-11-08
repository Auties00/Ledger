package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    @DisplayName("forces")
    class Forces {
        @Test
        @DisplayName("ask for no metadata of the file, in commits, checkpoints and the recovery after a crash")
        void noMetadata() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var forces = new AtomicLong();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.endsWith(BlobFile.FILE)) {
                    forces.incrementAndGet();
                }
            });
            CrashFileSystem survivor;
            try (var ledger = open(directory, false)) {
                ledger.commit(ledger.newBatch().put(FILES, "a", value(1, 50_000))).awaitDurable();
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(FILES, "b", value(2, 50_000))).awaitDurable();
                survivor = disk.afterPowerLoss(new Random(1));
            }
            assertTrue(forces.get() >= 3, () -> "forces of the file: " + forces);
            assertEquals(0, disk.metadataForces(directory.resolve(BlobFile.FILE)));
            var recovered = survivor.getPath("/store");
            try (var ledger = open(recovered, false)) {
                assertArrayEquals(value(2, 50_000), ledger.get(FILES, "b").orElseThrow());
            }
            assertEquals(0, survivor.metadataForces(recovered.resolve(BlobFile.FILE)));
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(longs = {1, 2, 3})
        @DisplayName("keep a durable value through a loss of power, whether its unit grows the file, fills freed pages, or grows a file a checkpoint cut")
        void powerLoss(long seed) throws Exception {
            var random = new Random(seed);
            var disk = CrashFileSystem.create();
            var kept = new HashMap<String, byte[]>();

            kept.put("grown", value(1, 1 << 20));
            CrashFileSystem survivor;
            try (var ledger = open(disk.getPath("/store"), false)) {
                ledger.commit(ledger.newBatch().put(FILES, "grown", kept.get("grown"))).awaitDurable();
                survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            }
            assertHolds(survivor, kept);

            disk = survivor;
            var blobs = disk.getPath("/store", BlobFile.FILE);
            try (var ledger = open(disk.getPath("/store"), false)) {
                // Replaces the unit, frees the old one, and takes its pages with no change of length
                kept.put("grown", value(2, 1 << 20));
                ledger.commit(ledger.newBatch().put(FILES, "grown", kept.get("grown"))).awaitDurable();
                settle(ledger);
                var length = Files.size(blobs);
                kept.put("refilled", value(3, 1 << 20));
                ledger.commit(ledger.newBatch().put(FILES, "refilled", kept.get("refilled"))).awaitDurable();
                assertEquals(length, Files.size(blobs), "the unit took new pages");
                survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            }
            assertHolds(survivor, kept);

            disk = survivor;
            blobs = disk.getPath("/store", BlobFile.FILE);
            try (var ledger = open(disk.getPath("/store"), false)) {
                // More than the 64 MiB of free pages from which a checkpoint cuts the file
                var bulk = value(4, 16 << 20);
                var batch = ledger.newBatch();
                var removal = ledger.newBatch();
                for (var i = 0; i < 5; i++) {
                    batch.put(FILES, "bulk" + i, bulk);
                    removal.delete(FILES, "bulk" + i);
                }
                ledger.commit(batch).awaitDurable();
                ledger.checkpoint();
                var longest = Files.size(blobs);
                ledger.commit(removal).awaitDurable();
                settle(ledger);
                var cut = Files.size(blobs);
                assertTrue(cut < longest, "the checkpoint kept the length of the file");
                // Larger than the free pages left below the end of the file
                kept.put("regrown", value(5, 4 << 20));
                ledger.commit(ledger.newBatch().put(FILES, "regrown", kept.get("regrown"))).awaitDurable();
                assertTrue(Files.size(blobs) > cut, "the unit did not grow the file");
                survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            }
            assertHolds(survivor, kept);
            CheckpointTest.verifyPages(survivor.getPath("/store"), false);
        }

        @Test
        @DisplayName("keep the values of commits forced beside checkpoints, by several threads, through a loss of power")
        void concurrentForces() throws Exception {
            var disk = CrashFileSystem.create();
            var acknowledged = new ConcurrentHashMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(disk.getPath("/store"), false);
                 var threads = Executors.newVirtualThreadPerTaskExecutor()) {
                var stop = new AtomicBoolean();
                var checkpoints = threads.submit(() -> {
                    while (!stop.get()) {
                        ledger.checkpoint();
                        Thread.yield();
                    }
                    return null;
                });
                var writers = new ArrayList<Future<?>>();
                for (var writer = 0; writer < 4; writer++) {
                    var id = writer;
                    writers.add(threads.submit(() -> {
                        for (var i = 0; i < 40; i++) {
                            var key = "w" + id + "-" + (i % 8);
                            var written = value(id * 1_000 + i, 64 << 10);
                            ledger.commit(ledger.newBatch().put(FILES, key, written)).awaitDurable();
                            acknowledged.put(key, written);
                        }
                        return null;
                    }));
                }
                for (var writer : writers) {
                    writer.get();
                }
                stop.set(true);
                checkpoints.get();
                survivor = disk.afterPowerLoss(new Random(7));
            }
            assertEquals(4 * 8, acknowledged.size());
            assertHolds(survivor, acknowledged);
            CheckpointTest.verifyPages(survivor.getPath("/store"), false);
        }

        /// Runs the checkpoints that take a replaced unit out of the tree and then, past every reader, free its pages.
        private static void settle(Ledger ledger) throws InterruptedException {
            for (var i = 0; i < 2; i++) {
                ledger.commit(ledger.newBatch().put(NOTES, "settled", new byte[]{(byte) i})).awaitDurable();
                ledger.checkpoint();
            }
        }

        private static void assertHolds(CrashFileSystem disk, Map<String, byte[]> values) {
            try (var ledger = open(disk.getPath("/store"), false)) {
                for (var entry : values.entrySet()) {
                    assertArrayEquals(entry.getValue(), ledger.get(FILES, entry.getKey()).orElseThrow(), entry.getKey());
                }
            }
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
                    var unit = ledger.blobsForTests().write(java.lang.foreign.MemorySegment.ofArray(value(2, 5_000)));
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
}
