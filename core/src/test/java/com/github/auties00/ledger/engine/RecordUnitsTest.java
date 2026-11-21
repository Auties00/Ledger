package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Units of the file of large values of format [PageFormat#RECORDS], which a store that encrypts writes for a value
/// longer than one record: their layout in the file, reads through every path before a checkpoint, after it and after
/// the replay of the log, tampering of their records and header, torn records at a crash, interrupted readers, the
/// arrays a read takes, and the bound on the units one open may seal.
class RecordUnitsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.bytes());
    private static final int FIRST = PageFormat.FIRST_RECORD_CONTENT;
    private static final int RECORD = PageFormat.RECORD_CONTENT;
    // Four records, the middle two whole and of the same length
    private static final int FOUR_RECORDS = FIRST + 2 * RECORD + 1_000;

    private static Ledger.Options options(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(FILES).keyspace(RESIDENT).keyspace(NOTES).largeValues(4_096).pageCache(256L << 20);
    }

    private static LiveLedger open(Path directory) {
        return (LiveLedger) options(directory).build();
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static PageRef unit(LiveLedger ledger, Keyspace<String, byte[]> keyspace, String key) {
        var head = ledger.state(keyspace).chain(MemorySegment.ofArray(key.getBytes())).head;
        return assertInstanceOf(Version.External.class, head).unit;
    }

    private static ByteBuffer readFile(Path directory, PageRef ref, long at, int length) throws IOException {
        try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.READ)) {
            var bytes = ByteBuffer.allocate(length);
            channel.read(bytes, PageFormat.offset(ref.page()) + at);
            return bytes.flip();
        }
    }

    private static void writeFile(Path directory, PageRef ref, long at, byte[] bytes) throws IOException {
        try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.WRITE)) {
            var buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                channel.write(buffer, PageFormat.offset(ref.page()) + at + buffer.position());
            }
            channel.force(true);
        }
    }

    private static byte[] fileBytes(Path directory, PageRef ref, long at, int length) throws IOException {
        var buffer = readFile(directory, ref, at, length);
        var bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    // Reads `key` through every path: three gets (a miss the cache refuses, one it takes, a hit), a view and a scan
    private static void readAll(LiveLedger ledger, Keyspace<String, byte[]> keyspace, String key, byte[] expected) {
        for (var read = 0; read < 3; read++) {
            assertArrayEquals(expected, ledger.get(keyspace, key).orElseThrow(), key + " at read " + read);
        }
        assertArrayEquals(expected, ledger.readEncoded(keyspace, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow(),
                key + " as a view");
        var scanned = new AtomicReference<byte[]>();
        ledger.forEach(keyspace, KeyRange.all(), (found, value) -> {
            if (found.equals(key)) {
                scanned.set(value);
            }
            return true;
        });
        assertArrayEquals(expected, scanned.get(), key + " in a scan");
    }

    static IntStream lengths() {
        // One record, one and a byte, a whole record after the first, two whole records, a last record that ends on a
        // page, four records, a unit that the pool still reads whole and one past it, and 16 MiB
        return IntStream.of(FIRST, FIRST + 1, RECORD, RECORD + 1, FIRST + RECORD, FIRST + 2 * RECORD,
                FIRST + RECORD + PageFormat.SIZE - PageFormat.TAG, FOUR_RECORDS, (1 << 20) - 5_000, (1 << 20) + 100, 16 << 20);
    }

    @ParameterizedTest(name = "{0} bytes")
    @MethodSource("lengths")
    @DisplayName("a value longer than a record is a unit of records with zeros after its last tag, read the same through every path in a paged and a resident key space, before a checkpoint, after it and after a replay")
    void roundTrip(int length, @TempDir Path directory) throws Exception {
        var format = length > FIRST ? PageFormat.RECORDS : PageFormat.CLOSE_TAG;
        var paged = value(length, length);
        var resident = value(length + 1, length);
        var later = value(length + 2, length);
        var ledger = open(directory);
        try {
            ledger.commit(ledger.newBatch().put(FILES, "v", paged).put(RESIDENT, "v", resident)).awaitDurable();
            var ref = unit(ledger, FILES, "v");
            assertEquals(PageFormat.pagesFor(format, length), ref.pages());
            var header = readFile(directory, ref, 0, PageFormat.HEADER);
            assertEquals(format, header.get(PageFormat.FORMAT));
            var end = (int) PageFormat.sealedLength(format, length);
            assertArrayEquals(new byte[ref.pages() * PageFormat.SIZE - end], fileBytes(directory, ref, end, ref.pages() * PageFormat.SIZE - end),
                    "a byte after the last tag");
            readAll(ledger, FILES, "v", paged);
            assertArrayEquals(resident, ledger.get(RESIDENT, "v").orElseThrow());
            ledger.clearPageCacheForTests();
            // A scan first, then the gets
            readAll(ledger, FILES, "v", paged);
            ledger.checkpoint();
            ledger.clearPageCacheForTests();
            readAll(ledger, FILES, "v", paged);
            ledger.commit(ledger.newBatch().put(FILES, "w", later).put(RESIDENT, "w", later)).awaitDurable();
        } finally {
            ledger.closeWithoutCheckpoint();
        }
        // The replay authenticates the paged unit and reads the resident one
        try (var reopened = open(directory)) {
            readAll(reopened, FILES, "v", paged);
            readAll(reopened, FILES, "w", later);
            readAll(reopened, RESIDENT, "v", resident);
            readAll(reopened, RESIDENT, "w", later);
        }
        CheckpointTest.verifyPages(directory, true);
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"tag-0", "tag-1", "tag-2", "tag-3", "content-2", "swapped", "truncated", "zeroed", "foreign",
            "format-1", "format-3", "reserved", "length-shorter", "length-fewer-records", "pages", "zeros"})
    @DisplayName("a changed record, tag or header of a unit of records fails every read as corruption, and a changed zero after its last tag leaves it intact")
    void tampered(String change, @TempDir Path directory) throws Exception {
        var expected = value(7, FOUR_RECORDS);
        PageRef ref;
        PageRef other;
        try (var ledger = open(directory)) {
            ledger.commit(ledger.newBatch().put(FILES, "other", value(8, FOUR_RECORDS))).awaitDurable();
            ledger.commit(ledger.newBatch().put(FILES, "v", expected)).awaitDurable();
            other = unit(ledger, FILES, "other");
            ref = unit(ledger, FILES, "v");
            // The tree's leaf points to the units, so the reopen replays nothing that would check them
            ledger.checkpoint();
        }
        assertEquals(4, PageFormat.records(FOUR_RECORDS));
        var unitBytes = ref.pages() * PageFormat.SIZE;
        switch (change) {
            case "tag-0", "tag-1", "tag-2", "tag-3" -> {
                var record = change.charAt(4) - '0';
                var at = PageFormat.recordPosition(record) + PageFormat.recordLength(record, FOUR_RECORDS) + PageFormat.TAG / 2;
                var bytes = fileBytes(directory, ref, at, 1);
                bytes[0] ^= 1;
                writeFile(directory, ref, at, bytes);
            }
            case "content-2" -> {
                var at = PageFormat.recordPosition(2) + 1_000;
                var bytes = fileBytes(directory, ref, at, 1);
                bytes[0] ^= 1;
                writeFile(directory, ref, at, bytes);
            }
            case "swapped" -> {
                var first = fileBytes(directory, ref, PageFormat.recordPosition(1), PageFormat.RECORD);
                var second = fileBytes(directory, ref, PageFormat.recordPosition(2), PageFormat.RECORD);
                writeFile(directory, ref, PageFormat.recordPosition(1), second);
                writeFile(directory, ref, PageFormat.recordPosition(2), first);
            }
            case "truncated" -> {
                // The unit is the last of the file, cut inside its last record
                try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.WRITE)) {
                    assertEquals(PageFormat.offset(ref.page()) + unitBytes, channel.size());
                    channel.truncate(PageFormat.offset(ref.page()) + PageFormat.recordPosition(3) + 100);
                    channel.force(true);
                }
            }
            // The last record as pages no write reached leave it
            case "zeroed" -> writeFile(directory, ref, PageFormat.recordPosition(3), new byte[unitBytes - (int) PageFormat.recordPosition(3)]);
            // The record of the same index of another unit of the same length
            case "foreign" -> writeFile(directory, ref, PageFormat.recordPosition(1), fileBytes(directory, other, PageFormat.recordPosition(1), PageFormat.RECORD));
            case "format-1" -> writeFile(directory, ref, PageFormat.FORMAT, new byte[]{PageFormat.VERSION});
            case "format-3" -> writeFile(directory, ref, PageFormat.FORMAT, new byte[]{PageFormat.CLOSE_TAG});
            case "reserved" -> writeFile(directory, ref, 3, new byte[]{1});
            case "length-shorter" -> {
                var bytes = new byte[4];
                Bytes.i32(bytes, 0, FOUR_RECORDS - 1);
                writeFile(directory, ref, PageFormat.LENGTH, bytes);
            }
            case "length-fewer-records" -> {
                var bytes = new byte[4];
                Bytes.i32(bytes, 0, FIRST + 2 * RECORD);
                writeFile(directory, ref, PageFormat.LENGTH, bytes);
            }
            case "pages" -> {
                var bytes = new byte[4];
                Bytes.i32(bytes, 0, ref.pages() + 1);
                writeFile(directory, ref, PageFormat.PAGES, bytes);
            }
            case "zeros" -> writeFile(directory, ref, unitBytes - 1, new byte[]{1});
            default -> throw new IllegalArgumentException(change);
        }
        try (var ledger = open(directory)) {
            if (change.equals("zeros")) {
                readAll(ledger, FILES, "v", expected);
                return;
            }
            for (var read = 0; read < 3; read++) {
                var failure = assertThrows(LedgerCorruptedException.class, () -> ledger.get(FILES, "v"), "read " + read);
                assertTrue(failure.getMessage().contains("fails authentication"), failure::getMessage);
            }
            assertThrows(LedgerCorruptedException.class, () -> ledger.readEncoded(FILES, "v", segment -> segment.toArray(ValueLayout.JAVA_BYTE)));
            assertThrows(LedgerCorruptedException.class, () -> ledger.forEach(FILES, KeyRange.all(), (_, _) -> true));
            if (!change.equals("foreign") && !change.equals("truncated")) {
                assertArrayEquals(value(8, FOUR_RECORDS), ledger.get(FILES, "other").orElseThrow());
            }
        }
    }

    @ParameterizedTest(name = "later commit: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a last commit only a prefix of whose records reached the file ends the log as a torn frame does, and one a later group follows is refused")
    void tornRecords(boolean later, @TempDir Path directory) throws Exception {
        var ledger = open(directory);
        ledger.commit(ledger.newBatch().put(NOTES, "kept", "before".getBytes())).awaitDurable();
        ledger.commit(ledger.newBatch().put(FILES, "lost", value(9, FOUR_RECORDS)).put(NOTES, "lost", "with it".getBytes())).awaitDurable();
        var ref = unit(ledger, FILES, "lost");
        if (later) {
            ledger.commit(ledger.newBatch().put(NOTES, "later", "after".getBytes())).awaitDurable();
        }
        ledger.closeWithoutCheckpoint();
        // Records 2 and 3 never reached the file: their pages hold zeros
        var from = PageFormat.recordPosition(2);
        writeFile(directory, ref, from, new byte[ref.pages() * PageFormat.SIZE - (int) from]);
        if (later) {
            assertThrows(LedgerCorruptedException.class, () -> open(directory));
            return;
        }
        try (var reopened = open(directory)) {
            assertArrayEquals("before".getBytes(), reopened.get(NOTES, "kept").orElseThrow());
            assertEquals(Optional.empty(), reopened.get(FILES, "lost"));
            assertEquals(Optional.empty(), reopened.get(NOTES, "lost"));
            assertEquals(0, reopened.size(FILES));
            reopened.commit(reopened.newBatch().put(FILES, "after", value(10, FOUR_RECORDS))).awaitDurable();
        }
        try (var reopened = open(directory)) {
            assertArrayEquals(value(10, FOUR_RECORDS), reopened.get(FILES, "after").orElseThrow());
        }
        CheckpointTest.verifyPages(directory, true);
    }

    private static LiveLedger openOn(Path directory) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(FILES).keyspace(RESIDENT).keyspace(NOTES)
                .commitWindow(Duration.ZERO)
                .checkpointEvery(1L << 40)
                .largeValues(4_096))
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static Map<String, String> contents(Ledger ledger) {
        var contents = new TreeMap<String, String>();
        for (var keyspace : List.of(FILES, RESIDENT, NOTES)) {
            ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                contents.put(keyspace.name() + "/" + key, HexFormat.of().formatHex(value));
                return true;
            });
            for (var key : new ArrayList<>(contents.keySet())) {
                if (key.startsWith(keyspace.name() + "/")) {
                    var name = key.substring(keyspace.name().length() + 1);
                    assertEquals(contents.get(key), HexFormat.of().formatHex(ledger.get(keyspace, name).orElseThrow()), key);
                }
            }
        }
        return contents;
    }

    /// Commits of values of records to a store on `directory`, recording the state each leaves visible.
    private static final class Workload {
        final List<Map<String, String>> history = new ArrayList<>(List.of(Map.of()));
        final TreeMap<String, String> state = new TreeMap<>();
        int durable;

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = openOn(directory);
                for (var c = 0; c < 6; c++) {
                    var n = c;
                    var length = FIRST + 1 + n * 150_000;
                    var key = "k" + n % 3;
                    var commit = write(ledger, batch -> {
                        batch.accept(FILES, key, value(n * 10, length));
                        batch.accept(NOTES, key, value(n * 10 + 1, 40));
                        if (n % 2 == 1) {
                            batch.accept(RESIDENT, key, value(n * 10 + 2, length + 3));
                        }
                    });
                    if (n == 2) {
                        commit.awaitDurable();
                        durable = history.size() - 1;
                        ledger.checkpoint();
                    }
                }
                ledger.close();
                durable = history.size() - 1;
            } catch (RuntimeException | InterruptedException failure) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // Stopped
                    }
                }
            }
        }

        private interface Puts {
            void accept(Keyspace<String, byte[]> keyspace, String key, byte[] value);
        }

        private Commit write(LiveLedger ledger, Consumer<Puts> writes) {
            var batch = new WriteBatch[]{ledger.newBatch()};
            var next = new TreeMap<>(state);
            writes.accept((keyspace, key, value) -> {
                batch[0].put(keyspace, key, value);
                next.put(keyspace.name() + "/" + key, HexFormat.of().formatHex(value));
            });
            var commit = ledger.commit(batch[0]);
            state.clear();
            state.putAll(next);
            history.add(new TreeMap<>(state));
            return commit;
        }
    }

    @Test
    @DisplayName("recovers a state between the last durable and the last visible commit at sampled forces of commits of values of records, whose unforced records a loss of power keeps in part")
    void powerLoss() throws Exception {
        var dry = CrashFileSystem.create();
        var dryRun = new Workload();
        dryRun.run(dry.getPath("/store"));
        assertEquals(dryRun.history.size() - 1, dryRun.durable, "the dry run failed");
        var forces = dry.forces();
        var random = new Random(forces);
        var points = new ArrayList<Long>();
        for (var i = 0; i < 16; i++) {
            points.add(forces * i / 16);
        }
        points.add(forces);
        for (var force : points) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            var workload = new Workload();
            workload.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            Map<String, String> recovered;
            try (var ledger = openOn(survivor.getPath("/store"))) {
                recovered = contents(ledger);
                var match = false;
                for (var commit = workload.durable; commit < workload.history.size(); commit++) {
                    match |= workload.history.get(commit).equals(recovered);
                }
                if (!match) {
                    fail("force " + force + " of " + forces + ": recovered no state from " + workload.durable + " to "
                            + (workload.history.size() - 1));
                }
                ledger.checkpoint();
                assertEquals(recovered, contents(ledger), "after a checkpoint");
            }
            try (var ledger = openOn(survivor.getPath("/store"))) {
                assertEquals(recovered, contents(ledger), "after a reopen");
            }
        }
    }

    @Test
    @DisplayName("readers interrupted again and again while they read units of records, in the middle of a record or between two, read every value whole, and leave the file usable")
    void interruptedReaders(@TempDir Path directory) throws Exception {
        var values = List.of(value(20, FOUR_RECORDS), value(21, 4 << 20));
        // A quarter of the cache is below each unit, so every get reads the file
        try (var ledger = (LiveLedger) options(directory).pageCache(4L << 20).build()) {
            for (var i = 0; i < values.size(); i++) {
                ledger.commit(ledger.newBatch().put(FILES, "k" + i, values.get(i))).awaitDurable();
            }
            var failure = new AtomicReference<Throwable>();
            var readers = new ArrayList<Thread>();
            for (var r = 0; r < 2; r++) {
                readers.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var round = 0; round < 30; round++) {
                            for (var i = 0; i < values.size(); i++) {
                                assertArrayEquals(values.get(i), ledger.get(FILES, "k" + i).orElseThrow(), "k" + i);
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    } finally {
                        Thread.interrupted();
                    }
                }));
            }
            var random = new Random(3);
            while (readers.stream().anyMatch(Thread::isAlive)) {
                readers.get(random.nextInt(readers.size())).interrupt();
                Thread.sleep(0, 200_000);
            }
            for (var reader : readers) {
                reader.join();
            }
            assertNull(failure.get(), () -> "a reader failed: " + failure.get());
            ledger.commit(ledger.newBatch().put(FILES, "after", values.get(1))).awaitDurable();
            assertArrayEquals(values.get(1), ledger.get(FILES, "after").orElseThrow());
        }
        try (var ledger = open(directory)) {
            for (var i = 0; i < values.size(); i++) {
                assertArrayEquals(values.get(i), ledger.get(FILES, "k" + i).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("an interrupted committer and reader write and read a unit of records, the interrupt closing the channel at the first record")
    void interruptedCallers(@TempDir Path directory) throws Exception {
        var first = value(30, FOUR_RECORDS);
        var second = value(31, FIRST + RECORD + 7);
        try (var ledger = open(directory)) {
            Thread.currentThread().interrupt();
            var commit = ledger.commit(ledger.newBatch().put(FILES, "a", first));
            assertTrue(Thread.interrupted(), "the committer lost its interrupt");
            commit.awaitDurable();
            ledger.clearPageCacheForTests();
            Thread.currentThread().interrupt();
            assertArrayEquals(first, ledger.get(FILES, "a").orElseThrow());
            assertTrue(Thread.interrupted(), "the reader lost its interrupt");
            ledger.commit(ledger.newBatch().put(FILES, "b", second)).awaitDurable();
        }
        try (var ledger = open(directory)) {
            assertArrayEquals(first, ledger.get(FILES, "a").orElseThrow());
            assertArrayEquals(second, ledger.get(FILES, "b").orElseThrow());
        }
    }

    @Test
    @DisplayName("a unit of records is written through one array of a record and read, copied, viewed and authenticated with no array of the unit's size but the value, and a read the cache takes opens it into one spare")
    void arrays(@TempDir Path directory) throws Exception {
        HeapPool.forceRegionForTests(2 << 20);
        var threads = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        var pool = new HeapPool(16 << 20);
        var released = new ArrayList<Integer>();
        pool.watchForTests(new HeapPool.Watcher() {
            @Override
            public void acquired(byte[] array) {
            }

            @Override
            public void released(byte[] array) {
                synchronized (released) {
                    released.add(array.length);
                }
            }
        });
        var length = 16 << 20;
        var written = value(40, length);
        // A quarter of the limit is below the unit, so the cache never takes it
        var refusing = PageCache.isolated(4L << 20);
        var taking = PageCache.isolated(256L << 20);
        var cipher = new PageCipher(new SecretKeySpec(KEY, "AES"));
        try {
            PageRef ref;
            try (var blobs = BlobFile.open(directory, cipher, refusing, pool, 0, new BitSet())) {
                blobs.begin(1);
                ref = blobs.write(written, 0, length);
                assertEquals(List.of(PageFormat.RECORD), released, "the arrays a write gave back");
                for (var read = 0; read < 3; read++) {
                    var acquires = pool.largeAcquiresForTests();
                    var before = threads.getCurrentThreadAllocatedBytes();
                    var value = blobs.copyValue(ref, length, PageCache.POINT);
                    var allocated = threads.getCurrentThreadAllocatedBytes() - before;
                    assertArrayEquals(written, value);
                    assertEquals(acquires, pool.largeAcquiresForTests(), "a copy took a spare");
                    if (read > 0) {
                        assertTrue(allocated - length < 128 << 10, allocated + " bytes allocated by read " + read);
                    }
                }
                var acquires = pool.largeAcquiresForTests();
                // The first may make the pool's second array of a record, which the opens go to
                assertTrue(blobs.authenticates(ref, length));
                var before = threads.getCurrentThreadAllocatedBytes();
                assertTrue(blobs.authenticates(ref, length));
                var allocated = threads.getCurrentThreadAllocatedBytes() - before;
                assertTrue(allocated < 128 << 10, allocated + " bytes allocated to authenticate");
                assertTrue(!blobs.authenticates(ref, length - 1), "authenticated another length");
                var unit = blobs.readUnit(ref);
                assertEquals(ref.pages() * PageFormat.SIZE, unit.length);
                assertArrayEquals(written, Arrays.copyOfRange(unit, PageFormat.HEADER, PageFormat.HEADER + length));
                assertArrayEquals(new byte[unit.length - PageFormat.HEADER - length], Arrays.copyOfRange(unit, PageFormat.HEADER + length, unit.length));
                var view = blobs.value(ref, length);
                assertEquals(length, view.byteSize());
                assertEquals(acquires, pool.largeAcquiresForTests(), "an authentication, a replay's read or a view took a spare");
                assertThrows(LedgerCorruptedException.class, () -> blobs.copyValue(ref, length - 1, PageCache.POINT));
                synchronized (released) {
                    assertTrue(released.stream().allMatch(array -> array == PageFormat.RECORD), "arrays given back: " + released);
                }
            }
            try (var blobs = BlobFile.open(directory, cipher, taking, pool, ref.page() + ref.pages(), new BitSet())) {
                blobs.begin(2);
                var acquires = pool.largeAcquiresForTests();
                assertArrayEquals(written, blobs.copyValue(ref, length, PageCache.POINT));
                assertArrayEquals(written, blobs.copyValue(ref, length, PageCache.POINT));
                var cached = blobs.cachedForTests(ref);
                assertTrue(cached != null, "not cached after two reads");
                assertEquals(acquires + 1, pool.largeAcquiresForTests(), "the spares the reads took, one the cache took among them");
                for (var at = PageFormat.HEADER + length; at < cached.length; at++) {
                    assertEquals(0, cached[at], "a byte past the content at " + at);
                }
            }
        } finally {
            pool.watchForTests(null);
            refusing.close();
            taking.close();
            HeapPool.forceRegionForTests(-1);
        }
    }

    @Test
    @DisplayName("an open seals at most 2^48 units, whose nonces keep the top 16 bits of their first 8 bytes for the index of a record, and a write past them takes no pages")
    void unitsPerOpen(@TempDir Path directory) throws Exception {
        var cache = PageCache.isolated(64L << 20);
        var cipher = new PageCipher(new SecretKeySpec(KEY, "AES"));
        var written = value(50, FIRST + RECORD + 99);
        try (var blobs = BlobFile.open(directory, cipher, cache, new HeapPool(4 << 20), 0, new BitSet())) {
            blobs.begin(1);
            blobs.counterForTests(BlobFile.UNITS_PER_OPEN - 1);
            var last = blobs.write(written, 0, written.length);
            assertEquals(BlobFile.UNITS_PER_OPEN - 1, last.nonceHigh());
            assertArrayEquals(written, blobs.copyValue(last, written.length, PageCache.POINT));
            assertTrue(blobs.authenticates(last, written.length));
            var units = blobs.unadoptedForTests();
            var pages = blobs.pageCount();
            assertThrows(IllegalStateException.class, () -> blobs.write(written, 0, written.length));
            assertThrows(IllegalStateException.class, () -> blobs.write(new byte[100], 0, 100));
            assertEquals(units, blobs.unadoptedForTests(), "a refused write left a unit in flight");
            assertEquals(pages, blobs.pageCount(), "a refused write took pages");
        } finally {
            cache.close();
        }
    }
}
