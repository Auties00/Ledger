package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// Reading ahead of a long scan: it changes neither what a scan returns nor what the cache holds, a failure reaches the
/// scan only from the unit the scan comes to, and no thread outlives the scan. The tests that force it use settings that
/// start at the first leaf; those through a store rely on its default settings, past a 1 MiB cache.
class ScanAheadTest {
    private static final ScanAhead.Settings READ_AHEAD = new ScanAhead.Settings(0, 3, 0, Integer.MAX_VALUE, true);
    private static final ScanAhead.Settings ALONE = new ScanAhead.Settings(TreeCursor.ADMITTED, 0, Integer.MAX_VALUE, Integer.MAX_VALUE, false);
    private static final Keyspace<String, byte[]> ITEMS = Keyspace.paged("items", KeyCodec.utf8(), ValueCodec.bytes());
    private static final int ENTRIES = 12_000;
    // Far above what these scans take, even on a loaded machine: passing it means the scan stalled
    private static final Duration STALLED = Duration.ofSeconds(60);

    private static PageFile open(Path directory, int cacheBytes) throws Exception {
        return PageFile.open(directory, new PageCipher(new SecretKeySpec(new byte[32], "AES")), cacheBytes, new HeapPool(1 << 20));
    }

    private static String key(int index) {
        return "k%06d".formatted(index);
    }

    /// {@return the value of entry `index`: one in five is in a blob, which takes one to three pages}
    private static byte[] value(int index) {
        var random = new Random(index);
        var value = new byte[index % 5 == 0 ? 2_500 + random.nextInt(9_000) : 40 + random.nextInt(200)];
        random.nextBytes(value);
        return value;
    }

    private static TreeRoot build(PageFile pages, int entries, IntFunction<byte[]> values) throws Exception {
        var ops = new ArrayList<TreeUpdate.Op>();
        for (var i = 0; i < entries; i++) {
            ops.add(new TreeUpdate.Op(segment(key(i)), MemorySegment.ofArray(values.apply(i)).asReadOnly()));
        }
        var writer = pages.writer(1);
        var root = new TreeUpdate(pages, writer, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet())).apply(null, ops, List.of());
        writer.flush();
        pages.cache().clearForTests();
        return root;
    }

    private static MemorySegment segment(String key) {
        return MemorySegment.ofArray(key.getBytes()).asReadOnly();
    }

    private static String string(MemorySegment key) {
        return new String(key.toArray(ValueLayout.JAVA_BYTE));
    }

    /// {@return the first key of `leaf`}
    private static String firstKey(byte[] leaf) {
        return string(TreeNodes.leafKey(leaf, TreeNodes.entry(leaf, 0)));
    }

    /// {@return the entries before the first in `leaf` that has its value in a blob}
    private static int firstBlobIndex(byte[] leaf) {
        for (var i = 0; i < TreeNodes.count(leaf); i++) {
            if (TreeNodes.isBlob(leaf, TreeNodes.entry(leaf, i))) {
                return i;
            }
        }
        throw new AssertionError("the leaf names no blob");
    }

    private static void corrupt(Path directory, int page) throws Exception {
        try (var channel = FileChannel.open(directory.resolve(PageFile.FILE), StandardOpenOption.WRITE)) {
            var junk = new byte[64];
            new Random(page).nextBytes(junk);
            channel.write(ByteBuffer.wrap(junk), PageFormat.offset(page) + 256);
        }
    }

    /// {@return the leaf at the middle of `leaves` that names a blob, so a scan has several leaves before it}
    private static int middleLeaf(List<byte[]> leaves) {
        for (var i = leaves.size() / 2; i < leaves.size(); i++) {
            var leaf = leaves.get(i);
            for (var n = 0; n < TreeNodes.count(leaf); n++) {
                if (TreeNodes.isBlob(leaf, TreeNodes.entry(leaf, n))) {
                    return i;
                }
            }
        }
        throw new AssertionError("no leaf names a blob");
    }

    private static int entriesBefore(List<byte[]> leaves, int leaf) {
        var entries = 0;
        for (var i = 0; i < leaf; i++) {
            entries += TreeNodes.count(leaves.get(i));
        }
        return entries;
    }

    private static void awaitFailure(ScanAhead ahead) throws InterruptedException {
        for (var i = 0; i < 10_000 && !ahead.failed(); i++) {
            Thread.sleep(1);
        }
        assertTrue(ahead.failed(), "the workers never reached the damaged unit");
    }

    @ParameterizedTest(name = "cache of {0} bytes")
    @ValueSource(ints = {1 << 20, 64 << 20})
    @DisplayName("returns every entry of a range in order, in either direction, as a cursor that reads alone does")
    void matchesAlone(int cacheBytes, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, cacheBytes)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var random = new Random(3);
            for (var round = 0; round < 24; round++) {
                var low = round < 4 ? 0 : random.nextInt(ENTRIES);
                var high = round < 2 ? ENTRIES : low + random.nextInt(ENTRIES - low + 1);
                var from = low == 0 ? null : segment(key(low));
                var to = high == ENTRIES ? null : segment(key(high));
                var descending = (round & 1) == 1;
                if (low == high) {
                    continue;
                }
                try (var cursor = new TreeCursor(pages, root, from, to, descending, READ_AHEAD)) {
                    for (var i = 0; i < high - low; i++) {
                        var index = descending ? high - 1 - i : low + i;
                        assertTrue(cursor.next(), "the cursor ended early");
                        assertEquals(key(index), string(cursor.key()));
                        // A scan skips the values a later write replaced: the workers read them all the same
                        if (index % 7 != 0) {
                            assertArrayEquals(value(index), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                        }
                    }
                    assertFalse(cursor.next(), "the cursor went past the range");
                }
                if (round % 5 == 0) {
                    pages.cache().clearForTests();
                }
            }
        }
    }

    @Test
    @DisplayName("leaves the cache as a cursor that reads alone does: the same entries, each read or unread alike")
    void leavesCacheAlone(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var alone = stateAfterScan(pages, root, ALONE);
            var starts = ScanAhead.startsForTests();
            var ahead = stateAfterScan(pages, root, new ScanAhead.Settings(TreeCursor.ADMITTED, 3, 1, Integer.MAX_VALUE, false));
            assertTrue(ScanAhead.startsForTests() > starts, "the scan never read ahead");
            assertEquals(alone.referenced(), ahead.referenced());
            assertEquals(alone.bytes(), ahead.bytes());
            assertEquals(alone.clock(), ahead.clock());
        }
    }

    private record CacheState(java.util.Map<Long, Boolean> referenced, long bytes, int clock) {
    }

    private static CacheState stateAfterScan(PageFile pages, TreeRoot root, ScanAhead.Settings settings) {
        pages.cache().clearForTests();
        // The scans that fill the cache, as many as it takes to hold every branch
        for (var i = 0; i < 2; i++) {
            scan(pages, root, ALONE);
        }
        var scanned = scan(pages, root, settings);
        assertEquals(ENTRIES, scanned);
        return new CacheState(pages.cache().referencedForTests(), pages.cache().size(), pages.cache().clockLengthForTests());
    }

    private static int scan(PageFile pages, TreeRoot root, ScanAhead.Settings settings) {
        var scanned = 0;
        try (var cursor = new TreeCursor(pages, root, null, null, false, settings)) {
            while (cursor.next()) {
                cursor.value();
                scanned++;
            }
        }
        return scanned;
    }

    @ParameterizedTest(name = "a leaf: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a damaged unit the scan stops before fails nothing, though the workers reached it")
    void stopsBeforeDamage(boolean damageLeaf, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var leaves = TreeSearch.leaves(pages, root);
            var damaged = middleLeaf(leaves);
            var leaf = leaves.get(damaged);
            corrupt(directory, damageLeaf ? Bytes.i32(leaf, PageFormat.PAGE)
                    : TreeNodes.blobRef(leaf, TreeNodes.entry(leaf, firstBlobIndex(leaf))).page());
            pages.cache().clearForTests();
            var before = entriesBefore(leaves, damaged);
            try (var cursor = new TreeCursor(pages, root, null, null, false, READ_AHEAD)) {
                for (var i = 0; i < before; i++) {
                    assertTrue(cursor.next());
                    assertArrayEquals(value(i), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                }
                awaitFailure(cursor.aheadForTests());
            }
        }
    }

    @ParameterizedTest(name = "a leaf: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a damaged unit the scan comes to fails on the scan's thread, at that entry, as when it reads alone")
    void failsAtDamage(boolean damageLeaf, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var leaves = TreeSearch.leaves(pages, root);
            var damaged = middleLeaf(leaves);
            var leaf = leaves.get(damaged);
            corrupt(directory, damageLeaf ? Bytes.i32(leaf, PageFormat.PAGE)
                    : TreeNodes.blobRef(leaf, TreeNodes.entry(leaf, firstBlobIndex(leaf))).page());
            var expected = entriesBefore(leaves, damaged) + (damageLeaf ? 0 : firstBlobIndex(leaf));
            var messages = new ArrayList<String>();
            for (var settings : List.of(ALONE, READ_AHEAD)) {
                pages.cache().clearForTests();
                var scanned = new int[1];
                try (var cursor = new TreeCursor(pages, root, null, null, false, settings)) {
                    var failure = assertThrows(LedgerCorruptedException.class, () -> {
                        while (cursor.next()) {
                            cursor.value();
                            scanned[0]++;
                        }
                    });
                    messages.add(failure.getMessage());
                    assertTrue(Arrays.stream(failure.getStackTrace()).anyMatch(frame -> frame.getClassName().equals(TreeCursor.class.getName())),
                            "the failure comes from a read of another thread");
                }
                assertEquals(expected, scanned[0]);
            }
            assertEquals(messages.getFirst(), messages.getLast());
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("reads no leaf past the end of the range")
    void stopsAtRange(boolean descending, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var leaves = TreeSearch.leaves(pages, root);
            var edge = leaves.size() / 2;
            var bound = segment(firstKey(leaves.get(edge)));
            // The cursor reads one leaf past the range to find it ended, and the workers leave that one to it
            corrupt(directory, Bytes.i32(leaves.get(descending ? edge - 2 : edge + 1), PageFormat.PAGE));
            pages.cache().clearForTests();
            ScanAhead ahead = null;
            try (var cursor = new TreeCursor(pages, root, descending ? bound : null, descending ? null : bound, descending, READ_AHEAD)) {
                while (cursor.next()) {
                    cursor.value();
                    if (cursor.aheadForTests() != null) {
                        ahead = cursor.aheadForTests();
                    }
                }
                // The workers have the time to reach the edge if they would go past it
                Thread.sleep(50);
            }
            assertNotNull(ahead, "the scan never read ahead");
            assertFalse(ahead.failed(), "the workers read past the range");
        }
    }

    @Test
    @DisplayName("an interrupt of the scan's thread stops nothing, and the thread keeps it")
    void keepsInterrupt(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            try (var cursor = new TreeCursor(pages, root, null, null, false, READ_AHEAD)) {
                for (var i = 0; i < ENTRIES; i++) {
                    if (i == ENTRIES / 2) {
                        Thread.currentThread().interrupt();
                    }
                    assertTrue(cursor.next());
                    assertArrayEquals(value(i), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                }
                assertFalse(cursor.next());
            }
            assertTrue(Thread.interrupted(), "the scan lost the interrupt");
            // The reader channels an interrupt closed were replaced
            pages.cache().clearForTests();
            assertEquals(ENTRIES, scan(pages, root, READ_AHEAD));
        } finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest(name = "stopping after {0} entries")
    @ValueSource(ints = {100, ENTRIES})
    @DisplayName("a scan and its close never wait for the scheduler of virtual threads, whose carriers may all be busy")
    void busyCarriers(int entries, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var available = ScanAhead.availableForTests();
            var release = new AtomicBoolean();
            var carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
            var busy = new CountDownLatch(carriers);
            var spinners = new ArrayList<Thread>();
            for (var i = 0; i < carriers; i++) {
                spinners.add(Thread.ofVirtual().start(() -> {
                    busy.countDown();
                    while (!release.get()) {
                        Thread.onSpinWait();
                    }
                }));
            }
            try {
                assertTrue(busy.await(30, TimeUnit.SECONDS), "the carriers never got busy");
                assertTimeoutPreemptively(STALLED, () -> {
                    try (var cursor = new TreeCursor(pages, root, null, null, false, READ_AHEAD)) {
                        for (var i = 0; i < entries; i++) {
                            assertTrue(cursor.next());
                            if (i == 0) {
                                assertNotNull(cursor.aheadForTests(), "the scan never read ahead");
                            }
                            assertArrayEquals(value(i), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                        }
                    }
                });
            } finally {
                release.set(true);
                for (var spinner : spinners) {
                    spinner.join();
                }
            }
            assertEquals(available, ScanAhead.availableForTests());
        }
    }

    @Test
    @DisplayName("a worker that dies with leaves it claimed stops the others, and the scan reads them itself")
    void workerDies(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            var available = ScanAhead.availableForTests();
            assertTimeoutPreemptively(STALLED, () -> {
                try (var cursor = new TreeCursor(pages, root, null, null, false, READ_AHEAD)) {
                    assertTrue(cursor.next());
                    var ahead = cursor.aheadForTests();
                    assertNotNull(ahead, "the scan never read ahead");
                    ahead.failPublishForTests(new OutOfMemoryError("a worker's"));
                    assertArrayEquals(value(0), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                    for (var i = 1; i < ENTRIES; i++) {
                        assertTrue(cursor.next());
                        assertArrayEquals(value(i), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                    }
                    assertFalse(cursor.next());
                    assertTrue(ahead.failed(), "the scan never met the failure");
                    assertNull(cursor.aheadForTests(), "the scan went on with its reader");
                }
            });
            assertEquals(available, ScanAhead.availableForTests());
        }
    }

    @Test
    @DisplayName("holds no more than the limit of bytes, and never a blob larger than a worker fetches")
    void boundsBytes(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, 1 << 20)) {
            // Every entry of a leaf names a blob: a leaf names over 3 MiB, whatever the slots
            IntFunction<byte[]> values = index -> {
                var value = new byte[index % 2 == 0 ? 30_000 : 100_000];
                new Random(index).nextBytes(value);
                return value;
            };
            var entries = 1_200;
            var root = build(pages, entries, values);
            var held = 0L;
            try (var cursor = new TreeCursor(pages, root, null, null, false, READ_AHEAD)) {
                for (var i = 0; i < entries; i++) {
                    assertTrue(cursor.next());
                    assertArrayEquals(values.apply(i), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                    if (i % 16 == 0) {
                        // The workers fill what they may while the scan waits
                        Thread.sleep(5);
                        var ahead = cursor.aheadForTests();
                        if (ahead != null) {
                            held = Math.max(held, ahead.heldForTests());
                        }
                    }
                }
            }
            assertTrue(held > 0, "the workers held nothing");
            assertTrue(held <= ScanAhead.MAX_BYTES + ScanAhead.MAX_LEAF_BYTES, () -> "the slots held too much");
        }
    }

    @Test
    @DisplayName("stops reading ahead when it finds the cache holds what comes next")
    void stopsWhenCached(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, 64 << 20)) {
            var root = build(pages, ENTRIES, ScanAheadTest::value);
            scan(pages, root, ALONE);
            // Only the first leaf misses
            var leaves = TreeSearch.leaves(pages, root);
            pages.cache().remove(Bytes.i32(leaves.getFirst(), PageFormat.PAGE));
            ScanAhead ahead = null;
            var settings = new ScanAhead.Settings(0, 3, 0, 4, true);
            try (var cursor = new TreeCursor(pages, root, null, null, false, settings)) {
                while (cursor.next()) {
                    cursor.value();
                    if (ahead == null) {
                        ahead = cursor.aheadForTests();
                    }
                }
                assertNotNull(ahead, "the first miss started no reader");
                assertNull(cursor.aheadForTests(), "the reader went on over a tree the cache holds");
            }
        }
    }

    private static Ledger store(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(ITEMS).pageCache(1 << 20).build();
    }

    /// Fills a store whose tree holds everything, then a scan leaves the cache full.
    private static Ledger filled(Path directory) throws Exception {
        try (var ledger = store(directory)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < ENTRIES; i++) {
                batch.put(ITEMS, key(i), value(i));
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
        }
        var ledger = store(directory);
        ledger.forEachEncoded(ITEMS, KeyRange.all(), (_, _) -> true);
        return ledger;
    }

    @Test
    @DisplayName("a store's scan past its full cache reads ahead, returns every entry, and ends its workers")
    void readsAheadInStore(@TempDir Path directory) throws Exception {
        assumeTrue(ScanAhead.Settings.DEFAULT.workers() > 0, "this machine has too few processors to read ahead");
        try (var ledger = filled(directory)) {
            var starts = ScanAhead.startsForTests();
            var available = ScanAhead.availableForTests();
            var next = new int[1];
            var visited = ledger.forEachEncoded(ITEMS, KeyRange.all(), (key, value) -> {
                var index = next[0]++;
                assertEquals(key(index), string(key));
                assertArrayEquals(value(index), value.toArray(ValueLayout.JAVA_BYTE));
                return true;
            });
            assertEquals(ENTRIES, visited);
            assertTrue(ScanAhead.startsForTests() > starts, "the scan never read ahead");
            assertEquals(available, ScanAhead.availableForTests());
        }
    }

    @Test
    @DisplayName("a visitor that stops or throws ends the workers before the scan returns")
    void endsWithVisitor(@TempDir Path directory) throws Exception {
        assumeTrue(ScanAhead.Settings.DEFAULT.workers() > 0, "this machine has too few processors to read ahead");
        try (var ledger = filled(directory)) {
            var starts = ScanAhead.startsForTests();
            var available = ScanAhead.availableForTests();
            var seen = new int[1];
            var failure = new IllegalArgumentException("the visitor's");
            var thrown = assertThrows(IllegalArgumentException.class, () -> ledger.forEachEncoded(ITEMS, KeyRange.all(), (_, _) -> {
                if (++seen[0] == ENTRIES - 10) {
                    assertTrue(ScanAhead.availableForTests() < available, "the scan is not reading ahead here");
                    throw failure;
                }
                return true;
            }));
            assertEquals(failure, thrown);
            assertEquals(available, ScanAhead.availableForTests());
            assertTrue(ScanAhead.startsForTests() > starts);
            seen[0] = 0;
            var visited = ledger.forEachEncoded(ITEMS, KeyRange.all(), (_, _) -> ++seen[0] < ENTRIES - 10);
            assertEquals(ENTRIES - 10, visited);
            assertEquals(available, ScanAhead.availableForTests());
        }
    }

    @Test
    @DisplayName("a close of the store during a scan fails the scan as it does without workers, and ends them")
    void closeDuringScan(@TempDir Path directory) throws Exception {
        assumeTrue(ScanAhead.Settings.DEFAULT.workers() > 0, "this machine has too few processors to read ahead");
        var ledger = filled(directory);
        var available = ScanAhead.availableForTests();
        var seen = new int[1];
        var failure = assertThrows(IllegalStateException.class, () -> ledger.forEachEncoded(ITEMS, KeyRange.all(), (_, _) -> {
            if (++seen[0] == ENTRIES / 2) {
                var closer = Thread.ofPlatform().start(ledger::close);
                while (closer.isAlive()) {
                    Thread.onSpinWait();
                }
            }
            return true;
        }));
        assertEquals("The store is closed", failure.getMessage());
        assertEquals(available, ScanAhead.availableForTests());
    }

    @Test
    @DisplayName("scans under writes and checkpoints that free and reuse pages see each entry whole, in order")
    void scansUnderCheckpoints(@TempDir Path directory) throws Exception {
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var done = new AtomicBoolean();
        try (var ledger = filled(directory)) {
            var writer = Thread.ofPlatform().start(() -> {
                var random = new Random(5);
                try {
                    while (!done.get()) {
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 1_000; i++) {
                            var index = random.nextInt(ENTRIES);
                            batch.put(ITEMS, key(index), value(index));
                        }
                        ledger.commit(batch).awaitDurable();
                        ledger.checkpoint();
                    }
                } catch (Throwable failure) {
                    failures.add(failure);
                }
            });
            var scanners = new ArrayList<Thread>();
            for (var scanner = 0; scanner < 4; scanner++) {
                var descending = scanner % 2 == 1;
                scanners.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var round = 0; round < 12; round++) {
                            var position = new int[]{descending ? ENTRIES - 1 : 0};
                            var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
                            var visited = ledger.forEachEncoded(ITEMS, range, (key, value) -> {
                                var index = position[0];
                                position[0] += descending ? -1 : 1;
                                assertEquals(key(index), string(key));
                                assertArrayEquals(value(index), value.toArray(ValueLayout.JAVA_BYTE));
                                return true;
                            });
                            assertEquals(ENTRIES, visited);
                        }
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                }));
            }
            for (var scanner : scanners) {
                scanner.join();
            }
            done.set(true);
            writer.join();
        }
        assertEquals(List.of(), List.copyOf(failures));
    }
}
