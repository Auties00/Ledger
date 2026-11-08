package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The copy-on-write tree on its own, against a sorted map: random rounds of writes and range removals, with every
/// freed page reused at once.
class TreeUpdateTest {
    private static final java.util.Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("random rounds leave the tree equal to the model, with every page reachable or free")
    void randomRounds(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 7 : 11);
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        var pool = new HeapPool(1 << 20);
        try (var pages = PageFile.open(directory, cipher, 1 << 20, pool)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var keys = new ArrayList<byte[]>();
            for (var i = 0; i < 30_000; i++) {
                var key = new byte[1 + random.nextInt(40)];
                random.nextBytes(key);
                keys.add(key);
            }
            TreeRoot root = null;
            var deltas = DeltaTable.EMPTY;
            var deepest = 0;
            var records = 0;
            for (var round = 0; round < 40; round++) {
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                var writes = round < 3 ? 12_000 : random.nextInt(600);
                for (var i = 0; i < writes; i++) {
                    var key = keys.get(random.nextInt(keys.size()));
                    states.put(key, random.nextInt(5) == 0 ? null : value(random));
                }
                var ranges = new ArrayList<TreeUpdate.Range>();
                if (round > 3 && random.nextInt(3) == 0) {
                    var a = keys.get(random.nextInt(keys.size()));
                    var b = keys.get(random.nextInt(keys.size()));
                    var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
                    var high = low == a ? b : a;
                    var open = random.nextInt(10);
                    var from = open == 0 ? null : low;
                    var to = open == 1 ? null : high;
                    if (from == null || to == null || UNSIGNED.compare(from, to) < 0) {
                        ranges.add(new TreeUpdate.Range(from == null ? null : segment(from), to == null ? null : segment(to)));
                        var removed = from == null && to == null ? model
                                : from == null ? model.headMap(to, false)
                                : to == null ? model.tailMap(from, true)
                                : model.subMap(from, true, to, false);
                        removed.clear();
                    }
                }
                var ops = new ArrayList<TreeUpdate.Op>();
                for (Map.Entry<byte[], byte[]> state : states.entrySet()) {
                    ops.add(new TreeUpdate.Op(segment(state.getKey()), state.getValue() == null ? null : segment(state.getValue())));
                    if (state.getValue() == null) {
                        model.remove(state.getKey());
                    } else {
                        model.put(state.getKey(), state.getValue());
                    }
                }
                var writer = pages.writer(random.nextLong());
                var update = new TreeUpdate(pages, writer, space);
                root = update.apply(root, ops, ranges);
                deltas = update.finishDeltas(deltas);
                writer.flush();
                for (var unit : update.freed()) {
                    space.release(unit.page(), unit.pages());
                }
                if (round % 7 == 0) {
                    // Units read back from the file, not from the cache
                    pages.cache().clearForTests();
                }
                verify(pages, root, model, random, keys);
                verifyNodes(pages, root);
                records = Math.max(records, verifyPages(pages, space, root, deltas));
                if (root != null) {
                    deepest = Math.max(deepest, TreeNodes.level(pages.read(root.ref(), PageFile.NODE, true)));
                }
            }
            assertTrue(deepest >= 2, () -> "the tree never grew past level " + 1);
            assertTrue(records > 0, "no leaf was ever written as a delta record");
        }
    }

    @ParameterizedTest(name = "cache of {0} bytes")
    @ValueSource(ints = {1 << 20, 64 << 20})
    @DisplayName("a long scan keeps the leaves it reads while the cache has room for them, and evicts no other")
    void scansFillRoomOnly(int cacheBytes, @TempDir Path directory) throws Exception {
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, cacheBytes, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 20_000; i++) {
                ops.add(new TreeUpdate.Op(segment("k%06d".formatted(i).getBytes()), segment(new byte[150])));
            }
            var writer = pages.writer(1);
            var root = new TreeUpdate(pages, writer, space).apply(null, ops, List.of());
            writer.flush();
            pages.cache().clearForTests();
            // A hot leaf, read before the scan
            var hot = segment("k000010".getBytes());
            assertTrue(TreeSearch.contains(pages, root, hot));
            var cursor = new TreeCursor(pages, root, null, null, false);
            var scanned = 0;
            while (cursor.next()) {
                cursor.value();
                scanned++;
            }
            assertEquals(20_000, scanned);
            assertEquals(TreeSearch.FOUND, TreeSearch.lookupCached(pages, root, hot), "the scan evicted the hot leaf");
            var late = TreeSearch.lookupCached(pages, root, segment("k019990".getBytes()));
            assertEquals(cacheBytes > 16 << 20 ? TreeSearch.FOUND : TreeSearch.UNKNOWN, late);
        }
    }

    @Test
    @DisplayName("subtrees updated in parallel at two levels of a tree, on as few workers as a machine of four processors has, finish while the writer flushes under its lock")
    void parallelUpdatesBesideFlushes(@TempDir Path directory) throws Exception {
        var random = new Random(23);
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            try {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var keys = new ArrayList<byte[]>();
                for (var i = 0; i < 200_000; i++) {
                    keys.add("k%07d".formatted(i).getBytes());
                }
                TreeRoot root = null;
                var deltas = DeltaTable.EMPTY;
                for (var round = 0; round < 12; round++) {
                    var ops = new ArrayList<TreeUpdate.Op>();
                    for (var key : keys) {
                        var value = new byte[100];
                        random.nextBytes(value);
                        model.put(key, value);
                        ops.add(new TreeUpdate.Op(segment(key), segment(value)));
                    }
                    var writer = pages.writer(random.nextLong());
                    var update = new TreeUpdate(pages, writer, space);
                    var before = root;
                    // A pool of its own, so a machine of many processors has only three workers to run into each other
                    root = assertTimeoutPreemptively(Duration.ofMinutes(2),
                            () -> workers.submit(() -> update.apply(before, ops, List.of())).get());
                    deltas = update.finishDeltas(deltas);
                    writer.flush();
                    for (var unit : update.freed()) {
                        space.release(unit.page(), unit.pages());
                    }
                }
                verify(pages, root, model, random, keys);
            } finally {
                // A close would wait for workers a failure of the timeout left stuck
                workers.shutdownNow();
            }
        }
    }

    /// Checks what each branch records of its children against the nodes themselves, and each node against its limits.
    static void verifyNodes(PageFile pages, TreeRoot root) {
        if (root != null) {
            var subtree = verifyNode(pages, root.ref());
            assertEquals(root.count(), subtree.count(), "the root counts wrong");
            assertEquals(root.blobs(), subtree.blobs(), "the root flags its blobs wrong");
        }
    }

    private static Subtree verifyNode(PageFile pages, PageRef ref) {
        var unit = pages.read(ref, PageFile.NODE, false);
        assertTrue(Bytes.i32(unit, PageFormat.LENGTH) - 2 <= TreeNodes.CAPACITY, "a node passes its capacity");
        assertTrue(TreeNodes.count(unit) > 0, "a node is empty");
        if (TreeNodes.isLeaf(unit)) {
            var blobs = false;
            byte[] previous = null;
            for (var entry : TreeNodes.leafEntries(unit)) {
                blobs |= entry.blob() != null;
                var key = bytes(entry.key());
                assertTrue(previous == null || UNSIGNED.compare(previous, key) < 0, "a leaf is out of order");
                previous = key;
            }
            return new Subtree(bytes(TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0))), TreeNodes.count(unit), blobs);
        }
        var count = 0L;
        var blobs = false;
        byte[] first = null;
        byte[] previous = null;
        for (var child : TreeNodes.children(unit)) {
            var subtree = verifyNode(pages, child.ref());
            assertArrayEquals(subtree.firstKey(), bytes(child.firstKey()), "a branch names the wrong first key");
            assertEquals(subtree.count(), child.count(), "a branch counts a child wrong");
            assertEquals(subtree.blobs(), child.blobs(), "a branch flags a child's blobs wrong");
            assertTrue(previous == null || UNSIGNED.compare(previous, subtree.firstKey()) < 0, "a branch is out of order");
            first = first == null ? subtree.firstKey() : first;
            previous = subtree.firstKey();
            count += subtree.count();
            blobs |= subtree.blobs();
        }
        return new Subtree(first, count, blobs);
    }

    private record Subtree(byte[] firstKey, long count, boolean blobs) {
    }

    private static byte[] value(Random random) {
        var size = switch (random.nextInt(20)) {
            case 0 -> 2_500 + random.nextInt(20_000);
            case 1 -> 1_900 + random.nextInt(200);
            default -> random.nextInt(300);
        };
        var value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static void verify(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, Random random, List<byte[]> keys) {
        assertEquals(model.size(), root == null ? 0 : root.count());
        assertScan(pages, root, model, null, null);
        for (var i = 0; i < 3; i++) {
            var a = keys.get(random.nextInt(keys.size()));
            var b = keys.get(random.nextInt(keys.size()));
            var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
            var high = low == a ? b : a;
            assertScan(pages, root, model, low, high);
            assertScan(pages, root, model, low, null);
            assertScan(pages, root, model, null, high);
            var expected = UNSIGNED.compare(low, high) < 0 ? model.subMap(low, true, high, false).size() : 0;
            assertEquals(expected, TreeSearch.count(pages, root, segment(low), segment(high)));
        }
        for (var i = 0; i < 50; i++) {
            var key = keys.get(random.nextInt(keys.size()));
            var expected = model.get(key);
            if (root == null) {
                assertNull(expected);
                continue;
            }
            assertEquals(expected != null, TreeSearch.contains(pages, root, segment(key)));
            var unit = TreeSearch.leaf(pages, root, key);
            var entry = TreeSearch.entryOf(unit, key);
            if (expected == null) {
                assertEquals(-1, entry);
            } else {
                assertArrayEquals(expected, bytes(TreeSearch.value(pages, unit, entry)));
                if (TreeNodes.isInline(unit, entry)) {
                    assertArrayEquals(expected, TreeNodes.inlineValueBytes(unit, entry));
                }
            }
        }
    }

    private static void assertScan(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to) {
        for (var descending : new boolean[]{false, true}) {
            var expected = from != null && to != null && UNSIGNED.compare(from, to) >= 0 ? new TreeMap<byte[], byte[]>(UNSIGNED)
                    : from == null && to == null ? model
                    : from == null ? model.headMap(to, false)
                    : to == null ? model.tailMap(from, true)
                    : model.subMap(from, true, to, false);
            var ordered = descending ? new ArrayList<>(expected.descendingMap().entrySet()) : new ArrayList<>(expected.entrySet());
            var cursor = new TreeCursor(pages, root, from, to, descending);
            for (var entry : ordered) {
                assertTrue(cursor.next(), "the cursor ended early");
                assertArrayEquals(entry.getKey(), bytes(cursor.key()));
                assertArrayEquals(entry.getValue(), bytes(cursor.value()));
            }
            assertFalse(cursor.next(), "the cursor went past the range");
        }
    }

    /// {@return the delta records the tree refers to}
    private static int verifyPages(PageFile pages, FreeSpace space, TreeRoot root, DeltaTable deltas) {
        var used = new BitSet();
        var records = new TreeMap<Integer, Integer>();
        if (root != null) {
            assertFalse(root.ref().isRecord(), "the root is a delta record");
            CheckpointTest.walk(pages, root.ref(), used, new BitSet(), records);
        }
        CheckpointTest.verifyDeltas(used, records, deltas);
        for (var page = PageFormat.FIRST_UNIT_PAGE; page < space.pageCount(); page++) {
            var free = space.isUnused(page);
            assertTrue(free != used.get(page), "page " + page + (free ? " is free and in use" : " is neither free nor in use"));
        }
        return records.values().stream().mapToInt(Integer::intValue).sum();
    }
}
