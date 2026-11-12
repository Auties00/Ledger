package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The cursor's touches of the entries, blobs and leaf ahead of it: scans of trees of every shape, with inline values,
/// blobs and units of the file of large values, read what the model holds whether the cache holds the units ahead or
/// not; a scan that stops leaves the cache's flags of the units it did not read as they were; a damaged entry the scan
/// does not reach is never reported; and scans stay right while the cache drops and rebuilds under them.
class TreeCursorTouchTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");
    private static final java.util.Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    /// An inline value whose entry, under a key of [#key(int)], takes half of a leaf: two fill a leaf, one stays alone.
    private static final int HALF_LEAF = TreeNodes.MAX_ENTRY - (2 + 2 + 7 + 1 + 2);

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("scans of a single leaf, of trees of many levels and of leaves of one entry read what the model holds, whether the cache holds the units ahead or not")
    void scansAgainstModel(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 41 : 43);
        try (var store = Store.open(directory, encrypted, 64 << 20)) {
            // A root that is a leaf
            var single = new TreeMap<byte[], byte[]>(UNSIGNED);
            var writes = new TreeMap<byte[], Value>(UNSIGNED);
            for (var i = 0; i < 5; i++) {
                writes.put(key(i), Value.random(random, i % 3));
            }
            var root = store.apply(null, writes, single);
            assertTrue(TreeNodes.isLeaf(store.pages.read(root.ref(), PageFile.NODE, true)));
            verify(store, root, single, random);

            // Many levels, after removals and overwrites
            var many = new TreeMap<byte[], byte[]>(UNSIGNED);
            writes.clear();
            for (var i = 0; i < 4_000; i++) {
                writes.put(key(i), Value.random(random, random.nextInt(10)));
            }
            root = store.apply(null, writes, many);
            for (var round = 0; round < 3; round++) {
                writes.clear();
                for (var i = 0; i < 400; i++) {
                    writes.put(key(random.nextInt(4_000)), random.nextInt(3) == 0 ? null : Value.random(random, random.nextInt(10)));
                }
                root = store.apply(root, writes, many);
                verify(store, root, many, random);
            }
            assertTrue(TreeNodes.level(store.pages.read(root.ref(), PageFile.NODE, true)) >= 2, "the tree has fewer than three levels");

            // Leaves of one entry each
            var alone = new TreeMap<byte[], byte[]>(UNSIGNED);
            writes.clear();
            for (var i = 0; i < 600; i++) {
                writes.put(key(i), Value.inline(random, HALF_LEAF));
            }
            root = store.apply(null, writes, alone);
            writes.clear();
            for (var i = 0; i < 600; i += 2) {
                writes.put(key(i), null);
            }
            for (var i = 1; i < 600; i += 10) {
                // Blobs among them, of both files
                writes.put(key(i), Value.random(random, i % 20 == 1 ? 1 : 2));
            }
            root = store.apply(root, writes, alone);
            assertTrue(leaves(store.pages, root).stream().anyMatch(leaf -> TreeNodes.count(leaf) == 1), "no leaf holds one entry");
            verify(store, root, alone, random);
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a scan that stops leaves the units it did not read as unread for the clock and the idle ticker, at a leaf's last entry and before a blob")
    void stoppedScanLeavesNoTrace(boolean descending, @TempDir Path directory) throws Exception {
        var random = new Random(descending ? 47 : 53);
        try (var store = Store.open(directory, true, 64 << 20)) {
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var writes = new TreeMap<byte[], Value>(UNSIGNED);
            for (var i = 0; i < 600; i++) {
                // Every third value in a blob, of either file
                writes.put(key(i), Value.random(random, i % 3 == 0 ? 1 + i / 3 % 2 : 0));
            }
            var root = store.apply(null, writes, model);
            var cache = store.pages.cache();
            IdleTicker.unwatch(cache);
            var leaves = leaves(store.pages, root);
            assertTrue(leaves.size() > 4, "the tree has too few leaves");
            var ordered = new ArrayList<>(descending ? model.descendingKeySet() : model.navigableKeySet());
            // Stops at each leaf's last entry in the scan's order, and before each blob
            var stops = new ArrayList<Integer>();
            for (var i = 0; i + 1 < ordered.size() && stops.size() < 40; i++) {
                var next = ordered.get(i + 1);
                if (leafOf(leaves, ordered.get(i)) != leafOf(leaves, next) || !isInline(leaves, next)) {
                    stops.add(i + 1);
                }
            }
            assertTrue(stops.size() >= 10);
            for (var visits : stops) {
                // Every unit cached and marked idle
                for (var leaf : leaves) {
                    store.pages.read(refOf(store.pages, root, leaf), PageFile.NODE, true);
                    for (var i = 0; i < TreeNodes.count(leaf); i++) {
                        var entry = TreeNodes.entry(leaf, i);
                        if (!TreeNodes.isInline(leaf, entry)) {
                            TreeSearch.value(store.pages, leaf, entry);
                        }
                    }
                }
                cache.tick();
                var cursor = new TreeCursor(store.pages, root, null, null, descending);
                var read = new HashSet<byte[]>();
                var visited = new HashSet<String>();
                for (var n = 0; n < visits; n++) {
                    assertTrue(cursor.next());
                    var key = bytes(cursor.key());
                    assertArrayEquals(ordered.get(n), key);
                    assertArrayEquals(model.get(key), bytes(cursor.value()));
                    visited.add(new String(key, StandardCharsets.US_ASCII));
                    read.add(leafOf(leaves, key));
                }
                cache.tick();
                for (var leaf : leaves) {
                    var ref = refOf(store.pages, root, leaf);
                    assertEquals(read.contains(leaf), cache.peek(ref) != null,
                            "after " + visits + " entries, the leaf of " + firstKey(leaf));
                    for (var i = 0; i < TreeNodes.count(leaf); i++) {
                        var entry = TreeNodes.entry(leaf, i);
                        if (TreeNodes.isInline(leaf, entry)) {
                            continue;
                        }
                        var key = new String(bytes(TreeNodes.leafKey(leaf, entry)), StandardCharsets.US_ASCII);
                        assertEquals(visited.contains(key), cachedBlob(cache, leaf, entry),
                                "after " + visits + " entries, the blob of " + key);
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("a damaged entry or blob reference after the entries a scan reads is never reported, and reads nothing past its leaf")
    void damagedEntriesAhead(@TempDir Path directory) throws Exception {
        try (var store = Store.open(directory, true, 1 << 20)) {
            var entries = List.of(
                    new TreeNodes.LeafEntry(segment("a"), segment("first"), null, 5),
                    new TreeNodes.LeafEntry(segment("b"), null, new PageRef(40_000, 3, 77, 78), 9_000),
                    new TreeNodes.LeafEntry(segment("c"), null, new PageRef(40_100, 1, 79, 80), 2_000, true),
                    new TreeNodes.LeafEntry(segment("d"), segment("fourth"), null, 6),
                    new TreeNodes.LeafEntry(segment("e"), segment("fifth"), null, 5));
            var content = 2 + entries.stream().mapToInt(TreeNodes.LeafEntry::size).sum();
            for (var damage = 0; damage < 4; damage++) {
                var leaf = TreeNodes.writeLeaf(entries, 0, entries.size(), content);
                var ref = new PageRef(30 + damage, 1, 5, damage);
                PageFile.header(leaf, PageFormat.LEAF, 0, ref.page(), 1, content, ref.nonceHigh(), ref.nonceLow());
                switch (damage) {
                    // The offset of the second entry past the leaf
                    case 0 -> Bytes.u16(leaf, PageFormat.HEADER + 2 + 2, 0xFFFF);
                    // The key length of the second entry past the leaf
                    case 1 -> Bytes.u16(leaf, TreeNodes.entry(leaf, 1), 0xFFF0);
                    // The key length of the third entry ending it at the leaf's last byte
                    case 2 -> Bytes.u16(leaf, TreeNodes.entry(leaf, 2), leaf.length - TreeNodes.entry(leaf, 2) - 3);
                    // The offset of every entry after the first past the leaf
                    default -> {
                        for (var i = 1; i < entries.size(); i++) {
                            Bytes.u16(leaf, PageFormat.HEADER + 2 + 2 * i, leaf.length - 1);
                        }
                    }
                }
                store.pages.cache().put(ref, leaf, true);
                var cursor = new TreeCursor(store.pages, new TreeRoot(ref, entries.size(), true), null, null, false);
                assertTrue(cursor.next(), "damage " + damage);
                assertEquals("a", string(cursor.key()));
                assertEquals("first", string(cursor.value()));
                assertFalse(cursor.lastOfLeaf());
                // From the other end the damaged entries come last
                var down = new TreeCursor(store.pages, new TreeRoot(ref, entries.size(), true), null, null, true);
                if (damage == 3) {
                    continue;
                }
                assertTrue(down.next());
                assertEquals("e", string(down.key()));
                assertEquals("fifth", string(down.value()));
            }
        }
    }

    @Test
    @DisplayName("a peek of a child or of a value's unit finds what a read finds, refuses a unit of another kind, and counts for neither the clock nor the idle ticker")
    void peeks(@TempDir Path directory) throws Exception {
        try (var store = Store.open(directory, false, 1 << 20)) {
            var cache = store.pages.cache();
            IdleTicker.unwatch(cache);
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var writes = new TreeMap<byte[], Value>(UNSIGNED);
            var random = new Random(59);
            for (var i = 0; i < 300; i++) {
                writes.put(key(i), Value.random(random, i % 3));
            }
            var root = store.apply(null, writes, model);
            // The units the checkpoint cached gone
            cache.clearForTests();
            var branch = store.pages.read(root.ref(), PageFile.NODE, true);
            assertFalse(TreeNodes.isLeaf(branch));
            var leaf = store.pages.node(branch, 1);
            assertSame(leaf, store.pages.peekNode(branch, 1));
            var inFile = -1;
            var external = -1;
            for (var i = 0; i < TreeNodes.count(leaf); i++) {
                var entry = TreeNodes.entry(leaf, i);
                if (TreeNodes.isBlob(leaf, entry)) {
                    inFile = entry;
                } else if (TreeNodes.isExternal(leaf, entry)) {
                    external = entry;
                }
            }
            assertTrue(inFile >= 0 && external >= 0, "the leaf has no blob of either file");
            assertNull(store.pages.peekBlob(leaf, inFile), "a blob no read cached");
            var value = TreeSearch.value(store.pages, leaf, inFile);
            var unit = store.pages.peekBlob(leaf, inFile);
            assertNotNull(unit);
            assertEquals(MemorySegment.ofArray(unit).asSlice(PageFormat.HEADER, value.byteSize()).mismatch(value), -1);
            TreeSearch.value(store.pages, leaf, external);
            var externalUnit = store.pages.peekBlob(leaf, external);
            assertNotNull(externalUnit);
            assertEquals(PageFormat.EXTERNAL, externalUnit[PageFormat.KIND]);

            // A unit of another kind under the key of a blob or a child
            var blobRef = TreeNodes.blobRef(leaf, inFile);
            var other = new byte[PageFormat.SIZE];
            PageFile.header(other, PageFormat.LEAF, 0, blobRef.page(), blobRef.pages(), 0, blobRef.nonceHigh(), blobRef.nonceLow());
            cache.put(blobRef, other, true);
            assertNull(store.pages.peekBlob(leaf, inFile), "a leaf where a blob belongs");
            var childRef = TreeNodes.childRef(branch, 2);
            var values = new byte[PageFormat.SIZE];
            PageFile.header(values, PageFormat.VALUE, 0, childRef.page(), 1, 0, childRef.nonceHigh(), childRef.nonceLow());
            cache.put(childRef, values, true);
            assertNull(store.pages.peekNode(branch, 2), "a blob where a node belongs");

            // Peeked and idle: the tick evicts it
            cache.tick();
            assertSame(leaf, store.pages.peekNode(branch, 1));
            assertNotNull(store.pages.peekBlob(leaf, external));
            cache.tick();
            assertNull(store.pages.peekNode(branch, 1), "a peek saved the leaf from the tick");
            assertNull(store.pages.peekBlob(leaf, external), "a peek saved the unit from the tick");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("scans in both directions read what the model holds while the cache they touch ahead in evicts, drops and rebuilds its table under them")
    void scansBesideEvictions(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 61 : 67);
        // A cache much smaller than the tree, so the units ahead come and go
        try (var store = Store.open(directory, encrypted, 1 << 20)) {
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var writes = new TreeMap<byte[], Value>(UNSIGNED);
            for (var i = 0; i < 3_000; i++) {
                writes.put(key(i), Value.random(random, random.nextInt(8)));
            }
            var root = store.apply(null, writes, model);
            var keys = new ArrayList<>(model.keySet());
            var failures = new ConcurrentLinkedQueue<String>();
            var done = new AtomicBoolean();
            var dropper = Thread.ofPlatform().start(() -> {
                var local = new Random(71);
                while (!done.get()) {
                    switch (local.nextInt(3)) {
                        case 0 -> store.pages.cache().clearForTests();
                        case 1 -> store.pages.cache().tick();
                        default -> Thread.onSpinWait();
                    }
                }
            });
            var scanners = new Thread[4];
            for (var t = 0; t < scanners.length; t++) {
                var seed = random.nextLong();
                scanners[t] = Thread.ofPlatform().start(() -> {
                    var local = new Random(seed);
                    for (var i = 0; i < 300 && failures.isEmpty(); i++) {
                        var a = keys.get(local.nextInt(keys.size()));
                        var b = keys.get(local.nextInt(keys.size()));
                        var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
                        var high = low == a ? b : a;
                        var descending = local.nextBoolean();
                        var range = model.subMap(low, true, high, false);
                        var expected = (descending ? range.descendingMap() : range).entrySet().iterator();
                        var cursor = new TreeCursor(store.pages, root, low, high, descending);
                        var limit = 1 + local.nextInt(80);
                        for (var n = 0; n < limit && expected.hasNext(); n++) {
                            var entry = expected.next();
                            if (!cursor.next() || !Arrays.equals(entry.getKey(), bytes(cursor.key()))
                                    || !Arrays.equals(entry.getValue(), bytes(cursor.value()))) {
                                failures.add("the scan of [" + string(low) + ", " + string(high) + ") at " + string(entry.getKey()));
                                break;
                            }
                        }
                    }
                });
            }
            for (var scanner : scanners) {
                scanner.join();
            }
            done.set(true);
            dropper.join();
            assertTrue(failures.isEmpty(), () -> String.join("\n", failures));
        }
    }

    /// Checks every scan of `model` in both directions: the whole tree, random ranges, and ranges a visitor leaves
    /// early, with the units ahead cached and not.
    private static void verify(Store store, TreeRoot root, TreeMap<byte[], byte[]> model, Random random) {
        var keys = new ArrayList<>(model.keySet());
        for (var cold : new boolean[]{false, true}) {
            assertScan(store.pages, root, model, null, null, Integer.MAX_VALUE);
            for (var i = 0; i < 20; i++) {
                if (cold) {
                    store.pages.cache().clearForTests();
                }
                var a = keys.get(random.nextInt(keys.size()));
                var b = keys.get(random.nextInt(keys.size()));
                var low = UNSIGNED.compare(a, b) <= 0 ? a : b;
                var high = low == a ? b : a;
                var limit = random.nextInt(3) == 0 ? 1 + random.nextInt(60) : Integer.MAX_VALUE;
                assertScan(store.pages, root, model, low, high, limit);
                assertScan(store.pages, root, model, low, null, limit);
                assertScan(store.pages, root, model, null, high, limit);
                // Bounds between the keys, just past one
                var past = Arrays.copyOf(a, a.length + 1);
                assertScan(store.pages, root, model, past, null, limit);
                assertScan(store.pages, root, model, null, past, limit);
            }
        }
    }

    private static void assertScan(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to, int limit) {
        for (var descending : new boolean[]{false, true}) {
            var expected = from != null && to != null && UNSIGNED.compare(from, to) >= 0 ? new TreeMap<byte[], byte[]>(UNSIGNED)
                    : from == null && to == null ? model
                    : from == null ? model.headMap(to, false)
                    : to == null ? model.tailMap(from, true)
                    : model.subMap(from, true, to, false);
            var ordered = new ArrayList<>((descending ? expected.descendingMap() : expected).entrySet());
            var cursor = new TreeCursor(pages, root, from, to, descending);
            var n = 0;
            for (var entry : ordered) {
                if (n++ == limit) {
                    return;
                }
                assertTrue(cursor.next(), "the cursor ended early");
                assertArrayEquals(entry.getKey(), bytes(cursor.key()));
                assertArrayEquals(entry.getValue(), bytes(cursor.value()));
            }
            assertFalse(cursor.next(), "the cursor went past the range");
        }
    }

    /// {@return the leaves of the tree under `root`, in key order}
    private static List<byte[]> leaves(PageFile pages, TreeRoot root) {
        var leaves = new ArrayList<byte[]>();
        collect(pages, pages.read(root.ref(), PageFile.NODE, true), leaves);
        return leaves;
    }

    private static void collect(PageFile pages, byte[] unit, List<byte[]> leaves) {
        if (TreeNodes.isLeaf(unit)) {
            leaves.add(unit);
            return;
        }
        for (var i = 0; i < TreeNodes.count(unit); i++) {
            collect(pages, pages.node(unit, i), leaves);
        }
    }

    /// {@return the reference of `leaf` in the tree under `root`}
    private static PageRef refOf(PageFile pages, TreeRoot root, byte[] leaf) {
        var branch = pages.read(root.ref(), PageFile.NODE, true);
        while (true) {
            var child = TreeNodes.childFor(branch, TreeNodes.leafKey(leaf, TreeNodes.entry(leaf, 0)));
            var ref = TreeNodes.childRef(branch, child);
            if (TreeNodes.level(branch) == 1) {
                return ref;
            }
            branch = pages.node(branch, child);
        }
    }

    private static byte[] leafOf(List<byte[]> leaves, byte[] key) {
        for (var leaf : leaves) {
            if (TreeSearch.entryOf(leaf, key) != -1) {
                return leaf;
            }
        }
        throw new AssertionError("no leaf holds " + string(key));
    }

    private static boolean isInline(List<byte[]> leaves, byte[] key) {
        var leaf = leafOf(leaves, key);
        return TreeNodes.isInline(leaf, TreeSearch.entryOf(leaf, key));
    }

    private static boolean cachedBlob(PageCache cache, byte[] leaf, int entry) {
        var ref = TreeNodes.blobRef(leaf, entry);
        var key = TreeNodes.isExternal(leaf, entry) ? PageCache.key(PageCache.BLOBS, ref.page()) : PageCache.dataKey(ref.page(), -1);
        return cache.peek(key, ref.nonceHigh(), ref.nonceLow(), ref.pages()) != null;
    }

    private static String firstKey(byte[] leaf) {
        return string(TreeNodes.leafKey(leaf, TreeNodes.entry(leaf, 0)));
    }

    private static byte[] key(int i) {
        return "k%06d".formatted(i).getBytes(StandardCharsets.US_ASCII);
    }

    private static MemorySegment segment(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.US_ASCII)).asReadOnly();
    }

    private static String string(MemorySegment segment) {
        return new String(bytes(segment), StandardCharsets.US_ASCII);
    }

    private static String string(byte[] bytes) {
        return new String(bytes, StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /// A value to write and where it goes: inline, in a blob of the data file, or in a unit of the file of large values.
    ///
    /// @param where 0 inline, 1 a blob, 2 a unit of the file of large values
    private record Value(byte[] bytes, int where) {
        /// {@return a value of `kind`: 1 a blob, 2 a unit of the file of large values, any other an inline value}
        static Value random(Random random, int kind) {
            var where = kind == 1 || kind == 2 ? kind : 0;
            var bytes = new byte[where == 0 ? random.nextInt(300) : 2_500 + random.nextInt(9_500)];
            random.nextBytes(bytes);
            return new Value(bytes, where);
        }

        static Value inline(Random random, int length) {
            var bytes = new byte[length];
            random.nextBytes(bytes);
            return new Value(bytes, 0);
        }
    }

    /// A data file and its file of large values, sharing one cache, and the pages they hand out.
    private record Store(PageFile pages, BlobFile blobs, FreeSpace space, long[] nonce, DeltaTable[] deltas) implements AutoCloseable {
        static Store open(Path directory, boolean encrypted, long cacheBytes) throws Exception {
            var pages = PageFile.open(directory, new PageCipher(encrypted ? KEY : null), cacheBytes, new HeapPool(4 << 20));
            var blobs = BlobFile.open(directory, new PageCipher(encrypted ? KEY : null), pages.cache(), new HeapPool(4 << 20), 0, new BitSet());
            blobs.begin(1);
            pages.useBlobs(blobs);
            return new Store(pages, blobs, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet()), new long[1],
                    new DeltaTable[]{DeltaTable.EMPTY});
        }

        /// Writes `writes`, a `null` value removing its key, to the tree under `root` and to `model`.
        ///
        /// @return the new root
        TreeRoot apply(TreeRoot root, TreeMap<byte[], Value> writes, TreeMap<byte[], byte[]> model) throws Exception {
            var ops = new ArrayList<TreeUpdate.Op>();
            for (Map.Entry<byte[], Value> write : writes.entrySet()) {
                var key = MemorySegment.ofArray(write.getKey().clone()).asReadOnly();
                var value = write.getValue();
                if (value == null) {
                    ops.add(new TreeUpdate.Op(key, null));
                    model.remove(write.getKey());
                    continue;
                }
                var external = value.where() == 2 ? blobs.write(value.bytes(), 0, value.bytes().length) : null;
                ops.add(new TreeUpdate.Op(new Chain(key, 0), Version.ofOwn(0, value.bytes().clone(), null, null, external), null));
                model.put(write.getKey(), value.bytes());
            }
            var writer = pages.writer(++nonce[0]);
            var update = new TreeUpdate(pages, writer, space);
            var updated = update.apply(root, ops, List.of());
            deltas[0] = update.finishDeltas(deltas[0]);
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
            return updated;
        }

        @Override
        public void close() {
            pages.close();
        }
    }
}
