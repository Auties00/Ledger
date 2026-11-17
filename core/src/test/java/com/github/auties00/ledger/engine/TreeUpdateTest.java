package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
    @DisplayName("a flush on a worker of a fork-join pool seals on no other worker, whose tasks may be waiting for the lock the flush holds")
    void sealsOffTheWorkers(@TempDir Path directory) throws Exception {
        var helpers = ConcurrentHashMap.<Thread>newKeySet();
        var helped = new CountDownLatch(1);
        var waited = new AtomicBoolean();
        var flusher = new AtomicReference<Thread>();
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES")) {
            @Override
            void seal(byte[] plain, int pages, byte[] out, int offset) {
                if (Thread.currentThread() == flusher.get()) {
                    // The first seal of the flushing worker waits for another thread to seal, so a seal forked to its
                    // deque is there for the idle workers to take
                    if (waited.compareAndSet(false, true)) {
                        try {
                            helped.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException exception) {
                            throw new IllegalStateException(exception);
                        }
                    }
                } else {
                    helpers.add(Thread.currentThread());
                    helped.countDown();
                }
                super.seal(plain, pages, out, offset);
            }
        };
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            workers.submit(() -> {
                flusher.set(Thread.currentThread());
                // Past the units from which a flush seals on every processor
                for (var i = 0; i < 256; i++) {
                    writer.write(PageFormat.VALUE, 0, space.allocate(1), 1, new byte[PageFormat.HEADER + 100], 100, false);
                }
                writer.flush();
                return null;
            }).get();
        }
        if (Runtime.getRuntime().availableProcessors() > 1) {
            assertFalse(helpers.isEmpty(), "no other thread sealed");
        }
        for (var helper : helpers) {
            assertTrue(helper.isVirtual(), "sealed on " + helper);
        }
    }

    @Test
    @DisplayName("subtrees updated in parallel at two levels of a tree, on as few workers as a machine of four processors has, leave the tree equal to the model")
    void parallelUpdatesOnFewWorkers(@TempDir Path directory) throws Exception {
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

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a checkpoint into an empty tree writes the puts as the model holds them, with removals left out, keys and values of every size and form")
    void newTree(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 31 : 37);
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var slab = new KeySlab();
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            while (states.size() < 6_000) {
                // Keys past the longest a slab chunk takes, too
                var key = new byte[random.nextInt(10) == 0 ? 257 + random.nextInt(200) : 1 + random.nextInt(40)];
                random.nextBytes(key);
                states.put(key, random.nextInt(6) == 0 ? null : value(random));
            }
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var state : states.entrySet()) {
                ops.add(op(slab, state.getKey(), state.getValue(), null, random));
                if (state.getValue() != null) {
                    model.put(state.getKey(), state.getValue());
                }
            }
            var writer = pages.writer(random.nextLong());
            var root = new TreeUpdate(pages, writer, space).apply(null, ops, List.of());
            writer.flush();
            pages.cache().clearForTests();
            verify(pages, root, model, random, new ArrayList<>(states.keySet()));
            verifyNodes(pages, root);
            verifyPages(pages, space, root, DeltaTable.EMPTY);
            // Removals alone leave the tree empty
            var removals = new ArrayList<TreeUpdate.Op>();
            for (var key : states.keySet()) {
                removals.add(new TreeUpdate.Op(segment(key), null));
            }
            assertNull(new TreeUpdate(pages, pages.writer(random.nextLong()), space).apply(null, removals, List.of()));
        }
    }

    @Test
    @DisplayName("the leaves of an empty tree split their entries as the split of every entry's size does, sit on consecutive pages a window at a time after the units of the window's values, and keep first keys that outlive their plaintext")
    void newTreeLeaves(@TempDir Path directory) throws Exception {
        var random = new Random(41);
        try (var pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(new byte[32], "AES")), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var slab = new KeySlab();
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            while (states.size() < 3_000) {
                var key = new byte[random.nextInt(10) == 0 ? 257 + random.nextInt(100) : 1 + random.nextInt(40)];
                random.nextBytes(key);
                states.put(key, random.nextInt(6) == 0 ? null : value(random));
            }
            var ops = new ArrayList<TreeUpdate.Op>();
            var expected = new ArrayList<byte[]>();
            // The form each put's value takes: 0 inline, 1 a blob of the data file, 2 a unit of the blob file
            var forms = new ArrayList<Integer>();
            var externalPage = 1_000_000;
            for (var state : states.entrySet()) {
                var value = state.getValue();
                var external = value != null && random.nextInt(8) == 0 ? new PageRef(externalPage++, 1, 2, 3) : null;
                ops.add(op(slab, state.getKey(), value, external, random));
                if (value != null) {
                    expected.add(state.getKey());
                    forms.add(external != null ? 2 : TreeNodes.LeafEntry.fitsInline(state.getKey().length, value.length) ? 0 : 1);
                }
            }
            var writer = pages.writer(random.nextLong());
            var leaves = new TreeUpdate(pages, writer, space).writeNewLeaves(ops);
            var units = new ArrayList<byte[]>();
            var sizes = new ArrayList<Integer>();
            var index = 0;
            // The units of each window in the order the file should hold them: its values in key order, then its leaves
            var layout = new ArrayList<List<PageRef>>();
            for (var l = 0; l < leaves.size(); l++) {
                var leaf = leaves.get(l);
                assertEquals(0, leaf.level());
                if (l % TreeUpdate.NEW_LEAVES_WINDOW == 0) {
                    layout.add(new ArrayList<>());
                    layout.add(new ArrayList<>());
                }
                var valueRefs = layout.get(layout.size() - 2);
                layout.getLast().add(leaf.ref());
                var unit = writer.staged(leaf.ref());
                units.add(unit);
                assertEquals(TreeNodes.count(unit), leaf.count());
                assertEquals(TreeNodes.fill(unit), leaf.fill());
                assertArrayEquals(bytes(TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0))), bytes(leaf.firstKey()));
                var blobs = false;
                for (var i = 0; i < TreeNodes.count(unit); i++) {
                    var entry = TreeNodes.entry(unit, i);
                    assertArrayEquals(expected.get(index), bytes(TreeNodes.leafKey(unit, entry)));
                    var form = TreeNodes.isExternal(unit, entry) ? 2 : TreeNodes.isBlob(unit, entry) ? 1 : 0;
                    assertEquals(forms.get(index), form, "entry " + index + " holds its value in another form");
                    if (form == 1) {
                        valueRefs.add(TreeNodes.blobRef(unit, entry));
                    }
                    blobs |= form != 0;
                    sizes.add(TreeNodes.entryAt(unit, entry).size());
                    index++;
                }
                assertEquals(blobs, leaf.blobs());
            }
            assertEquals(expected.size(), index);
            // The runs the split of every entry gives, which a tree written of all its entries at once takes
            var all = sizes.stream().mapToInt(Integer::intValue).toArray();
            var runs = TreeNodes.split(all, all.length);
            assertEquals(runs.size(), leaves.size());
            for (var i = 0; i < runs.size(); i++) {
                assertEquals(runs.get(i)[1] - runs.get(i)[0], leaves.get(i).count(), "leaf " + i + " splits elsewhere");
            }
            assertTrue(layout.size() >= 4, "the leaves fill a single window");
            var next = PageFormat.FIRST_UNIT_PAGE;
            for (var refs : layout) {
                for (var ref : refs) {
                    assertEquals(next, ref.page(), "a unit is not where the one before it ends");
                    next = ref.page() + ref.pages();
                }
            }
            // A pool that hands the plaintext on, or a cache that drops it, leaves the first keys as they were
            var firstKeys = new ArrayList<byte[]>();
            for (var leaf : leaves) {
                firstKeys.add(bytes(leaf.firstKey()));
            }
            writer.flush();
            pages.cache().clearForTests();
            for (var unit : units) {
                Arrays.fill(unit, (byte) 0x5A);
            }
            for (var i = 0; i < leaves.size(); i++) {
                assertArrayEquals(firstKeys.get(i), bytes(leaves.get(i).firstKey()), "a first key is a view of its leaf");
                assertTrue(leaves.get(i).firstKey().isReadOnly());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an empty tree whose values pass the writer's staged limit is written to the file without a page left unwritten below one written")
    void newTreeFileGrowth(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 43 : 47);
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var slab = new KeySlab();
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            while (states.size() < 20_000) {
                var key = new byte[8 + random.nextInt(24)];
                random.nextBytes(key);
                // About 20 MiB of units of values, past the 8 MiB the writer stages before it writes
                var value = new byte[random.nextInt(8) == 0 ? 2_500 + random.nextInt(6_000) : random.nextInt(200)];
                random.nextBytes(value);
                states.put(key, value);
            }
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var state : states.entrySet()) {
                ops.add(op(slab, state.getKey(), state.getValue(), null, random));
            }
            var writer = pages.writer(random.nextLong());
            var leaves = new TreeUpdate(pages, writer, space).writeNewLeaves(ops);
            assertTrue(leaves.size() > 2 * TreeUpdate.NEW_LEAVES_WINDOW, "the leaves fill fewer than three windows");
            var length = Files.size(directory.resolve(PageFile.FILE));
            assertTrue(length > 0, "the writer wrote nothing before the end");
            // A leaf still staged below the end of the file is a page the file grew past
            for (var leaf : leaves) {
                if (PageFormat.offset(leaf.ref().page()) < length) {
                    assertNull(writer.staged(leaf.ref()), "the file grew past leaf " + leaf.ref().page() + " before it was written");
                }
            }
            assertEquals(states.size(), leaves.stream().mapToLong(TreeNodes.Child::count).sum());
            writer.flush();
        }
    }

    /// {@return the state of `key` with `value`, or with none if `null`, as a checkpoint takes it: the chain of the key
    /// in its words, in a slab or in a segment; the value on an array of its own or a segment, or in the unit
    /// `external` of the blob file if it is not `null`}
    private static TreeUpdate.Op op(KeySlab slab, byte[] key, byte[] value, PageRef external, Random random) {
        var segment = segment(key);
        var chain = random.nextBoolean() ? slab.chain(segment, Segments.hash(segment)) : new Chain(segment, 0);
        if (value == null) {
            return new TreeUpdate.Op(chain, null, null);
        }
        var version = external != null ? Version.ofOwn(0, value.clone(), null, null, external)
                : random.nextBoolean() ? Version.ofOwn(0, value.clone(), null, null, null)
                : new Version(0, segment(value), null, null);
        return new TreeUpdate.Op(chain, version, null);
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
        assertTrue(TreeNodes.fill(unit) <= TreeNodes.CAPACITY, "a node passes its capacity");
        assertTrue(PageFormat.fits(1, Bytes.i32(unit, PageFormat.LENGTH)), "a node passes its page");
        assertEquals(PageFormat.INDEXED, unit[PageFormat.FORMAT], "a node has no search index");
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

    static void verify(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, Random random, List<byte[]> keys) {
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
    static int verifyPages(PageFile pages, FreeSpace space, TreeRoot root, DeltaTable deltas) {
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
