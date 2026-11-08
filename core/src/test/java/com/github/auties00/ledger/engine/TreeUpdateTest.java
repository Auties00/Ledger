package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.engine.TreeNodes.Child;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The copy-on-write tree on its own, against a sorted map: random rounds of writes and range removals, with every
/// freed page reused at once; and the reads of a branch's children together, through caches that hold some of them or
/// evict them, with a unit of the file that fails authentication, and with a file that cannot be read.
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
            assertTrue(TreeSearch.find(pages, root, hot) != null);
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

    @ParameterizedTest(name = "cache of {0} bytes, encrypted: {1}")
    @CsvSource({"262144,true", "67108864,true", "262144,false"})
    @DisplayName("branches whose children mix cached nodes, delta records, cached record leaves and dropped subtrees read them together, cache no leaf of a record they read, and the tree stays equal to the model")
    void batchedChildren(int cacheBytes, boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(cacheBytes + (encrypted ? 1 : 0));
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        try (var pages = PageFile.open(directory, cipher, cacheBytes, new HeapPool(1 << 20))) {
            var tree = new Fixture(pages);
            var keys = keys(40_000);
            var all = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var key : keys) {
                all.put(key, filler(random, 100));
            }
            tree.apply(all, List.of(), random.nextLong(), null);
            for (var round = 0; round < 24; round++) {
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                // A few leaves of each branch, which become delta records; every fourth round enough writes to update
                // the subtrees in parallel
                var writes = round % 4 == 3 ? 6_000 : 300;
                for (var i = 0; i < writes; i++) {
                    states.put(keys.get(random.nextInt(keys.size())), random.nextInt(6) == 0 ? null : filler(random, 60 + random.nextInt(80)));
                }
                var ranges = new ArrayList<TreeUpdate.Range>();
                if (round % 3 == 2) {
                    // Whole leaves, which the update drops without reading them
                    var from = random.nextInt(keys.size() - 600);
                    ranges.add(new TreeUpdate.Range(segment(keys.get(from)), segment(keys.get(from + 500))));
                }
                // The cache holds a random share of the tree, as reads left it
                pages.cache().clearForTests();
                for (var i = 0; i < 300; i++) {
                    TreeSearch.find(pages, tree.root, segment(keys.get(random.nextInt(keys.size()))));
                }
                // The update gives up nearly every record it reads, so the leaf must not take room from the live ones
                var uncached = new ArrayList<PageRef>();
                for (var child : leafChildren(pages, tree.root)) {
                    if (child.ref().isRecord() && pages.cache().get(child.ref()) == null) {
                        uncached.add(child.ref());
                    }
                }
                tree.apply(states, ranges, random.nextLong(), () -> {
                    for (var ref : uncached) {
                        assertNull(pages.cache().get(ref), "the update cached the leaf of the record at page " + ref.page());
                    }
                });
                verify(pages, tree.root, tree.model, random, keys);
                verifyPages(pages, tree.space, tree.root, tree.deltas);
            }
            assertTrue(pages.batchedLeavesForTests() > 0, "no record's base was read with its siblings");
        }
    }

    @ParameterizedTest(name = "{0}, {1} writes")
    @CsvSource({"leaf,400", "delta page,400", "base,400", "leaf,6000", "delta page,6000", "base,6000"})
    @DisplayName("a unit that fails authentication among the children a branch reads together fails the update, which keeps no page it took, and the same writes apply once the unit is whole")
    void corruptChild(String corrupted, int writes, @TempDir Path directory) throws Exception {
        var random = new Random(writes + corrupted.length());
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, 64 << 20, new HeapPool(1 << 20))) {
            var tree = new Fixture(pages);
            var keys = keys(40_000);
            var all = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var key : keys) {
                all.put(key, filler(random, 100));
            }
            tree.apply(all, List.of(), random.nextLong(), null);
            var sparse = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < 300; i++) {
                sparse.put(keys.get(random.nextInt(keys.size())), filler(random, 90));
            }
            tree.apply(sparse, List.of(), random.nextLong(), null);
            var children = leafChildren(pages, tree.root);
            Child victim = null;
            for (var i = 0; i < children.size() && victim == null; i++) {
                var child = children.get((children.size() / 2 + i) % children.size());
                if (child.ref().isRecord() != corrupted.equals("leaf")) {
                    victim = child;
                }
            }
            assertNotNull(victim, "the tree has no leaf of the kind to corrupt");
            var page = switch (corrupted) {
                case "base" -> DeltaLeaves.base(victim.ref(), pages.read(victim.ref().unit(), PageFormat.DELTA, false)).page();
                default -> victim.ref().page();
            };
            // Writes across the whole tree, one of them to the leaf the corrupted unit makes
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < writes; i++) {
                states.put(keys.get(i * (keys.size() / writes)), filler(random, 90));
            }
            states.put(bytes(victim.firstKey()), filler(random, 90));
            var original = flip(directory, page);
            pages.cache().clearForTests();
            var pageCount = tree.space.pageCount();
            var free = freePages(tree.space, pageCount);
            var writer = pages.writer(random.nextLong());
            var update = new TreeUpdate(pages, writer, tree.space);
            var ops = ops(states);
            var root = tree.root;
            assertThrows(LedgerCorruptedException.class, () -> update.apply(root, ops, List.of()));
            writer.discard();
            for (var taken : update.allocated()) {
                tree.space.release(taken.page(), taken.pages());
            }
            assertEquals(free, freePages(tree.space, pageCount), "the failed update kept a page of the tree, or one it took");
            for (var unused = pageCount; unused < tree.space.pageCount(); unused++) {
                assertTrue(tree.space.isUnused(unused), "the failed update kept page " + unused);
            }
            write(directory, page, original);
            tree.apply(states, List.of(), random.nextLong(), null);
            verify(pages, tree.root, tree.model, random, keys);
            verifyPages(pages, tree.space, tree.root, tree.deltas);
        }
    }

    @ParameterizedTest(name = "{0} writes")
    @ValueSource(ints = {400, 6_000})
    @DisplayName("a file that stops being readable while a branch reads its children together fails the update with an I/O failure, which keeps no page it took")
    void unreadableChildren(int writes) throws Exception {
        var random = new Random(writes);
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        try (var pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(new byte[32], "AES")), 64 << 20, new HeapPool(1 << 20))) {
            var tree = new Fixture(pages);
            var keys = keys(40_000);
            var all = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var key : keys) {
                all.put(key, filler(random, 100));
            }
            tree.apply(all, List.of(), random.nextLong(), null);
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < writes; i++) {
                states.put(keys.get(i * (keys.size() / writes)), filler(random, 90));
            }
            // The branches stay cached, so the first reads of the update are those of a branch's leaves together
            pages.cache().clearForTests();
            cacheBranches(pages, tree.root.ref());
            var pageCount = tree.space.pageCount();
            var free = freePages(tree.space, pageCount);
            disk.stopAfter(0);
            assertThrows(IOException.class, () -> Files.createFile(directory.resolve("stop")));
            var writer = pages.writer(random.nextLong());
            var update = new TreeUpdate(pages, writer, tree.space);
            var ops = ops(states);
            var root = tree.root;
            assertThrows(LedgerIoException.class, () -> update.apply(root, ops, List.of()));
            writer.discard();
            for (var taken : update.allocated()) {
                tree.space.release(taken.page(), taken.pages());
            }
            assertEquals(free, freePages(tree.space, pageCount), "the failed update kept a page of the tree, or one it took");
            for (var unused = pageCount; unused < tree.space.pageCount(); unused++) {
                assertTrue(tree.space.isUnused(unused), "the failed update kept page " + unused);
            }
        }
    }

    /// A tree in a data file of its own, beside the sorted map it must equal.
    private static final class Fixture {
        private final PageFile pages;
        private final FreeSpace space;
        private final TreeMap<byte[], byte[]> model;
        private TreeRoot root;
        private DeltaTable deltas;

        private Fixture(PageFile pages) {
            this.pages = pages;
            this.space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            this.model = new TreeMap<>(UNSIGNED);
            this.deltas = DeltaTable.EMPTY;
        }

        /// Applies the removals of `ranges`, then `states`, a value or `null` for a removal by key, to the tree and
        /// the model, as one checkpoint; runs `updated`, if any, once the tree is updated and before its units are
        /// written.
        private void apply(TreeMap<byte[], byte[]> states, List<TreeUpdate.Range> ranges, long nonce, Runnable updated) throws IOException {
            for (var range : ranges) {
                model.subMap(bytes(range.from()), true, bytes(range.to()), false).clear();
            }
            for (var state : states.entrySet()) {
                if (state.getValue() == null) {
                    model.remove(state.getKey());
                } else {
                    model.put(state.getKey(), state.getValue());
                }
            }
            var writer = pages.writer(nonce);
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops(states), ranges);
            if (updated != null) {
                updated.run();
            }
            deltas = update.finishDeltas(deltas);
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
        }
    }

    private static List<TreeUpdate.Op> ops(TreeMap<byte[], byte[]> states) {
        var ops = new ArrayList<TreeUpdate.Op>(states.size());
        for (var state : states.entrySet()) {
            ops.add(new TreeUpdate.Op(segment(state.getKey()), state.getValue() == null ? null : segment(state.getValue())));
        }
        return ops;
    }

    private static List<byte[]> keys(int count) {
        var keys = new ArrayList<byte[]>(count);
        for (var i = 0; i < count; i++) {
            keys.add("k%07d".formatted(i).getBytes());
        }
        return keys;
    }

    private static byte[] filler(Random random, int size) {
        var value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    /// {@return the children of the branches above the leaves of the tree under `root`, in key order, read without
    /// caching}
    private static List<Child> leafChildren(PageFile pages, TreeRoot root) {
        var children = new ArrayList<Child>();
        collectLeafChildren(pages, root.ref(), children);
        return children;
    }

    private static void collectLeafChildren(PageFile pages, PageRef ref, List<Child> children) {
        var unit = pages.read(ref, PageFile.NODE, false);
        if (TreeNodes.isLeaf(unit)) {
            return;
        }
        for (var child : TreeNodes.children(unit)) {
            if (child.level() == 0) {
                children.add(child);
            } else {
                collectLeafChildren(pages, child.ref(), children);
            }
        }
    }

    /// Caches every branch of the tree under `ref`.
    private static void cacheBranches(PageFile pages, PageRef ref) {
        var unit = pages.read(ref, PageFile.NODE, true);
        for (var child : TreeNodes.children(unit)) {
            if (child.level() > 0) {
                cacheBranches(pages, child.ref());
            }
        }
    }

    /// {@return the free pages of `space` below `pageCount`}
    private static BitSet freePages(FreeSpace space, int pageCount) {
        var free = new BitSet();
        for (var page = PageFormat.FIRST_UNIT_PAGE; page < pageCount; page++) {
            free.set(page, space.isUnused(page));
        }
        return free;
    }

    /// Flips a bit of the sealed page `page` of the data file in `directory`.
    ///
    /// @return the page as it was
    private static byte[] flip(Path directory, int page) throws IOException {
        var original = new byte[PageFormat.SIZE];
        try (var channel = FileChannel.open(directory.resolve(PageFile.FILE), StandardOpenOption.READ)) {
            var buffer = ByteBuffer.wrap(original);
            while (buffer.hasRemaining()) {
                if (channel.read(buffer, PageFormat.offset(page) + buffer.position()) < 0) {
                    throw new IOException("The data file ends before page " + page);
                }
            }
        }
        var flipped = original.clone();
        flipped[PageFormat.SIZE / 2] ^= 1;
        write(directory, page, flipped);
        return original;
    }

    private static void write(Path directory, int page, byte[] sealed) throws IOException {
        try (var channel = FileChannel.open(directory.resolve(PageFile.FILE), StandardOpenOption.WRITE)) {
            var buffer = ByteBuffer.wrap(sealed);
            while (buffer.hasRemaining()) {
                channel.write(buffer, PageFormat.offset(page) + buffer.position());
            }
        }
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
            var hit = root == null ? null : TreeSearch.find(pages, root, segment(key));
            var expected = model.get(key);
            if (expected == null) {
                assertNull(hit);
            } else {
                assertArrayEquals(expected, bytes(TreeSearch.value(pages, hit.unit(), hit.entry())));
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
            var cursor = new TreeCursor(pages, root, from == null ? null : segment(from), to == null ? null : segment(to), descending);
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
