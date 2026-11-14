package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The reads the page cache screens: a leaf, a branch of level 1 or a value a point read of a paged key space misses
/// goes to the cache at its second read, and one a scan misses at its third, whether the reads of the file are warm or
/// cold, as the array the first read refused when the collector has not taken it, and a delta page goes with the leaf
/// of its record, while the branches of level 2 or more, roots, the lookups of a commit and the reads of a resident key
/// space cache at once. The tree tests run on a cache with a budget of its own, which only the test ticks.
class ScreenedReadsTest {
    private static final long MIB = 1 << 20;
    private static final byte[] KEY = new byte[32];
    private static final int KEYS = 20_000;

    /// A tree on a data file whose cache has a budget of its own, which screens while the file's reads are warm.
    private static final class Tree implements AutoCloseable {
        final PageCache cache;
        final PageFile pages;
        final FreeSpace space;
        final Random random;
        TreeRoot root;
        DeltaTable deltas;

        Tree(Path directory) throws Exception {
            this.cache = PageCache.isolated(64 * MIB);
            IdleTicker.unwatch(cache);
            this.pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(KEY, "AES")), cache, new HeapPool(1 << 20));
            // Whatever a read of the test's disk takes
            pages.forceWarmReadsForTests(true);
            this.space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            this.random = new Random(5);
            this.deltas = DeltaTable.EMPTY;
        }

        void apply(List<TreeUpdate.Op> ops) throws Exception {
            var writer = pages.writer(random.nextLong());
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, List.of());
            deltas = update.finishDeltas(deltas);
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
            space.rewindRuns();
        }

        /// Fills the tree with `keys` keys of values of `length` bytes and empties the cache.
        Tree filled(int keys, int length) throws Exception {
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < keys; i++) {
                ops.add(new TreeUpdate.Op(segment(key(i)), segment(new byte[length])));
            }
            apply(ops);
            cache.clearForTests();
            return this;
        }

        /// {@return the references from the root to the leaf of `key`, the leaf last; the cache stays as it is}
        List<PageRef> path(byte[] key) {
            var path = new ArrayList<PageRef>();
            var ref = root.ref();
            while (true) {
                path.add(ref);
                var held = pages.peek(ref, PageFile.NODE);
                var unit = held != null ? held : pages.read(ref, PageFile.NODE, false);
                if (TreeNodes.isLeaf(unit)) {
                    return path;
                }
                ref = TreeNodes.childRef(unit, TreeNodes.childFor(unit, MemorySegment.ofArray(key)));
            }
        }

        /// {@return the leaf of `key`, read as a read of a paged key space reads it}
        byte[] leaf(int key) {
            return TreeSearch.leaf(pages, root, key(key), PageCache.POINT);
        }

        /// {@return the value of `key`, read as a read of a paged key space reads it}
        byte[] value(int key) {
            var leaf = leaf(key);
            var entry = TreeSearch.entryOf(leaf, key(key));
            assertTrue(entry != -1, "no key " + key);
            return TreeSearch.valueBytes(pages, leaf, entry, PageCache.POINT);
        }

        @Override
        public void close() {
            pages.close();
        }
    }

    private static byte[] key(int i) {
        return "k%06d".formatted(i).getBytes();
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    @Nested
    @DisplayName("a read of a tree")
    class Trees {
        @Test
        @DisplayName("of a point read caches a leaf and its branch of level 1 it read once only at the second read, and the higher branches and the root at the first")
        void leafAtSecondRead(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var path = tree.path(key(100));
                assertTrue(path.size() > 2, "a tree of " + path.size() + " levels");
                var leaf = path.getLast();
                var parent = path.get(path.size() - 2);
                tree.leaf(100);
                for (var ref : path.subList(0, path.size() - 2)) {
                    assertNotNull(tree.cache.peek(ref), "the branch " + ref);
                }
                assertNull(tree.cache.peek(parent), "the branch of level 1 read once");
                var refusedParent = tree.cache.recallWeak(PageCache.dataKey(parent.page(), parent.slot()), parent);
                assertNotNull(refusedParent, "the branch of level 1 the first read refused");
                assertEquals(1, TreeNodes.level(refusedParent));
                assertNull(tree.cache.peek(leaf), "the leaf read once");
                var bytes = tree.cache.size();
                var read = tree.leaf(100);
                assertSame(read, tree.cache.peek(leaf), "the leaf read twice");
                assertTrue(tree.cache.referencedForTests(leaf));
                assertSame(refusedParent, tree.cache.peek(parent), "the branch of level 1 read twice");
                assertTrue(tree.cache.referencedForTests(parent));
                assertEquals(bytes + refusedParent.length + read.length, tree.cache.size());
                assertSame(read, tree.leaf(100), "a hit");
            }
        }

        @Test
        @DisplayName("caches the branches of level 1 a scan reads among its first units at their third scan, and those past them at once")
        void scannedBranches(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var firstPath = tree.path(key(0));
                var lastPath = tree.path(key(KEYS - 1));
                assertTrue(firstPath.size() > 2, "a tree of " + firstPath.size() + " levels");
                var firstParent = firstPath.get(firstPath.size() - 2);
                var lastParent = lastPath.get(lastPath.size() - 2);
                assertNotEquals(firstParent, lastParent);
                tree.cache.clearForTests();
                var cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                var entries = 0;
                while (cursor.next()) {
                    entries++;
                }
                assertEquals(KEYS, entries);
                for (var ref : firstPath.subList(0, firstPath.size() - 2)) {
                    assertNotNull(tree.cache.peek(ref), "the branch " + ref);
                }
                assertNull(tree.cache.peek(firstParent), "a branch of level 1 among the first units the scan reads");
                assertNotNull(tree.cache.peek(lastParent), "a branch of level 1 past them");
                var refusedParent = tree.cache.recallWeak(PageCache.dataKey(firstParent.page(), firstParent.slot()), firstParent);
                assertNotNull(refusedParent, "the branch of level 1 the first scan refused");
                cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                assertTrue(cursor.next());
                assertNull(tree.cache.peek(firstParent), "a branch of level 1 the scan read twice");
                assertEquals(2, tree.cache.reads(firstParent), "the reads of the branch the doorkeeper saw");
                cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                assertTrue(cursor.next());
                assertSame(refusedParent, tree.cache.peek(firstParent), "a branch of level 1 the scan read three times");
                assertTrue(tree.cache.referencedForTests(firstParent));
                // A scan that does not screen caches the branches of level 1 it reads at once
                tree.cache.clearForTests();
                cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, false);
                assertTrue(cursor.next());
                assertNotNull(tree.cache.peek(firstParent), "a branch of level 1 an unscreened scan read");
            }
        }

        @Test
        @DisplayName("of a point read caches a value it read once only at the second read")
        void valueAtSecondRead(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(200, 3_000)) {
                var leaf = tree.leaf(7);
                var entry = TreeSearch.entryOf(leaf, key(7));
                assertFalse(TreeNodes.isInline(leaf, entry), "the value is inline");
                var blob = TreeNodes.blobRef(leaf, entry);
                tree.cache.clearForTests();
                assertArrayEquals(new byte[3_000], tree.value(7));
                assertNull(tree.cache.peek(blob), "the value read once");
                assertArrayEquals(new byte[3_000], tree.value(7));
                assertNotNull(tree.cache.peek(blob), "the value read twice");
            }
        }

        @Test
        @DisplayName("caches the root of a tree of one leaf at the first read")
        void rootLeaf(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(3, 10)) {
                assertTrue(TreeNodes.isLeaf(tree.leaf(1)));
                assertNotNull(tree.cache.peek(tree.root.ref()), "the root");
            }
        }

        @Test
        @DisplayName("of a point read caches at the second read the very leaf and value the first read refused")
        void recalled(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(200, 3_000)) {
                var leaf = tree.path(key(7)).getLast();
                tree.cache.clearForTests();
                var first = tree.leaf(7);
                var entry = TreeSearch.entryOf(first, key(7));
                var blob = TreeNodes.blobRef(first, entry);
                var value = TreeSearch.value(tree.pages, first, entry, true, true, PageCache.POINT);
                assertNull(tree.cache.peek(leaf), "the leaf read once");
                assertNull(tree.cache.peek(blob), "the value read once");
                var second = tree.leaf(7);
                assertSame(first, second, "the leaf read twice");
                assertSame(first, tree.cache.peek(leaf));
                assertTrue(tree.cache.referencedForTests(leaf));
                assertEquals(value, TreeSearch.value(tree.pages, second, entry, true, true, PageCache.POINT));
                assertNotNull(tree.cache.peek(blob), "the value read twice");
            }
        }

        @Test
        @DisplayName("screens what it reads while the reads of the file are cold, as the first read leaves the unit in the OS cache, scans that read ahead included")
        void coldReads(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(2_000, 3_000)) {
                tree.pages.forceColdReadsForTests(true);
                var leaf = tree.path(key(100)).getLast();
                tree.leaf(100);
                assertNull(tree.cache.peek(leaf), "the leaf read once from a cold file");
                assertTrue(tree.cache.doorkeeperForTests().keysForTests() > 0, "the doorkeeper never started");
                tree.leaf(100);
                assertNotNull(tree.cache.peek(leaf), "the leaf read twice from a cold file");

                tree.cache.clearForTests();
                var from = key(500);
                var to = key(540);
                var leaves = new ArrayList<PageRef>();
                for (var i = 500; i < 540; i++) {
                    var ref = tree.path(key(i)).getLast();
                    if (leaves.isEmpty() || !leaves.getLast().equals(ref)) {
                        leaves.add(ref);
                    }
                }
                for (var pass = 0; pass < 3; pass++) {
                    var cursor = new TreeCursor(tree.pages, tree.root, from, to, false, 40, true);
                    var entries = 0;
                    try {
                        while (cursor.next()) {
                            assertEquals(3_000, cursor.value().byteSize());
                            if (pass > 0) {
                                assertEquals(0, cursor.heldBytesForTests(), "scan " + pass + " read ahead what a scan read before");
                            }
                            entries++;
                        }
                    } finally {
                        cursor.release();
                    }
                    assertEquals(40, entries);
                    if (pass < 2) {
                        for (var ref : leaves) {
                            assertNull(tree.cache.peek(ref), "a leaf cold scans read " + (pass + 1) + " times");
                        }
                    }
                }
                for (var ref : leaves) {
                    assertNotNull(tree.cache.peek(ref), "a leaf cold scans read three times");
                }
            }
        }

        @Test
        @DisplayName("of a key a commit writes caches the leaf at once, as the next checkpoint reads it to replace it")
        void lookups(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var leaf = tree.path(key(100)).getLast();
                assertTrue(TreeSearch.contains(tree.pages, tree.root, MemorySegment.ofArray(key(100))));
                assertNotNull(tree.cache.peek(leaf), "the leaf a lookup read");
                assertEquals(0, tree.cache.doorkeeperForTests().keysForTests(), "the lookup was screened");
            }
        }

        @Test
        @DisplayName("of a point read of a delta record caches neither its leaf, its base nor its delta page at the first read, refuses the delta page with the leaf, and caches the leaf the first read made at the second")
        void deltaRecords(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))));
                var record = tree.path(key(100)).getLast();
                assertTrue(record.isRecord(), "the leaf of the key is no delta record");
                var base = TreeNodes.base(tree.pages.read(record, PageFile.NODE, false), record);
                var page = record.unit();
                tree.cache.clearForTests();
                var first = tree.leaf(100);
                assertNull(tree.cache.peek(record), "the leaf of the record read once");
                assertNull(tree.cache.peek(base), "the base of the record read once");
                assertNull(tree.cache.peek(page), "the delta page of a leaf the cache refused");
                var refusedPage = tree.cache.recallWeak(PageCache.dataKey(page.page(), page.slot()), page);
                assertNotNull(refusedPage, "the delta page refused with the leaf");
                assertEquals(PageFormat.DELTA, refusedPage[PageFormat.KIND]);
                assertEquals(PageFormat.cachedLength(1, Bytes.i32(refusedPage, PageFormat.LENGTH)), refusedPage.length,
                        "the delta page is not opened as the cache keeps it");
                var leaf = tree.leaf(100);
                assertSame(first, leaf, "the leaf the first read made");
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record read twice");
                assertTrue(tree.cache.referencedForTests(record));
                assertNull(tree.cache.peek(base), "the base of a leaf read back from the first read");
                assertNull(tree.cache.peek(page), "the delta page of a leaf read back from the first read");

                // Once the collector took the first read's leaf, the second makes it again, its base and its delta page
                // as the first entries to evict
                tree.cache.clearForTests();
                assertTrue(tree.cache.doorkeeperForTests().holds(PageCache.dataKey(record.page(), record.slot()), record.nonceHigh()));
                leaf = tree.leaf(100);
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record made again");
                assertNotNull(tree.cache.peek(base), "the base of the record made again, as the first entry to evict");
                assertFalse(tree.cache.referencedForTests(base), "the base counts as read");
                assertNotNull(tree.cache.peek(page), "the delta page of the record made again, as the first entry to evict");
                assertFalse(tree.cache.referencedForTests(page), "the delta page counts as read");
                var entry = TreeSearch.entryOf(leaf, key(100));
                assertArrayEquals(new byte[151], TreeSearch.valueBytes(tree.pages, leaf, entry, PageCache.POINT));
            }
        }

        @Test
        @DisplayName("gives the reads of the other records of a delta page the page a refused leaf left weakly held, and caches that array with the first leaf the cache takes")
        void sharedDeltaPage(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                // Keys in leaves far apart, whose records the checkpoint packs into its delta pages
                var keys = new int[]{100, 3_000, 6_000, 9_000, 12_000, 15_000};
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var key : keys) {
                    ops.add(new TreeUpdate.Op(segment(key(key)), segment(new byte[151])));
                }
                tree.apply(ops);
                var page = tree.path(key(keys[0])).getLast().unit();
                // The keys whose records share the delta page of the first
                var sharing = new ArrayList<Integer>();
                for (var key : keys) {
                    var record = tree.path(key(key)).getLast();
                    assertTrue(record.isRecord(), "the leaf of key " + key + " is no delta record");
                    if (record.unit().equals(page)) {
                        sharing.add(key);
                    }
                }
                assertTrue(sharing.size() >= 3, "only " + sharing.size() + " records share a delta page");
                var pageKey = PageCache.dataKey(page.page(), page.slot());
                tree.cache.clearForTests();

                tree.leaf(sharing.get(0));
                var refusedPage = tree.cache.recallWeak(pageKey, page);
                assertNotNull(refusedPage, "the delta page refused with the first leaf");
                var refusals = tree.cache.refusalsForTests();
                // The first read of a neighbour takes the page from the refused units and refuses only its own leaf
                var neighbour = tree.path(key(sharing.get(1))).getLast();
                var second = tree.leaf(sharing.get(1));
                assertNull(tree.cache.peek(neighbour), "the neighbour's leaf read once");
                assertNull(tree.cache.peek(page), "the delta page of two refused leaves");
                assertSame(refusedPage, tree.cache.recallWeak(pageKey, page), "the delta page read again");
                assertEquals(refusals + 2, tree.cache.refusalsForTests(), "the refusals of the neighbour's first read: its branch of level 1 and its leaf");
                var entry = TreeSearch.entryOf(second, key(sharing.get(1)));
                assertArrayEquals(new byte[151], TreeSearch.valueBytes(tree.pages, second, entry, PageCache.POINT));

                // A lookup of a commit caches the leaf it reads at once, and the delta page it takes from the refused
                // units with it, as the first entry to evict
                var third = tree.path(key(sharing.get(2))).getLast();
                assertTrue(TreeSearch.contains(tree.pages, tree.root, MemorySegment.ofArray(key(sharing.get(2)))));
                assertNotNull(tree.cache.peek(third), "the leaf a lookup read");
                assertSame(refusedPage, tree.cache.peek(page), "the delta page of a leaf the cache took");
                assertFalse(tree.cache.referencedForTests(page), "the delta page counts as read");
            }
        }

        @Test
        @DisplayName("caches the units a scan reads first at their third scan, and past them those it caches anyway")
        void scans(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                var entries = 0;
                while (cursor.next()) {
                    entries++;
                }
                assertEquals(KEYS, entries);
                var first = tree.path(key(0)).getLast();
                var last = tree.path(key(KEYS - 1)).getLast();
                assertNull(tree.cache.peek(first), "a leaf of the first the scan reads");
                assertNotNull(tree.cache.peek(last), "a leaf past them, with room in the cache");
                assertFalse(tree.cache.referencedForTests(last));
                cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                assertTrue(cursor.next());
                assertNull(tree.cache.peek(first), "a leaf of the first the scan read twice");
                assertEquals(2, tree.cache.reads(first), "the reads of the leaf the doorkeeper saw");
                cursor = new TreeCursor(tree.pages, tree.root, null, null, false, 0, true);
                assertTrue(cursor.next());
                assertNotNull(tree.cache.peek(first), "a leaf of the first the scan read three times");
                assertTrue(tree.cache.referencedForTests(first));
            }
        }

        @Test
        @DisplayName("readers on many threads read every value right through screened reads of delta records and branches while the collector takes refused units, and a checkpoint then replaces the records they read")
        void concurrentDeltaReads(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var random = new Random(17);
                var values = new int[KEYS];
                // Rounds of scattered changes, so most leaves become delta records packed into shared delta pages
                for (var round = 1; round <= 4; round++) {
                    var changed = new TreeSet<Integer>();
                    for (var i = 0; i < 300; i++) {
                        changed.add(random.nextInt(KEYS));
                    }
                    var ops = new ArrayList<TreeUpdate.Op>();
                    for (var key : changed) {
                        values[key] = round;
                        var value = new byte[150 + round];
                        value[0] = (byte) round;
                        ops.add(new TreeUpdate.Op(segment(key(key)), segment(value)));
                    }
                    tree.apply(ops);
                }
                var records = 0;
                for (var key = 0; key < KEYS; key += 100) {
                    if (tree.path(key(key)).getLast().isRecord()) {
                        records++;
                    }
                }
                assertTrue(records > 20, records + " delta records among 200 keys");
                tree.cache.clearForTests();
                var root = tree.root;
                var failures = new ConcurrentLinkedQueue<String>();
                var threads = new Thread[8];
                for (var t = 0; t < threads.length; t++) {
                    var seed = random.nextLong();
                    var collector = t == 0;
                    threads[t] = Thread.ofPlatform().start(() -> {
                        var local = new Random(seed);
                        for (var i = 0; i < 4_000 && failures.isEmpty(); i++) {
                            try {
                                if (collector && i % 500 == 0) {
                                    System.gc();
                                }
                                var key = local.nextInt(KEYS);
                                if (local.nextInt(50) == 0) {
                                    var cursor = new TreeCursor(tree.pages, root, key(key), null, false, 0, true);
                                    for (var n = key; n < Math.min(KEYS, key + 60) && cursor.next(); n++) {
                                        if (!Arrays.equals(key(n), cursor.key().toArray(ValueLayout.JAVA_BYTE))
                                                || cursor.value().byteSize() != 150 + values[n]) {
                                            failures.add("the scan from " + key + " at " + n);
                                        }
                                    }
                                    cursor.release();
                                } else {
                                    var screen = local.nextBoolean() ? PageCache.POINT : PageCache.SCAN;
                                    var leaf = TreeSearch.leaf(tree.pages, root, key(key), screen);
                                    var entry = TreeSearch.entryOf(leaf, key(key));
                                    var value = entry == -1 ? null : TreeSearch.valueBytes(tree.pages, leaf, entry, screen);
                                    if (value == null || value.length != 150 + values[key] || (values[key] != 0 && value[0] != values[key])) {
                                        failures.add("the value of " + key);
                                    }
                                }
                            } catch (RuntimeException exception) {
                                failures.add(exception.toString());
                            }
                        }
                    });
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertTrue(failures.isEmpty(), () -> failures.size() + " wrong reads, first: " + failures.peek());

                // A checkpoint that rewrites records whose delta pages the readers left refused, then every value
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var key = 50; key < KEYS; key += 97) {
                    values[key] = 5;
                    var value = new byte[155];
                    value[0] = 5;
                    ops.add(new TreeUpdate.Op(segment(key(key)), segment(value)));
                }
                tree.apply(ops);
                for (var key = 0; key < KEYS; key++) {
                    var leaf = tree.leaf(key);
                    var entry = TreeSearch.entryOf(leaf, key(key));
                    assertTrue(entry != -1, "no key " + key);
                    var value = TreeSearch.valueBytes(tree.pages, leaf, entry, PageCache.POINT);
                    assertEquals(150 + values[key], value.length, "key " + key);
                    if (values[key] != 0) {
                        assertEquals(values[key], value[0], "key " + key);
                    }
                }
            }
        }

        @Test
        @DisplayName("of a scan counts one read of a leaf and of a value for each scan, the recall of the cursor and that of the file included, whether or not the collector took what the first scan refused")
        void scanCountsOnce(@TempDir Path directory) throws Exception {
            for (var collected : new boolean[]{false, true}) {
                try (var tree = new Tree(java.nio.file.Files.createDirectories(directory.resolve("collected-" + collected))).filled(200, 3_000)) {
                    var leaf = tree.path(key(7)).getLast();
                    var unit = tree.pages.read(leaf, PageFile.NODE, false);
                    var blob = TreeNodes.blobRef(unit, TreeSearch.entryOf(unit, key(7)));
                    tree.cache.clearForTests();
                    for (var scan = 0; scan < 3; scan++) {
                        var cursor = new TreeCursor(tree.pages, tree.root, key(7), key(8), false, 1, true);
                        try {
                            assertTrue(cursor.next());
                            assertArrayEquals(new byte[3_000], cursor.value().toArray(ValueLayout.JAVA_BYTE));
                            assertFalse(cursor.next());
                        } finally {
                            cursor.release();
                        }
                        if (scan < 2) {
                            assertNull(tree.cache.peek(leaf), "the leaf after scan " + scan + ", collected " + collected);
                            assertNull(tree.cache.peek(blob), "the value after scan " + scan + ", collected " + collected);
                            assertEquals(scan + 1, tree.cache.reads(leaf), "the reads of the leaf after scan " + scan);
                            assertEquals(scan + 1, tree.cache.reads(blob), "the reads of the value after scan " + scan);
                            if (collected) {
                                // As a collection that took the arrays the scan refused leaves the cache
                                tree.cache.clearForTests();
                            }
                        }
                    }
                    assertNotNull(tree.cache.peek(leaf), "the leaf a scan read three times, collected " + collected);
                    assertNotNull(tree.cache.peek(blob), "the value a scan read three times, collected " + collected);
                    assertTrue(tree.cache.referencedForTests(leaf));
                    assertTrue(tree.cache.referencedForTests(blob));
                }
            }
        }

        @Test
        @DisplayName("of a scan leaves the leaf and the value it refused where they were at the second read, reading nothing, and caches those very arrays at the third")
        void scanRecalled(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(200, 3_000)) {
                var leaf = tree.path(key(7)).getLast();
                tree.cache.clearForTests();
                var first = TreeSearch.leaf(tree.pages, tree.root, key(7), PageCache.SCAN);
                var blob = TreeNodes.blobRef(first, TreeSearch.entryOf(first, key(7)));
                var value = tree.pages.read(blob, PageFormat.VALUE, true, true, PageCache.SCAN);
                assertNull(tree.cache.peek(leaf), "the leaf read once");
                assertNull(tree.cache.peek(blob), "the value read once");
                var bytes = tree.cache.size();
                assertSame(first, TreeSearch.leaf(tree.pages, tree.root, key(7), PageCache.SCAN), "the leaf read twice");
                assertSame(value, tree.pages.read(blob, PageFormat.VALUE, true, true, PageCache.SCAN), "the value read twice");
                assertNull(tree.cache.peek(leaf), "the leaf read twice");
                assertNull(tree.cache.peek(blob), "the value read twice");
                assertEquals(bytes, tree.cache.size());
                assertSame(first, TreeSearch.leaf(tree.pages, tree.root, key(7), PageCache.SCAN), "the leaf read three times");
                assertSame(value, tree.pages.read(blob, PageFormat.VALUE, true, true, PageCache.SCAN), "the value read three times");
                assertSame(first, tree.cache.peek(leaf));
                assertSame(value, tree.cache.peek(blob));
                assertTrue(tree.cache.referencedForTests(leaf));
                assertTrue(tree.cache.referencedForTests(blob));
                assertEquals(bytes + first.length + value.length, tree.cache.size());
            }
        }

        @Test
        @DisplayName("of a scan of a delta record caches neither its leaf, its base nor its delta page at the first two reads, refuses the delta page with the leaf, and caches the leaf the first read made at the third")
        void scanDeltaRecords(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))));
                var record = tree.path(key(100)).getLast();
                assertTrue(record.isRecord(), "the leaf of the key is no delta record");
                var base = TreeNodes.base(tree.pages.read(record, PageFile.NODE, false), record);
                var page = record.unit();
                var pageKey = PageCache.dataKey(page.page(), page.slot());
                tree.cache.clearForTests();
                var first = TreeSearch.leaf(tree.pages, tree.root, key(100), PageCache.SCAN);
                var refusedPage = tree.cache.recallWeak(pageKey, page);
                assertNotNull(refusedPage, "the delta page refused with the leaf");
                for (var read = 1; read <= 2; read++) {
                    if (read == 2) {
                        assertSame(first, TreeSearch.leaf(tree.pages, tree.root, key(100), PageCache.SCAN));
                    }
                    assertNull(tree.cache.peek(record), "the leaf of the record read " + read + " times");
                    assertNull(tree.cache.peek(base), "the base of the record read " + read + " times");
                    assertNull(tree.cache.peek(page), "the delta page of a leaf read " + read + " times");
                    assertSame(refusedPage, tree.cache.recallWeak(pageKey, page), "the delta page, which the other records share");
                }
                var leaf = TreeSearch.leaf(tree.pages, tree.root, key(100), PageCache.SCAN);
                assertSame(first, leaf, "the leaf the first read made");
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record read three times");
                assertTrue(tree.cache.referencedForTests(record));
                assertNull(tree.cache.peek(base), "the base of a leaf read back from the first read");
                assertNull(tree.cache.peek(page), "the delta page of a leaf read back from the first read");

                // With the arrays the reads refused collected after each read, the third read makes the leaf again,
                // its delta page as the first entry to evict
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[152]))));
                record = tree.path(key(100)).getLast();
                assertTrue(record.isRecord(), "the leaf of the key is no delta record");
                page = record.unit();
                tree.cache.clearForTests();
                for (var read = 1; read <= 2; read++) {
                    TreeSearch.leaf(tree.pages, tree.root, key(100), PageCache.SCAN);
                    assertNull(tree.cache.peek(page), "the delta page of a leaf read " + read + " times");
                    tree.cache.clearForTests();
                    assertEquals(read, tree.cache.reads(record), "the reads of the record");
                }
                leaf = TreeSearch.leaf(tree.pages, tree.root, key(100), PageCache.SCAN);
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record made again");
                assertNotNull(tree.cache.peek(page), "the delta page of the record made again, as the first entry to evict");
                assertFalse(tree.cache.referencedForTests(page), "the delta page counts as read");
                var entry = TreeSearch.entryOf(leaf, key(100));
                assertArrayEquals(new byte[152], TreeSearch.valueBytes(tree.pages, leaf, entry, PageCache.SCAN));
            }
        }

        @Test
        @DisplayName("finds each unit it read once whether or not it was cached, in a tree read again and again")
        void readsBack(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(2_000, 600)) {
                var random = new Random(11);
                for (var i = 0; i < 20_000; i++) {
                    var key = random.nextInt(2_000);
                    assertArrayEquals(new byte[600], tree.value(key), "key " + key);
                }
            }
        }
    }

    @Nested
    @DisplayName("the page file")
    class Files {
        @Test
        @DisplayName("turns screening on for its cache, whether its own reads are warm or cold")
        void turnsScreeningOn(@TempDir Path directory) throws Exception {
            var cache = PageCache.isolated(64 * MIB);
            IdleTicker.unwatch(cache);
            var ref = new PageRef(10, 1, 3, 4);
            var unit = new byte[PageFormat.SIZE];
            PageFile.header(unit, PageFormat.LEAF, 0, 10, 1, 0, 3, 4);
            cache.putScreened(ref, unit, PageCache.POINT);
            assertSame(unit, cache.peek(ref), "a cache of no file");
            cache.clearForTests();
            try (var pages = PageFile.open(directory, new PageCipher(null), cache, new HeapPool(1 << 20))) {
                assertEquals(0, pages.readMicrosForTests(), "the average of a file that read nothing");
                cache.putScreened(ref, unit, PageCache.POINT);
                assertNull(cache.peek(ref), "a read of a warm file");
                pages.forceColdReadsForTests(true);
                var other = new PageRef(11, 1, 3, 5);
                var otherUnit = new byte[PageFormat.SIZE];
                PageFile.header(otherUnit, PageFormat.LEAF, 0, 11, 1, 0, 3, 5);
                cache.putScreened(other, otherUnit, PageCache.POINT);
                assertNull(cache.peek(other), "a read of a cold file");
            }
        }
    }

    @Nested
    @DisplayName("the clock")
    class Clock {
        private static PageRef ref(int page) {
            return new PageRef(page, 1, 1_000L + page, page);
        }

        private static byte[] unit(PageRef ref) {
            var unit = new byte[PageFormat.SIZE];
            PageFile.header(unit, PageFormat.LEAF, 0, ref.page(), 1, 0, ref.nonceHigh(), ref.nonceLow());
            return unit;
        }

        @Test
        @DisplayName("records in the doorkeeper the units a read used that it evicts, never those cached as written and evicted unread")
        void recordsOnlyReadUnits() {
            var cache = PageCache.isolated(64L * PageFormat.SIZE);
            IdleTicker.unwatch(cache);
            try {
                cache.screen(true);
                // A first read the cache refuses, so the doorkeeper records
                var first = ref(1);
                cache.putScreened(first, unit(first), PageCache.POINT);
                assertNull(cache.peek(first), "refused at its first read");
                // Cached as read, cached as written then read, and cached as written only, in that order
                var read = ref(10);
                cache.put(read, unit(read), true);
                var readLater = ref(11);
                cache.put(readLater, unit(readLater), false);
                assertNotNull(cache.get(PageCache.dataKey(readLater.page(), readLater.slot()), readLater));
                for (var page = 100; page < 400; page++) {
                    var written = ref(page);
                    cache.put(written, unit(written), false);
                }
                assertNull(cache.peek(read), "evicted");
                assertNull(cache.peek(readLater), "evicted");
                assertTrue(cache.seen(read), "a unit cached as read and evicted");
                assertTrue(cache.seen(readLater), "a unit cached as written, read, and evicted");
                var unread = 0;
                for (var page = 100; page < 200; page++) {
                    var written = ref(page);
                    assertNull(cache.peek(written), "evicted");
                    if (cache.seen(written)) {
                        unread++;
                    }
                }
                assertEquals(0, unread, "units cached as written and evicted unread that the doorkeeper records");
                assertEquals(0, cache.doorkeeperForTests().rotationsForTests(), "rotations of the doorkeeper");
                assertTrue(cache.seen(first), "the first read the doorkeeper recorded");
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Stores {
        private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
        private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());

        // Opened while every new page file reads warm, whatever the test's disk takes
        private static Ledger open(Path directory, long largeValues) {
            PageFile.forceWarmNewForTests(true);
            try {
                return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                        .keyspace(SESSIONS).pageCache(64 * MIB).largeValues(largeValues).build();
            } finally {
                PageFile.forceWarmNewForTests(false);
            }
        }

        private static String key(int i) {
            return "m%06d".formatted(i);
        }

        private static byte[] value(int i, int length) {
            var value = new byte[length];
            value[0] = (byte) i;
            value[length - 1] = (byte) (i >> 8);
            return value;
        }

        @Test
        @DisplayName("caches the leaf and the value of a paged key read once only once it reads them again, values of the file of large values included")
        void pagedGets(@TempDir Path directory) throws Exception {
            for (var largeValues : new long[]{Long.MAX_VALUE, 4_096}) {
                var store = directory.resolve("large-" + (largeValues != Long.MAX_VALUE));
                try (var ledger = open(store, largeValues)) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 2_000; i++) {
                        batch.put(MESSAGES, key(i), value(i, 6_000));
                    }
                    ledger.commit(batch).awaitDurable();
                }
                // Reopened, so memory holds no version and every read goes to the tree
                try (var ledger = (LiveLedger) open(store, largeValues)) {
                    // The first read caches the root and the branches above the parents of the leaves; then the first
                    // reads of keys in other leaves cache nothing
                    ledger.clearPageCacheForTests();
                    assertArrayEquals(value(1_999, 6_000), ledger.get(MESSAGES, key(1_999)).orElseThrow());
                    var branches = ledger.pageCacheBytesForTests();
                    // Farther apart than the entries of a leaf
                    for (var i = 0; i < 1_600; i += 400) {
                        assertArrayEquals(value(i, 6_000), ledger.get(MESSAGES, key(i)).orElseThrow());
                    }
                    assertEquals(branches, ledger.pageCacheBytesForTests(), "the bytes cached by first reads");
                    for (var i = 0; i < 1_600; i += 400) {
                        assertArrayEquals(value(i, 6_000), ledger.get(MESSAGES, key(i)).orElseThrow());
                    }
                    assertTrue(ledger.pageCacheBytesForTests() >= branches + 4 * 6_000L,
                            ledger.pageCacheBytesForTests() + " bytes cached after the branches' " + branches);
                }
            }
        }

        @Test
        @DisplayName("caches the leaves and the values of a paged range a scan reads only at the third scan, values of the file of large values included")
        void pagedScans(@TempDir Path directory) throws Exception {
            for (var largeValues : new long[]{Long.MAX_VALUE, 4_096}) {
                var store = directory.resolve("large-" + (largeValues != Long.MAX_VALUE));
                try (var ledger = open(store, largeValues)) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 2_000; i++) {
                        batch.put(MESSAGES, key(i), value(i, 6_000));
                    }
                    ledger.commit(batch).awaitDurable();
                }
                try (var ledger = (LiveLedger) open(store, largeValues)) {
                    ledger.clearPageCacheForTests();
                    // Caches the branches on the way to the range
                    assertArrayEquals(value(1_999, 6_000), ledger.get(MESSAGES, key(1_999)).orElseThrow());
                    var branches = ledger.pageCacheBytesForTests();
                    var range = KeyRange.between(key(0), key(20));
                    for (var scan = 0; scan < 3; scan++) {
                        var seen = new int[1];
                        ledger.forEach(MESSAGES, range, (key, value) -> {
                            assertArrayEquals(value(seen[0], 6_000), value);
                            seen[0]++;
                            return true;
                        });
                        assertEquals(20, seen[0]);
                        if (scan < 2) {
                            assertEquals(branches, ledger.pageCacheBytesForTests(), "the bytes cached by scan " + scan);
                        }
                    }
                    assertTrue(ledger.pageCacheBytesForTests() >= branches + 20 * 6_000L,
                            ledger.pageCacheBytesForTests() + " bytes cached after the branches' " + branches);
                }
            }
        }

        @Test
        @DisplayName("reads every value back through screened reads, scans and gets, after a reopen")
        void readsBack(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, 4_096)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 3_000; i++) {
                    batch.put(MESSAGES, key(i), value(i, i % 3 == 0 ? 5_000 : 300));
                    batch.put(SESSIONS, key(i), value(i, 200));
                }
                ledger.commit(batch).awaitDurable();
            }
            try (var ledger = open(directory, 4_096)) {
                for (var round = 0; round < 3; round++) {
                    for (var i = 0; i < 3_000; i += 13) {
                        assertArrayEquals(value(i, i % 3 == 0 ? 5_000 : 300), ledger.get(MESSAGES, key(i)).orElseThrow());
                        assertArrayEquals(value(i, 200), ledger.get(SESSIONS, key(i)).orElseThrow());
                    }
                    var seen = new int[1];
                    ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                        var i = Integer.parseInt(key.substring(1));
                        assertArrayEquals(value(i, i % 3 == 0 ? 5_000 : 300), value);
                        seen[0]++;
                        return true;
                    });
                    assertEquals(3_000, seen[0]);
                }
            }
        }
    }
}
