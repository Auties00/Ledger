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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The reads the page cache screens: a leaf or a value a read of a paged key space misses goes to the cache at its
/// second read, whether the reads of the file are warm or cold, as the array the first read refused when the collector
/// has not taken it, while branches, roots, the lookups of a commit and the reads of a resident key space cache at once. The tree tests run on a cache with a budget
/// of its own, which only the test ticks.
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
            return TreeSearch.leaf(pages, root, key(key), true);
        }

        /// {@return the value of `key`, read as a read of a paged key space reads it}
        byte[] value(int key) {
            var leaf = leaf(key);
            var entry = TreeSearch.entryOf(leaf, key(key));
            assertTrue(entry != -1, "no key " + key);
            return TreeSearch.valueBytes(pages, leaf, entry, true);
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
        @DisplayName("caches a leaf it read once only at the second read, and the branches and the root at the first")
        void leafAtSecondRead(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                var path = tree.path(key(100));
                assertTrue(path.size() > 2, "a tree of " + path.size() + " levels");
                var leaf = path.getLast();
                tree.leaf(100);
                for (var ref : path.subList(0, path.size() - 1)) {
                    assertNotNull(tree.cache.peek(ref), "the branch " + ref);
                }
                assertNull(tree.cache.peek(leaf), "the leaf read once");
                var bytes = tree.cache.size();
                var read = tree.leaf(100);
                assertSame(read, tree.cache.peek(leaf), "the leaf read twice");
                assertTrue(tree.cache.referencedForTests(leaf));
                assertEquals(bytes + read.length, tree.cache.size());
                assertSame(read, tree.leaf(100), "a hit");
            }
        }

        @Test
        @DisplayName("caches a value it read once only at the second read")
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
        @DisplayName("caches at the second read the very leaf and value the first read refused")
        void recalled(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(200, 3_000)) {
                var leaf = tree.path(key(7)).getLast();
                tree.cache.clearForTests();
                var first = tree.leaf(7);
                var entry = TreeSearch.entryOf(first, key(7));
                var blob = TreeNodes.blobRef(first, entry);
                var value = TreeSearch.value(tree.pages, first, entry, true, true, true);
                assertNull(tree.cache.peek(leaf), "the leaf read once");
                assertNull(tree.cache.peek(blob), "the value read once");
                var second = tree.leaf(7);
                assertSame(first, second, "the leaf read twice");
                assertSame(first, tree.cache.peek(leaf));
                assertTrue(tree.cache.referencedForTests(leaf));
                assertEquals(value, TreeSearch.value(tree.pages, second, entry, true, true, true));
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
                for (var pass = 0; pass < 2; pass++) {
                    var cursor = new TreeCursor(tree.pages, tree.root, from, to, false, 40, true);
                    var entries = 0;
                    try {
                        while (cursor.next()) {
                            assertEquals(3_000, cursor.value().byteSize());
                            if (pass == 1) {
                                assertEquals(0, cursor.heldBytesForTests(), "a second scan read ahead what it read before");
                            }
                            entries++;
                        }
                    } finally {
                        cursor.release();
                    }
                    assertEquals(40, entries);
                    if (pass == 0) {
                        for (var ref : leaves) {
                            assertNull(tree.cache.peek(ref), "a leaf a cold scan read once");
                        }
                    }
                }
                for (var ref : leaves) {
                    assertNotNull(tree.cache.peek(ref), "a leaf a cold scan read twice");
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
        @DisplayName("of a delta record caches neither its leaf nor its base at the first read, keeps its delta page, and caches the leaf the first read made at the second")
        void deltaRecords(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory).filled(KEYS, 150)) {
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))));
                var record = tree.path(key(100)).getLast();
                assertTrue(record.isRecord(), "the leaf of the key is no delta record");
                var base = TreeNodes.base(tree.pages.read(record, PageFile.NODE, false), record);
                tree.cache.clearForTests();
                var first = tree.leaf(100);
                assertNull(tree.cache.peek(record), "the leaf of the record read once");
                assertNull(tree.cache.peek(base), "the base of the record read once");
                assertNotNull(tree.cache.peek(record.unit()), "the delta page, which the other records share");
                assertFalse(tree.cache.referencedForTests(record.unit()), "the delta page counts as read");
                var leaf = tree.leaf(100);
                assertSame(first, leaf, "the leaf the first read made");
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record read twice");
                assertTrue(tree.cache.referencedForTests(record));
                assertNull(tree.cache.peek(base), "the base of a leaf read back from the first read");

                // Once the collector took the first read's leaf, the second makes it again, its base as the first entry
                // to evict
                tree.cache.clearForTests();
                assertTrue(tree.cache.doorkeeperForTests().holds(PageCache.dataKey(record.page(), record.slot()), record.nonceHigh()));
                leaf = tree.leaf(100);
                assertSame(leaf, tree.cache.peek(record), "the leaf of the record made again");
                assertNotNull(tree.cache.peek(base), "the base of the record made again, as the first entry to evict");
                assertFalse(tree.cache.referencedForTests(base), "the base counts as read");
                var entry = TreeSearch.entryOf(leaf, key(100));
                assertArrayEquals(new byte[151], TreeSearch.valueBytes(tree.pages, leaf, entry, true));
            }
        }

        @Test
        @DisplayName("caches the units a scan reads first at their second scan, and past them those it caches anyway")
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
                assertNotNull(tree.cache.peek(first), "a leaf of the first the scan read twice");
                assertTrue(tree.cache.referencedForTests(first));
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
            cache.putScreened(ref, unit);
            assertSame(unit, cache.peek(ref), "a cache of no file");
            cache.clearForTests();
            try (var pages = PageFile.open(directory, new PageCipher(null), cache, new HeapPool(1 << 20))) {
                assertEquals(0, pages.readMicrosForTests(), "the average of a file that read nothing");
                cache.putScreened(ref, unit);
                assertNull(cache.peek(ref), "a read of a warm file");
                pages.forceColdReadsForTests(true);
                var other = new PageRef(11, 1, 3, 5);
                var otherUnit = new byte[PageFormat.SIZE];
                PageFile.header(otherUnit, PageFormat.LEAF, 0, 11, 1, 0, 3, 5);
                cache.putScreened(other, otherUnit);
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
                cache.putScreened(first, unit(first));
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
                    // The first read caches the branches; then the first reads of keys in other leaves cache nothing
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
