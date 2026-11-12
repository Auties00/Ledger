package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What a checkpoint leaves in the page cache: the nodes it reads to replace stay out of it or as cold as they were, a
/// node it finds it does not change counts as read, and what it writes evicts other entries only within an allowance.
/// The tree tests run on a cache with a budget of its own, as the process's budget is shared with the other tests.
class CheckpointCacheTest {
    private static final long MIB = 1 << 20;
    private static final byte[] KEY = new byte[32];
    private static final int KEYS = 20_000;
    private static final Keyspace<String, String> VALUES = Keyspace.paged("values", KeyCodec.utf8(), ValueCodec.utf8());

    /// A tree on a data file whose cache has a budget of its own, which only the test ticks.
    private static final class Tree implements AutoCloseable {
        final HeapPool pool;
        final PageCache cache;
        final PageFile pages;
        final FreeSpace space;
        final Random random;
        TreeRoot root;
        DeltaTable deltas;

        Tree(Path directory, long cacheBytes) throws Exception {
            this(directory, cacheBytes, true);
        }

        /// Makes the tree; a plaintext one reads each unit into one array of the pool, which an encrypted one does only
        /// for a unit it keeps whole.
        Tree(Path directory, long cacheBytes, boolean encrypted) throws Exception {
            this.pool = new HeapPool(1 << 20);
            this.cache = PageCache.isolated(cacheBytes);
            IdleTicker.unwatch(cache);
            this.pages = PageFile.open(directory, new PageCipher(encrypted ? new SecretKeySpec(KEY, "AES") : null), cache, pool);
            this.space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            this.random = new Random(5);
            this.deltas = DeltaTable.EMPTY;
        }

        /// Applies `ops`, reporting the arrays the pool gave meanwhile, each a read of the file as no flush ran yet.
        int apply(List<TreeUpdate.Op> ops, PageFile.Writer writer) throws Exception {
            var acquires = pool.acquiresForTests();
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, List.of());
            deltas = update.finishDeltas(deltas);
            var reads = pool.acquiresForTests() - acquires;
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
            space.rewindRuns();
            return reads;
        }

        int apply(List<TreeUpdate.Op> ops) throws Exception {
            return apply(ops, pages.writer(random.nextLong()));
        }

        /// Fills the tree with [#KEYS] keys and empties the cache.
        Tree filled() throws Exception {
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < KEYS; i++) {
                ops.add(new TreeUpdate.Op(segment(key(i)), segment(new byte[150])));
            }
            apply(ops);
            cache.clearForTests();
            return this;
        }

        /// {@return the references from the root to the leaf of `key`, the leaf last; the cache's entries stay cold}
        List<PageRef> path(byte[] key) {
            var path = new ArrayList<PageRef>();
            var ref = root.ref();
            while (true) {
                path.add(ref);
                var unit = unit(ref);
                if (TreeNodes.isLeaf(unit)) {
                    return path;
                }
                ref = TreeNodes.childRef(unit, TreeNodes.childFor(unit, MemorySegment.ofArray(key)));
            }
        }

        private byte[] unit(PageRef ref) {
            var held = pages.peek(ref, PageFile.NODE);
            return held != null ? held : pages.read(ref, PageFile.NODE, false);
        }

        /// Turns the leaves of `keys` into delta records.
        void makeRecords(int... keys) throws Exception {
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var key : keys) {
                ops.add(new TreeUpdate.Op(segment(key(key)), segment(new byte[151])));
            }
            apply(ops);
            for (var key : keys) {
                assertTrue(path(key(key)).getLast().isRecord(), "the leaf of key " + key + " is no delta record");
            }
        }

        /// Reads every key back through the cache as it stands, then from the file.
        void verify(int value) {
            for (var round = 0; round < 2; round++) {
                for (var i = 0; i < KEYS; i += 7) {
                    assertArrayEquals(new byte[value], read(pages, root, key(i)), "key " + i);
                }
                cache.clearForTests();
            }
            assertEquals(KEYS, root.count());
        }

        @Override
        public void close() {
            pages.close();
        }
    }

    private static byte[] key(int i) {
        return "k%06d".formatted(i).getBytes();
    }

    /// {@return the value the tree under `root` holds for `key`, read through the cache as a reader reads it}
    private static byte[] read(PageFile pages, TreeRoot root, byte[] key) {
        var leaf = TreeSearch.leaf(pages, root, key);
        var entry = TreeSearch.entryOf(leaf, key);
        assertTrue(entry != -1, () -> "no key " + new String(key));
        return bytes(TreeSearch.value(pages, leaf, entry, false));
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /// Fills the rest of `cache` with entries that count as read, at pages no tree of these tests reaches.
    private static void fillWithReadEntries(PageCache cache) {
        for (var i = 0; cache.hasRoom(PageFormat.SIZE); i++) {
            cache.put(new PageRef(1 << 30 | i, 1, 0x7777, i), new byte[PageFormat.SIZE], true);
        }
    }

    @Nested
    @DisplayName("the nodes a checkpoint reads")
    class Reads {
        @Test
        @DisplayName("are not cached when it replaces them")
        void replacedOnMiss(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled()) {
                var old = tree.path(key(100));
                var oldRoot = tree.root;
                tree.cache.clearForTests();
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))));
                for (var ref : old) {
                    assertNull(tree.cache.peek(ref), "the replaced node " + ref);
                }
                assertNull(tree.cache.peek(oldRoot.ref()));
                var path = tree.path(key(100));
                assertTrue(path.getLast().isRecord());
                for (var ref : path) {
                    assertNotNull(tree.cache.peek(ref), "the new node " + ref);
                    assertFalse(tree.cache.referencedForTests(ref), "the new node " + ref + " counts as read");
                }
                assertArrayEquals(new byte[151], read(tree.pages, tree.root, key(100)));
            }
        }

        @Test
        @DisplayName("stay as cold as they were when it replaces them, and leave at the next tick")
        void replacedOnHit(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB)) {
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var i = 0; i < KEYS; i++) {
                    ops.add(new TreeUpdate.Op(segment(key(i)), segment(new byte[150])));
                }
                tree.apply(ops);
                var old = tree.path(key(100));
                for (var ref : old) {
                    assertNotNull(tree.cache.peek(ref), "the written node " + ref);
                }
                tree.cache.tick();
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))));
                for (var ref : old) {
                    assertFalse(tree.cache.referencedForTests(ref), "the replaced node " + ref + " was promoted");
                }
                tree.cache.tick();
                for (var ref : old) {
                    assertNull(tree.cache.peek(ref), "the replaced node " + ref + " counted as used for the ticker");
                }
                for (var ref : tree.path(key(100))) {
                    assertNotNull(tree.cache.peek(ref), "the new node " + ref);
                }
            }
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = {"removing an absent key", "writing the value a key has"})
        @DisplayName("count as read when the writes change nothing under them")
        void survivors(String writes, @TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled()) {
                tree.makeRecords(100);
                var root = tree.root;
                for (var key : List.of(100, 5_000)) {
                    tree.cache.clearForTests();
                    var op = writes.startsWith("removing")
                            ? new TreeUpdate.Op(segment((new String(key(key)) + "x").getBytes()), null)
                            : new TreeUpdate.Op(segment(key(key)), segment(new byte[key == 100 ? 151 : 150]));
                    var path = tree.path(key(key));
                    tree.cache.clearForTests();
                    tree.apply(List.of(op));
                    assertEquals(root, tree.root);
                    for (var ref : path) {
                        assertNotNull(tree.cache.peek(ref), "the node " + ref);
                        assertTrue(tree.cache.referencedForTests(ref), "the node " + ref);
                    }
                }
            }
        }

        @Test
        @DisplayName("count as read in a tree of one leaf the writes do not change")
        void rootSurvivor(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB)) {
                tree.apply(List.of(new TreeUpdate.Op(segment(key(1)), segment(new byte[10]))));
                var root = tree.root;
                tree.cache.clearForTests();
                tree.apply(List.of(new TreeUpdate.Op(segment(key(2)), null)));
                assertEquals(root, tree.root);
                assertTrue(tree.cache.referencedForTests(root.ref()));
            }
        }

        @Test
        @DisplayName("are not cached when a removed range drops them")
        void dropped(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled()) {
                var old = tree.path(key(100));
                tree.cache.clearForTests();
                var update = new TreeUpdate(tree.pages, tree.pages.writer(9), tree.space);
                var range = new TreeUpdate.Range(segment(key(0)), segment(key(KEYS / 2)));
                var root = update.apply(tree.root, List.of(), List.of(range));
                assertEquals(KEYS - KEYS / 2, root.count());
                for (var ref : old) {
                    assertNull(tree.cache.peek(ref), "the dropped node " + ref);
                }
            }
        }

        @Test
        @DisplayName("read a delta page once for all the records of it that the checkpoint replaces, and keep it cached")
        void deltaPageReadOnce(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB, false).filled()) {
                // In different leaves that sit next to one another, so the records fill one delta page
                var keys = new int[]{100, 150, 200, 250};
                tree.makeRecords(keys);
                var branches = new HashSet<PageRef>();
                var records = new ArrayList<PageRef>();
                var bases = new HashSet<PageRef>();
                var deltaPages = new HashSet<PageRef>();
                for (var key : keys) {
                    var path = tree.path(key(key));
                    var record = path.getLast();
                    branches.addAll(path.subList(0, path.size() - 1));
                    records.add(record);
                    deltaPages.add(record.unit());
                    bases.add(TreeNodes.base(tree.pages.read(record, PageFile.NODE, false), record));
                }
                assertEquals(keys.length, new HashSet<>(records).size(), "the keys share a leaf");
                assertEquals(1, deltaPages.size(), "the records fill more than one delta page");
                tree.cache.clearForTests();
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var key : keys) {
                    ops.add(new TreeUpdate.Op(segment(key(key)), segment(new byte[152])));
                }
                var reads = tree.apply(ops, tree.pages.writer(tree.random.nextLong()));
                assertEquals(branches.size() + bases.size() + deltaPages.size(), reads);
                for (var page : deltaPages) {
                    assertTrue(tree.cache.referencedForTests(page), "the delta page, whose other records live on");
                }
                for (var base : bases) {
                    assertNull(tree.cache.peek(base), "the base of a replaced record");
                }
                for (var record : records) {
                    assertNull(tree.cache.peek(record), "the leaf of a replaced record");
                }
                for (var key : keys) {
                    assertArrayEquals(new byte[152], read(tree.pages, tree.root, key(key)));
                }
            }
        }

        @Test
        @DisplayName("treat a cached unit of another kind as a miss, which the file then refuses")
        void otherKind(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB)) {
                tree.apply(List.of(new TreeUpdate.Op(segment(key(1)), segment(new byte[10]))));
                var leaf = tree.root.ref();
                assertNotNull(tree.pages.peek(leaf, PageFormat.LEAF));
                assertNull(tree.pages.peek(leaf, PageFormat.BRANCH));
                assertThrows(LedgerCorruptedException.class, () -> tree.pages.readForReplace(leaf, PageFormat.BRANCH));
                assertFalse(tree.cache.referencedForTests(leaf));
            }
        }
    }

    @Nested
    @DisplayName("what a checkpoint writes")
    class Admission {
        @ParameterizedTest(name = "a limit of {0} MiB over {1} caches gives {2} KiB")
        @CsvSource({"1, 1, 512", "256, 1, 131072", "256, 4, 32768", "64, 8, 16384", "1, 4, 512", "16, 8, 8192"})
        @DisplayName("may evict others for half of the cache's share of the limit, with a floor")
        void allowance(long limit, int caches, long kibibytes) {
            var budget = new PageCache.Budget();
            var opened = new ArrayList<PageCache>();
            for (var i = 0; i < caches; i++) {
                opened.add(PageCache.in(budget, limit * MIB));
            }
            for (var cache : opened) {
                assertEquals(kibibytes << 10, cache.admission());
            }
            for (var cache : opened) {
                cache.close();
            }
        }

        @Test
        @DisplayName("is admitted within the allowance by exactly as many threads as it allows, and past it into free room only")
        void admits(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, MIB)) {
                var writer = tree.pages.writer(1);
                assertTrue(writer.admits(PageFormat.SIZE), "an empty cache has room");
                fillWithReadEntries(tree.cache);
                assertFalse(tree.cache.hasRoom(PageFormat.SIZE));
                // The call above took one of the 128 units of the allowance
                var admitted = new AtomicInteger();
                var start = new CountDownLatch(1);
                var threads = new ArrayList<Thread>();
                for (var i = 0; i < 8; i++) {
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException exception) {
                            throw new IllegalStateException(exception);
                        }
                        for (var call = 0; call < 200; call++) {
                            if (writer.admits(PageFormat.SIZE)) {
                                admitted.incrementAndGet();
                            }
                        }
                    }));
                }
                start.countDown();
                for (var thread : threads) {
                    thread.join();
                }
                assertEquals(127, admitted.get());
                assertFalse(writer.admits(PageFormat.SIZE));
                var next = tree.pages.writer(2);
                assertTrue(next.admits(PageFormat.SIZE), "the allowance is per checkpoint");
                tree.cache.remove(new PageRef(1 << 30, 1, 0x7777, 0));
                assertTrue(writer.admits(PageFormat.SIZE), "free room takes a unit past the allowance");
            }
        }

        @Test
        @DisplayName("counts nothing for a unit the cache would never keep")
        void tooLarge(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, MIB)) {
                var writer = tree.pages.writer(1);
                fillWithReadEntries(tree.cache);
                for (var i = 0; i < 1_000; i++) {
                    assertFalse(writer.admits(MIB / 4 + 1));
                }
                for (var i = 0; i < 128; i++) {
                    assertTrue(writer.admits(PageFormat.SIZE), "unit " + i);
                }
                assertFalse(writer.admits(PageFormat.SIZE));
            }
        }

        @ParameterizedTest(name = "a cache of {0} bytes")
        @ValueSource(longs = {MIB, 2 * MIB})
        @DisplayName("leaves the first units within the allowance and every branch when it is more than the cache holds")
        void largerThanTheCache(long cacheBytes, @TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, cacheBytes).filled()) {
                var old = tree.path(key(0));
                fillWithReadEntries(tree.cache);
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var i = 0; i < KEYS; i++) {
                    ops.add(new TreeUpdate.Op(segment(key(i)), segment(new byte[160])));
                }
                tree.apply(ops);
                assertTrue(tree.cache.size() <= cacheBytes);
                for (var ref : old) {
                    assertNull(tree.cache.peek(ref), "the replaced node " + ref);
                }
                // The leaves count by the bytes the cache keeps of those a checkpoint writes, which end with their content
                var cachedBytes = 0L;
                var leafBytes = 0L;
                var seen = new TreeSet<Integer>();
                for (var i = 0; i < KEYS; i += 3) {
                    var path = tree.path(key(i));
                    for (var ref : path.subList(0, path.size() - 1)) {
                        assertNotNull(tree.cache.peek(ref), "the branch " + ref);
                    }
                    var leaf = path.getLast();
                    if (seen.add(leaf.page())) {
                        var cached = tree.cache.peek(leaf);
                        var unit = cached != null ? cached : tree.pages.read(leaf, PageFile.NODE, false);
                        var bytes = PageFormat.HEADER + Bytes.i32(unit, PageFormat.LENGTH);
                        leafBytes += bytes;
                        if (cached != null) {
                            assertEquals(bytes, cached.length);
                            cachedBytes += bytes;
                        }
                    }
                }
                var allowed = cacheBytes / 2;
                assertEquals(allowed, tree.cache.admission());
                assertTrue(leafBytes > 2 * allowed, leafBytes + " bytes of leaves");
                // The leaf that would pass the allowance and those after it stay out but for the room the read entries
                // they evicted left, each longer than a leaf, and the clock may take the very first one once it finds
                // only read entries, in each thread that raced for it
                assertTrue(cachedBytes <= allowed + allowed / 8 && cachedBytes >= allowed - 17L * PageFormat.SIZE,
                        cachedBytes + " bytes of leaves of an allowance of " + allowed);
                tree.verify(160);
            }
        }

        @Test
        @DisplayName("past the allowance drops a stale leaf at the place of a delta record instead of caching the new one")
        void staleRecord(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, MIB).filled()) {
                var run = tree.space.allocateRun(8, 64);
                var page = (int) (run >>> 32);
                tree.space.release(page, (int) run);
                tree.space.rewindRuns();
                fillWithReadEntries(tree.cache);
                // Kept from the clock by a read mark, as the clock clears those before it evicts any
                var stale = new PageRef(page, 1, 0x1234, 5, 0);
                var bystander = new PageRef(page, 1, 0x1234, 5, 200);
                tree.cache.remove(new PageRef(1 << 30, 1, 0x7777, 0));
                tree.cache.remove(new PageRef(1 << 30 | 1, 1, 0x7777, 1));
                tree.cache.put(stale, new byte[PageFormat.SIZE], true);
                tree.cache.put(bystander, new byte[PageFormat.SIZE], true);
                var writer = tree.pages.writer(tree.random.nextLong());
                for (var i = 0; i < 128; i++) {
                    writer.admits(PageFormat.SIZE);
                }
                tree.apply(List.of(new TreeUpdate.Op(segment(key(100)), segment(new byte[151]))), writer);
                var record = tree.path(key(100)).getLast();
                assertTrue(record.isRecord());
                assertEquals(page, record.page(), "the record is not where the stale leaf is");
                assertEquals(0, record.slot());
                assertNull(tree.cache.peek(stale));
                assertNotNull(tree.cache.peek(bystander));
                assertNull(tree.cache.peek(record), "past the allowance, with no room");
                assertNull(tree.cache.peek(record.unit()), "past the allowance, with no room");
                assertArrayEquals(new byte[151], read(tree.pages, tree.root, key(100)));
            }
        }
    }

    @Nested
    @DisplayName("a store whose checkpoints rewrite more than its page cache holds")
    class Store {
        // A key in this many: at about 20 entries a leaf, one key a leaf, each leaf a delta record of its own
        private static final int SPARSE = 40;
        // Units the 1 MiB cache of these stores admits a checkpoint, half its limit: more records than this pass it
        private static final long ALLOWANCE = MIB / 2 / PageFormat.SIZE;
        private static final int POWER_LOSS_KEYS = 8_000;
        private static final int POWER_LOSS_COMMITS = 8;

        private static String value(int commit, int key) {
            return commit + ":" + key + ":" + "v".repeat(180);
        }

        private static String key(int i) {
            return "k%06d".formatted(i);
        }

        /// {@return the commit that wrote `value`}
        private static int commitOf(String value) {
            return Integer.parseInt(value.substring(0, value.indexOf(':')));
        }

        /// {@return whether `commit` of the history the tests follow writes every key: two commits that do, each
        /// replacing whole leaves or delta records, then two that write one key in [#SPARSE], the first writing delta
        /// records and the second replacing them}
        private static boolean isFull(int commit) {
            return commit % 4 < 2;
        }

        /// {@return the commit whose value `key` holds after the history up to `commit`}
        private static int writerOf(int commit, int key) {
            return key % SPARSE == 0 || isFull(commit) ? commit : commit - commit % 4 + 1;
        }

        private static Commit write(Ledger ledger, int commit, int keys) {
            var batch = ledger.newBatch();
            for (var i = 0; i < keys; i += isFull(commit) ? 1 : SPARSE) {
                batch.put(VALUES, key(i), value(commit, i));
            }
            return ledger.commit(batch);
        }

        private static Ledger.Options options(Path directory, Encryption encryption) {
            return Ledger.builder().directory(directory).encryption(encryption).keyspace(VALUES).pageCache(MIB);
        }

        private static void verify(Ledger ledger, int commit, int keys) {
            assertEquals(keys, ledger.size(VALUES));
            var seen = new AtomicInteger();
            ledger.forEach(VALUES, KeyRange.all(), (key, value) -> {
                var i = seen.getAndIncrement();
                assertEquals(value(writerOf(commit, i), i), value, key);
                return true;
            });
            assertEquals(keys, seen.get());
        }

        /// Checks the pages of a store closed after a sparse commit, and that its trees hold more delta records than
        /// the allowance, so some were written past it.
        private static void verifyRecords(Path directory, boolean encrypted) throws IOException {
            var records = CheckpointTest.verifyPages(directory, encrypted);
            assertTrue(records > ALLOWANCE, () -> records + " delta records, the allowance is " + ALLOWANCE);
        }

        @ParameterizedTest(name = "encrypted: {0}, cache cleared: {1}")
        @CsvSource({"true, false", "true, true", "false, false", "false, true"})
        @DisplayName("reads back every key, with exact counts, after each checkpoint and a reopen")
        void readsBack(boolean encrypted, boolean clear, @TempDir Path directory) throws Exception {
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var keys = 30_000;
            var commits = 8;
            try (var ledger = options(directory, encryption).build()) {
                for (var commit = 0; commit < commits; commit++) {
                    write(ledger, commit, keys);
                    ledger.checkpoint();
                    if (clear) {
                        ((LiveLedger) ledger).clearPageCacheForTests();
                    }
                    verify(ledger, commit, keys);
                }
            }
            try (var ledger = options(directory, encryption).build()) {
                verify(ledger, commits - 1, keys);
            }
            verifyRecords(directory, encrypted);
        }

        @Test
        @DisplayName("keeps every point read and snapshot exact while checkpoints run")
        void concurrentReaders(@TempDir Path directory) throws Exception {
            var keys = 12_000;
            var commits = 12;
            var failure = new AtomicReference<Throwable>();
            var stop = new AtomicBoolean();
            // The newest commit a write made visible
            var visible = new AtomicInteger();
            try (var ledger = options(directory, Encryption.key(KEY)).build()) {
                write(ledger, 0, keys);
                var readers = new ArrayList<Thread>();
                for (var reader = 0; reader < 4; reader++) {
                    var seed = reader;
                    readers.add(Thread.ofPlatform().start(() -> {
                        var random = new Random(seed);
                        try {
                            while (!stop.get()) {
                                var from = visible.get();
                                var key = random.nextInt(keys);
                                var value = ledger.get(VALUES, key(key)).orElseThrow();
                                var writer = commitOf(value);
                                var oldest = writerOf(from, key);
                                assertTrue(writer >= oldest, () -> "commit " + writer + " older than " + oldest);
                                assertEquals(value(writer, key), value);
                                try (var snapshot = ledger.snapshot()) {
                                    var first = random.nextInt(keys - 300);
                                    // A key every commit writes names the commit the snapshot sees
                                    var commit = commitOf(snapshot.get(VALUES, key(first - first % SPARSE)).orElseThrow());
                                    var seen = new AtomicInteger();
                                    snapshot.forEach(VALUES, KeyRange.between(key(first), key(first + 300)), (_, read) -> {
                                        var i = first + seen.getAndIncrement();
                                        assertEquals(value(writerOf(commit, i), i), read, "a snapshot saw two commits");
                                        return true;
                                    });
                                    assertEquals(300, seen.get());
                                    assertEquals(keys, snapshot.size(VALUES));
                                }
                            }
                        } catch (Throwable exception) {
                            failure.compareAndSet(null, exception);
                        }
                    }));
                }
                for (var commit = 1; commit < commits && failure.get() == null; commit++) {
                    write(ledger, commit, keys);
                    visible.set(commit);
                    ledger.checkpoint();
                }
                stop.set(true);
                for (var thread : readers) {
                    thread.join();
                }
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                verify(ledger, commits - 1, keys);
            }
            verifyRecords(directory, true);
        }

        /// The history of [#powerLoss(long)], as far as the file system lets it go.
        private static final class History {
            int visible = -1;
            int durable = -1;

            void run(Path path) throws InterruptedException {
                Ledger ledger = null;
                try {
                    ledger = options(path, Encryption.key(KEY)).build();
                    for (var commit = 0; commit < POWER_LOSS_COMMITS; commit++) {
                        var written = write(ledger, commit, POWER_LOSS_KEYS);
                        visible = commit;
                        written.awaitDurable();
                        durable = commit;
                        ledger.checkpoint();
                    }
                } catch (RuntimeException _) {
                    // The file system stopped where it was told to
                } finally {
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (RuntimeException _) {
                            // The file system stopped: closing only lets go of the threads
                        }
                    }
                }
            }
        }

        /// {@return every force of the history, counted by a run the file system does not stop, which also shows that
        /// the history leaves more delta records in the file than the allowance}
        static LongStream forces() throws Exception {
            var disk = CrashFileSystem.create();
            var history = new History();
            history.run(disk.getPath("/store"));
            assertEquals(POWER_LOSS_COMMITS - 1, history.durable);
            verifyRecords(disk.getPath("/store"), true);
            return LongStream.rangeClosed(0, disk.forces());
        }

        @ParameterizedTest(name = "stop at force {0}")
        @MethodSource("forces")
        @DisplayName("recovers one whole commit when the power fails during a checkpoint")
        void powerLoss(long force) throws Exception {
            var encryption = Encryption.key(KEY);
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            var history = new History();
            history.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(force));
            var survivorPath = survivor.getPath("/store");
            try (var recovered = options(survivorPath, encryption).build()) {
                var size = recovered.size(VALUES);
                if (size == 0) {
                    assertEquals(-1, history.durable, "a durable commit was lost");
                } else {
                    assertEquals(POWER_LOSS_KEYS, size);
                    var commit = commitOf(recovered.get(VALUES, key(0)).orElseThrow());
                    assertTrue(commit >= history.durable && commit <= history.visible,
                            () -> "commit " + commit + " outside " + history.durable + " to " + history.visible);
                    verify(recovered, commit, POWER_LOSS_KEYS);
                }
                // A full commit, then a sparse one, on what the power failure left
                for (var commit = 101; commit <= 102; commit++) {
                    write(recovered, commit, POWER_LOSS_KEYS);
                    recovered.checkpoint();
                    verify(recovered, commit, POWER_LOSS_KEYS);
                }
            }
            verifyRecords(survivorPath, true);
        }
    }

    @Nested
    @DisplayName("the units a checkpoint replaced")
    class Released {
        private static PageRef unit(int page, long nonceHigh, int nonceLow) {
            return new PageRef(page, 1, nonceHigh, nonceLow);
        }

        /// {@return a unit of `kind` whose header names `ref`, as the cache finds units of the files by their header}
        private static byte[] sealedAs(byte kind, PageRef ref) {
            var unit = new byte[PageFormat.SIZE];
            PageFile.header(unit, kind, 0, ref.page(), ref.pages(), 0, ref.nonceHigh(), ref.nonceLow());
            return unit;
        }

        @Test
        @DisplayName("wait for the readers, then go to the cache's drop whole, the delta records with them, once each")
        void handedOver() {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var page = space.allocate(4);
            var units = List.of(unit(page, 1, 1), unit(page + 1, 1, 2));
            var records = List.of(new PageRef(page + 2, 1, 1, 3, 0), new PageRef(page + 2, 1, 1, 3, 1));
            space.pend(10, units, records);
            space.pend(20, List.of(), List.of());
            space.releasePassed(10);
            assertTrue(space.takeReleased().isEmpty(), "a reader at the publish still reads them");
            space.releasePassed(11);
            var released = space.takeReleased();
            assertEquals(2, released.size());
            assertSame(units, released.get(0), "the commit path copied the units");
            assertSame(records, released.get(1), "the commit path copied the records");
            assertTrue(space.isUnused(page) && space.isUnused(page + 1));
            assertFalse(space.isUnused(page + 2), "the delta page of dropped records may hold live ones");
            assertTrue(space.takeReleased().isEmpty());
        }

        @Test
        @DisplayName("reach exactly one taker when releases race the takes")
        void releaseRacesTake() throws Exception {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var pended = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<List<PageRef>, Boolean>());
            var taken = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<List<PageRef>, Boolean>());
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var taker = Thread.ofPlatform().start(() -> {
                try {
                    while (!stop.get()) {
                        for (var list : space.takeReleased()) {
                            assertTrue(taken.add(list), "a list taken twice");
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            var releasers = new ArrayList<Thread>();
            var published = new java.util.concurrent.atomic.AtomicLong();
            for (var thread = 0; thread < 4; thread++) {
                var lists = new ArrayList<List<PageRef>>();
                for (var i = 0; i < 2_000; i++) {
                    lists.add(List.of(unit(space.allocate(1), thread, i)));
                }
                pended.addAll(lists);
                releasers.add(Thread.ofPlatform().unstarted(() -> {
                    for (var list : lists) {
                        space.pend(published.incrementAndGet(), list);
                        space.releasePassed(Long.MAX_VALUE);
                    }
                }));
            }
            for (var thread : releasers) {
                thread.start();
            }
            for (var thread : releasers) {
                thread.join();
            }
            stop.set(true);
            taker.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            for (var list : space.takeReleased()) {
                assertTrue(taken.add(list), "a list taken twice");
            }
            assertEquals(pended, taken);
        }

        @Test
        @DisplayName("leave the cache by file and nonce, the leaves of dropped records included, past a hold of the lock")
        void removedExactly() {
            var cache = PageCache.isolated(64 * MIB);
            IdleTicker.unwatch(cache);
            try {
                var dead = unit(10, 7, 1);
                var reused = unit(11, 7, 2);
                var reusedNow = unit(11, 8, 1);
                var record = new PageRef(12, 1, 7, 3, 2);
                var bystander = new PageRef(12, 1, 7, 3, 3);
                var blob = unit(10, 9, 1);
                cache.put(dead, sealedAs(PageFormat.LEAF, dead), true);
                cache.put(reusedNow, sealedAs(PageFormat.LEAF, reusedNow), true);
                cache.put(record, new byte[100], true);
                cache.put(bystander, new byte[100], true);
                cache.put(PageCache.key(PageCache.BLOBS, 10), blob, sealedAs(PageFormat.EXTERNAL, blob), true);
                // Units of other pages, more than one hold of the lock drops
                var many = new ArrayList<PageRef>();
                for (var i = 0; i < 5_000; i++) {
                    var ref = unit(1_000 + i, 7, 100 + i);
                    cache.put(ref, sealedAs(PageFormat.LEAF, ref), false);
                    many.add(ref);
                }
                cache.removeAll(PageCache.DATA, List.of(List.of(dead, reused, record), many));
                assertNull(cache.peek(dead));
                assertNull(cache.peek(record));
                for (var ref : many) {
                    assertNull(cache.peek(ref), "unit " + ref);
                }
                assertNotNull(cache.peek(reusedNow), "a later checkpoint's unit at a page released before");
                assertNotNull(cache.peek(bystander), "a live record of the same delta page");
                assertNotNull(cache.get(PageCache.key(PageCache.BLOBS, 10), blob), "the unit of the same page of the other file");
                assertEquals(PageFormat.SIZE + 100 + PageFormat.SIZE, cache.size());
                cache.removeAll(PageCache.BLOBS, List.of(List.of(unit(10, 9, 2)), List.of(dead)));
                assertNotNull(cache.get(PageCache.key(PageCache.BLOBS, 10), blob), "a unit of another nonce at its page");
                cache.removeAll(PageCache.BLOBS, List.of(List.of(blob)));
                assertNull(cache.get(PageCache.key(PageCache.BLOBS, 10), blob));
                assertNotNull(cache.peek(reusedNow));
                assertEquals(PageFormat.SIZE + 100, cache.size());
                assertEquals(2, cache.clockLengthForTests());
            } finally {
                cache.close();
            }
        }

        private static final int KEYS = 20_000;
        private static final int LARGE_KEYS = 64;
        private static final int LARGE = 20_000;

        private static String key(int i) {
            return "k%06d".formatted(i);
        }

        private static String value(int round, int i, boolean large) {
            return round + ":" + i + ":" + "v".repeat(large ? LARGE : 150 + round);
        }

        private static void write(Ledger ledger, int round, boolean large) {
            var batch = ledger.newBatch();
            for (var i = 0; i < (large ? LARGE_KEYS : KEYS); i++) {
                batch.put(VALUES, key(i), value(round, i, large));
            }
            ledger.commit(batch);
        }

        private static void verify(com.github.auties00.ledger.View view, int round, boolean large) {
            var keys = large ? LARGE_KEYS : KEYS;
            assertEquals(keys, view.size(VALUES));
            for (var i = 0; i < keys; i += large ? 1 : 7) {
                assertEquals(value(round, i, large), view.get(VALUES, key(i)).orElseThrow(), key(i));
            }
        }

        /// Waits until the store's page cache holds at most `bytes`, as the drop runs on a thread of its own.
        private static void awaitAtMost(LiveLedger ledger, long bytes) throws InterruptedException {
            var deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (ledger.pageCacheBytesForTests() > bytes) {
                assertTrue(System.nanoTime() < deadline, () -> "the cache still holds " + ledger.pageCacheBytesForTests()
                        + " bytes, not at most " + bytes);
                Thread.sleep(5);
            }
        }

        @ParameterizedTest(name = "large values: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("leave the cache once no reader reads them, and not before")
        void leaveTheCache(boolean large, @TempDir Path directory) throws Exception {
            var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(VALUES)
                    .pageCache(64 * MIB).largeValues(large ? 4_096 : Long.MAX_VALUE);
            try (var ledger = (LiveLedger) options.build()) {
                write(ledger, 0, large);
                ledger.checkpoint();
                // The values of the file of large values enter the cache as they are read
                verify(ledger, 0, large);
                var first = ledger.pageCacheBytesForTests();
                var replaced = large ? (long) LARGE_KEYS * LARGE : first;
                var snapshot = ledger.snapshot();
                write(ledger, 1, large);
                ledger.checkpoint();
                // A commit finds the readers past the checkpoint, the next checkpoint drops what it freed
                ledger.commit(ledger.newBatch().put(VALUES, key(0), value(1, 0, large)));
                ledger.checkpoint();
                var held = ledger.pageCacheBytesForTests();
                // The new leaves enter the cache as they are written, the new large values only once read
                var written = large ? 0 : replaced * 9 / 10;
                assertTrue(held >= first + written, () -> "the replaced units left while a reader reads them: " + held
                        + " bytes after " + first);
                verify(snapshot, 0, large);
                snapshot.close();
                ledger.commit(ledger.newBatch().put(VALUES, key(0), value(1, 0, large)));
                ledger.checkpoint();
                awaitAtMost(ledger, held - replaced * 9 / 10);
                verify(ledger, 1, large);
            }
            try (var ledger = (LiveLedger) options.build()) {
                verify(ledger, 1, large);
            }
        }
    }
}
