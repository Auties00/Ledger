package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor.ChangeKind;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.IntUnaryOperator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Which leaves and delta pages a checkpoint leaves in the page cache depending on whether the next checkpoint follows
/// it at once: the tree tests drive a writer whose saturation they set, on a cache with a budget of its own; the store
/// tests run explicit checkpoints, which nothing follows unless the test makes commits pile up behind one, through a
/// file system that holds the checkpoint's first write to the data file.
class CheckpointSaturationTest {
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
            this.pool = new HeapPool(1 << 20);
            this.cache = PageCache.isolated(cacheBytes);
            IdleTicker.unwatch(cache);
            this.pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(KEY, "AES")), cache, pool);
            this.space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            this.random = new Random(9);
            this.deltas = DeltaTable.EMPTY;
        }

        /// {@return a writer whose checkpoint the next one does not follow}
        PageFile.Writer unsaturated() {
            return pages.writer(random.nextLong(), () -> false, () -> false);
        }

        /// Applies `ops` through `writer`, then flushes it and frees what the update replaced.
        void apply(List<TreeUpdate.Op> ops, PageFile.Writer writer) throws Exception {
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, List.of());
            deltas = update.finishDeltas(deltas);
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
            space.rewindRuns();
        }

        /// Fills the tree with `keys` keys of 150-byte values through a writer the next checkpoint follows, which leaves
        /// its leaves in the cache, none counted as read.
        Tree fill(int keys) throws Exception {
            apply(ops(0, keys, 1, 150), pages.writer(random.nextLong()));
            return this;
        }

        /// Fills the tree as [#fill(int)] does, and empties the cache.
        Tree filled(int keys) throws Exception {
            fill(keys);
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

        /// {@return the value of `key`, read through the cache as a reader reads it, which caches what it reads}
        byte[] read(byte[] key) {
            var leaf = TreeSearch.leaf(pages, root, key);
            var entry = TreeSearch.entryOf(leaf, key);
            assertTrue(entry != -1, () -> "no key " + new String(key));
            return TreeSearch.value(pages, leaf, entry, false).toArray(ValueLayout.JAVA_BYTE);
        }

        /// Reads every `step`-th key of the first `keys` back through the cache as it stands, then from the file.
        void verify(int keys, int step, int value) {
            for (var round = 0; round < 2; round++) {
                for (var i = 0; i < keys; i += step) {
                    assertArrayEquals(new byte[value], read(key(i)), "key " + i);
                }
                cache.clearForTests();
            }
            assertEquals(keys, root.count());
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

    /// {@return writes of a `value`-byte value to every `step`-th key from `from` to `to`}
    private static List<TreeUpdate.Op> ops(int from, int to, int step, int value) {
        var ops = new ArrayList<TreeUpdate.Op>();
        for (var i = from; i < to; i += step) {
            ops.add(new TreeUpdate.Op(segment(key(i)), segment(new byte[value])));
        }
        return ops;
    }

    /// Fills the rest of `cache` with entries that count as read, at pages no tree of these tests reaches.
    private static void fillWithReadEntries(PageCache cache) {
        for (var i = 0; cache.hasRoom(PageFormat.SIZE); i++) {
            cache.put(new PageRef(1 << 30 | i, 1, 0x7777, i), new byte[PageFormat.SIZE], true);
        }
    }

    @Nested
    @DisplayName("a checkpoint the next one does not follow at once")
    class Unsaturated {
        @Test
        @DisplayName("leaves the leaves it writes whole out of the cache and caches every branch")
        void leavesOut(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                tree.apply(ops(0, KEYS, 1, 160), tree.unsaturated());
                for (var i = 0; i < KEYS; i += 97) {
                    var path = tree.path(key(i));
                    for (var ref : path.subList(0, path.size() - 1)) {
                        assertNotNull(tree.cache.peek(ref), "the branch " + ref);
                    }
                    assertFalse(path.getLast().isRecord());
                    assertNull(tree.cache.peek(path.getLast()), "the leaf " + path.getLast());
                }
                assertTrue(tree.cache.size() < 64 * PageFormat.SIZE, tree.cache.size() + " bytes cached");
                tree.verify(KEYS, 1, 160);
            }
        }

        @Test
        @DisplayName("leaves its delta records' leaves and its delta pages out of the cache, and reads them back")
        void recordsOut(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                tree.apply(ops(0, KEYS, 40, 151), tree.unsaturated());
                for (var i = 0; i < KEYS; i += 40) {
                    var record = tree.path(key(i)).getLast();
                    assertTrue(record.isRecord(), "the leaf of key " + i + " is no delta record");
                    assertNull(tree.cache.peek(record), "the leaf of the record of key " + i);
                    assertNull(tree.cache.peek(record.unit()), "the delta page of key " + i);
                }
                for (var i = 0; i < KEYS; i += 40) {
                    assertArrayEquals(new byte[151], tree.read(key(i)), "key " + i);
                }
                // The reads cached the records' leaves: a checkpoint replaces them, then one replaces those, and the
                // delta pages the first ones leave empty go back to the free space for the second
                for (var round = 0; round < 3; round++) {
                    tree.apply(ops(0, KEYS, 40, 152 + round), tree.unsaturated());
                    for (var i = 0; i < KEYS; i += 40) {
                        assertArrayEquals(new byte[152 + round], tree.read(key(i)), "key " + i + " in round " + round);
                    }
                    tree.cache.clearForTests();
                    for (var i = 1; i < KEYS; i += 40) {
                        assertArrayEquals(new byte[150], tree.read(key(i)), "key " + i + " in round " + round);
                    }
                }
                assertEquals(KEYS, tree.root.count());
            }
        }

        @Test
        @DisplayName("caches a delta record's leaf that replaces a leaf the cache holds, and no other")
        void replacedCachedRecord(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                tree.read(key(100));
                assertNotNull(tree.cache.peek(tree.path(key(100)).getLast()), "the read leaf");
                assertNull(tree.cache.peek(tree.path(key(5_000)).getLast()), "the leaf no one read");
                var ops = new ArrayList<TreeUpdate.Op>();
                ops.add(new TreeUpdate.Op(segment(key(100)), segment(new byte[151])));
                ops.add(new TreeUpdate.Op(segment(key(5_000)), segment(new byte[151])));
                tree.apply(ops, tree.unsaturated());
                var read = tree.path(key(100)).getLast();
                var unread = tree.path(key(5_000)).getLast();
                assertTrue(read.isRecord() && unread.isRecord());
                assertNotNull(tree.cache.peek(read), "the record of the leaf a reader holds");
                assertFalse(tree.cache.referencedForTests(read), "the new record counts as read");
                assertNull(tree.cache.peek(unread), "the record of the leaf no one read");
                assertArrayEquals(new byte[151], tree.read(key(100)));
                assertArrayEquals(new byte[151], tree.read(key(5_000)));
                tree.verify(KEYS, 7, 150);
            }
        }

        @Test
        @DisplayName("caches the leaves written whole that replace a leaf the cache holds, and no other")
        void replacedCachedLeaves(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                tree.read(key(100));
                var before = tree.path(key(5_000)).getLast();
                // Enough new keys after each of the two keys that their leaves split, written whole
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var key : new int[]{100, 5_000}) {
                    for (var i = 0; i < 40; i++) {
                        ops.add(new TreeUpdate.Op(segment((new String(key(key)) + "-%02d".formatted(i)).getBytes()), segment(new byte[150])));
                    }
                }
                assertNull(tree.cache.peek(before));
                tree.apply(ops, tree.unsaturated());
                for (var i = 0; i < 40; i += 5) {
                    var read = tree.path((new String(key(100)) + "-%02d".formatted(i)).getBytes()).getLast();
                    var unread = tree.path((new String(key(5_000)) + "-%02d".formatted(i)).getBytes()).getLast();
                    assertFalse(read.isRecord() || unread.isRecord(), "a split leaf is written whole");
                    assertNotNull(tree.cache.peek(read), "a leaf of the leaf a reader holds");
                    assertNull(tree.cache.peek(unread), "a leaf of the leaf no one read");
                }
                assertEquals(KEYS + 80, tree.root.count());
            }
        }

        @Test
        @DisplayName("leaves out the leaves that replace those a checkpoint cached and no read used, round after round")
        void unreadCheckpointLeaves(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).fill(KEYS)) {
                for (var i = 0; i < KEYS; i += 97) {
                    var leaf = tree.path(key(i)).getLast();
                    assertNotNull(tree.cache.peek(leaf), "the leaf the saturated checkpoint wrote for key " + i);
                    assertFalse(tree.cache.referencedForTests(leaf), "the leaf of key " + i + " counts as read");
                }
                for (var round = 0; round < 3; round++) {
                    var before = tree.cache.size();
                    tree.apply(ops(0, KEYS, 1, 160 + round), tree.unsaturated());
                    for (var i = 0; i < KEYS; i += 97) {
                        var leaf = tree.path(key(i)).getLast();
                        assertFalse(leaf.isRecord());
                        assertNull(tree.cache.peek(leaf), "the leaf of key " + i + " in round " + round);
                    }
                    assertTrue(tree.cache.size() < before + 64 * PageFormat.SIZE,
                            tree.cache.size() - before + " bytes cached in round " + round);
                }
                tree.verify(KEYS, 1, 162);
            }
        }

        @Test
        @DisplayName("leaves out the leaf that replaces one a screened read refused, and caches the one that replaces a leaf read twice")
        void screenedReads(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                TreeSearch.leaf(tree.pages, tree.root, key(100), true);
                TreeSearch.leaf(tree.pages, tree.root, key(5_000), true);
                TreeSearch.leaf(tree.pages, tree.root, key(5_000), true);
                assertNull(tree.cache.peek(tree.path(key(100)).getLast()), "the leaf read once");
                assertTrue(tree.cache.referencedForTests(tree.path(key(5_000)).getLast()), "the leaf read twice");
                var ops = new ArrayList<TreeUpdate.Op>();
                ops.add(new TreeUpdate.Op(segment(key(100)), segment(new byte[151])));
                ops.add(new TreeUpdate.Op(segment(key(5_000)), segment(new byte[151])));
                tree.apply(ops, tree.unsaturated());
                var once = tree.path(key(100)).getLast();
                var twice = tree.path(key(5_000)).getLast();
                assertTrue(once.isRecord() && twice.isRecord());
                assertNull(tree.cache.peek(once), "the record of the leaf read once");
                assertNotNull(tree.cache.peek(twice), "the record of the leaf read twice");
                assertFalse(tree.cache.referencedForTests(twice), "the new record counts as read");
                assertArrayEquals(new byte[151], tree.read(key(100)));
                assertArrayEquals(new byte[151], tree.read(key(5_000)));
                tree.verify(KEYS, 7, 150);
            }
        }

        @Test
        @DisplayName("caches at its first read the leaf that replaces one a screened read refused, for a reader that reads each version once")
        void readOncePerVersion(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                for (var round = 0; round < 6; round++) {
                    var leaf = tree.path(key(100)).getLast();
                    if (round < 2) {
                        assertNull(tree.cache.peek(leaf), "the leaf of key 100 before the read of round " + round);
                    } else {
                        assertNotNull(tree.cache.peek(leaf), "the leaf of key 100 before the read of round " + round);
                    }
                    TreeSearch.leaf(tree.pages, tree.root, key(100), true);
                    if (round == 0) {
                        assertNull(tree.cache.peek(leaf), "the leaf the first read refused");
                    } else {
                        assertTrue(tree.cache.referencedForTests(leaf), "the leaf of key 100 after the read of round " + round);
                    }
                    var ops = new ArrayList<TreeUpdate.Op>();
                    ops.add(new TreeUpdate.Op(segment(key(100)), segment(new byte[151 + round])));
                    ops.add(new TreeUpdate.Op(segment(key(5_000)), segment(new byte[151 + round])));
                    tree.apply(ops, tree.unsaturated());
                    var unread = tree.path(key(5_000)).getLast();
                    assertNull(tree.cache.peek(unread), "the leaf of key 5,000 in round " + round);
                    assertFalse(tree.cache.seen(unread), "the leaf of key 5,000 seen in round " + round);
                }
                assertArrayEquals(new byte[156], tree.read(key(100)));
                assertArrayEquals(new byte[156], tree.read(key(5_000)));
            }
        }

        @ParameterizedTest(name = "screening {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("keeps a leaf only the checkpoint read out of the leaves a read used, so the next checkpoint leaves its replacement out")
        void keptLeafUnread(boolean screening, @TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                tree.cache.screen(screening);
                var leaf = tree.path(key(100)).getLast();
                // A removal of a key the tree lacks reads the leaf of key 100 and leaves it as it is
                tree.apply(List.of(new TreeUpdate.Op(segment((new String(key(100)) + "a").getBytes()), null)), tree.unsaturated());
                assertEquals(leaf, tree.path(key(100)).getLast(), "the leaf the removal left");
                assertFalse(tree.cache.referencedForTests(leaf), "the leaf only the checkpoint read counts as read");
                if (screening) {
                    assertNull(tree.cache.peek(leaf), "the leaf only the checkpoint read, past the doorkeeper");
                    TreeSearch.leaf(tree.pages, tree.root, key(100), true);
                    assertNull(tree.cache.peek(leaf), "the leaf after a reader's first read");
                } else {
                    assertNotNull(tree.cache.peek(leaf), "the leaf only the checkpoint read, cached as written");
                }
                tree.apply(ops(100, 101, 1, 151), tree.unsaturated());
                var replacement = tree.path(key(100)).getLast();
                assertNull(tree.cache.peek(replacement), "the replacement of the leaf only the checkpoint read");
                assertArrayEquals(new byte[151], tree.read(key(100)));
                tree.verify(KEYS, 7, 150);
            }
        }

        @Test
        @DisplayName("caches the leaf that replaces one a reader read among those a checkpoint cached, for that checkpoint only")
        void readCheckpointLeaf(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).fill(KEYS)) {
                assertNotNull(tree.cache.peek(tree.path(key(5_000)).getLast()), "the leaf the saturated checkpoint wrote");
                tree.read(key(100));
                var ops = new ArrayList<TreeUpdate.Op>();
                ops.add(new TreeUpdate.Op(segment(key(100)), segment(new byte[151])));
                ops.add(new TreeUpdate.Op(segment(key(5_000)), segment(new byte[151])));
                tree.apply(ops, tree.unsaturated());
                var read = tree.path(key(100)).getLast();
                var unread = tree.path(key(5_000)).getLast();
                assertTrue(read.isRecord() && unread.isRecord());
                assertNotNull(tree.cache.peek(read), "the record of the leaf a reader read");
                assertNull(tree.cache.peek(unread), "the record of the leaf the checkpoint cached and no one read");
                // No read used the record the cache took: the checkpoint after leaves the one that replaces it out
                ops.clear();
                ops.add(new TreeUpdate.Op(segment(key(100)), segment(new byte[152])));
                ops.add(new TreeUpdate.Op(segment(key(5_000)), segment(new byte[152])));
                tree.apply(ops, tree.unsaturated());
                read = tree.path(key(100)).getLast();
                unread = tree.path(key(5_000)).getLast();
                assertTrue(read.isRecord() && unread.isRecord());
                assertNull(tree.cache.peek(read), "the record of the record no one read");
                assertNull(tree.cache.peek(unread), "the record of the leaf no one read");
                assertArrayEquals(new byte[152], tree.read(key(100)));
                assertArrayEquals(new byte[152], tree.read(key(5_000)));
                tree.verify(KEYS, 7, 150);
            }
        }

        @Test
        @DisplayName("writes more than a flush holds and reads its own leaves back from the file")
        void crossesAFlush(@TempDir Path directory) throws Exception {
            // About 13 MiB of leaves, past the 8 MiB a writer queues before it flushes
            var keys = 70_000;
            try (var tree = new Tree(directory, 64 * MIB)) {
                tree.apply(ops(0, keys, 1, 150), tree.unsaturated());
                assertTrue(tree.cache.size() < 256 * PageFormat.SIZE, tree.cache.size() + " bytes cached");
                tree.verify(keys, 13, 150);
                // Removals that leave the leaves under a quarter full, so the update merges leaves it wrote itself
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var i = 0; i < keys; i++) {
                    ops.add(new TreeUpdate.Op(segment(key(i)), i % 8 == 0 ? segment(new byte[151]) : null));
                }
                tree.apply(ops, tree.unsaturated());
                assertEquals(keys / 8 + (keys % 8 == 0 ? 0 : 1), tree.root.count());
                for (var round = 0; round < 2; round++) {
                    for (var i = 0; i < keys; i += 8) {
                        assertArrayEquals(new byte[151], tree.read(key(i)), "key " + i);
                    }
                    tree.cache.clearForTests();
                }
            }
        }

        @Test
        @DisplayName("gives none of the plaintext of the leaves it leaves out to the pool")
        void noPooledPlaintext(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB).filled(KEYS)) {
                var writer = tree.unsaturated();
                var update = new TreeUpdate(tree.pages, writer, tree.space);
                var root = update.apply(tree.root, ops(0, KEYS, 1, 160), List.of());
                update.finishDeltas(tree.deltas);
                var plaintext = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
                var leaves = new ArrayList<PageRef>();
                var pending = new ArrayList<PageRef>();
                pending.add(root.ref());
                while (!pending.isEmpty()) {
                    var ref = pending.removeLast();
                    var unit = writer.staged(ref);
                    assertNotNull(unit, "a unit of the update is not queued");
                    if (TreeNodes.isLeaf(unit)) {
                        plaintext.add(unit);
                        leaves.add(ref);
                    } else {
                        for (var child : TreeNodes.children(unit)) {
                            pending.add(child.ref());
                        }
                    }
                }
                assertTrue(plaintext.size() > 100, plaintext.size() + " leaves");
                var released = new AtomicInteger();
                var leaked = new AtomicInteger();
                tree.pool.watchForTests(new HeapPool.Watcher() {
                    @Override
                    public void acquired(byte[] array) {
                    }

                    @Override
                    public void released(byte[] array) {
                        released.incrementAndGet();
                        if (plaintext.contains(array)) {
                            leaked.incrementAndGet();
                        }
                    }
                });
                writer.flush();
                tree.pool.watchForTests(null);
                assertTrue(released.get() > 0, "the flush gave its sealed arrays back");
                assertEquals(0, leaked.get(), "plaintext leaves went to the pool");
                for (var leaf : leaves) {
                    assertNull(tree.cache.peek(leaf), "the leaf " + leaf);
                }
            }
        }
    }

    @Nested
    @DisplayName("the writer of a checkpoint")
    class Writer {
        @Test
        @DisplayName("asks whether the next checkpoint follows until it does, then never again")
        void latches(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, 64 * MIB)) {
                var asked = new AtomicInteger();
                BooleanSupplier saturation = () -> asked.incrementAndGet() >= 3;
                var writer = tree.pages.writer(1, () -> false, saturation);
                assertFalse(writer.admits(PageFormat.SIZE));
                assertFalse(writer.saturated());
                assertTrue(writer.admits(PageFormat.SIZE));
                for (var i = 0; i < 100; i++) {
                    assertTrue(writer.saturated());
                    assertTrue(writer.admits(PageFormat.SIZE));
                }
                assertEquals(3, asked.get());
                var followed = tree.pages.writer(2);
                assertTrue(followed.saturated());
                assertTrue(followed.admits(PageFormat.SIZE));
            }
        }

        @Test
        @DisplayName("counts nothing against the allowance for a unit it leaves out")
        void allowance(@TempDir Path directory) throws Exception {
            try (var tree = new Tree(directory, MIB)) {
                fillWithReadEntries(tree.cache);
                var saturated = new AtomicBoolean();
                var writer = tree.pages.writer(1, () -> false, saturated::get);
                for (var i = 0; i < 1_000; i++) {
                    assertFalse(writer.admits(PageFormat.SIZE));
                }
                // A leaf that replaces one a read used goes in, and counts
                assertTrue(writer.admits(PageFormat.SIZE, true));
                saturated.set(true);
                var admitted = 0;
                for (var i = 0; i < 1_000; i++) {
                    if (writer.admits(PageFormat.SIZE)) {
                        admitted++;
                    }
                }
                assertEquals(MIB / 2 / PageFormat.SIZE - 1, admitted);
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        private static final int VALUE = 1_000;

        private static String value(int round, int key) {
            return round + ":" + key + ":" + "v".repeat(VALUE);
        }

        private static String key(int i) {
            return "k%06d".formatted(i);
        }

        /// {@return the options of a store whose checkpoints start on their own every `every` bytes of log}
        private static Ledger.Options options(Path directory, long every) {
            return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(VALUES)
                    .pageCache(64 * MIB).checkpointEvery(every);
        }

        private static Ledger.Options options(Path directory) {
            return options(directory, 256 * MIB);
        }

        /// Writes `value(round, i)` to the keys from `from` to `to` and waits for it to be durable.
        private static void write(Ledger ledger, int round, int from, int to) throws InterruptedException {
            var batch = ledger.newBatch();
            for (var i = from; i < to; i++) {
                batch.put(VALUES, key(i), value(round, i));
            }
            ledger.commit(batch).awaitDurable();
        }

        /// Checks that `view` holds `keys` keys, key `i` with the value of `roundOf(i)`, through the cache and, for a
        /// store, through the file.
        private static void verify(View view, int keys, IntUnaryOperator roundOf) {
            assertEquals(keys, view.size(VALUES));
            var seen = new AtomicInteger();
            view.forEach(VALUES, KeyRange.all(), (key, value) -> {
                var i = seen.getAndIncrement();
                assertEquals(value(roundOf.applyAsInt(i), i), value, key);
                return true;
            });
            assertEquals(keys, seen.get());
            for (var i = 0; i < keys; i += 37) {
                assertEquals(value(roundOf.applyAsInt(i), i), view.get(VALUES, key(i)).orElseThrow());
            }
        }

        private static void awaitBackground(LiveLedger ledger) throws InterruptedException {
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (ledger.checkpointer().startedForTests()) {
                assertTrue(System.nanoTime() < deadline, "the checkpoint started on its own thread never ended");
                Thread.sleep(5);
            }
            assertNull(ledger.checkpointer().lastFailure());
        }

        @Test
        @DisplayName("an explicit checkpoint that no commit follows caches only its branches, and reads back")
        void explicit(@TempDir Path directory) throws Exception {
            var keys = 900;
            try (var ledger = options(directory).build()) {
                var live = (LiveLedger) ledger;
                write(ledger, 0, 0, keys);
                live.clearPageCacheForTests();
                ledger.checkpoint();
                assertTrue(live.pageCacheBytesForTests() < 16 * PageFormat.SIZE, live.pageCacheBytesForTests() + " bytes cached");
                verify(ledger, keys, _ -> 0);
                live.clearPageCacheForTests();
                verify(ledger, keys, _ -> 0);
            }
            try (var ledger = options(directory).build()) {
                verify(ledger, keys, _ -> 0);
            }
        }

        @Test
        @DisplayName("a checkpoint that ends with the next one due makes the next one cache its leaves from the first")
        void carriesOver() throws Exception {
            var disk = CrashFileSystem.create();
            var path = disk.getPath("/store");
            // About 0.2, 1.1 and 0.5 MiB of log: the commits made during the held checkpoint pass the interval, and
            // all of them stay below the two intervals past which commits wait for a checkpoint
            var first = 200;
            var during = 1_100;
            var last = 500;
            try (var ledger = options(path, MIB).build()) {
                var live = (LiveLedger) ledger;
                write(ledger, 0, 0, first);
                var reached = new CountDownLatch(1);
                var release = new CountDownLatch(1);
                var armed = new AtomicBoolean(true);
                disk.intercept((file, kind) -> {
                    if (kind == ChangeKind.WRITE && file.endsWith(PageFile.FILE) && armed.compareAndSet(true, false)) {
                        reached.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException exception) {
                            throw new IOException(exception);
                        }
                    }
                });
                var failure = new AtomicReference<Throwable>();
                var checkpoint = Thread.ofPlatform().start(() -> {
                    try {
                        ledger.checkpoint();
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                });
                assertTrue(reached.await(60, TimeUnit.SECONDS));
                // More than an interval of log past the held checkpoint's cut: the next checkpoint is due as it ends
                write(ledger, 1, first, first + during);
                assertTrue(live.checkpointer().startedForTests(), "the commits started no checkpoint");
                release.countDown();
                checkpoint.join();
                assertNull(failure.get());
                // The commits started the next checkpoint, which waited for the lock and caches its leaves from the first
                awaitBackground(live);
                assertTrue(live.pageCacheBytesForTests() > 128 * PageFormat.SIZE, live.pageCacheBytesForTests() + " bytes cached");
                // That one ended with nothing behind it, and no read used the leaves it cached: the next leaves out the
                // leaves that replace them. The cache only loses what the checkpoints before replaced meanwhile
                write(ledger, 2, first, first + last);
                var before = live.pageCacheBytesForTests();
                ledger.checkpoint();
                assertTrue(live.pageCacheBytesForTests() < before + 16 * PageFormat.SIZE,
                        live.pageCacheBytesForTests() - before + " bytes cached");
                verify(ledger, first + during, i -> i < first ? 0 : i < first + last ? 2 : 1);
                live.clearPageCacheForTests();
                verify(ledger, first + during, i -> i < first ? 0 : i < first + last ? 2 : 1);
            }
            try (var ledger = options(path, MIB).build()) {
                verify(ledger, first + during, i -> i < first ? 0 : i < first + last ? 2 : 1);
            }
        }

        @ParameterizedTest(name = "cache cleared: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a checkpoint that fails after flushing part of its units gives back its pages to one that succeeds")
        void failsAfterAFlush(boolean clear) throws Exception {
            var disk = CrashFileSystem.create();
            var path = disk.getPath("/store");
            // About 12 MiB of leaves, past the 8 MiB a checkpoint queues before it flushes
            var keys = 11_000;
            try (var ledger = options(path).build()) {
                var live = (LiveLedger) ledger;
                write(ledger, 0, 0, keys);
                var writes = new AtomicInteger();
                var failed = new AtomicBoolean();
                disk.intercept((file, kind) -> {
                    if (!file.endsWith(PageFile.FILE)) {
                        return;
                    }
                    if (kind == ChangeKind.WRITE) {
                        writes.incrementAndGet();
                    } else if (kind == ChangeKind.FORCE && failed.compareAndSet(false, true)) {
                        throw new IOException("The device failed");
                    }
                });
                assertThrows(LedgerIoException.class, ledger::checkpoint);
                // Each flush writes a window of 1 MiB at a time at least: more than 8 writes took two flushes
                assertTrue(writes.get() > 8, "the checkpoint wrote " + writes.get() + " times");
                disk.intercept((_, _) -> {
                });
                if (clear) {
                    live.clearPageCacheForTests();
                }
                verify(ledger, keys, _ -> 0);
                ledger.checkpoint();
                if (clear) {
                    live.clearPageCacheForTests();
                }
                verify(ledger, keys, _ -> 0);
                write(ledger, 1, 0, keys / 2);
                ledger.checkpoint();
                verify(ledger, keys, i -> i < keys / 2 ? 1 : 0);
            }
            try (var ledger = options(path).build()) {
                verify(ledger, keys, i -> i < keys / 2 ? 1 : 0);
            }
        }

        @Test
        @DisplayName("a snapshot taken before checkpoints that leave their leaves out reads its keys while they run and reuse pages")
        void pinnedSnapshot(@TempDir Path directory) throws Exception {
            var keys = 2_000;
            try (var ledger = options(directory).build()) {
                var live = (LiveLedger) ledger;
                write(ledger, 0, 0, keys);
                ledger.checkpoint();
                try (var snapshot = ledger.snapshot()) {
                    for (var round = 1; round <= 3; round++) {
                        write(ledger, round, 0, keys);
                        ledger.checkpoint();
                        live.clearPageCacheForTests();
                        verify(snapshot, keys, _ -> 0);
                        var current = round;
                        verify(ledger, keys, _ -> current);
                    }
                }
                // The pages the snapshot held go back, and the next checkpoints write over them
                write(ledger, 4, 0, keys);
                ledger.checkpoint();
                write(ledger, 5, keys / 3, keys);
                ledger.checkpoint();
                verify(ledger, keys, i -> i < keys / 3 ? 4 : 5);
                live.clearPageCacheForTests();
                verify(ledger, keys, i -> i < keys / 3 ? 4 : 5);
            }
        }
    }
}
