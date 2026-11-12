package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Scans whose cursor reads ahead because the file's reads are cold: they pass what a cursor that never reads ahead
/// passes and leave the page cache as it leaves it, they use what they read ahead, a unit they read ahead and cannot
/// open fails only the scan that reaches it, as it fails without reading ahead, and a read that joins units across a
/// gap opens each of them alone. Each tree is written straight through a [TreeUpdate], with values inline, in blobs of
/// a few pages and in blobs too long to read ahead, and the cold reads are forced, so no timing is involved.
class ColdScanReadAheadTest {
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final byte[] KEY = new byte[32];

    /// A tree written through [TreeUpdate] and the sorted map it must equal.
    private static final class Tree implements AutoCloseable {
        final Path directory;
        final PageFile pages;
        // Sized as a store sizes its pool for this cache, a quarter of it, and never trimmed by the idle ticker
        final HeapPool pool = new HeapPool(16L << 20, false);
        final FreeSpace space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
        final TreeMap<byte[], byte[]> model = new TreeMap<>(UNSIGNED);
        final Random random;
        TreeRoot root;
        DeltaTable deltas = DeltaTable.EMPTY;
        long nonce;

        Tree(Path directory, boolean encrypted, long seed) throws IOException {
            this.directory = directory;
            this.pages = PageFile.open(directory, new PageCipher(encrypted ? new SecretKeySpec(KEY, "AES") : null), 64L << 20, pool);
            this.random = new Random(seed);
        }

        void apply(TreeMap<byte[], byte[]> states) throws IOException {
            var ops = new ArrayList<TreeUpdate.Op>(states.size());
            states.forEach((key, value) -> {
                ops.add(new TreeUpdate.Op(MemorySegment.ofArray(key.clone()).asReadOnly(), MemorySegment.ofArray(value.clone()).asReadOnly()));
                model.put(key, value);
            });
            var writer = pages.writer(++nonce);
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, TreeUpdate.merge(List.of()));
            deltas = update.finishDeltas(deltas);
            writer.flush();
            update.freed().forEach(unit -> space.release(unit.page(), unit.pages()));
        }

        /// Writes `count` keys whose values are inline, in blobs of up to three pages, or in a blob of 18 pages.
        void load(int count) throws IOException {
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < count; i++) {
                states.put(key(i), value());
            }
            apply(states);
        }

        /// Rewrites a few scattered values, which leaves some leaves as delta records.
        void touch(int count) throws IOException {
            var keys = new ArrayList<>(model.keySet());
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < count; i++) {
                states.put(keys.get(random.nextInt(keys.size() - 1)), small());
            }
            apply(states);
        }

        byte[] value() {
            var kind = random.nextInt(100);
            if (kind < 2) {
                return bytes(70_000);
            }
            if (kind < 35) {
                return bytes(2_500 + random.nextInt(9_000));
            }
            return small();
        }

        byte[] small() {
            return bytes(10 + random.nextInt(200));
        }

        byte[] bytes(int length) {
            var bytes = new byte[length];
            random.nextBytes(bytes);
            return bytes;
        }

        /// {@return the leaves under the branches of level 1, in key order, each with its parent and index}
        List<Leaf> leaves() {
            var leaves = new ArrayList<Leaf>();
            collect(pages.read(root.ref(), PageFile.NODE, false), leaves);
            return leaves;
        }

        private void collect(byte[] unit, List<Leaf> leaves) {
            if (TreeNodes.level(unit) == 1) {
                for (var i = 0; i < TreeNodes.count(unit); i++) {
                    var ref = TreeNodes.childRef(unit, i);
                    var leaf = pages.read(ref, PageFile.NODE, false);
                    leaves.add(new Leaf(ref, unit, i, TreeNodes.leafKey(leaf, TreeNodes.entry(leaf, 0)).toArray(ValueLayout.JAVA_BYTE),
                            TreeNodes.count(leaf), leaf));
                }
                return;
            }
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                collect(pages.read(TreeNodes.childRef(unit, i), PageFile.NODE, false), leaves);
            }
        }

        /// Flips a byte in the middle of the unit `ref` names, in the file.
        void damage(PageRef ref) throws IOException {
            try (var channel = FileChannel.open(directory.resolve(PageFile.FILE), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                var position = PageFormat.offset(ref.page()) + PageFormat.SIZE / 2;
                var buffer = ByteBuffer.allocate(1);
                channel.read(buffer, position);
                buffer.put(0, (byte) (buffer.get(0) ^ 0x5A)).rewind();
                channel.write(buffer, position);
            }
        }

        @Override
        public void close() {
            pages.close();
        }
    }

    private record Leaf(PageRef ref, byte[] parent, int index, byte[] firstKey, int count, byte[] unit) {
    }

    private static byte[] key(int i) {
        return "key%07d".formatted(i).getBytes(StandardCharsets.US_ASCII);
    }

    /// {@return the entries a cursor passes, at most `limit`, the bytes the cache holds after it and the bytes the
    /// emptied pool holds after it, and, from a second cursor of the same scan over the same emptied cache, the bytes
    /// the pool then holds and the heap that second scan allocates on the calling thread}
    private static Result scan(Tree tree, byte[] from, byte[] to, boolean descending, int limit, boolean cold, long wanted) {
        // Two trims with no acquire between them empty the pool
        tree.pool.trimIfIdle();
        tree.pool.trimIfIdle();
        tree.pages.forceColdReadsForTests(cold);
        try {
            tree.pages.cache().clearForTests();
            var entries = entries(tree, from, to, descending, limit, wanted);
            var cached = tree.pages.cache().size();
            var pooled = tree.pool.pooledBytesForTests();
            tree.pages.cache().clearForTests();
            var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
            var before = threads.getCurrentThreadAllocatedBytes();
            entries(tree, from, to, descending, limit, wanted);
            var allocated = threads.getCurrentThreadAllocatedBytes() - before;
            return new Result(entries, cached, pooled, tree.pool.pooledBytesForTests(), allocated);
        } finally {
            tree.pages.forceColdReadsForTests(false);
        }
    }

    /// {@return the entries a cursor passes, at most `limit`, which then gives back what it read ahead into, as a scan
    /// of the store does}
    private static List<byte[][]> entries(Tree tree, byte[] from, byte[] to, boolean descending, int limit, long wanted) {
        var cursor = new TreeCursor(tree.pages, tree.root, from, to, descending, wanted);
        try {
            var entries = new ArrayList<byte[][]>();
            while (entries.size() < limit && cursor.next()) {
                entries.add(new byte[][]{cursor.key().toArray(ValueLayout.JAVA_BYTE), cursor.value().toArray(ValueLayout.JAVA_BYTE)});
            }
            return entries;
        } finally {
            cursor.release();
        }
    }

    private record Result(List<byte[][]> entries, long cached, long pooled, long pooledAgain, long allocated) {
    }

    private static List<byte[][]> expected(Tree tree, byte[] from, byte[] to, boolean descending, int limit) {
        var range = from == null && to == null ? tree.model
                : from == null ? tree.model.headMap(to, false)
                : to == null ? tree.model.tailMap(from, true)
                : tree.model.subMap(from, true, to, false);
        var view = descending ? range.descendingMap() : range;
        var entries = new ArrayList<byte[][]>();
        for (var entry : view.entrySet()) {
            if (entries.size() == limit) {
                break;
            }
            entries.add(new byte[][]{entry.getKey(), entry.getValue()});
        }
        return entries;
    }

    private static void assertSameEntries(List<byte[][]> expected, List<byte[][]> actual, String message) {
        assertEquals(expected.size(), actual.size(), message + ": entries");
        for (var i = 0; i < expected.size(); i++) {
            assertArrayEquals(expected.get(i)[0], actual.get(i)[0], message + ": key " + i);
            assertArrayEquals(expected.get(i)[1], actual.get(i)[1], message + ": value " + i);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("passes what a cursor that never reads ahead passes, caches what it caches and allocates little more, its pool arrays reused, at every leaf boundary, both ways")
    void sameAsWithout(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var tree = new Tree(directory, encrypted, encrypted ? 7 : 11)) {
            tree.load(4_000);
            tree.touch(40);
            var leaves = tree.leaves();
            assertTrue(leaves.size() > 2 * TreeCursor.GATHER_LEAVES, "too few leaves: " + leaves.size());
            assertTrue(leaves.stream().anyMatch(leaf -> leaf.ref().isRecord()), "no leaf is a delta record");
            var bounds = new ArrayList<byte[]>();
            bounds.add(null);
            for (var i = 0; i < leaves.size(); i += 3) {
                bounds.add(leaves.get(i).firstKey());
            }
            // Keys between those the tree holds, and past them
            bounds.add("key0001234x".getBytes(StandardCharsets.US_ASCII));
            bounds.add("zzz".getBytes(StandardCharsets.US_ASCII));
            var random = new Random(3);
            var cases = 0;
            for (var from : bounds) {
                for (var descending : new boolean[]{false, true}) {
                    for (var limit : new int[]{1, 7, 60, Integer.MAX_VALUE}) {
                        var to = random.nextBoolean() ? null : bounds.get(1 + random.nextInt(bounds.size() - 1));
                        if (from != null && to != null && Arrays.compareUnsigned(from, to) >= 0) {
                            continue;
                        }
                        var low = from;
                        var message = "from " + (low == null ? null : new String(low, StandardCharsets.US_ASCII)) + " to "
                                + (to == null ? null : new String(to, StandardCharsets.US_ASCII)) + " descending " + descending + " limit " + limit;
                        var expected = expected(tree, low, to, descending, limit);
                        var plain = scan(tree, low, to, descending, limit, false, 0);
                        assertSameEntries(expected, plain.entries(), message + ", without reading ahead");
                        for (var wanted : new long[]{limit, Long.MAX_VALUE}) {
                            var cold = scan(tree, low, to, descending, limit, true, wanted);
                            assertSameEntries(expected, cold.entries(), message + ", reading ahead for " + wanted);
                            assertEquals(plain.cached(), cold.cached(), message + ": the cache holds other units, reading ahead for " + wanted);
                            assertEquals(cold.pooled(), cold.pooledAgain(), message + ": the pool grows by the same scan again, reading ahead for " + wanted);
                            // The leaves and blobs it read ahead lie in arrays of the pool, and only those the cursor
                            // reaches are copied out, as its own reads copy them; what is left is its bookkeeping
                            assertTrue(cold.allocated() <= plain.allocated() + plain.allocated() / 4 + (64 << 10),
                                    message + ": a scan reading ahead allocates " + cold.allocated() + " bytes, against "
                                            + plain.allocated() + " without, reading ahead for " + wanted);
                        }
                        cases++;
                    }
                }
            }
            assertTrue(cases > 100, "only " + cases + " cases");
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("uses the siblings and blobs it read ahead, not the file, once it has read them")
    void usesWhatItRead(boolean descending, @TempDir Path directory) throws Exception {
        try (var tree = new Tree(directory, true, descending ? 13 : 17)) {
            tree.load(4_000);
            var leaves = tree.leaves();
            var step = descending ? -1 : 1;
            // A leaf with two siblings after it, in the scan's order, under the same parent, whose blobs one gather reads
            var start = -1;
            for (var i = 2; i < leaves.size() - 2 && start < 0; i++) {
                var leaf = leaves.get(i);
                if (leaves.get(i + step).parent() == leaf.parent() && leaves.get(i + 2 * step).parent() == leaf.parent()
                        && gatheredBlobs(leaf).size() + gatheredBlobs(leaves.get(i + step)).size()
                        + gatheredBlobs(leaves.get(i + 2 * step)).size() <= TreeCursor.GATHER_VALUES) {
                    start = i;
                }
            }
            assertTrue(start >= 0);
            var from = descending ? null : leaves.get(start).firstKey();
            var to = descending ? leaves.get(start + 1).firstKey() : null;
            var expected = expected(tree, from, to, descending, Integer.MAX_VALUE);
            var batch = leaves.get(start).count() + leaves.get(start + step).count() + leaves.get(start + 2 * step).count();
            tree.pages.cache().clearForTests();
            tree.pages.forceColdReadsForTests(true);
            try {
                var cursor = new TreeCursor(tree.pages, tree.root, from, to, descending, Long.MAX_VALUE);
                // The cursor gathered its first leaf with the two after it; damaged now, the file no longer holds them
                for (var sibling = 1; sibling <= 2; sibling++) {
                    var leaf = leaves.get(start + sibling * step);
                    tree.damage(leaf.ref());
                    for (var blob : gatheredBlobs(leaf)) {
                        tree.damage(blob);
                    }
                }
                for (var i = 0; i < batch; i++) {
                    assertTrue(cursor.next(), "entry " + i);
                    assertArrayEquals(expected.get(i)[0], cursor.key().toArray(ValueLayout.JAVA_BYTE), "key " + i);
                    assertArrayEquals(expected.get(i)[1], cursor.value().toArray(ValueLayout.JAVA_BYTE), "value " + i);
                }
            } finally {
                tree.pages.forceColdReadsForTests(false);
            }
            // Without reading ahead, the same scan reads the damaged leaves
            tree.pages.cache().clearForTests();
            var plain = new TreeCursor(tree.pages, tree.root, from, to, descending);
            assertThrows(LedgerCorruptedException.class, () -> {
                for (var i = 0; i < batch; i++) {
                    plain.next();
                    plain.value();
                }
            });
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a damaged unit read ahead fails no scan that stops before it, and the scan that reaches it as it fails without reading ahead")
    void damagedAhead(boolean descending, @TempDir Path directory) throws Exception {
        try (var tree = new Tree(directory, true, descending ? 19 : 23)) {
            tree.load(4_000);
            var leaves = tree.leaves();
            var step = descending ? -1 : 1;
            var start = -1;
            for (var i = 2; i < leaves.size() - 2 && start < 0; i++) {
                var leaf = leaves.get(i);
                if (leaves.get(i + step).parent() == leaf.parent() && leaves.get(i + 2 * step).parent() == leaf.parent()
                        && hasBlob(leaves.get(i + 2 * step))) {
                    start = i;
                }
            }
            assertTrue(start >= 0);
            // The next leaf, and a blob of the one after it
            tree.damage(leaves.get(start + step).ref());
            var third = leaves.get(start + 2 * step);
            for (var i = 0; i < third.count(); i++) {
                var entry = TreeNodes.entry(third.unit(), i);
                if (TreeNodes.isBlob(third.unit(), entry)) {
                    tree.damage(TreeNodes.blobRef(third.unit(), entry));
                    break;
                }
            }
            var from = descending ? null : leaves.get(start).firstKey();
            var to = descending ? leaves.get(start + 1).firstKey() : null;
            var count = leaves.get(start).count();
            var expected = expected(tree, from, to, descending, count);
            for (var wanted : new long[]{count, Long.MAX_VALUE}) {
                var stopped = scan(tree, from, to, descending, count, true, wanted);
                assertSameEntries(expected, stopped.entries(), "a scan of the first leaf, for " + wanted);
            }
            var plain = assertThrows(LedgerCorruptedException.class, () -> scan(tree, from, to, descending, count + 1, false, 0));
            var cold = assertThrows(LedgerCorruptedException.class, () -> scan(tree, from, to, descending, count + 1, true, Long.MAX_VALUE));
            assertEquals(plain.getMessage(), cold.getMessage());
            // Past the damaged leaf, from the one after it, to its damaged blob
            var past = descending ? null : third.firstKey();
            var pastTo = descending ? leaves.get(start + 2 * step + 1).firstKey() : null;
            var plainBlob = assertThrows(LedgerCorruptedException.class, () -> scan(tree, past, pastTo, descending, Integer.MAX_VALUE, false, 0));
            var coldBlob = assertThrows(LedgerCorruptedException.class, () -> scan(tree, past, pastTo, descending, Integer.MAX_VALUE, true, Long.MAX_VALUE));
            assertEquals(plainBlob.getMessage(), coldBlob.getMessage());
        }
    }

    /// {@return the blobs of `leaf` short enough for a gather to read}
    private static List<PageRef> gatheredBlobs(Leaf leaf) {
        var blobs = new ArrayList<PageRef>();
        for (var i = 0; i < leaf.count(); i++) {
            var entry = TreeNodes.entry(leaf.unit(), i);
            if (TreeNodes.isBlob(leaf.unit(), entry) && TreeNodes.blobRef(leaf.unit(), entry).pages() <= TreeCursor.GATHER_VALUE_PAGES) {
                blobs.add(TreeNodes.blobRef(leaf.unit(), entry));
            }
        }
        return blobs;
    }

    private static boolean hasBlob(Leaf leaf) {
        for (var i = 0; i < leaf.count(); i++) {
            if (TreeNodes.isBlob(leaf.unit(), TreeNodes.entry(leaf.unit(), i))) {
                return true;
            }
        }
        return false;
    }

    /// {@return the units `refs` name, read ahead and each opened as `kind`, or `null` where one cannot be}
    private static byte[][] prefetched(PageFile pages, List<PageRef> refs, byte kind) {
        var offsets = new int[refs.size()];
        var sealed = pages.prefetch(refs.toArray(PageRef[]::new), refs.size(), offsets);
        try {
            var units = new byte[refs.size()][];
            for (var i = 0; i < units.length; i++) {
                units[i] = offsets[i] < 0 ? null : pages.openPrefetched(refs.get(i), kind, sealed, offsets[i]);
            }
            return units;
        } finally {
            pages.releasePrefetched(sealed);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a read that joins units across gaps opens each alone: a damaged, misnamed or missing one hides none of the others")
    void joinedReads(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var tree = new Tree(directory, encrypted, 29)) {
            var pages = tree.pages;
            var writer = pages.writer(1);
            var random = new Random(31);
            var refs = new ArrayList<PageRef>();
            var values = new ArrayList<byte[]>();
            for (var i = 0; i < 12; i++) {
                // Gaps of 0 to 9 pages: the widest splits the units into two reads
                var gap = i == 6 ? PageFile.GAP_PAGES + 1 : random.nextInt(3);
                if (gap > 0) {
                    tree.space.allocate(gap);
                }
                var length = 100 + random.nextInt(6_000);
                var value = tree.bytes(length);
                var unitPages = PageFormat.pagesFor(length);
                var unit = new byte[PageFormat.HEADER + length];
                System.arraycopy(value, 0, unit, PageFormat.HEADER, length);
                refs.add(writer.write(PageFormat.VALUE, 0, tree.space.allocate(unitPages), unitPages, unit, length, false));
                values.add(value);
            }
            writer.flush();
            pages.cache().clearForTests();
            tree.damage(refs.get(3));
            var misnamed = new PageRef(refs.get(8).page(), refs.get(8).pages(), refs.get(8).nonceHigh() + 1, refs.get(8).nonceLow());
            var last = refs.getLast();
            // Past the end of the file, joined with the last unit
            var missing = new PageRef(last.page() + last.pages() + 2, 1, 1, 1);
            var asked = new ArrayList<>(refs);
            asked.set(8, misnamed);
            asked.add(missing);
            // Out of page order, as the cursor asks for them going down
            java.util.Collections.reverse(asked);
            var units = prefetched(pages, asked, PageFormat.VALUE);
            for (var i = 0; i < asked.size(); i++) {
                var ref = asked.get(i);
                var at = refs.indexOf(ref);
                if (ref == missing || ref == misnamed || at == 3) {
                    assertNull(units[i], "unit " + i);
                    continue;
                }
                assertNotNull(units[i], "unit " + at);
                assertEquals(PageFormat.cachedLength(ref.pages(), values.get(at).length), units[i].length, "unit " + at + " is not as the cache keeps it");
                assertArrayEquals(values.get(at), Arrays.copyOfRange(units[i], PageFormat.HEADER, PageFormat.HEADER + values.get(at).length));
                var read = pages.read(ref, PageFormat.VALUE, true);
                assertArrayEquals(read, units[i], "unit " + at + " differs from what a read caches");
            }
            // A kind the units do not have opens none of them
            pages.cache().clearForTests();
            var leaves = prefetched(pages, refs, PageFormat.LEAF);
            for (var unit : leaves) {
                assertNull(unit);
            }
            assertEquals(0, pages.cache().size(), "a read ahead cached a unit");
        }
    }

    @Test
    @DisplayName("counts reads as cold after a few slow ones and warm after a few quick ones, whatever a sample says")
    void detector(@TempDir Path directory) throws Exception {
        try (var tree = new Tree(directory, false, 37)) {
            var pages = tree.pages;
            assertFalse(pages.coldReads());
            for (var i = 0; i < 4; i++) {
                pages.sampleRead(145_000, PageFormat.SIZE);
            }
            assertTrue(pages.coldReads(), pages.readMicrosForTests() + " microseconds");
            for (var i = 0; i < 8; i++) {
                pages.sampleRead(5_000, PageFormat.SIZE);
            }
            assertFalse(pages.coldReads(), pages.readMicrosForTests() + " microseconds");
            // A read a pause stretched cannot overflow the average, nor keep it cold for long
            for (var i = 0; i < 100; i++) {
                pages.sampleRead(Long.MAX_VALUE, PageFormat.SIZE);
            }
            assertTrue(pages.readMicrosForTests() > 0 && pages.readMicrosForTests() <= 2 * PageFile.COLD_READ_MICROS, pages.readMicrosForTests() + " microseconds");
            for (var i = 0; i < 10; i++) {
                pages.sampleRead(5_000, PageFormat.SIZE);
            }
            assertFalse(pages.coldReads(), pages.readMicrosForTests() + " microseconds");
            pages.sampleRead(-1, PageFormat.SIZE);
            assertTrue(pages.readMicrosForTests() >= 0);
            // Quick reads of the OS cache, two of them stretched in a row: still warm; a third in a row reads as cold
            for (var i = 0; i < 20; i++) {
                pages.sampleRead(5_000, PageFormat.SIZE);
            }
            pages.sampleRead(Long.MAX_VALUE, PageFormat.SIZE);
            pages.sampleRead(Long.MAX_VALUE, PageFormat.SIZE);
            assertFalse(pages.coldReads(), pages.readMicrosForTests() + " microseconds after two stretched reads");
            pages.sampleRead(Long.MAX_VALUE, PageFormat.SIZE);
            assertTrue(pages.coldReads(), pages.readMicrosForTests() + " microseconds after three stretched reads");
            for (var i = 0; i < 3; i++) {
                pages.sampleRead(5_000, PageFormat.SIZE);
            }
            assertFalse(pages.coldReads(), pages.readMicrosForTests() + " microseconds after three quick reads");
            // A long read out of the OS cache is no read of the device
            for (var i = 0; i < 8; i++) {
                pages.sampleRead(400_000, PageFile.RUN_PAGES * PageFormat.SIZE);
            }
            assertFalse(pages.coldReads(), pages.readMicrosForTests() + " microseconds");
            pages.forceColdReadsForTests(true);
            assertTrue(pages.coldReads());
            pages.forceColdReadsForTests(false);
            assertFalse(pages.coldReads());
        }
    }

    @Test
    @DisplayName("a store closed under scans that read ahead fails them only as a closed store fails a scan")
    void closedUnderScans(@TempDir Path directory) throws Exception {
        var messages = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
        var random = new Random(41);
        try (var seed = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(messages).build()) {
            var batch = seed.newBatch();
            for (var i = 0; i < 3_000; i++) {
                var value = new byte[random.nextInt(3) == 0 ? 3_000 + random.nextInt(4_000) : 50];
                random.nextBytes(value);
                batch.put(messages, "m%06d".formatted(i), value);
            }
            seed.commit(batch);
            seed.checkpoint();
        }
        PageFile.forceColdNewForTests(true);
        try {
            for (var round = 0; round < 5; round++) {
                var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(messages).build();
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var stop = new AtomicBoolean();
                var threads = new ArrayList<Thread>();
                var clear = round % 2 == 0;
                for (var t = 0; t < 4; t++) {
                    var descending = t % 2 == 1;
                    threads.add(Thread.ofVirtual().start(() -> {
                        try {
                            while (!stop.get()) {
                                if (clear) {
                                    ((LiveLedger) ledger).clearPageCacheForTests();
                                }
                                var range = KeyRange.<String>all();
                                ledger.forEach(messages, descending ? range.reversed() : range, (_, _) -> true);
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                Thread.sleep(20 + 20L * round);
                ledger.close();
                stop.set(true);
                for (var thread : threads) {
                    thread.join(30_000);
                    assertFalse(thread.isAlive(), "a scan did not end");
                }
                for (var failure : failures) {
                    if (!(failure instanceof IllegalStateException) && !(failure instanceof LedgerIoException)) {
                        fail("a scan failed with " + failure, failure);
                    }
                }
            }
        } finally {
            PageFile.forceColdNewForTests(false);
        }
        // The store is whole
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(messages).build()) {
            assertEquals(3_000, ledger.forEach(messages, KeyRange.all(), (_, _) -> true));
        }
    }

    @Test
    @DisplayName("a store whose reads are cold passes every scan, range and first as one whose reads are not")
    void storeScans(@TempDir Path directory) throws Exception {
        var messages = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
        var random = new Random(43);
        var model = new TreeMap<String, byte[]>();
        try (var seed = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(messages).build()) {
            var batch = seed.newBatch();
            for (var i = 0; i < 3_000; i++) {
                var value = new byte[random.nextInt(3) == 0 ? 3_000 + random.nextInt(4_000) : 50];
                random.nextBytes(value);
                var key = "m%06d".formatted(i);
                batch.put(messages, key, value);
                model.put(key, value);
            }
            seed.commit(batch);
            seed.checkpoint();
            // Some keys only in memory, shadowing the tree's
            var later = seed.newBatch();
            for (var i = 0; i < 3_000; i += 97) {
                var key = "m%06d".formatted(i);
                later.put(messages, key, new byte[]{1});
                model.put(key, new byte[]{1});
            }
            seed.commit(later);
            seed.checkpoint();
        }
        PageFile.forceColdNewForTests(true);
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(messages).build()) {
            for (var i = 0; i < 200; i++) {
                ((LiveLedger) ledger).clearPageCacheForTests();
                var from = "m%06d".formatted(random.nextInt(3_100));
                var to = "m%06d".formatted(random.nextInt(3_100));
                if (from.compareTo(to) >= 0) {
                    continue;
                }
                var limit = 1 + random.nextInt(80);
                var descending = random.nextBoolean();
                var range = KeyRange.between(from, to);
                var expected = new ArrayList<Map.Entry<String, byte[]>>(descending
                        ? model.subMap(from, true, to, false).descendingMap().entrySet()
                        : model.subMap(from, true, to, false).entrySet());
                var found = ledger.range(messages, descending ? range.reversed() : range, limit);
                assertEquals(Math.min(limit, expected.size()), found.size());
                for (var j = 0; j < found.size(); j++) {
                    assertEquals(expected.get(j).getKey(), found.get(j).key());
                    assertArrayEquals(expected.get(j).getValue(), found.get(j).value());
                }
                var visited = new ArrayList<String>();
                ledger.forEach(messages, descending ? range.reversed() : range, (key, value) -> {
                    assertArrayEquals(model.get(key), value, key);
                    visited.add(key);
                    return visited.size() < limit;
                });
                assertEquals(found.stream().map(com.github.auties00.ledger.Entry::key).toList(), visited);
                var first = ledger.first(messages, descending ? range.reversed() : range);
                assertEquals(expected.isEmpty() ? null : expected.getFirst().getKey(), first.map(com.github.auties00.ledger.Entry::key).orElse(null));
            }
        } finally {
            PageFile.forceColdNewForTests(false);
        }
    }
}
