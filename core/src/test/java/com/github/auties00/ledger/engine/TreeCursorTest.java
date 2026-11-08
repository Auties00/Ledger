package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The cursor's views of a leaf and its per-leaf bound check, against [PerEntryCursor], the cursor that compares every
/// entry with the bound and makes a view for every key and value: random trees with delta leaves, bounds at and around
/// the leaves' edges, and leaves without entries, which only a hand-made tree holds.
class TreeCursorTest {
    private static final java.util.Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("random trees scan as the per-entry cursor does, with bounds at, between and outside the leaves")
    void randomTrees(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 31 : 37);
        try (var pages = open(directory, encrypted, 1 << 20)) {
            var built = randomTree(pages, random);
            assertTrue(built.records() > 0, "no leaf was ever written as a delta record");
            var leaves = TreeSearch.leaves(pages, built.root());
            assertTrue(leaves.size() > 8, "the tree has too few leaves to cross");
            assertScan(pages, built.root(), built.model(), null, null);
            for (var i = 0; i < 400; i++) {
                var from = bound(random, leaves);
                var to = bound(random, leaves);
                if (from != null && to != null && UNSIGNED.compare(from, to) > 0) {
                    var swap = from;
                    from = to;
                    to = swap;
                }
                if (from != null && to != null && UNSIGNED.compare(from, to) == 0) {
                    continue;
                }
                assertScan(pages, built.root(), built.model(), from, to);
                if (i % 50 == 0) {
                    pages.cache().clearForTests();
                }
            }
        }
    }

    @Test
    @DisplayName("every first and last key of a leaf works as either bound, in either direction")
    void everyLeafEdge(@TempDir Path directory) throws Exception {
        var random = new Random(41);
        try (var pages = open(directory, true, 1 << 20)) {
            var built = randomTree(pages, random);
            var leaves = TreeSearch.leaves(pages, built.root());
            for (var leaf : leaves) {
                if (TreeNodes.count(leaf) == 0) {
                    continue;
                }
                var first = keyAt(leaf, 0);
                var last = keyAt(leaf, TreeNodes.count(leaf) - 1);
                // The key right after the last: an exclusive bound there takes the last entry in, and the next out
                var pastLast = Arrays.copyOf(last, last.length + 1);
                assertScan(pages, built.root(), built.model(), first, null);
                assertScan(pages, built.root(), built.model(), null, first);
                assertScan(pages, built.root(), built.model(), last, null);
                assertScan(pages, built.root(), built.model(), null, last);
                assertScan(pages, built.root(), built.model(), first, pastLast);
                assertScan(pages, built.root(), built.model(), null, pastLast);
                if (UNSIGNED.compare(first, last) < 0) {
                    assertScan(pages, built.root(), built.model(), first, last);
                }
            }
        }
    }

    @Test
    @DisplayName("a cursor over no tree, or over a range before or past the tree, has no entry")
    void emptyRanges(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 20)) {
            assertFalse(new TreeCursor(pages, null, null, null, false).next());
            assertFalse(new TreeCursor(pages, null, null, null, true).next());
            var built = randomTree(pages, new Random(43));
            var lowest = new byte[]{0};
            var highest = new byte[64];
            Arrays.fill(highest, (byte) 0xFF);
            // A descending seek that finds no child before the bound leaves the cursor without a leaf
            assertScan(pages, built.root(), built.model(), null, new byte[0]);
            assertScan(pages, built.root(), built.model(), null, lowest);
            assertScan(pages, built.root(), built.model(), highest, null);
            var after = Arrays.copyOf(highest, 65);
            assertScan(pages, built.root(), built.model(), highest, after);
        }
    }

    @ParameterizedTest(name = "scan of {0} bound")
    @ValueSource(strings = {"no", "from", "to", "both"})
    @DisplayName("leaves without entries are stepped over, whichever leaf a seek lands on")
    void emptyLeaves(String bounds, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 20)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            var empty1 = leaf(writer, space, model);
            var low = leaf(writer, space, model, "b", "d");
            var empty2 = leaf(writer, space, model);
            var empty3 = leaf(writer, space, model);
            var high = leaf(writer, space, model, "f", "h");
            var empty4 = leaf(writer, space, model);
            var children = List.of(
                    child("a", empty1, 0),
                    child("b", low, 2),
                    child("e", empty2, 0),
                    child("eg", empty3, 0),
                    child("f", high, 2),
                    child("i", empty4, 0));
            var unit = new byte[PageFormat.SIZE];
            var length = TreeNodes.writeBranch(unit, children, 0, children.size());
            var root = new TreeRoot(writer.write(PageFormat.BRANCH, 1, space.allocate(1), 1, unit, length, false), 4, false);
            writer.flush();
            var points = new String[]{"a", "aa", "b", "c", "d", "da", "e", "ef", "eg", "eh", "f", "g", "h", "ha", "i", "j"};
            switch (bounds) {
                case "no" -> assertScan(pages, root, model, null, null);
                case "from" -> {
                    for (var from : points) {
                        assertScan(pages, root, model, from.getBytes(), null);
                    }
                }
                case "to" -> {
                    for (var to : points) {
                        assertScan(pages, root, model, null, to.getBytes());
                    }
                }
                default -> {
                    for (var from : points) {
                        for (var to : points) {
                            if (from.compareTo(to) < 0) {
                                assertScan(pages, root, model, from.getBytes(), to.getBytes());
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("a root leaf without entries has no entry, and neither bound reads one")
    void emptyRootLeaf(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 20)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            var ref = leaf(writer, space, new TreeMap<>(UNSIGNED));
            writer.flush();
            var root = new TreeRoot(ref, 0, false);
            for (var descending : new boolean[]{false, true}) {
                assertFalse(new TreeCursor(pages, root, null, null, descending).next());
                assertFalse(new TreeCursor(pages, root, segment("a".getBytes()), segment("b".getBytes()), descending).next());
            }
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("keys and inline values are read-only, stop at their own end, and stay valid after the cursor left the leaf")
    void views(boolean descending, @TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 20)) {
            var built = randomTree(pages, new Random(47));
            var cursor = new TreeCursor(pages, built.root(), null, null, descending);
            var keys = new ArrayList<MemorySegment>();
            var values = new ArrayList<MemorySegment>();
            var inline = 0;
            while (cursor.next()) {
                var key = cursor.key();
                var value = cursor.value();
                assertReadOnly(key);
                assertReadOnly(value);
                assertThrows(IndexOutOfBoundsException.class, () -> key.get(ValueLayout.JAVA_BYTE, key.byteSize()));
                assertThrows(IndexOutOfBoundsException.class, () -> value.get(ValueLayout.JAVA_BYTE, value.byteSize()));
                keys.add(key);
                values.add(value);
                if (value.byteSize() < 300) {
                    inline++;
                }
            }
            assertTrue(inline > 0, "the tree holds no inline value");
            var ordered = new ArrayList<>((descending ? built.model().descendingMap() : built.model()).entrySet());
            assertEquals(ordered.size(), keys.size());
            for (var i = 0; i < ordered.size(); i++) {
                assertArrayEquals(ordered.get(i).getKey(), bytes(keys.get(i)));
                assertArrayEquals(ordered.get(i).getValue(), bytes(values.get(i)));
            }
        }
    }

    @Test
    @DisplayName("a scan makes one view per leaf: the keys of a leaf share a scope, and no two leaves do")
    void oneViewPerLeaf(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 20)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 5_000; i++) {
                ops.add(new TreeUpdate.Op(segment("k%06d".formatted(i).getBytes()), segment(new byte[100])));
            }
            var writer = pages.writer(1);
            var root = new TreeUpdate(pages, writer, space).apply(null, ops, List.of());
            writer.flush();
            for (var descending : new boolean[]{false, true}) {
                var scopes = Collections.newSetFromMap(new IdentityHashMap<MemorySegment.Scope, Boolean>());
                var cursor = new TreeCursor(pages, root, null, null, descending);
                while (cursor.next()) {
                    scopes.add(cursor.key().scope());
                    scopes.add(cursor.value().scope());
                }
                assertEquals(TreeSearch.leaves(pages, root).size(), scopes.size());
            }
        }
    }

    @Test
    @DisplayName("cursors of several threads read the same leaves, through a cache that evicts them, without seeing each other")
    void concurrentCursors(@TempDir Path directory) throws Exception {
        try (var pages = open(directory, true, 1 << 18)) {
            var built = randomTree(pages, new Random(53));
            var leaves = TreeSearch.leaves(pages, built.root());
            try (var executor = Executors.newFixedThreadPool(4)) {
                var tasks = new ArrayList<Callable<Void>>();
                for (var thread = 0; thread < 4; thread++) {
                    var random = new Random(thread);
                    tasks.add(() -> {
                        for (var i = 0; i < 60; i++) {
                            var from = bound(random, leaves);
                            var to = bound(random, leaves);
                            if (from != null && to != null && UNSIGNED.compare(from, to) >= 0) {
                                continue;
                            }
                            assertScan(pages, built.root(), built.model(), from, to);
                        }
                        return null;
                    });
                }
                for (var future : executor.invokeAll(tasks)) {
                    future.get();
                }
            }
        }
    }

    @ParameterizedTest(name = "range removal: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("scans merge the tree with versions in memory on every leaf, and with a range removal over it")
    void scansOverMemory(boolean removal, @TempDir Path directory) {
        var keyspace = Keyspace.paged("names", KeyCodec.bytes(), ValueCodec.bytes());
        var random = new Random(removal ? 59 : 61);
        var model = new TreeMap<byte[], byte[]>(UNSIGNED);
        var encryption = Encryption.key(new byte[32]);
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(keyspace).build()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 12_000; i++) {
                var key = key(i * 2);
                var value = new byte[random.nextInt(200)];
                random.nextBytes(value);
                batch.put(keyspace, key, value);
                model.put(key, value);
            }
            ledger.commit(batch);
            ledger.checkpoint();
            batch = ledger.newBatch();
            // Often enough for the tree's entries to meet a version in memory in every leaf
            for (var i = 0; i < 12_000; i += 16) {
                switch (i % 3) {
                    case 0 -> {
                        // A new key, between two of the tree's
                        var value = new byte[]{1, (byte) i};
                        batch.put(keyspace, key(i * 2 + 1), value);
                        model.put(key(i * 2 + 1), value);
                    }
                    case 1 -> {
                        var value = new byte[]{2, (byte) i};
                        batch.put(keyspace, key(i * 2), value);
                        model.put(key(i * 2), value);
                    }
                    default -> {
                        batch.delete(keyspace, key(i * 2));
                        model.remove(key(i * 2));
                    }
                }
            }
            ledger.commit(batch);
            if (removal) {
                batch = ledger.newBatch();
                batch.deleteRange(keyspace, KeyRange.between(key(5_000), key(9_000)));
                ledger.commit(batch);
                model.subMap(key(5_000), true, key(9_000), false).clear();
            }
            assertLedgerScan(ledger, keyspace, model, KeyRange.all());
            for (var i = 0; i < 60; i++) {
                var a = random.nextInt(25_000);
                var b = random.nextInt(25_000);
                if (a != b) {
                    assertLedgerScan(ledger, keyspace, model, KeyRange.between(key(Math.min(a, b)), key(Math.max(a, b))));
                }
            }
        }
    }

    private static void assertLedgerScan(Ledger ledger, Keyspace<byte[], byte[]> keyspace, TreeMap<byte[], byte[]> model, KeyRange<byte[]> range) {
        var expected = range.from().isPresent() ? model.subMap(range.from().get(), true, range.to().orElseThrow(), false) : model;
        for (var descending : new boolean[]{false, true}) {
            var ordered = new ArrayList<>((descending ? expected.descendingMap() : expected).entrySet());
            var visited = new int[1];
            ledger.forEachEncoded(keyspace, descending ? range.reversed() : range, (key, value) -> {
                var entry = ordered.get(visited[0]++);
                assertArrayEquals(entry.getKey(), bytes(key));
                assertArrayEquals(entry.getValue(), bytes(value));
                return true;
            });
            assertEquals(ordered.size(), visited[0]);
        }
    }

    private static byte[] key(int index) {
        return "k%07d".formatted(index).getBytes();
    }

    private static PageFile open(Path directory, boolean encrypted, int cacheBytes) throws IOException {
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
        return PageFile.open(directory, cipher, cacheBytes, new HeapPool(1 << 20));
    }

    /// A tree and the entries it holds.
    ///
    /// @param root the root
    /// @param model the entries
    /// @param records the delta records the tree refers to
    private record Built(TreeRoot root, TreeMap<byte[], byte[]> model, int records) {
    }

    /// {@return a tree made by a bulk write and rounds of few writes and removals, as checkpoints make one, with some
    /// of its leaves read back from delta records}
    private static Built randomTree(PageFile pages, Random random) throws IOException {
        var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
        var model = new TreeMap<byte[], byte[]>(UNSIGNED);
        var keys = new ArrayList<byte[]>();
        for (var i = 0; i < 12_000; i++) {
            var key = new byte[1 + random.nextInt(30)];
            random.nextBytes(key);
            // Some keys share prefixes with others, the order prefixes of a compare decide
            if (i > 0 && random.nextInt(4) == 0) {
                var other = keys.get(random.nextInt(i));
                System.arraycopy(other, 0, key, 0, Math.min(other.length, key.length));
            }
            keys.add(key);
        }
        TreeRoot root = null;
        var deltas = DeltaTable.EMPTY;
        var records = 0;
        for (var round = 0; round < 12; round++) {
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            var writes = round == 0 ? 10_000 : 40 + random.nextInt(60);
            for (var i = 0; i < writes; i++) {
                var value = new byte[random.nextInt(20) == 0 ? 2_500 + random.nextInt(4_000) : random.nextInt(250)];
                random.nextBytes(value);
                states.put(keys.get(random.nextInt(keys.size())), round > 0 && random.nextInt(3) == 0 ? null : value);
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
            var writer = pages.writer(round + 1);
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, List.of());
            deltas = update.finishDeltas(deltas);
            writer.flush();
            for (var unit : update.freed()) {
                space.release(unit.page(), unit.pages());
            }
        }
        pages.cache().clearForTests();
        var refs = new java.util.TreeMap<Integer, Integer>();
        CheckpointTest.walk(pages, root.ref(), new BitSet(), new BitSet(), refs);
        for (var count : refs.values()) {
            records += count;
        }
        return new Built(root, model, records);
    }

    /// {@return a bound for a scan of a tree of `leaves`: none, or a key at, inside, between or outside them}
    private static byte[] bound(Random random, List<byte[]> leaves) {
        var leaf = leaves.get(random.nextInt(leaves.size()));
        var count = TreeNodes.count(leaf);
        if (count == 0) {
            return null;
        }
        var first = keyAt(leaf, 0);
        var last = keyAt(leaf, count - 1);
        return switch (random.nextInt(9)) {
            case 0 -> null;
            case 1 -> first;
            case 2 -> last;
            // Right after the last key, which is before the first key of the next leaf
            case 3 -> Arrays.copyOf(last, last.length + 1);
            // Right before the first key, as far as a key can be, between this leaf and the one before
            case 4 -> first[first.length - 1] == 0 ? Arrays.copyOf(first, first.length - 1) : before(first);
            case 5 -> keyAt(leaf, random.nextInt(count));
            case 6 -> Arrays.copyOf(keyAt(leaf, random.nextInt(count)), keyAt(leaf, 0).length / 2 + 1);
            case 7 -> new byte[]{0};
            default -> {
                var high = new byte[40];
                Arrays.fill(high, (byte) 0xFF);
                yield high;
            }
        };
    }

    private static byte[] before(byte[] key) {
        var lower = key.clone();
        lower[lower.length - 1]--;
        return lower;
    }

    private static byte[] keyAt(byte[] leaf, int index) {
        return bytes(TreeNodes.leafKey(leaf, TreeNodes.entry(leaf, index)));
    }

    /// Writes a leaf of `keys`, whose values are the keys, and adds them to `model`.
    private static PageRef leaf(PageFile.Writer writer, FreeSpace space, TreeMap<byte[], byte[]> model, String... keys) throws IOException {
        var entries = new ArrayList<TreeNodes.LeafEntry>();
        for (var key : keys) {
            var bytes = key.getBytes();
            entries.add(new TreeNodes.LeafEntry(segment(bytes), segment(bytes), null, bytes.length));
            model.put(bytes, bytes);
        }
        var unit = new byte[PageFormat.SIZE];
        var length = TreeNodes.writeLeaf(unit, entries, 0, entries.size());
        return writer.write(PageFormat.LEAF, 0, space.allocate(1), 1, unit, length, false);
    }

    private static TreeNodes.Child child(String firstKey, PageRef ref, long count) {
        return new TreeNodes.Child(segment(firstKey.getBytes()), ref, count, false, 0, -1);
    }

    private static void assertReadOnly(MemorySegment segment) {
        assertTrue(segment.isReadOnly());
        if (segment.byteSize() > 0) {
            assertThrows(IllegalArgumentException.class, () -> segment.set(ValueLayout.JAVA_BYTE, 0, (byte) 1));
        }
    }

    /// Asserts that a cursor over `from` to `to` passes the entries of a [PerEntryCursor] over the same range, and that
    /// these are the ones `model` holds there, in both directions.
    private static void assertScan(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to) {
        var low = from == null ? null : segment(from);
        var high = to == null ? null : segment(to);
        var range = from == null && to == null ? model
                : from == null ? model.headMap(to, false)
                : to == null ? model.tailMap(from, true)
                : model.subMap(from, true, to, false);
        for (var descending : new boolean[]{false, true}) {
            var cursor = new TreeCursor(pages, root, low, high, descending);
            var oracle = new PerEntryCursor(pages, root, low, high, descending);
            var ordered = (descending ? range.descendingMap() : range).entrySet().iterator();
            while (oracle.next()) {
                java.util.function.Supplier<String> bounds = () -> "from " + Arrays.toString(from) + " to " + Arrays.toString(to);
                assertTrue(cursor.next(), bounds);
                var key = bytes(oracle.key());
                assertArrayEquals(key, bytes(cursor.key()), bounds);
                assertEquals(0, cursor.compareKey(segment(key)), bounds);
                assertTrue(cursor.compareKey(segment(Arrays.copyOf(key, key.length + 1))) < 0, bounds);
                if (key.length > 0) {
                    assertTrue(cursor.compareKey(segment(Arrays.copyOf(key, key.length - 1))) > 0, bounds);
                }
                var value = bytes(oracle.value());
                assertArrayEquals(value, bytes(cursor.value()), bounds);
                var expected = ordered.next();
                assertArrayEquals(expected.getKey(), key, bounds);
                assertArrayEquals(expected.getValue(), value, bounds);
            }
            assertFalse(cursor.next(), "the cursor went past the range");
            assertFalse(cursor.next(), "an exhausted cursor moved");
            assertFalse(ordered.hasNext(), "the per-entry cursor ended early");
        }
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /// The walk of a tree with no state per leaf: every entry is compared with the bound, and every key and value is a
    /// view of its own.
    private static final class PerEntryCursor {
        private final PageFile pages;
        private final MemorySegment from;
        private final MemorySegment to;
        private final boolean descending;
        private final List<byte[]> path = new ArrayList<>();
        private final List<Integer> indexes = new ArrayList<>();
        private byte[] leaf;
        private int index;
        private int entry;
        private boolean started;
        private boolean exhausted;

        PerEntryCursor(PageFile pages, TreeRoot root, MemorySegment from, MemorySegment to, boolean descending) {
            this.pages = pages;
            this.from = from;
            this.to = to;
            this.descending = descending;
            if (root == null) {
                exhausted = true;
            } else {
                seek(pages.read(root.ref(), PageFile.NODE, true));
            }
        }

        private void seek(byte[] unit) {
            while (!TreeNodes.isLeaf(unit)) {
                int child;
                if (descending) {
                    child = to == null ? TreeNodes.count(unit) - 1 : TreeNodes.lastChildBefore(unit, to);
                    if (child < 0) {
                        exhausted = true;
                        return;
                    }
                } else {
                    child = from == null ? 0 : TreeNodes.childFor(unit, from);
                }
                path.add(unit);
                indexes.add(child);
                unit = pages.read(TreeNodes.childRef(unit, child), PageFile.NODE, true);
            }
            leaf = unit;
            if (descending) {
                index = (to == null ? TreeNodes.count(unit) : TreeNodes.lowerBound(unit, to)) - 1;
            } else {
                index = from == null ? 0 : TreeNodes.lowerBound(unit, from);
            }
        }

        boolean next() {
            if (exhausted) {
                return false;
            }
            if (started) {
                index += descending ? -1 : 1;
            } else {
                started = true;
            }
            while (index < 0 || index >= TreeNodes.count(leaf)) {
                if (!nextLeaf()) {
                    exhausted = true;
                    return false;
                }
            }
            entry = TreeNodes.entry(leaf, index);
            var offset = TreeNodes.leafKeyOffset(entry);
            var length = TreeNodes.leafKeyLength(leaf, entry);
            if (descending ? from != null && TreeNodes.compare(leaf, offset, length, from) < 0
                    : to != null && TreeNodes.compare(leaf, offset, length, to) >= 0) {
                exhausted = true;
                return false;
            }
            return true;
        }

        private boolean nextLeaf() {
            while (!path.isEmpty()) {
                var unit = path.getLast();
                var child = indexes.getLast() + (descending ? -1 : 1);
                if (child >= 0 && child < TreeNodes.count(unit)) {
                    indexes.set(indexes.size() - 1, child);
                    var node = pages.read(TreeNodes.childRef(unit, child), PageFile.NODE, true);
                    while (!TreeNodes.isLeaf(node)) {
                        var edge = descending ? TreeNodes.count(node) - 1 : 0;
                        path.add(node);
                        indexes.add(edge);
                        node = pages.read(TreeNodes.childRef(node, edge), PageFile.NODE, true);
                    }
                    leaf = node;
                    index = descending ? TreeNodes.count(node) - 1 : 0;
                    return true;
                }
                path.removeLast();
                indexes.removeLast();
            }
            return false;
        }

        MemorySegment key() {
            return TreeNodes.leafKey(leaf, entry);
        }

        MemorySegment value() {
            return TreeSearch.value(pages, leaf, entry);
        }
    }
}
