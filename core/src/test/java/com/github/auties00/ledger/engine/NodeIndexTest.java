package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The search index of leaves and branches: its searches against a sorted list and against the search of the same
/// node without an index, on keys built to share prefixes, tie on their heads and end inside them; the room it takes;
/// and trees of nodes without one, as an older version wrote them, that checkpoints rewrite in part.
class NodeIndexTest {
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final byte[] HEX = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    @Nested
    @DisplayName("a search")
    class Searches {
        @ParameterizedTest(name = "keys of kind {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6})
        @DisplayName("of a leaf finds the entry the sorted keys put first at or after a key, with and without an index")
        void leaves(int kind) {
            var random = new Random(31L * kind + 1);
            try (var arena = Arena.ofConfined()) {
                for (var round = 0; round < 300; round++) {
                    var keys = keys(random, kind, count(random, round));
                    var entries = new ArrayList<TreeNodes.LeafEntry>();
                    for (var key : keys) {
                        var value = new byte[random.nextInt(4)];
                        random.nextBytes(value);
                        entries.add(new TreeNodes.LeafEntry(segment(key), segment(value), null, value.length));
                    }
                    var indexed = leaf(entries, true);
                    var plain = leaf(entries, false);
                    assertEquals(PageFormat.INDEXED, indexed[PageFormat.FORMAT]);
                    assertEquals(PageFormat.VERSION, plain[PageFormat.FORMAT]);
                    assertEquals(PageFormat.VERSION, stripped(indexed)[PageFormat.FORMAT]);
                    for (var probe : probes(random, kind, keys)) {
                        var expected = lowerBound(keys, probe);
                        for (var unit : List.of(indexed, plain, stripped(indexed))) {
                            assertEquals(expected, TreeNodes.lowerBound(unit, probe), () -> describe(keys, probe));
                            assertEquals(expected, TreeNodes.lowerBound(unit, segment(probe)), () -> describe(keys, probe));
                            assertEquals(expected, TreeNodes.lowerBound(unit, nativeSegment(arena, probe)), () -> describe(keys, probe));
                        }
                        var found = TreeSearch.entryOf(indexed, probe);
                        var at = Arrays.binarySearch(keys.toArray(byte[][]::new), probe, UNSIGNED);
                        if (at < 0) {
                            assertEquals(-1, found, () -> describe(keys, probe));
                        } else {
                            assertEquals(TreeNodes.entry(indexed, at), found, () -> describe(keys, probe));
                        }
                    }
                }
            }
        }

        @ParameterizedTest(name = "keys of kind {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6})
        @DisplayName("of a branch finds the child that holds a key and the last child before it, with and without an index")
        void branches(int kind) {
            var random = new Random(37L * kind + 5);
            for (var round = 0; round < 300; round++) {
                var keys = keys(random, kind, Math.max(1, count(random, round)));
                var children = new ArrayList<TreeNodes.Child>();
                for (var i = 0; i < keys.size(); i++) {
                    var ref = random.nextInt(4) == 0 ? new PageRef(10 + i, 1, random.nextLong(), random.nextInt(), random.nextInt(50))
                            : new PageRef(10 + i, 1, random.nextLong(), random.nextInt());
                    children.add(new TreeNodes.Child(segment(keys.get(i)), ref, 1 + random.nextInt(100), random.nextBoolean(), 0, -1));
                }
                var indexed = branch(children);
                var plain = stripped(indexed);
                assertEquals(PageFormat.INDEXED, indexed[PageFormat.FORMAT]);
                for (var i = 0; i < children.size(); i++) {
                    assertEquals(children.get(i).ref(), TreeNodes.childRef(indexed, i));
                    assertEquals(children.get(i).ref(), TreeNodes.childRef(plain, i));
                    assertEquals(children.get(i).count(), TreeNodes.childCount(indexed, i));
                    assertEquals(0, Segments.compare(children.get(i).firstKey(), TreeNodes.childKey(indexed, i)));
                }
                for (var probe : probes(random, kind, keys)) {
                    var after = lowerBound(keys, probe);
                    var through = upperBound(keys, probe);
                    var childFor = Math.max(0, through - 1);
                    for (var unit : List.of(indexed, plain)) {
                        assertEquals(childFor, TreeNodes.childFor(unit, probe), () -> describe(keys, probe));
                        assertEquals(childFor, TreeNodes.childFor(unit, segment(probe)), () -> describe(keys, probe));
                        assertEquals(after - 1, TreeNodes.lastChildBefore(unit, probe), () -> describe(keys, probe));
                    }
                }
            }
        }

        @ParameterizedTest(name = "chats: {0}")
        @ValueSource(ints = {1, 2})
        @DisplayName("of a leaf or a branch by a key in a segment, on the heap or not, allocates nothing, whatever the length of the prefix")
        void segmentKeysAllocateNothing(int chats) throws ReflectiveOperationException {
            var random = new Random(41L + chats);
            var keys = new TreeSet<byte[]>(UNSIGNED);
            while (keys.size() < 60) {
                // One chat's ids sharing their first hex digit leave a prefix of 9 bytes, two chats one of 3: neither
                // a whole number of words, so the prefix comparison reaches the bytes after the last word
                var key = idKey(0x200 + random.nextInt(chats), random.nextLong());
                key[8] = 'A';
                keys.add(key);
            }
            var sorted = new ArrayList<>(keys);
            var leaf = leaf(entries(sorted, 8), true);
            var children = new ArrayList<TreeNodes.Child>();
            for (var i = 0; i < sorted.size(); i++) {
                children.add(new TreeNodes.Child(segment(sorted.get(i)), new PageRef(10 + i, 1, i, i), 1, false, 0, -1));
            }
            var branch = branch(children);
            assertEquals(chats == 1 ? 9 : 3, leaf[PageFormat.INDEX] & 0x0F);
            assertEquals(chats == 1 ? 9 : 3, branch[PageFormat.INDEX] & 0x0F);
            try (var arena = Arena.ofConfined()) {
                var probes = new ArrayList<MemorySegment>();
                for (var i = 0; i < sorted.size(); i += 3) {
                    probes.add(segment(sorted.get(i)));
                    probes.add(nativeSegment(arena, sorted.get(i)));
                }
                // A key below the prefix and one that ends inside it
                probes.add(segment(new byte[]{0, 0, 1}));
                probes.add(nativeSegment(arena, Arrays.copyOf(sorted.getFirst(), 6)));
                var calls = 2_000;
                var fewest = Long.MAX_VALUE;
                var found = 0L;
                for (var round = 0; round < 40; round++) {
                    var before = allocatedBytes();
                    for (var call = 0; call < calls; call++) {
                        for (var probe : probes) {
                            found += TreeNodes.lowerBound(leaf, probe) + TreeNodes.childFor(branch, probe);
                        }
                    }
                    fewest = Math.min(fewest, allocatedBytes() - before);
                }
                assertTrue(found > 0);
                // The reads of the allocated bytes allocate a few bytes of their own, under one byte per search
                assertEquals(0, fewest / (calls * probes.size()), "bytes allocated by a search");
            }
        }

        @Test
        @DisplayName("of an empty leaf finds nothing, and of a leaf of one entry the entry or nothing")
        void smallNodes() {
            var empty = leaf(List.of(), true);
            assertEquals(0, TreeNodes.lowerBound(empty, new byte[0]));
            assertEquals(0, TreeNodes.lowerBound(empty, new byte[]{1, 2, 3}));
            var single = leaf(List.of(new TreeNodes.LeafEntry(segment(new byte[]{5, 6}), segment(new byte[0]), null, 0)), true);
            assertEquals(0, TreeNodes.lowerBound(single, new byte[]{5}));
            assertEquals(0, TreeNodes.lowerBound(single, new byte[]{5, 6}));
            assertEquals(1, TreeNodes.lowerBound(single, new byte[]{5, 6, 0}));
            assertEquals(1, TreeNodes.lowerBound(single, new byte[]{6}));
            assertEquals(0, TreeNodes.lowerBound(single, new byte[0]));
        }
    }

    @Nested
    @DisplayName("the index")
    class Layout {
        @Test
        @DisplayName("takes 8-byte heads in a leaf holding the end of one chat and the start of the next, 4-byte ones in a chat")
        void wideHeads() {
            var straddling = new ArrayList<byte[]>();
            for (var chat : new int[]{0x105, 0x106}) {
                for (var i = 0; i < 30; i++) {
                    straddling.add(idKey(chat, 1_000L * chat + i));
                }
            }
            straddling.sort(UNSIGNED);
            var wide = leaf(entries(straddling, 8), true);
            assertTrue((wide[PageFormat.INDEX] & 0x80) != 0, "the leaf took 4-byte heads");
            assertEquals(3, wide[PageFormat.INDEX] & 0x0F, "the prefix is the chats' shared bytes");
            var narrow = leaf(entries(straddling.subList(0, 30), 8), true);
            assertEquals(0, narrow[PageFormat.INDEX] & 0x80, "a leaf of one chat took 8-byte heads");
            assertTrue((narrow[PageFormat.INDEX] & 0x0F) >= 8, "the prefix leaves out the chat and the id's first bytes");
            for (var unit : List.of(wide, narrow)) {
                var count = TreeNodes.count(unit);
                for (var i = 0; i < count; i++) {
                    var key = TreeNodes.leafKey(unit, TreeNodes.entry(unit, i)).toArray(ValueLayout.JAVA_BYTE);
                    assertEquals(i, TreeNodes.lowerBound(unit, key));
                }
            }
        }

        @Test
        @DisplayName("takes 4-byte heads when 8-byte ones would not fit, and every planned node fits its page")
        void plannedNodesFit() {
            var random = new Random(3);
            // Keys sharing more bytes than a prefix holds, whose heads tie in long runs, filled to capacity
            var keys = new TreeSet<byte[]>(UNSIGNED);
            while (keys.size() < 4_000) {
                var key = new byte[20 + random.nextInt(3)];
                Arrays.fill(key, 0, 19, (byte) 7);
                key[19] = (byte) random.nextInt(3);
                for (var i = 20; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(256);
                }
                keys.add(key);
            }
            var entries = new ArrayList<TreeNodes.LeafEntry>();
            for (var key : keys) {
                var value = new byte[random.nextInt(40)];
                entries.add(new TreeNodes.LeafEntry(segment(key), segment(value), null, value.length));
            }
            var sizes = entries.stream().mapToInt(TreeNodes.LeafEntry::size).toArray();
            for (var run : TreeNodes.split(sizes, sizes.length, TreeNodes.TARGET_FULL)) {
                var unit = leaf(entries.subList(run[0], run[1]), true);
                var content = unit.length - PageFormat.HEADER;
                assertTrue(PageFormat.fits(1, content), "a node of " + content + " bytes passes its page");
                assertEquals(TreeNodes.fill(sizes, run[0], run[1]), TreeNodes.fill(unit));
                assertEquals(TreeNodes.fill(unit), TreeNodes.fill(stripped(unit)));
                var index = unit[PageFormat.INDEX];
                var heads = (index & 0x80) != 0 ? 8 : 4;
                assertEquals(2 + (index & 0x0F) + TreeNodes.fill(unit) + (heads - 4) * TreeNodes.count(unit), content);
                assertEquals(NodeIndex.MAX_PREFIX, index & 0x0F);
            }
        }

        @Test
        @DisplayName("keeps its format and index byte through the header a checkpoint writes, which every other unit loses")
        void header() {
            var entries = entries(List.of(new byte[]{1, 2, 3, 4, 5, 6}, new byte[]{1, 2, 3, 9, 9}), 10);
            var unit = TreeNodes.writeLeaf(entries, 0, entries.size());
            var index = unit[PageFormat.INDEX];
            PageFile.header(unit, PageFormat.LEAF, 0, 9, 1, unit.length - PageFormat.HEADER, 4, 5);
            assertEquals(PageFormat.INDEXED, unit[PageFormat.FORMAT]);
            assertEquals(index, unit[PageFormat.INDEX]);
            TreeNodes.deltaHeader(unit, new PageRef(3, 1, 4, 5), unit.length - PageFormat.HEADER);
            assertEquals(PageFormat.INDEXED, unit[PageFormat.FORMAT]);
            assertEquals(index, unit[PageFormat.INDEX]);
            assertTrue(TreeNodes.madeFromDelta(unit));
            assertEquals(1, TreeNodes.lowerBound(unit, new byte[]{1, 2, 3, 5}));
            var value = unit.clone();
            PageFile.header(value, PageFormat.VALUE, 0, 9, 1, 10, 4, 5);
            assertEquals(PageFormat.VERSION, value[PageFormat.FORMAT]);
            assertEquals(0, value[PageFormat.INDEX]);
        }

        @Test
        @DisplayName("fails a node whose index runs past its content or sets reserved bits as corruption")
        void corrupt() {
            var keys = keys(new Random(1), 1, 40);
            var unit = leaf(entries(keys, 8), true);
            var probe = keys.get(7);
            var reserved = unit.clone();
            reserved[PageFormat.INDEX] |= 0x10;
            assertThrows(LedgerCorruptedException.class, () -> TreeNodes.lowerBound(reserved, probe));
            var past = unit.clone();
            Bytes.i32(past, PageFormat.LENGTH, 2 + 2 * TreeNodes.count(past));
            assertThrows(LedgerCorruptedException.class, () -> TreeNodes.lowerBound(past, probe));
            assertThrows(LedgerCorruptedException.class, () -> TreeNodes.lowerBound(past, segment(probe)));
            var wide = unit.clone();
            wide[PageFormat.INDEX] = (byte) (0x80 | NodeIndex.MAX_PREFIX);
            Bytes.i32(wide, PageFormat.LENGTH, 2 + 2 * TreeNodes.count(wide) + NodeIndex.MAX_PREFIX + 7 * TreeNodes.count(wide));
            assertThrows(LedgerCorruptedException.class, () -> TreeNodes.lowerBound(wide, probe));
        }
    }

    @Nested
    @DisplayName("a delta record")
    class Records {
        @Test
        @DisplayName("counts the bytes it writes, without the offsets and heads of a leaf")
        void recordSize() {
            var random = new Random(5);
            var changes = new ArrayList<TreeNodes.LeafEntry>();
            for (var key : keys(random, 1, 30)) {
                changes.add(switch (random.nextInt(3)) {
                    case 0 -> TreeNodes.LeafEntry.removed(segment(key));
                    case 1 -> new TreeNodes.LeafEntry(segment(key), null, new PageRef(5, 2, 7, 8), 9_000, random.nextBoolean());
                    default -> new TreeNodes.LeafEntry(segment(key), segment(new byte[256]), null, 256);
                });
            }
            var buffer = new byte[1 << 16];
            var position = 0;
            for (var change : changes) {
                position = TreeNodes.writeEntry(buffer, position, change);
            }
            assertEquals(16 + 2 + position, DeltaLeaves.recordSize(changes));
        }

        @Test
        @DisplayName("an older version wrote makes a leaf without an index when one with would pass its page")
        void olderRecordWithoutIndex() {
            var keys = new ArrayList<byte[]>();
            for (var i = 0; i < 64; i++) {
                keys.add(idKey(9, i));
            }
            keys.sort(UNSIGNED);
            var entries = entries(keys, 32);
            var base = TreeNodes.writeLeaf(entries, 0, entries.size(), false);
            PageFile.header(base, PageFormat.LEAF, 0, 8, 1, base.length - PageFormat.HEADER, 5, 6);
            assertTrue(entries.stream().mapToInt(TreeNodes.LeafEntry::size).sum() > TreeNodes.CAPACITY);
            assertTrue(PageFormat.fits(1, base.length - PageFormat.HEADER));
            var changes = List.of(new TreeNodes.LeafEntry(segment(keys.get(5)), segment(new byte[32]), null, 32));
            var page = recordPage(new PageRef(8, 1, 5, 6), changes);
            var leaf = DeltaLeaves.materialize(new PageRef(20, 1, 1, 2, 0), (ref, kind) -> kind == PageFormat.DELTA ? page : base);
            assertEquals(PageFormat.VERSION, leaf[PageFormat.FORMAT]);
            assertEquals(64, TreeNodes.count(leaf));
            for (var i = 0; i < keys.size(); i++) {
                assertEquals(i, TreeNodes.lowerBound(leaf, keys.get(i)));
            }
            assertArrayEquals(new byte[32], TreeNodes.inlineValueBytes(leaf, TreeNodes.entry(leaf, 5)));
            // A record that leaves the leaf room for an index makes one with
            var removals = new ArrayList<TreeNodes.LeafEntry>();
            for (var i = 0; i < 10; i++) {
                removals.add(TreeNodes.LeafEntry.removed(segment(keys.get(i))));
            }
            var smaller = recordPage(new PageRef(8, 1, 5, 6), removals);
            var indexed = DeltaLeaves.materialize(new PageRef(20, 1, 1, 2, 0), (ref, kind) -> kind == PageFormat.DELTA ? smaller : base);
            assertEquals(PageFormat.INDEXED, indexed[PageFormat.FORMAT]);
            assertEquals(54, TreeNodes.count(indexed));
            assertEquals(0, TreeNodes.lowerBound(indexed, keys.get(10)));
        }
    }

    @Nested
    @DisplayName("a tree an older version wrote")
    class OlderTrees {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("reads as it was and stays equal to the model through checkpoints that rewrite it in part")
        void rounds(boolean encrypted, @TempDir Path directory) {
            assertTimeoutPreemptively(Duration.ofMinutes(3), () -> {
                var random = new Random(encrypted ? 41 : 43);
                try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
                    var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                    var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                    var keys = new ArrayList<byte[]>();
                    for (var chat = 1; chat <= 20; chat++) {
                        for (var i = 0; i < 300; i++) {
                            var key = idKey(chat, i);
                            var value = new byte[32];
                            random.nextBytes(value);
                            model.put(key, value);
                            keys.add(key);
                        }
                    }
                    var older = OlderTree.write(pages, space, model, random, 4);
                    pages.cache().clearForTests();
                    var root = older.root();
                    var deltas = older.deltas();
                    assertEquals(PageFormat.VERSION, pages.read(root.ref(), PageFile.NODE, true)[PageFormat.FORMAT]);
                    TreeUpdateTest.verify(pages, root, model, random, keys);
                    TreeUpdateTest.verifyPages(pages, space, root, deltas);
                    var records = 0;
                    for (var round = 0; round < 30; round++) {
                        var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                        var writes = random.nextInt(4) == 0 ? 2_000 : random.nextInt(60);
                        for (var i = 0; i < writes; i++) {
                            var key = random.nextInt(5) == 0 ? idKey(1 + random.nextInt(21), 300 + random.nextInt(2_000))
                                    : keys.get(random.nextInt(keys.size()));
                            var value = random.nextInt(6) == 0 ? null : new byte[random.nextInt(3) == 0 ? random.nextInt(200) : 32];
                            if (value != null) {
                                random.nextBytes(value);
                            }
                            states.put(key, value);
                            keys.add(key);
                        }
                        var ranges = new ArrayList<TreeUpdate.Range>();
                        if (random.nextInt(5) == 0) {
                            var chat = 1 + random.nextInt(21);
                            var from = ByteBuffer.allocate(4).putInt(chat).array();
                            var to = ByteBuffer.allocate(4).putInt(chat + 1).array();
                            ranges.add(new TreeUpdate.Range(segment(from), segment(to)));
                            model.subMap(from, true, to, false).clear();
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
                        if (round % 3 == 0) {
                            pages.cache().clearForTests();
                        }
                        TreeUpdateTest.verify(pages, root, model, random, keys);
                        records = Math.max(records, TreeUpdateTest.verifyPages(pages, space, root, deltas));
                        if (root != null) {
                            verifyFills(pages, root.ref());
                        }
                    }
                    assertTrue(records > 0, "no leaf was ever written as a delta record");
                }
            });
        }

        @Test
        @DisplayName("whose root keeps one leaf an older record made too large for an index splits that leaf in two")
        void rootRecordSplits(@TempDir Path directory) {
            assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
                var random = new Random(9);
                try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(1 << 20))) {
                    var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                    var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                    for (var i = 0; i < 64; i++) {
                        model.put(idKey(1, i), new byte[32]);
                    }
                    for (var i = 0; i < 10; i++) {
                        model.put(idKey(2, i), new byte[32]);
                    }
                    // The first leaf is the 64 keys of chat 1, the second those of chat 2; every leaf takes a record
                    var older = OlderTree.write(pages, space, model, random, 1);
                    pages.cache().clearForTests();
                    var root = older.root();
                    assertEquals(1, TreeNodes.level(pages.read(root.ref(), PageFile.NODE, true)));
                    assertTrue(TreeNodes.childRef(pages.read(root.ref(), PageFile.NODE, true), 0).isRecord());
                    var from = ByteBuffer.allocate(4).putInt(2).array();
                    var to = ByteBuffer.allocate(4).putInt(3).array();
                    model.subMap(from, true, to, false).clear();
                    var writer = pages.writer(random.nextLong());
                    var update = new TreeUpdate(pages, writer, space);
                    root = update.apply(root, List.of(), List.of(new TreeUpdate.Range(segment(from), segment(to))));
                    var deltas = update.finishDeltas(older.deltas());
                    writer.flush();
                    for (var unit : update.freed()) {
                        space.release(unit.page(), unit.pages());
                    }
                    pages.cache().clearForTests();
                    var top = pages.read(root.ref(), PageFile.NODE, true);
                    assertEquals(1, TreeNodes.level(top), "the leaf was not split under a branch");
                    assertEquals(2, TreeNodes.count(top));
                    assertEquals(64, root.count());
                    TreeUpdateTest.verify(pages, root, model, random, new ArrayList<>(model.keySet()));
                    TreeUpdateTest.verifyPages(pages, space, root, deltas);
                    TreeUpdateTest.verifyNodes(pages, root);
                }
            });
        }
    }

    /// A tree written as an older version wrote it: nodes without an index, filled to the capacity that version's
    /// nodes had, and a delta record on every `recordEvery`th leaf, which changes one value of its base.
    ///
    /// @param root the root
    /// @param deltas the table of the delta pages the records fill
    private record OlderTree(TreeRoot root, DeltaTable deltas) {
        /// The capacity of a node without an index: the content of a page but the count of its entries.
        private static final int CAPACITY = PageFormat.capacity(1) - 2;

        static OlderTree write(PageFile pages, FreeSpace space, TreeMap<byte[], byte[]> model, Random random, int recordEvery) throws Exception {
            var writer = pages.writer(random.nextLong());
            var counts = new PageCounts();
            var packer = new DeltaLeaves.Packer(writer, new DeltaLeaves.Packer.Space() {
                @Override
                public long take(int least, int most) {
                    return space.allocateRun(least, most);
                }

                @Override
                public void giveBack(int page, int pages) {
                    space.release(page, pages);
                }
            }, counts::add);
            var entries = new ArrayList<TreeNodes.LeafEntry>();
            for (var entry : model.entrySet()) {
                entries.add(new TreeNodes.LeafEntry(segment(entry.getKey()), segment(entry.getValue()), null, entry.getValue().length));
            }
            var level = new ArrayList<TreeNodes.Child>();
            for (var run : runs(entries, entry -> entry.size() - NodeIndex.HEAD)) {
                var unit = TreeNodes.writeLeaf(entries, run[0], run[1], false);
                var ref = writer.write(PageFormat.LEAF, 0, space.allocate(1), 1, unit, unit.length - PageFormat.HEADER, false);
                if (level.size() % recordEvery == 0) {
                    var at = run[0] + random.nextInt(run[1] - run[0]);
                    var key = entries.get(at).key().toArray(ValueLayout.JAVA_BYTE);
                    var value = new byte[(int) entries.get(at).value().byteSize()];
                    random.nextBytes(value);
                    model.put(key, value);
                    var changes = List.of(new TreeNodes.LeafEntry(segment(key), segment(value), null, value.length));
                    ref = packer.append(ref, changes, DeltaLeaves.recordSize(changes));
                }
                level.add(new TreeNodes.Child(entries.get(run[0]).key(), ref, run[1] - run[0], false, 0, -1));
            }
            packer.close();
            var height = 0;
            while (level.size() > 1 || height == 0) {
                height++;
                var next = new ArrayList<TreeNodes.Child>();
                for (var run : runs(level, child -> child.size() - NodeIndex.HEAD)) {
                    var unit = stripped(TreeNodes.writeBranch(level, run[0], run[1]));
                    var ref = writer.write(PageFormat.BRANCH, height, space.allocate(1), 1, unit, unit.length - PageFormat.HEADER, false);
                    var count = 0L;
                    for (var i = run[0]; i < run[1]; i++) {
                        count += level.get(i).count();
                    }
                    next.add(new TreeNodes.Child(level.get(run[0]).firstKey(), ref, count, false, height, -1));
                }
                level = next;
            }
            writer.flush();
            var top = level.getFirst();
            return new OlderTree(new TreeRoot(top.ref(), top.count(), false), DeltaTable.EMPTY.apply(counts, new ArrayList<>()));
        }

        /// {@return the runs of `items` that fill nodes of the older capacity one after another, by the sizes `size`
        /// gives}
        private static <T> List<int[]> runs(List<T> items, Function<T, Integer> size) {
            var runs = new ArrayList<int[]>();
            var start = 0;
            var fill = 0;
            for (var i = 0; i < items.size(); i++) {
                var bytes = size.apply(items.get(i));
                if (i > start && fill + bytes > CAPACITY) {
                    runs.add(new int[]{start, i});
                    start = i;
                    fill = 0;
                }
                fill += bytes;
            }
            runs.add(new int[]{start, items.size()});
            return runs;
        }
    }

    /// Checks that every node of the subtree under `ref` fits its page, and that one with an index fits it as the
    /// planner counts it.
    private static void verifyFills(PageFile pages, PageRef ref) {
        var unit = pages.read(ref, PageFile.NODE, false);
        assertTrue(PageFormat.fits(1, Bytes.i32(unit, PageFormat.LENGTH)));
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            assertTrue(TreeNodes.fill(unit) <= TreeNodes.CAPACITY, "a node with an index passes the planned capacity");
        }
        if (!TreeNodes.isLeaf(unit)) {
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                verifyFills(pages, TreeNodes.childRef(unit, i));
            }
        }
    }

    /// {@return a delta page holding one record of `changes` to `base`, in slot 0}
    private static byte[] recordPage(PageRef base, List<TreeNodes.LeafEntry> changes) {
        var page = new byte[PageFormat.SIZE];
        var record = PageFormat.SIZE - PageFormat.TAG - DeltaLeaves.recordSize(changes);
        Bytes.u16(page, PageFormat.HEADER, 1);
        Bytes.u16(page, PageFormat.HEADER + 2, record);
        Bytes.i32(page, record, base.page());
        Bytes.i64(page, record + 4, base.nonceHigh());
        Bytes.i32(page, record + 12, base.nonceLow());
        Bytes.u16(page, record + 16, changes.size());
        var position = record + 18;
        for (var change : changes) {
            position = TreeNodes.writeEntry(page, position, change);
        }
        return page;
    }

    /// {@return `unit`, a leaf or a branch with an index, as an older version wrote it: without the index}
    static byte[] stripped(byte[] unit) {
        var count = TreeNodes.count(unit);
        var index = unit[PageFormat.INDEX];
        var removed = (index & 0x0F) + ((index & 0x80) != 0 ? 8 : 4) * count;
        var start = PageFormat.HEADER + 2 + 2 * count;
        var out = new byte[unit.length - removed];
        System.arraycopy(unit, 0, out, 0, start);
        System.arraycopy(unit, start + removed, out, start, unit.length - start - removed);
        for (var i = 0; i < count; i++) {
            Bytes.u16(out, PageFormat.HEADER + 2 + 2 * i, TreeNodes.entry(unit, i) - removed);
        }
        out[PageFormat.FORMAT] = PageFormat.VERSION;
        out[PageFormat.INDEX] = 0;
        Bytes.i32(out, PageFormat.LENGTH, out.length - PageFormat.HEADER);
        return out;
    }

    private static byte[] leaf(List<TreeNodes.LeafEntry> entries, boolean indexed) {
        var unit = TreeNodes.writeLeaf(entries, 0, entries.size(), indexed);
        PageFile.header(unit, PageFormat.LEAF, 0, 9, 1, unit.length - PageFormat.HEADER, 1, 2);
        return unit;
    }

    private static byte[] branch(List<TreeNodes.Child> children) {
        var unit = TreeNodes.writeBranch(children, 0, children.size());
        PageFile.header(unit, PageFormat.BRANCH, 1, 9, 1, unit.length - PageFormat.HEADER, 1, 2);
        return unit;
    }

    private static List<TreeNodes.LeafEntry> entries(List<byte[]> keys, int valueLength) {
        var entries = new ArrayList<TreeNodes.LeafEntry>();
        for (var key : keys) {
            entries.add(new TreeNodes.LeafEntry(segment(key), segment(new byte[valueLength]), null, valueLength));
        }
        return entries;
    }

    /// {@return how many keys a node of round `round` holds: the few-entry cases first, then up to 120}
    private static int count(Random random, int round) {
        return round < 12 ? round : 1 + random.nextInt(120);
    }

    /// {@return `count` distinct keys of `kind`, sorted}
    ///
    /// The kinds: 0 bytes of a small alphabet with zeros, so keys end inside heads and are prefixes of each other; 1
    /// ids of one or two chats; 2 messages of one or two chats; 3 keys sharing more than a prefix holds; 4 keys whose
    /// heads all tie; 5 random bytes; 6 keys of one byte and their extensions by zeros.
    private static List<byte[]> keys(Random random, int kind, int count) {
        var keys = new TreeSet<byte[]>(UNSIGNED);
        var chat = 100 + random.nextInt(1_000);
        var common = new byte[3 + random.nextInt(20)];
        random.nextBytes(common);
        var attempts = 0;
        while (keys.size() < count && attempts++ < 100_000) {
            keys.add(key(random, kind, chat, common));
        }
        return new ArrayList<>(keys);
    }

    private static byte[] key(Random random, int kind, int chat, byte[] common) {
        return switch (kind) {
            case 0 -> {
                var key = new byte[random.nextInt(10)];
                var alphabet = new byte[]{0, 1, 2, (byte) 0xFF};
                for (var i = 0; i < key.length; i++) {
                    key[i] = alphabet[random.nextInt(alphabet.length)];
                }
                yield key;
            }
            case 1 -> idKey(chat + random.nextInt(2), random.nextLong());
            case 2 -> ByteBuffer.allocate(32)
                    .putInt(chat + random.nextInt(2))
                    .putLong(1_700_000_000_000L + random.nextInt(1 << 20))
                    .put(idOf(random.nextLong()))
                    .array();
            case 3 -> {
                var key = Arrays.copyOf(common, common.length + random.nextInt(6));
                for (var i = common.length; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(4);
                }
                yield key;
            }
            case 4 -> {
                var key = Arrays.copyOf(common, common.length + 10 + random.nextInt(3));
                key[common.length / 2] = (byte) random.nextInt(2);
                for (var i = common.length + 9; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(256);
                }
                yield key;
            }
            case 5 -> {
                var key = new byte[random.nextInt(24)];
                random.nextBytes(key);
                yield key;
            }
            default -> {
                var key = new byte[1 + random.nextInt(12)];
                key[0] = (byte) (1 + random.nextInt(2));
                if (random.nextBoolean() && key.length > 1) {
                    key[key.length - 1] = (byte) random.nextInt(3);
                }
                yield key;
            }
        };
    }

    /// {@return keys to search for: every key, and keys just around, inside and outside each, and new keys}
    private static List<byte[]> probes(Random random, int kind, List<byte[]> keys) {
        var probes = new ArrayList<byte[]>();
        probes.add(new byte[0]);
        probes.add(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        for (var key : keys) {
            probes.add(key);
            probes.add(Arrays.copyOf(key, key.length + 1));
            var past = Arrays.copyOf(key, key.length + 1);
            past[key.length] = (byte) 0xFF;
            probes.add(past);
            for (var length = 0; length < key.length; length += 1 + random.nextInt(3)) {
                probes.add(Arrays.copyOf(key, length));
            }
            if (key.length > 0) {
                var at = random.nextInt(key.length);
                var lower = key.clone();
                lower[at]--;
                probes.add(lower);
                var higher = key.clone();
                higher[at]++;
                probes.add(higher);
                var zeros = Arrays.copyOf(key, key.length + 1 + random.nextInt(9));
                probes.add(zeros);
            }
        }
        var chat = 100 + random.nextInt(1_000);
        var common = keys.isEmpty() ? new byte[4] : Arrays.copyOf(keys.getFirst(), Math.min(keys.getFirst().length, 20));
        for (var i = 0; i < 40; i++) {
            probes.add(key(random, kind, chat, common));
        }
        return probes;
    }

    private static byte[] idKey(int chat, long seed) {
        return ByteBuffer.allocate(24).putInt(chat).put(idOf(seed)).array();
    }

    private static byte[] idOf(long seed) {
        var bits = seed * 0x9E3779B97F4A7C15L;
        bits ^= bits >>> 29;
        bits *= 0xBF58476D1CE4E5B9L;
        bits ^= bits >>> 32;
        var id = new byte[20];
        id[0] = '3';
        id[1] = 'E';
        id[2] = 'B';
        id[3] = '0';
        for (var i = 0; i < 16; i++) {
            id[4 + i] = HEX[(int) (bits >>> (60 - 4 * i)) & 0xF];
        }
        return id;
    }

    private static int lowerBound(List<byte[]> keys, byte[] probe) {
        var index = 0;
        while (index < keys.size() && UNSIGNED.compare(keys.get(index), probe) < 0) {
            index++;
        }
        return index;
    }

    private static int upperBound(List<byte[]> keys, byte[] probe) {
        var index = 0;
        while (index < keys.size() && UNSIGNED.compare(keys.get(index), probe) <= 0) {
            index++;
        }
        return index;
    }

    private static String describe(List<byte[]> keys, byte[] probe) {
        var text = new StringBuilder("probe ").append(Arrays.toString(probe)).append(" in");
        for (var key : keys) {
            text.append(' ').append(Arrays.toString(key));
        }
        return text.toString();
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    // The bytes the calling thread allocated so far, by reflection: this module reads no management module
    private static long allocatedBytes() throws ReflectiveOperationException {
        var threads = Class.forName("java.lang.management.ManagementFactory").getMethod("getThreadMXBean").invoke(null);
        return (long) Class.forName("com.sun.management.ThreadMXBean").getMethod("getCurrentThreadAllocatedBytes").invoke(threads);
    }

    private static MemorySegment nativeSegment(Arena arena, byte[] bytes) {
        var segment = arena.allocate(Math.max(1, bytes.length)).asSlice(0, bytes.length);
        MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, bytes.length);
        return segment;
    }

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
    }

    @Test
    @DisplayName("the value the touched bytes are tested against is one no byte takes")
    void untouched() throws Exception {
        var field = NodeIndex.class.getDeclaredField("untouched");
        field.setAccessible(true);
        assertEquals(Integer.MIN_VALUE, field.getInt(null));
        assertFalse(field.getInt(null) >= Byte.MIN_VALUE && field.getInt(null) <= Byte.MAX_VALUE);
    }
}
