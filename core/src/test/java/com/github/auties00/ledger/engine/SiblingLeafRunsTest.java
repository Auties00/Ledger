package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Sibling leaves a checkpoint writes whole, split together: the fill each run reaches, which leaves join a run, and
/// that the tree stays sound for its readers, for the free space and for recovery. A harness applies one update at a
/// time to a tree and a sorted map, and checks the contents, every branch's record of its children and every page.
class SiblingLeafRunsTest {
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

    @Nested
    @DisplayName("splitting items into nodes")
    class Split {
        /// The rule of a node split on its own, kept here as the baseline a single leaf must reproduce.
        private static List<int[]> baseline(int[] sizes, int count) {
            var total = 0L;
            for (var i = 0; i < count; i++) {
                total += sizes[i];
            }
            var remainingPages = Math.max(1, (int) ((total + TreeNodes.CAPACITY - 1) / TreeNodes.CAPACITY));
            var remaining = total;
            var runs = new ArrayList<int[]>();
            var index = 0;
            while (index < count) {
                var goal = (remaining + remainingPages - 1) / remainingPages;
                var start = index;
                var fill = 0L;
                while (index < count && fill + sizes[index] <= TreeNodes.CAPACITY && (fill < goal || index == start)) {
                    fill += sizes[index++];
                }
                runs.add(new int[]{start, index});
                remaining -= fill;
                remainingPages = Math.max(1, remainingPages - 1);
            }
            return runs;
        }

        private static int[] randomSizes(Random random) {
            var sizes = new int[random.nextInt(3_000)];
            var most = 1 + switch (random.nextInt(3)) {
                case 0 -> 80;
                case 1 -> 600;
                default -> TreeNodes.MAX_ENTRY - 1;
            };
            for (var i = 0; i < sizes.length; i++) {
                sizes[i] = 8 + random.nextInt(most);
            }
            return sizes;
        }

        private static void assertCovers(List<int[]> runs, int[] sizes) {
            var next = 0;
            for (var run : runs) {
                assertEquals(next, run[0], "the runs skip or repeat an item");
                assertTrue(run[1] > run[0], "a run is empty");
                var fill = 0L;
                for (var i = run[0]; i < run[1]; i++) {
                    fill += sizes[i];
                }
                assertTrue(fill <= TreeNodes.CAPACITY, "a node passes its capacity");
                next = run[1];
            }
            assertEquals(sizes.length, next, "the runs miss items");
        }

        @Test
        @DisplayName("a node split on its own splits as it always did, byte for byte")
        void alone() {
            var random = new Random(1);
            for (var round = 0; round < 2_000; round++) {
                var sizes = randomSizes(random);
                assertArrayEquals(baseline(sizes, sizes.length).toArray(int[][]::new), TreeNodes.split(sizes, sizes.length).toArray(int[][]::new));
            }
        }

        @Test
        @DisplayName("at a full target the split matches a node's own wherever filling each node in turn takes no more nodes")
        void fullTarget() {
            var random = new Random(2);
            var same = 0;
            for (var round = 0; round < 2_000; round++) {
                var sizes = randomSizes(random);
                var total = Arrays.stream(sizes).asLongStream().sum();
                if (TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_FULL) == Math.max(sizes.length == 0 ? 0 : 1, (total + TreeNodes.CAPACITY - 1) / TreeNodes.CAPACITY)) {
                    same++;
                    assertArrayEquals(baseline(sizes, sizes.length).toArray(int[][]::new), TreeNodes.split(sizes, sizes.length, TreeNodes.TARGET_FULL).toArray(int[][]::new));
                }
            }
            assertTrue(same > 500, "the comparison ran " + same + " times");
        }

        @ParameterizedTest(name = "target {0}")
        @ValueSource(ints = {TreeNodes.TARGET_ROOMY, TreeNodes.TARGET_NEAR_FULL, TreeNodes.TARGET_FULL})
        @DisplayName("any target covers every item in order, never passes a node's capacity and fills to the target or under")
        void targets(int target) {
            var random = new Random(target);
            for (var round = 0; round < 2_000; round++) {
                var sizes = randomSizes(random);
                var runs = TreeNodes.split(sizes, sizes.length, target);
                assertCovers(runs, sizes);
                var total = Arrays.stream(sizes).asLongStream().sum();
                var nodes = TreeNodes.nodesFor(sizes, sizes.length, target);
                assertTrue(16 * total <= (long) target * TreeNodes.CAPACITY * nodes, "the count leaves no room for the target");
                // Entries too large for the room left stop some nodes short of their share, so a few more may follow
                assertTrue(runs.size() <= nodes + nodes / 16 + 1, runs.size() + " nodes where " + nodes + " were asked for");
            }
        }

        @Test
        @DisplayName("items that stop short of the capacity spread over the nodes they take, with no small node at the end")
        void noTail() {
            var sizes = new int[29];
            // 14 of these fill a node as far as they go, and 29 still add up to under two nodes
            var size = 2 * TreeNodes.CAPACITY / 29;
            assertTrue(size * 15 > TreeNodes.CAPACITY && size * 29L <= 2L * TreeNodes.CAPACITY);
            Arrays.fill(sizes, size);
            assertArrayEquals(new int[][]{{0, 14}, {14, 28}, {28, 29}}, TreeNodes.split(sizes, 29).toArray(int[][]::new));
            assertArrayEquals(new int[][]{{0, 10}, {10, 20}, {20, 29}}, TreeNodes.split(sizes, 29, TreeNodes.TARGET_FULL).toArray(int[][]::new));
        }

        @Test
        @DisplayName("the count of nodes is the larger of the target's and of filling each node in turn, on a total past an int too")
        void counts() {
            var sizes = new int[4_000_000];
            Arrays.fill(sizes, 1_000);
            var total = 4_000_000_000L;
            // Four of these fit in a node, which leaves it short of the capacity
            assertEquals(1_000_000, TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_FULL));
            var divisor = 7L * TreeNodes.CAPACITY;
            assertEquals((8 * total + divisor - 1) / divisor, TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_ROOMY));
            assertEquals(0, TreeNodes.nodesFor(new int[0], 0, TreeNodes.TARGET_ROOMY));
            assertTrue(TreeNodes.split(new int[0], 0, TreeNodes.TARGET_ROOMY).isEmpty());
        }
    }

    /// A tree and the sorted map it must equal.
    private static final class Harness implements AutoCloseable {
        final PageFile pages;
        final FreeSpace space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
        final TreeMap<byte[], byte[]> model = new TreeMap<>(UNSIGNED);
        final Random random;
        TreeRoot root;
        DeltaTable deltas = DeltaTable.EMPTY;
        long nonce;
        int deepestRecords;

        Harness(Path directory, boolean encrypted, long seed) throws IOException {
            this.pages = PageFile.open(directory, new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null), 8 << 20, new HeapPool(1 << 20));
            this.random = new Random(seed);
        }

        /// Applies `states`, a state per key (`null` removes it), and `ranges`, and checks the tree.
        void apply(TreeMap<byte[], byte[]> states, List<byte[][]> ranges) throws IOException {
            var removals = new ArrayList<TreeUpdate.Range>();
            for (var range : ranges) {
                removals.add(new TreeUpdate.Range(segment(range[0]), segment(range[1])));
                model.subMap(range[0], true, range[1], false).clear();
            }
            var ops = new ArrayList<TreeUpdate.Op>(states.size());
            states.forEach((key, value) -> {
                ops.add(new TreeUpdate.Op(segment(key), value == null ? null : segment(value)));
                if (value == null) {
                    model.remove(key);
                } else {
                    model.put(key, value);
                }
            });
            var writer = pages.writer(++nonce);
            var update = new TreeUpdate(pages, writer, space);
            root = update.apply(root, ops, TreeUpdate.merge(removals));
            deltas = update.finishDeltas(deltas);
            writer.flush();
            update.freed().forEach(unit -> space.release(unit.page(), unit.pages()));
            verify();
        }

        void verify() {
            assertEquals(model.size(), root == null ? 0 : root.count());
            var cursor = new TreeCursor(pages, root, null, null, false);
            for (var entry : model.entrySet()) {
                assertTrue(cursor.next(), "the cursor ended early");
                assertArrayEquals(entry.getKey(), cursor.key().toArray(ValueLayout.JAVA_BYTE));
                assertArrayEquals(entry.getValue(), cursor.value().toArray(ValueLayout.JAVA_BYTE));
            }
            assertFalse(cursor.next(), "the cursor went past the model");
            TreeUpdateTest.verifyNodes(pages, root);
            var used = new BitSet();
            var records = new TreeMap<Integer, Integer>();
            if (root != null) {
                assertFalse(root.ref().isRecord(), "the root is a delta record");
                CheckpointTest.walk(pages, root.ref(), used, new BitSet(), records);
            }
            CheckpointTest.verifyDeltas(used, records, deltas);
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < space.pageCount(); page++) {
                assertTrue(space.isUnused(page) != used.get(page), "page " + page + " is both or neither of free and in use");
            }
            deepestRecords = Math.max(deepestRecords, records.values().stream().mapToInt(Integer::intValue).sum());
        }

        /// {@return the leaves of the tree, in key order}
        List<Leaf> leaves() {
            var leaves = new ArrayList<Leaf>();
            if (root != null) {
                collect(root.ref(), leaves);
            }
            return leaves;
        }

        private void collect(PageRef ref, List<Leaf> leaves) {
            var unit = pages.read(ref, PageFile.NODE, false);
            if (TreeNodes.isLeaf(unit)) {
                leaves.add(new Leaf(ref, TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0)).toArray(ValueLayout.JAVA_BYTE), TreeNodes.count(unit), TreeNodes.fill(unit)));
                return;
            }
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                collect(TreeNodes.childRef(unit, i), leaves);
            }
        }

        /// {@return the keys of the model that leaf `index` of `leaves` holds}
        List<byte[]> keys(List<Leaf> leaves, int index) {
            var from = leaves.get(index).firstKey();
            var range = index + 1 < leaves.size() ? model.subMap(from, true, leaves.get(index + 1).firstKey(), false) : model.tailMap(from, true);
            return new ArrayList<>(range.keySet());
        }

        double fill() {
            var leaves = leaves();
            return leaves.stream().mapToLong(Leaf::fill).sum() / (double) (leaves.size() * (long) TreeNodes.CAPACITY);
        }

        int level() {
            return TreeNodes.level(pages.read(root.ref(), PageFile.NODE, false));
        }

        byte[] bytes(int length) {
            var bytes = new byte[length];
            random.nextBytes(bytes);
            return bytes;
        }

        /// Loads `count` random keys into an empty tree in one update.
        void load(int count, int keyLength, int valueLength) throws IOException {
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            while (states.size() < count) {
                states.put(bytes(keyLength), bytes(valueLength));
            }
            apply(states, List.of());
        }

        /// Takes most entries out of every other leaf, then of the leaves between: each is written alone, so the leaves
        /// stay as few and as empty as those removals leave them.
        void thin(double removed) throws IOException {
            for (var offset = 0; offset < 2; offset++) {
                var leaves = leaves();
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var i = offset; i < leaves.size(); i += 2) {
                    var keys = keys(leaves, i);
                    for (var key : keys.subList(0, Math.max(1, (int) (keys.size() * removed)))) {
                        states.put(key, null);
                    }
                }
                apply(states, List.of());
            }
        }

        @Override
        public void close() throws IOException {
            pages.close();
        }
    }

    private record Leaf(PageRef ref, byte[] firstKey, int count, int fill) {
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    /// {@return the bytes a leaf takes for an entry of `key` and `value`}
    private static int entrySize(byte[] key, byte[] value) {
        return TreeNodes.SLOT + 2 + key.length + 1 + 2 + value.length;
    }

    private static List<byte[]> firstKeys(List<Leaf> leaves) {
        return leaves.stream().map(Leaf::firstKey).toList();
    }

    private static void assertSameKeys(List<byte[]> expected, List<byte[]> actual) {
        assertArrayEquals(expected.toArray(byte[][]::new), actual.toArray(byte[][]::new));
    }

    @Nested
    @DisplayName("the fill of a run")
    class Fill {
        @Test
        @DisplayName("overwrites of the same size pack a thin run full, and from there leave its boundaries alone")
        void overwrites(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 1)) {
                tree.load(20_000, 16, 256);
                tree.thin(0.4);
                var thin = tree.leaves().size();
                assertTrue(tree.fill() < 0.7, "the tree is " + tree.fill() + " full");
                for (var round = 0; round < 3; round++) {
                    overwriteAll(tree);
                }
                assertTrue(tree.fill() > 0.9, "overwrites left the tree " + tree.fill() + " full");
                assertTrue(tree.leaves().size() < thin * 3 / 4, tree.leaves().size() + " leaves out of " + thin);
                var settled = firstKeys(tree.leaves());
                overwriteAll(tree);
                assertSameKeys(settled, firstKeys(tree.leaves()));
            }
        }

        private static void overwriteAll(Harness tree) throws IOException {
            var states = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var key : tree.model.keySet()) {
                states.put(key, tree.bytes(256));
            }
            tree.apply(states, List.of());
        }

        @Test
        @DisplayName("changes in place that resize their entries leave room for more of them")
        void resized(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 2)) {
                tree.load(20_000, 16, 256);
                tree.thin(0.4);
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var key : tree.model.keySet()) {
                    states.put(key, tree.bytes(240));
                }
                tree.apply(states, List.of());
                assertTrue(tree.fill() > 0.85 && tree.fill() <= 15d / 16, "the tree is " + tree.fill() + " full");
            }
        }

        @Test
        @DisplayName("new keys scattered over full leaves split them to seven eighths, not in two halves")
        void scattered(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 3)) {
                tree.load(170_000, 24, 32);
                assertTrue(tree.fill() > 0.95);
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                while (states.size() < 30_000) {
                    states.put(tree.bytes(24), tree.bytes(32));
                }
                tree.apply(states, List.of());
                assertEquals(200_000, tree.model.size());
                assertTrue(tree.fill() > 0.84 && tree.fill() < 0.885, "the tree is " + tree.fill() + " full");
            }
        }

        @Test
        @DisplayName("blocks of new keys, as chats append, leave their leaves full")
        void blocks(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 4)) {
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var chat = 0; chat < 400; chat++) {
                    for (var message = 0; message < 13; message++) {
                        states.put(messageKey(chat, message), tree.bytes(256));
                    }
                }
                tree.apply(states, List.of());
                states = new TreeMap<>(UNSIGNED);
                for (var chat = 0; chat < 400; chat++) {
                    for (var message = 13; message < 26; message++) {
                        states.put(messageKey(chat, message), tree.bytes(256));
                    }
                }
                tree.apply(states, List.of());
                // A leaf holds 14 of these entries at most, which is 0.93 of it; seven eighths stops at 0.90
                assertTrue(tree.fill() > 0.93, "the tree is " + tree.fill() + " full");
            }
        }

        private static byte[] messageKey(int chat, int message) {
            return ByteBuffer.allocate(8).putInt(chat).putInt(message).array();
        }

        @ParameterizedTest(name = "blocks of [{0}] keys, past a leaf's last key: {1}, values of {2} bytes: {3} sixteenths")
        @CsvSource({
                "'', false, 64, 16",
                "'', false, 60, 15",
                "1, false, 64, 15",
                "2, false, 64, 15",
                "3, false, 64, 16",
                "1, true, 64, 15",
                "2, true, 64, 15",
                "3, true, 64, 16",
                "1 1, false, 64, 14",
                "2 2, false, 64, 14",
                "3 1, false, 64, 14",
                "4 1, false, 64, 16",
                "3 3, false, 64, 16",
                "1 1, true, 64, 14",
                "2 2, true, 64, 14",
                "3 1, true, 64, 14",
                "4 1, true, 64, 16",
                "3 3, true, 64, 16",
                "2 2 2 2 2 2 2 2, true, 64, 14",
                "3 3 3 3 3 3 3 3, true, 64, 16"
        })
        @DisplayName("the new keys of a run and the gaps they land in, the one past a leaf's last key included, set its fill")
        void gaps(String blocks, boolean past, int valueLength, int target, @TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 12)) {
                tree.load(3_000, 16, 64);
                tree.thin(0.4);
                // Under a root of one level every leaf is a child of the root, and all of them are one run
                assertEquals(1, tree.level());
                var leaves = tree.leaves();
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var key : tree.model.keySet()) {
                    states.put(key, tree.bytes(valueLength));
                }
                var lengths = blocks.isEmpty() ? new int[0] : Arrays.stream(blocks.split(" ")).mapToInt(Integer::parseInt).toArray();
                for (var block = 0; block < lengths.length; block++) {
                    var keys = tree.keys(leaves, (block + 1) * leaves.size() / (lengths.length + 1));
                    var after = past ? keys.getLast() : keys.getFirst();
                    for (var i = 0; i < lengths[block]; i++) {
                        // Sorts right after its old key and before the next one
                        var key = Arrays.copyOf(after, after.length + 1);
                        key[after.length] = (byte) i;
                        states.put(key, tree.bytes(valueLength));
                    }
                }
                tree.apply(states, List.of());
                var keys = new ArrayList<>(tree.model.keySet());
                var sizes = tree.model.entrySet().stream().mapToInt(entry -> entrySize(entry.getKey(), entry.getValue())).toArray();
                assertTrue(TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_ROOMY) > TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_NEAR_FULL)
                        && TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_NEAR_FULL) > TreeNodes.nodesFor(sizes, sizes.length, TreeNodes.TARGET_FULL),
                        "the targets take as many leaves as each other");
                var expected = TreeNodes.split(sizes, sizes.length, target).stream().map(run -> keys.get(run[0])).toList();
                assertSameKeys(expected, firstKeys(tree.leaves()));
            }
        }

        @Test
        @DisplayName("removals and inserts that cancel out keep room in the leaves, and rewrite no more of them each time")
        void churn(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 5)) {
                tree.load(40_000, 32, 120);
                var live = new ArrayList<>(tree.model.keySet());
                java.util.Collections.shuffle(live, tree.random);
                var oldest = 0;
                for (var round = 0; round < 12; round++) {
                    var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                    for (var i = 0; i < 2_000; i++) {
                        states.put(live.get(oldest++), null);
                        var key = tree.bytes(32);
                        live.add(key);
                        states.put(key, tree.bytes(120));
                    }
                    tree.apply(states, List.of());
                    assertEquals(40_000, tree.model.size());
                }
                assertTrue(tree.deepestRecords > 0, "no leaf took a delta record");
                assertTrue(tree.fill() < 0.9, "the leaves keep no room: " + tree.fill());
            }
        }
    }

    @Nested
    @DisplayName("the leaves of a run")
    class Runs {
        @Test
        @DisplayName("a leaf written whole with no such neighbour splits as it always did")
        void alone(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 6)) {
                tree.load(20_000, 16, 64);
                var leaves = tree.leaves();
                assertTrue(tree.level() >= 1 && leaves.size() > 8);
                var at = leaves.size() / 2;
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                var low = leaves.get(at).firstKey();
                var high = leaves.get(at + 1).firstKey();
                // Keys between the leaf's first and its neighbour's, enough to split it
                while (states.size() < 40) {
                    var key = between(low, high, tree.random);
                    if (key != null && !tree.model.containsKey(key)) {
                        states.put(key, tree.bytes(64));
                    }
                }
                var expected = new TreeMap<byte[], byte[]>(UNSIGNED);
                expected.putAll(tree.model.subMap(low, true, high, false));
                expected.putAll(states);
                var sizes = expected.entrySet().stream().mapToInt(entry -> entrySize(entry.getKey(), entry.getValue())).toArray();
                var keys = new ArrayList<>(expected.keySet());
                var expectedFirsts = new ArrayList<byte[]>();
                for (var run : TreeNodes.split(sizes, sizes.length)) {
                    expectedFirsts.add(keys.get(run[0]));
                }
                assertTrue(expectedFirsts.size() > 1, "the writes did not split the leaf");
                tree.apply(states, List.of());
                var after = tree.leaves();
                assertEquals(leaves.size() + expectedFirsts.size() - 1, after.size());
                assertSameKeys(expectedFirsts, firstKeys(after.subList(at, at + expectedFirsts.size())));
                assertSameKeys(firstKeys(leaves.subList(0, at)), firstKeys(after.subList(0, at)));
            }
        }

        /// {@return a random key from `low` to before `high`, or `null` if one of the keys drawn is out of the range}
        private static byte[] between(byte[] low, byte[] high, Random random) {
            var key = low.clone();
            var at = Arrays.mismatch(low, high);
            var tail = new byte[key.length - at - 1];
            random.nextBytes(tail);
            System.arraycopy(tail, 0, key, at + 1, tail.length);
            return UNSIGNED.compare(key, low) > 0 && UNSIGNED.compare(key, high) < 0 ? key : null;
        }

        @Test
        @DisplayName("a root leaf splits as it always did")
        void rootLeaf(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 7)) {
                tree.load(10, 16, 64);
                assertEquals(0, tree.level());
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                while (states.size() < 60) {
                    states.put(tree.bytes(16), tree.bytes(64));
                }
                tree.apply(states, List.of());
                var sizes = new int[70];
                Arrays.fill(sizes, TreeNodes.SLOT + 2 + 16 + 1 + 2 + 64);
                var expected = TreeNodes.split(sizes, 70);
                assertEquals(expected.size(), tree.leaves().size());
                assertEquals(expected.getFirst()[1], tree.leaves().getFirst().count());
            }
        }

        @ParameterizedTest(name = "thin leaves: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a delta record or an unchanged leaf ends a run, a dropped leaf does not, and an emptied run writes nothing")
        void boundaries(boolean thin, @TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, true, 8)) {
                tree.load(24_000, 16, 64);
                if (thin) {
                    tree.thin(0.5);
                }
                var leaves = tree.leaves();
                var at = 5;
                assertTrue(leaves.size() > at + 20);
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                var ranges = new ArrayList<byte[][]>();
                // Two leaves written whole, then a delta record: a run ends there
                overwrite(tree, leaves, at, states);
                overwrite(tree, leaves, at + 1, states);
                var record = tree.keys(leaves, at + 2);
                states.put(record.get(record.size() / 2), tree.bytes(64));
                // Written whole, dropped, written whole: one run
                overwrite(tree, leaves, at + 3, states);
                ranges.add(new byte[][]{leaves.get(at + 4).firstKey(), leaves.get(at + 5).firstKey()});
                overwrite(tree, leaves, at + 5, states);
                // An unchanged leaf ends it
                var unchanged = leaves.get(at + 6);
                // A run with all its keys removed
                for (var key : tree.keys(leaves, at + 7)) {
                    states.put(key, null);
                }
                for (var key : tree.keys(leaves, at + 8)) {
                    states.put(key, null);
                }
                var survivor = leaves.get(at + 9);
                tree.apply(states, ranges);
                var after = tree.leaves();
                var firsts = firstKeys(after);
                var deltaLeaf = after.stream().filter(leaf -> Arrays.equals(leaf.firstKey(), leaves.get(at + 2).firstKey())).findFirst().orElseThrow();
                assertTrue(deltaLeaf.ref().isRecord(), "the leaf a write barely changed is no delta record");
                assertEquals(leaves.get(at + 2).count(), deltaLeaf.count());
                assertTrue(firsts.stream().anyMatch(first -> Arrays.equals(first, unchanged.firstKey())), "the unchanged leaf moved");
                assertTrue(after.stream().anyMatch(leaf -> leaf.ref().equals(unchanged.ref())), "the unchanged leaf was rewritten");
                assertTrue(after.stream().anyMatch(leaf -> leaf.ref().equals(survivor.ref())), "the leaf past the removed run was rewritten");
                assertFalse(firsts.stream().anyMatch(first -> Arrays.equals(first, leaves.get(at + 7).firstKey())), "a removed leaf is still there");
                assertFalse(firsts.stream().anyMatch(first -> Arrays.equals(first, leaves.get(at + 4).firstKey())), "a dropped leaf is still there");
                // A delta record's neighbours and the dropped leaf's stay in the order of their keys, and the leaves
                // before the first run were not touched
                assertSameKeys(firstKeys(leaves.subList(0, at)), firsts.subList(0, at));
            }
        }

        private static void overwrite(Harness tree, List<Leaf> leaves, int index, TreeMap<byte[], byte[]> states) {
            for (var key : tree.keys(leaves, index)) {
                states.put(key, tree.bytes(64));
            }
        }

        @Test
        @DisplayName("a run that saves a leaf ends with fewer leaves than each on its own, and one that would not keeps its boundaries")
        void savesOrStays(@TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, false, 9)) {
                tree.load(24_000, 16, 64);
                var dense = tree.leaves();
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                overwrite(tree, dense, 3, states);
                overwrite(tree, dense, 4, states);
                overwrite(tree, dense, 5, states);
                tree.apply(states, List.of());
                assertSameKeys(firstKeys(dense), firstKeys(tree.leaves()));
                tree.thin(0.5);
                var thin = tree.leaves();
                states = new TreeMap<>(UNSIGNED);
                overwrite(tree, thin, 3, states);
                overwrite(tree, thin, 4, states);
                overwrite(tree, thin, 5, states);
                overwrite(tree, thin, 6, states);
                tree.apply(states, List.of());
                assertTrue(tree.leaves().size() < thin.size(), "the run saved no leaf");
            }
        }

        static Stream<Arguments> plans() {
            return Stream.of(
                    Arguments.of(false, true), Arguments.of(false, false),
                    Arguments.of(true, true), Arguments.of(true, false));
        }

        @ParameterizedTest(name = "parallel: {0}, encrypted: {1}")
        @MethodSource("plans")
        @DisplayName("rounds of random plans per leaf leave the tree equal to the model, with every page reachable or free")
        void randomPlans(boolean parallel, boolean encrypted, @TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, encrypted, parallel ? 10 : 11)) {
                // Under a root of one level, so that with enough writes the tasks of the root's children are leaves
                tree.load(parallel ? 8_000 : 40_000, 8, 8);
                var parallelRounds = 0;
                for (var round = 0; round < 40; round++) {
                    var leaves = tree.leaves();
                    var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                    var ranges = new ArrayList<byte[][]>();
                    for (var i = 0; i < leaves.size(); i++) {
                        plan(tree, leaves, i, states, ranges);
                    }
                    if (parallel) {
                        // A branch updates its children in parallel from 4,096 writes below it; overwrites bring them
                        // there and leave the tree no deeper
                        for (var key : tree.model.keySet()) {
                            if (states.size() >= 4_200) {
                                break;
                            }
                            if (!states.containsKey(key) && inNoRange(key, ranges) && tree.random.nextBoolean()) {
                                states.put(key, tree.bytes(8));
                            }
                        }
                        if (tree.level() == 1 && states.size() >= 4_096) {
                            parallelRounds++;
                        }
                    }
                    tree.apply(states, ranges);
                    if (round % 7 == 0) {
                        tree.pages.cache().clearForTests();
                        tree.verify();
                    }
                    if (tree.model.size() < 3_000) {
                        var refill = new TreeMap<byte[], byte[]>(UNSIGNED);
                        while (refill.size() < 8_000) {
                            refill.put(tree.bytes(8), tree.bytes(8));
                        }
                        tree.apply(refill, List.of());
                    }
                }
                assertTrue(tree.deepestRecords > 0, "no leaf was ever written as a delta record");
                assertTrue(!parallel || parallelRounds >= 10, "only " + parallelRounds + " rounds updated leaves in parallel");
            }
        }

        private static boolean inNoRange(byte[] key, List<byte[][]> ranges) {
            for (var range : ranges) {
                if (UNSIGNED.compare(key, range[0]) >= 0 && UNSIGNED.compare(key, range[1]) < 0) {
                    return false;
                }
            }
            return true;
        }

        /// Gives leaf `index` a random share of the round: nothing, a delta record's worth, a rewrite, inserts, growth,
        /// removal of its keys, or a removed range.
        private static void plan(Harness tree, List<Leaf> leaves, int index, TreeMap<byte[], byte[]> states, List<byte[][]> ranges) {
            var keys = tree.keys(leaves, index);
            var random = tree.random;
            switch (random.nextInt(9)) {
                case 0, 1 -> {
                }
                case 2 -> states.put(keys.get(random.nextInt(keys.size())), tree.bytes(8));
                case 3, 4 -> {
                    for (var key : keys) {
                        states.put(key, tree.bytes(8));
                    }
                }
                case 5 -> {
                    for (var i = 0; i < keys.size(); i += 3) {
                        states.put(keys.get(i), tree.bytes(8 + random.nextInt(24)));
                    }
                }
                case 6 -> {
                    for (var i = 0; i < 80; i++) {
                        var key = keys.get(random.nextInt(keys.size())).clone();
                        key[7] ^= (byte) (1 + random.nextInt(255));
                        if (UNSIGNED.compare(key, leaves.get(index).firstKey()) > 0
                                && (index + 1 == leaves.size() || UNSIGNED.compare(key, leaves.get(index + 1).firstKey()) < 0)
                                && !tree.model.containsKey(key)) {
                            states.put(key, tree.bytes(8));
                        }
                    }
                }
                case 7 -> {
                    for (var key : keys) {
                        states.put(key, null);
                    }
                }
                default -> {
                    if (index + 1 < leaves.size()) {
                        ranges.add(new byte[][]{leaves.get(index).firstKey(), leaves.get(index + 1).firstKey()});
                    }
                }
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("every leaf of a branch rewritten in parallel tasks gives its units up once, in the branch above")
        void parallelRewrites(boolean encrypted, @TempDir Path directory) throws Exception {
            try (var tree = new Harness(directory, encrypted, 12)) {
                tree.load(8_000, 8, 8);
                assertEquals(1, tree.level());
                tree.thin(0.5);
                var before = tree.leaves().size();
                var states = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var key : tree.model.keySet()) {
                    states.put(key, tree.bytes(8));
                }
                while (states.size() < 4_200) {
                    states.put(tree.bytes(8), tree.bytes(8));
                }
                tree.apply(states, List.of());
                assertTrue(tree.leaves().size() < before * 3 / 4, tree.leaves().size() + " leaves out of " + before);
                assertEquals(0, tree.deltas.size());
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        static IntStream seeds() {
            return IntStream.range(0, 24);
        }

        @ParameterizedTest(name = "seed {0}")
        @MethodSource("seeds")
        @DisplayName("recovers a state between its last durable commit and its last visible one, whichever write the failure stops")
        void recovers(int seed) throws Exception {
            var encryption = seed % 2 == 0 ? Encryption.key(KEY) : Encryption.none();
            var dry = CrashFileSystem.create();
            new Workload(seed, encryption).run(dry.getPath("/store"));
            var random = new Random(seed * 131L + 7);
            var disk = CrashFileSystem.create();
            if (random.nextBoolean()) {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            } else {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            }
            var workload = new Workload(seed, encryption);
            workload.run(disk.getPath("/store"));
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = open(survivor.getPath("/store"), encryption)) {
                var recovered = contents(ledger);
                var match = false;
                for (var commit = workload.durable; commit <= workload.visible && !match; commit++) {
                    match = workload.history.get(commit).equals(recovered);
                }
                if (!match) {
                    fail("The store recovered a state no commit from " + workload.durable + " to " + workload.visible + " left");
                }
                assertEquals(recovered.size(), ledger.size(MESSAGES));
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
            }
            CheckpointTest.verifyPages(survivor.getPath("/store"), seed % 2 == 0);
        }

        private static Ledger open(Path directory, Encryption encryption) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES))
                    .segmentSize(64 * 1024)
                    .build();
        }

        private static TreeMap<String, String> contents(Ledger ledger) {
            var entries = new TreeMap<String, String>();
            ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                entries.put(key, value);
                return true;
            });
            return entries;
        }

        /// Rounds of commits and checkpoints whose updates make runs of every kind, recording the state after each commit.
        private static final class Workload {
            private final Random random;
            private final Encryption encryption;
            final List<TreeMap<String, String>> history = new ArrayList<>();
            final TreeMap<String, String> state = new TreeMap<>();
            int visible;
            int durable;

            Workload(int seed, Encryption encryption) {
                this.random = new Random(seed);
                this.encryption = encryption;
                history.add(new TreeMap<>());
            }

            void run(Path directory) {
                Ledger ledger = null;
                try {
                    ledger = open(directory, encryption);
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 1_500; i++) {
                        var key = "k%05d".formatted(i * 2);
                        var value = value(200);
                        batch.put(MESSAGES, key, value);
                        state.put(key, value);
                    }
                    commit(ledger, batch);
                    ledger.checkpoint();
                    durable = visible;
                    for (var round = 0; round < 8; round++) {
                        batch = ledger.newBatch();
                        for (var key : new ArrayList<>(state.keySet())) {
                            switch (random.nextInt(6)) {
                                case 0 -> {
                                    batch.delete(MESSAGES, key);
                                    state.remove(key);
                                }
                                case 1, 2 -> {
                                    var value = value(190 + random.nextInt(30));
                                    batch.put(MESSAGES, key, value);
                                    state.put(key, value);
                                }
                                default -> {
                                }
                            }
                        }
                        for (var i = 0; i < 200; i++) {
                            var key = "k%05d".formatted(random.nextInt(3_000));
                            var value = value(200);
                            batch.put(MESSAGES, key, value);
                            state.put(key, value);
                        }
                        commit(ledger, batch);
                        if (round % 3 == 2) {
                            var from = "k%05d".formatted(random.nextInt(2_500));
                            var to = "k%05d".formatted(random.nextInt(2_500));
                            if (from.compareTo(to) < 0) {
                                batch = ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(from, to));
                                state.subMap(from, to).clear();
                                commit(ledger, batch);
                            }
                        }
                        ledger.checkpoint();
                        durable = visible;
                    }
                    ledger.close();
                    durable = visible;
                } catch (RuntimeException failure) {
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (RuntimeException _) {
                            // The file system stopped: closing only lets go of the threads
                        }
                    }
                }
            }

            private void commit(Ledger ledger, com.github.auties00.ledger.WriteBatch batch) {
                ledger.commit(batch);
                visible++;
                history.add(new TreeMap<>(state));
            }

            private String value(int length) {
                var builder = new StringBuilder(length);
                for (var i = 0; i < length; i++) {
                    builder.append((char) ('a' + random.nextInt(26)));
                }
                return builder.toString();
            }
        }

        @ParameterizedTest(name = "values of {0} and {1} bytes")
        @CsvSource({"200, 200", "150, 240"})
        @DisplayName("snapshots read one commit's state whole while checkpoints split runs of the leaves they read")
        void readers(int shorter, int longer, @TempDir Path directory) throws Exception {
            var keys = 4_000;
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES).pageCache(1 << 20).build()) {
                var batch = ledger.newBatch();
                for (var i = 0; i < keys; i++) {
                    batch.put(MESSAGES, "k%05d".formatted(i), stamped(0, shorter));
                }
                ledger.commit(batch);
                ledger.checkpoint();
                var stop = new AtomicBoolean();
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var threads = new ArrayList<Thread>();
                threads.add(Thread.ofPlatform().start(() -> {
                    while (!stop.get()) {
                        try {
                            ledger.checkpoint();
                        } catch (Throwable failure) {
                            failures.add(failure);
                            return;
                        }
                    }
                }));
                for (var reader = 0; reader < 3; reader++) {
                    var random = new Random(reader);
                    threads.add(Thread.ofPlatform().start(() -> {
                        while (!stop.get()) {
                            try (var snapshot = ledger.snapshot()) {
                                var stamp = snapshot.get(MESSAGES, "k00000").orElseThrow().substring(0, 6);
                                var seen = new int[1];
                                snapshot.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                                    if (!value.startsWith(stamp)) {
                                        throw new AssertionError(key + " is at " + value.substring(0, 6) + " in a snapshot at " + stamp);
                                    }
                                    if (key.startsWith("k")) {
                                        seen[0]++;
                                    }
                                    return true;
                                });
                                if (seen[0] != keys) {
                                    throw new AssertionError(seen[0] + " keys in a snapshot at " + stamp);
                                }
                                var key = "k%05d".formatted(random.nextInt(keys));
                                if (!snapshot.get(MESSAGES, key).orElseThrow().startsWith(stamp)) {
                                    throw new AssertionError(key + " in a snapshot at " + stamp);
                                }
                            } catch (Throwable failure) {
                                failures.add(failure);
                                return;
                            }
                        }
                    }));
                }
                var random = new Random(99);
                for (var round = 1; round <= 60 && failures.isEmpty(); round++) {
                    var next = ledger.newBatch();
                    for (var i = 0; i < keys; i++) {
                        next.put(MESSAGES, "k%05d".formatted(i), stamped(round, round % 2 == 0 ? shorter : longer));
                    }
                    // Scattered keys of this round, gone in the next, so that runs take inserts and removals too
                    next.deleteRange(MESSAGES, KeyRange.atLeast("n"));
                    for (var i = 0; i < 100; i++) {
                        next.put(MESSAGES, "n%05d".formatted(random.nextInt(99_999)), stamped(round, shorter));
                    }
                    ledger.commit(next);
                }
                stop.set(true);
                for (var thread : threads) {
                    thread.join();
                }
                if (!failures.isEmpty()) {
                    throw new AssertionError(failures.peek());
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }

        private static String stamped(int round, int length) {
            return "%06d".formatted(round) + "v".repeat(length);
        }
    }
}
