package com.github.auties00.ledger.engine;

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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The search index of cached nodes: every search of a node holding one against a sorted list of its keys and against
/// the plain search of the same node, and the paths that build an index or leave a node without one.
class NodeIndexTest {
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final byte[] KEY = new byte[32];

    @Nested
    @DisplayName("searches of a single node")
    class Nodes {
        static Stream<KeyShape> shapes() {
            return Stream.of(KeyShape.values());
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("shapes")
        @DisplayName("a leaf finds what its sorted keys and its plain search find")
        void leaves(KeyShape shape) {
            var random = new Random(shape.ordinal() * 31L + 1);
            var indexes = 0;
            for (var round = 0; round < 200; round++) {
                var keys = shape.keys(random, 1 + random.nextInt(600));
                var plain = leaf(keys);
                var indexed = NodeIndex.attach(plain);
                indexes += indexed != plain ? 1 : 0;
                assertArrayEquals(plain, Arrays.copyOf(indexed, PageFormat.SIZE));
                for (var query : queries(keys, random)) {
                    var expected = lowerBound(keys, query, 0);
                    assertEquals(expected, TreeNodes.lowerBound(plain, segment(query)), "plain");
                    assertEquals(expected, TreeNodes.lowerBound(indexed, segment(query)), () -> "lowerBound " + Arrays.toString(query));
                }
            }
            if (shape.indexes) {
                assertTrue(indexes > 0, "no leaf of this shape got an index");
            }
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("shapes")
        @DisplayName("a branch finds what its sorted keys and its plain search find, the first child below every key")
        void branches(KeyShape shape) {
            var random = new Random(shape.ordinal() * 37L + 2);
            var indexes = 0;
            for (var round = 0; round < 200; round++) {
                var keys = shape.keys(random, 1 + random.nextInt(100));
                var plain = branch(keys);
                var indexed = NodeIndex.attach(plain);
                indexes += indexed != plain ? 1 : 0;
                assertArrayEquals(plain, Arrays.copyOf(indexed, PageFormat.SIZE));
                for (var query : queries(keys, random)) {
                    var childFor = Math.max(0, upperBound(keys, query) - 1);
                    var lastBefore = lowerBound(keys, query, 0) - 1;
                    assertEquals(childFor, TreeNodes.childFor(plain, segment(query)), "plain");
                    assertEquals(lastBefore, TreeNodes.lastChildBefore(plain, segment(query)), "plain");
                    assertEquals(childFor, TreeNodes.childFor(indexed, segment(query)), () -> "childFor " + Arrays.toString(query));
                    assertEquals(lastBefore, TreeNodes.lastChildBefore(indexed, segment(query)), () -> "lastChildBefore " + Arrays.toString(query));
                }
            }
            if (shape.indexes) {
                assertTrue(indexes > 0, "no branch of this shape got an index");
            }
        }

        @Test
        @DisplayName("takes a key in native memory as one in the heap")
        void nativeKeys() {
            var random = new Random(5);
            var keys = KeyShape.MESSAGE_IDS.keys(random, 40);
            var indexed = NodeIndex.attach(leaf(keys));
            assertTrue(NodeIndex.isPresent(indexed));
            try (var arena = Arena.ofConfined()) {
                for (var query : queries(keys, random)) {
                    var key = arena.allocate(Math.max(1, query.length)).asSlice(0, query.length);
                    MemorySegment.copy(query, 0, key, ValueLayout.JAVA_BYTE, 0, query.length);
                    assertEquals(lowerBound(keys, query, 0), TreeNodes.lowerBound(indexed, key));
                }
            }
        }

        @Test
        @DisplayName("holds no index for a node without enough keys to search")
        void fewKeys() {
            var keys = new ArrayList<byte[]>();
            for (var count = 0; count < NodeIndex.MIN_HEADS; count++) {
                var leaf = leaf(keys);
                assertSame(leaf, NodeIndex.attach(leaf), () -> keys.size() + " keys");
                keys.add(new byte[]{(byte) (count * 10)});
            }
            var branchKeys = new ArrayList<>(keys);
            var branch = branch(branchKeys);
            assertSame(branch, NodeIndex.attach(branch), "the first child's key is not a searched key");
            branchKeys.add(new byte[]{100});
            assertTrue(NodeIndex.isPresent(NodeIndex.attach(branch(branchKeys))));
            assertTrue(NodeIndex.isPresent(NodeIndex.attach(leaf(branchKeys))));
        }

        @Test
        @DisplayName("holds no index when a run of equal heads passes a quarter of the keys")
        void degenerateHeads() {
            var keys = new TreeSet<>(UNSIGNED);
            // Two chats sharing the first bytes of their ids and of their message ids
            for (var i = 0; i < 16; i++) {
                keys.add(("\0\0\1" + "3EB0" + "%04x".formatted(i)).getBytes(StandardCharsets.ISO_8859_1));
                keys.add(("\0\0\2" + "3EB0" + "%04x".formatted(i)).getBytes(StandardCharsets.ISO_8859_1));
            }
            var straddling = leaf(new ArrayList<>(keys));
            assertSame(straddling, NodeIndex.attach(straddling));
            // A key and the keys it starts, past the prefix only zeros
            var zeros = new ArrayList<byte[]>();
            for (var i = 0; i < 8; i++) {
                zeros.add(new byte[5 + i]);
            }
            var padded = leaf(zeros);
            assertSame(padded, NodeIndex.attach(padded));
            // Keys sharing more than the prefix an index holds and the head after it
            var longKeys = new ArrayList<byte[]>();
            for (var i = 0; i < 16; i++) {
                var key = new byte[NodeIndex.MAX_PREFIX + 8];
                key[key.length - 1] = (byte) i;
                longKeys.add(key);
            }
            var shared = leaf(longKeys);
            assertSame(shared, NodeIndex.attach(shared));
        }

        @Test
        @DisplayName("keeps an index of runs up to a quarter of the keys, which it decides by comparing the keys")
        void shortRuns() {
            var keys = new ArrayList<byte[]>();
            for (var run = 0; run < 8; run++) {
                for (var i = 0; i < 4; i++) {
                    keys.add(new byte[]{(byte) run, 1, 2, 3, (byte) i});
                }
            }
            var indexed = NodeIndex.attach(leaf(keys));
            assertTrue(NodeIndex.isPresent(indexed));
            for (var query : queries(keys, new Random(9))) {
                assertEquals(lowerBound(keys, query, 0), TreeNodes.lowerBound(indexed, segment(query)));
            }
        }

        @Test
        @DisplayName("holds at most the longest prefix and a head per key past the page")
        void bounded() {
            var keys = new ArrayList<byte[]>();
            for (var i = 0; i < 8; i++) {
                var key = new byte[300];
                key[NodeIndex.MAX_PREFIX + 1] = (byte) (i + 1);
                keys.add(key);
            }
            var indexed = NodeIndex.attach(leaf(keys));
            assertEquals(PageFormat.SIZE + 4 + NodeIndex.MAX_PREFIX + 4 * keys.size(), indexed.length);
            for (var query : queries(keys, new Random(13))) {
                assertEquals(lowerBound(keys, query, 0), TreeNodes.lowerBound(indexed, segment(query)));
            }
        }

        @Test
        @DisplayName("takes a node whose entries run past its page for a damaged one")
        void malformed() {
            var keys = KeyShape.MESSAGE_IDS.keys(new Random(1), 16);
            var offset = leaf(keys);
            Bytes.u16(offset, PageFormat.HEADER + 2 + 2 * 3, PageFormat.SIZE - 1);
            assertThrows(LedgerCorruptedException.class, () -> NodeIndex.attach(offset));
            var keyLength = leaf(keys);
            Bytes.u16(keyLength, TreeNodes.entry(keyLength, 15), PageFormat.SIZE);
            assertThrows(LedgerCorruptedException.class, () -> NodeIndex.attach(keyLength));
            var count = leaf(keys);
            Bytes.u16(count, PageFormat.HEADER, 65_535);
            assertThrows(LedgerCorruptedException.class, () -> NodeIndex.attach(count));
        }

        @Test
        @DisplayName("leaves any other unit as it is")
        void otherUnits() {
            for (var kind : new byte[]{PageFormat.VALUE, PageFormat.DELTA, PageFormat.CATALOG, PageFormat.SUPERBLOCK}) {
                var unit = new byte[PageFormat.SIZE];
                unit[PageFormat.KIND] = kind;
                Bytes.u16(unit, PageFormat.HEADER, 100);
                assertSame(unit, NodeIndex.attach(unit));
            }
            var longer = new byte[2 * PageFormat.SIZE];
            longer[PageFormat.KIND] = PageFormat.LEAF;
            assertSame(longer, NodeIndex.attach(longer));
        }
    }

    @Nested
    @DisplayName("nodes of a tree")
    class Trees {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("lookups, counts and cursors over indexed and plain nodes agree with a sorted map")
        void randomQueries(boolean encrypted, @TempDir Path directory) throws Exception {
            var random = new Random(encrypted ? 17 : 19);
            try (var pages = open(directory, encrypted, 64 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, random, 12_000);
                var keys = new ArrayList<>(model.keySet());
                // Starts with every node as the checkpoint cached it, without an index, and moves to read nodes
                for (var round = 0; round < 3; round++) {
                    for (var i = 0; i < 300; i++) {
                        var query = query(keys, random);
                        assertLookup(pages, root, model, query);
                        var other = query(keys, random);
                        var low = UNSIGNED.compare(query, other) <= 0 ? query : other;
                        var high = low == query ? other : query;
                        assertEquals(low == high || UNSIGNED.compare(low, high) >= 0 ? 0 : model.subMap(low, true, high, false).size(),
                                TreeSearch.count(pages, root, segment(low), segment(high)));
                        assertEquals(model.tailMap(low, true).size(), TreeSearch.count(pages, root, segment(low), null));
                        assertEquals(model.headMap(high, false).size(), TreeSearch.count(pages, root, null, segment(high)));
                        if (i % 30 == 0) {
                            assertCursor(pages, root, model, low, high, false, 20);
                            assertCursor(pages, root, model, low, high, true, 20);
                            assertCursor(pages, root, model, low, null, false, 20);
                            assertCursor(pages, root, model, null, high, true, 20);
                        }
                    }
                    if (round == 0) {
                        pages.cache().clearForTests();
                    }
                    if (round == 1) {
                        // Half of the leaves back to the plain form, beside the indexed ones
                        evictLeaves(pages, root, random);
                    }
                }
                assertTrue(indexed(pages, root) > 0, "no node of the tree got an index");
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a descending range ends at a branch's first child, whichever side of its key the range ends")
        void descendingAtFirstChild(boolean encrypted, @TempDir Path directory) throws Exception {
            var random = new Random(encrypted ? 23 : 29);
            try (var pages = open(directory, encrypted, 64 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, random, 60_000);
                pages.cache().clearForTests();
                var top = pages.read(root.ref(), PageFile.NODE, true);
                assertTrue(TreeNodes.level(top) >= 2, "the tree has one level of branches");
                assertTrue(NodeIndex.isPresent(top));
                var plainTop = Arrays.copyOf(top, PageFormat.SIZE);
                var branches = new ArrayList<byte[]>();
                branches.add(top);
                for (var child : new int[]{0, 1, TreeNodes.count(top) - 1}) {
                    branches.add(pages.read(TreeNodes.childRef(top, child), PageFile.NODE, true));
                }
                var checked = 0;
                for (var branch : branches) {
                    var first = TreeNodes.childKey(branch, 0).toArray(ValueLayout.JAVA_BYTE);
                    for (var to : aroundKey(first)) {
                        var plain = Arrays.copyOf(branch, PageFormat.SIZE);
                        assertEquals(TreeNodes.lastChildBefore(plain, segment(to)), TreeNodes.lastChildBefore(branch, segment(to)));
                        assertEquals(TreeNodes.childFor(plain, segment(to)), TreeNodes.childFor(branch, segment(to)));
                        assertCursor(pages, root, model, null, to, true, 10);
                        assertCursor(pages, root, model, to, null, false, 10);
                        checked += NodeIndex.isPresent(branch) ? 1 : 0;
                    }
                }
                assertTrue(checked > 3, "no indexed branch was searched at its first child");
                // Below every key of the tree: the root's first child is the end of the range
                assertEquals(-1, TreeNodes.lastChildBefore(top, segment(new byte[0])));
                assertEquals(-1, TreeNodes.lastChildBefore(plainTop, segment(new byte[0])));
                var cursor = new TreeCursor(pages, root, null, segment(new byte[0]), true);
                assertFalse(cursor.next());
            }
        }

        @Test
        @DisplayName("reads through the cache index only what the cache takes as read")
        void whatGetsAnIndex(@TempDir Path directory) throws Exception {
            try (var pages = open(directory, true, 64 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, new Random(3), 8_000);
                var ref = firstLeaf(pages, root);
                pages.cache().clearForTests();
                assertFalse(NodeIndex.isPresent(pages.read(ref, PageFile.NODE, false)), "a read past the cache");
                assertNull(pages.cached(ref, PageFile.NODE));
                assertFalse(NodeIndex.isPresent(pages.read(ref, PageFile.NODE, true, false)), "a read the cache evicts first");
                pages.cache().clearForTests();
                assertFalse(NodeIndex.isPresent(pages.readToReplace(ref, PageFile.NODE, true)), "a read of a checkpoint");
                assertFalse(NodeIndex.isPresent(pages.read(ref, PageFile.NODE, true)), "a hit keeps the unit the cache holds");
                pages.cache().clearForTests();
                var indexed = pages.read(ref, PageFile.NODE, true);
                assertTrue(NodeIndex.isPresent(indexed), "a read that counts");
                assertSame(indexed, pages.cached(ref, PageFile.NODE));
                assertSame(indexed, pages.read(ref, PageFile.NODE, false));
                assertSame(indexed, pages.readToReplace(ref, PageFile.NODE, true));
                pages.cache().clearForTests();
                assertArrayEquals(Arrays.copyOf(indexed, PageFormat.SIZE), pages.read(ref, PageFile.NODE, false));
            }
        }

        @Test
        @DisplayName("charges the cache for the whole array")
        void accounted(@TempDir Path directory) throws Exception {
            try (var pages = open(directory, true, 64 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, new Random(3), 8_000);
                var ref = firstLeaf(pages, root);
                pages.cache().clearForTests();
                assertEquals(0, pages.cache().size());
                var unit = pages.read(ref, PageFile.NODE, true);
                assertTrue(unit.length > PageFormat.SIZE);
                assertEquals(unit.length, pages.cache().size());
                pages.cache().clearForTests();
                assertEquals(0, pages.cache().size());
            }
        }

        @Test
        @DisplayName("never changes the file or the plaintext of a node, nor its reserved header bytes")
        void onlyInMemory(@TempDir Path directory) throws Exception {
            try (var pages = open(directory, true, 64 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, new Random(41), 8_000);
                pages.force();
                var file = directory.resolve(PageFile.FILE);
                var before = Files.readAllBytes(file);
                pages.cache().clearForTests();
                for (var key : model.keySet()) {
                    assertNotNull(TreeSearch.find(pages, root, segment(key)));
                }
                assertTrue(indexed(pages, root) > 0);
                pages.force();
                assertArrayEquals(before, Files.readAllBytes(file));
                pages.cache().clearForTests();
                walk(pages, root.ref(), unit -> {
                    assertEquals(PageFormat.SIZE, unit.length);
                    assertEquals(0, Bytes.i32(unit, PageFormat.NONCE + 12));
                    return unit;
                });
            }
        }

        @Test
        @DisplayName("a leaf made of a delta record gets an index on a read, and none from the checkpoint that wrote it")
        void deltaLeaves(@TempDir Path directory) throws Exception {
            var random = new Random(47);
            try (var pages = open(directory, true, 64 << 20)) {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, random, 12_000, space);
                var keys = new ArrayList<>(model.keySet());
                var ops = new TreeMap<byte[], byte[]>(UNSIGNED);
                for (var i = 0; i < 400; i++) {
                    ops.put(keys.get(random.nextInt(keys.size())), value(random));
                }
                var writer = pages.writer(random.nextLong());
                var list = new ArrayList<TreeUpdate.Op>();
                for (var op : ops.entrySet()) {
                    list.add(new TreeUpdate.Op(segment(op.getKey()), segment(op.getValue())));
                    model.put(op.getKey(), op.getValue());
                }
                var update = new TreeUpdate(pages, writer, space);
                root = update.apply(root, list, List.of());
                update.finishDeltas(DeltaTable.EMPTY);
                writer.flush();
                var written = records(pages, root);
                assertFalse(written.isEmpty(), "the update wrote no delta record");
                for (var ref : written) {
                    var cached = pages.cached(ref, PageFormat.LEAF);
                    assertNotNull(cached, "the checkpoint caches the leaf of its record");
                    assertFalse(NodeIndex.isPresent(cached), "the leaf of a record got an index from its checkpoint");
                }
                assertEquals(0, indexed(pages, root), "a checkpoint built an index");
                for (var key : keys) {
                    assertLookup(pages, root, model, key);
                }
                pages.cache().clearForTests();
                var read = 0;
                for (var ref : records(pages, root)) {
                    assertFalse(NodeIndex.isPresent(pages.read(ref, PageFormat.LEAF, false)), "a read past the cache");
                    assertNull(pages.cached(ref, PageFormat.LEAF));
                    var unit = pages.read(ref, PageFormat.LEAF, true);
                    assertSame(unit, pages.cached(ref, PageFormat.LEAF));
                    read += NodeIndex.isPresent(unit) ? 1 : 0;
                    var leafKeys = new ArrayList<byte[]>();
                    for (var entry : TreeNodes.leafEntries(unit)) {
                        leafKeys.add(entry.key().toArray(ValueLayout.JAVA_BYTE));
                    }
                    for (var query : queries(leafKeys, random)) {
                        assertEquals(lowerBound(leafKeys, query, 0), TreeNodes.lowerBound(unit, segment(query)));
                    }
                }
                assertTrue(read > 0, "no leaf of a record got an index from a read");
                for (var key : keys) {
                    assertLookup(pages, root, model, key);
                }
            }
        }

        @Test
        @DisplayName("readers racing evictions, plain reads and indexed reads of the same pages see every key")
        void concurrentReaders(@TempDir Path directory) throws Exception {
            // Far less than the tree, so that readers evict and read nodes again all along
            try (var pages = open(directory, true, 1 << 20)) {
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var root = build(pages, model, new Random(53), 12_000);
                var keys = new ArrayList<>(model.keySet());
                var refs = new ArrayList<PageRef>();
                walk(pages, root.ref(), refs);
                var stop = new AtomicBoolean();
                try (var executor = Executors.newFixedThreadPool(6)) {
                    var tasks = new ArrayList<java.util.concurrent.Future<?>>();
                    for (var thread = 0; thread < 4; thread++) {
                        var seed = thread;
                        tasks.add(executor.submit((Callable<Void>) () -> {
                            var random = new Random(seed);
                            for (var i = 0; i < 20_000; i++) {
                                var key = query(keys, random);
                                assertLookup(pages, root, model, key);
                                if (i % 2_000 == 0) {
                                    assertCursor(pages, root, model, key, null, false, 5);
                                    assertCursor(pages, root, model, null, key, true, 5);
                                }
                            }
                            return null;
                        }));
                    }
                    for (var thread = 0; thread < 2; thread++) {
                        var seed = 100 + thread;
                        executor.submit((Callable<Void>) () -> {
                            var random = new Random(seed);
                            while (!stop.get()) {
                                var ref = refs.get(random.nextInt(refs.size()));
                                switch (random.nextInt(4)) {
                                    case 0 -> pages.cache().remove(ref.page());
                                    case 1 -> pages.readToReplace(ref, PageFile.NODE, true);
                                    case 2 -> pages.read(ref, PageFile.NODE, true, false);
                                    default -> pages.read(ref, PageFile.NODE, true);
                                }
                            }
                            return null;
                        });
                    }
                    try {
                        for (var task : tasks) {
                            task.get();
                        }
                    } finally {
                        stop.set(true);
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("a whole store")
    class Stores {
        private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

        private static Ledger open(Path directory, Encryption encryption) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES))
                    .segmentSize(16_384)
                    .build();
        }

        private static String key(int chat, int message) {
            return "chat%04d/3EB0%016x".formatted(chat, message * 2_654_435_761L & 0xFFFF_FFFFL);
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {1, 2, 3, 4, 5, 6})
        @DisplayName("recovers what it made durable when the power fails while reads build indexes")
        void powerLoss(int seed) throws Exception {
            var encryption = seed % 2 == 0 ? Encryption.none() : Encryption.key(KEY);
            var dry = CrashFileSystem.create();
            new History(seed, encryption).run(dry.getPath("/store"));
            var disk = CrashFileSystem.create();
            var random = new Random(seed * 131L);
            disk.stopAtForce(random.nextLong(dry.forces() + 1));
            var history = new History(seed, encryption);
            history.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = open(survivor.getPath("/store"), encryption)) {
                // Every durable key, twice: from the file, then through what the first read cached
                for (var round = 0; round < 2; round++) {
                    for (var entry : history.durable.entrySet()) {
                        var found = ledger.get(MESSAGES, entry.getKey()).orElse(null);
                        if (!entry.getValue().equals(found)) {
                            assertEquals(history.pending.get(entry.getKey()), found, entry.getKey());
                        }
                    }
                }
                var seen = new TreeMap<String, String>();
                ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                    seen.put(key, value);
                    return true;
                });
                assertEquals(seen.size(), ledger.size(MESSAGES));
                assertTrue(seen.keySet().containsAll(history.durable.keySet()));
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
            }
            CheckpointTest.verifyPages(survivor.getPath("/store"), encryption != Encryption.none());
        }

        /// Commits, checkpoints and reads the keys back after each checkpoint with a cold cache.
        private static final class History {
            private final Random random;
            private final Encryption encryption;
            final Map<String, String> durable;
            // The writes of the batch being committed, which recovery may keep
            final Map<String, String> pending;

            History(int seed, Encryption encryption) {
                this.random = new Random(seed);
                this.encryption = encryption;
                this.durable = new HashMap<>();
                this.pending = new HashMap<>();
            }

            void run(Path directory) {
                try (var ledger = open(directory, encryption)) {
                    for (var round = 0; round < 6; round++) {
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 300; i++) {
                            var key = key(random.nextInt(3), random.nextInt(2_000));
                            var value = "v" + random.nextInt(1_000_000);
                            batch.put(MESSAGES, key, value);
                            pending.put(key, value);
                        }
                        ledger.commit(batch).awaitDurable();
                        durable.putAll(pending);
                        pending.clear();
                        ledger.checkpoint();
                        ((LiveLedger) ledger).clearPageCacheForTests();
                        for (var entry : durable.entrySet()) {
                            assertEquals(entry.getValue(), ledger.get(MESSAGES, entry.getKey()).orElse(null));
                        }
                    }
                } catch (RuntimeException | InterruptedException _) {
                    // The file system stopped where it was told to
                }
            }
        }

        @Test
        @DisplayName("gives the same answers across a reopen, with the cache cold and warm")
        void reopen(@TempDir Path directory) throws Exception {
            var encryption = Encryption.key(KEY);
            var model = new TreeMap<String, String>();
            var random = new Random(61);
            try (var ledger = open(directory, encryption)) {
                for (var round = 0; round < 8; round++) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 1_000; i++) {
                        var key = key(random.nextInt(8), random.nextInt(4_000));
                        batch.put(MESSAGES, key, "value" + i);
                        model.put(key, "value" + i);
                    }
                    ledger.commit(batch);
                    ledger.checkpoint();
                }
            }
            try (var ledger = open(directory, encryption)) {
                for (var round = 0; round < 2; round++) {
                    for (var entry : model.entrySet()) {
                        assertEquals(entry.getValue(), ledger.get(MESSAGES, entry.getKey()).orElse(null));
                    }
                    for (var i = 0; i < 40; i++) {
                        var from = model.ceilingKey(key(random.nextInt(8), random.nextInt(4_000)));
                        var to = model.ceilingKey(key(random.nextInt(8), random.nextInt(4_000)));
                        if (from == null || to == null || from.compareTo(to) >= 0) {
                            continue;
                        }
                        var seen = new ArrayList<String>();
                        ledger.forEach(MESSAGES, KeyRange.between(from, to), (key, _) -> seen.add(key));
                        assertEquals(new ArrayList<>(model.subMap(from, to).keySet()), seen);
                    }
                }
            }
        }
    }

    /// The forms of keys a node may hold.
    enum KeyShape {
        RANDOM(true) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[1 + random.nextInt(24)];
                random.nextBytes(key);
                return key;
            }
        },
        HIGH_BYTES(true) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[6];
                key[0] = (byte) 0xFF;
                key[1] = (byte) 0xFE;
                random.nextBytes(key);
                key[0] = (byte) 0xFF;
                key[1] = (byte) 0xFE;
                return key;
            }
        },
        SHARED_PREFIX(true) {
            private static final byte[] PREFIX = "a shared prefix/".getBytes(StandardCharsets.ISO_8859_1);

            @Override
            byte[] key(Random random, int index) {
                var key = Arrays.copyOf(PREFIX, PREFIX.length + 1 + random.nextInt(9));
                for (var i = PREFIX.length; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(256);
                }
                return key;
            }
        },
        SHORT(true) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[random.nextInt(3)];
                random.nextBytes(key);
                return key;
            }
        },
        PREFIXES_OF_OTHERS(false) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[random.nextInt(30)];
                Arrays.fill(key, (byte) (random.nextBoolean() ? 0 : 'a'));
                return key;
            }
        },
        ZERO_ENDINGS(false) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[4 + random.nextInt(8)];
                key[0] = 7;
                key[1] = 8;
                if (random.nextBoolean()) {
                    key[key.length - 1] = (byte) random.nextInt(3);
                }
                return key;
            }
        },
        LONG_PREFIX(true) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[NodeIndex.MAX_PREFIX + random.nextInt(6) + 1];
                Arrays.fill(key, (byte) 'p');
                for (var i = NodeIndex.MAX_PREFIX; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(256);
                }
                return key;
            }
        },
        LONG_SHARED(false) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[NodeIndex.MAX_PREFIX + 12];
                Arrays.fill(key, (byte) 'p');
                key[key.length - 1] = (byte) random.nextInt(256);
                return key;
            }
        },
        MESSAGE_IDS(true) {
            @Override
            byte[] key(Random random, int index) {
                return ("\0\0\0\1" + "3EB0" + "%016x".formatted(random.nextLong())).getBytes(StandardCharsets.ISO_8859_1);
            }
        },
        STRADDLING_CHATS(false) {
            @Override
            byte[] key(Random random, int index) {
                var chat = (char) (1 + random.nextInt(2));
                return ("\0\0\0" + chat + "3EB0" + "%016x".formatted(random.nextLong())).getBytes(StandardCharsets.ISO_8859_1);
            }
        },
        MESSAGES(true) {
            @Override
            byte[] key(Random random, int index) {
                var key = new byte[4 + 8 + 12];
                key[3] = 9;
                Bytes.i64(key, 4, 1_700_000_000_000L + random.nextInt(1_000_000));
                for (var i = 12; i < key.length; i++) {
                    key[i] = (byte) random.nextInt(256);
                }
                return key;
            }
        };

        final boolean indexes;

        KeyShape(boolean indexes) {
            this.indexes = indexes;
        }

        abstract byte[] key(Random random, int index);

        /// {@return up to `count` sorted, distinct keys of this shape that fit in a node}
        List<byte[]> keys(Random random, int count) {
            var keys = new TreeSet<>(UNSIGNED);
            var bytes = 0;
            for (var i = 0; i < count; i++) {
                var key = key(random, i);
                // The larger of an entry of a leaf with an empty value and an entry of a branch
                if (bytes + 2 + 27 + key.length + 2 > TreeNodes.CAPACITY) {
                    break;
                }
                if (keys.add(key)) {
                    bytes += 2 + 27 + key.length + 2;
                }
            }
            return new ArrayList<>(keys);
        }
    }

    /// {@return a leaf of `keys`, each with an empty value}
    private static byte[] leaf(List<byte[]> keys) {
        var entries = new ArrayList<TreeNodes.LeafEntry>();
        for (var key : keys) {
            entries.add(new TreeNodes.LeafEntry(segment(key), MemorySegment.ofArray(new byte[0]), null, 0));
        }
        var unit = new byte[PageFormat.SIZE];
        var length = TreeNodes.writeLeaf(unit, entries, 0, entries.size());
        PageFile.header(unit, PageFormat.LEAF, 0, 2, 1, length, 1, 1);
        return unit;
    }

    /// {@return a branch whose children start at `keys`}
    private static byte[] branch(List<byte[]> keys) {
        var children = new ArrayList<TreeNodes.Child>();
        for (var i = 0; i < keys.size(); i++) {
            children.add(new TreeNodes.Child(segment(keys.get(i)), new PageRef(i + 2, 1, 1, i), 1, false, 0, -1));
        }
        var unit = new byte[PageFormat.SIZE];
        var length = TreeNodes.writeBranch(unit, children, 0, children.size());
        PageFile.header(unit, PageFormat.BRANCH, 1, 2, 1, length, 1, 1);
        return unit;
    }

    /// {@return the keys to search `keys` for: each of them, the keys around each, what they share, and random ones}
    private static List<byte[]> queries(List<byte[]> keys, Random random) {
        var queries = new ArrayList<byte[]>();
        queries.add(new byte[0]);
        queries.add(new byte[]{0});
        queries.add(new byte[]{(byte) 0xFF});
        var fulls = new byte[200];
        Arrays.fill(fulls, (byte) 0xFF);
        queries.add(fulls);
        var step = Math.max(1, keys.size() / 40);
        for (var i = 0; i < keys.size(); i += step) {
            queries.addAll(aroundKey(keys.get(i)));
        }
        if (!keys.isEmpty()) {
            var first = keys.getFirst();
            var last = keys.getLast();
            var mismatch = Arrays.mismatch(first, last);
            var shared = mismatch == -1 ? first.length : mismatch;
            for (var length = Math.max(0, shared - 2); length <= shared; length++) {
                var prefix = Arrays.copyOf(first, length);
                queries.add(prefix);
                queries.add(Arrays.copyOf(prefix, length + 1));
                queries.add(Arrays.copyOf(prefix, length + 3));
                queries.add(Arrays.copyOf(prefix, length + 5));
                var above = Arrays.copyOf(prefix, length + 1);
                above[length] = (byte) 0xFF;
                queries.add(above);
            }
            queries.addAll(aroundKey(first));
            queries.addAll(aroundKey(last));
        }
        for (var i = 0; i < 10; i++) {
            var query = new byte[random.nextInt(40)];
            random.nextBytes(query);
            queries.add(query);
        }
        return queries;
    }

    /// {@return `key`, the key just above it, keys just below it, and its ending cut off}
    private static List<byte[]> aroundKey(byte[] key) {
        var around = new ArrayList<byte[]>();
        around.add(key);
        around.add(Arrays.copyOf(key, key.length + 1));
        if (key.length > 0) {
            around.add(Arrays.copyOf(key, key.length - 1));
            var below = key.clone();
            below[below.length - 1]--;
            around.add(below);
            var above = key.clone();
            above[above.length - 1]++;
            around.add(above);
        }
        return around;
    }

    /// {@return the first of `keys`, from `from`, at or after `query`, or their count}
    private static int lowerBound(List<byte[]> keys, byte[] query, int from) {
        for (var i = from; i < keys.size(); i++) {
            if (UNSIGNED.compare(keys.get(i), query) >= 0) {
                return i;
            }
        }
        return keys.size();
    }

    /// {@return the first of `keys` after `query`, or their count}
    private static int upperBound(List<byte[]> keys, byte[] query) {
        for (var i = 0; i < keys.size(); i++) {
            if (UNSIGNED.compare(keys.get(i), query) > 0) {
                return i;
            }
        }
        return keys.size();
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes.clone()).asReadOnly();
    }

    private static byte[] value(Random random) {
        var value = new byte[random.nextInt(40)];
        random.nextBytes(value);
        return value;
    }

    private static PageFile open(Path directory, boolean encrypted, long cacheBytes) throws IOException {
        var cipher = new PageCipher(encrypted ? new SecretKeySpec(KEY, "AES") : null);
        return PageFile.open(directory, cipher, cacheBytes, new HeapPool(1 << 20));
    }

    private static TreeRoot build(PageFile pages, TreeMap<byte[], byte[]> model, Random random, int keys) throws IOException {
        return build(pages, model, random, keys, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet()));
    }

    /// {@return the root of a new tree of `keys` keys in the manner of a chat's messages, also put into `model`}
    private static TreeRoot build(PageFile pages, TreeMap<byte[], byte[]> model, Random random, int keys, FreeSpace space) throws IOException {
        for (var i = 0; i < keys; i++) {
            var chat = random.nextInt(40);
            var id = "%04d3EB0%016x".formatted(chat, random.nextLong()).getBytes(StandardCharsets.ISO_8859_1);
            model.put(id, value(random));
        }
        var ops = new ArrayList<TreeUpdate.Op>();
        for (var entry : model.entrySet()) {
            ops.add(new TreeUpdate.Op(segment(entry.getKey()), segment(entry.getValue())));
        }
        var writer = pages.writer(random.nextLong());
        var root = new TreeUpdate(pages, writer, space).apply(null, ops, List.of());
        writer.flush();
        return root;
    }

    /// {@return a key of `keys`, or one near it}
    private static byte[] query(List<byte[]> keys, Random random) {
        var key = keys.get(random.nextInt(keys.size()));
        var around = aroundKey(key);
        return around.get(random.nextInt(around.size()));
    }

    private static void assertLookup(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] key) {
        var hit = TreeSearch.find(pages, root, segment(key));
        var expected = model.get(key);
        if (expected == null) {
            assertNull(hit, () -> "found " + Arrays.toString(key));
        } else {
            assertNotNull(hit, () -> "did not find " + Arrays.toString(key));
            assertArrayEquals(expected, TreeSearch.value(pages, hit.unit(), hit.entry()).toArray(ValueLayout.JAVA_BYTE));
        }
    }

    /// Checks up to `limit` entries of a cursor over a range, and its end if it reaches it.
    private static void assertCursor(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to, boolean descending, int limit) {
        var range = from == null ? model.headMap(to, false) : to == null ? model.tailMap(from, true) : model.subMap(from, true, to, false);
        var ordered = new ArrayList<>(descending ? range.descendingMap().keySet() : range.keySet());
        var cursor = new TreeCursor(pages, root, from == null ? null : segment(from), to == null ? null : segment(to), descending);
        for (var i = 0; i < Math.min(limit, ordered.size()); i++) {
            assertTrue(cursor.next(), "the cursor ended early");
            assertArrayEquals(ordered.get(i), cursor.key().toArray(ValueLayout.JAVA_BYTE));
        }
        if (ordered.size() <= limit) {
            assertFalse(cursor.next(), "the cursor went past the range");
        }
    }

    /// Reads the tree under `ref` past the cache, applying `check` to every node.
    private static void walk(PageFile pages, PageRef ref, java.util.function.UnaryOperator<byte[]> check) {
        var unit = check.apply(pages.read(ref, PageFile.NODE, false));
        if (!TreeNodes.isLeaf(unit)) {
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                walk(pages, TreeNodes.childRef(unit, i), check);
            }
        }
    }

    private static void walk(PageFile pages, PageRef ref, List<PageRef> refs) {
        refs.add(ref);
        var unit = pages.read(ref, PageFile.NODE, false);
        if (!TreeNodes.isLeaf(unit)) {
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                walk(pages, TreeNodes.childRef(unit, i), refs);
            }
        }
    }

    /// {@return the nodes of the tree under `root` the cache holds with an index}
    private static int indexed(PageFile pages, TreeRoot root) {
        var refs = new ArrayList<PageRef>();
        walk(pages, root.ref(), refs);
        var indexed = 0;
        for (var ref : refs) {
            var unit = pages.cached(ref, PageFile.NODE);
            indexed += unit != null && NodeIndex.isPresent(unit) ? 1 : 0;
        }
        return indexed;
    }

    /// Takes about half of the leaves of the tree under `root` out of the cache, and puts them back without an index.
    private static void evictLeaves(PageFile pages, TreeRoot root, Random random) {
        var refs = new ArrayList<PageRef>();
        walk(pages, root.ref(), refs);
        for (var ref : refs) {
            if (random.nextBoolean() && !ref.isRecord()) {
                pages.cache().remove(ref.page());
                pages.readToReplace(ref, PageFile.NODE, true);
            }
        }
    }

    private static PageRef firstLeaf(PageFile pages, TreeRoot root) {
        var ref = root.ref();
        var unit = pages.read(ref, PageFile.NODE, false);
        while (!TreeNodes.isLeaf(unit)) {
            ref = TreeNodes.childRef(unit, 0);
            unit = pages.read(ref, PageFile.NODE, false);
        }
        return ref;
    }

    /// {@return the delta records the tree under `root` refers to}
    private static List<PageRef> records(PageFile pages, TreeRoot root) {
        var refs = new ArrayList<PageRef>();
        walk(pages, root.ref(), refs);
        refs.removeIf(ref -> !ref.isRecord());
        return refs;
    }
}
