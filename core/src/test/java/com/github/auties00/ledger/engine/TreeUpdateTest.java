package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The copy-on-write tree on its own, against a sorted map: random rounds of writes and range removals, with every
/// freed page reused at once.
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
                verifyNodes(pages, root);
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
            assertTrue(TreeSearch.contains(pages, root, hot));
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

    @Test
    @DisplayName("a flush on a worker of a fork-join pool seals on no other worker, whose tasks may be waiting for the lock the flush holds")
    void sealsOffTheWorkers(@TempDir Path directory) throws Exception {
        var helpers = ConcurrentHashMap.<Thread>newKeySet();
        var helped = new CountDownLatch(1);
        var waited = new AtomicBoolean();
        var flusher = new AtomicReference<Thread>();
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES")) {
            @Override
            void seal(byte[] plain, int pages, byte[] out, int offset) {
                if (Thread.currentThread() == flusher.get()) {
                    // The first seal of the flushing worker waits for another thread to seal, so a seal forked to its
                    // deque is there for the idle workers to take
                    if (waited.compareAndSet(false, true)) {
                        try {
                            helped.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException exception) {
                            throw new IllegalStateException(exception);
                        }
                    }
                } else {
                    helpers.add(Thread.currentThread());
                    helped.countDown();
                }
                super.seal(plain, pages, out, offset);
            }
        };
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            workers.submit(() -> {
                flusher.set(Thread.currentThread());
                // Past the units from which a flush seals on every processor
                for (var i = 0; i < 256; i++) {
                    writer.write(PageFormat.VALUE, 0, space.allocate(1), 1, new byte[PageFormat.HEADER + 100], 100, false);
                }
                writer.flush();
                return null;
            }).get();
        }
        if (Runtime.getRuntime().availableProcessors() > 1) {
            assertFalse(helpers.isEmpty(), "no other thread sealed");
        }
        for (var helper : helpers) {
            assertTrue(helper.isVirtual(), "sealed on " + helper);
        }
    }

    @Test
    @DisplayName("subtrees updated in parallel at two levels of a tree, on as few workers as a machine of four processors has, leave the tree equal to the model")
    void parallelUpdatesOnFewWorkers(@TempDir Path directory) throws Exception {
        var random = new Random(23);
        var cipher = new PageCipher(new SecretKeySpec(new byte[32], "AES"));
        try (var pages = PageFile.open(directory, cipher, 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            try {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var keys = new ArrayList<byte[]>();
                for (var i = 0; i < 200_000; i++) {
                    keys.add("k%07d".formatted(i).getBytes());
                }
                TreeRoot root = null;
                var deltas = DeltaTable.EMPTY;
                for (var round = 0; round < 12; round++) {
                    var ops = new ArrayList<TreeUpdate.Op>();
                    for (var key : keys) {
                        var value = new byte[100];
                        random.nextBytes(value);
                        model.put(key, value);
                        ops.add(new TreeUpdate.Op(segment(key), segment(value)));
                    }
                    var writer = pages.writer(random.nextLong());
                    var update = new TreeUpdate(pages, writer, space);
                    var before = root;
                    // A pool of its own, so a machine of many processors has only three workers to run into each other
                    root = assertTimeoutPreemptively(Duration.ofMinutes(2),
                            () -> workers.submit(() -> update.apply(before, ops, List.of())).get());
                    deltas = update.finishDeltas(deltas);
                    writer.flush();
                    for (var unit : update.freed()) {
                        space.release(unit.page(), unit.pages());
                    }
                }
                verify(pages, root, model, random, keys);
            } finally {
                // A close would wait for workers a failure of the timeout left stuck
                workers.shutdownNow();
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a tree that held no key is written in two passes into the data file a merge of every write into one list writes, unit for unit, with values on segments, own arrays, in the log and in units of the large value file, past the bytes a writer queues before it flushes")
    void newTreeLayout(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 31 : 37);
        var writes = new ArrayList<Write>();
        var forms = Form.values();
        var queued = 0L;
        for (var key : sortedKeys(random, 24_000)) {
            var form = random.nextInt(10) == 0 ? Form.REMOVED : forms[random.nextInt(Form.REMOVED.ordinal())];
            var value = random.nextInt(40) == 0 ? new byte[2_500 + random.nextInt(24_000)] : value(random);
            random.nextBytes(value);
            writes.add(new Write(key, value, form));
            if (form != Form.REMOVED && form != Form.EXTERNAL && !TreeNodes.LeafEntry.fitsInline(key.length, value.length)) {
                queued += (long) PageFormat.pagesFor(value.length) * PageFormat.SIZE;
            }
        }
        // More than the 8 MiB a writer queues before it flushes, in value units alone
        assertTrue(queued > 8 << 20, "the value units take " + queued + " bytes");
        try (var log = new Log(directory.resolve("1.log"))) {
            for (var write : writes) {
                log.place(write.value());
            }
            log.write();
            var streamed = layOut(directory.resolve("streamed"), encrypted, ops(writes, log), false, null);
            var merged = layOut(directory.resolve("merged"), encrypted, ops(writes, log), true, null);
            assertEquals(merged.root(), streamed.root());
            assertEquals(merged.allocated(), streamed.allocated(), "the pages taken, in order");
            assertEquals(-1, Arrays.mismatch(merged.file(), streamed.file()), "the data files differ");
        }
    }

    @Test
    @DisplayName("values a spill moves to the log or to arrays of their own between the two passes of a new tree are written where a merge of every write into one list puts them, and read back")
    void spillBetweenPasses(@TempDir Path directory) throws Exception {
        var random = new Random(43);
        var writes = new ArrayList<Write>();
        for (var key : sortedKeys(random, 20_000)) {
            var value = switch (random.nextInt(4)) {
                case 0 -> new byte[random.nextInt(SpillQueue.LOG_VALUE_MIN)];
                case 1 -> new byte[SpillQueue.LOG_VALUE_MIN + random.nextInt(1_900)];
                case 2 -> new byte[2_500 + random.nextInt(6_000)];
                default -> value(random);
            };
            random.nextBytes(value);
            writes.add(new Write(key, value, random.nextBoolean() ? Form.SEGMENT : Form.OWN));
        }
        var model = new TreeMap<byte[], byte[]>(UNSIGNED);
        for (var write : writes) {
            model.put(write.key(), write.value());
        }
        try (var log = new Log(directory.resolve("1.log"))) {
            for (var write : writes) {
                log.place(write.value());
            }
            log.write();
            var ops = ops(writes, log);
            var toLog = new AtomicInteger();
            var toArray = new AtomicInteger();
            Runnable spill = () -> {
                for (var i = 0; i < ops.size(); i += 2) {
                    var version = ops.get(i).version();
                    if (log.spill(version, writes.get(i).value())) {
                        (version.sourceForTests() instanceof LogValue ? toLog : toArray).incrementAndGet();
                    }
                }
            };
            var streamed = layOut(directory.resolve("streamed"), true, ops, false, spill);
            assertTrue(toLog.get() > 0, "no value moved to the log");
            assertTrue(toArray.get() > 0, "no value moved to an array of its own");
            var merged = layOut(directory.resolve("merged"), true, ops(writes, log), true, null);
            assertEquals(merged.root(), streamed.root());
            assertEquals(-1, Arrays.mismatch(merged.file(), streamed.file()), "the data files differ");
            var keys = new ArrayList<>(model.keySet());
            try (var pages = PageFile.open(directory.resolve("streamed"), cipher(true), 1 << 20, new HeapPool(1 << 20))) {
                verify(pages, streamed.root(), model, random, keys);
                verifyNodes(pages, streamed.root());
            }
        }
    }

    @Test
    @DisplayName("writes that only remove keys, with or without removed ranges, leave a tree that held no key empty, and take and write no page")
    void newTreeOfRemovals(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(3);
            var update = new TreeUpdate(pages, writer, space);
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 100; i++) {
                ops.add(new TreeUpdate.Op(segment("k%03d".formatted(i).getBytes()), null));
            }
            var range = new TreeUpdate.Range(segment("k010".getBytes()), segment("k050".getBytes()));
            assertNull(update.apply(null, ops, List.of(range)));
            assertNull(update.apply(null, ops, List.of()));
            assertNull(update.apply(null, List.of(), List.of(new TreeUpdate.Range(null, null))));
            writer.flush();
            assertTrue(update.allocated().isEmpty(), "pages taken: " + update.allocated());
            assertEquals(PageFormat.FIRST_UNIT_PAGE, space.pageCount());
        }
        assertEquals((long) PageFormat.FIRST_UNIT_PAGE * PageFormat.SIZE, Files.size(directory.resolve(PageFile.FILE)));
    }

    @Test
    @DisplayName("every node an update writes, in subtrees updated in parallel, as delta records and in merges of underfull nodes too, is known above by a copy of its first key that shares no array with the node's plaintext")
    void firstKeysKeepNoUnit(@TempDir Path directory) throws Exception {
        var random = new Random(41);
        try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(1 << 20));
             var workers = new ForkJoinPool(3)) {
            try {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<byte[], byte[]>(UNSIGNED);
                var keys = new ArrayList<byte[]>();
                for (var i = 0; i < 60_000; i++) {
                    keys.add("k%07d".formatted(i).getBytes());
                }
                var failures = new ConcurrentLinkedQueue<String>();
                var leaves = new AtomicInteger();
                var branches = new AtomicInteger();
                var records = new AtomicInteger();
                TreeRoot root = null;
                var deltas = DeltaTable.EMPTY;
                for (var round = 0; round < 8; round++) {
                    var ops = new ArrayList<TreeUpdate.Op>();
                    var ranges = new ArrayList<TreeUpdate.Range>();
                    if (round == 6) {
                        // A range that ends inside a leaf, leaving it under a quarter full for its neighbour to take
                        var from = keys.get(20_003);
                        var to = keys.get(30_011);
                        ranges.add(new TreeUpdate.Range(segment(from), segment(to)));
                        model.subMap(from, true, to, false).clear();
                    }
                    for (var key : keys) {
                        // All keys at first, then a few, as delta records take them, then most
                        var writes = round < 3 || round == 7 || random.nextInt(60) == 0;
                        if (!writes) {
                            continue;
                        }
                        if (round > 2 && random.nextInt(8) == 0) {
                            model.remove(key);
                            ops.add(new TreeUpdate.Op(segment(key), null));
                            continue;
                        }
                        var value = new byte[60 + random.nextInt(80)];
                        random.nextBytes(value);
                        model.put(key, value);
                        ops.add(new TreeUpdate.Op(segment(key), segment(value)));
                    }
                    var writer = pages.writer(random.nextLong());
                    var update = new TreeUpdate(pages, writer, space);
                    update.writtenHookForTests(child -> {
                        if (child.ref().isRecord()) {
                            records.incrementAndGet();
                            return;
                        }
                        var unit = writer.staged(child.ref());
                        if (unit == null) {
                            // Flushed at once, as the writer had queued as much as it holds
                            return;
                        }
                        (child.level() == 0 ? leaves : branches).incrementAndGet();
                        if (MemorySegment.ofArray(unit).asOverlappingSlice(child.firstKey()).isPresent()) {
                            failures.add("the first key of a node of level " + child.level() + " views its plaintext");
                        }
                        var first = child.level() == 0 ? TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0)) : TreeNodes.childKey(unit, 0);
                        if (Segments.compare(first, child.firstKey()) != 0) {
                            failures.add("a node of level " + child.level() + " is known by another first key than its own");
                        }
                    });
                    var before = root;
                    root = workers.submit(() -> update.apply(before, ops, ranges)).get();
                    deltas = update.finishDeltas(deltas);
                    writer.flush();
                    for (var unit : update.freed()) {
                        space.release(unit.page(), unit.pages());
                    }
                    verifyNodes(pages, root);
                }
                verify(pages, root, model, random, keys);
                verifyPages(pages, space, root, deltas);
                assertTrue(failures.isEmpty(), () -> String.join("\n", failures));
                assertTrue(leaves.get() > 0, "no leaf was checked");
                assertTrue(branches.get() > 0, "no branch was checked");
                assertTrue(records.get() > 0, "no leaf was written as a delta record");
            } finally {
                workers.shutdownNow();
            }
        }
    }

    /// How the version of a write holds its value.
    private enum Form {
        /// A slice of a segment, as a batch leaves it.
        SEGMENT,
        /// An array of its own.
        OWN,
        /// A place in the log.
        LOGGED,
        /// A unit of the large value file, which the tree takes as it is.
        EXTERNAL,
        /// No value: the write removes its key.
        REMOVED
    }

    private record Write(byte[] key, byte[] value, Form form) {
    }

    private record Laid(TreeRoot root, List<PageRef> allocated, byte[] file) {
    }

    /// {@return `count` distinct random keys of 1 to 40 bytes, in order}
    private static List<byte[]> sortedKeys(Random random, int count) {
        var keys = new TreeSet<byte[]>(UNSIGNED);
        while (keys.size() < count) {
            var key = new byte[1 + random.nextInt(40)];
            random.nextBytes(key);
            keys.add(key);
        }
        return new ArrayList<>(keys);
    }

    /// {@return the states of `writes`, with versions of their own, which hold their values as each write's form says}
    private static List<TreeUpdate.Op> ops(List<Write> writes, Log log) {
        var ops = new ArrayList<TreeUpdate.Op>(writes.size());
        for (var i = 0; i < writes.size(); i++) {
            var write = writes.get(i);
            var key = new Chain(segment(write.key()), 0);
            var value = write.value();
            var version = switch (write.form()) {
                case SEGMENT -> new Version(0, segment(value), null, null);
                case OWN -> Version.ofOwn(0, value.clone(), null, null, null);
                case LOGGED -> log.logged(value);
                case EXTERNAL -> Version.ofOwn(0, value.clone(), null, null,
                        new PageRef(1 << 24 | i, PageFormat.pagesFor(value.length), 5, i));
                case REMOVED -> null;
            };
            ops.add(new TreeUpdate.Op(key, version, null));
        }
        return ops;
    }

    /// {@return the tree `ops` leave in a new data file in `directory`, written in two passes, or from one merged list
    /// if `merged`, with `between` run between the passes if it is not `null`}
    private static Laid layOut(Path directory, boolean encrypted, List<TreeUpdate.Op> ops, boolean merged, Runnable between)
            throws Exception {
        Files.createDirectories(directory);
        TreeRoot root;
        List<PageRef> allocated;
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(42);
            var update = new TreeUpdate(pages, writer, space);
            if (between != null) {
                update.passHookForTests(between);
            }
            root = merged ? update.applyMergedForTests(ops) : update.apply(null, ops, List.of());
            writer.flush();
            allocated = List.copyOf(update.allocated());
            verifyNodes(pages, root);
        }
        return new Laid(root, allocated, Files.readAllBytes(directory.resolve(PageFile.FILE)));
    }

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
    }

    /// A segment of a log without encryption that holds values at places of their own, from which versions read them
    /// back as a store's do once their values left the heap.
    private static final class Log implements AutoCloseable {
        private final Path path;
        private final SpillQueue queue;
        private final LogSegmentView view;
        private final ByteArrayOutputStream bytes;
        // Where each value's frame starts, by the value's array
        private final IdentityHashMap<byte[], Long> places;

        Log(Path path) {
            this.path = path;
            this.queue = new SpillQueue(0, false);
            this.view = queue.segment(1, path, null);
            this.bytes = new ByteArrayOutputStream();
            this.places = new IdentityHashMap<>();
        }

        /// Places `value` as the body of a frame of its own.
        void place(byte[] value) {
            places.put(value, (long) bytes.size());
            bytes.writeBytes(new byte[LogWriter.FRAME_HEADER]);
            bytes.writeBytes(value);
        }

        /// Writes the values placed to the segment's file.
        void write() throws IOException {
            Files.write(path, bytes.toByteArray());
        }

        /// {@return a version of `value`, placed, that holds it in the log}
        Version logged(byte[] value) {
            var crypto = queue.takeCrypto();
            try {
                return Version.ofLogged(0, crypto.logValue(view, places.get(value), 0, 0, value, 0, value.length), value.length);
            } finally {
                queue.giveBack(crypto);
            }
        }

        /// Moves the value of `version`, the bytes of `value`, placed, off its segment or array, as a spill does.
        ///
        /// @return whether it moved
        boolean spill(Version version, byte[] value) {
            var crypto = queue.takeCrypto();
            try {
                return version.spill(crypto, view, places.get(value), 0, 0, null);
            } finally {
                queue.giveBack(crypto);
            }
        }

        @Override
        public void close() {
            view.retire();
        }
    }

    /// Checks what each branch records of its children against the nodes themselves, and each node against its limits.
    static void verifyNodes(PageFile pages, TreeRoot root) {
        if (root != null) {
            var subtree = verifyNode(pages, root.ref());
            assertEquals(root.count(), subtree.count(), "the root counts wrong");
            assertEquals(root.blobs(), subtree.blobs(), "the root flags its blobs wrong");
        }
    }

    private static Subtree verifyNode(PageFile pages, PageRef ref) {
        var unit = pages.read(ref, PageFile.NODE, false);
        assertTrue(TreeNodes.fill(unit) <= TreeNodes.CAPACITY, "a node passes its capacity");
        assertTrue(PageFormat.fits(1, Bytes.i32(unit, PageFormat.LENGTH)), "a node passes its page");
        assertEquals(PageFormat.INDEXED, unit[PageFormat.FORMAT], "a node has no search index");
        assertTrue(TreeNodes.count(unit) > 0, "a node is empty");
        if (TreeNodes.isLeaf(unit)) {
            var blobs = false;
            byte[] previous = null;
            for (var entry : TreeNodes.leafEntries(unit)) {
                blobs |= entry.blob() != null;
                var key = bytes(entry.key());
                assertTrue(previous == null || UNSIGNED.compare(previous, key) < 0, "a leaf is out of order");
                previous = key;
            }
            return new Subtree(bytes(TreeNodes.leafKey(unit, TreeNodes.entry(unit, 0))), TreeNodes.count(unit), blobs);
        }
        var count = 0L;
        var blobs = false;
        byte[] first = null;
        byte[] previous = null;
        for (var child : TreeNodes.children(unit)) {
            var subtree = verifyNode(pages, child.ref());
            assertArrayEquals(subtree.firstKey(), bytes(child.firstKey()), "a branch names the wrong first key");
            assertEquals(subtree.count(), child.count(), "a branch counts a child wrong");
            assertEquals(subtree.blobs(), child.blobs(), "a branch flags a child's blobs wrong");
            assertTrue(previous == null || UNSIGNED.compare(previous, subtree.firstKey()) < 0, "a branch is out of order");
            first = first == null ? subtree.firstKey() : first;
            previous = subtree.firstKey();
            count += subtree.count();
            blobs |= subtree.blobs();
        }
        return new Subtree(first, count, blobs);
    }

    private record Subtree(byte[] firstKey, long count, boolean blobs) {
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

    static void verify(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, Random random, List<byte[]> keys) {
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
            var expected = model.get(key);
            if (root == null) {
                assertNull(expected);
                continue;
            }
            assertEquals(expected != null, TreeSearch.contains(pages, root, segment(key)));
            var unit = TreeSearch.leaf(pages, root, key);
            var entry = TreeSearch.entryOf(unit, key);
            if (expected == null) {
                assertEquals(-1, entry);
            } else {
                assertArrayEquals(expected, bytes(TreeSearch.value(pages, unit, entry)));
                if (TreeNodes.isInline(unit, entry)) {
                    assertArrayEquals(expected, TreeNodes.inlineValueBytes(unit, entry));
                }
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
            var cursor = new TreeCursor(pages, root, from, to, descending);
            for (var entry : ordered) {
                assertTrue(cursor.next(), "the cursor ended early");
                assertArrayEquals(entry.getKey(), bytes(cursor.key()));
                assertArrayEquals(entry.getValue(), bytes(cursor.value()));
            }
            assertFalse(cursor.next(), "the cursor went past the range");
        }
    }

    /// {@return the delta records the tree refers to}
    static int verifyPages(PageFile pages, FreeSpace space, TreeRoot root, DeltaTable deltas) {
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
