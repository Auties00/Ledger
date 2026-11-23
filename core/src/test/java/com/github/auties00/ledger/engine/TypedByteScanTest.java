package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.WriteBatch;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Typed scans of byte array key spaces, which copy each key and value of the tree or of a run straight out of the
/// array that holds it ([StoredCursor#keyBytes()], [StoredCursor#valueBytes()]): they return what a scan of the
/// encoded entries and the point reads return, in arrays of their own, over inline values, blobs of the data file read
/// cold, read ahead, recalled and past the units a cursor admits, values in the file of large values, a replayed run
/// over the tree with its removals, and range removals newer than the tree; and a cursor that copies leaves the page
/// cache, its screening and its reads ahead exactly as one that hands out segments.
class TypedByteScanTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    // Byte array values under text keys, which the scan decodes
    private static final Keyspace<String, byte[]> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.bytes());
    private static final int LARGE_VALUES = 12 << 10;
    // Inline, at the log's threshold, in blobs of the data file of one and two pages, and in the file of large values
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 700, 3_000, 6_000, 20_000};
    private static final int MESSAGE_COUNT = 700;

    private static LiveLedger open(Path directory, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(NAMES).commitWindow(Duration.ZERO)
                .largeValues(MESSAGES, LARGE_VALUES).largeValues(NAMES, LARGE_VALUES))
                .segmentSize(64 << 10)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static String name(int message) {
        return "n%05d".formatted(message);
    }

    private static byte[] value(int message, int generation) {
        var value = new byte[LENGTHS[Math.floorMod(message * 5 + generation, LENGTHS.length)]];
        for (var i = 0; i < value.length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i * 7);
        }
        return value;
    }

    /// What the store should hold, by message.
    private static final class Expected {
        final TreeMap<Integer, byte[]> messages = new TreeMap<>();

        void put(WriteBatch batch, int message, int generation) {
            var value = value(message, generation);
            batch.put(MESSAGES, key(message), value).put(NAMES, name(message), value);
            messages.put(message, value);
        }

        void delete(WriteBatch batch, int message) {
            batch.delete(MESSAGES, key(message)).delete(NAMES, name(message));
            messages.remove(message);
        }
    }

    @ParameterizedTest(name = "cold reads: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("typed scans of byte arrays return what encoded scans and point reads return, in arrays of their own, over the tree, a replayed run, range removals and memory")
    void sameAsEncoded(boolean cold, @TempDir Path directory) throws Exception {
        var expected = new Expected();
        try (var ledger = open(directory, Long.MAX_VALUE)) {
            for (var first = 0; first < MESSAGE_COUNT; first += 50) {
                var batch = ledger.newBatch();
                for (var message = first; message < first + 50; message++) {
                    expected.put(batch, message, 0);
                }
                ledger.commit(batch).awaitDurable();
            }
            ledger.checkpoint();
            // A tail the open replays into a run over the tree: overwrites, removals of the tree's keys and of keys
            // never written, and new keys
            var batch = ledger.newBatch();
            for (var message = 0; message < MESSAGE_COUNT + 60; message += 7) {
                if (message % 3 == 0) {
                    expected.delete(batch, message);
                } else {
                    expected.put(batch, message, 1);
                }
            }
            ledger.commit(batch).awaitDurable();
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, 0)) {
            assertNotNull(ledger.state(MESSAGES).run, "the open replayed no run");
            ledger.pagesForTests().forceColdReadsForTests(cold);
            verifyRepeatedly(ledger, expected);
            // Range removals newer than the tree, and versions in memory over both
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key(100), key(180)))
                    .deleteRange(NAMES, KeyRange.between(name(100), name(180))));
            expected.messages.subMap(100, 180).clear();
            var writes = ledger.newBatch();
            for (var message = 150; message < 260; message += 9) {
                expected.put(writes, message, 2);
            }
            ledger.commit(writes);
            assertTrue(ledger.state(MESSAGES).removals.length > 0, "no range removal is newer than the tree");
            verifyRepeatedly(ledger, expected);
            try (var snapshot = ledger.snapshot()) {
                verify(snapshot, expected);
            }
            ledger.update(transaction -> verify(transaction, expected));
            ledger.checkpoint();
            assertNull(ledger.state(MESSAGES).run, "the run outlived the checkpoint");
            verifyRepeatedly(ledger, expected);
        }
        try (var ledger = open(directory, 0)) {
            ledger.pagesForTests().forceColdReadsForTests(cold);
            verifyRepeatedly(ledger, expected);
        }
    }

    /// Verifies `ledger` against `expected` from an empty cache and four times again, so the later scans read the
    /// units the cache refused, recalled, and at last cached.
    private static void verifyRepeatedly(LiveLedger ledger, Expected expected) {
        ledger.clearPageCacheForTests();
        for (var round = 0; round < 5; round++) {
            verify(ledger, expected);
        }
    }

    /// Checks every typed scan of `view` against `expected`, a scan of the encoded entries and point reads.
    private static void verify(View view, Expected expected) {
        var all = expected.messages;
        // Whole scans both ways, whose arrays the test then changes: the next scan must not see that
        for (var descending : new boolean[]{false, true}) {
            var model = descending ? all.descendingMap() : all;
            var range = descending ? KeyRange.<byte[]>all().reversed() : KeyRange.<byte[]>all();
            var scanned = new ArrayList<byte[][]>();
            view.forEach(MESSAGES, range, (key, value) -> scanned.add(new byte[][]{key, value}));
            assertEntries(model, scanned, "forEach descending " + descending);
            var encoded = new ArrayList<byte[][]>();
            view.forEachEncoded(MESSAGES, range, (key, value) -> encoded.add(new byte[][]{
                    key.toArray(ValueLayout.JAVA_BYTE), value.toArray(ValueLayout.JAVA_BYTE)}));
            assertEquals(scanned.size(), encoded.size());
            for (var i = 0; i < scanned.size(); i++) {
                assertArrayEquals(encoded.get(i)[0], scanned.get(i)[0], "forEachEncoded key " + i);
                assertArrayEquals(encoded.get(i)[1], scanned.get(i)[1], "forEachEncoded value " + i);
            }
            for (var entry : scanned) {
                assertArrayEquals(entry[1], view.get(MESSAGES, entry[0]).orElseThrow(), "get");
                entry[0][0] ^= 0x55;
                if (entry[1].length > 0) {
                    entry[1][entry[1].length - 1] ^= 0x55;
                }
            }
            var again = new ArrayList<byte[][]>();
            view.forEach(MESSAGES, range, (key, value) -> again.add(new byte[][]{key, value}));
            assertEntries(model, again, "forEach after changing the arrays of a scan, descending " + descending);
            var names = new ArrayList<byte[][]>();
            view.forEach(NAMES, descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all(),
                    (name, value) -> names.add(new byte[][]{key(Integer.parseInt(name.substring(1))), value}));
            assertEntries(model, names, "names descending " + descending);
        }
        // Ranges, limits, early stops and first entries, both ways
        for (var from = -20; from < MESSAGE_COUNT + 80; from += 113) {
            var to = from + 157;
            var window = all.subMap(from, true, to, false);
            for (var descending : new boolean[]{false, true}) {
                var model = descending ? window.descendingMap() : window;
                var range = KeyRange.between(key(Math.max(0, from)), key(to));
                range = descending ? range.reversed() : range;
                if (from < 0) {
                    range = descending ? KeyRange.below(key(to)).reversed() : KeyRange.below(key(to));
                }
                var message = "from " + from + " to " + to + " descending " + descending;
                for (var limit : new int[]{1, 7, 100, Integer.MAX_VALUE}) {
                    var entries = view.range(MESSAGES, range, limit);
                    var listed = new ArrayList<byte[][]>();
                    for (Entry<byte[], byte[]> entry : entries) {
                        listed.add(new byte[][]{entry.key(), entry.value()});
                    }
                    assertEntries(head(model, limit), listed, message + " limit " + limit);
                    var stopped = new ArrayList<byte[][]>();
                    var stop = limit;
                    view.forEach(MESSAGES, range, (key, value) -> {
                        stopped.add(new byte[][]{key, value});
                        return stopped.size() < stop;
                    });
                    assertEntries(head(model, limit), stopped, message + " stopped at " + limit);
                }
                var first = view.first(MESSAGES, range);
                if (model.isEmpty()) {
                    assertTrue(first.isEmpty(), message);
                } else {
                    assertArrayEquals(key(model.firstKey()), first.orElseThrow().key(), message);
                    assertArrayEquals(model.firstEntry().getValue(), first.orElseThrow().value(), message);
                }
            }
        }
    }

    private static NavigableMap<Integer, byte[]> head(NavigableMap<Integer, byte[]> model, int limit) {
        var head = new TreeMap<Integer, byte[]>(model.comparator());
        for (var entry : model.entrySet()) {
            if (head.size() == limit) {
                break;
            }
            head.put(entry.getKey(), entry.getValue());
        }
        return head;
    }

    private static void assertEntries(NavigableMap<Integer, byte[]> model, List<byte[][]> entries, String message) {
        assertEquals(model.size(), entries.size(), message + ": entries");
        var index = 0;
        for (var entry : model.entrySet()) {
            assertArrayEquals(key(entry.getKey()), entries.get(index)[0], message + ": key of message " + entry.getKey());
            assertArrayEquals(entry.getValue(), entries.get(index++)[1], message + ": value of message " + entry.getKey());
        }
    }

    @ParameterizedTest(name = "cold reads: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a cursor that copies its keys and values out leaves the cache, its screening, recalls and reads ahead as one that hands out segments")
    void sameCacheAsSegments(boolean cold, @TempDir Path directory) throws Exception {
        var source = directory.resolve("source");
        TreeRoot root;
        List<PageRef> units;
        try (var tree = new SourceTree(source)) {
            root = tree.root;
            units = tree.units();
        }
        assertTrue(units.size() > 4 * TreeCursor.ADMITTED, "too few units: " + units.size());
        for (var attempt = 0; ; attempt++) {
            var collections = collections();
            var states = run(directory.resolve("attempt" + attempt), source, root, units, cold);
            if (states.segments().equals(states.bytes())) {
                return;
            }
            // A collection may have taken an array one cache refused and not yet the other's, which one recalls and the
            // other reads from the file
            if (collections == collections() || attempt == 5) {
                for (var i = 0; i < SCANS.size(); i++) {
                    assertEquals(states.segments().get(i), states.bytes().get(i), "the cache after scan " + i);
                }
            }
        }
    }

    private static long collections() {
        var count = 0L;
        for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
            count += Math.max(0, bean.getCollectionCount());
        }
        return count;
    }

    /// The scans of one run, each screened as a paged key space's and expecting to take `wanted` entries.
    private record Scan(int from, int to, boolean descending, long wanted, int limit) {
    }

    private static final List<Scan> SCANS = List.of(
            new Scan(-1, -1, false, Long.MAX_VALUE, Integer.MAX_VALUE),
            new Scan(100, 2_400, true, 60, 60),
            new Scan(900, -1, false, 50, 50),
            new Scan(-1, -1, true, Long.MAX_VALUE, Integer.MAX_VALUE),
            new Scan(100, 2_400, true, 60, 60),
            new Scan(100, 2_400, true, 60, 60),
            new Scan(100, 2_400, true, 60, 60),
            new Scan(900, -1, false, 50, 50),
            new Scan(900, -1, false, 50, 50),
            new Scan(900, -1, false, 50, 50));

    /// The states of two caches after each of [#SCANS], one read through cursors that hand out segments and one
    /// through cursors that copy.
    private record States(List<CacheState> segments, List<CacheState> bytes) {
    }

    /// {@return the states of the caches of two copies of the tree in `source` after each of [#SCANS], scanned in
    /// lockstep, entry by entry, so a collection runs between the same reads of both}
    private static States run(Path directory, Path source, TreeRoot root, List<PageRef> units, boolean cold)
            throws IOException {
        var segments = Copy.of(directory.resolve("segments"), source, cold);
        try (segments; var bytes = Copy.of(directory.resolve("bytes"), source, cold)) {
            var states = new States(new ArrayList<>(), new ArrayList<>());
            for (var scan : SCANS) {
                var from = scan.from() < 0 ? null : SourceTree.key(scan.from());
                var to = scan.to() < 0 ? null : SourceTree.key(scan.to());
                var segmentCursor = new TreeCursor(segments.pages, root, from, to, scan.descending(), scan.wanted(), true);
                var bytesCursor = new TreeCursor(bytes.pages, root, from, to, scan.descending(), scan.wanted(), true);
                try {
                    for (var taken = 0; taken < scan.limit(); taken++) {
                        var next = segmentCursor.next();
                        assertEquals(next, bytesCursor.next(), "the cursors ended apart");
                        if (!next) {
                            break;
                        }
                        segmentCursor.took();
                        bytesCursor.took();
                        var key = segmentCursor.key().toArray(ValueLayout.JAVA_BYTE);
                        var value = segmentCursor.value().toArray(ValueLayout.JAVA_BYTE);
                        assertArrayEquals(key, bytesCursor.keyBytes());
                        assertArrayEquals(value, bytesCursor.valueBytes());
                    }
                } finally {
                    segmentCursor.release();
                    bytesCursor.release();
                }
                states.segments().add(CacheState.of(segments.cache, units, segments.pool));
                states.bytes().add(CacheState.of(bytes.cache, units, bytes.pool));
            }
            return states;
        }
    }

    /// A copy of the tree's files, read through a cache and pool of its own.
    private record Copy(PageFile pages, PageCache cache, HeapPool pool) implements AutoCloseable {
        static Copy of(Path directory, Path source, boolean cold) throws IOException {
            Files.createDirectories(directory);
            try (var files = Files.list(source)) {
                for (var file : files.toList()) {
                    Files.copy(file, directory.resolve(file.getFileName()));
                }
            }
            var pool = new HeapPool(16L << 20, false);
            pool.holdCollectableForTests(true);
            // Smaller than the tree, so reads past the units the cursor admits find the cache full at times
            var cache = PageCache.isolated(3L << 20);
            var pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(KEY, "AES")), cache, pool);
            cache.pinRefusalsForTests();
            pages.forceColdReadsForTests(cold);
            return new Copy(pages, cache, pool);
        }

        @Override
        public void close() {
            pages.close();
        }
    }

    /// What a scan left in the cache: its size, clock and refusals, which of `units` it holds, used since the clock
    /// passed and saw, and the bytes the pool keeps.
    private record CacheState(long size, int clock, int refusals, int refused, String held, String referenced,
                              String seen, long pooled) {
        static CacheState of(PageCache cache, List<PageRef> units, HeapPool pool) {
            var held = new StringBuilder();
            var referenced = new StringBuilder();
            var seen = new StringBuilder();
            for (var unit : units) {
                held.append(cache.peek(unit) == null ? '0' : '1');
                referenced.append(cache.referencedForTests(unit) ? '1' : '0');
                seen.append(cache.seen(unit) ? '1' : '0');
            }
            return new CacheState(cache.size(), cache.clockLengthForTests(), cache.refusalsForTests(),
                    cache.refusedForTests(), held.toString(), referenced.toString(), seen.toString(),
                    pool.pooledBytesForTests() + pool.collectableBytesForTests());
        }
    }

    /// A tree of inline values and blobs of the data file written straight through a [TreeUpdate].
    private static final class SourceTree implements AutoCloseable {
        final PageFile pages;
        final TreeRoot root;

        SourceTree(Path directory) throws IOException {
            Files.createDirectories(directory);
            pages = PageFile.open(directory, new PageCipher(new SecretKeySpec(KEY, "AES")), PageCache.isolated(64L << 20),
                    new HeapPool(16L << 20, false));
            var random = new Random(41);
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 3_000; i++) {
                var length = switch (random.nextInt(10)) {
                    case 0, 1, 2 -> 2_500 + random.nextInt(9_000);
                    case 3 -> 0;
                    default -> 10 + random.nextInt(300);
                };
                var value = new byte[length];
                random.nextBytes(value);
                ops.add(new TreeUpdate.Op(MemorySegment.ofArray(key(i)).asReadOnly(), MemorySegment.ofArray(value).asReadOnly()));
            }
            var writer = pages.writer(1);
            var update = new TreeUpdate(pages, writer, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet()));
            root = update.apply(null, ops, TreeUpdate.merge(List.of()));
            update.finishDeltas(DeltaTable.EMPTY);
            writer.flush();
        }

        static byte[] key(int i) {
            return "key%06d".formatted(i).getBytes(StandardCharsets.US_ASCII);
        }

        /// {@return every node and blob of the tree}
        List<PageRef> units() {
            var units = new ArrayList<PageRef>();
            collect(root.ref(), units);
            return units;
        }

        private void collect(PageRef ref, List<PageRef> units) {
            units.add(ref);
            var unit = pages.read(ref, PageFile.NODE, false);
            if (TreeNodes.isLeaf(unit)) {
                for (var i = 0; i < TreeNodes.count(unit); i++) {
                    var entry = TreeNodes.entry(unit, i);
                    if (TreeNodes.isBlob(unit, entry)) {
                        units.add(TreeNodes.blobRef(unit, entry));
                    }
                }
                return;
            }
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                collect(TreeNodes.childRef(unit, i), units);
            }
        }

        @Override
        public void close() {
            pages.close();
        }
    }
}
