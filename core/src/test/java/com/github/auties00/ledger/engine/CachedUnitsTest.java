package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Units held in memory without their padding: those a checkpoint builds as long as their content seal as whole ones
/// would, those the page cache reads open to the start of their plaintext and count only what they keep, and readers
/// that share a cache too small for their tree read every value right while it keeps and evicts units of every length.
class CachedUnitsTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? KEY : null);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("seals a unit whose array ends with its content, or at its tag, as the unit its whole pages hold")
    void sealsShortUnits(boolean encrypted) {
        var random = new Random(encrypted ? 3 : 5);
        // 20 pages pass the slice the cipher is fed in
        for (var pages : new int[]{1, 2, 3, 20}) {
            var capacity = PageFormat.capacity(pages);
            for (var content : new int[]{0, 1, 15, 17, capacity / 2, capacity - 1, capacity}) {
                var whole = new byte[pages * PageFormat.SIZE];
                random.nextBytes(whole);
                // Zeros up to the tag; the bytes of the tag a whole array holds are not sealed
                Arrays.fill(whole, PageFormat.HEADER + content, whole.length - PageFormat.TAG, (byte) 0);
                PageFile.header(whole, PageFormat.VALUE, 0, 7, pages, content, random.nextLong(), random.nextInt());
                var expected = sealed(encrypted, whole, pages, 0, random);
                for (var length : new int[]{PageFormat.HEADER + content, whole.length - PageFormat.TAG}) {
                    var shorter = Arrays.copyOf(whole, length);
                    for (var offset : new int[]{0, 100}) {
                        assertArrayEquals(expected, sealed(encrypted, shorter, pages, offset, random),
                                pages + " pages, " + content + " bytes of content in " + length + " at " + offset);
                    }
                }
            }
        }
        var cipher = cipher(encrypted);
        var unit = new byte[PageFormat.HEADER + 100];
        PageFile.header(unit, PageFormat.VALUE, 0, 7, 1, 101, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> cipher.seal(unit, 1, new byte[PageFormat.SIZE], 0),
                "an array that ends before its content");
        var longer = new byte[2 * PageFormat.SIZE];
        PageFile.header(longer, PageFormat.VALUE, 0, 7, 1, 100, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> cipher.seal(longer, 1, new byte[2 * PageFormat.SIZE], 0),
                "an array longer than its unit");
    }

    /// {@return the unit `plain` seals into, sealed into an array of noise at `offset`, as the pool gives them}
    ///
    /// Each seal takes a cipher of its own: the JDK refuses to encrypt twice under one nonce with the same cipher.
    private static byte[] sealed(boolean encrypted, byte[] plain, int pages, int offset, Random random) {
        var out = new byte[offset + pages * PageFormat.SIZE + 64];
        random.nextBytes(out);
        cipher(encrypted).seal(plain, pages, out, offset);
        return Arrays.copyOfRange(out, offset, offset + pages * PageFormat.SIZE);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("opens a unit kept short of its pages to the start of its plaintext, giving back the array it opens it into")
    void opensShortUnits(boolean encrypted) {
        var cipher = cipher(encrypted);
        var pool = new HeapPool(1 << 20, false);
        var random = new Random(encrypted ? 13 : 17);
        for (var pages : new int[]{1, 2, 3}) {
            var tag = pages * PageFormat.SIZE - PageFormat.TAG;
            var capacity = PageFormat.capacity(pages);
            for (var content : new int[]{0, 100, capacity / 2, capacity}) {
                var plain = new byte[pages * PageFormat.SIZE];
                random.nextBytes(plain);
                Arrays.fill(plain, PageFormat.HEADER + content, plain.length, (byte) 0);
                PageFile.header(plain, PageFormat.LEAF, 0, 9, pages, content, random.nextLong(), random.nextInt());
                var sealed = new byte[100 + pages * PageFormat.SIZE];
                cipher.seal(plain, pages, sealed, 100);
                assertArrayEquals(plain, cipher.open(sealed, 100, pages, pages * PageFormat.SIZE, pool));
                for (var keep : new int[]{PageFormat.HEADER, PageFormat.HEADER + content, tag}) {
                    assertArrayEquals(Arrays.copyOf(plain, keep), cipher.open(sealed, 100, pages, keep, pool),
                            pages + " pages, " + content + " bytes of content, " + keep + " kept");
                }
                var pooled = pool.pooledBytesForTests();
                // A damaged unit opens to nothing, and the array it was opened into goes back to the pool
                for (var damaged : new int[]{100 + PageFormat.HEADER + random.nextInt(tag - PageFormat.HEADER), 100 + PageFormat.KIND}) {
                    sealed[damaged] ^= 1;
                    assertNull(cipher.open(sealed, 100, pages, PageFormat.HEADER + content, pool), "byte " + damaged + " damaged");
                    assertEquals(pooled, pool.pooledBytesForTests());
                    sealed[damaged] ^= 1;
                }
            }
        }
        var sealed = new byte[PageFormat.SIZE];
        assertThrows(IllegalArgumentException.class, () -> cipher.open(sealed, 0, 1, PageFormat.HEADER - 1, pool));
        assertThrows(IllegalArgumentException.class, () -> cipher.open(sealed, 0, 1, PageFormat.SIZE - PageFormat.TAG + 1, pool));
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("caches a value's unit as its header and value, as written and as read, unless that drops too little of it")
    void cachesValueUnits(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            var random = new Random(19);
            var sizes = new int[]{2_500, 3_800, 6_000, 70_000};
            // 3,800 bytes leave 264 of padding in their page, and 70,000 a ninth of a page in each of their 18
            var kept = new int[]{PageFormat.HEADER + 2_500, PageFormat.SIZE, PageFormat.HEADER + 6_000, 18 * PageFormat.SIZE};
            var refs = new PageRef[sizes.length];
            var values = new byte[sizes.length][];
            var written = 0L;
            for (var i = 0; i < sizes.length; i++) {
                values[i] = new byte[sizes[i]];
                random.nextBytes(values[i]);
                var unitPages = PageFormat.pagesFor(sizes[i]);
                var unit = new byte[PageFormat.HEADER + sizes[i]];
                System.arraycopy(values[i], 0, unit, PageFormat.HEADER, sizes[i]);
                refs[i] = writer.write(PageFormat.VALUE, 0, space.allocate(unitPages), unitPages, unit, sizes[i], true);
                written += unit.length;
            }
            assertEquals(written, pages.cache().size(), "the units as the checkpoint built them");
            writer.flush();
            pages.cache().clearForTests();
            var cached = 0L;
            for (var i = 0; i < sizes.length; i++) {
                var unit = pages.read(refs[i], PageFormat.VALUE, true);
                assertEquals(kept[i], unit.length, sizes[i] + " bytes");
                assertEquals(kept[i], PageFormat.cachedLength(refs[i].pages(), sizes[i]));
                cached += unit.length;
                assertEquals(cached, pages.cache().size());
                assertArrayEquals(values[i], Arrays.copyOfRange(unit, PageFormat.HEADER, PageFormat.HEADER + sizes[i]));
                assertSame(unit, pages.read(refs[i], PageFormat.VALUE, true), "the second read missed the cache");
            }
            pages.cache().clearForTests();
            for (var i = 0; i < sizes.length; i++) {
                var unit = pages.read(refs[i], PageFormat.VALUE, false);
                assertEquals(refs[i].pages() * PageFormat.SIZE, unit.length, "a read past the cache");
                assertArrayEquals(values[i], Arrays.copyOfRange(unit, PageFormat.HEADER, PageFormat.HEADER + sizes[i]));
            }
            assertEquals(0, pages.cache().size());
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("caches a unit of the file of large values as its header and value, but one of megabytes whole")
    void cachesExternalUnits(boolean encrypted, @TempDir Path directory) throws Exception {
        var cache = PageCache.isolated(64L << 20);
        try (var blobs = BlobFile.open(directory, cipher(encrypted), cache, new HeapPool(4 << 20), 0, new BitSet())) {
            blobs.begin(1);
            var random = new Random(23);
            var small = new byte[2_500];
            random.nextBytes(small);
            var large = new byte[(1 << 20) + 100];
            random.nextBytes(large);
            var smallRef = blobs.write(small, 0, small.length);
            var largeRef = blobs.write(large, 0, large.length);
            var smallUnit = blobs.read(smallRef, true);
            assertEquals(PageFormat.HEADER + small.length, smallUnit.length);
            assertEquals(PageFormat.HEADER + small.length, cache.size());
            assertSame(smallUnit, blobs.read(smallRef, true), "the second read missed the cache");
            assertArrayEquals(small, Arrays.copyOfRange(smallUnit, PageFormat.HEADER, PageFormat.HEADER + small.length));
            var largeUnit = blobs.read(largeRef, true);
            assertEquals(largeRef.pages() * PageFormat.SIZE, largeUnit.length);
            assertEquals(PageFormat.HEADER + small.length + largeUnit.length, cache.size());
            assertArrayEquals(large, Arrays.copyOfRange(largeUnit, PageFormat.HEADER, PageFormat.HEADER + large.length));
            // What the replay of the log reads
            var replayed = blobs.readUnit(smallRef);
            assertEquals(smallRef.pages() * PageFormat.SIZE, replayed.length);
            assertArrayEquals(small, Arrays.copyOfRange(replayed, PageFormat.HEADER, PageFormat.HEADER + small.length));
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("fails a value its leaf records longer than its blob holds as corruption, from the cache and from the file")
    void valueLongerThanBlob(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            var ref = writer.write(PageFormat.VALUE, 0, space.allocate(1), 1, new byte[PageFormat.HEADER + 2_500], 2_500, true);
            writer.flush();
            var entries = List.of(new TreeNodes.LeafEntry(segment("key"), null, ref, 2_600));
            var leaf = TreeNodes.writeLeaf(entries, 0, 1, 2 + entries.getFirst().size());
            var entry = TreeNodes.entry(leaf, 0);
            assertThrows(LedgerCorruptedException.class, () -> TreeSearch.value(pages, leaf, entry), "the unit the checkpoint cached");
            assertThrows(LedgerCorruptedException.class, () -> TreeSearch.valueBytes(pages, leaf, entry), "an array from the unit the checkpoint cached");
            pages.cache().clearForTests();
            assertThrows(LedgerCorruptedException.class, () -> TreeSearch.value(pages, leaf, entry, false), "the unit read whole");
            assertThrows(LedgerCorruptedException.class, () -> TreeSearch.value(pages, leaf, entry), "the unit read for the cache");
            pages.cache().clearForTests();
            assertThrows(LedgerCorruptedException.class, () -> TreeSearch.valueBytes(pages, leaf, entry), "an array from the unit read for the cache");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("caches the leaf a delta record makes as its header and content, which later checkpoints merge into")
    void deltaLeaves(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, cipher(encrypted), 16 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var random = new Random(29);
            var model = new TreeMap<String, byte[]>();
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 2_000; i++) {
                var key = "k%05d".formatted(i);
                var value = value(random, 100);
                model.put(key, value);
                ops.add(new TreeUpdate.Op(segment(key), MemorySegment.ofArray(value)));
            }
            var writer = pages.writer(1);
            var update = new TreeUpdate(pages, writer, space);
            var root = update.apply(null, ops, List.of());
            var deltas = update.finishDeltas(DeltaTable.EMPTY);
            writer.flush();
            assertEquals(1, TreeNodes.level(pages.read(root.ref(), PageFile.NODE, true)), "the root is not a branch of leaves");
            for (var round = 0; round < 6; round++) {
                // The keys of one leaf below the root change one at a time: its record grows by a change a round
                var key = "k%05d".formatted(1_000 + round);
                var value = value(random, 100);
                model.put(key, value);
                writer = pages.writer(2 + round);
                update = new TreeUpdate(pages, writer, space);
                root = update.apply(root, List.of(new TreeUpdate.Op(segment(key), MemorySegment.ofArray(value))), List.of());
                deltas = update.finishDeltas(deltas);
                writer.flush();
                var branch = pages.read(root.ref(), PageFile.NODE, true);
                var record = TreeNodes.childRef(branch, TreeNodes.childFor(branch, segment(key)));
                assertTrue(record.isRecord(), "round " + round + " wrote the leaf whole");
                var leaf = pages.cached(record, PageFile.NODE);
                assertNotNull(leaf, "the leaf of the record is not cached");
                assertEquals(PageFormat.HEADER + Bytes.i32(leaf, PageFormat.LENGTH), leaf.length);
                if (round % 2 == 1) {
                    // Made again from the delta page and the base, for the cache
                    pages.cache().clearForTests();
                    var read = pages.read(record, PageFile.NODE, true);
                    assertEquals(PageFormat.HEADER + Bytes.i32(read, PageFormat.LENGTH), read.length);
                    assertSame(read, pages.cached(record, PageFile.NODE));
                }
                verify(pages, root, model);
            }
            assertTrue(deltas.size() > 0, "no delta page is in use");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("readers of a tree many times larger than the cache read every value right while it keeps and evicts units of every length")
    void readersShareASmallCache(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 31 : 37);
        var model = new TreeMap<String, byte[]>();
        TreeRoot root;
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(1 << 20))) {
            var ops = new ArrayList<TreeUpdate.Op>();
            for (var i = 0; i < 4_000; i++) {
                var key = "m%06d".formatted(i);
                var kind = random.nextInt(20);
                // Values in blobs of one to three pages, a few of up to eight, and inline ones
                var value = value(random, kind < 5 ? 2_500 + random.nextInt(9_500) : kind == 5 ? 13_000 + random.nextInt(18_000)
                        : 20 + random.nextInt(280));
                model.put(key, value);
                ops.add(new TreeUpdate.Op(segment(key), MemorySegment.ofArray(value)));
            }
            var writer = pages.writer(1);
            var update = new TreeUpdate(pages, writer, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet()));
            root = update.apply(null, ops, List.of());
            update.finishDeltas(DeltaTable.EMPTY);
            writer.flush();
            pages.force();
        }
        var keys = new ArrayList<>(model.keySet());
        var tree = root;
        // Reopened, so every unit comes from the file
        try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(4 << 20))) {
            var failures = new ConcurrentLinkedQueue<String>();
            var threads = new Thread[8];
            for (var t = 0; t < threads.length; t++) {
                var seed = random.nextLong();
                threads[t] = Thread.ofPlatform().start(() -> {
                    var local = new Random(seed);
                    for (var i = 0; i < 3_000 && failures.isEmpty(); i++) {
                        try {
                            if (local.nextInt(40) == 0) {
                                var from = keys.get(local.nextInt(keys.size()));
                                var expected = model.tailMap(from, true).entrySet().iterator();
                                var cursor = new TreeCursor(pages, tree, from.getBytes(StandardCharsets.US_ASCII), null, false);
                                for (var n = 0; n < 100 && cursor.next(); n++) {
                                    var entry = expected.next();
                                    if (!entry.getKey().equals(string(cursor.key())) || !Arrays.equals(entry.getValue(), bytes(cursor.value()))) {
                                        failures.add("the scan from " + from + " at " + entry.getKey());
                                    }
                                }
                            } else {
                                var key = keys.get(local.nextInt(keys.size()));
                                var encoded = key.getBytes(StandardCharsets.US_ASCII);
                                var unit = TreeSearch.leaf(pages, tree, encoded);
                                var at = TreeSearch.entryOf(unit, encoded);
                                if (at == -1 || !Arrays.equals(model.get(key), bytes(TreeSearch.value(pages, unit, at)))) {
                                    failures.add("the value of " + key);
                                }
                            }
                        } catch (RuntimeException exception) {
                            failures.add(exception.toString());
                        }
                    }
                });
            }
            for (var thread : threads) {
                thread.join();
            }
            assertTrue(failures.isEmpty(), () -> failures.size() + " wrong reads, first: " + failures.peek());
        }
    }

    private static void verify(PageFile pages, TreeRoot root, TreeMap<String, byte[]> model) {
        assertEquals(model.size(), root.count());
        var cursor = new TreeCursor(pages, root, null, null, false);
        for (var entry : model.entrySet()) {
            assertTrue(cursor.next(), "the cursor ended early");
            assertEquals(entry.getKey(), string(cursor.key()));
            assertArrayEquals(entry.getValue(), bytes(cursor.value()));
        }
        for (var entry : model.entrySet()) {
            var encoded = entry.getKey().getBytes(StandardCharsets.US_ASCII);
            var unit = TreeSearch.leaf(pages, root, encoded);
            var at = TreeSearch.entryOf(unit, encoded);
            assertNotEquals(-1, at, entry.getKey());
            assertArrayEquals(entry.getValue(), bytes(TreeSearch.value(pages, unit, at)));
        }
    }

    private static byte[] value(Random random, int size) {
        var value = new byte[size];
        random.nextBytes(value);
        return value;
    }

    private static MemorySegment segment(String key) {
        return MemorySegment.ofArray(key.getBytes(StandardCharsets.US_ASCII)).asReadOnly();
    }

    private static String string(MemorySegment segment) {
        return new String(bytes(segment), StandardCharsets.US_ASCII);
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }
}
