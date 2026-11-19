package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The page cache on its own: lookups by nonce, eviction order, the slots of its table, the byte limit and the budget
/// caches share, the memory of the entries it drops, and the doorkeeper that screens what reads miss.
class PageCacheTest {
    private static final long LIMIT = 1 << 20;
    // The bytes of a doorkeeper's two generations of the fewest keys, at 9 bits a key: the first one, of two levels,
    // takes about 9.6 and the later ones about 8.2
    private static final long TWO_GENERATIONS = 2L * Doorkeeper.FIRST_KEYS * 9 / 8;

    @Test
    @DisplayName("finds a unit only under the nonce it was cached with")
    void lookups() {
        var cache = PageCache.isolated(LIMIT);
        var unit = unit(new PageRef(5, 1, 1, 1));
        cache.put(new PageRef(5, 1, 1, 1), unit, false);
        assertSame(unit, cache.get(new PageRef(5, 1, 1, 1)));
        assertNull(cache.get(new PageRef(5, 1, 1, 2)));
        cache.remove(5);
        assertNull(cache.get(new PageRef(5, 1, 1, 1)));
    }

    @Test
    @DisplayName("finds a unit only under the pages it was cached with, whatever the length of its array")
    void lookupsByPages() {
        var cache = PageCache.isolated(LIMIT);
        // A unit of two pages kept without its padding
        var unit = new byte[PageFormat.HEADER + 2_500];
        PageFile.header(unit, PageFormat.LEAF, 0, 5, 2, 2_500, 1, 1);
        cache.put(new PageRef(5, 2, 1, 1), unit, false);
        assertSame(unit, cache.get(new PageRef(5, 2, 1, 1)));
        assertNull(cache.get(new PageRef(5, 1, 1, 1)), "a unit of one page at the same page and nonce");
        assertNull(cache.get(new PageRef(5, 3, 1, 1)), "a unit of three pages at the same page and nonce");
    }

    @Test
    @DisplayName("counts the bytes of the arrays it holds, not of their units' pages")
    void countsArrays() {
        var cache = PageCache.isolated(LIMIT);
        cache.put(new PageRef(5, 1, 1, 1), new byte[PageFormat.HEADER + 2_500], false);
        cache.put(new PageRef(9, 3, 1, 2), new byte[PageFormat.HEADER + 9_000], false);
        assertEquals(2L * PageFormat.HEADER + 11_500, cache.size());
        cache.remove(5);
        assertEquals(PageFormat.HEADER + 9_000, cache.size());
        cache.put(new PageRef(9, 3, 1, 3), new byte[3 * PageFormat.SIZE], false);
        assertEquals(3 * PageFormat.SIZE, cache.size(), "the replaced entry still counts");
    }

    @Test
    @DisplayName("evicts at a tick of the idle ticker the entries no read used since the tick before, however far below its limit")
    void idleEviction() {
        var cache = PageCache.isolated(LIMIT);
        // Ticked by the test alone
        IdleTicker.unwatch(cache);
        for (var i = 0; i < 8; i++) {
            cache.put(page(i), unit(page(i)), i % 2 == 0);
        }
        // Less than a whole period without reads: marked, not evicted
        cache.tick();
        assertEquals(8L * PageFormat.SIZE, cache.size());
        var read = cache.get(page(3));
        cache.tick();
        assertEquals(PageFormat.SIZE, cache.size());
        assertSame(read, cache.get(page(3)));
        assertNull(cache.get(page(2)));
        cache.tick();
        cache.tick();
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("stays within its limit")
    void limit() {
        var cache = PageCache.isolated(LIMIT);
        for (var i = 0; i < 200; i++) {
            cache.put(new PageRef(2 + i * 16, 16, 1, i), new byte[16 * PageFormat.SIZE], i % 3 == 0);
            assertTrue(cache.size() <= LIMIT, "holds " + cache.size() + " bytes");
        }
    }

    @Test
    @DisplayName("lets one cache of a shared budget use the whole limit while the other is empty, and a cache that starts reading take back its share")
    void sharedBudget() {
        var budget = new PageCache.Budget();
        var busy = PageCache.in(budget, LIMIT);
        var idle = PageCache.in(budget, LIMIT);
        var unit = PageFormat.SIZE;
        for (var i = 0; i < 1_000; i++) {
            busy.put(page(i), new byte[unit], true);
        }
        assertTrue(busy.size() >= LIMIT - unit, "the busy cache holds " + busy.size() + " bytes of " + LIMIT);
        for (var i = 0; i < 1_000; i++) {
            idle.put(page(i), new byte[unit], true);
            assertTrue(budget.total() <= LIMIT, "the caches hold " + budget.total() + " bytes together");
        }
        // Each holds its share, as both keep reading
        assertTrue(idle.size() >= LIMIT / 2 - unit, "the cache that started reading holds " + idle.size() + " bytes");
        assertTrue(busy.size() >= LIMIT / 2 - unit, "the busy cache kept " + busy.size() + " bytes");
        idle.close();
        assertEquals(busy.size(), budget.total(), "a closed cache keeps no bytes in the budget");
    }

    @Test
    @DisplayName("caches nothing a read ends with after its cache closed, so the budget keeps no bytes of a closed store")
    void putAfterClose() {
        var budget = new PageCache.Budget();
        var open = PageCache.in(budget, LIMIT);
        var closed = PageCache.in(budget, LIMIT);
        open.put(new PageRef(1, 1, 1, 1), new byte[PageFormat.SIZE], true);
        closed.close();
        closed.put(new PageRef(2, 1, 1, 2), new byte[PageFormat.SIZE], true);
        assertNull(closed.get(new PageRef(2, 1, 1, 2)));
        assertEquals(open.size(), budget.total());
    }

    @Test
    @DisplayName("gives back the bytes and the limit of a cache never closed once the collector takes it")
    void collectedCache() throws InterruptedException {
        var budget = new PageCache.Budget();
        var kept = PageCache.in(budget, LIMIT);
        fillAndForget(budget);
        for (var attempt = 0; attempt < 50 && (budget.total() != kept.size() || budget.limit() != LIMIT); attempt++) {
            System.gc();
            Thread.sleep(20);
        }
        assertEquals(kept.size(), budget.total(), "the forgotten cache still counts its bytes");
        assertEquals(LIMIT, budget.limit(), "the forgotten cache still raises the limit");
    }

    private static void fillAndForget(PageCache.Budget budget) {
        var forgotten = PageCache.in(budget, 4 * LIMIT);
        for (var i = 0; i < 16; i++) {
            forgotten.put(page(i), new byte[PageFormat.SIZE], true);
        }
    }

    @Test
    @DisplayName("holds the largest limit an open cache of the budget asked for")
    void budgetLimit() {
        var budget = new PageCache.Budget();
        var small = PageCache.in(budget, LIMIT);
        var large = PageCache.in(budget, 4 * LIMIT);
        assertEquals(4 * LIMIT, budget.limit());
        large.close();
        assertEquals(LIMIT, budget.limit());
        small.close();
        assertEquals(0, budget.limit());
    }

    @Test
    @DisplayName("keeps the clock bounded while pages are replaced and removed under the limit")
    void boundedClock() {
        var cache = PageCache.isolated(LIMIT);
        for (var i = 0; i < 100_000; i++) {
            cache.put(new PageRef(2 + i % 4, 1, 1, i), new byte[PageFormat.SIZE], true);
            if (i % 2 == 0) {
                cache.remove(2 + (i + 1) % 4);
            }
        }
        assertTrue(cache.clockLengthForTests() <= 4, "the clock holds " + cache.clockLengthForTests() + " entries");
        assertEquals(PageCache.MIN_CAPACITY, cache.capacityForTests(), "the slots of departed entries are reused or cleared");
    }

    @Test
    @DisplayName("lets go of the array of an entry it replaced, removed or refused to replace")
    void dropsArrays() throws InterruptedException {
        var cache = PageCache.isolated(64L << 20);
        var replaced = cached(cache, 1);
        cache.put(new PageRef(1, 1, 2, 2), new byte[PageFormat.SIZE], true);
        var removed = cached(cache, 2);
        cache.remove(2);
        var refused = cached(cache, 3);
        // Larger than a quarter of the limit, so the cache keeps nothing for page 3
        cache.put(new PageRef(3, 4097, 2, 2), new byte[4097 * PageFormat.SIZE], true);
        for (var attempt = 0; attempt < 20 && (replaced.get() != null || removed.get() != null || refused.get() != null); attempt++) {
            System.gc();
            Thread.sleep(10);
        }
        assertNull(replaced.get(), "the replaced array is still reachable");
        assertNull(removed.get(), "the removed array is still reachable");
        assertNull(refused.get(), "the array of the refused page is still reachable");
    }

    private static WeakReference<byte[]> cached(PageCache cache, int page) {
        var unit = unit(new PageRef(page, 1, 1, 1));
        cache.put(new PageRef(page, 1, 1, 1), unit, true);
        return new WeakReference<>(unit);
    }

    @Test
    @DisplayName("finds a unit only under the pages it was cached with")
    void lookupByPages() {
        var cache = PageCache.isolated(LIMIT);
        var ref = new PageRef(5, 2, 1, 1);
        var unit = new byte[2 * PageFormat.SIZE];
        PageFile.header(unit, PageFormat.VALUE, 0, 5, 2, 0, 1, 1);
        cache.put(ref, unit, true);
        assertSame(unit, cache.get(ref));
        assertNull(cache.get(new PageRef(5, 1, 1, 1)));
    }

    @Test
    @DisplayName("treats a unit whose header does not name the reference as absent, unless it is a delta record's leaf, whose header names its base")
    void headerCheck() {
        var cache = PageCache.isolated(LIMIT);
        var ref = new PageRef(5, 1, 1, 1);
        var other = new PageRef(6, 1, 1, 1);
        cache.put(ref, unit(other), true);
        assertNull(cache.get(ref));
        assertNull(cache.peek(ref));

        var record = new PageRef(7, 1, 2, 2, 3);
        var leaf = new byte[PageFormat.SIZE];
        TreeNodes.deltaHeader(leaf, other, 0);
        cache.put(record, leaf, true);
        assertSame(leaf, cache.get(record));
        assertNull(cache.get(record.unit()));
        assertNull(cache.get(new PageRef(7, 1, 2, 2, 4)));
        assertNull(cache.get(new PageRef(7, 1, 2, 3, 3)));
    }

    @Test
    @DisplayName("tells apart the keys of the two files and of each delta record of a page")
    void keysOfFiles() {
        var cache = PageCache.isolated(LIMIT);
        var ref = new PageRef(5, 1, 1, 1);
        var data = unit(ref);
        var blob = unit(ref);
        cache.put(ref, data, true);
        cache.put(PageCache.key(PageCache.BLOBS, 5), ref, blob, true);
        assertSame(data, cache.get(ref));
        assertSame(blob, cache.get(PageCache.key(PageCache.BLOBS, 5), ref));
        assertEquals(PageCache.key(PageCache.DATA, 5), PageCache.dataKey(5, -1));
        assertNotEquals(PageCache.dataKey(5, 0), PageCache.dataKey(5, 1));
        cache.remove(5);
        assertNull(cache.get(ref));
        assertSame(blob, cache.get(PageCache.key(PageCache.BLOBS, 5), ref));
    }

    @Test
    @DisplayName("starts the probes of consecutive pages of a file at consecutive slots, and finds every unit of runs of pages that share their slots across files and table sizes")
    void runsOfPages() {
        for (var file : new int[]{PageCache.DATA, PageCache.BLOBS}) {
            for (var page = 0; page < 1_000; page++) {
                assertEquals((PageCache.homeForTests(PageCache.key(file, page), 1 << 14) + 1) & ((1 << 14) - 1),
                        PageCache.homeForTests(PageCache.key(file, page + 1), 1 << 14));
            }
        }
        var cache = PageCache.isolated(LIMIT);
        var keys = new ArrayList<long[]>();
        // Runs a table's size apart for any size the table takes, so most keys find their first slot taken
        for (var lap = 0; lap < 4; lap++) {
            for (var page = 0; page < 1_024; page++) {
                var at = lap * (1 << 16) + page;
                keys.add(new long[]{PageCache.key(PageCache.DATA, at), at});
                keys.add(new long[]{PageCache.key(PageCache.BLOBS, at), at});
                keys.add(new long[]{PageCache.dataKey(at, lap), at});
            }
        }
        var units = new ArrayList<byte[]>();
        for (var key : keys) {
            var ref = new PageRef((int) key[1], 1, key[0], (int) key[1]);
            var unit = shortUnit(ref);
            units.add(unit);
            cache.put(key[0], ref, unit, true);
        }
        for (var i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            assertSame(units.get(i), cache.get(key[0], new PageRef((int) key[1], 1, key[0], (int) key[1])), "key " + Long.toHexString(key[0]));
        }
        assertEquals(keys.size(), cache.clockLengthForTests());
    }

    @Test
    @DisplayName("replaces a key in its slot, with the old nonce gone and the bytes exact")
    void replacement() {
        var budget = new PageCache.Budget();
        var cache = PageCache.in(budget, LIMIT);
        var first = new PageRef(5, 1, 1, 1);
        var second = new PageRef(5, 1, 1, 2);
        var replacement = new byte[64];
        PageFile.header(replacement, PageFormat.LEAF, 0, 5, 1, 0, 1, 2);
        cache.put(first, unit(first), true);
        cache.put(second, replacement, true);
        assertNull(cache.get(first));
        assertSame(replacement, cache.get(second));
        assertEquals(64, cache.size());
        assertEquals(64, budget.total());
        assertEquals(1, cache.clockLengthForTests());
        cache.remove(5);
        assertEquals(0, cache.size());
        assertEquals(0, budget.total());
        assertEquals(0, cache.clockLengthForTests());
        cache.remove(5);
        assertEquals(0, budget.total());
    }

    @Test
    @DisplayName("evicts the units no read used in the order they came, however their keys hash")
    void insertionOrder() {
        var cache = PageCache.isolated(16L * PageFormat.SIZE);
        for (var i = 0; i < 16; i++) {
            cache.put(page(i), unit(page(i)), false);
        }
        for (var i = 16; i < 32; i++) {
            cache.put(page(i), unit(page(i)), false);
            assertNull(cache.peek(page(i - 16)), "page " + (i - 16) + " came first");
            assertNotNull(cache.peek(page(i - 15)), "page " + (i - 15) + " came after it");
            assertNotNull(cache.peek(page(i)), "the newest unit waits behind the others");
        }
    }

    @Test
    @DisplayName("gives a unit a read used one more pass of the clock")
    void secondChance() {
        var cache = PageCache.isolated(16L * PageFormat.SIZE);
        for (var i = 0; i < 16; i++) {
            cache.put(page(i), unit(page(i)), false);
        }
        assertNotNull(cache.get(page(0)));
        cache.put(page(16), unit(page(16)), false);
        assertNotNull(cache.peek(page(0)), "a read kept it");
        assertNull(cache.peek(page(1)));
        // Its pass used up, it now waits behind the ones that came after it
        for (var i = 17; i < 32; i++) {
            cache.put(page(i), unit(page(i)), false);
        }
        assertNotNull(cache.peek(page(0)));
        cache.put(page(32), unit(page(32)), false);
        assertNull(cache.peek(page(0)));
    }

    @Test
    @DisplayName("a peek counts neither for the clock nor for the idle ticker")
    void peekLeavesNoTrace() {
        var cache = PageCache.isolated(16L * PageFormat.SIZE);
        IdleTicker.unwatch(cache);
        for (var i = 0; i < 16; i++) {
            cache.put(page(i), unit(page(i)), false);
        }
        assertNotNull(cache.peek(page(0)));
        cache.put(page(16), unit(page(16)), false);
        assertNull(cache.peek(page(0)), "a peek does not save it from the clock");

        cache.tick();
        assertNotNull(cache.peek(page(1)));
        assertNotNull(cache.get(page(2)));
        cache.tick();
        assertNull(cache.peek(page(1)), "a peek does not save it from the tick");
        assertNotNull(cache.peek(page(2)), "a read does");
    }

    @Test
    @DisplayName("keeps the order and the flags of its entries when the table grows")
    void growth() {
        var cache = PageCache.isolated(48L * PageFormat.SIZE);
        for (var i = 0; i < 48; i++) {
            cache.put(page(i), unit(page(i)), false);
            if (i == PageCache.MIN_CAPACITY / 2 - 2) {
                assertEquals(PageCache.MIN_CAPACITY, cache.capacityForTests());
                assertNotNull(cache.get(page(1)));
            }
        }
        assertTrue(cache.capacityForTests() >= 4 * PageCache.MIN_CAPACITY, "the table holds " + cache.capacityForTests() + " slots");
        for (var i = 0; i < 48; i++) {
            assertNotNull(cache.peek(page(i)), "page " + i + " survived the growth");
        }
        assertEquals(48, cache.clockLengthForTests());
        cache.put(page(48), unit(page(48)), false);
        assertNull(cache.peek(page(0)));
        cache.put(page(49), unit(page(49)), false);
        assertNotNull(cache.peek(page(1)), "the read before the growth still counts");
        assertNull(cache.peek(page(2)));
    }

    @Test
    @DisplayName("holds its table to one size under steady eviction, however many keys pass through")
    void churn() {
        var cache = PageCache.isolated(8L * PageFormat.SIZE);
        var capacity = -1;
        for (var i = 0; i < 100_000; i++) {
            cache.put(page(i), unit(page(i)), i % 3 == 0);
            if (i == 64) {
                capacity = cache.capacityForTests();
            } else if (i > 64) {
                assertEquals(capacity, cache.capacityForTests());
            }
            assertTrue(cache.capacityForTests() <= 32, "the table holds " + cache.capacityForTests() + " slots");
            assertTrue(cache.tombstonesForTests() <= cache.capacityForTests() * 3 / 4, "the table holds " + cache.tombstonesForTests() + " departed slots");
        }
        assertEquals(8, cache.clockLengthForTests());
        assertEquals(8L * PageFormat.SIZE, cache.size());
    }

    @Test
    @DisplayName("shrinks at a tick a table that few of its slots use, keeping what it holds")
    void shrink() {
        var cache = PageCache.isolated(64L << 20);
        IdleTicker.unwatch(cache);
        var units = new byte[1_000][];
        for (var i = 0; i < units.length; i++) {
            units[i] = shortUnit(page(i));
            cache.put(page(i), units[i], true);
        }
        assertEquals(2_048, cache.capacityForTests());
        for (var i = 10; i < units.length; i++) {
            cache.remove(page(i).page());
        }
        assertEquals(990, cache.tombstonesForTests());
        cache.tick();
        // Eight slots an entry at most, and never below the smallest table
        assertEquals(Math.max(PageCache.MIN_CAPACITY, 64), cache.capacityForTests());
        assertEquals(0, cache.tombstonesForTests());
        for (var i = 0; i < 10; i++) {
            assertSame(units[i], cache.get(page(i)));
        }
        assertEquals(10, cache.clockLengthForTests());
        assertEquals(10L * units[0].length, cache.size());
        for (var i = 0; i < 5; i++) {
            cache.tick();
        }
        assertEquals(0, cache.size());
        assertEquals(PageCache.MIN_CAPACITY, cache.capacityForTests());
    }

    @Test
    @DisplayName("empties at a clear, and gives back every byte")
    void clear() {
        var budget = new PageCache.Budget();
        var cache = PageCache.in(budget, LIMIT);
        for (var i = 0; i < 100; i++) {
            cache.put(page(i), shortUnit(page(i)), true);
        }
        cache.clearForTests();
        assertEquals(0, cache.size());
        assertEquals(0, budget.total());
        assertEquals(0, cache.clockLengthForTests());
        assertNull(cache.get(page(0)));
        cache.put(page(0), shortUnit(page(0)), true);
        assertNotNull(cache.get(page(0)));
    }

    @Test
    @DisplayName("finds a child of a branch without a reference under the key of the reference, a delta record's leaf included")
    void childrenOfBranches(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(null), LIMIT, new HeapPool(1 << 20))) {
            var plain = new PageRef(10, 1, 7, -8);
            var record = new PageRef(11, 1, -9, 10, 300);
            var absent = new PageRef(12, 1, 11, 12);
            var value = new PageRef(13, 1, 13, 14);
            var children = new ArrayList<TreeNodes.Child>();
            for (var ref : List.of(plain, record, absent, value)) {
                var firstKey = MemorySegment.ofArray(new byte[]{(byte) ref.page()});
                children.add(new TreeNodes.Child(firstKey, ref, 1, false, 0, -1));
            }
            var branch = TreeNodes.writeBranch(children, 0, children.size());
            PageFile.header(branch, PageFormat.BRANCH, 1, 20, 1, branch.length - PageFormat.HEADER, 1, 1);
            for (var i = 0; i < children.size(); i++) {
                assertEquals(children.get(i).ref(), TreeNodes.childRef(branch, i));
            }
            var cache = pages.cache();
            var leaf = unit(plain);
            var recordLeaf = new byte[PageFormat.SIZE];
            TreeNodes.deltaHeader(recordLeaf, plain, 0);
            cache.put(plain, leaf, true);
            cache.put(record, recordLeaf, true);
            var values = new byte[PageFormat.SIZE];
            PageFile.header(values, PageFormat.VALUE, 0, 13, 1, 0, 13, 14);
            cache.put(value, values, true);
            assertSame(leaf, pages.cachedNode(branch, 0));
            assertSame(leaf, pages.node(branch, 0));
            assertSame(recordLeaf, pages.cachedNode(branch, 1));
            assertSame(recordLeaf, pages.node(branch, 1));
            assertNull(pages.cachedNode(branch, 2));
            assertNull(pages.cachedNode(branch, 3), "a unit of another kind is no node");
        }
    }

    @Test
    @DisplayName("a checkpoint's writer that waits for the cache's lock holds up none of the other tasks passing through the writer")
    void writerWaitsOutsideItsLock(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(null), LIMIT, new HeapPool(1 << 20))) {
            var writer = pages.writer(1);
            var unit = new byte[PageFormat.SIZE];
            var failure = new AtomicReference<Exception>();
            var queued = new CountDownLatch(1);
            var refs = new AtomicReference<PageRef>();
            var writing = new Thread(() -> {
                try {
                    refs.set(writer.write(PageFormat.LEAF, 0, 10, 1, unit, 0, true));
                } catch (IOException exception) {
                    failure.set(exception);
                }
            });
            var other = new Thread(() -> {
                while (writer.written() == 0) {
                    Thread.onSpinWait();
                }
                queued.countDown();
            });
            other.setDaemon(true);
            pages.cache().whileLockedForTests(() -> {
                writing.start();
                other.start();
                try {
                    assertTrue(queued.await(10, TimeUnit.SECONDS), "the writer's lock stayed held while the cache's was taken");
                    assertNull(refs.get(), "the write waits for the cache");
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
            });
            writing.join();
            assertNull(failure.get());
            assertSame(unit, pages.cache().get(refs.get()), "the write ends with its unit cached");
            assertSame(unit, writer.staged(refs.get()));
        }
    }

    @Nested
    @DisplayName("while it screens what reads miss")
    class Screening {
        /// {@return a cache the test alone ticks, which screens what its reads miss and keeps its table of refused units
        /// through collections, so a recall of a unit the test holds finds it whatever the collector does}
        private static PageCache screening(long limit) {
            var cache = weakScreening(limit);
            cache.pinRefusalsForTests();
            return cache;
        }

        /// {@return a cache the test alone ticks, which screens what its reads miss and reaches its table of refused
        /// units only weakly, as a store's does}
        private static PageCache weakScreening(long limit) {
            var cache = PageCache.isolated(limit);
            IdleTicker.unwatch(cache);
            cache.screen(true);
            return cache;
        }

        /// Collects until the collector took the table of the refused units of `cache`.
        private static void collectRefusals(PageCache cache) {
            for (var attempt = 0; attempt < 20 && cache.refusalSlotsForTests() != 0; attempt++) {
                System.gc();
            }
            assertEquals(0, cache.refusalSlotsForTests(), "the collector left the table of refused units");
        }

        @Test
        @DisplayName("caches a unit at its second read, not at its first")
        void secondRead() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)), PageCache.POINT);
            assertNull(cache.peek(page(0)));
            assertEquals(0, cache.size());
            var second = unit(page(0));
            cache.putScreened(page(0), second, PageCache.POINT);
            assertSame(second, cache.peek(page(0)));
            assertTrue(cache.referencedForTests(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
        }

        @Test
        @DisplayName("tells units apart by their nonce, so the unit a checkpoint wrote over a page read before takes two reads")
        void byNonce() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)), PageCache.POINT);
            var rewritten = new PageRef(page(0).page(), 1, 9, 9);
            cache.putScreened(rewritten, unit(rewritten), PageCache.POINT);
            assertNull(cache.peek(rewritten));
            cache.putScreened(rewritten, unit(rewritten), PageCache.POINT);
            assertNotNull(cache.peek(rewritten));
        }

        @Test
        @DisplayName("drops at a first read the entry of another unit at the same page, by exactly its bytes, and keeps the others")
        void dropsOtherUnit() {
            var cache = screening(LIMIT);
            cache.put(page(1), unit(page(1)), true);
            var old = page(0);
            cache.put(old, shortUnit(old), false);
            var bytes = cache.size();
            var length = cache.clockLengthForTests();
            var rewritten = new PageRef(old.page(), 1, 7, 8);
            cache.putScreened(rewritten, unit(rewritten), PageCache.POINT);
            assertEquals(bytes - PageFormat.HEADER, cache.size());
            assertEquals(length - 1, cache.clockLengthForTests());
            assertNull(cache.get(rewritten));
            assertNull(cache.get(old));
            assertNotNull(cache.peek(page(1)));
        }

        @Test
        @DisplayName("keeps the entry of the very unit a first read missed, which a read racing it may have cached")
        void keepsSameUnit() {
            var cache = screening(LIMIT);
            var written = unit(page(0));
            // As a checkpoint writes it
            cache.put(page(0), written, false);
            cache.putScreened(page(0), unit(page(0)), PageCache.POINT);
            assertSame(written, cache.peek(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
        }

        @Test
        @DisplayName("caches a branch of level 2 or more at its first read, recording nothing, and screens one of level 1 as a leaf, but at a point read's fourth read through recalls")
        void branches() {
            var cache = screening(LIMIT);
            for (var level = 2; level <= 4; level++) {
                var ref = page(level);
                var branch = new byte[PageFormat.SIZE];
                PageFile.header(branch, PageFormat.BRANCH, level, ref.page(), 1, 0, ref.nonceHigh(), ref.nonceLow());
                cache.putScreened(ref, branch, PageCache.SCAN);
                assertSame(branch, cache.peek(ref), "a branch of level " + level);
            }
            assertEquals(0, cache.doorkeeperForTests().keysForTests());
            var ref = page(0);
            var branch = new byte[PageFormat.SIZE];
            PageFile.header(branch, PageFormat.BRANCH, 1, ref.page(), 1, 0, ref.nonceHigh(), ref.nonceLow());
            cache.putScreened(ref, branch, PageCache.POINT);
            assertNull(cache.peek(ref), "a branch of level 1 read once");
            assertTrue(cache.seen(ref), "a branch of level 1 the doorkeeper recorded");
            assertSame(branch, cache.recall(ref, PageCache.POINT), "a branch of level 1 read twice");
            assertNull(cache.peek(ref), "a branch of level 1 a point read read twice");
            assertEquals(2, cache.reads(ref));
            assertEquals(0, cache.doorkeeperForTests().thriceKeysForTests(), "a point read started the third filter");
            assertSame(branch, cache.recall(ref, PageCache.POINT), "a branch of level 1 read three times");
            assertNull(cache.peek(ref), "a branch of level 1 a point read recalled twice");
            assertEquals(0, cache.doorkeeperForTests().thriceKeysForTests(), "a point read's recall started the third filter");
            assertSame(branch, cache.recall(ref, PageCache.POINT), "a branch of level 1 read four times");
            assertSame(branch, cache.peek(ref));
            // A point read's leaf of the same store still takes two reads
            var leaf = page(5);
            cache.putScreened(leaf, unit(leaf), PageCache.POINT);
            var second = unit(leaf);
            cache.putScreened(leaf, second, PageCache.POINT);
            assertSame(second, cache.peek(leaf), "a leaf a point read read twice");
            // A scan's read of one takes it at its fifth read through recalls, as a leaf's
            var scanned = page(1);
            var scannedBranch = new byte[PageFormat.SIZE];
            PageFile.header(scannedBranch, PageFormat.BRANCH, 1, scanned.page(), 1, 0, scanned.nonceHigh(), scanned.nonceLow());
            cache.putScreened(scanned, scannedBranch, PageCache.SCAN);
            assertSame(scannedBranch, cache.recall(scanned, PageCache.SCAN), "a branch of level 1 a scan read twice");
            assertNull(cache.peek(scanned), "a branch of level 1 a scan read twice");
            assertSame(scannedBranch, cache.recall(scanned, PageCache.SCAN), "a branch of level 1 a scan read three times");
            assertNull(cache.peek(scanned), "a branch of level 1 a scan read three times");
            assertSame(scannedBranch, cache.recall(scanned, PageCache.SCAN), "a branch of level 1 a scan read four times");
            assertNull(cache.peek(scanned), "a branch of level 1 a scan read four times");
            assertSame(scannedBranch, cache.recall(scanned, PageCache.SCAN), "a branch of level 1 a scan read five times");
            assertSame(scannedBranch, cache.peek(scanned));
        }

        @Test
        @DisplayName("caches at the first read when no file turned screening on, or a file turned it off, and screens no plain put")
        void unscreened() {
            var unturned = PageCache.isolated(LIMIT);
            unturned.putScreened(page(0), unit(page(0)), PageCache.POINT);
            assertNotNull(unturned.peek(page(0)));
            var off = PageCache.isolated(LIMIT);
            off.screen(true);
            off.screen(false);
            off.putScreened(page(0), unit(page(0)), PageCache.POINT);
            assertNotNull(off.peek(page(0)));
            assertNull(off.recall(page(1), PageCache.POINT), "a refusal of a cache that refuses nothing");
            var warm = screening(LIMIT);
            warm.put(page(0), unit(page(0)), true);
            warm.put(page(1), unit(page(1)), false);
            assertNotNull(warm.peek(page(0)));
            assertNotNull(warm.peek(page(1)));
            assertEquals(0, warm.doorkeeperForTests().keysForTests(), "a put started the doorkeeper");
        }

        @Test
        @DisplayName("recalls the very array of a unit it refused, which its second read then neither reads nor opens, and caches it at a point read's third read, as an entry a read used")
        void recall() {
            var cache = screening(LIMIT);
            var first = unit(page(0));
            cache.putScreened(page(0), first, PageCache.POINT);
            assertNull(cache.peek(page(0)));
            assertSame(first, cache.recall(page(0), PageCache.POINT), "the second read");
            assertNull(cache.peek(page(0)), "the second read, a recall");
            assertEquals(0, cache.size());
            assertEquals(1, cache.reads(page(0)), "the reads the doorkeeper counts after a recall past a point read's");
            assertSame(first, cache.recall(page(0), PageCache.POINT), "the third read");
            assertSame(first, cache.peek(page(0)));
            assertTrue(cache.referencedForTests(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
            assertNull(cache.recall(page(0), PageCache.POINT), "a recall of a unit the cache already took");
            assertSame(first, cache.peek(page(0)));
            assertEquals(1, cache.clockLengthForTests());
        }

        @Test
        @DisplayName("caches a unit a scan reads at its fourth read, not at its first, second or third, and one a point read reads at its second")
        void fourthRead() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)), PageCache.SCAN);
            assertNull(cache.peek(page(0)), "a scan's first read");
            cache.putScreened(page(0), unit(page(0)), PageCache.SCAN);
            assertNull(cache.peek(page(0)), "a scan's second read");
            cache.putScreened(page(0), unit(page(0)), PageCache.SCAN);
            assertNull(cache.peek(page(0)), "a scan's third read");
            assertEquals(0, cache.size());
            assertEquals(3, cache.reads(page(0)));
            var fourth = unit(page(0));
            cache.putScreened(page(0), fourth, PageCache.SCAN);
            assertSame(fourth, cache.peek(page(0)), "a scan's fourth read");
            assertTrue(cache.referencedForTests(page(0)));
            cache.putScreened(page(1), unit(page(1)), PageCache.SCAN);
            var point = unit(page(1));
            cache.putScreened(page(1), point, PageCache.POINT);
            assertSame(point, cache.peek(page(1)), "a point read of a unit a scan read once");
        }

        @Test
        @DisplayName("of a scan recalls at the second, third and fourth reads the very array it refused, leaving it uncached and in its slot, and caches it at the fifth")
        void scanRecall() {
            var cache = screening(LIMIT);
            var first = unit(page(0));
            cache.putScreened(page(0), first, PageCache.SCAN);
            for (var read = 2; read <= 4; read++) {
                assertSame(first, cache.recall(page(0), PageCache.SCAN), "read " + read);
                assertNull(cache.peek(page(0)));
                assertEquals(0, cache.size());
                assertEquals(0, cache.clockLengthForTests());
            }
            assertSame(first, cache.recall(page(0), PageCache.SCAN), "the fifth read");
            assertSame(first, cache.peek(page(0)));
            assertTrue(cache.referencedForTests(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
            assertNull(cache.recall(page(0), PageCache.SCAN), "a recall of a unit the cache already took");

            var other = unit(page(1));
            cache.putScreened(page(1), other, PageCache.SCAN);
            assertSame(other, cache.recall(page(1), PageCache.POINT), "a point read of a unit a scan refused");
            assertNull(cache.peek(page(1)), "a point read's second read, a recall");
            assertSame(other, cache.recall(page(1), PageCache.POINT), "a point read's third read");
            assertSame(other, cache.peek(page(1)), "cached at a point read's third read");
        }

        @Test
        @DisplayName("of a scan counts a recall as a read only when it finds the array, so a second read whose array the collector took is refused again, and the recalls after it cache the unit at the fifth read")
        void scanRecallOfCollected() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)), PageCache.SCAN);
            // As a collection that took the array would leave the table of refused units
            cache.clearForTests();
            assertNull(cache.recall(page(0), PageCache.SCAN), "a recall of a collected array");
            assertEquals(1, cache.reads(page(0)), "the reads after a recall that found nothing");
            var second = unit(page(0));
            cache.putScreened(page(0), second, PageCache.SCAN);
            assertNull(cache.peek(page(0)), "the second read, from the file");
            assertEquals(2, cache.reads(page(0)));
            assertSame(second, cache.recall(page(0), PageCache.SCAN), "the third read");
            assertNull(cache.peek(page(0)), "the third read");
            assertEquals(3, cache.reads(page(0)));
            assertSame(second, cache.recall(page(0), PageCache.SCAN), "the fourth read");
            assertNull(cache.peek(page(0)), "the fourth read");
            assertEquals(3, cache.reads(page(0)), "the reads the doorkeeper counts, as many as a read from the file needs");
            assertSame(second, cache.recall(page(0), PageCache.SCAN), "the fifth read");
            assertSame(second, cache.peek(page(0)));
        }

        @Test
        @DisplayName("of a scan counts the reads of a unit it refused on the refusal, so a recall caches it at the fifth read though the doorkeeper forgot a read, and records each read for a read from the file")
        void scanRecallCounts() {
            var cache = screening(LIMIT);
            var first = unit(page(0));
            cache.putScreened(page(0), first, PageCache.SCAN);
            assertSame(first, cache.recall(page(0), PageCache.SCAN), "the second read");
            assertEquals(2, cache.reads(page(0)));
            cache.doorkeeperForTests().forgetTwiceForTests(Doorkeeper.FIRST_KEYS);
            assertEquals(1, cache.reads(page(0)), "the reads once the doorkeeper forgot the second");
            assertSame(first, cache.recall(page(0), PageCache.SCAN), "the third read");
            assertNull(cache.peek(page(0)), "the third read");
            assertEquals(3, cache.reads(page(0)), "the reads the third recall recorded");
            assertSame(first, cache.recall(page(0), PageCache.SCAN), "the fourth read");
            assertNull(cache.peek(page(0)), "the fourth read");
            assertSame(first, cache.recall(page(0), PageCache.SCAN), "the fifth read");
            assertSame(first, cache.peek(page(0)), "cached at the fifth read");

            var other = unit(page(1));
            cache.putScreened(page(1), other, PageCache.SCAN);
            assertSame(other, cache.recall(page(1), PageCache.SCAN), "the second read");
            assertSame(other, cache.recall(page(1), PageCache.SCAN), "the third read");
            // As a collection that took the array would leave the table of refused units
            cache.forgetRefusedForTests();
            var fourth = unit(page(1));
            cache.putScreened(page(1), fourth, PageCache.SCAN);
            assertSame(fourth, cache.peek(page(1)), "a fourth read from the file, after three the doorkeeper recorded");
        }

        @ParameterizedTest(name = "{0}")
        @CsvSource({
                "a point read's leaf, false, 1",
                "a point read's branch of level 1, true, 1",
                "a scan's leaf, false, 3",
                "a scan's branch of level 1, true, 3"
        })
        @DisplayName("caches a unit it refused a read later through recalls than from the file, and at the read a read from the file needs once the collector took the array, after any number of recalls")
        void recallsCacheAReadLater(String name, boolean branch, int screen) {
            // The reads before the one that caches a unit read from the file: one for a point read's leaf, two for its
            // branch of level 1, three for a scan's units
            var needed = screen == PageCache.POINT && branch ? 2 : screen;
            var ref = page(0);
            var cache = screening(LIMIT);
            var held = node(ref, branch);
            cache.putScreened(ref, held, screen);
            for (var read = 2; read <= needed + 1; read++) {
                assertSame(held, cache.recall(ref, screen), "recall at read " + read);
                assertNull(cache.peek(ref), "recall at read " + read);
            }
            assertSame(held, cache.recall(ref, screen), "recall at read " + (needed + 2));
            assertSame(held, cache.peek(ref), "cached through recalls at read " + (needed + 2));
            if (screen == PageCache.POINT) {
                assertEquals(0, cache.doorkeeperForTests().thriceKeysForTests(), "a point read started the third filter");
            }

            // The collector takes the array after some recalls: the reads from the file that follow cache the unit at
            // the read a read from the file needs, or at the next if the recalls passed it
            for (var recalls = 0; recalls <= needed; recalls++) {
                var other = page(10 + recalls);
                var each = screening(LIMIT);
                var first = node(other, branch);
                each.putScreened(other, first, screen);
                for (var recall = 0; recall < recalls; recall++) {
                    assertSame(first, each.recall(other, screen), "recall " + (recall + 1) + " of " + recalls);
                    assertNull(each.peek(other), "recall " + (recall + 1) + " of " + recalls);
                }
                each.forgetRefusedForTests();
                var reads = 1 + recalls;
                while (reads < needed) {
                    each.putScreened(other, node(other, branch), screen);
                    reads++;
                    assertNull(each.peek(other), "read " + reads + " from the file, after " + recalls + " recalls");
                }
                var cached = node(other, branch);
                each.putScreened(other, cached, screen);
                assertSame(cached, each.peek(other), "read " + (reads + 1) + ", from the file, after " + recalls + " recalls");
            }
        }

        @Test
        @DisplayName("starts no filter of the doorkeeper past those a read from the file needs as point reads recall leaves and branches of level 1 to their admission")
        void pointRecallsFilters() {
            var cache = screening(LIMIT);
            var held = new ArrayList<byte[]>();
            for (var i = 0; i < 64; i++) {
                var leaf = unit(page(i));
                held.add(leaf);
                cache.putScreened(page(i), leaf, PageCache.POINT);
                assertSame(leaf, cache.recall(page(i), PageCache.POINT));
                assertSame(leaf, cache.recall(page(i), PageCache.POINT));
                assertSame(leaf, cache.peek(page(i)), "leaf " + i);
            }
            var doorkeeper = cache.doorkeeperForTests();
            assertEquals(0, doorkeeper.twiceKeysForTests(), "the recalls of point reads of leaves started the second filter");
            assertEquals(0, doorkeeper.thriceKeysForTests());
            for (var i = 100; i < 164; i++) {
                var branch = node(page(i), true);
                held.add(branch);
                cache.putScreened(page(i), branch, PageCache.POINT);
                for (var recall = 0; recall < 3; recall++) {
                    assertSame(branch, cache.recall(page(i), PageCache.POINT));
                }
                assertSame(branch, cache.peek(page(i)), "branch " + i);
            }
            assertTrue(doorkeeper.twiceKeysForTests() > 0, "the second filter a point read's branch of level 1 asks for");
            assertEquals(0, doorkeeper.thriceKeysForTests(), "the recalls of point reads of branches started the third filter");
            assertEquals(128, held.size());
        }

        @Test
        @DisplayName("counts a unit a scan refused again after the collector took its first array once in its table of refused units")
        void refusedAgainOnce() {
            var cache = screening(LIMIT);
            var held = new ArrayList<byte[]>();
            for (var i = 0; i < 16; i++) {
                var unit = shortUnit(page(i));
                held.add(unit);
                cache.putScreened(page(i), unit, PageCache.SCAN);
            }
            var slots = cache.refusalSlotsForTests();
            for (var round = 0; round < 20; round++) {
                for (var i = 0; i < 16; i++) {
                    var unit = shortUnit(page(i));
                    held.set(i, unit);
                    cache.refuse(PageCache.dataKey(page(i).page(), page(i).slot()), page(i), unit);
                }
            }
            assertEquals(slots, cache.refusalSlotsForTests(), "the table grew for units it held already");
            assertTrue(cache.refusedForTests() <= 16, cache.refusedForTests() + " slots held for 16 units");
            for (var i = 0; i < 16; i++) {
                assertSame(held.get(i), cache.recall(page(i), PageCache.POINT), "unit " + i);
            }
        }

        @Test
        @DisplayName("recalls nothing for another nonce or length at the same page, for a unit it never refused, or after a clear or the close")
        void recallsOnlyTheUnit() {
            var cache = screening(LIMIT);
            var ref = page(0);
            var unit = unit(ref);
            cache.putScreened(ref, unit, PageCache.POINT);
            assertNull(cache.recall(new PageRef(ref.page(), 1, 9, 9), PageCache.POINT), "another nonce");
            assertNull(cache.recall(new PageRef(ref.page(), 2, ref.nonceHigh(), ref.nonceLow()), PageCache.POINT), "other pages");
            assertNull(cache.recall(page(1), PageCache.POINT), "a unit never refused");
            assertEquals(0, cache.size());
            cache.clearForTests();
            assertNull(cache.recall(ref, PageCache.POINT), "after a clear");
            cache.putScreened(page(2), unit(page(2)), PageCache.POINT);
            var refused = unit(page(3));
            cache.putScreened(page(3), refused, PageCache.POINT);
            cache.close();
            assertNull(cache.recall(page(3), PageCache.POINT), "after the close");
            assertEquals(0, cache.size());
        }

        @Test
        @DisplayName("holds the arrays of the units it refused only weakly, so a collection takes them")
        void refusalsWeak() {
            var cache = screening(LIMIT);
            var held = new ArrayList<WeakReference<byte[]>>();
            for (var i = 0; i < 64; i++) {
                var unit = unit(page(i));
                held.add(new WeakReference<>(unit));
                cache.putScreened(page(i), unit, PageCache.POINT);
            }
            for (var attempt = 0; attempt < 10 && held.stream().anyMatch(reference -> reference.get() != null); attempt++) {
                System.gc();
            }
            assertTrue(held.stream().allMatch(reference -> reference.get() == null), "an array the cache refused stayed reachable");
            for (var i = 0; i < 64; i++) {
                assertNull(cache.recall(page(i), PageCache.POINT));
            }
            assertEquals(0, cache.size());
        }

        @Test
        @DisplayName("grows its table of refused units with the refusals, up to its most slots, and recalls most units still reachable")
        void refusalsGrow() {
            var cache = screening(LIMIT);
            var units = new ArrayList<byte[]>();
            var count = PageCache.MAX_REFUSALS / 4;
            for (var i = 0; i < count; i++) {
                var unit = shortUnit(page(i));
                units.add(unit);
                cache.putScreened(page(i), unit, PageCache.POINT);
            }
            assertEquals(PageCache.MAX_REFUSALS, cache.refusalSlotsForTests());
            var recalled = 0;
            for (var i = 0; i < count; i++) {
                var unit = cache.recall(page(i), PageCache.POINT);
                if (unit != null) {
                    assertSame(units.get(i), unit);
                    recalled++;
                }
            }
            assertTrue(recalled >= count * 3 / 4, recalled + " of " + count + " units recalled");
            for (var i = count; i < 2 * count; i++) {
                cache.putScreened(page(i), shortUnit(page(i)), PageCache.POINT);
            }
            assertEquals(PageCache.MAX_REFUSALS, cache.refusalSlotsForTests(), "the table grew past its most slots");
        }

        @Test
        @DisplayName("reaches its table of refused units only weakly, so a collection takes it though the units stay reachable, and rebuilds it at the next refusal with as many slots, or half as many down to the fewest if it took fewer refusals than an eighth of them")
        void refusalTableWeak() {
            var cache = weakScreening(LIMIT);
            var held = new ArrayList<byte[]>();
            // Pinned while the test reads what it built, so a collection meanwhile does not change it
            cache.pinRefusalsForTests();
            for (var i = 0; i < 1_000; i++) {
                var unit = shortUnit(page(i));
                held.add(unit);
                cache.refuse(PageCache.dataKey(page(i).page(), page(i).slot()), page(i), unit);
            }
            var slots = cache.refusalSlotsForTests();
            var taken = cache.refusalsForTests();
            assertTrue(slots >= 1_024, "the table holds " + slots + " slots for 1,000 refusals");
            cache.unpinRefusalsForTests();
            collectRefusals(cache);
            assertNull(cache.recall(page(0), PageCache.POINT), "a unit still reachable whose table the collector took");
            var expected = taken < slots / 8 ? Math.max(PageCache.MIN_REFUSALS, slots / 2) : slots;
            var fewest = 0;
            for (var next = 2_000; ; next++) {
                cache.pinRefusalsForTests();
                cache.refuse(PageCache.dataKey(page(next).page(), page(next).slot()), page(next), shortUnit(page(next)));
                assertEquals(expected, cache.refusalSlotsForTests(), "the table built after a collection, at refusal " + next);
                assertEquals(1, cache.refusalsForTests());
                cache.unpinRefusalsForTests();
                // Twice at the fewest: a table of the fewest slots is rebuilt at as many
                if (expected == PageCache.MIN_REFUSALS && ++fewest == 2) {
                    break;
                }
                collectRefusals(cache);
                // A table that took one refusal is rebuilt at half its slots, down to the fewest
                expected = Math.max(PageCache.MIN_REFUSALS, expected / 2);
            }
            java.lang.ref.Reference.reachabilityFence(held);
            cache.close();
            assertEquals(0, cache.refusalSlotsForTests(), "the table of a closed cache");
            cache.refuse(PageCache.dataKey(page(0).page(), page(0).slot()), page(0), shortUnit(page(0)));
            assertEquals(0, cache.refusalSlotsForTests(), "a table built after the close");
        }

        @Test
        @DisplayName("builds one table of refused units however many first refusals race to build it, and counts each refusal in it")
        void refusalsRace() throws InterruptedException {
            var cache = screening(LIMIT);
            // Fewer than a quarter of the first table's slots, so no refusal grows it
            var threads = PageCache.MIN_REFUSALS / 8;
            for (var round = 0; round < 50; round++) {
                cache.clearForTests();
                var start = new CountDownLatch(1);
                var done = new CountDownLatch(threads);
                var held = new ArrayList<byte[]>();
                for (var thread = 0; thread < threads; thread++) {
                    var ref = page(round * threads + thread);
                    var unit = shortUnit(ref);
                    held.add(unit);
                    Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            cache.refuse(PageCache.dataKey(ref.page(), ref.slot()), ref, unit);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }
                start.countDown();
                assertTrue(done.await(30, TimeUnit.SECONDS));
                assertEquals(PageCache.MIN_REFUSALS, cache.refusalSlotsForTests(), "round " + round);
                assertEquals(threads, cache.refusalsForTests(), "the refusals the table counted in round " + round);
                java.lang.ref.Reference.reachabilityFence(held);
            }
        }

        @Test
        @DisplayName("empties at a later refusal the slots of the units the collector took")
        void refusalsForgotten() throws InterruptedException {
            var cache = screening(LIMIT);
            var gone = new ArrayList<WeakReference<byte[]>>();
            for (var i = 0; i < 48; i++) {
                var unit = unit(page(i));
                gone.add(new WeakReference<>(unit));
                cache.putScreened(page(i), unit, PageCache.POINT);
            }
            assertTrue(cache.refusedForTests() > 0);
            for (var attempt = 0; attempt < 10 && gone.stream().anyMatch(reference -> reference.get() != null); attempt++) {
                System.gc();
            }
            assertTrue(gone.stream().allMatch(reference -> reference.get() == null), "an array the cache refused stayed reachable");
            var kept = new ArrayList<byte[]>();
            for (var attempt = 0; attempt < 200 && cache.refusedForTests() > kept.size(); attempt++) {
                var unit = unit(page(1_000 + attempt));
                kept.add(unit);
                cache.putScreened(page(1_000 + attempt), unit, PageCache.POINT);
                Thread.sleep(5);
            }
            assertTrue(cache.refusedForTests() <= kept.size(), cache.refusedForTests() + " slots held for " + kept.size() + " units");
        }

        @Test
        @DisplayName("takes back at its first read a unit a read used that the clock evicted, and not one it held only as written or never held")
        void evictedReadmitted() {
            var cache = screening(16L * PageFormat.SIZE);
            // Starts the doorkeeper, which records evictions only once it holds bits
            cache.putScreened(page(100), unit(page(100)), PageCache.POINT);
            // As a read caches it, then units as a checkpoint writes them, more than the cache holds
            cache.put(page(0), unit(page(0)), true);
            for (var i = 1; i < 33; i++) {
                cache.put(page(i), unit(page(i)), false);
            }
            assertNull(cache.peek(page(0)), "the cache kept the unit a read used");
            assertNull(cache.peek(page(1)), "the cache kept the oldest written unit");
            cache.putScreened(page(0), unit(page(0)), PageCache.POINT);
            assertNotNull(cache.peek(page(0)), "an evicted unit a read used, read again");
            cache.putScreened(page(1), unit(page(1)), PageCache.POINT);
            assertNull(cache.peek(page(1)), "an evicted unit cached only as written, read once");
            cache.putScreened(page(200), unit(page(200)), PageCache.POINT);
            assertNull(cache.peek(page(200)), "a unit read once");
        }

        @Test
        @DisplayName("lets go of the doorkeeper's bits as it closes, and a read after it caches and records nothing")
        void close() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)), PageCache.POINT);
            var started = cache.doorkeeperForTests().bytesForTests();
            assertTrue(started > 0 && started <= Doorkeeper.FIRST_KEYS, started + " bytes for its first level");
            cache.close();
            assertEquals(0, cache.doorkeeperForTests().bytesForTests());
            cache.putScreened(page(1), unit(page(1)), PageCache.POINT);
            cache.putScreened(page(1), unit(page(1)), PageCache.POINT);
            assertNull(cache.peek(page(1)));
            assertEquals(0, cache.doorkeeperForTests().bytesForTests());
        }

        @Test
        @DisplayName("caches once a unit that several reads miss at once, and keeps the budget and the clock exact under racing reads of two caches")
        void racingReads() throws Exception {
            var cache = screening(LIMIT);
            var readers = 4;
            for (var key = 0; key < 200; key++) {
                var ref = page(key);
                var start = new CountDownLatch(1);
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < readers; t++) {
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException exception) {
                            throw new IllegalStateException(exception);
                        }
                        for (var read = 0; read < 2; read++) {
                            if (cache.get(ref) == null) {
                                cache.putScreened(ref, unit(ref), PageCache.POINT);
                            }
                        }
                    }));
                }
                start.countDown();
                for (var thread : threads) {
                    thread.join();
                }
                assertNotNull(cache.peek(ref), "a unit every reader read twice, " + key);
            }
            assertEquals(200, cache.clockLengthForTests());
            assertEquals(200L * PageFormat.SIZE, cache.size());

            var budget = new PageCache.Budget();
            var caches = new PageCache[]{PageCache.in(budget, 64L * PageFormat.SIZE), PageCache.in(budget, 64L * PageFormat.SIZE)};
            for (var shared : caches) {
                IdleTicker.unwatch(shared);
                shared.screen(true);
            }
            var failure = new AtomicReference<Throwable>();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 8; t++) {
                var seed = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new java.util.Random(seed);
                        for (var i = 0; i < 20_000; i++) {
                            var shared = caches[random.nextInt(2)];
                            var ref = page(random.nextInt(300));
                            if (shared.get(ref) == null) {
                                shared.putScreened(ref, unit(ref), PageCache.POINT);
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                }));
            }
            for (var thread : threads) {
                thread.join();
            }
            assertNull(failure.get());
            assertEquals(caches[0].size() + caches[1].size(), budget.total());
            assertTrue(budget.total() <= 64L * PageFormat.SIZE, "the caches hold " + budget.total() + " bytes");
            for (var shared : caches) {
                assertEquals(shared.size(), (long) shared.clockLengthForTests() * PageFormat.SIZE);
            }
        }

        @Test
        @DisplayName("caches once a unit that several scans miss four times at once, recalls included, and keeps the clock exact")
        void racingScans() throws Exception {
            var cache = screening(LIMIT);
            var failure = new AtomicReference<Throwable>();
            for (var key = 0; key < 200; key++) {
                var ref = page(key);
                var start = new CountDownLatch(1);
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < 4; t++) {
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var read = 0; read < 4; read++) {
                                if (cache.get(ref) == null && cache.recall(ref, PageCache.SCAN) == null) {
                                    cache.putScreened(ref, unit(ref), PageCache.SCAN);
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.set(throwable);
                        }
                    }));
                }
                start.countDown();
                for (var thread : threads) {
                    thread.join();
                }
                assertNull(failure.get());
                assertNotNull(cache.peek(ref), "a unit every scan read four times, " + key);
            }
            assertEquals(200, cache.clockLengthForTests());
            assertEquals(200L * PageFormat.SIZE, cache.size());
        }

        @Test
        @DisplayName("misses no more often than a cache that takes every read, under uniform lookups of four times the units it holds")
        void uniformLookups() {
            var units = 1_000;
            var screened = screening((long) units * PageFormat.HEADER);
            var plain = PageCache.isolated((long) units * PageFormat.HEADER);
            IdleTicker.unwatch(plain);
            var random = new java.util.Random(3);
            var misses = new long[2];
            for (var i = 0; i < 400_000; i++) {
                var ref = page(random.nextInt(4 * units));
                var counted = i >= 200_000;
                if (screened.get(ref) == null) {
                    screened.putScreened(ref, shortUnit(ref), PageCache.POINT);
                    misses[0] += counted ? 1 : 0;
                }
                if (plain.get(ref) == null) {
                    plain.put(ref, shortUnit(ref), true);
                    misses[1] += counted ? 1 : 0;
                }
            }
            assertTrue(misses[0] <= misses[1] * 103 / 100, misses[0] + " misses screened, " + misses[1] + " not");
            assertTrue(screened.doorkeeperForTests().rotationsForTests() > 0, "the doorkeeper never rotated");
            assertTrue(screened.doorkeeperForTests().bytesForTests() <= TWO_GENERATIONS,
                    "the doorkeeper holds " + screened.doorkeeperForTests().bytesForTests() + " bytes");
        }
    }

    @Nested
    @DisplayName("the doorkeeper")
    class Doorkeepers {
        @Test
        @DisplayName("takes at most 5% of the keys it never saw for seen ones as it grows to the most keys of a 256 MiB cache, and past two rotations")
        void falsePositives() {
            var doorkeeper = new Doorkeeper();
            // One key per 4 KiB page of a 256 MiB limit
            var most = 1 << 16;
            var checkpoints = new int[]{1_023, 2_047, 4_095, 8_191, 16_383, 32_767, most - 1, most + most / 2,
                    2 * most - 1, 2 * most + most / 2, 3 * most - 1};
            var next = 0;
            for (var i = 1; i <= 3 * most; i++) {
                doorkeeper.admit(i * 7_919L, 12_345, most, 1);
                if (next < checkpoints.length && i == checkpoints[next]) {
                    var positives = 0;
                    var unseen = 100_000;
                    for (var probe = 0; probe < unseen; probe++) {
                        if (doorkeeper.holds((1L << 40) + probe, 999)) {
                            positives++;
                        }
                    }
                    assertTrue(positives <= unseen / 20, positives + " of " + unseen + " unseen keys taken for seen ones after "
                            + i + " records, " + doorkeeper.rotationsForTests() + " rotations");
                    next++;
                }
                assertTrue(doorkeeper.bytesForTests() <= 160 << 10, doorkeeper.bytesForTests() + " bytes");
            }
            assertEquals(checkpoints.length, next);
            assertTrue(doorkeeper.rotationsForTests() >= 2, doorkeeper.rotationsForTests() + " rotations");
        }

        @Test
        @DisplayName("forgets no key as it grows")
        void growthKeepsKeys() {
            var doorkeeper = new Doorkeeper();
            var keys = 100_000;
            for (var i = 0; i < keys; i++) {
                doorkeeper.admit(i, 7, 1 << 17, 1);
            }
            assertTrue(doorkeeper.keysForTests() >= keys / 2, "it holds " + doorkeeper.keysForTests() + " keys");
            assertEquals(0, doorkeeper.rotationsForTests());
            for (var i = 0; i < keys; i++) {
                assertTrue(doorkeeper.admit(i, 7, 1 << 17, 1), "key " + i);
            }
        }

        @Test
        @DisplayName("rotates at the most keys it may hold, bounded to two generations, and remembers every key it recorded among the last a generation holds")
        void rotation() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            var keys = 10 * most;
            // A key the filter took for one it saw, a false positive, is not recorded, and may be forgotten
            var recorded = new boolean[keys];
            for (var i = 0; i < keys; i++) {
                recorded[i] = !doorkeeper.admit(i, 7, most, 1);
                assertTrue(doorkeeper.bytesForTests() <= TWO_GENERATIONS, doorkeeper.bytesForTests() + " bytes");
            }
            assertTrue(doorkeeper.rotationsForTests() >= 8, doorkeeper.rotationsForTests() + " rotations");
            assertEquals(most, doorkeeper.keysForTests());
            for (var i = keys - most; i < keys; i++) {
                assertTrue(!recorded[i] || doorkeeper.admit(i, 7, most, 1), "key " + i);
            }
        }

        @Test
        @DisplayName("counts a key read again and again once, so it neither grows nor rotates for it")
        void repeatedKeys() {
            var doorkeeper = new Doorkeeper();
            assertFalse(doorkeeper.admit(5, 5, Doorkeeper.FIRST_KEYS, 1));
            for (var i = 0; i < 100_000; i++) {
                assertTrue(doorkeeper.admit(5, 5, Doorkeeper.FIRST_KEYS, 1));
            }
            assertEquals(0, doorkeeper.rotationsForTests());
            assertEquals(Doorkeeper.LEVEL_KEYS, doorkeeper.keysForTests());
        }

        @Test
        @DisplayName("tells apart a key under two nonces, and records an eviction only once it holds bits")
        void noncesAndEvictions() {
            var doorkeeper = new Doorkeeper();
            doorkeeper.remember(1, 1, Doorkeeper.FIRST_KEYS);
            assertEquals(0, doorkeeper.bytesForTests(), "an eviction started the doorkeeper");
            assertFalse(doorkeeper.admit(1, 1, Doorkeeper.FIRST_KEYS, 1));
            assertFalse(doorkeeper.admit(1, 2, Doorkeeper.FIRST_KEYS, 1));
            assertTrue(doorkeeper.admit(1, 1, Doorkeeper.FIRST_KEYS, 1));
            doorkeeper.remember(2, 1, Doorkeeper.FIRST_KEYS);
            assertTrue(doorkeeper.admit(2, 1, Doorkeeper.FIRST_KEYS, 1));
        }

        @Test
        @DisplayName("counts the reads of a key to three, admits a scan's at the fourth, a point read's branch's at the third and a point read's at the second, and starts its second and third filters only at their first records")
        void counts() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            assertEquals(0, doorkeeper.reads(1, 1));
            assertFalse(doorkeeper.admit(1, 1, most, 1));
            assertEquals(1, doorkeeper.reads(1, 1));
            assertTrue(doorkeeper.holds(1, 1));
            assertTrue(doorkeeper.admit(1, 1, most, 1));
            assertEquals(0, doorkeeper.twiceKeysForTests(), "a point read started the second filter");
            var point = doorkeeper.bytesForTests();

            assertFalse(doorkeeper.admit(2, 1, most, 3));
            assertEquals(1, doorkeeper.reads(2, 1));
            assertEquals(0, doorkeeper.twiceKeysForTests(), "a scan's first read started the second filter");
            assertFalse(doorkeeper.admit(2, 1, most, 3));
            assertEquals(2, doorkeeper.reads(2, 1));
            assertTrue(doorkeeper.twiceKeysForTests() > 0, "the second filter never started");
            assertEquals(0, doorkeeper.thriceKeysForTests(), "a scan's second read started the third filter");
            assertTrue(doorkeeper.bytesForTests() > point, "the bytes of the second filter");
            var twice = doorkeeper.bytesForTests();
            assertFalse(doorkeeper.admit(2, 1, most, 3));
            assertEquals(3, doorkeeper.reads(2, 1));
            assertTrue(doorkeeper.holds(2, 1));
            assertTrue(doorkeeper.thriceKeysForTests() > 0, "the third filter never started");
            assertTrue(doorkeeper.bytesForTests() > twice, "the bytes of the third filter");
            assertEquals(doorkeeper.onceKeysForTests() + doorkeeper.twiceKeysForTests() + doorkeeper.thriceKeysForTests(),
                    doorkeeper.keysForTests());
            assertTrue(doorkeeper.admit(2, 1, most, 3));
            assertTrue(doorkeeper.admit(2, 1, most, 2), "a point read's branch of a key a scan read three times");
            assertTrue(doorkeeper.admit(2, 1, most, 1), "a point read of a key a scan read three times");

            // A key a point read recorded takes two more reads of a scan, and one a scan recorded none of a point read
            assertFalse(doorkeeper.admit(3, 1, most, 1));
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            assertTrue(doorkeeper.admit(3, 1, most, 3));

            // A point read's branch of level 1 is cached at its third read, and one a scan read twice at its first
            assertFalse(doorkeeper.admit(4, 1, most, 2));
            assertFalse(doorkeeper.admit(4, 1, most, 2));
            assertEquals(2, doorkeeper.reads(4, 1));
            assertTrue(doorkeeper.admit(4, 1, most, 2));
            assertFalse(doorkeeper.admit(6, 1, most, 3));
            assertFalse(doorkeeper.admit(6, 1, most, 3));
            assertTrue(doorkeeper.admit(6, 1, most, 2), "a point read's branch a scan read twice");
        }

        @Test
        @DisplayName("never starts its third filter for point reads, of leaves or of branches, expected or not")
        void pointReadsHoldTwoFilters() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            for (var key = 0; key < 4 * most; key++) {
                for (var read = 0; read < 3; read++) {
                    doorkeeper.admit(key, 1, most, 1 + (key & 1));
                }
                doorkeeper.expect(key + (1L << 40), 1, most, 1 + key % 4);
                doorkeeper.admit(key + (1L << 40), 1, most, 2);
                doorkeeper.remember(key + (1L << 41), 1, most);
            }
            assertTrue(doorkeeper.twiceKeysForTests() > 0, "a point read's branch never started the second filter");
            assertEquals(0, doorkeeper.thriceKeysForTests(), "a point read started the third filter");
            assertTrue(doorkeeper.bytesForTests() <= 2 * TWO_GENERATIONS, doorkeeper.bytesForTests() + " bytes");
        }

        @Test
        @DisplayName("records an eviction as a read of a point read, then of a point read's branch once the second filter started, and of a scan once a scan's third read started the third")
        void evictionsCountAll() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            doorkeeper.admit(100, 1, most, 1);
            doorkeeper.remember(1, 1, most);
            assertEquals(1, doorkeeper.reads(1, 1), "an eviction before any second read");
            assertEquals(0, doorkeeper.twiceKeysForTests(), "an eviction started the second filter");
            assertTrue(doorkeeper.admit(1, 1, most, 1));
            doorkeeper.admit(101, 1, most, 2);
            doorkeeper.admit(101, 1, most, 2);
            doorkeeper.remember(2, 1, most);
            assertEquals(2, doorkeeper.reads(2, 1), "an eviction after a second read");
            assertEquals(0, doorkeeper.thriceKeysForTests(), "an eviction started the third filter");
            assertTrue(doorkeeper.admit(2, 1, most, 2), "a point read's branch of an evicted key");
            doorkeeper.admit(102, 1, most, 3);
            doorkeeper.admit(102, 1, most, 3);
            doorkeeper.admit(102, 1, most, 3);
            doorkeeper.remember(3, 1, most);
            assertEquals(3, doorkeeper.reads(3, 1), "an eviction after a scan's third read");
            assertTrue(doorkeeper.admit(3, 1, most, 3), "a scan's read of an evicted key");
        }

        @Test
        @DisplayName("counts a key only the third filter holds, once the second forgot it, as read three times by every read")
        void thriceAloneCounts() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            for (var key = 1; key <= 2; key++) {
                for (var read = 0; read < 3; read++) {
                    assertFalse(doorkeeper.admit(key, 1, most, 3));
                }
            }
            doorkeeper.forgetTwiceForTests(most);
            assertTrue(doorkeeper.twiceKeysForTests() > 0);
            for (var key = 1; key <= 2; key++) {
                assertEquals(3, doorkeeper.reads(key, 1), "key " + key);
            }
            assertTrue(doorkeeper.admit(1, 1, most, 2), "a point read's branch only the third filter holds");
            assertTrue(doorkeeper.admit(2, 1, most, 3), "a scan's read of a key only the third filter holds");
            assertTrue(doorkeeper.admit(2, 1, most, 3), "a scan's read of a key only the third filter holds, again");

            // A key the second filter forgot that only the first holds takes the reads it lacks again, and no more
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            doorkeeper.forgetTwiceForTests(most);
            assertEquals(1, doorkeeper.reads(3, 1));
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            assertFalse(doorkeeper.admit(3, 1, most, 3));
            assertTrue(doorkeeper.admit(3, 1, most, 3));
        }

        @Test
        @DisplayName("hides the three expectations from holds and reads, and counts a key expected after one, two or three reads, or more, as read that often")
        void expectations() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            doorkeeper.expect(1, 1, most, 1);
            assertFalse(doorkeeper.admit(1, 1, most, 1), "an expectation before the first record");
            // Keys 10 + 10 * r + n: expected after r reads, read with n reads needed
            for (var reads = 1; reads <= 4; reads++) {
                for (var needed = 1; needed <= 3; needed++) {
                    doorkeeper.expect(10L * reads + needed, 1, most, reads);
                }
            }
            for (var key = 11; key <= 43; key++) {
                assertFalse(doorkeeper.holds(key, 1), "key " + key + " held");
                assertEquals(0, doorkeeper.reads(key, 1), "key " + key + " read");
            }
            for (var reads = 1; reads <= 4; reads++) {
                for (var needed = 1; needed <= 3; needed++) {
                    var key = 10L * reads + needed;
                    var counted = Math.min(reads, 3);
                    // A key expected after r reads takes the reads it needs past them, one a read
                    for (var read = counted; read < needed; read++) {
                        assertFalse(doorkeeper.admit(key, 1, most, needed), "key " + key + " at read " + (read - counted + 1));
                        assertEquals(read + 1, doorkeeper.reads(key, 1), "key " + key + " after a refusal");
                    }
                    assertTrue(doorkeeper.admit(key, 1, most, needed), "key " + key + " expected after " + reads
                            + " reads, needing " + needed);
                    if (counted >= needed) {
                        assertEquals(0, doorkeeper.reads(key, 1), "a key admitted as expected counts no read");
                    }
                }
            }
        }

        @Test
        @DisplayName("lets go of the three filters as it closes, and records nothing after it")
        void closeAll() {
            var doorkeeper = new Doorkeeper();
            for (var read = 0; read < 3; read++) {
                doorkeeper.admit(1, 1, Doorkeeper.FIRST_KEYS, 3);
            }
            assertTrue(doorkeeper.thriceKeysForTests() > 0);
            doorkeeper.close();
            assertEquals(0, doorkeeper.bytesForTests());
            assertEquals(0, doorkeeper.keysForTests());
            for (var read = 0; read < 4; read++) {
                assertFalse(doorkeeper.admit(2, 1, Doorkeeper.FIRST_KEYS, 3));
            }
            doorkeeper.remember(3, 1, Doorkeeper.FIRST_KEYS);
            doorkeeper.expect(4, 1, Doorkeeper.FIRST_KEYS, 3);
            assertEquals(0, doorkeeper.bytesForTests());
            assertEquals(0, doorkeeper.reads(1, 1));
            assertFalse(doorkeeper.holds(1, 1));
        }

        @Test
        @DisplayName("holds no filter once it closed while scans on many threads start its third")
        void closeRacingThirdFilter() throws Exception {
            for (var round = 0; round < 200; round++) {
                var doorkeeper = new Doorkeeper();
                var most = Doorkeeper.FIRST_KEYS;
                for (var key = 0; key < 64; key++) {
                    doorkeeper.admit(key, 1, most, 3);
                    doorkeeper.admit(key, 1, most, 3);
                }
                assertEquals(0, doorkeeper.thriceKeysForTests());
                var start = new CountDownLatch(1);
                var failure = new AtomicReference<Throwable>();
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < 4; t++) {
                    var first = t * 16;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var key = first; key < first + 16; key++) {
                                doorkeeper.admit(key, 1, most, 3);
                                doorkeeper.remember(key, 1, most);
                                doorkeeper.reads(key, 1);
                            }
                        } catch (Throwable e) {
                            failure.compareAndSet(null, e);
                        }
                    }));
                }
                start.countDown();
                doorkeeper.close();
                for (var thread : threads) {
                    thread.join(TimeUnit.SECONDS.toMillis(30));
                    assertFalse(thread.isAlive());
                }
                assertNull(failure.get());
                assertEquals(0, doorkeeper.bytesForTests(), "bits held after the close, round " + round);
                assertEquals(0, doorkeeper.thriceKeysForTests(), "a third filter started after the close, round " + round);
                assertFalse(doorkeeper.admit(0, 1, most, 3));
                assertEquals(0, doorkeeper.reads(0, 1));
            }
        }

        @Test
        @DisplayName("grows and rotates its three filters each on its own, bounded to two generations each, under scans' reads of many keys")
        void scanRotation() {
            var doorkeeper = new Doorkeeper();
            var most = Doorkeeper.FIRST_KEYS;
            for (var i = 0; i < 10 * most; i++) {
                doorkeeper.admit(i, 7, most, 3);
                if (i % 2 == 0) {
                    doorkeeper.admit(i, 7, most, 3);
                    doorkeeper.admit(i, 7, most, 3);
                }
                assertTrue(doorkeeper.bytesForTests() <= 3 * TWO_GENERATIONS, doorkeeper.bytesForTests() + " bytes");
            }
            assertTrue(doorkeeper.rotationsForTests() >= 12, doorkeeper.rotationsForTests() + " rotations");
            assertEquals(most, doorkeeper.onceKeysForTests());
            assertEquals(most, doorkeeper.twiceKeysForTests());
            assertEquals(most, doorkeeper.thriceKeysForTests());
            var thrice = 0;
            for (var i = 10 * most - most; i < 10 * most; i += 2) {
                if (doorkeeper.reads(i, 7) == 3) {
                    thrice++;
                }
            }
            assertTrue(thrice >= most / 2 * 9 / 10, thrice + " of the last keys read three times counted three times");
        }
    }

    private static PageRef page(int index) {
        return new PageRef(index + 2, 1, 1, index + 2);
    }

    /// A page with the header `ref` names.
    private static byte[] unit(PageRef ref) {
        var unit = new byte[ref.pages() * PageFormat.SIZE];
        PageFile.header(unit, PageFormat.LEAF, 0, ref.page(), ref.pages(), 0, ref.nonceHigh(), ref.nonceLow());
        return unit;
    }

    /// A leaf, or a branch of level 1, with the header `ref` names.
    private static byte[] node(PageRef ref, boolean branch) {
        var unit = new byte[ref.pages() * PageFormat.SIZE];
        PageFile.header(unit, branch ? PageFormat.BRANCH : PageFormat.LEAF, branch ? 1 : 0, ref.page(), ref.pages(), 0,
                ref.nonceHigh(), ref.nonceLow());
        return unit;
    }

    /// The header `ref` names and no more, for tests of many entries: the cache counts the bytes of the array.
    private static byte[] shortUnit(PageRef ref) {
        var unit = new byte[PageFormat.HEADER];
        PageFile.header(unit, PageFormat.LEAF, 0, ref.page(), ref.pages(), 0, ref.nonceHigh(), ref.nonceLow());
        return unit;
    }
}
