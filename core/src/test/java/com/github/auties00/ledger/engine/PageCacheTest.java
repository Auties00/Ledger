package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
        assertEquals(64, cache.capacityForTests(), "the slots of departed entries are reused or cleared");
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
            if (i == 10) {
                assertEquals(64, cache.capacityForTests());
                assertNotNull(cache.get(page(1)));
            }
        }
        assertTrue(cache.capacityForTests() > 64, "the table holds " + cache.capacityForTests() + " slots");
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
        for (var i = 0; i < 100_000; i++) {
            cache.put(page(i), unit(page(i)), i % 3 == 0);
            assertEquals(64, cache.capacityForTests());
            assertTrue(cache.tombstonesForTests() <= 48, "the table holds " + cache.tombstonesForTests() + " departed slots");
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
        assertEquals(64, cache.capacityForTests());
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
        assertEquals(64, cache.capacityForTests());
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
        /// {@return a cache the test alone ticks, which screens what its reads miss}
        private static PageCache screening(long limit) {
            var cache = PageCache.isolated(limit);
            IdleTicker.unwatch(cache);
            cache.screen(true);
            return cache;
        }

        @Test
        @DisplayName("caches a unit at its second read, not at its first")
        void secondRead() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)));
            assertNull(cache.peek(page(0)));
            assertEquals(0, cache.size());
            var second = unit(page(0));
            cache.putScreened(page(0), second);
            assertSame(second, cache.peek(page(0)));
            assertTrue(cache.referencedForTests(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
        }

        @Test
        @DisplayName("tells units apart by their nonce, so the unit a checkpoint wrote over a page read before takes two reads")
        void byNonce() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)));
            var rewritten = new PageRef(page(0).page(), 1, 9, 9);
            cache.putScreened(rewritten, unit(rewritten));
            assertNull(cache.peek(rewritten));
            cache.putScreened(rewritten, unit(rewritten));
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
            cache.putScreened(rewritten, unit(rewritten));
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
            cache.putScreened(page(0), unit(page(0)));
            assertSame(written, cache.peek(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
        }

        @Test
        @DisplayName("caches a branch at its first read, recording nothing")
        void branches() {
            var cache = screening(LIMIT);
            var ref = page(0);
            var branch = new byte[PageFormat.SIZE];
            PageFile.header(branch, PageFormat.BRANCH, 1, ref.page(), 1, 0, ref.nonceHigh(), ref.nonceLow());
            cache.putScreened(ref, branch);
            assertSame(branch, cache.peek(ref));
            assertEquals(0, cache.doorkeeperForTests().keysForTests());
        }

        @Test
        @DisplayName("caches at the first read when no file turned screening on, or a file turned it off, and screens no plain put")
        void unscreened() {
            var unturned = PageCache.isolated(LIMIT);
            unturned.putScreened(page(0), unit(page(0)));
            assertNotNull(unturned.peek(page(0)));
            var off = PageCache.isolated(LIMIT);
            off.screen(true);
            off.screen(false);
            off.putScreened(page(0), unit(page(0)));
            assertNotNull(off.peek(page(0)));
            assertNull(off.recall(page(1)), "a refusal of a cache that refuses nothing");
            var warm = screening(LIMIT);
            warm.put(page(0), unit(page(0)), true);
            warm.put(page(1), unit(page(1)), false);
            assertNotNull(warm.peek(page(0)));
            assertNotNull(warm.peek(page(1)));
            assertEquals(0, warm.doorkeeperForTests().keysForTests(), "a put started the doorkeeper");
        }

        @Test
        @DisplayName("recalls the very array of a unit it refused, which its second read then neither reads nor opens, as an entry a read used")
        void recall() {
            var cache = screening(LIMIT);
            var first = unit(page(0));
            cache.putScreened(page(0), first);
            assertNull(cache.peek(page(0)));
            assertSame(first, cache.recall(page(0)));
            assertSame(first, cache.peek(page(0)));
            assertTrue(cache.referencedForTests(page(0)));
            assertEquals(PageFormat.SIZE, cache.size());
            assertNull(cache.recall(page(0)), "a recall of a unit the cache already took");
            assertSame(first, cache.peek(page(0)));
            assertEquals(1, cache.clockLengthForTests());
        }

        @Test
        @DisplayName("recalls nothing for another nonce or length at the same page, for a unit it never refused, or after a clear or the close")
        void recallsOnlyTheUnit() {
            var cache = screening(LIMIT);
            var ref = page(0);
            var unit = unit(ref);
            cache.putScreened(ref, unit);
            assertNull(cache.recall(new PageRef(ref.page(), 1, 9, 9)), "another nonce");
            assertNull(cache.recall(new PageRef(ref.page(), 2, ref.nonceHigh(), ref.nonceLow())), "other pages");
            assertNull(cache.recall(page(1)), "a unit never refused");
            assertEquals(0, cache.size());
            cache.clearForTests();
            assertNull(cache.recall(ref), "after a clear");
            cache.putScreened(page(2), unit(page(2)));
            var refused = unit(page(3));
            cache.putScreened(page(3), refused);
            cache.close();
            assertNull(cache.recall(page(3)), "after the close");
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
                cache.putScreened(page(i), unit);
            }
            for (var attempt = 0; attempt < 10 && held.stream().anyMatch(reference -> reference.get() != null); attempt++) {
                System.gc();
            }
            assertTrue(held.stream().allMatch(reference -> reference.get() == null), "an array the cache refused stayed reachable");
            for (var i = 0; i < 64; i++) {
                assertNull(cache.recall(page(i)));
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
                cache.putScreened(page(i), unit);
            }
            assertEquals(PageCache.MAX_REFUSALS, cache.refusalSlotsForTests());
            var recalled = 0;
            for (var i = 0; i < count; i++) {
                var unit = cache.recall(page(i));
                if (unit != null) {
                    assertSame(units.get(i), unit);
                    recalled++;
                }
            }
            assertTrue(recalled >= count * 3 / 4, recalled + " of " + count + " units recalled");
            for (var i = count; i < 2 * count; i++) {
                cache.putScreened(page(i), shortUnit(page(i)));
            }
            assertEquals(PageCache.MAX_REFUSALS, cache.refusalSlotsForTests(), "the table grew past its most slots");
        }

        @Test
        @DisplayName("empties at a later refusal the slots of the units the collector took")
        void refusalsForgotten() throws InterruptedException {
            var cache = screening(LIMIT);
            var gone = new ArrayList<WeakReference<byte[]>>();
            for (var i = 0; i < 48; i++) {
                var unit = unit(page(i));
                gone.add(new WeakReference<>(unit));
                cache.putScreened(page(i), unit);
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
                cache.putScreened(page(1_000 + attempt), unit);
                Thread.sleep(5);
            }
            assertTrue(cache.refusedForTests() <= kept.size(), cache.refusedForTests() + " slots held for " + kept.size() + " units");
        }

        @Test
        @DisplayName("takes back at its first read a unit a read used that the clock evicted, and not one it held only as written or never held")
        void evictedReadmitted() {
            var cache = screening(16L * PageFormat.SIZE);
            // Starts the doorkeeper, which records evictions only once it holds bits
            cache.putScreened(page(100), unit(page(100)));
            // As a read caches it, then units as a checkpoint writes them, more than the cache holds
            cache.put(page(0), unit(page(0)), true);
            for (var i = 1; i < 33; i++) {
                cache.put(page(i), unit(page(i)), false);
            }
            assertNull(cache.peek(page(0)), "the cache kept the unit a read used");
            assertNull(cache.peek(page(1)), "the cache kept the oldest written unit");
            cache.putScreened(page(0), unit(page(0)));
            assertNotNull(cache.peek(page(0)), "an evicted unit a read used, read again");
            cache.putScreened(page(1), unit(page(1)));
            assertNull(cache.peek(page(1)), "an evicted unit cached only as written, read once");
            cache.putScreened(page(200), unit(page(200)));
            assertNull(cache.peek(page(200)), "a unit read once");
        }

        @Test
        @DisplayName("lets go of the doorkeeper's bits as it closes, and a read after it caches and records nothing")
        void close() {
            var cache = screening(LIMIT);
            cache.putScreened(page(0), unit(page(0)));
            var started = cache.doorkeeperForTests().bytesForTests();
            assertTrue(started > 0 && started <= Doorkeeper.FIRST_KEYS, started + " bytes for its first level");
            cache.close();
            assertEquals(0, cache.doorkeeperForTests().bytesForTests());
            cache.putScreened(page(1), unit(page(1)));
            cache.putScreened(page(1), unit(page(1)));
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
                                cache.putScreened(ref, unit(ref));
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
                                shared.putScreened(ref, unit(ref));
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
                    screened.putScreened(ref, shortUnit(ref));
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
                doorkeeper.admit(i * 7_919L, 12_345, most);
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
                doorkeeper.admit(i, 7, 1 << 17);
            }
            assertTrue(doorkeeper.keysForTests() >= keys / 2, "it holds " + doorkeeper.keysForTests() + " keys");
            assertEquals(0, doorkeeper.rotationsForTests());
            for (var i = 0; i < keys; i++) {
                assertTrue(doorkeeper.admit(i, 7, 1 << 17), "key " + i);
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
                recorded[i] = !doorkeeper.admit(i, 7, most);
                assertTrue(doorkeeper.bytesForTests() <= TWO_GENERATIONS, doorkeeper.bytesForTests() + " bytes");
            }
            assertTrue(doorkeeper.rotationsForTests() >= 8, doorkeeper.rotationsForTests() + " rotations");
            assertEquals(most, doorkeeper.keysForTests());
            for (var i = keys - most; i < keys; i++) {
                assertTrue(!recorded[i] || doorkeeper.admit(i, 7, most), "key " + i);
            }
        }

        @Test
        @DisplayName("counts a key read again and again once, so it neither grows nor rotates for it")
        void repeatedKeys() {
            var doorkeeper = new Doorkeeper();
            assertFalse(doorkeeper.admit(5, 5, Doorkeeper.FIRST_KEYS));
            for (var i = 0; i < 100_000; i++) {
                assertTrue(doorkeeper.admit(5, 5, Doorkeeper.FIRST_KEYS));
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
            assertFalse(doorkeeper.admit(1, 1, Doorkeeper.FIRST_KEYS));
            assertFalse(doorkeeper.admit(1, 2, Doorkeeper.FIRST_KEYS));
            assertTrue(doorkeeper.admit(1, 1, Doorkeeper.FIRST_KEYS));
            doorkeeper.remember(2, 1, Doorkeeper.FIRST_KEYS);
            assertTrue(doorkeeper.admit(2, 1, Doorkeeper.FIRST_KEYS));
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

    /// The header `ref` names and no more, for tests of many entries: the cache counts the bytes of the array.
    private static byte[] shortUnit(PageRef ref) {
        var unit = new byte[PageFormat.HEADER];
        PageFile.header(unit, PageFormat.LEAF, 0, ref.page(), ref.pages(), 0, ref.nonceHigh(), ref.nonceLow());
        return unit;
    }
}
