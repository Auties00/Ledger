package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The page cache on its own: lookups by nonce, the byte limit and the budget caches share, and the memory of the
/// entries it drops.
class PageCacheTest {
    private static final long LIMIT = 1 << 20;

    @Test
    @DisplayName("finds a unit only under the nonce it was cached with")
    void lookups() {
        var cache = PageCache.isolated(LIMIT);
        var unit = new byte[PageFormat.SIZE];
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
            cache.put(new PageRef(i, 1, 1, i), new byte[PageFormat.SIZE], i % 2 == 0);
        }
        // Less than a whole period without reads: marked, not evicted
        cache.tick();
        assertEquals(8L * PageFormat.SIZE, cache.size());
        var read = cache.get(new PageRef(3, 1, 1, 3));
        cache.tick();
        assertEquals(PageFormat.SIZE, cache.size());
        assertSame(read, cache.get(new PageRef(3, 1, 1, 3)));
        assertNull(cache.get(new PageRef(2, 1, 1, 2)));
        cache.tick();
        cache.tick();
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("stays within its limit")
    void limit() {
        var cache = PageCache.isolated(LIMIT);
        for (var i = 0; i < 200; i++) {
            cache.put(new PageRef(i * 16, 16, 1, i), new byte[16 * PageFormat.SIZE], i % 3 == 0);
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
            busy.put(new PageRef(i, 1, 1, i), new byte[unit], true);
        }
        assertTrue(busy.size() >= LIMIT - unit, "the busy cache holds " + busy.size() + " bytes of " + LIMIT);
        for (var i = 0; i < 1_000; i++) {
            idle.put(new PageRef(i, 1, 1, i), new byte[unit], true);
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
            forgotten.put(new PageRef(i, 1, 1, i), new byte[PageFormat.SIZE], true);
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
            cache.put(new PageRef(i % 4, 1, 1, i), new byte[PageFormat.SIZE], true);
            if (i % 2 == 0) {
                cache.remove((i + 1) % 4);
            }
        }
        assertTrue(cache.clockLengthForTests() <= 2 * 4 + 64 + 1, "the clock holds " + cache.clockLengthForTests() + " entries");
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
        var unit = new byte[PageFormat.SIZE];
        cache.put(new PageRef(page, 1, 1, 1), unit, true);
        return new WeakReference<>(unit);
    }
}
