package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.WeakReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The page cache on its own: lookups by nonce, the byte limit, and the memory of the entries it drops.
class PageCacheTest {
    private static final long LIMIT = 1 << 20;

    @Test
    @DisplayName("finds a unit only under the nonce it was cached with")
    void lookups() {
        var cache = new PageCache(LIMIT);
        var unit = new byte[PageFormat.SIZE];
        cache.put(new PageRef(5, 1, 1, 1), unit, false);
        assertSame(unit, cache.get(new PageRef(5, 1, 1, 1)));
        assertNull(cache.get(new PageRef(5, 1, 1, 2)));
        cache.remove(5);
        assertNull(cache.get(new PageRef(5, 1, 1, 1)));
    }

    @Test
    @DisplayName("stays within its limit")
    void limit() {
        var cache = new PageCache(LIMIT);
        for (var i = 0; i < 200; i++) {
            cache.put(new PageRef(i * 16, 16, 1, i), new byte[16 * PageFormat.SIZE], i % 3 == 0);
            assertTrue(cache.size() <= LIMIT, "holds " + cache.size() + " bytes");
        }
    }

    @Test
    @DisplayName("keeps the clock bounded while pages are replaced and removed under the limit")
    void boundedClock() {
        var cache = new PageCache(LIMIT);
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
        var cache = new PageCache(64L << 20);
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
