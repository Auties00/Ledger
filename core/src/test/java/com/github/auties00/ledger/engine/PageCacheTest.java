package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The page cache on its own: lookups by nonce, eviction order, the slots of its table, the byte limit and the budget
/// caches share, and the memory of the entries it drops.
class PageCacheTest {
    private static final long LIMIT = 1 << 20;

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
    @DisplayName("refuses the keys the slots keep for themselves")
    void reservedKeys() {
        var cache = PageCache.isolated(LIMIT);
        var superblock = new PageRef(0, 1, 1, 1);
        assertThrows(IllegalArgumentException.class, () -> cache.put(superblock, unit(superblock), true));
        assertThrows(IllegalArgumentException.class, () -> cache.put(-1L, page(0), unit(page(0)), true));
        assertNull(cache.get(superblock));
        cache.remove(0);
        assertEquals(0, cache.size());
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
            var sizes = children.stream().mapToInt(TreeNodes.Child::size).toArray();
            var length = TreeNodes.contentLength(sizes, 0, sizes.length);
            var branch = TreeNodes.writeBranch(children, 0, sizes.length, length);
            PageFile.header(branch, PageFormat.BRANCH, 1, 20, 1, length, 1, 1);
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
