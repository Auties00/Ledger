package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The recycling of a store's arrays, and the trim of a pool left idle.
class HeapPoolTest {
    @Test
    @DisplayName("hands back a released array of the size asked for")
    void recycles() {
        var pool = new HeapPool(1 << 20);
        var array = pool.acquire(5_000);
        assertEquals(8_192, array.length);
        pool.release(array);
        assertSame(array, pool.acquire(8_000));
    }

    @Test
    @DisplayName("the process's pool keeps as much as the most any store that joined it asks for, and less once it leaves")
    void processPool() {
        var pool = HeapPool.process();
        var owner = new Object();
        HeapPool.join(owner, 1L << 40);
        assertEquals(1L << 40, pool.maxPooledBytesForTests());
        HeapPool.leave(owner);
        assertTrue(pool.maxPooledBytesForTests() < 1L << 40, "the cap stayed at what the store that left asked");
    }

    @Test
    @DisplayName("frees the arrays of a size once no array of it was acquired between two trims, and keeps them while one was")
    void trimsWhenIdle() {
        // Not the process's trimmer's, which would trim it between the calls here
        var pool = new HeapPool(1 << 20, false);
        pool.release(pool.acquire(4_096));
        pool.release(pool.acquire(65_536));
        pool.trimIfIdle();
        assertEquals(4_096 + 65_536, pool.pooledBytesForTests(), "acquired since the last trim");
        pool.release(pool.acquire(4_096));
        pool.trimIfIdle();
        assertEquals(4_096, pool.pooledBytesForTests(), "only the size acquired since the last trim");
        pool.trimIfIdle();
        assertEquals(0, pool.pooledBytesForTests(), "idle since the last trim");
        // A trimmed pool recycles again
        pool.release(pool.acquire(4_096));
        assertEquals(4_096, pool.pooledBytesForTests());
    }

    @Test
    @DisplayName("hands back a released collectable array, kept apart from the others, and trims them once none was acquired between two trims")
    void recyclesCollectable() {
        var pool = new HeapPool(1 << 20, false);
        pool.holdCollectableForTests(true);
        var array = pool.acquireCollectable(5_000);
        assertEquals(8_192, array.length);
        pool.releaseCollectable(array);
        assertEquals(0, pool.pooledBytesForTests(), "kept with the other arrays");
        assertEquals(8_192, pool.collectableBytesForTests());
        assertSame(array, pool.acquireCollectable(8_000));
        assertNotSame(array, pool.acquire(8_000), "handed out as one of the other arrays");
        pool.releaseCollectable(array);
        // Larger than a pooled size, or past half of what the pool keeps: left to the collector
        pool.releaseCollectable(new byte[(1 << 20) + 1]);
        pool.releaseCollectable(pool.acquireCollectable(1 << 20));
        assertEquals(8_192, pool.collectableBytesForTests());
        pool.trimIfIdle();
        assertEquals(8_192, pool.collectableBytesForTests(), "acquired since the last trim");
        pool.trimIfIdle();
        assertEquals(0, pool.collectableBytesForTests(), "idle since the last trim");
        assertEquals(4_096, pool.acquireCollectable(1).length);
    }

    @Test
    @DisplayName("frees the free collectable arrays at a collection, unless held for a test")
    void collectsCollectable() {
        var pool = new HeapPool(1 << 20, false);
        pool.releaseCollectable(pool.acquireCollectable(4_096));
        assertEquals(4_096, pool.collectableBytesForTests());
        for (var i = 0; i < 50 && pool.collectableBytesForTests() != 0; i++) {
            System.gc();
        }
        assertEquals(0, pool.collectableBytesForTests(), "a collection left the free collectable arrays");
        pool.holdCollectableForTests(true);
        var array = pool.acquireCollectable(4_096);
        pool.releaseCollectable(array);
        System.gc();
        assertSame(array, pool.acquireCollectable(4_096), "a collection freed the held arrays");
        pool.releaseCollectable(array);
        pool.collectForTests();
        assertEquals(0, pool.collectableBytesForTests());
    }

    @Test
    @DisplayName("keeps the free collectable arrays through a collection while a collectable array is out, and frees them at one once every array is back")
    void holdsCollectableWhileOut() {
        var pool = new HeapPool(4 << 20, false);
        var out = pool.acquireCollectable(4_096);
        var arrays = new ArrayList<byte[]>();
        for (var i = 0; i < 8; i++) {
            arrays.add(pool.acquireCollectable(256 << 10));
        }
        arrays.forEach(pool::releaseCollectable);
        arrays.clear();
        assertEquals(1, pool.collectableOutForTests());
        var free = 8L * (256 << 10);
        assertEquals(free, pool.collectableBytesForTests());
        // A collection in the middle of a burst, as a young one the burst's own allocations start
        System.gc();
        System.gc();
        assertEquals(free, pool.collectableBytesForTests(), "a collection freed the free arrays while one was out");
        pool.releaseCollectable(out);
        assertEquals(0, pool.collectableOutForTests());
        for (var i = 0; i < 50 && pool.collectableBytesForTests() != 0; i++) {
            System.gc();
        }
        assertEquals(0, pool.collectableBytesForTests(), "a collection with every array back left the free ones");
    }
}
