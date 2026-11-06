package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    @DisplayName("frees its arrays once no array was acquired between two trims, and keeps them while one was")
    void trimsWhenIdle() {
        var pool = new HeapPool(1 << 20);
        pool.release(pool.acquire(4_096));
        pool.release(pool.acquire(65_536));
        pool.trimIfIdle();
        assertEquals(4_096 + 65_536, pool.pooledBytesForTests(), "acquired since the last trim");
        pool.release(pool.acquire(4_096));
        pool.trimIfIdle();
        assertEquals(4_096 + 65_536, pool.pooledBytesForTests(), "acquired since the last trim");
        pool.trimIfIdle();
        assertEquals(0, pool.pooledBytesForTests(), "idle since the last trim");
        // A trimmed pool recycles again
        pool.release(pool.acquire(4_096));
        assertEquals(4_096, pool.pooledBytesForTests());
    }
}
