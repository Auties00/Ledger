package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The recycling of a store's arrays, the reserve a pool holds strongly and the overflow it keeps only while work holds
/// it, and the trim of a pool left idle. The tests that look for collections run `System.gc()`, a full one.
class HeapPoolTest {
    private static final int CHUNK = 256 << 10;

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

    /// Releases `count` new arrays of `length` bytes into `pool`.
    ///
    /// @return the arrays, by identity
    private static List<byte[]> releaseNew(HeapPool pool, int count, int length) {
        var arrays = new ArrayList<byte[]>();
        for (var i = 0; i < count; i++) {
            var array = new byte[length];
            arrays.add(array);
            pool.release(array);
        }
        return arrays;
    }

    /// Runs collections until `pool` holds no overflow, at most 50.
    private static void collectOverflow(HeapPool pool) {
        for (var i = 0; i < 50 && pool.overflowBytesForTests() != 0; i++) {
            System.gc();
        }
    }

    @Test
    @DisplayName("holds strongly at most a reserve of each size and in all, and keeps the arrays past it, up to its maximum, in the overflow")
    void reserve() {
        var pool = new HeapPool(64L << 20, false);
        var hold = pool.hold();
        releaseNew(pool, 12, CHUNK);
        assertEquals(HeapPool.RESERVE_PER_SIZE, pool.reservedBytesForTests(), "the reserve of a size");
        assertEquals(12L * CHUNK - HeapPool.RESERVE_PER_SIZE, pool.overflowBytesForTests());
        assertEquals(12L * CHUNK, pool.pooledBytesForTests());
        for (var shift = 12; shift <= 20; shift++) {
            releaseNew(pool, (int) (HeapPool.RESERVE_PER_SIZE >> shift), 1 << shift);
        }
        assertEquals(HeapPool.RESERVE, pool.reservedBytesForTests(), "the reserve in all");
        // Half the maximum for a size, its reserve and its overflow together
        releaseNew(pool, 200, CHUNK);
        assertEquals(32L << 20, pool.freeForTests(CHUNK).size() * (long) CHUNK);
        // Past the maximum in all
        releaseNew(pool, 64, HeapPool.MAX_POOLED);
        assertEquals(64L << 20, pool.pooledBytesForTests());
        Reference.reachabilityFence(hold);
    }

    @Test
    @DisplayName("a pool that keeps no more than its reserve keeps every free array strongly")
    void smallPool() {
        var pool = new HeapPool(1 << 20, false);
        releaseNew(pool, 8, 128 << 10);
        assertEquals(512L << 10, pool.reservedBytesForTests(), "half the maximum for a size");
        assertEquals(0, pool.overflowBytesForTests());
        releaseNew(pool, 8, 64 << 10);
        assertEquals(1L << 20, pool.reservedBytesForTests());
        assertEquals(0, pool.overflowBytesForTests());
    }

    @Test
    @DisplayName("hands out the reserve's arrays before the overflow's, and the overflow's before new ones")
    void acquiresReserveFirst() {
        var pool = new HeapPool(64L << 20, false);
        pool.holdOverflowForTests(true);
        var arrays = releaseNew(pool, 6, CHUNK);
        for (var i = 0; i < 6; i++) {
            assertSame(arrays.get(i), pool.acquire(CHUNK), "array " + i);
        }
        assertEquals(0, pool.pooledBytesForTests());
        var fresh = pool.acquire(CHUNK);
        assertTrue(arrays.stream().noneMatch(array -> array == fresh), "an array handed out twice");
    }

    @Test
    @DisplayName("frees the overflow at the first collection that finds no hold on it, and keeps the reserve")
    void collectsOverflow() {
        var pool = new HeapPool(64L << 20, false);
        var arrays = releaseNew(pool, 12, CHUNK);
        var probe = new WeakReference<>(arrays.getLast());
        var reserved = arrays.subList(0, 4).toArray(byte[][]::new);
        arrays.clear();
        for (var i = 0; i < 50 && probe.get() != null; i++) {
            System.gc();
        }
        assertNull(probe.get(), "a collection left an array of the overflow");
        assertEquals(0, pool.overflowBytesForTests());
        assertEquals(4L * CHUNK, pool.reservedBytesForTests());
        for (var array : reserved) {
            assertSame(array, pool.acquire(CHUNK), "a collection freed the reserve");
        }
        // The next overflow is made as arrays go past the reserve again
        var hold = pool.hold();
        releaseNew(pool, 6, CHUNK);
        assertEquals(2L * CHUNK, pool.overflowBytesForTests());
        Reference.reachabilityFence(hold);
    }

    @Test
    @DisplayName("keeps the overflow through collections while a hold is kept, one taken before the overflow too, and frees it once it is dropped")
    void holdsOverflow() {
        var pool = new HeapPool(64L << 20, false);
        // Taken before any array went past the reserve: the overflow the releases make is the one it holds
        var hold = pool.hold();
        var arrays = releaseNew(pool, 12, CHUNK);
        // Collections in the middle of the work, as the young ones its own allocations start
        System.gc();
        System.gc();
        assertEquals(8L * CHUNK, pool.overflowBytesForTests(), "a collection freed the overflow while it was held");
        for (var i = 0; i < 12; i++) {
            assertSame(arrays.get(i), pool.acquire(CHUNK), "array " + i);
        }
        arrays.forEach(pool::release);
        Reference.reachabilityFence(hold);
        hold = null;
        arrays.clear();
        collectOverflow(pool);
        assertEquals(0, pool.overflowBytesForTests(), "a collection with no hold left the overflow");
    }

    @Test
    @DisplayName("a trim gives the collector the overflow's arrays of a size idle since the last trim, held or not, and keeps those of a size taken since")
    void trimsOverflow() {
        var pool = new HeapPool(64L << 20, false);
        var hold = pool.hold();
        pool.release(pool.acquire(CHUNK));
        pool.release(pool.acquire(64 << 10));
        releaseNew(pool, 8, CHUNK);
        releaseNew(pool, 20, 64 << 10);
        pool.trimIfIdle();
        assertEquals(5L * CHUNK + 5L * (64 << 10), pool.overflowBytesForTests(), "acquired since the last trim");
        pool.release(pool.acquire(CHUNK));
        pool.trimIfIdle();
        assertEquals(5L * CHUNK, pool.overflowBytesForTests(), "only the size acquired since the last trim");
        assertEquals(HeapPool.RESERVE_PER_SIZE, pool.reservedBytesForTests());
        pool.trimIfIdle();
        assertEquals(0, pool.pooledBytesForTests(), "idle since the last trim");
        Reference.reachabilityFence(hold);
    }

    @Test
    @DisplayName("a lower maximum gives the collector the free arrays past it, one of 0 moves the reserve to the overflow, and a higher one fills the reserve from the overflow")
    void limits() {
        var pool = new HeapPool(64L << 20, false);
        var hold = pool.hold();
        for (var shift = 12; shift <= 20; shift++) {
            releaseNew(pool, (int) ((2L << 20) >> shift), 1 << shift);
        }
        assertEquals(HeapPool.RESERVE, pool.reservedBytesForTests());
        assertEquals(18L << 20, pool.pooledBytesForTests());
        pool.limit(8L << 20);
        assertEquals(8L << 20, pool.pooledBytesForTests(), "the free arrays in all");
        assertEquals(HeapPool.RESERVE, pool.reservedBytesForTests());
        pool.limit(2L << 20);
        assertEquals(2L << 20, pool.pooledBytesForTests(), "the free arrays in all");
        for (var shift = 12; shift <= 20; shift++) {
            assertTrue(pool.freeForTests(1 << shift).size() * (1L << shift) <= 1L << 20, "the free arrays of " + (1 << shift) + " bytes");
        }
        pool.limit(64L << 20);
        releaseNew(pool, 8, CHUNK);
        var overflow = pool.overflowBytesForTests();
        assertTrue(overflow > 0, "the overflow");
        var pooled = pool.pooledBytesForTests();
        pool.limit(0);
        assertEquals(0, pool.reservedBytesForTests(), "the reserve with no store open");
        assertEquals(pooled, pool.overflowBytesForTests(), "the overflow, the reserve's arrays included");
        releaseNew(pool, 2, CHUNK);
        assertEquals(pooled, pool.pooledBytesForTests(), "a pool that keeps nothing takes nothing back");
        pool.limit(64L << 20);
        var fits = 0L;
        for (var shift = 12; shift <= 20; shift++) {
            fits += Math.min(HeapPool.RESERVE_PER_SIZE, pool.freeForTests(1 << shift).size() * (1L << shift));
        }
        assertEquals(Math.min(HeapPool.RESERVE, fits), pool.reservedBytesForTests(), "the reserve, filled from the overflow");
        assertEquals(pooled, pool.pooledBytesForTests(), "the free arrays in all");
        Reference.reachabilityFence(hold);
    }

    @Test
    @DisplayName("the process's pool hands a store opened after the last one closed the arrays the closed one gave back, before any collection and after one")
    void processPoolReopen() {
        var pool = HeapPool.process();
        var owner = new Object();
        HeapPool.join(owner, 64L << 20);
        // The arrays other tests left, so the reserve has room for the chunks below
        for (var shift = 12; shift <= 20; shift++) {
            while (!pool.freeForTests(1 << shift).isEmpty()) {
                pool.acquire(1 << shift);
            }
        }
        var released = new ArrayList<WeakReference<byte[]>>();
        for (var array : releaseNew(pool, (int) (HeapPool.RESERVE_PER_SIZE / CHUNK), CHUNK)) {
            released.add(new WeakReference<>(array));
        }
        assertEquals(HeapPool.RESERVE_PER_SIZE, pool.reservedBytesForTests(), "the reserve");
        HeapPool.leave(owner);
        if (pool.maxPooledBytesForTests() == 0) {
            assertEquals(0, pool.reservedBytesForTests(), "the reserve with no store open");
            assertEquals(HeapPool.RESERVE_PER_SIZE, pool.overflowBytesForTests(), "the reserve moved to the overflow");
        }
        var reopened = new Object();
        HeapPool.join(reopened, 64L << 20);
        assertEquals(HeapPool.RESERVE_PER_SIZE, pool.reservedBytesForTests(), "the reserve filled from the overflow");
        // The reserve is held strongly again: a collection keeps it
        System.gc();
        for (var reference : released) {
            var array = pool.acquire(CHUNK);
            assertSame(reference.get(), array, "a chunk the closed store gave back");
        }
        HeapPool.leave(reopened);
    }

    @Test
    @DisplayName("the process's pool keeps no more reserve than the stores still open ask for once one leaves, and its overflow until a collection with no hold")
    void processPoolLeave() {
        var pool = HeapPool.process();
        var owner = new Object();
        HeapPool.join(owner, 64L << 20);
        var hold = pool.hold();
        releaseNew(pool, 8, HeapPool.MAX_POOLED);
        assertTrue(pool.overflowBytesForTests() > 0, "the overflow");
        HeapPool.leave(owner);
        var left = pool.maxPooledBytesForTests();
        assertTrue(pool.reservedBytesForTests() <= Math.min(HeapPool.RESERVE, left), "the reserve past what the stores still open ask");
        if (left == 0) {
            assertTrue(pool.overflowBytesForTests() > 0, "the last leave dropped the overflow a hold keeps");
        }
        Reference.reachabilityFence(hold);
        hold = null;
        collectOverflow(pool);
        assertEquals(0, pool.overflowBytesForTests(), "a collection with no hold left the overflow");
    }

    @Test
    @DisplayName("hands no array to two takers at once, nor keeps one free twice, across threads that hold, release past the reserve and trim while collections run")
    void concurrent() throws Exception {
        var pool = new HeapPool(32L << 20, false);
        Set<byte[]> out = ConcurrentHashMap.newKeySet();
        var failures = new ConcurrentLinkedQueue<String>();
        var stop = new AtomicBoolean();
        var start = new CountDownLatch(1);
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 8; t++) {
            var seed = t;
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(seed);
                try {
                    start.await();
                    for (var round = 0; round < 200 && failures.isEmpty(); round++) {
                        var hold = random.nextBoolean() ? pool.hold() : null;
                        var taken = new ArrayList<byte[]>();
                        for (var i = random.nextInt(12); i >= 0; i--) {
                            var array = pool.acquire(1 << (12 + random.nextInt(7)));
                            if (!out.add(array)) {
                                failures.add("an array handed out while out");
                            }
                            array[0] = (byte) seed;
                            array[array.length - 1] = (byte) round;
                            taken.add(array);
                        }
                        Thread.yield();
                        for (var array : taken) {
                            if (array[0] != (byte) seed || array[array.length - 1] != (byte) round) {
                                failures.add("an array written while out");
                            }
                            out.remove(array);
                            pool.release(array);
                        }
                        Reference.reachabilityFence(hold);
                    }
                } catch (InterruptedException exception) {
                    failures.add("interrupted");
                }
            }));
        }
        var collector = Thread.ofPlatform().start(() -> {
            while (!stop.get()) {
                System.gc();
                pool.trimIfIdle();
                LockSupport.parkNanos(1_000_000);
            }
        });
        start.countDown();
        for (var thread : threads) {
            thread.join();
        }
        stop.set(true);
        collector.join();
        assertEquals(List.of(), List.copyOf(failures));
        pool.holdOverflowForTests(true);
        var free = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
        var bytes = 0L;
        for (var shift = 12; shift <= 20; shift++) {
            for (var array : pool.freeForTests(1 << shift)) {
                assertTrue(free.add(array), "an array of " + array.length + " bytes is free twice");
                bytes += array.length;
            }
        }
        assertEquals(bytes, pool.pooledBytesForTests(), "the counts of the free arrays");
        assertTrue(pool.reservedBytesForTests() <= HeapPool.RESERVE);
        assertTrue(pool.pooledBytesForTests() <= 32L << 20);
    }
}
