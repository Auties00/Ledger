package com.github.auties00.ledger.engine;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/// Recycled heap arrays in power-of-two sizes from 4 KiB to 1 MiB, for the memory a store fills and empties over and
/// over: write batches, log buffers and pages.
///
/// The store keeps its memory on the heap because the JDK's AES-GCM runs its intrinsic only on arrays, and copies
/// native memory through small arrays of its own; recycling the arrays keeps that memory from becoming garbage. A
/// request above 1 MiB gets an array of its own, which is not recycled. Arrays come back with the contents of their
/// last use: callers write before they read.
///
/// A pool no array was taken from for between [#IDLE_MILLIS] and twice that gives its free arrays to the collector:
/// a store whose last checkpoint left a megabyte of sealed arrays in its pool kept them as long as it stayed idle,
/// which a process of many stores, most of them idle, paid for each.
public final class HeapPool {
    private static final int MIN_SHIFT = 12;
    private static final int MAX_SHIFT = 20;
    /// How long a pool goes without an acquire, at least, before it frees its arrays.
    static final long IDLE_MILLIS = 30_000;

    private final ConcurrentLinkedQueue<byte[]>[] free;
    private final AtomicLong pooledBytes;
    // The most of the stores' asks, for the process's pool
    private volatile long maxPooledBytes;
    // The bytes each size holds free, which may not take more than half of maxPooledBytes: a checkpoint's flush
    // releases its many small arrays at once, and would otherwise leave no room for the large ones
    private final AtomicLong[] pooledBySize;
    // The acquires so far, a plain count: a stale read only delays a trim, or trims a pool just used, whose next
    // acquire then allocates
    private int acquires;
    // The count of acquires the trimmer saw last; the trimmer's alone
    private int acquiresSeen;

    // The pool the stores of the process share, made with the first; guarded by the class
    private static HeapPool process;
    // The stores that share it and the free bytes each asked it to keep, weakly; guarded by the class
    private static final Map<Object, Long> OWNERS = new WeakHashMap<>();

    /// {@return the pool the stores of the process share}
    ///
    /// It keeps as many free bytes as the most any store that [joined][#join(Object, long)] it asked for, and none
    /// once every one left. A pool of each store's own let a process of many busy stores keep a pool for each.
    static HeapPool process() {
        synchronized (HeapPool.class) {
            if (process == null) {
                process = new HeapPool(0);
            }
            return process;
        }
    }

    /// Records that the store `owner` shares the process's pool and asks it to keep `maxPooledBytes` of free arrays;
    /// it is held weakly, so a store never closed stops counting once the collector takes it.
    static void join(Object owner, long maxPooledBytes) {
        synchronized (HeapPool.class) {
            OWNERS.put(owner, maxPooledBytes);
            resize();
        }
    }

    /// Records that the store `owner` no longer shares the process's pool, as it closes.
    static void leave(Object owner) {
        synchronized (HeapPool.class) {
            OWNERS.remove(owner);
            resize();
        }
    }

    // Holding the class's lock
    private static void resize() {
        var largest = 0L;
        for (var asked : OWNERS.values()) {
            largest = Math.max(largest, asked);
        }
        process().maxPooledBytes = largest;
    }

    /// Creates a pool that keeps at most `maxPooledBytes` of free arrays.
    ///
    /// @param maxPooledBytes the most memory the free arrays may hold; arrays released past it are left to the
    ///                       collector
    public HeapPool(long maxPooledBytes) {
        this(maxPooledBytes, true);
    }

    /// Creates a pool that keeps at most `maxPooledBytes` of free arrays, and that the trimmer of the process trims
    /// only if `trimmed`; for tests that trim it themselves.
    @SuppressWarnings("unchecked")
    HeapPool(long maxPooledBytes, boolean trimmed) {
        this.free = new ConcurrentLinkedQueue[MAX_SHIFT - MIN_SHIFT + 1];
        for (var i = 0; i < free.length; i++) {
            free[i] = new ConcurrentLinkedQueue<>();
        }
        this.pooledBytes = new AtomicLong();
        this.maxPooledBytes = maxPooledBytes;
        this.pooledBySize = new AtomicLong[free.length];
        for (var i = 0; i < free.length; i++) {
            pooledBySize[i] = new AtomicLong();
        }
        if (trimmed) {
            Trimmer.watch(this);
        }
    }

    /// {@return an array of at least `capacity` bytes}
    ///
    /// @param capacity the least length, not negative
    public byte[] acquire(int capacity) {
        if (capacity > 1 << MAX_SHIFT) {
            return new byte[capacity];
        }
        var shift = Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
        acquires++;
        var array = free[shift - MIN_SHIFT].poll();
        if (array == null) {
            return new byte[1 << shift];
        }
        pooledBytes.addAndGet(-array.length);
        pooledBySize[shift - MIN_SHIFT].addAndGet(-array.length);
        return array;
    }

    /// Returns `array` to the pool, unless it is not one of the pooled sizes or the pool is full.
    ///
    /// @param array an array [#acquire] returned, which the caller no longer uses
    public void release(byte[] array) {
        var length = array.length;
        if (Integer.bitCount(length) != 1 || length < 1 << MIN_SHIFT || length > 1 << MAX_SHIFT) {
            return;
        }
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        if (pooledBySize[size].addAndGet(length) > maxPooledBytes / 2) {
            pooledBySize[size].addAndGet(-length);
            return;
        }
        if (pooledBytes.addAndGet(length) > maxPooledBytes) {
            pooledBytes.addAndGet(-length);
            pooledBySize[size].addAndGet(-length);
            return;
        }
        free[size].offer(array);
    }

    /// Gives the free arrays to the collector if no array was acquired since the last call.
    void trimIfIdle() {
        var seen = acquires;
        if (seen == acquiresSeen) {
            for (var i = 0; i < free.length; i++) {
                byte[] array;
                while ((array = free[i].poll()) != null) {
                    pooledBytes.addAndGet(-array.length);
                    pooledBySize[i].addAndGet(-array.length);
                }
            }
        }
        acquiresSeen = seen;
    }

    /// {@return the free bytes the pool keeps at most; for tests}
    long maxPooledBytesForTests() {
        return maxPooledBytes;
    }

    /// {@return the bytes of the free arrays; for tests}
    long pooledBytesForTests() {
        return pooledBytes.get();
    }

    /// The thread of the process that trims idle pools, started with the first pool.
    private static final class Trimmer {
        // The pools of the process, weakly: a store's pool goes when the store does
        private static final Set<HeapPool> POOLS = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

        static {
            Thread.ofVirtual().name("ledger-pool-trimmer").start(Trimmer::run);
        }

        private Trimmer() {
        }

        static void watch(HeapPool pool) {
            POOLS.add(pool);
        }

        private static void run() {
            while (true) {
                try {
                    Thread.sleep(IDLE_MILLIS);
                } catch (InterruptedException _) {
                    return;
                }
                for (var pool : POOLS.toArray(HeapPool[]::new)) {
                    pool.trimIfIdle();
                }
            }
        }
    }
}
