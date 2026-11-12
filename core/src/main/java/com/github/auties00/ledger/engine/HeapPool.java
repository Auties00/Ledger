package com.github.auties00.ledger.engine;

import java.util.List;
import java.util.Map;
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
/// The free arrays of a size none was taken of for a tick of the [IdleTicker], between one and two of its periods, go
/// to the collector, so neither a store idle after a checkpoint, in a process of many stores, nor one that goes on
/// taking the small arrays of its commits after a history sync keeps the arrays it no longer takes.
public final class HeapPool {
    private static final int MIN_SHIFT = 12;
    private static final int MAX_SHIFT = 20;
    /// The longest array the pool recycles, 1 MiB; a longer one is the caller's own.
    static final int MAX_POOLED = 1 << MAX_SHIFT;
    private final ConcurrentLinkedQueue<byte[]>[] free;
    private final AtomicLong pooledBytes;
    // The most of the stores' asks, for the process's pool
    private volatile long maxPooledBytes;
    // The bytes each size holds free, which may not take more than half of maxPooledBytes: a checkpoint's flush
    // releases its many small arrays at once, and would otherwise leave no room for the large ones
    private final AtomicLong[] pooledBySize;
    // The acquires of each size so far, plain counts: a stale read only delays a trim, or trims a size just used,
    // whose next acquire then allocates
    private final int[] acquires;
    // The counts of acquires the trimmer saw last; the trimmer's alone
    private final int[] acquiresSeen;
    // What the idle ticker calls, which holds it weakly: the pool keeps it as long as it lives
    private final IdleTicker.Watched trimmer;
    // What sees the arrays the pool hands out again and takes back, or null; for tests
    private volatile Watcher watcher;

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

    /// Creates a pool that keeps at most `maxPooledBytes` of free arrays, and that the [IdleTicker] trims only if
    /// `trimmed`; for tests that trim it themselves.
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
        this.acquires = new int[free.length];
        this.acquiresSeen = new int[free.length];
        this.trimmer = trimmed ? this::trimIfIdle : null;
        if (trimmed) {
            IdleTicker.watch(trimmer);
        }
    }

    /// {@return an array of at least `capacity` bytes}
    ///
    /// @param capacity the least length, not negative
    public byte[] acquire(int capacity) {
        if (capacity > MAX_POOLED) {
            return new byte[capacity];
        }
        var shift = Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
        acquires[shift - MIN_SHIFT]++;
        var array = free[shift - MIN_SHIFT].poll();
        if (array == null) {
            return new byte[1 << shift];
        }
        pooledBytes.addAndGet(-array.length);
        pooledBySize[shift - MIN_SHIFT].addAndGet(-array.length);
        var watching = watcher;
        if (watching != null) {
            watching.acquired(array);
        }
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
        var watching = watcher;
        if (watching != null) {
            watching.released(array);
        }
        free[size].offer(array);
    }

    /// Gives the free arrays of each size to the collector if no array of that size was acquired since the last call.
    void trimIfIdle() {
        for (var i = 0; i < free.length; i++) {
            var seen = acquires[i];
            if (seen == acquiresSeen[i]) {
                byte[] array;
                while ((array = free[i].poll()) != null) {
                    pooledBytes.addAndGet(-array.length);
                    pooledBySize[i].addAndGet(-array.length);
                }
            }
            acquiresSeen[i] = seen;
        }
    }

    /// Makes `watcher` see every array the pool takes back from now on, before another thread can acquire it, and every
    /// array it hands out again, or stops watching if `null`; for tests.
    void watchForTests(Watcher watcher) {
        this.watcher = watcher;
    }

    /// What sees the arrays a pool hands out again and takes back; for tests.
    interface Watcher {
        /// Sees `array`, which the pool held free, as it hands it out.
        void acquired(byte[] array);

        /// Sees `array` as the pool takes it back to hand it out again.
        void released(byte[] array);
    }

    /// {@return the arrays of every size taken from the pool so far; for tests that count the reads of a file}
    int acquiresForTests() {
        var total = 0;
        for (var count : acquires) {
            total += count;
        }
        return total;
    }

    /// {@return the free bytes the pool keeps at most; for tests}
    long maxPooledBytesForTests() {
        return maxPooledBytes;
    }

    /// {@return the bytes of the free arrays; for tests}
    long pooledBytesForTests() {
        return pooledBytes.get();
    }

    /// {@return the free arrays of `length` bytes, a pooled size, as they stand; for tests}
    List<byte[]> freeForTests(int length) {
        return List.copyOf(free[Integer.numberOfTrailingZeros(length) - MIN_SHIFT]);
    }
}
