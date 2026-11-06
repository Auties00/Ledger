package com.github.auties00.ledger.engine;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/// Recycled heap arrays in power-of-two sizes from 4 KiB to 1 MiB, for the memory a store fills and empties over and
/// over: write batches, log buffers and pages.
///
/// The store keeps its memory on the heap because the JDK's AES-GCM runs its intrinsic only on arrays, and copies
/// native memory through small arrays of its own; recycling the arrays keeps that memory from becoming garbage. A
/// request above 1 MiB gets an array of its own, which is not recycled. Arrays come back with the contents of their
/// last use: callers write before they read.
public final class HeapPool {
    private static final int MIN_SHIFT = 12;
    private static final int MAX_SHIFT = 20;

    private final ConcurrentLinkedQueue<byte[]>[] free;
    private final AtomicLong pooledBytes;
    private final long maxPooledBytes;
    // The bytes each size holds free, which may not take more than half of maxPooledBytes: a checkpoint's flush
    // releases its many small arrays at once, and would otherwise leave no room for the large ones
    private final AtomicLong[] pooledBySize;

    /// Creates a pool that keeps at most `maxPooledBytes` of free arrays.
    ///
    /// @param maxPooledBytes the most memory the free arrays may hold; arrays released past it are left to the
    ///                       collector
    @SuppressWarnings("unchecked")
    public HeapPool(long maxPooledBytes) {
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
    }

    /// {@return an array of at least `capacity` bytes}
    ///
    /// @param capacity the least length, not negative
    public byte[] acquire(int capacity) {
        if (capacity > 1 << MAX_SHIFT) {
            return new byte[capacity];
        }
        var shift = Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
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
}
