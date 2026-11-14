package com.github.auties00.ledger.engine;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/// Recycled heap arrays in power-of-two sizes from 4 KiB to 1 MiB, for the memory a store fills and empties over and
/// over: write batches, log buffers and pages.
///
/// The store keeps its memory on the heap because the JDK's AES-GCM runs its intrinsic only on arrays, and copies
/// native memory through small arrays of its own; recycling the arrays keeps that memory from becoming garbage. A
/// request above 1 MiB gets an array of its own, which is not recycled. Arrays come back with the contents of their
/// last use: callers write before they read.
///
/// The pool holds strongly a reserve of free arrays, at most [#RESERVE_PER_SIZE] of each size and [#RESERVE] in all,
/// enough for the arrays the commits, reads and writes of large values of a few stores take and give back one at a
/// time. It keeps the free arrays past the reserve, up to the bytes the stores ask it for, in an overflow of each size
/// it reaches only weakly, unless work that takes many arrays at once [holds][#hold(int)] it: a large batch's rope and
/// the body of a large commit's frame hold the overflow of every size, a checkpoint those of the sizes of its pages and
/// of the units it writes. While any such work holds the overflow of a size, a collection, young or full, keeps it;
/// once none does, the first collection frees it. A large commit gives back tens of megabytes of chunks as its frame is
/// sealed, which a pool that held every free array strongly kept for as long as the store went on taking an array of
/// their size, and after the last store closed; the next large batch holds what is left of them from its first chunk,
/// so large commits that follow one another reuse them, unless a collection ran between the two. A checkpoint that
/// writes no unit of a chunk's size leaves them to that collection: one that held them kept the chunks of a bulk write,
/// tens of megabytes, for as long as it wrote the pages of that write.
///
/// The free arrays of a size none was taken of for a tick of the [IdleTicker], between one and two of its periods, go
/// to the collector, the reserve's and the overflow's alike, so neither a store idle after a checkpoint, in a process
/// of many stores, nor one that goes on taking the small arrays of its commits after a history sync keeps the arrays
/// it no longer takes.
///
/// The arrays of work that comes in bursts, such as the reads ahead of a scan while the file reads from the device or
/// the leaves a resident key space loads, are [collectable][#acquireCollectable(int)]: the pool recycles them as it
/// does the others, and holds the free ones strongly while any collectable array is out, so a collection in the middle
/// of a scan or of a run of leaves, young or full, keeps them; once every one is back, it reaches them only weakly, so
/// the first collection after that frees them. Kept strongly at rest, the sealed and opened arrays of a scan's reads
/// ahead would stay in the pool for as long as scans read ahead, one of each size a round reads into, 520 KiB after
/// 1,000 cold chat pages. A collection between two scans, of any kind, frees them, and the next scan allocates them
/// again, once.
public final class HeapPool {
    private static final int MIN_SHIFT = 12;
    private static final int MAX_SHIFT = 20;
    /// The longest array the pool recycles, 1 MiB; a longer one is the caller's own.
    static final int MAX_POOLED = 1 << MAX_SHIFT;
    /// The count of the sizes the pool recycles, from 4 KiB to 1 MiB, the bound of every [#size(int)].
    static final int SIZES = MAX_SHIFT - MIN_SHIFT + 1;
    /// The most bytes of free arrays of one size the pool holds strongly, 1 MiB: 256 of the smallest arrays, which
    /// reads and small commits take, four chunks of a rope, one of the longest arrays.
    static final long RESERVE_PER_SIZE = 1L << 20;
    /// The most bytes of free arrays the pool holds strongly, 4 MiB: more than the growth arrays of eight writers'
    /// batches, about 2 MiB, take at once.
    static final long RESERVE = 4L << 20;
    // The free arrays of the reserve, of each size
    private final ConcurrentLinkedQueue<byte[]>[] free;
    // The bytes of the reserve's free arrays
    private final AtomicLong pooledBytes;
    // The most of the stores' asks, for the process's pool: the most bytes of free arrays, the reserve's and the
    // overflow's together
    private volatile long maxPooledBytes;
    // The bytes of the reserve's free arrays of each size. A size, its reserve and its overflow together, may not
    // hold more than half of maxPooledBytes: a checkpoint's flush releases its many small arrays at once, and would
    // otherwise leave no room for the large ones
    private final AtomicLong[] pooledBySize;
    // The free arrays past the reserve, of each size, each set of them reached through a reference of its own, which
    // only the holds of work reach strongly besides; replaced, holding the overflow's lock, once the collector cleared
    // it. Its count is its own and read through the reference without reaching the set, so a cleared one needs no
    // fix-up and a sum over the sizes keeps none alive
    private final AtomicReferenceArray<OverflowReference> overflow;
    private final Object overflowLock;
    // Whether the overflow is kept strongly whatever holds it, guarded by the overflow's lock; for tests that find a
    // released array again, or count the arrays a run of work allocates, across collections
    private boolean holdOverflow;
    // The overflow of each size while a test keeps it strongly, guarded by the overflow's lock
    private final Overflow[] heldOverflow;
    // The acquires of each size so far, plain counts: a stale read only delays a trim, or trims a size just used,
    // whose next acquire then allocates
    private final int[] acquires;
    // The counts of acquires the trimmer saw last; the trimmer's alone
    private final int[] acquiresSeen;
    // What the idle ticker calls, which holds it weakly: the pool keeps it as long as it lives
    private final IdleTicker.Watched trimmer;
    // What sees the arrays the pool hands out again and takes back, or null; for tests
    private volatile Watcher watcher;
    // The free collectable arrays, which only this reference reaches; replaced, holding the lock, once the collector
    // cleared it or a trim dropped it
    private volatile WeakReference<Collectable> collectable;
    private final Object collectableLock;
    // The collectable acquires so far, and the count the trimmer saw last, as for the other arrays
    private int collectableAcquires;
    private int collectableAcquiresSeen;
    // The collectable arrays of pooled sizes handed out and not given back yet; while any is, the free ones are held
    // strongly too. One never given back holds them until a trim of an idle pool drops them
    private final AtomicInteger collectableOut;
    // Whether the free collectable arrays are kept strongly whatever is out, guarded by the lock; for tests that count
    // the bytes a burst allocates across collections
    private boolean holdCollectable;
    // The free collectable arrays while they are kept strongly, guarded by the lock
    private Collectable heldCollectable;

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

    /// Records that the store `owner` no longer shares the process's pool, as it closes, which keeps no more free arrays
    /// than the stores still open ask for, as [#limit(long)] does: once the last leaves, every free array is in the
    /// overflow of its size, which the next collection frees unless a store opened before then takes them again.
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
        process().limit(largest);
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
        this.free = new ConcurrentLinkedQueue[SIZES];
        for (var i = 0; i < free.length; i++) {
            free[i] = new ConcurrentLinkedQueue<>();
        }
        this.pooledBytes = new AtomicLong();
        this.maxPooledBytes = maxPooledBytes;
        this.pooledBySize = new AtomicLong[free.length];
        for (var i = 0; i < free.length; i++) {
            pooledBySize[i] = new AtomicLong();
        }
        this.overflow = new AtomicReferenceArray<>(free.length);
        for (var i = 0; i < free.length; i++) {
            overflow.set(i, new OverflowReference(null));
        }
        this.overflowLock = new Object();
        this.heldOverflow = new Overflow[free.length];
        this.acquires = new int[free.length];
        this.acquiresSeen = new int[free.length];
        this.collectable = new WeakReference<>(null);
        this.collectableLock = new Object();
        this.collectableOut = new AtomicInteger();
        this.trimmer = trimmed ? this::trimIfIdle : null;
        if (trimmed) {
            IdleTicker.watch(trimmer);
        }
    }

    /// Keeps at most `maxPooledBytes` of free arrays from now on: the reserve's arrays past its new size move to the
    /// overflow, the reserve takes the overflow's arrays while it has room, and the overflow gives the collector its
    /// arrays past `maxPooledBytes`, unless `maxPooledBytes` is 0.
    ///
    /// With no store open, the pool holds nothing strongly and keeps every free array in the overflow: reached only
    /// weakly unless work still holds it, it goes at the next collection, and a store opened before then takes its
    /// arrays again, its reserve first, as a store opened and closed around each large commit does.
    void limit(long maxPooledBytes) {
        this.maxPooledBytes = maxPooledBytes;
        var perSize = maxPooledBytes / 2;
        var sizeReserve = Math.min(RESERVE_PER_SIZE, perSize);
        var reserve = Math.min(RESERVE, maxPooledBytes);
        for (var i = 0; i < free.length; i++) {
            Overflow set = null;
            while (pooledBySize[i].get() > sizeReserve) {
                var array = pollFree(i);
                if (array == null) {
                    break;
                }
                set = set == null ? overflow(i) : set;
                set.offer(array);
            }
        }
        for (var i = free.length - 1; i >= 0; i--) {
            Overflow set = null;
            while (pooledBytes.get() > reserve) {
                var array = pollFree(i);
                if (array == null) {
                    break;
                }
                set = set == null ? overflow(i) : set;
                set.offer(array);
            }
        }
        for (var i = 0; i < free.length; i++) {
            var set = overflow.get(i).get();
            while (set != null && pooledBySize[i].get() < sizeReserve && pooledBytes.get() < reserve) {
                var array = set.poll();
                if (array == null) {
                    break;
                }
                if (!offerReserve(i, array, sizeReserve, reserve)) {
                    set.offer(array);
                    break;
                }
            }
        }
        if (maxPooledBytes == 0) {
            return;
        }
        for (var i = 0; i < free.length; i++) {
            var set = overflow.get(i).get();
            while (set != null && set.bytes.get() + pooledBySize[i].get() > perSize && set.drop()) {
                // Dropped one more
            }
        }
        var total = overflowBytes();
        for (var i = free.length - 1; i >= 0 && total + pooledBytes.get() > maxPooledBytes; i--) {
            var set = overflow.get(i).get();
            while (set != null && total + pooledBytes.get() > maxPooledBytes) {
                var array = set.poll();
                if (array == null) {
                    break;
                }
                total -= array.length;
            }
        }
    }

    /// {@return a free array of the reserve of the size `index`, taken out of it, or `null` if it has none}
    private byte[] pollFree(int index) {
        var array = free[index].poll();
        if (array != null) {
            pooledBytes.addAndGet(-array.length);
            pooledBySize[index].addAndGet(-array.length);
        }
        return array;
    }

    /// Puts the free `array` of the size `index` in the reserve, if it stays within `sizeReserve` bytes of that size
    /// and `reserve` in all.
    ///
    /// @return whether it went in
    private boolean offerReserve(int index, byte[] array, long sizeReserve, long reserve) {
        var length = array.length;
        if (pooledBySize[index].addAndGet(length) <= sizeReserve) {
            if (pooledBytes.addAndGet(length) <= reserve) {
                free[index].offer(array);
                return true;
            }
            pooledBytes.addAndGet(-length);
        }
        pooledBySize[index].addAndGet(-length);
        return false;
    }

    /// Gives the collector a free array of the reserve of the size `index`.
    ///
    /// @return whether there was one
    private boolean dropFree(int index) {
        return pollFree(index) != null;
    }

    /// {@return an array of at least `capacity` bytes}
    ///
    /// A free array of the reserve comes first, then one of the overflow.
    ///
    /// @param capacity the least length, not negative
    public byte[] acquire(int capacity) {
        if (capacity > MAX_POOLED) {
            return new byte[capacity];
        }
        var shift = shift(capacity);
        var size = shift - MIN_SHIFT;
        acquires[size]++;
        var array = free[size].poll();
        if (array != null) {
            pooledBytes.addAndGet(-array.length);
            pooledBySize[size].addAndGet(-array.length);
        } else {
            var set = overflow.get(size).get();
            if (set == null || (array = set.poll()) == null) {
                return new byte[1 << shift];
            }
        }
        var watching = watcher;
        if (watching != null) {
            watching.acquired(array);
        }
        return array;
    }

    /// Returns `array` to the pool, unless it is not one of the pooled sizes or the pool is full.
    ///
    /// It goes to the reserve while the reserve has room, and to the overflow of its size past it, which the next
    /// collection frees unless work [holds][#hold(int)] it. The bound on the free bytes in all is checked against a sum
    /// of the counts of the reserve and of the overflow of every size that is no snapshot, so it is not enforced
    /// exactly: releases at the same moment, to the reserve or of other sizes, may pass it by an array each.
    ///
    /// @param array an array [#acquire] returned, which the caller no longer uses
    public void release(byte[] array) {
        var length = array.length;
        if (Integer.bitCount(length) != 1 || length < 1 << MIN_SHIFT || length > 1 << MAX_SHIFT) {
            return;
        }
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        var max = maxPooledBytes;
        var perSize = max / 2;
        if (pooledBySize[size].addAndGet(length) <= Math.min(RESERVE_PER_SIZE, perSize)) {
            if (pooledBytes.addAndGet(length) <= Math.min(RESERVE, max)) {
                var watching = watcher;
                if (watching != null) {
                    watching.released(array);
                }
                free[size].offer(array);
                return;
            }
            pooledBytes.addAndGet(-length);
        }
        if (pooledBySize[size].addAndGet(-length) + length > perSize) {
            return;
        }
        var set = overflow(size);
        // Counted in its overflow before the sum over the sizes, so of two releases at once the later sum sees both
        if (set.bytes.addAndGet(length) + pooledBySize[size].get() > perSize || overflowBytes() + pooledBytes.get() > max) {
            set.bytes.addAndGet(-length);
            return;
        }
        var watching = watcher;
        if (watching != null) {
            watching.released(array);
        }
        set.free.offer(array);
    }

    /// {@return what keeps the pool's overflow of every size from the collector for as long as the caller holds it}
    ///
    /// The same as [#hold(int)] for the longest pooled arrays.
    Object hold() {
        return hold(MAX_POOLED);
    }

    /// {@return what keeps the pool's overflow of the arrays of up to `longest` bytes from the collector for as long as
    /// the caller holds it}
    ///
    /// Work that takes many arrays of the pool and gives them back as it goes or as it ends holds the overflow of their
    /// sizes from before its first array until after its last is back, then drops the reference: a collection in the
    /// middle of the work, or one between its end and the start of the next work that holds the overflow, keeps the
    /// arrays the work takes again. The overflow of longer arrays stays reached only weakly, so the first collection
    /// frees it unless other work holds it. Work whose holder becomes unreachable without dropping the reference holds
    /// nothing from then on.
    ///
    /// A hold reaches the overflow of each of its sizes the pool keeps their arrays in for as long as it is held, made
    /// now for a size that has none: the collector clears the pool's reference to an overflow only once nothing holds
    /// it, and the pool makes another only once it did.
    ///
    /// @param longest the length of the longest arrays whose overflow the caller holds, at least 1; arrays of over
    ///                1 MiB have none
    Object hold(int longest) {
        var sizes = Math.min(MAX_SHIFT, shift(longest)) - MIN_SHIFT + 1;
        var sets = new Overflow[sizes];
        for (var i = 0; i < sizes; i++) {
            sets[i] = overflow(i);
        }
        return sets;
    }

    /// {@return what keeps the pool's overflow of the size of the arrays [#acquire(int)] hands out for `capacity`
    /// bytes from the collector for as long as the caller holds it, or `null` if they are the caller's own}
    ///
    /// The same as [#hold(int)] for that size alone: work that takes arrays of a few sizes, which it knows only as it
    /// goes, holds the overflow of each from before it takes the first array of it.
    ///
    /// @param capacity the least length of the arrays, not negative
    Object holdSize(int capacity) {
        var size = size(capacity);
        return size < 0 ? null : overflow(size);
    }

    /// {@return the index of the size of the arrays [#acquire(int)] hands out for `capacity` bytes, below [#SIZES], or
    /// -1 if they are over 1 MiB and not recycled}
    ///
    /// @param capacity the least length of the arrays, not negative
    static int size(int capacity) {
        return capacity > MAX_POOLED ? -1 : shift(capacity) - MIN_SHIFT;
    }

    /// {@return the exponent of the least power of two that is at least `capacity` and 4 KiB}
    private static int shift(int capacity) {
        return Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
    }

    /// {@return the overflow of the size `index`, made if the collector cleared the last}
    private Overflow overflow(int index) {
        var set = overflow.get(index).get();
        if (set != null) {
            return set;
        }
        synchronized (overflowLock) {
            set = overflow.get(index).get();
            if (set == null) {
                set = new Overflow();
                overflow.set(index, new OverflowReference(set));
                if (holdOverflow) {
                    heldOverflow[index] = set;
                }
            }
            return set;
        }
    }

    /// {@return the bytes of the overflow's free arrays of every size, those of an overflow the collector cleared aside}
    ///
    /// It reads the count of each size through its reference, so the sum keeps no overflow alive through a collection
    /// that marks the heap while it runs.
    private long overflowBytes() {
        var total = 0L;
        for (var i = 0; i < free.length; i++) {
            total += overflow.get(i).bytes();
        }
        return total;
    }

    /// {@return an array of at least `capacity` bytes for work that comes in bursts, which goes back to the pool through
    /// [#releaseCollectable(byte\[\])]}
    ///
    /// A burst reuses the arrays of the burst before it, unless a collection ran while none was out.
    ///
    /// @param capacity the least length, not negative
    byte[] acquireCollectable(int capacity) {
        if (capacity > 1 << MAX_SHIFT) {
            return new byte[capacity];
        }
        var shift = shift(capacity);
        collectableAcquires++;
        if (collectableOut.getAndIncrement() == 0) {
            holdCollectable();
        }
        var set = collectable.get();
        if (set != null) {
            var array = set.free[shift - MIN_SHIFT].poll();
            if (array != null) {
                set.bytes[shift - MIN_SHIFT].addAndGet(-array.length);
                return array;
            }
        }
        return new byte[1 << shift];
    }

    /// Returns `array` to the free collectable arrays, unless it is not one of the pooled sizes or the free collectable
    /// arrays of its size hold half the bytes the pool keeps.
    ///
    /// @param array an array [#acquireCollectable(int)] returned, which the caller no longer uses
    void releaseCollectable(byte[] array) {
        var length = array.length;
        if (Integer.bitCount(length) != 1 || length < 1 << MIN_SHIFT || length > 1 << MAX_SHIFT) {
            return;
        }
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        var set = collectable.get();
        if (set == null) {
            synchronized (collectableLock) {
                set = collectable.get();
                if (set == null) {
                    set = new Collectable(free.length);
                    collectable = new WeakReference<>(set);
                    // This array is still counted out, so a set made while others are out is held as they are
                    if (holdCollectable || collectableOut.get() > 0) {
                        heldCollectable = set;
                    }
                }
            }
        }
        if (set.bytes[size].addAndGet(length) > maxPooledBytes / 2) {
            set.bytes[size].addAndGet(-length);
        } else {
            set.free[size].offer(array);
        }
        // Counted back once it is free, so no collection runs between the two with the set reached only weakly
        if (collectableOut.decrementAndGet() == 0) {
            letGoCollectable();
        }
    }

    // Holds the free collectable arrays strongly, as an array is out
    private void holdCollectable() {
        synchronized (collectableLock) {
            if (collectableOut.get() > 0) {
                heldCollectable = collectable.get();
            }
        }
    }

    // Reaches the free collectable arrays only weakly again, as none is out, unless a test holds them
    private void letGoCollectable() {
        synchronized (collectableLock) {
            if (collectableOut.get() == 0 && !holdCollectable) {
                heldCollectable = null;
            }
        }
    }

    /// Gives the free arrays of each size to the collector if no array of that size was acquired since the last call,
    /// and the free collectable arrays if no collectable one was.
    ///
    /// The overflow's free arrays of such a size go too, whether work holds that overflow or not: work that held it a
    /// whole tick without taking an array of its size does not need them.
    void trimIfIdle() {
        for (var i = 0; i < free.length; i++) {
            var seen = acquires[i];
            if (seen == acquiresSeen[i]) {
                while (dropFree(i)) {
                    // Dropped one more
                }
                var set = overflow.get(i).get();
                while (set != null && set.drop()) {
                    // Dropped one more
                }
            }
            acquiresSeen[i] = seen;
        }
        var seen = collectableAcquires;
        if (seen == collectableAcquiresSeen) {
            dropCollectable();
        }
        collectableAcquiresSeen = seen;
    }

    // Leaves the free collectable arrays to the collector
    private void dropCollectable() {
        synchronized (collectableLock) {
            collectable = new WeakReference<>(null);
            heldCollectable = null;
        }
    }

    /// Keeps the free collectable arrays strongly too, so that no collection frees them, or only weakly again; for
    /// tests that count the bytes a burst allocates.
    void holdCollectableForTests(boolean hold) {
        synchronized (collectableLock) {
            holdCollectable = hold;
            heldCollectable = hold || collectableOut.get() > 0 ? collectable.get() : null;
        }
    }

    /// {@return the collectable arrays of pooled sizes handed out and not given back; for tests}
    int collectableOutForTests() {
        return collectableOut.get();
    }

    /// Drops the free collectable arrays, as a collection that found them unreachable does; for tests.
    void collectForTests() {
        dropCollectable();
    }

    /// {@return the bytes of the free collectable arrays; for tests}
    long collectableBytesForTests() {
        var set = collectable.get();
        if (set == null) {
            return 0;
        }
        var total = 0L;
        for (var bytes : set.bytes) {
            total += bytes.get();
        }
        return total;
    }

    /// Keeps the overflow of every size strongly too, so that no collection frees it, or only as holds keep it again;
    /// for tests that find a released array again, or count the arrays a run of work allocates, across collections.
    void holdOverflowForTests(boolean hold) {
        synchronized (overflowLock) {
            holdOverflow = hold;
            for (var i = 0; i < free.length; i++) {
                heldOverflow[i] = hold ? overflow(i) : null;
            }
        }
    }

    /// {@return the bytes of the overflow's free arrays, of every size; for tests}
    long overflowBytesForTests() {
        return overflowBytes();
    }

    /// {@return the overflow of the arrays of `length` bytes, a pooled size, or `null` if it has none; for tests that
    /// look for its collection through a weak reference}
    Object overflowForTests(int length) {
        return overflow.get(Integer.numberOfTrailingZeros(length) - MIN_SHIFT).get();
    }

    /// {@return the bytes of the reserve's free arrays; for tests}
    long reservedBytesForTests() {
        return pooledBytes.get();
    }

    /// The free arrays of one size past the reserve, and the bytes they hold.
    private static final class Overflow {
        final ConcurrentLinkedQueue<byte[]> free;
        final AtomicLong bytes;

        Overflow() {
            this.free = new ConcurrentLinkedQueue<>();
            this.bytes = new AtomicLong();
        }

        /// Gives the collector one of the free arrays.
        ///
        /// @return whether there was one
        boolean drop() {
            return poll() != null;
        }

        /// {@return one of the free arrays, taken out of the overflow, or `null` if it has none}
        byte[] poll() {
            var array = free.poll();
            if (array != null) {
                bytes.addAndGet(-array.length);
            }
            return array;
        }

        /// Puts the free `array` in the overflow, whatever it holds.
        void offer(byte[] array) {
            bytes.addAndGet(array.length);
            free.offer(array);
        }
    }

    /// The pool's weak reference to the overflow of one size, which carries the overflow's count of bytes, so a sum over
    /// the sizes reads it without reaching the overflow.
    private static final class OverflowReference extends WeakReference<Overflow> {
        private final AtomicLong bytes;

        /// Creates a reference to `set`, or one already cleared if `null`.
        OverflowReference(Overflow set) {
            super(set);
            this.bytes = set == null ? new AtomicLong() : set.bytes;
        }

        /// {@return the bytes of the overflow's free arrays, or 0 once the collector cleared it}
        long bytes() {
            return refersTo(null) ? 0 : bytes.get();
        }
    }

    /// The free collectable arrays of each size, and the bytes those of each size hold.
    private static final class Collectable {
        final ConcurrentLinkedQueue<byte[]>[] free;
        final AtomicLong[] bytes;

        @SuppressWarnings("unchecked")
        Collectable(int sizes) {
            this.free = new ConcurrentLinkedQueue[sizes];
            this.bytes = new AtomicLong[sizes];
            for (var i = 0; i < sizes; i++) {
                free[i] = new ConcurrentLinkedQueue<>();
                bytes[i] = new AtomicLong();
            }
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

    /// {@return the arrays of every size taken from the pool so far, collectable ones included; for tests that count
    /// the reads of a file}
    int acquiresForTests() {
        var total = collectableAcquires;
        for (var count : acquires) {
            total += count;
        }
        return total;
    }

    /// {@return the free bytes the pool keeps at most; for tests}
    long maxPooledBytesForTests() {
        return maxPooledBytes;
    }

    /// {@return the bytes of the free arrays, the reserve's and the overflow's, collectable ones aside; for tests}
    long pooledBytesForTests() {
        return pooledBytes.get() + overflowBytesForTests();
    }

    /// {@return the free arrays of `length` bytes, a pooled size, as they stand: the reserve's, then the overflow's;
    /// for tests}
    List<byte[]> freeForTests(int length) {
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        var arrays = new ArrayList<>(free[size]);
        var set = overflow.get(size).get();
        if (set != null) {
            arrays.addAll(set.free);
        }
        return List.copyOf(arrays);
    }
}
