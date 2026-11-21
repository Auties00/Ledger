package com.github.auties00.ledger.engine;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
/// time. It keeps the free arrays past the reserve, up to the bytes the stores ask it for, in an overflow it reaches
/// only weakly, unless work that takes many arrays at once [holds][#hold()] it: a large batch's rope, the body of a
/// large commit's frame. While any such work holds it, a collection, young or full, keeps it; once none does, the
/// first collection frees it. A large commit gives back tens of megabytes of chunks as its frame is sealed, which a
/// pool that held every free array strongly kept for as long as the store went on taking an array of their size, and
/// after the last store closed; the next large batch holds what is left of them from its first chunk, so large commits
/// that follow one another reuse them, unless a collection ran between the two.
///
/// The chunks a large commit's body or a bulk batch's rope gives back go to the reserve within its bounds, as other
/// arrays do, but kept apart ([#releaseBurst(byte\[\])]): such commits come one after another while a client stores
/// large values or a history, and each takes the chunks of the one before it again. Held strongly while such commits
/// go on, the chunks grow old after a few collections and stay; an overflow that a collection between two commits
/// cleared would make the next commit allocate them again, and each young collection during a commit copy them,
/// which, at the rate of a client storing large values one by one, doubles the pauses and makes G1 grow the heap to
/// keep its time down. The first collection after such chunks came back moves them to the overflow, where the next
/// commit takes them again; once commits stop coming, the collection after that frees them. The move runs in the
/// action of a [Cleaner] after that first collection, so a collection right after it may find the chunks still in the
/// reserve, up to [#RESERVE_PER_SIZE] of their size. A body or a rope of more chunks than the reserve has room for,
/// [#BURST_KEPT_CHUNKS], such as that of an app-state snapshot's bulk commit, gives them to the overflow alone
/// ([#releaseHeld(byte\[\])]), as the reserve would keep only a few of them: the next such commit, which holds the
/// overflow from its first chunk, takes them again, and the first collection after the last frees them all.
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
///
/// Above 1 MiB, the reads of the units of the file of large values ([BlobFile]) recycle at most [#LARGE_SPARES]
/// arrays of their own ([#acquireLarge(int)]), reached only weakly, and those of half a region of the G1 collector or
/// more sized to fill whole regions: G1 gives such an array regions of its own, which a dead one keeps committed until
/// a marking frees them, so a read that missed the page cache and allocated the sealed unit, the opened one and the
/// caller's copy kept three of them committed, where a read that copies the value out, before a checkpoint takes the
/// unit or after, allocates the caller's alone ([BlobFile#copyValue(PageRef, int, int)]). A unit of format
/// [PageFormat#RECORDS], which a store that encrypts writes for a value longer than a record, takes no spare on a miss
/// the cache refuses, nor on the replay of a commit that wrote it: its records pass through an array of the pool of
/// [PageFormat#RECORD] bytes, half a region of G1 at regions of 1 MiB, kept in the reserve and reused, and open
/// straight into the caller's array; a read the cache takes opens them into one spare, which the cache keeps. A unit
/// of an earlier format takes a spare for its sealed bytes: a miss the cache refuses takes that one, whose content opens
/// straight into the caller's array, and one the cache takes, or one of a unit of the first format, whose tag ends its
/// pages, takes the sealed and the opened unit, as the replay of a commit's large value of that format does. A read
/// that gives a view of the value, which the caller may keep, still opens the unit into an array of its own. Only an
/// array that never left its reader comes back: one the page cache, a version, a view or a caller received never does,
/// as a reader may still read it.
///
/// The window a checkpoint seals its units into ([PageFile.Writer]), which is no pooled size, comes back apart
/// ([#releaseWindow(byte\[\])]), at most [#WINDOW_SPARES] of them, reached only weakly: the next checkpoint of any store
/// of the process takes it again, unless a collection ran between the two.
public final class HeapPool {
    private static final int MIN_SHIFT = 12;
    private static final int MAX_SHIFT = 20;
    /// The longest array the pool recycles, 1 MiB; a longer one is the caller's own.
    static final int MAX_POOLED = 1 << MAX_SHIFT;
    /// The most bytes of free arrays of one size the pool holds strongly, 1 MiB: 256 of the smallest arrays, which
    /// reads and small commits take, four chunks of a rope, one of the longest arrays.
    static final long RESERVE_PER_SIZE = 1L << 20;
    /// The most chunks of a large commit's body or a bulk batch's rope that go back as a burst's
    /// ([#releaseBurst(byte\[\])]), the four the reserve has room for; the chunks of a longer one go to the overflow
    /// alone ([#releaseHeld(byte\[\])]).
    static final int BURST_KEPT_CHUNKS = (int) (RESERVE_PER_SIZE / FrameBody.CHUNK_BYTES);
    /// The most bytes of free arrays the pool holds strongly, 4 MiB: more than the growth arrays of eight writers'
    /// batches, about 2 MiB, take at once.
    static final long RESERVE = 4L << 20;
    /// The most arrays above [#MAX_POOLED] the pool keeps for the reads of large units: the sealed and the opened
    /// array of one read of a unit of an earlier format than [PageFormat#RECORDS] that missed the cache and that the
    /// cache takes; a miss the cache refuses keeps one. Each more would keep the regions of a unit committed while a
    /// marking is pending.
    static final int LARGE_SPARES = 2;
    /// The most windows of checkpoints' writers the pool keeps ([#releaseWindow(byte\[\])]): one for each of two
    /// checkpoints that run at once, in stores of the process that close or tick together.
    static final int WINDOW_SPARES = 2;
    // Moves the chunks of bursts a pool's reserve keeps to its overflow once a collection took the sentinel it
    // registered
    private static final Cleaner COLLECTIONS = Cleaner.create(Thread.ofVirtual().name("ledger-pool-collections").factory());
    // The region of G1 tests force, or -1 for the JVM's
    private static volatile long forcedRegionForTests = -1;
    // The free arrays of the reserve, of each size
    private final ConcurrentLinkedQueue<byte[]>[] free;
    // The free arrays of the reserve that bursts gave back, of each size, apart from the others so that a collection
    // can move them, and them alone, to the overflow. The reserve's counts and bounds are theirs too
    private final ConcurrentLinkedQueue<byte[]>[] burstFree;
    // Whether a sentinel is registered whose collection moves them; written holding the burst lock
    private volatile boolean burstWatched;
    private final Object burstLock;
    // What a collection of a sentinel runs, holding the pool weakly so that a sentinel never keeps a pool alive; null
    // for a pool whose tests move the arrays themselves
    private final Runnable burstCheck;
    // The bytes of the reserve's free arrays
    private final AtomicLong pooledBytes;
    // The most of the stores' asks, for the process's pool: the most bytes of free arrays, the reserve's and the
    // overflow's together
    private volatile long maxPooledBytes;
    // The bytes of the reserve's free arrays of each size. A size, its reserve and its overflow together, may not
    // hold more than half of maxPooledBytes: a long body or rope releases its many chunks at once, and would otherwise
    // leave no room for the other sizes
    private final AtomicLong[] pooledBySize;
    // The free arrays past the reserve, which only this reference and the holds of work reach; replaced, holding the
    // overflow's lock, once the collector cleared it. Its counts are its own, so a cleared one needs no fix-up
    private volatile WeakReference<Overflow> overflow;
    private final Object overflowLock;
    // Whether the overflow is kept strongly whatever holds it, guarded by the overflow's lock; for tests that find a
    // released array again, or count the arrays a run of work allocates, across collections
    private boolean holdOverflow;
    // The overflow while a test keeps it strongly, guarded by the overflow's lock
    private Overflow heldOverflow;
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
    // Guards largeSpares, largeNext and the counts of large acquires
    private final Object largeLock;
    // The free arrays above MAX_POOLED, each of a length largeLength gives and reached only weakly; or null slots
    private final WeakReference<byte[]>[] largeSpares;
    // The slot the next array given back replaces when every slot holds one
    private int largeNext;
    // The large acquires so far, and the count the trimmer saw last, as for the other arrays
    private int largeAcquires;
    private int largeAcquiresSeen;
    // Guards windowSpares
    private final Object windowLock;
    // The windows the writers of checkpoints gave back, reached only weakly; or null slots
    private final WeakReference<byte[]>[] windowSpares;

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
    /// overflow, which the next collection frees unless a store opened before then takes them again.
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

    /// Creates a pool that keeps at most `maxPooledBytes` of free arrays, and that the [IdleTicker] trims and whose
    /// arrays of bursts collections move only if `trimmed`; for tests that do both themselves.
    @SuppressWarnings("unchecked")
    HeapPool(long maxPooledBytes, boolean trimmed) {
        this.free = new ConcurrentLinkedQueue[MAX_SHIFT - MIN_SHIFT + 1];
        this.burstFree = new ConcurrentLinkedQueue[free.length];
        for (var i = 0; i < free.length; i++) {
            free[i] = new ConcurrentLinkedQueue<>();
            burstFree[i] = new ConcurrentLinkedQueue<>();
        }
        this.burstLock = new Object();
        this.burstCheck = trimmed ? new BurstCheck(this) : null;
        this.pooledBytes = new AtomicLong();
        this.maxPooledBytes = maxPooledBytes;
        this.pooledBySize = new AtomicLong[free.length];
        for (var i = 0; i < free.length; i++) {
            pooledBySize[i] = new AtomicLong();
        }
        this.overflow = new WeakReference<>(null);
        this.overflowLock = new Object();
        this.acquires = new int[free.length];
        this.acquiresSeen = new int[free.length];
        this.collectable = new WeakReference<>(null);
        this.collectableLock = new Object();
        this.collectableOut = new AtomicInteger();
        this.largeLock = new Object();
        this.largeSpares = new WeakReference[LARGE_SPARES];
        this.windowLock = new Object();
        this.windowSpares = new WeakReference[WINDOW_SPARES];
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
        var set = overflow.get();
        for (var i = 0; i < free.length; i++) {
            while (pooledBySize[i].get() > sizeReserve) {
                var array = pollFree(i);
                if (array == null) {
                    break;
                }
                set = set == null ? overflow() : set;
                set.offer(i, array);
            }
        }
        for (var i = free.length - 1; i >= 0; i--) {
            while (pooledBytes.get() > reserve) {
                var array = pollFree(i);
                if (array == null) {
                    break;
                }
                set = set == null ? overflow() : set;
                set.offer(i, array);
            }
        }
        if (set == null) {
            return;
        }
        for (var i = 0; i < free.length; i++) {
            while (pooledBySize[i].get() < sizeReserve && pooledBytes.get() < reserve) {
                var array = set.poll(i);
                if (array == null) {
                    break;
                }
                if (!offerReserve(i, array, sizeReserve, reserve)) {
                    set.offer(i, array);
                    break;
                }
            }
        }
        if (maxPooledBytes == 0) {
            return;
        }
        for (var i = 0; i < free.length; i++) {
            while (set.bytes[i].get() + pooledBySize[i].get() > perSize && set.drop(i)) {
                // Dropped one more
            }
        }
        for (var i = free.length - 1; i >= 0; i--) {
            while (set.total.get() + pooledBytes.get() > maxPooledBytes && set.drop(i)) {
                // Dropped one more
            }
        }
    }

    /// {@return a free array of the reserve of the size `index`, taken out of it, or `null` if it has none}
    private byte[] pollFree(int index) {
        var array = free[index].poll();
        if (array == null) {
            array = burstFree[index].poll();
        }
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
    /// A free array of the reserve comes first, one a burst gave back after the others, then one of the overflow.
    ///
    /// @param capacity the least length, not negative
    public byte[] acquire(int capacity) {
        if (capacity > MAX_POOLED) {
            return new byte[capacity];
        }
        var shift = Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
        var size = shift - MIN_SHIFT;
        acquires[size]++;
        var array = free[size].poll();
        if (array == null) {
            array = burstFree[size].poll();
        }
        if (array != null) {
            pooledBytes.addAndGet(-array.length);
            pooledBySize[size].addAndGet(-array.length);
        } else {
            var set = overflow.get();
            if (set == null || (array = set.free[size].poll()) == null) {
                return new byte[1 << shift];
            }
            set.bytes[size].addAndGet(-array.length);
            set.total.addAndGet(-array.length);
        }
        var watching = watcher;
        if (watching != null) {
            watching.acquired(array);
        }
        return array;
    }

    /// Returns `array` to the pool, unless it is not one of the pooled sizes or the pool is full.
    ///
    /// It goes to the reserve while the reserve has room, and to the overflow past it, which the next collection frees
    /// unless work [holds][#hold()] it.
    ///
    /// @param array an array [#acquire] returned, which the caller no longer uses
    public void release(byte[] array) {
        releaseTo(array, free);
    }

    /// Returns `array`, one of the chunks a large commit's body or a bulk batch's rope of at most
    /// [#BURST_KEPT_CHUNKS] chunks gives back, to the pool, unless it is not one of the pooled sizes or the pool is
    /// full.
    ///
    /// It goes where [#release(byte\[\])] would put it, within the same bounds, but the reserve keeps it apart from
    /// the arrays work takes and gives back one at a time, and the first collection after it moves it to the overflow,
    /// which the next collection frees unless work holds it or took it back. So commits that follow one another take
    /// the chunks of the one before them from the reserve, or, after a collection, from the overflow, and give them
    /// back to the reserve, which holds them through the next collection: the chunks grow old and stay. Once such
    /// commits stop, the first collection moves the reserve's chunks and the one after the move frees them. The move
    /// runs in the action of a [Cleaner] after that first collection, so a collection right after it may find them
    /// still in the reserve, up to [#RESERVE_PER_SIZE] of their size; only one sentinel is registered per move.
    ///
    /// @param array an array [#acquire] returned, which the caller no longer uses
    void releaseBurst(byte[] array) {
        if (releaseTo(array, burstFree)) {
            if (!burstWatched && burstCheck != null) {
                watchCollection();
            }
        }
    }

    /// Returns `array` to the pool as [#release(byte\[\])] does, its reserve's share to `reserve`, the queues of the
    /// arrays work gives back one at a time or of those bursts give back.
    ///
    /// @return whether the reserve took it
    private boolean releaseTo(byte[] array, ConcurrentLinkedQueue<byte[]>[] reserve) {
        var length = array.length;
        if (Integer.bitCount(length) != 1 || length < 1 << MIN_SHIFT || length > 1 << MAX_SHIFT) {
            return false;
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
                reserve[size].offer(array);
                return true;
            }
            pooledBytes.addAndGet(-length);
        }
        if (pooledBySize[size].addAndGet(-length) + length > perSize) {
            return false;
        }
        releaseToOverflow(size, array, perSize, max);
        return false;
    }

    /// Registers a sentinel whose collection runs [#moveBursts()], unless one is registered already.
    ///
    /// Nothing reaches the sentinel, so the next collection, young or full, takes it.
    private void watchCollection() {
        synchronized (burstLock) {
            if (!burstWatched) {
                burstWatched = true;
                COLLECTIONS.register(new Object(), burstCheck);
            }
        }
    }

    /// Moves the reserve's arrays bursts gave back to the overflow, within its bounds, once a collection took the
    /// sentinel [#watchCollection()] registered; the next burst that gives one back registers another.
    ///
    /// A burst that holds the overflow takes them again from there, and the next collection frees what no work holds
    /// or took back. A burst that gives an array back while they move registers a sentinel of its own, so none stays
    /// in the reserve past the collection after it.
    private void moveBursts() {
        synchronized (burstLock) {
            burstWatched = false;
        }
        var max = maxPooledBytes;
        var perSize = max / 2;
        for (var i = 0; i < burstFree.length; i++) {
            byte[] array;
            while ((array = burstFree[i].poll()) != null) {
                pooledBytes.addAndGet(-array.length);
                pooledBySize[i].addAndGet(-array.length);
                offerOverflow(i, array, perSize, max);
            }
        }
    }

    /// What the collection of a sentinel runs: the move of its pool's arrays of bursts, if the collector has not taken
    /// the pool.
    private static final class BurstCheck implements Runnable {
        private final WeakReference<HeapPool> pool;

        BurstCheck(HeapPool pool) {
            this.pool = new WeakReference<>(pool);
        }

        @Override
        public void run() {
            var pool = this.pool.get();
            if (pool != null) {
                pool.moveBursts();
            }
        }
    }

    /// Returns `array`, one of the chunks of a body or a rope of more than [#BURST_KEPT_CHUNKS] or the array of a
    /// checkpoint's unit longer than its window, to the overflow alone, never to the reserve, unless it is not one of
    /// the pooled sizes or the pool is full.
    ///
    /// The caller holds the overflow ([#hold()]) from before its first array goes back until after its last has, so
    /// a collection during its work keeps them and its next commit takes them again; once it ends, the first
    /// collection frees them, where the reserve would keep up to [#RESERVE_PER_SIZE] of each size it used. An array
    /// given back while nothing holds the overflow is left to the next collection.
    ///
    /// @param array an array [#acquire] returned, which the caller no longer uses
    void releaseHeld(byte[] array) {
        var length = array.length;
        if (Integer.bitCount(length) != 1 || length < 1 << MIN_SHIFT || length > 1 << MAX_SHIFT) {
            return;
        }
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        var max = maxPooledBytes;
        var perSize = max / 2;
        if (pooledBySize[size].get() + length > perSize) {
            return;
        }
        releaseToOverflow(size, array, perSize, max);
    }

    /// Puts `array`, of the size `size`, which the caller gives back, in the overflow, unless the size would then hold
    /// more than `perSize` bytes, the reserve's and the overflow's together, or the pool more than `max`.
    private void releaseToOverflow(int size, byte[] array, long perSize, long max) {
        var set = reserveOverflow(size, array.length, perSize, max);
        if (set == null) {
            return;
        }
        var watching = watcher;
        if (watching != null) {
            watching.released(array);
        }
        set.free[size].offer(array);
    }

    /// Moves `array`, a free array of the size `size` taken out of the reserve, to the overflow within the bounds
    /// [#releaseToOverflow(int, byte\[\], long, long)] keeps, or leaves it to the collector past them.
    private void offerOverflow(int size, byte[] array, long perSize, long max) {
        var set = reserveOverflow(size, array.length, perSize, max);
        if (set != null) {
            set.free[size].offer(array);
        }
    }

    /// Counts `length` more bytes of the size `size` in the overflow, if the size then holds at most `perSize` bytes,
    /// the reserve's and the overflow's together, and the pool at most `max`.
    ///
    /// @return the overflow that counts them, or `null` if they are past a bound
    private Overflow reserveOverflow(int size, int length, long perSize, long max) {
        var set = overflow();
        if (set.bytes[size].addAndGet(length) + pooledBySize[size].get() > perSize) {
            set.bytes[size].addAndGet(-length);
            return null;
        }
        if (set.total.addAndGet(length) + pooledBytes.get() > max) {
            set.total.addAndGet(-length);
            set.bytes[size].addAndGet(-length);
            return null;
        }
        return set;
    }

    /// {@return the length of the array [#acquireLarge(int)] gives for `capacity` bytes: if the bytes and the array's
    /// [headroom][ValueSlab#HEADROOM] take half a region of G1 or more, as many whole regions as they take, less the
    /// headroom; else, or if the JVM runs another collector or cannot say, `capacity`}
    ///
    /// G1 gives an array of half a region or more whole regions of its own, so the bytes past `capacity` cost no
    /// memory, and an array of one length serves every unit whose length rounds up to the same regions. A shorter
    /// array shares its region with others, so one of whole regions would take memory its unit does not need, which
    /// the page cache, counting such an array by its unit's pages ([PageCache#charge(byte\[\])]), would not see.
    static int largeLength(int capacity) {
        var region = region();
        if (region <= ValueSlab.HEADROOM || (long) capacity + ValueSlab.HEADROOM < region / 2) {
            return capacity;
        }
        var length = (capacity + ValueSlab.HEADROOM + region - 1) / region * region - ValueSlab.HEADROOM;
        return length > Integer.MAX_VALUE - ValueSlab.HEADROOM ? capacity : (int) length;
    }

    /// {@return the bytes of a region of G1, or 0 if the JVM runs another collector or cannot say}
    ///
    /// The first call asks the JVM, which loads the management classes, tens of milliseconds the first time: only the
    /// arrays of a unit above [#MAX_POOLED] ever ask.
    static long region() {
        var forced = forcedRegionForTests;
        return forced != -1 ? forced : ValueSlab.collectorRegion();
    }

    /// Makes [#region()] give `regionBytes`, 0 for a JVM that runs another collector, or the JVM's answer if `-1`; for
    /// tests.
    static void forceRegionForTests(long regionBytes) {
        forcedRegionForTests = regionBytes;
    }

    /// {@return an array of [#largeLength(int)] bytes, at least `capacity`, for a read of a unit above [#MAX_POOLED],
    /// which goes back through [#releaseLarge(byte\[\])]}
    ///
    /// Only a free array of exactly that length is reused, so the page cache, which counts such an array by the pages
    /// of its unit, never holds one of more regions than its unit needs, nor one of a whole region for a unit G1 would
    /// not give a region of its own.
    ///
    /// @param capacity the least length, above [#MAX_POOLED]
    byte[] acquireLarge(int capacity) {
        var length = largeLength(capacity);
        synchronized (largeLock) {
            largeAcquires++;
            for (var slot = 0; slot < largeSpares.length; slot++) {
                var spare = largeSpares[slot];
                var array = spare == null ? null : spare.get();
                if (array != null && array.length == length) {
                    largeSpares[slot] = null;
                    var watching = watcher;
                    if (watching != null) {
                        watching.acquired(array);
                    }
                    return array;
                }
            }
        }
        return new byte[length];
    }

    /// Keeps `array`, which [#acquireLarge(int)] returned, for the next read of a large unit, reached only weakly: in a
    /// free slot, or in place of the oldest spare. An array of another length is left to the collector.
    ///
    /// @param array an array no one else reaches or will: never one the page cache, a version, a view or a caller
    ///              received, as a reader may still read it, and a later read would write it
    void releaseLarge(byte[] array) {
        var length = array.length;
        if (length <= MAX_POOLED || largeLength(length) != length) {
            return;
        }
        var watching = watcher;
        if (watching != null) {
            watching.released(array);
        }
        synchronized (largeLock) {
            var slot = -1;
            for (var i = 0; i < largeSpares.length; i++) {
                var spare = largeSpares[i];
                if (spare == null || spare.refersTo(null)) {
                    slot = i;
                    break;
                }
            }
            if (slot < 0) {
                slot = largeNext;
                largeNext = (largeNext + 1) % largeSpares.length;
            }
            largeSpares[slot] = new WeakReference<>(array);
        }
    }

    /// {@return the shortest window a checkpoint's writer gave back ([#releaseWindow(byte\[\])]) that holds at least
    /// `capacity` bytes, or `null` if the pool keeps none}
    ///
    /// The window of a checkpoint is the one array it seals its units into, which it holds from its first flush to its
    /// last: the checkpoints of the stores of a process, the closes of many stores one after another or the ticks of
    /// idle ones, take the window of the one before them again, as they took the arrays of their seals from the pool
    /// before they had a window.
    byte[] acquireWindow(int capacity) {
        synchronized (windowLock) {
            var best = -1;
            byte[] taken = null;
            for (var slot = 0; slot < windowSpares.length; slot++) {
                var spare = windowSpares[slot];
                var array = spare == null ? null : spare.get();
                if (array != null && array.length >= capacity && (taken == null || array.length < taken.length)) {
                    best = slot;
                    taken = array;
                }
            }
            if (taken != null) {
                windowSpares[best] = null;
            }
            return taken;
        }
    }

    /// Keeps `array`, a checkpoint's window, for the window of the next checkpoint of any store of the process,
    /// reached only weakly, so the first collection after it frees it, as it frees the overflow: in a free slot, or in
    /// place of the shortest spare if that one is shorter.
    ///
    /// @param array a window whose every flush sealed and wrote its units: one a seal or a write failed on may hold
    ///              the plaintext of a value, which the window of another store's checkpoint would keep
    void releaseWindow(byte[] array) {
        synchronized (windowLock) {
            var slot = 0;
            var shortest = Integer.MAX_VALUE;
            for (var i = 0; i < windowSpares.length && shortest > 0; i++) {
                var spare = windowSpares[i];
                var kept = spare == null ? null : spare.get();
                var length = kept == null ? 0 : kept.length;
                if (length < shortest) {
                    shortest = length;
                    slot = i;
                }
            }
            if (shortest < array.length) {
                windowSpares[slot] = new WeakReference<>(array);
            }
        }
    }

    /// {@return the bytes of the windows the pool keeps; for tests}
    long windowSpareBytesForTests() {
        synchronized (windowLock) {
            var bytes = 0L;
            for (var spare : windowSpares) {
                var array = spare == null ? null : spare.get();
                if (array != null) {
                    bytes += array.length;
                }
            }
            return bytes;
        }
    }

    /// Drops the arrays above [#MAX_POOLED] the pool keeps, so a test starts with no spare an earlier one left; for
    /// tests.
    void dropLargeSparesForTests() {
        synchronized (largeLock) {
            Arrays.fill(largeSpares, null);
            largeNext = 0;
        }
    }

    /// {@return the arrays above [#MAX_POOLED] taken so far, spares or new; for tests that count them}
    int largeAcquiresForTests() {
        synchronized (largeLock) {
            return largeAcquires;
        }
    }

    /// {@return the arrays above [#MAX_POOLED] the pool keeps that the collector has not taken; for tests}
    int largeSparesForTests() {
        synchronized (largeLock) {
            var held = 0;
            for (var spare : largeSpares) {
                if (spare != null && !spare.refersTo(null)) {
                    held++;
                }
            }
            return held;
        }
    }

    /// {@return what keeps the pool's overflow from the collector for as long as the caller holds it}
    ///
    /// Work that takes many arrays of the pool and gives them back as it goes or as it ends holds the overflow from
    /// before its first array until after its last is back, then drops the reference: a collection in the middle of
    /// the work, or one between its end and the start of the next work that holds the overflow, keeps the arrays the
    /// work takes again. Work whose holder becomes unreachable without dropping the reference holds nothing from then
    /// on.
    ///
    /// A hold reaches the overflow the pool keeps its arrays in for as long as it is held: the collector clears the
    /// pool's reference to an overflow only once nothing holds it.
    Object hold() {
        return overflow();
    }

    /// {@return the overflow, made if the collector cleared the last}
    private Overflow overflow() {
        var set = overflow.get();
        if (set != null) {
            return set;
        }
        synchronized (overflowLock) {
            set = overflow.get();
            if (set == null) {
                set = new Overflow(free.length);
                overflow = new WeakReference<>(set);
                if (holdOverflow) {
                    heldOverflow = set;
                }
            }
            return set;
        }
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
        var shift = Math.max(MIN_SHIFT, 32 - Integer.numberOfLeadingZeros(Math.max(1, capacity) - 1));
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
    /// The overflow's free arrays of such a size go too, whether work holds the overflow or not: work that held it a
    /// whole tick without taking an array of that size does not need them. The arrays above [#MAX_POOLED] go if no
    /// read took one.
    void trimIfIdle() {
        var set = overflow.get();
        for (var i = 0; i < free.length; i++) {
            var seen = acquires[i];
            if (seen == acquiresSeen[i]) {
                while (dropFree(i)) {
                    // Dropped one more
                }
                while (set != null && set.drop(i)) {
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
        synchronized (largeLock) {
            if (largeAcquires == largeAcquiresSeen) {
                Arrays.fill(largeSpares, null);
            }
            largeAcquiresSeen = largeAcquires;
        }
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

    /// Keeps the overflow strongly too, so that no collection frees it, or only as holds keep it again; for tests that
    /// find a released array again, or count the arrays a run of work allocates, across collections.
    void holdOverflowForTests(boolean hold) {
        synchronized (overflowLock) {
            holdOverflow = hold;
            heldOverflow = hold ? overflow() : null;
        }
    }

    /// {@return the bytes of the overflow's free arrays; for tests}
    long overflowBytesForTests() {
        var set = overflow.get();
        return set == null ? 0 : set.total.get();
    }

    /// {@return the bytes of the reserve's free arrays; for tests}
    long reservedBytesForTests() {
        return pooledBytes.get();
    }

    /// {@return the bytes of the reserve's free arrays of `length` bytes, a pooled size; for tests}
    long reservedBytesForTests(int length) {
        return pooledBySize[Integer.numberOfTrailingZeros(length) - MIN_SHIFT].get();
    }

    /// {@return the bytes of the reserve's free arrays that bursts gave back; for tests}
    long burstBytesForTests() {
        var total = 0L;
        for (var queue : burstFree) {
            for (var array : queue) {
                total += array.length;
            }
        }
        return total;
    }

    /// {@return whether a sentinel is registered whose collection moves the arrays of bursts; for tests}
    boolean watchingCollectionsForTests() {
        return burstWatched;
    }

    /// Moves the arrays of bursts to the overflow on the caller's thread, as the collection of a sentinel does; for
    /// tests of a pool made untrimmed, which registers no sentinel.
    void collectedForTests() {
        moveBursts();
    }

    /// The free arrays past the reserve, of each size, and the bytes they hold, of each size and in all.
    private static final class Overflow {
        final ConcurrentLinkedQueue<byte[]>[] free;
        final AtomicLong[] bytes;
        final AtomicLong total;

        @SuppressWarnings("unchecked")
        Overflow(int sizes) {
            this.free = new ConcurrentLinkedQueue[sizes];
            this.bytes = new AtomicLong[sizes];
            for (var i = 0; i < sizes; i++) {
                free[i] = new ConcurrentLinkedQueue<>();
                bytes[i] = new AtomicLong();
            }
            this.total = new AtomicLong();
        }

        /// Gives the collector a free array of the size `index`.
        ///
        /// @return whether there was one
        boolean drop(int index) {
            return poll(index) != null;
        }

        /// {@return a free array of the size `index`, taken out of the overflow, or `null` if it has none}
        byte[] poll(int index) {
            var array = free[index].poll();
            if (array != null) {
                bytes[index].addAndGet(-array.length);
                total.addAndGet(-array.length);
            }
            return array;
        }

        /// Puts the free `array` of the size `index` in the overflow, whatever it holds.
        void offer(int index, byte[] array) {
            bytes[index].addAndGet(array.length);
            total.addAndGet(array.length);
            free[index].offer(array);
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

    /// {@return the free arrays of `length` bytes, a pooled size, as they stand: the reserve's, those bursts gave it,
    /// then the overflow's; for tests}
    List<byte[]> freeForTests(int length) {
        var size = Integer.numberOfTrailingZeros(length) - MIN_SHIFT;
        var arrays = new ArrayList<>(free[size]);
        arrays.addAll(burstFree[size]);
        var set = overflow.get();
        if (set != null) {
            arrays.addAll(set.free[size]);
        }
        return List.copyOf(arrays);
    }
}
