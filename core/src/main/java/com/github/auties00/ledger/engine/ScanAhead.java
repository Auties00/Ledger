package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/// Reads the leaves a [TreeCursor] comes to next and the value blobs they name, on threads of its own, so the reads and
/// the decryption of a long scan that misses the page cache overlap with the work on the entries.
///
/// The threads are platform threads because the cursor waits for them: on virtual threads a scan would stall for as long
/// as other virtual threads keep every carrier busy, and for good if those wait on something the scan's caller holds.
///
/// The workers walk the tree ahead of the cursor over a copy of its path, claim the next leaves in the cursor's order
/// and publish what they fetched in one slot per leaf. The cursor takes the slots in order, one for each leaf it comes
/// to, so the slots bound how far the workers go, as does a limit of bytes.
///
/// A worker never decides what a scan sees. It reads through the same authenticated checks as any read, and whatever it
/// could not fetch stays out of its slot: the cursor reads that unit itself, and if the unit is damaged, or the store
/// closed, the failure reaches the scan from that read, on the scan's thread, at the unit it belongs to; a scan that
/// stops before the unit never meets it. Any failure in a worker, wherever it comes from, stops the workers and is
/// otherwise dropped.
///
/// A worker leaves the state of the page cache alone, but for the branches on its walk, which every read of the tree
/// caches: it looks units up with [PageFile#peek(PageRef, byte)] and never caches a leaf or a blob. The cursor applies
/// the same rule to a fetched unit as to one it reads, when it comes to it.
///
/// The cursor must [#close()] this before the read that holds its pages back from reuse ends: the workers read units of
/// that read's tree, which a later checkpoint may free once it ended.
final class ScanAhead {
    // A power of two: the slot of a leaf is its position in the scan modulo this
    private static final int SLOTS = 8;
    private static final int SLOT_MASK = SLOTS - 1;
    /// The bytes of blobs, with the leaves read so far, past which a worker fetches no more blobs and claims no more
    /// leaves: the slots alone would let leaves of many blobs hold megabytes, outside the budget of the page cache.
    static final long MAX_BYTES = 512 << 10;
    /// The bytes the leaves of the slots may add to [#MAX_BYTES], as a worker reads the leaves it claimed anyway.
    static final long MAX_LEAF_BYTES = (long) SLOTS * PageFormat.SIZE;
    /// The largest blob a worker fetches: a larger one takes a read of its own whichever thread runs it, so reading it
    /// early saves little for the memory it holds.
    private static final int MAX_BLOB_BYTES = 64 << 10;
    /// The leaves in consecutive pages a worker claims at once, and reads with one call: more would leave too few slots
    /// for the other workers.
    private static final int CLAIM = 2;
    // How long the cursor spins for its leaf before it parks. The workers stay a few microseconds ahead at best in the
    // part of a scan that misses the cache, where a park and an unpark for every leaf would cost more than they overlap
    private static final long CURSOR_SPIN_NANOS = 50_000;
    // How long a worker spins for a free slot before it parks, about the time the cursor takes to free half of them
    private static final long WORKER_SPIN_NANOS = 20_000;
    // Bounds on a park that a publish or a free slot ends: past them a thread looks again for the end of the walk, which
    // wakes nobody
    private static final long CURSOR_PARK_NANOS = 1_000_000;
    private static final long WORKER_PARK_NANOS = 50_000_000;
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    // Every long scan of the process that reads ahead takes processors from everything else the process does
    private static final Semaphore ACTIVE = new Semaphore(Math.max(1, PROCESSORS / 2));
    private static final AtomicLong STARTS = new AtomicLong();

    private final PageFile pages;
    private final MemorySegment from;
    private final MemorySegment to;
    private final boolean descending;
    private final Thread scanner;
    private final Thread[] threads;
    // Guards the walk below and the claims
    private final Object lock;
    private final byte[][] path;
    private final int[] indexes;
    private int depth;
    // The first key of the leaf claimed last, in a descending scan with a lower bound
    private MemorySegment lowest;
    // The leaves claimed so far; written under the lock
    private volatile long claimed;
    // The position in the scan of the next leaf the cursor takes; written by the cursor
    private volatile long taken;
    // The bytes the slots hold, or that the workers reserved for them
    private final AtomicLong held;
    // Set under the lock once no worker claims any more leaves
    private volatile boolean ended;
    // Set once the workers should drop what they did not read yet
    private volatile boolean stopped;
    private volatile boolean failed;
    // The position of the leaf the parked cursor waits for, or -1
    private volatile long awaited;
    // A bit for each worker parked until a slot is free
    private final AtomicInteger sleeping;
    private final AtomicReferenceArray<Fetched> slots;
    // The stop flag, as PageFile.readUncached takes it
    private final BooleanSupplier stop;
    // Read and written by the cursor
    private boolean closed;
    // What the next publish of a worker throws; for tests
    private volatile Error publishFailure;

    /// When a cursor reads ahead.
    ///
    /// @param admitted the leaves and blobs the cursor reads into the cache first, without reading ahead
    /// @param workers the threads that read ahead, 0 for none
    /// @param missRun the leaves and blobs in a row that must miss the cache before the cursor starts
    /// @param idleRun the leaves in a row the workers must find nothing to fetch for before the cursor stops them
    /// @param withRoom whether the cursor starts even while the cache has room for what it reads
    record Settings(int admitted, int workers, int missRun, int idleRun, boolean withRoom) {
        /// What a scan runs with.
        ///
        /// A run of misses tells the part of a scan the cache does not hold from a stray miss in a part it does, where
        /// the workers would find nothing to read. With two processors or fewer the workers would take time from the
        /// scan's own thread and the log's. Three workers read about as fast as a scan goes through the entries they
        /// read, so a fourth would only wait for slots.
        static final Settings DEFAULT = new Settings(TreeCursor.ADMITTED, PROCESSORS <= 2 ? 0 : Math.min(3, PROCESSORS - 1), 16, 32, false);
    }

    /// What the workers fetched for one leaf, safely published by its slot.
    ///
    /// @param ref the leaf
    /// @param leaf the plaintext of the leaf, or `null` if the cursor must find it itself: the cache held it, it is a
    ///             delta record, or a worker failed or stopped first
    /// @param blobs the plaintext of the blobs fetched, by the index of their entry in the leaf, or `null` if there are
    ///              none
    /// @param bytes the bytes the slots counted for this
    record Fetched(PageRef ref, byte[] leaf, byte[][] blobs, long bytes) {
        /// {@return the plaintext of the blob of the leaf's entry at `index`, or `null` if it was not fetched}
        byte[] blob(int index) {
            return blobs == null ? null : blobs[index];
        }

        /// {@return whether the workers fetched nothing for the leaf}
        boolean isEmpty() {
            return leaf == null && blobs == null;
        }
    }

    private ScanAhead(PageFile pages, MemorySegment from, MemorySegment to, boolean descending, byte[][] path, int[] indexes,
                      int depth, int workers) {
        this.pages = pages;
        this.from = from;
        this.to = to;
        this.descending = descending;
        this.scanner = Thread.currentThread();
        this.lock = new Object();
        this.path = path.clone();
        this.indexes = indexes.clone();
        this.depth = depth;
        this.lowest = descending && from != null ? TreeNodes.childKey(path[depth - 1], indexes[depth - 1]) : null;
        this.held = new AtomicLong();
        this.awaited = -1;
        this.sleeping = new AtomicInteger();
        this.slots = new AtomicReferenceArray<>(SLOTS);
        this.stop = () -> stopped;
        this.threads = new Thread[workers];
        for (var i = 0; i < workers; i++) {
            threads[i] = Thread.ofPlatform().daemon().name("ledger-scan-ahead").unstarted(new Worker(i));
        }
    }

    /// Starts reading the leaves after the one a cursor just came to, which child `indexes[depth - 1]` of the branch
    /// `path[depth - 1]` names.
    ///
    /// @return the reader, or `null` if the settings have no workers, or the long scans of the process already take the
    ///         processors they may
    static ScanAhead start(PageFile pages, MemorySegment from, MemorySegment to, boolean descending, byte[][] path, int[] indexes,
                           int depth, Settings settings) {
        if (settings.workers() == 0 || !ACTIVE.tryAcquire()) {
            return null;
        }
        STARTS.incrementAndGet();
        ScanAhead ahead = null;
        try {
            ahead = new ScanAhead(pages, from, to, descending, path, indexes, depth, settings.workers());
            for (var thread : ahead.threads) {
                thread.start();
            }
            return ahead;
        } catch (RuntimeException | Error _) {
            // A scan goes on without a reader ahead, on its own thread
            if (ahead != null) {
                ahead.close();
            } else {
                ACTIVE.release();
            }
            return null;
        }
    }

    /// {@return what the workers fetched for the leaf `ref`, the next the cursor comes to, or `null` if they will not
    /// fetch it: they stopped or reached the end of the range, the walks disagree, or the thread is interrupted}
    ///
    /// It waits for the workers while they read the leaf, and never throws.
    Fetched next(PageRef ref) {
        var position = taken;
        var slot = (int) position & SLOT_MASK;
        var fetched = slots.get(slot);
        if (fetched == null) {
            fetched = await(position, slot);
            if (fetched == null) {
                return null;
            }
        }
        if (!fetched.ref().equals(ref)) {
            failed = true;
            return null;
        }
        slots.set(slot, null);
        taken = position + 1;
        held.addAndGet(-fetched.bytes());
        // Once half the slots are free, and one worker, which wakes the next: an unpark costs the cursor more than it
        // takes to go through a leaf
        if (sleeping.get() != 0 && claimed - (position + 1) <= SLOTS / 2 && held.get() <= MAX_BYTES / 2) {
            wakeWorker();
        }
        return fetched;
    }

    private Fetched await(long position, int slot) {
        var deadline = System.nanoTime() + CURSOR_SPIN_NANOS;
        do {
            var fetched = slots.get(slot);
            if (fetched != null) {
                return fetched;
            }
            if (ended && position >= claimed) {
                return null;
            }
            Thread.onSpinWait();
        } while (System.nanoTime() < deadline);
        awaited = position;
        try {
            while (true) {
                var fetched = slots.get(slot);
                if (fetched != null) {
                    return fetched;
                }
                if ((ended && position >= claimed) || Thread.currentThread().isInterrupted()) {
                    return null;
                }
                if (!workersAlive()) {
                    // The last of them may have published the leaf since the look at the slot
                    return slots.get(slot);
                }
                LockSupport.parkNanos(this, CURSOR_PARK_NANOS);
            }
        } finally {
            awaited = -1;
        }
    }

    private boolean workersAlive() {
        for (var thread : threads) {
            if (thread.isAlive()) {
                return true;
            }
        }
        return false;
    }

    private void wakeWorker() {
        var asleep = sleeping.get();
        if (asleep != 0) {
            var worker = Integer.numberOfTrailingZeros(asleep);
            var bit = 1 << worker;
            if ((sleeping.getAndAccumulate(~bit, (left, right) -> left & right) & bit) != 0) {
                LockSupport.unpark(threads[worker]);
            }
        }
    }

    /// {@return whether a worker failed or the walks disagreed, so reading ahead is of no more use to this scan}
    boolean failed() {
        return failed;
    }

    /// {@return the bytes the slots hold now; for tests}
    long heldForTests() {
        return held.get();
    }

    /// {@return how many readers ahead started so far in the process; for tests}
    static long startsForTests() {
        return STARTS.get();
    }

    /// {@return how many more readers ahead the process may run now; for tests}
    static int availableForTests() {
        return ACTIVE.availablePermits();
    }

    /// Makes the next publish of a worker throw `failure`; for tests.
    void failPublishForTests(Error failure) {
        publishFailure = failure;
    }

    /// Stops the workers and waits for them to end, whatever the interrupt status of the thread, which it keeps.
    ///
    /// A worker in the middle of a read finishes it first. Calling this again does nothing.
    void close() {
        if (closed) {
            return;
        }
        closed = true;
        synchronized (lock) {
            stopped = true;
            ended = true;
        }
        for (var thread : threads) {
            LockSupport.unpark(thread);
        }
        var interrupted = false;
        for (var thread : threads) {
            while (true) {
                try {
                    thread.join();
                    break;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        ACTIVE.release();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Ends the workers after a failure, and wakes every thread that may be parked until one of them does something.
    private void abandon() {
        synchronized (lock) {
            failed = true;
            stopped = true;
            ended = true;
        }
        for (var thread : threads) {
            LockSupport.unpark(thread);
        }
        LockSupport.unpark(scanner);
    }

    private boolean hasRoom() {
        return claimed - taken < SLOTS && held.get() < MAX_BYTES;
    }

    /// {@return whether the slots took `bytes` more bytes}
    private boolean reserve(long bytes) {
        while (true) {
            var current = held.get();
            if (current + bytes > MAX_BYTES) {
                return false;
            }
            if (held.compareAndSet(current, current + bytes)) {
                return true;
            }
        }
    }

    /// {@return the next leaf of the walk, or `null` at the end of the tree or of the range}
    private PageRef nextLeaf() {
        var step = descending ? -1 : 1;
        while (depth > 0) {
            var unit = path[depth - 1];
            var child = indexes[depth - 1] + step;
            if (child >= 0 && child < TreeNodes.count(unit)) {
                indexes[depth - 1] = child;
                while (TreeNodes.level(unit) > 1) {
                    unit = pages.read(TreeNodes.childRef(unit, child), PageFile.NODE, true);
                    child = descending ? TreeNodes.count(unit) - 1 : 0;
                    path[depth] = unit;
                    indexes[depth++] = child;
                }
                return pastRange(unit, child) ? null : TreeNodes.childRef(unit, child);
            }
            depth--;
        }
        return null;
    }

    /// {@return the leaf after `previous` if the same branch names it and it is in the page next to it, or `null`}
    private PageRef nextConsecutive(PageRef previous) {
        var unit = path[depth - 1];
        var child = indexes[depth - 1] + (descending ? -1 : 1);
        if (previous.isRecord() || child < 0 || child >= TreeNodes.count(unit)) {
            return null;
        }
        var ref = TreeNodes.childRef(unit, child);
        if (ref.isRecord() || Math.abs(ref.page() - previous.page()) != 1 || pastRange(unit, child)) {
            return null;
        }
        indexes[depth - 1] = child;
        return ref;
    }

    /// {@return whether every key of child `child` of the branch `unit`, the next of the walk, is past the range}
    private boolean pastRange(byte[] unit, int child) {
        if (!descending) {
            return to != null && Segments.compare(TreeNodes.childKey(unit, child), to) >= 0;
        }
        if (from == null) {
            return false;
        }
        // The keys of a child are below the first key of the child after it
        var past = Segments.compare(lowest, from) <= 0;
        lowest = TreeNodes.childKey(unit, child);
        return past;
    }

    /// One of the threads that read ahead, and what it claimed.
    private final class Worker implements Runnable {
        private final int bit;
        // The leaves of the current claim, the first at position first of the scan
        private final PageRef[] claim;
        private int count;
        private long first;
        // The leaves of the claim to read from the file
        private final PageRef[] missing;
        // The leaves read from the file, which the cursor takes
        private final byte[][] read;
        // The leaves the cache holds, which name blobs but are for the cursor to find
        private final byte[][] peeked;
        private final byte[][][] blobs;
        private final long[] bytes;
        // The blobs of the claim to read, the leaf of the claim each is for and the index of its entry there
        private PageRef[] blobRefs;
        private byte[][] blobUnits;
        private int[] blobLeaves;
        private int[] blobEntries;

        Worker(int index) {
            this.bit = 1 << index;
            this.claim = new PageRef[CLAIM];
            this.missing = new PageRef[CLAIM];
            this.read = new byte[CLAIM][];
            this.peeked = new byte[CLAIM][];
            this.blobs = new byte[CLAIM][][];
            this.bytes = new long[CLAIM];
            this.blobRefs = new PageRef[16];
            this.blobUnits = new byte[16][];
            this.blobLeaves = new int[16];
            this.blobEntries = new int[16];
        }

        @Override
        public void run() {
            try {
                while (claim()) {
                    try {
                        fetch();
                    } catch (Throwable _) {
                        // The slots hold what was read, and the cursor meets this failure if it reads the same unit
                        abandon();
                    }
                    publish();
                }
            } catch (Throwable _) {
                // The leaves this worker claimed never reach their slots: once the other workers end, the cursor stops
                // waiting for them and reads on alone
                abandon();
            }
        }

        /// Claims the next leaves in the order of the scan, waiting for the cursor to free a slot.
        ///
        /// @return whether it claimed any; `false` if the workers are to end
        private boolean claim() {
            while (true) {
                synchronized (lock) {
                    if (ended) {
                        return false;
                    }
                    if (hasRoom()) {
                        return take();
                    }
                }
                awaitRoom();
            }
        }

        private boolean take() {
            first = claimed;
            count = 0;
            PageRef next;
            try {
                next = nextLeaf();
            } catch (Throwable _) {
                // A branch on the walk: the cursor reads it too, if it gets there
                abandon();
                return false;
            }
            if (next == null) {
                ended = true;
                return false;
            }
            claim[count++] = next;
            while (count < CLAIM && first + count - taken < SLOTS && (next = nextConsecutive(next)) != null) {
                claim[count++] = next;
            }
            claimed = first + count;
            if (sleeping.get() != 0 && hasRoom()) {
                wakeWorker();
            }
            return true;
        }

        private void awaitRoom() {
            var deadline = System.nanoTime() + WORKER_SPIN_NANOS;
            while (System.nanoTime() < deadline) {
                if (ended || hasRoom()) {
                    return;
                }
                Thread.onSpinWait();
            }
            sleeping.getAndAccumulate(bit, (left, right) -> left | right);
            if (!ended && !hasRoom()) {
                LockSupport.parkNanos(ScanAhead.this, WORKER_PARK_NANOS);
            }
            sleeping.getAndAccumulate(~bit, (left, right) -> left & right);
        }

        /// Reads what the leaves of the claim need: those the cache does not hold, then the blobs they name that it does
        /// not hold, within the limit of bytes.
        private void fetch() {
            for (var i = 0; i < count; i++) {
                var ref = claim[i];
                if (ref.isRecord()) {
                    // A delta record goes through the read that makes its leaf and caches the parts
                    continue;
                }
                peeked[i] = pages.peek(ref, PageFile.NODE);
                if (peeked[i] == null) {
                    missing[i] = ref;
                    bytes[i] = PageFormat.SIZE;
                    held.addAndGet(PageFormat.SIZE);
                }
            }
            pages.readUncached(missing, count, PageFile.NODE, read, stop);
            fetchBlobs();
        }

        private void fetchBlobs() {
            var found = 0;
            search:
            for (var i = 0; i < count; i++) {
                var leaf = read[i] != null ? read[i] : peeked[i];
                if (leaf == null || !TreeNodes.isLeaf(leaf)) {
                    continue;
                }
                var entries = TreeNodes.count(leaf);
                for (var n = 0; n < entries; n++) {
                    var index = descending ? entries - 1 - n : n;
                    var entry = TreeNodes.entry(leaf, index);
                    if (!TreeNodes.isBlob(leaf, entry) || !inRange(leaf, entry)) {
                        continue;
                    }
                    var ref = TreeNodes.blobRef(leaf, entry);
                    var length = ref.pages() * PageFormat.SIZE;
                    if (length > MAX_BLOB_BYTES || pages.peek(ref, PageFormat.VALUE) != null) {
                        continue;
                    }
                    if (!reserve(length)) {
                        break search;
                    }
                    bytes[i] += length;
                    if (found == blobRefs.length) {
                        blobRefs = Arrays.copyOf(blobRefs, found * 2);
                        blobUnits = Arrays.copyOf(blobUnits, found * 2);
                        blobLeaves = Arrays.copyOf(blobLeaves, found * 2);
                        blobEntries = Arrays.copyOf(blobEntries, found * 2);
                    }
                    blobRefs[found] = ref;
                    blobLeaves[found] = i;
                    blobEntries[found++] = index;
                }
            }
            try {
                pages.readUncached(blobRefs, found, PageFormat.VALUE, blobUnits, stop);
            } finally {
                for (var i = 0; i < found; i++) {
                    var unit = blobUnits[i];
                    if (unit != null) {
                        var owner = blobLeaves[i];
                        if (blobs[owner] == null) {
                            blobs[owner] = new byte[TreeNodes.count(read[owner] != null ? read[owner] : peeked[owner])][];
                        }
                        blobs[owner][blobEntries[i]] = unit;
                        blobUnits[i] = null;
                    }
                }
            }
        }

        /// {@return whether the entry `entry` of the leaf `unit` is in the range of the scan}
        private boolean inRange(byte[] unit, int entry) {
            var offset = TreeNodes.leafKeyOffset(entry);
            var length = TreeNodes.leafKeyLength(unit, entry);
            return (from == null || TreeNodes.compare(unit, offset, length, from) >= 0)
                    && (to == null || TreeNodes.compare(unit, offset, length, to) < 0);
        }

        /// Hands the leaves of the claim over to the cursor, with what was read for them, and forgets them.
        private void publish() {
            if (publishFailure != null) {
                synchronized (lock) {
                    var failure = publishFailure;
                    publishFailure = null;
                    if (failure != null) {
                        throw failure;
                    }
                }
            }
            for (var i = 0; i < count; i++) {
                var position = first + i;
                slots.set((int) position & SLOT_MASK, new Fetched(claim[i], read[i], blobs[i], bytes[i]));
                if (awaited == position) {
                    LockSupport.unpark(scanner);
                }
                claim[i] = null;
                missing[i] = null;
                read[i] = null;
                peeked[i] = null;
                blobs[i] = null;
                bytes[i] = 0;
            }
        }
    }
}
