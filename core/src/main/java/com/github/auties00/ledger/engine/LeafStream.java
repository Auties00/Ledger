package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/// The leaves of a tree from a key on, read and opened ahead of the one thread that takes them in key order
/// ([#next()]), by worker virtual threads and by that thread itself, for the loader of a resident key space.
///
/// The stream reads the nodes on the path to its first key alone, so its first leaves are known after two or three
/// reads, and the branches after them on a thread of its own, with one coalesced read a level. The leaves are claimed in
/// key order, in groups of at most [PageFile#RUN_PAGES] pages that each claimer reads with one call per run of
/// consecutive pages, and each leaf is opened into an array of the store's pool and handed to the taker as soon as it
/// authenticates, ahead of the rest of its group: the taker installs a leaf while the next ones are opened. No group is
/// claimed that starts more than [#lead] leaves past the taker, which bounds the plaintext held. A leaf the cache holds
/// when it is read is taken from the cache, and its array, which others may still read, never goes to the pool.
///
/// The stream waits for the reads that were claimed, never for its threads: a worker that gets no carrier while an
/// application's virtual threads hold them all claims nothing, the taker reads every leaf no worker claimed, and the
/// branches if the thread that reads them has not started, so [#close()] returns once the claimed reads end, and the
/// tree's pages are let go of only then. A worker that starts after that finds nothing to claim and reads nothing.
///
/// A failed read fails its leaves, and the claims stop there: the taker takes the leaves before them and then throws
/// the failure.
final class LeafStream implements AutoCloseable {
    /// The most workers a stream reads on: with the taker, as many threads as the data file has read channels.
    static final int MAX_WORKERS = PageFile.READ_CHANNELS - 1;
    /// The pages of the first group a worker claims, and the most the taker claims: the first leaves reach the taker
    /// after a short read, and those the taker reads keep it from installing for a short time; the groups after the
    /// first grow with the leaves read, up to [PageFile#RUN_PAGES].
    private static final int FIRST_GROUP_PAGES = 8;
    /// The value of [#claimed] once the stream is closed.
    private static final int CLOSED = Integer.MAX_VALUE;
    private static final int PENDING = 0;
    private static final int READY = 1;
    private static final int FAILED = 2;
    /// [#expansion]: the branches after the path are still to read, are being read, or are read or never will be.
    private static final int WAITING = 0;
    private static final int EXPANDING = 1;
    private static final int EXPANDED = 2;
    /// [#waitingOn] while the taker waits for no leaf.
    private static final int NONE = -1;
    /// The leaves the taker takes between two wake-ups of the workers parked at the end of the lead: woken at every
    /// leaf, they would park and unpark for each, as every leaf moves the lead by one.
    private static final int WAKE_EVERY = 16;

    /// A leaf of the stream and what was read of it.
    static final class Slot {
        final PageRef ref;
        // The lowest key of the leaf, a read-only view of its parent, or null for a leaf with no parent
        private final MemorySegment first;
        // The plaintext, written before the volatile store of state that publishes it; dropped once taken
        private byte[] unit;
        // Whether unit is an array of the pool, which the stream gives back
        private boolean pooled;
        // Why the leaf could not be read, written before the volatile store of state
        private Throwable failure;
        private volatile int state;

        private Slot(TreeSearch.Leaf leaf) {
            this.ref = leaf.ref();
            this.first = leaf.first();
        }

        /// {@return the plaintext of the leaf, valid until it is [taken][LeafStream#taken(Slot)]}
        byte[] unit() {
            return unit;
        }
    }

    private final PageFile pages;
    // The thread that takes the leaves and closes the stream
    private final Thread taker;
    // Receives each pooled array before it goes back to the pool; for tests, or null
    private final Consumer<byte[]> released;
    // The leaves known so far, in key order; replaced by a longer copy as the branches after the path are read
    private volatile Slot[] plan;
    // Set once plan holds every leaf, after its last copy
    private volatile boolean planned;
    // The lowest key of the subtrees plan lacks, or null once it lacks none
    private volatile MemorySegment unplanned;
    // The subtrees after the path, read by the thread that claimed the expansion alone
    private final List<List<TreeSearch.Leaf>> rest;
    private final AtomicInteger expansion;
    private volatile Throwable expansionFailure;
    // The leaves below it are claimed; CLOSED once the stream is
    private final AtomicInteger claimed;
    // The leaves whose claimed read has ended, read or failed
    private final AtomicInteger finished;
    // The lowest leaf whose read failed: no group is claimed past it
    private final AtomicInteger failedFrom;
    // The leaves the taker took; written by the taker alone
    private volatile int taken;
    // The most leaves past the taker a group may start at
    private volatile int lead;
    // Whether the workers claim groups of the most pages from the first
    private volatile boolean wide;
    // The leaf the taker waits for, or NONE
    private volatile int waitingOn;
    private final AtomicReferenceArray<Thread> workers;
    // The workers started; the taker's alone
    private int started;
    // The workers parked until the taker or the plan moves
    private final AtomicInteger idle;
    // Whether a wait of the taker cleared its interrupt, which close gives back; the taker's alone
    private boolean interrupted;

    /// Reads the path to `from` in the tree under `root` and starts `workers` workers, which read as many pages a
    /// group as they can from the first if `wide`, and a thread for the branches after the path if any.
    ///
    /// The caller is the taker, which must close the stream before the tree's pages may be freed.
    ///
    /// @param released receives each pooled array before it goes back to the pool, for tests; or `null`
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if a node on the path fails authentication
    /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read
    LeafStream(PageFile pages, TreeRoot root, MemorySegment from, int workers, boolean wide, Consumer<byte[]> released) {
        var path = TreeSearch.path(pages, root, from);
        this.pages = pages;
        this.taker = Thread.currentThread();
        this.released = released;
        this.plan = slots(path.leaves());
        this.rest = path.rest();
        this.unplanned = unplanned(rest, 0);
        this.planned = unplanned == null;
        this.expansion = new AtomicInteger(planned ? EXPANDED : WAITING);
        this.claimed = new AtomicInteger();
        this.finished = new AtomicInteger();
        this.failedFrom = new AtomicInteger(Integer.MAX_VALUE);
        this.wide = wide;
        this.lead = leadOf(workers);
        this.waitingOn = NONE;
        this.workers = new AtomicReferenceArray<>(MAX_WORKERS);
        this.idle = new AtomicInteger();
        if (!planned) {
            try {
                Thread.ofVirtual().name("ledger-resident-plan").start(this::expand);
            } catch (RuntimeException | Error _) {
                // The taker reads the branches itself once it needs them
            }
        }
        startWorkers(workers);
    }

    private static Slot[] slots(List<TreeSearch.Leaf> leaves) {
        var slots = new Slot[leaves.size()];
        for (var i = 0; i < slots.length; i++) {
            slots[i] = new Slot(leaves.get(i));
        }
        return slots;
    }

    /// {@return the lowest key of the subtrees of `rest` from list `from` on, or `null` if they are empty}
    private static MemorySegment unplanned(List<List<TreeSearch.Leaf>> rest, int from) {
        for (var i = from; i < rest.size(); i++) {
            if (!rest.get(i).isEmpty()) {
                return rest.get(i).getFirst().first();
            }
        }
        return null;
    }

    /// {@return the lead of a stream that reads on `workers` workers: the pages of a full group for each, and for the
    /// taker}
    private static int leadOf(int workers) {
        return Math.max(1, workers) * PageFile.RUN_PAGES;
    }

    /// Starts workers until `workers` run, or the system refuses one: the taker reads whatever they do not.
    private void startWorkers(int workers) {
        while (started < Math.min(workers, MAX_WORKERS)) {
            Thread worker;
            try {
                worker = Thread.ofVirtual().name("ledger-resident-read").unstarted(this::work);
                this.workers.set(started, worker);
                worker.start();
            } catch (RuntimeException | Error _) {
                this.workers.set(started, null);
                return;
            }
            started++;
        }
    }

    /// Has every worker read as many pages a group as it can and as far ahead as a full group each, and starts workers
    /// until [#MAX_WORKERS] run; by the taker.
    void widen() {
        wide = true;
        startWorkers(MAX_WORKERS);
        lead = leadOf(started);
        wakeWorkers();
    }

    /// {@return the workers started}
    int workers() {
        return started;
    }

    /// {@return the lowest key of the leaves past those the stream may read before the taker moves, or `null` if it may
    /// read to the last leaf}
    ///
    /// Past the leaves known so far, it is the lowest key of the subtrees still to read, which is lower than the bound
    /// once they are read.
    MemorySegment reading() {
        var bound = taken + lead;
        var complete = planned;
        var slots = plan;
        if (bound < slots.length) {
            return slots[bound].first;
        }
        return complete ? null : unplanned;
    }

    /// {@return whether [#next()] would return without waiting}
    boolean ready() {
        var complete = planned;
        var slots = plan;
        var next = taken;
        return next < slots.length ? slots[next].state != PENDING : complete;
    }

    /// {@return the next leaf, read and opened, or `null` after the last}
    ///
    /// It reads the leaf itself if no worker claimed it, with the few after it, and waits if one did.
    ///
    /// @throws RuntimeException the failure of the read of the leaf, or of the branches that name it
    /// @throws Error likewise
    Slot next() {
        var next = taken;
        while (true) {
            var state = expansion.get();
            var complete = planned;
            var slots = plan;
            if (next >= slots.length) {
                if (complete) {
                    return null;
                }
                if (state == EXPANDED) {
                    throw rethrown(expansionFailure);
                }
                if (state == WAITING) {
                    expand();
                } else {
                    // Woken once the branches are read
                    park();
                }
                continue;
            }
            var slot = slots[next];
            switch (slot.state) {
                case READY -> {
                    return slot;
                }
                case FAILED -> throw rethrown(slot.failure);
                default -> {
                }
            }
            // Every leaf before it is taken, so claimed: no worker claimed it unless the claims passed it
            if (claimed.get() == next) {
                var end = groupEnd(slots, next, Math.min(FIRST_GROUP_PAGES, next + 1));
                if (claimed.compareAndSet(next, end)) {
                    read(slots, next, end);
                }
                continue;
            }
            waitingOn = next;
            if (slot.state == PENDING) {
                park();
            }
            waitingOn = NONE;
        }
    }

    /// Records that the taker is done with `slot`, the leaf [#next()] returned last, and gives its array back to the
    /// pool if it is one of the pool's.
    void taken(Slot slot) {
        drop(slot);
        var next = taken + 1;
        taken = next;
        if (next % WAKE_EVERY == 0 && idle.get() > 0) {
            wakeWorkers();
        }
    }

    private void drop(Slot slot) {
        var unit = slot.unit;
        slot.unit = null;
        if (slot.pooled && unit != null) {
            if (released != null) {
                released.accept(unit);
            }
            pages.release(unit);
        }
    }

    /// Stops the claims, waits until every claimed read and the read of the branches end, whatever the interrupt
    /// status of the taker, which it keeps, and gives back to the pool the arrays of the leaves not taken; by the
    /// taker, once.
    @Override
    public void close() {
        var frontier = claimed.getAndSet(CLOSED);
        if (frontier == CLOSED) {
            return;
        }
        expansion.compareAndSet(WAITING, EXPANDED);
        wakeWorkers();
        while (finished.get() < frontier || expansion.get() == EXPANDING) {
            park();
        }
        var slots = plan;
        for (var i = taken; i < frontier; i++) {
            if (slots[i].state == READY) {
                drop(slots[i]);
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Claims and reads groups of leaves until none is left to claim, the claims stop or the stream closes.
    private void work() {
        while (true) {
            var start = claimed.get();
            if (start == CLOSED || start >= failedFrom.get()) {
                return;
            }
            var state = expansion.get();
            var complete = planned;
            var slots = plan;
            if (start >= slots.length) {
                if (complete || state == EXPANDED) {
                    return;
                }
                if (state == WAITING) {
                    expand();
                } else {
                    rest(start, slots.length);
                }
                continue;
            }
            if (start >= taken + lead) {
                rest(start, slots.length);
                continue;
            }
            var cap = wide ? PageFile.RUN_PAGES : Math.clamp(start, FIRST_GROUP_PAGES, PageFile.RUN_PAGES);
            var end = groupEnd(slots, start, cap);
            if (claimed.compareAndSet(start, end)) {
                read(slots, start, end);
            }
        }
    }

    /// Parks a worker that found no group to claim at `start`, with `known` leaves known, until the taker, the plan or
    /// the claims move; it may return early.
    private void rest(int start, int known) {
        idle.incrementAndGet();
        try {
            // Read again after the count, which every thread that moves them reads after it does, the taker once every
            // WAKE_EVERY leaves: it moves on until then, as it reads whatever no worker claimed
            if (claimed.get() == start && (start >= taken + lead || (plan.length == known && expansion.get() == EXPANDING))) {
                LockSupport.park(this);
                // No one interrupts a worker, but an interrupt left set would turn every park into a spin
                Thread.interrupted();
            }
        } finally {
            idle.decrementAndGet();
        }
    }

    private void wakeWorkers() {
        for (var i = 0; i < workers.length(); i++) {
            var worker = workers.get(i);
            if (worker != null) {
                LockSupport.unpark(worker);
            }
        }
    }

    /// Parks the taker until a read it waits for moves; it may return early.
    private void park() {
        LockSupport.park(this);
        interrupted |= Thread.interrupted();
    }

    /// {@return the end of the group of leaves of `slots` from `start` on, which holds at most `cap` pages, at least
    /// one leaf, and a delta record only alone}
    private static int groupEnd(Slot[] slots, int start, int cap) {
        if (slots[start].ref.isRecord()) {
            return start + 1;
        }
        var groupPages = slots[start].ref.pages();
        var end = start + 1;
        while (end < slots.length && end - start < Long.SIZE) {
            var ref = slots[end].ref;
            if (ref.isRecord() || groupPages + ref.pages() > cap) {
                break;
            }
            groupPages += ref.pages();
            end++;
        }
        return end;
    }

    /// Reads the leaves of `slots` from `start` to `end`, excluded, which the caller claimed, one call for each run of
    /// consecutive pages, the run of the first leaf first; then counts them finished, read or failed.
    private void read(Slot[] slots, int start, int end) {
        try {
            if (end - start == 1 && slots[start].ref.isRecord()) {
                // A delta record reads as the leaf it makes of its base, which may be in the cache
                publish(slots[start], pages.read(slots[start].ref, PageFile.NODE, false), false);
                return;
            }
            var order = new int[end - start];
            for (var i = 0; i < order.length; i++) {
                order[i] = start + i;
            }
            sortByPage(slots, order);
            var runs = new ArrayList<int[]>();
            var from = 0;
            for (var i = 1; i <= order.length; i++) {
                if (i == order.length || slots[order[i]].ref.page() != slots[order[i - 1]].ref.page() + slots[order[i - 1]].ref.pages()) {
                    runs.add(Arrays.copyOfRange(order, from, i));
                    from = i;
                }
            }
            // The run of the lowest leaf first, as the taker waits for the leaves in key order
            runs.sort((left, right) -> Integer.compare(min(left), min(right)));
            var refs = new PageRef[Long.SIZE];
            for (var run : runs) {
                for (var i = 0; i < run.length; i++) {
                    refs[i] = slots[run[i]].ref;
                }
                pages.readOwned(refs, run.length, PageFile.NODE, (index, unit, pooled) -> publish(slots[run[index]], unit, pooled));
            }
        } catch (Throwable failure) {
            fail(slots, start, end, failure);
        } finally {
            finished.addAndGet(end - start);
            if (Thread.currentThread() != taker) {
                LockSupport.unpark(taker);
            }
        }
    }

    private static int min(int[] indexes) {
        var min = Integer.MAX_VALUE;
        for (var index : indexes) {
            min = Math.min(min, index);
        }
        return min;
    }

    /// Sorts `order`, indexes of `slots`, by the page of their leaves; a group is a few dozen leaves.
    private static void sortByPage(Slot[] slots, int[] order) {
        for (var i = 1; i < order.length; i++) {
            var index = order[i];
            var page = slots[index].ref.page();
            var j = i - 1;
            while (j >= 0 && slots[order[j]].ref.page() > page) {
                order[j + 1] = order[j];
                j--;
            }
            order[j + 1] = index;
        }
    }

    /// Publishes the plaintext of `slot`, and wakes the taker if it waits for it.
    private void publish(Slot slot, byte[] unit, boolean pooled) {
        slot.unit = unit;
        slot.pooled = pooled;
        slot.state = READY;
        var waiting = waitingOn;
        if (waiting != NONE && plan[waiting] == slot) {
            LockSupport.unpark(taker);
        }
    }

    /// Fails the leaves of `slots` from `start` to `end`, excluded, not read, with `failure`, and stops the claims at
    /// the first.
    private void fail(Slot[] slots, int start, int end, Throwable failure) {
        for (var i = start; i < end; i++) {
            var slot = slots[i];
            if (slot.state == PENDING) {
                slot.failure = failure;
                slot.state = FAILED;
                failedFrom.accumulateAndGet(i, Math::min);
            }
        }
    }

    /// Reads the branches after the path and adds their leaves to the plan, a list of [#rest] at a time, unless another
    /// thread claimed the read or the stream closed.
    private void expand() {
        if (!expansion.compareAndSet(WAITING, EXPANDING)) {
            return;
        }
        try {
            for (var i = 0; i < rest.size(); i++) {
                if (claimed.get() == CLOSED) {
                    return;
                }
                var nodes = rest.get(i);
                if (nodes.isEmpty()) {
                    continue;
                }
                var leaves = TreeSearch.leavesUnder(pages, nodes, wide ? PageFile.READ_CHANNELS : 1);
                var known = plan;
                var grown = Arrays.copyOf(known, known.length + leaves.size());
                for (var j = 0; j < leaves.size(); j++) {
                    grown[known.length + j] = new Slot(leaves.get(j));
                }
                plan = grown;
                unplanned = unplanned(rest, i + 1);
                wake();
            }
            planned = true;
        } catch (Throwable failure) {
            expansionFailure = failure;
        } finally {
            expansion.set(EXPANDED);
            wake();
        }
    }

    /// Wakes the workers and the taker, which the plan or the expansion moved for.
    private void wake() {
        wakeWorkers();
        if (Thread.currentThread() != taker) {
            LockSupport.unpark(taker);
        }
    }

    private static RuntimeException rethrown(Throwable failure) {
        return switch (failure) {
            case null -> new IllegalStateException("The stream is closed");
            case RuntimeException exception -> exception;
            case Error error -> throw error;
            default -> new IllegalStateException(failure);
        };
    }
}
