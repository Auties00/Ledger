package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
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
/// consecutive pages. The leaves of a group read are then opened by every thread of the stream that has nothing else to
/// do, one claim a leaf in key order, the taker's included: the reads are spread over the workers by group and the opens
/// by leaf, so a key space of one group is still opened on every thread, and the taker installs a leaf while the next
/// ones are opened. No group is claimed that starts more than [#lead] leaves past the taker, the workers hold at most
/// [#SEALED_GROUPS] groups read and not all opened, which bounds the pages held sealed, and no thread opens a leaf more
/// than [#OPENED_AHEAD] past the taker, which bounds the plaintext: a leaf is opened
/// into an array the stream uses again for a later leaf once the taker took it, and lets go of as it closes, so a load
/// keeps no memory past its end, in the store's pool or elsewhere, and allocates no more plaintext than it holds at once.
/// A leaf the cache holds when it is read is taken from the cache, and its array, which others may still read, is never
/// written again.
///
/// The stream waits for the reads and opens that were claimed, never for its threads, and no thread parks while it
/// holds one: a thread takes its read channel without waiting ([PageFile#tryLease()]) before it claims a group or the
/// branches, and rests with none if every channel is in use, as a thread that parked holding work could get no carrier
/// to resume on while an application's virtual threads hold them all. The taker reads every leaf no worker claimed and
/// the branches no thread took, opens the leaves no thread is opening, and alone waits for a free channel, so
/// [#close()] returns once the claimed work ends, and the tree's pages are let go of only then. A worker that starts
/// after that finds nothing to claim and reads nothing.
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
    // Read, and waiting for a thread to open it
    private static final int SEALED = 1;
    private static final int READY = 2;
    private static final int FAILED = 3;
    /// [#expansion]: the branches after the path are still to read, are being read, or are read or never will be.
    private static final int WAITING = 0;
    private static final int EXPANDING = 1;
    private static final int EXPANDED = 2;
    /// [#waitingOn] while the taker waits for no leaf.
    private static final int NONE = -1;
    /// The leaves the taker takes between two wake-ups of the workers parked at the end of the lead: woken at every
    /// leaf, they would park and unpark for each, as every leaf moves the lead by one.
    private static final int WAKE_EVERY = 16;
    /// The most leaves past the taker a thread of the stream opens, other than the one the taker waits for: opened as soon
    /// as they were read, the leaves of [#lead] read ahead were held as plaintext at once, most of them long before the
    /// taker reached them. Four wake-ups' worth: the taker installs a leaf in about the time a thread opens one, and the
    /// workers resume at every [#WAKE_EVERY] leaves it takes.
    static final int OPENED_AHEAD = 4 * WAKE_EVERY;
    /// The most groups the workers hold read and not yet all opened. The pages of each wait in an array of the store's
    /// pool, which the pool keeps once it comes back, so the groups held at once are what a load leaves in the pool:
    /// with the opens held to [#OPENED_AHEAD], the groups read up to the lead waited sealed, seven of the largest arrays
    /// where a load before the stream held five on four processors. Four groups are [#OPENED_AHEAD] leaves being opened
    /// and three groups more read ahead of them.
    static final int SEALED_GROUPS = 4;
    /// Why a thread of the stream rests ([#rest(int, int, int)]): the next group starts past the lead, the leaves after
    /// the plan are being read, every read channel is in use, the claimed reads have yet to end, or the workers hold
    /// [#SEALED_GROUPS] groups already.
    private static final int AHEAD = 0;
    private static final int UNPLANNED = 1;
    private static final int NO_CHANNEL = 2;
    private static final int READING = 3;
    private static final int HELD = 4;

    /// A leaf of the stream and what was read of it.
    static final class Slot {
        final PageRef ref;
        // Where the leaf is in the plan
        private final int index;
        // The lowest key of the leaf, a read-only view of its parent, or null for a leaf with no parent
        private final MemorySegment first;
        // The plaintext, written before the volatile store of state that publishes it; dropped once taken
        private byte[] unit;
        // Whether unit is an array of the stream's, which it uses again once the leaf is taken
        private boolean owned;
        // Why the leaf could not be read, written before the volatile store of state
        private Throwable failure;
        // While sealed: the pages the leaf is opened from, read only by the thread that claimed its open
        private Sealed sealed;
        // The leaves of its group to open, written before the volatile store of state that seals it
        private Openings openings;
        // The thread that opened the leaf, or null for one taken from the cache; for tests
        Thread openedBy;
        private volatile int state;

        private Slot(TreeSearch.Leaf leaf, int index) {
            this.ref = leaf.ref();
            this.index = index;
            this.first = leaf.first();
        }

        /// {@return the plaintext of the leaf, valid until it is [taken][LeafStream#taken(Slot)]}
        byte[] unit() {
            return unit;
        }
    }

    /// Pages read with one call, an array of the store's pool given back once the last leaf in them is opened.
    private static final class Sealed {
        private final byte[] array;
        // The first page read
        private final int first;
        // The leaves in the pages still to open
        private final AtomicInteger unopened;
        // The group of a worker the pages are part of, or null for the taker's
        private final Group group;

        private Sealed(byte[] array, int first, int leaves, Group group) {
            this.array = array;
            this.first = first;
            this.unopened = new AtomicInteger(leaves);
            this.group = group;
        }
    }

    /// A group a worker claimed, which holds one of the [#SEALED_GROUPS] until its read ended and the arrays of its pages
    /// went back to the pool.
    private final class Group {
        // One for the read, and one for each array of its pages not given back
        private final AtomicInteger holds = new AtomicInteger(1);

        private void hold() {
            holds.incrementAndGet();
        }

        private void release() {
            if (holds.decrementAndGet() == 0) {
                freeGroups.incrementAndGet();
                if (idle.get() > 0) {
                    wakeWorkers();
                }
            }
        }
    }

    /// The leaves of a group read and not yet opened, in key order, which each thread of the stream claims one at a time.
    private static final class Openings {
        private final Slot[] slots;
        private final AtomicInteger next;

        private Openings(Slot[] slots) {
            this.slots = slots;
            this.next = new AtomicInteger();
        }

        /// {@return the lowest leaf no thread claimed the open of, now claimed by the caller, or `null` if none is left}
        Slot claim() {
            if (next.get() >= slots.length) {
                return null;
            }
            var index = next.getAndIncrement();
            return index < slots.length ? slots[index] : null;
        }

        /// {@return the lowest leaf no thread claimed the open of, or `null` if none is left}
        Slot next() {
            var index = next.get();
            return index < slots.length ? slots[index] : null;
        }

        /// {@return whether every open is claimed}
        boolean claimed() {
            return next.get() >= slots.length;
        }
    }

    private final PageFile pages;
    // The thread that takes the leaves and closes the stream
    private final Thread taker;
    // Receives each array of the stream's before it is used again or let go of; for tests, or null
    private final Consumer<byte[]> released;
    // The arrays of one page of the leaves taken, which the next opens write over; let go of as the stream closes
    private final ConcurrentLinkedQueue<byte[]> spare;
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
    // The leaves whose claimed read or open has ended, read, opened or failed
    private final AtomicInteger finished;
    // The lowest leaf whose read failed: no group is claimed past it
    private final AtomicInteger failedFrom;
    // The groups read whose opens are not all claimed, about in key order
    private final ConcurrentLinkedQueue<Openings> openable;
    // The leaves the taker took; written by the taker alone
    private volatile int taken;
    // The most leaves past the taker a group may start at
    private volatile int lead;
    // Whether the workers claim groups of the most pages from the first
    private volatile boolean wide;
    // The leaf the taker waits for, or NONE
    private volatile int waitingOn;
    // Set by close before it waits for the claimed work, which then wakes it as it ends
    private volatile boolean closing;
    private final AtomicReferenceArray<Thread> workers;
    // The thread that reads the branches after the path, or null if there are none
    private final Thread planner;
    // The workers started; the taker's alone
    private int started;
    // The threads of the stream parked until the taker, the plan, the claims, the opens or the channels move
    private final AtomicInteger idle;
    // The groups of SEALED_GROUPS the workers may claim now
    private final AtomicInteger freeGroups;
    // Whether a wait of the taker cleared its interrupt, which close gives back; the taker's alone
    private boolean interrupted;

    /// Reads the path to `from` in the tree under `root` and starts `workers` workers, which read as many pages a
    /// group as they can from the first if `wide`, and a thread for the branches after the path if any.
    ///
    /// The caller is the taker, which must close the stream before the tree's pages may be freed.
    ///
    /// @param released receives each array of the stream's before it is used again or let go of, for tests; or `null`
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if a node on the path fails authentication
    /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read
    LeafStream(PageFile pages, TreeRoot root, MemorySegment from, int workers, boolean wide, Consumer<byte[]> released) {
        var path = TreeSearch.path(pages, root, from);
        this.pages = pages;
        this.taker = Thread.currentThread();
        this.released = released;
        this.spare = new ConcurrentLinkedQueue<>();
        this.plan = slots(path.leaves());
        this.rest = path.rest();
        this.unplanned = unplanned(rest, 0);
        this.planned = unplanned == null;
        this.expansion = new AtomicInteger(planned ? EXPANDED : WAITING);
        this.claimed = new AtomicInteger();
        this.finished = new AtomicInteger();
        this.failedFrom = new AtomicInteger(Integer.MAX_VALUE);
        this.openable = new ConcurrentLinkedQueue<>();
        this.wide = wide;
        this.lead = leadOf(workers);
        this.waitingOn = NONE;
        this.workers = new AtomicReferenceArray<>(MAX_WORKERS);
        this.idle = new AtomicInteger();
        this.freeGroups = new AtomicInteger(SEALED_GROUPS);
        this.planner = planned ? null : Thread.ofVirtual().name("ledger-resident-plan").unstarted(this::plan);
        if (planner != null) {
            try {
                planner.start();
            } catch (RuntimeException | Error _) {
                // The taker reads the branches itself once it needs them
            }
        }
        startWorkers(workers);
    }

    private static Slot[] slots(List<TreeSearch.Leaf> leaves) {
        var slots = new Slot[leaves.size()];
        for (var i = 0; i < slots.length; i++) {
            slots[i] = new Slot(leaves.get(i), i);
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
    ///
    /// The workers it starts open the leaves of the groups already read too, so the leaves claimed before it are
    /// opened on every thread.
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

    /// {@return the count of the groups of [#SEALED_GROUPS] the workers may claim now, which is [#SEALED_GROUPS] once
    /// every group they claimed ended; for tests}
    AtomicInteger freeGroupsForTests() {
        return freeGroups;
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

    /// {@return whether [#next()] would return without waiting for a read}
    boolean ready() {
        var complete = planned;
        var slots = plan;
        var next = taken;
        return next < slots.length ? slots[next].state != PENDING : complete;
    }

    /// {@return the next leaf, read and opened, or `null` after the last}
    ///
    /// It reads the leaf itself if no worker claimed it, with the few after it, opens it if no other thread does, and
    /// waits if one does either.
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
                    // No other thread took the branches: read them here, waiting for a channel if none is free
                    expand(null);
                } else {
                    // Woken once the branches are read, by a thread that holds its channel and so never parks
                    park();
                }
                continue;
            }
            var slot = slots[next];
            var leaf = slot.state;
            if (leaf == READY) {
                return slot;
            }
            if (leaf == FAILED) {
                throw rethrown(slot.failure);
            }
            if (leaf == SEALED) {
                // The lowest open of its group no thread claimed: this leaf, unless another thread opens it
                var open = slot.openings.claim();
                if (open != null) {
                    open(open);
                    continue;
                }
            } else if (claimed.get() == next) {
                // Every leaf before it is taken, so claimed: no worker claimed it unless the claims passed it
                var end = groupEnd(slots, next, Math.min(FIRST_GROUP_PAGES, next + 1));
                if (claimed.compareAndSet(next, end)) {
                    read(slots, next, end, null, null);
                }
                continue;
            }
            waitingOn = next;
            leaf = slot.state;
            // Opens the leaves of later groups while another thread reads or opens this one
            if ((leaf == PENDING || leaf == SEALED && slot.openings.claimed()) && !openOne(next + OPENED_AHEAD)) {
                park();
            }
            waitingOn = NONE;
        }
    }

    /// Records that the taker is done with `slot`, the leaf [#next()] returned last, and keeps its array for a later
    /// leaf if it is one of the stream's.
    void taken(Slot slot) {
        drop(slot, true);
        var next = taken + 1;
        taken = next;
        if (next % WAKE_EVERY == 0 && idle.get() > 0) {
            wakeWorkers();
        }
    }

    /// Lets go of the plaintext of `slot`, keeping its array for a later leaf if `reuse` and it is one of the stream's
    /// of one page.
    private void drop(Slot slot, boolean reuse) {
        var unit = slot.unit;
        slot.unit = null;
        if (slot.owned && unit != null) {
            if (released != null) {
                released.accept(unit);
            }
            if (reuse && unit.length == PageFormat.SIZE) {
                spare.offer(unit);
            }
        }
    }

    /// Stops the claims, waits until every claimed read and open and the read of the branches end, whatever the
    /// interrupt status of the taker, which it keeps, and lets go of every plaintext array of the stream; by the taker,
    /// once.
    ///
    /// It counts the opens no thread claimed finished without opening them, so it waits for no thread that would.
    @Override
    public void close() {
        var frontier = claimed.getAndSet(CLOSED);
        if (frontier == CLOSED) {
            return;
        }
        closing = true;
        expansion.compareAndSet(WAITING, EXPANDED);
        wakeWorkers();
        while (finished.get() < frontier || expansion.get() == EXPANDING) {
            if (!openOne(Integer.MAX_VALUE)) {
                park();
            }
        }
        var slots = plan;
        for (var i = taken; i < frontier; i++) {
            if (slots[i].state == READY) {
                drop(slots[i], false);
            }
        }
        spare.clear();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// Opens the leaves of the groups read and claims and reads groups of leaves until none is left to claim, the claims
    /// stop or the stream closes.
    private void work() {
        while (true) {
            // The leaves read before those not claimed yet, which the taker needs first
            if (openOne(taken + OPENED_AHEAD)) {
                continue;
            }
            var start = claimed.get();
            if (start == CLOSED || start >= failedFrom.get()) {
                return;
            }
            var state = expansion.get();
            var complete = planned;
            var slots = plan;
            if (start >= slots.length) {
                if (complete || state == EXPANDED) {
                    if (finished.get() >= start) {
                        return;
                    }
                    // The groups others read may still leave leaves to open
                    rest(start, slots.length, READING);
                } else if (state == WAITING) {
                    if (!tryExpand()) {
                        rest(start, slots.length, NO_CHANNEL);
                    }
                } else {
                    rest(start, slots.length, UNPLANNED);
                }
                continue;
            }
            if (start >= taken + lead) {
                rest(start, slots.length, AHEAD);
                continue;
            }
            var cap = wide ? PageFile.RUN_PAGES : Math.clamp(start, FIRST_GROUP_PAGES, PageFile.RUN_PAGES);
            var end = groupEnd(slots, start, cap);
            if (!takeGroup()) {
                rest(start, slots.length, HELD);
                continue;
            }
            var group = new Group();
            // The channel first: once the group is claimed, the taker may wait for it
            var lease = pages.tryLease();
            if (lease == null) {
                group.release();
                rest(start, slots.length, NO_CHANNEL);
                continue;
            }
            if (claimed.compareAndSet(start, end)) {
                read(slots, start, end, lease, group);
            } else {
                lease.close();
                group.release();
                if (idle.get() > 0) {
                    wakeWorkers();
                }
            }
        }
    }

    /// {@return whether it took one of the groups of [#SEALED_GROUPS], which the caller releases through a [Group]}
    private boolean takeGroup() {
        for (var free = freeGroups.get(); free > 0; free = freeGroups.get()) {
            if (freeGroups.compareAndSet(free, free - 1)) {
                return true;
            }
        }
        return false;
    }

    /// Reads the branches after the path, unless another thread does or the stream closes; on the planner's thread.
    private void plan() {
        while (expansion.get() == WAITING) {
            var start = claimed.get();
            if (start == CLOSED) {
                return;
            }
            if (!tryExpand()) {
                rest(start, plan.length, NO_CHANNEL);
            }
        }
    }

    /// Reads the branches after the path on a channel taken without waiting, unless another thread took them.
    ///
    /// @return whether it had a channel to read them on
    private boolean tryExpand() {
        var lease = pages.tryLease();
        if (lease == null) {
            return false;
        }
        try {
            expand(lease);
        } finally {
            lease.close();
            if (idle.get() > 0) {
                wakeWorkers();
            }
        }
        return true;
    }

    /// Parks a thread of the stream that found nothing to do at claim `start`, with `known` leaves known, for `reason`,
    /// until the taker, the plan, the claims, the opens or the channels move; it may return early.
    ///
    /// It holds no claim, so the taker never waits for it.
    private void rest(int start, int known, int reason) {
        idle.incrementAndGet();
        try {
            // Read again after the count, which every thread that moves them reads after it does, the taker once every
            // WAKE_EVERY leaves: it moves on until then, as it reads and opens whatever no worker claimed
            var still = !openable(taken + OPENED_AHEAD) && claimed.get() == start && switch (reason) {
                case AHEAD -> start >= taken + lead;
                case UNPLANNED -> plan.length == known && expansion.get() == EXPANDING;
                case READING -> finished.get() < start;
                case HELD -> freeGroups.get() <= 0;
                default -> !pages.leasable();
            };
            if (still) {
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
        if (planner != null) {
            LockSupport.unpark(planner);
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
    /// consecutive pages, and leaves them to open to every thread of the stream; then gives `lease` back.
    ///
    /// @param lease the channel to read on, or `null` for the taker, which waits for one
    /// @param group the group of a worker, which the read releases once it ended, or `null` for the taker's
    private void read(Slot[] slots, int start, int end, PageFile.Lease lease, Group group) {
        // The leaves of the group that end here: those the cache holds, a delta record, or those a failure fails
        var ended = 0;
        try {
            if (end - start == 1 && slots[start].ref.isRecord()) {
                // A delta record reads as the leaf it makes of its base, which may be in the cache
                ended = 1;
                try {
                    publish(slots[start], pages.read(slots[start].ref, PageFile.NODE, false, lease), false);
                } catch (Throwable failure) {
                    fail(slots[start], failure);
                }
                return;
            }
            var missing = new Slot[end - start];
            var count = 0;
            for (var i = start; i < end; i++) {
                var hit = pages.cached(slots[i].ref, PageFile.NODE);
                if (hit != null) {
                    publish(slots[i], hit, false);
                    ended++;
                } else {
                    missing[count++] = slots[i];
                }
            }
            if (count == 0) {
                return;
            }
            missing = Arrays.copyOf(missing, count);
            try {
                seal(missing, lease, group);
            } catch (Throwable failure) {
                for (var slot : missing) {
                    fail(slot, failure);
                }
                ended += count;
                return;
            }
            var openings = new Openings(missing);
            for (var slot : missing) {
                slot.openings = openings;
                slot.state = SEALED;
            }
            try {
                openable.add(openings);
            } catch (Throwable _) {
                // No thread finds them but through the taker's next leaf, so this one opens them
                for (Slot slot; (slot = openings.claim()) != null; ) {
                    open(slot);
                }
            }
            // The taker opens them itself if it waits for one, or closes
            var waiting = waitingOn;
            if ((closing || waiting >= start && waiting < end) && Thread.currentThread() != taker) {
                LockSupport.unpark(taker);
            }
        } finally {
            // Given back once the group is sealed: if the hand-off parks, the taker opens the leaves without this thread
            if (lease != null) {
                lease.close();
            }
            if (group != null) {
                group.release();
            }
            ended(ended);
            if (idle.get() > 0) {
                wakeWorkers();
            }
        }
    }

    /// Reads the pages of `missing`, leaves in key order, one call for each run of consecutive pages, and points each
    /// leaf at the pages it is in; on a failure it gives back what it read and throws.
    private void seal(Slot[] missing, PageFile.Lease lease, Group group) {
        var order = missing.clone();
        sortByPage(order);
        var buffers = new ArrayList<Sealed>(1);
        try {
            var from = 0;
            for (var i = 1; i <= order.length; i++) {
                if (i == order.length || order[i].ref.page() != order[i - 1].ref.page() + order[i - 1].ref.pages()) {
                    var first = order[from].ref.page();
                    var last = order[i - 1].ref;
                    var sealed = new Sealed(pages.readSealed(first, last.page() + last.pages() - first, lease), first, i - from, group);
                    if (group != null) {
                        group.hold();
                    }
                    buffers.add(sealed);
                    for (var j = from; j < i; j++) {
                        order[j].sealed = sealed;
                    }
                    from = i;
                }
            }
        } catch (Throwable failure) {
            for (var buffer : buffers) {
                pages.release(buffer.array);
                if (group != null) {
                    group.release();
                }
            }
            for (var slot : missing) {
                slot.sealed = null;
            }
            throw failure;
        }
    }

    /// Opens the lowest leaf no thread claimed the open of in the oldest group read that has one below leaf `bound`.
    ///
    /// @return whether it found one to open
    private boolean openOne(int bound) {
        for (var openings : openable) {
            var next = openings.next();
            if (next == null) {
                openable.remove(openings);
            } else if (next.index < bound) {
                // Past the bound if other threads claimed the leaves below it meanwhile, by a leaf a thread at most
                var slot = openings.claim();
                if (slot != null) {
                    open(slot);
                    return true;
                }
                openable.remove(openings);
            }
        }
        return false;
    }

    /// {@return whether a group read holds a leaf below leaf `bound` that no thread claimed the open of}
    private boolean openable(int bound) {
        for (var openings : openable) {
            var next = openings.next();
            if (next != null && next.index < bound) {
                return true;
            }
        }
        return false;
    }

    /// Opens `slot`, whose open the caller claimed, unless the stream closed, and counts it finished either way.
    private void open(Slot slot) {
        var sealed = slot.sealed;
        slot.sealed = null;
        try {
            if (claimed.get() != CLOSED) {
                var length = slot.ref.pages() * PageFormat.SIZE;
                var unit = length == PageFormat.SIZE ? spare.poll() : null;
                if (unit == null) {
                    unit = new byte[length];
                }
                try {
                    pages.openInto(sealed.array, (slot.ref.page() - sealed.first) * PageFormat.SIZE, slot.ref, PageFile.NODE, unit);
                } catch (Throwable failure) {
                    // What the open left in it is no unit's, and the next open writes over it
                    if (length == PageFormat.SIZE) {
                        spare.offer(unit);
                    }
                    throw failure;
                }
                slot.openedBy = Thread.currentThread();
                publish(slot, unit, true);
            }
        } catch (Throwable failure) {
            fail(slot, failure);
        } finally {
            if (sealed.unopened.decrementAndGet() == 0) {
                pages.release(sealed.array);
                if (sealed.group != null) {
                    sealed.group.release();
                }
            }
            ended(1);
        }
    }

    /// Counts `leaves` leaves finished, and wakes the taker if it waits for them to close.
    private void ended(int leaves) {
        if (leaves == 0) {
            return;
        }
        finished.addAndGet(leaves);
        if (closing && Thread.currentThread() != taker) {
            LockSupport.unpark(taker);
        }
    }

    /// Sorts `slots` by the page of their leaves; a group is a few dozen leaves.
    private static void sortByPage(Slot[] slots) {
        for (var i = 1; i < slots.length; i++) {
            var slot = slots[i];
            var page = slot.ref.page();
            var j = i - 1;
            while (j >= 0 && slots[j].ref.page() > page) {
                slots[j + 1] = slots[j];
                j--;
            }
            slots[j + 1] = slot;
        }
    }

    /// Publishes the plaintext of `slot`, and wakes the taker if it waits for it.
    private void publish(Slot slot, byte[] unit, boolean owned) {
        slot.unit = unit;
        slot.owned = owned;
        slot.state = READY;
        wakeTakerFor(slot);
    }

    /// Fails `slot` with `failure`, stops the claims at it, and wakes the taker if it waits for it.
    private void fail(Slot slot, Throwable failure) {
        slot.failure = failure;
        slot.state = FAILED;
        failedFrom.accumulateAndGet(slot.index, Math::min);
        wakeTakerFor(slot);
    }

    private void wakeTakerFor(Slot slot) {
        if (waitingOn == slot.index && Thread.currentThread() != taker) {
            LockSupport.unpark(taker);
        }
    }

    /// Reads the branches after the path and adds their leaves to the plan, a list of [#rest] at a time, unless another
    /// thread claimed the read or the stream closed.
    ///
    /// @param lease the channel to read on, or `null` for the taker, which waits for one
    private void expand(PageFile.Lease lease) {
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
                var leaves = TreeSearch.leavesUnder(pages, nodes, lease);
                var known = plan;
                var grown = Arrays.copyOf(known, known.length + leaves.size());
                for (var j = 0; j < leaves.size(); j++) {
                    grown[known.length + j] = new Slot(leaves.get(j), known.length + j);
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
