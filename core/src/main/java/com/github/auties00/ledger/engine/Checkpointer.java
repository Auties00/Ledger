package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/// Writes the checkpoints of a store in a directory.
///
/// A checkpoint is built, then made durable. Its build takes, holding the publishing role, the keys written since the
/// last one and the range removals since, as of the latest sequence ([LiveLedger#cutCheckpoint]); then, while commits
/// go on, it applies them to the trees copy on write ([TreeUpdate]) and writes the catalog and the free lists. Making it
/// durable forces the file, writes the superblock the last one did not use and forces it. Only then are the new trees
/// published, the versions the trees now hold dropped from memory and the log segments before the cut retired. A crash
/// at any point leaves the last durable superblock and everything it uses whole: a checkpoint writes only pages no
/// durable state uses.
///
/// The store starts one on a platform thread of its own whenever the log has grown by the checkpoint interval since
/// the last; [Ledger#checkpoint()][com.github.auties00.ledger.Ledger#checkpoint()] and
/// [Ledger#close()][com.github.auties00.ledger.Ledger#close()] run one on the caller's thread, once those started before
/// are durable. One builds at a time, and they become durable in the order they were built.
///
/// A checkpoint may build while the one before it is made durable, as [#OVERLAP] decides: it then updates the trees of
/// that one, which no read sees yet, and its free lists count as free the units that one frees
/// ([FreeSpace#stage(List)]), so it writes no page the durable state or that one uses. It writes its
/// superblock only once that one's is durable, since it takes the slot of the one before that. If that one fails
/// before its superblock, this one is given up too, and that one's pages go back once this one no longer reads them.
///
/// Every unit of a checkpoint is sealed under a nonce made of 8 random bytes drawn for the checkpoint and a counter,
/// so no nonce repeats under the store's key, even across a checkpoint that a crash cut short.
///
/// A checkpoint that fails before its superblock gives its pages back, and the next one retries. One that fails while
/// it writes its superblock leaves it unknown whether that superblock is durable, so the pages it wrote stay in use and
/// no checkpoint runs again until the store reopens: overwriting either superblock could leave none that authenticates.
/// The log still holds every commit.
final class Checkpointer implements IdleTicker.Watched {
    /// When a checkpoint started on its own thread may build while the one before it is made durable: never where the
    /// forces beside a checkpoint's work keep the device busy while it builds ([PageFile#FORCE_BESIDE]), as the device
    /// bounds the checkpoints there and a cut taken earlier only rewrites the same leaves for fewer writes; elsewhere
    /// once commits wait for the trees, which they would otherwise do while the processors stay idle through the force.
    static final Overlap OVERLAP = PageFile.FORCE_BESIDE ? Overlap.NEVER : Overlap.BEHIND;
    /// The obsolete segments a closed store keeps for reuse: none. The next open writes into the segment the log kept
    /// for it ([LogWriter#nextOpenSegment()]), so a spare saves it nothing, and forcing a spare that a copy of the store
    /// left in the OS cache held the first durable commit after the open for tens of milliseconds.
    private static final int SPARES = 0;
    /// The checkpoint intervals of log the trees may lack before commits wait for a checkpoint, whatever the heap.
    private static final int MAX_BACKLOG = 6;
    /// The checkpoint intervals of log the trees may lack before commits wait for a checkpoint, if the writes behind it
    /// hold more heap than the store's budget: one being checkpointed and the next.
    private static final int MIN_BACKLOG = 2;
    /// The heap, by the log's estimate, the writes the trees lack must hold before a store that went a tick of the
    /// [IdleTicker] without a commit checkpoints them: below it, a store that writes now and then would checkpoint
    /// after almost every write.
    static final long IDLE_BACKLOG = 1L << 20;
    // The checkpointers of the open stores of the process, whose writes the trees lack share one heap budget: a budget
    // of half the heap for each store let a process of several busy stores buffer more than the heap
    private static final Set<Checkpointer> OPEN = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));
    // The epoch of the file of large values the last durable superblock records
    private volatile int durableEpoch;
    // The highest epoch an earlier open used that only its log segments record, if above durableEpoch
    private final int loggedEpoch;

    private final LiveLedger ledger;
    private final PageFile pages;
    private final FreeSpace space;
    // The file of large values, or null
    private final BlobFile blobs;
    private final LogWriter log;
    private final LogFiles files;
    private final long every;
    private final Overlap overlap;
    // Held by a build, and by a caller's checkpoint from before it waits for those built before until it is durable;
    // fair, so a caller's checkpoint is not passed by the builds the store keeps starting on its own threads
    private final ReentrantLock lock;
    // Whether a thread of the store's own builds a checkpoint, or is about to
    private final AtomicBoolean scheduled;
    private final SecureRandom random;
    // The log's queued bytes at the last cut
    private volatile long cutBytes;
    private volatile RuntimeException lastFailure;
    private final Consumer<? super LedgerException> failures;
    // The log's queued bytes at the cut of the last published checkpoint
    private volatile long publishedBytes;
    // The log's estimate of the heap its commits held at the cut of the last published checkpoint
    private volatile long publishedHeap;
    // The heap the writes the trees lack may hold, by the log's estimate, before commits wait for a checkpoint
    private final long heapBudget;
    // Signalled whenever a checkpoint ends, for the commits that wait on one
    private final ReentrantLock progressLock;
    private final Condition progress;
    // Set when a superblock write failed: no checkpoint runs again in this open
    private volatile LedgerIoException stopped;
    // The log's queued bytes at the last tick of the idle ticker; the ticker's alone
    private long appendedAtTick;
    // Set as the store starts closing: no tick starts a checkpoint, and one started on its own thread writes nothing
    private volatile boolean closed;
    // Guards the checkpoints built and not durable yet, the build running, and the delta pages of the last durable
    // checkpoint; signalled whenever one of them changes
    private final ReentrantLock stageLock;
    private final Condition stageChanged;
    // The checkpoints built and not yet published and pruned, or given up, oldest first
    private final ArrayDeque<Built> queue;
    // Whether a checkpoint is building, and the checkpoint not published it builds on, or null
    private boolean building;
    private Built buildingOn;
    // The checkpoints of the queue, for the reads that do not take the stage lock: all of them, those neither
    // published nor given up, and whether one is being given up
    private volatile int queued;
    private volatile int unpublished;
    private volatile boolean failing;
    // The builds that started on a checkpoint not published yet; guarded by the stage lock
    private long overlapped;
    // The delta pages of the last durable checkpoint, and the blob that holds their table, or null
    private DeltaTable deltas;
    private PageRef deltaTable;

    /// When a checkpoint started on its own thread may build while the one before it is not published.
    enum Overlap {
        /// Never: a checkpoint builds once the one before it is published, and is made durable on the thread that
        /// built it, which holds the build's lock until then.
        NEVER,
        /// While the log has grown by the checkpoint interval since the cut of the one before, and commits wait for the
        /// trees ([#throttle()]): a cut taken while they do not would only shrink the checkpoint.
        BEHIND,
        /// While the log has grown by the checkpoint interval since the cut of the one before.
        ALWAYS
    }

    /// Creates the checkpointer of `ledger`.
    ///
    /// @param every the log growth that starts a checkpoint, in bytes
    /// @param deltas the delta pages of the last durable checkpoint
    /// @param deltaTable the blob holding their table, or `null` if there is none
    /// @param failures receives the failures of the checkpoints started on their own threads
    /// @param durableEpoch the epoch of the file of large values the last superblock records
    /// @param loggedEpoch the highest epoch recovery found, in the superblock or in the log, which units may take
    /// @param overlap when a checkpoint started on its own thread may build beside the one before
    Checkpointer(LiveLedger ledger, PageFile pages, FreeSpace space, BlobFile blobs, LogWriter log, LogFiles files, long every,
                 DeltaTable deltas, PageRef deltaTable, Consumer<? super LedgerException> failures,
                 int durableEpoch, int loggedEpoch, long heapBudget, Overlap overlap) {
        this.ledger = ledger;
        this.pages = pages;
        this.space = space;
        this.blobs = blobs;
        this.log = log;
        this.files = files;
        this.every = every;
        this.overlap = overlap;
        this.deltas = deltas;
        this.deltaTable = deltaTable;
        this.lock = new ReentrantLock(true);
        this.scheduled = new AtomicBoolean();
        this.random = new SecureRandom();
        this.failures = failures;
        this.durableEpoch = durableEpoch;
        this.loggedEpoch = loggedEpoch;
        this.heapBudget = heapBudget;
        this.progressLock = new ReentrantLock();
        this.progress = progressLock.newCondition();
        this.stageLock = new ReentrantLock();
        this.stageChanged = stageLock.newCondition();
        this.queue = new ArrayDeque<>(4);
    }

    /// Starts counting the store's writes toward the heap budget of the process, those the open replayed included:
    /// `replayedBytes` of log holding `replayedHeap` by the log's estimate, which count as log the trees lack, as if
    /// the writer had queued them before it opened. The first checkpoint starts as soon as they are an interval, and
    /// commits wait for it while they hold the budget; counted from 0, they let a crashed store that replayed six
    /// intervals take six more, and up to the whole budget, before it waited.
    void open(long replayedBytes, long replayedHeap) {
        cutBytes = -replayedBytes;
        publishedBytes = -replayedBytes;
        publishedHeap = -replayedHeap;
        appendedAtTick = log.appendedBytes();
        OPEN.add(this);
        IdleTicker.watch(this);
    }

    /// Stops the checkpoints the store starts on its own threads, as it starts closing: one started from now on writes
    /// nothing, so none outlives the files a close without a final checkpoint closes.
    void beginClose() {
        closed = true;
    }

    /// Waits for a checkpoint building on its own thread to end and for those built to be durable, then stops
    /// counting the store's writes toward the heap budget of the process and checkpointing it when idle; once it has
    /// closed, before its files close.
    void close() {
        closed = true;
        // A build holds the lock while it runs, and one that takes it after this finds the store closed
        lock.lock();
        lock.unlock();
        // Those built before go on to their superblocks on their own threads
        drain();
        OPEN.remove(this);
        IdleTicker.unwatch(this);
    }

    /// Starts a checkpoint if the store committed nothing since the last tick of the [IdleTicker] and the writes the
    /// trees lack hold at least [#IDLE_BACKLOG] of heap.
    ///
    /// Checkpoints otherwise start once every interval of log, so a store that wrote less than one and stopped kept
    /// those writes in memory, more than their bytes in the log, for as long as it stayed open.
    @Override
    public void tick() {
        var appended = log.appendedBytes();
        var idle = appended == appendedAtTick;
        appendedAtTick = appended;
        if (idle && !closed && stopped == null && backlogHeap() >= IDLE_BACKLOG) {
            start();
        }
    }

    /// Starts a checkpoint on a thread of its own if the log has grown enough since the last cut, and none is building.
    void maybeStart() {
        if (log.appendedBytes() - cutBytes >= every) {
            start();
        }
    }

    /// Starts a checkpoint on a thread of its own, unless one is building or the ones built before keep it from
    /// building; a thread the system refuses is reported as a failed checkpoint, which the next start retries.
    private void start() {
        if (scheduled.get() || !mayBuild() || !scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            startThread();
        } catch (Throwable refused) {
            // As under a limit of processes: left scheduled, no checkpoint would start again, and commits that fell
            // behind would wait for one forever
            var failure = new LedgerIoException("A checkpoint could not start its thread", new IOException(refused));
            lastFailure = failure;
            scheduled.set(false);
            signalProgress();
            report(failure);
        }
    }

    private void startThread() {
        Thread.ofPlatform().daemon().name("ledger-checkpoint").start(this::runOwn);
    }

    /// Builds a checkpoint on the store's own thread, then makes it durable: holding the build's lock if [#overlap]
    /// never builds the next one beside it, after letting it go otherwise, so the next one may build meanwhile.
    private void runOwn() {
        Built built = null;
        try {
            lock.lock();
            try {
                if (closed) {
                    // Started before the store began closing, and found the lock after
                    lastFailure = null;
                    return;
                }
                if (stopped != null) {
                    throw stopped;
                }
                var base = beginBuild(true);
                if (base == null) {
                    // Those built before keep it from building now; the next of them to be durable starts it again
                    return;
                }
                built = build(false, base);
                if (built == null) {
                    lastFailure = null;
                } else if (built.failed) {
                    // Given up with the checkpoint it was built on, whose failure is reported
                    built = null;
                } else if (overlap == Overlap.NEVER) {
                    finish(built);
                    built = null;
                }
            } finally {
                lock.unlock();
            }
        } catch (RuntimeException failure) {
            // The log still holds every commit; the next checkpoint retries
            built = null;
            lastFailure = failure;
            report(failure);
        } finally {
            scheduled.set(false);
            signalProgress();
        }
        if (built != null) {
            // The next checkpoint may build while this one is made durable
            maybeStart();
            finishOwn(built);
        }
    }

    /// Makes `built` durable on the thread that built it, once those built before are, and reports its failure.
    private void finishOwn(Built built) {
        try {
            if (awaitTurn(built)) {
                finish(built);
            }
        } catch (RuntimeException failure) {
            lastFailure = failure;
            report(failure);
        } finally {
            signalProgress();
        }
        // The next checkpoint, which this one may have kept from building
        maybeStart();
    }

    private void report(RuntimeException failure) {
        var reported = failure instanceof LedgerException known ? known
                : new LedgerCorruptedException("A checkpoint found the store inconsistent", failure);
        try {
            failures.accept(reported);
        } catch (RuntimeException _) {
            // A listener that throws loses its report, not the store
        }
    }

    private void signalProgress() {
        progressLock.lock();
        try {
            progress.signalAll();
        } finally {
            progressLock.unlock();
        }
    }

    /// Makes the caller wait while the log holds more checkpoint intervals the trees do not than the heap allows, and a
    /// checkpoint is building or being made durable to take them in: more than [#MAX_BACKLOG], or more than
    /// [#MIN_BACKLOG] while the writes the trees of every open store of the process lack hold more than the heap budget.
    ///
    /// A fixed backlog of three intervals held commits back on a large heap that had room for more, a quarter of the
    /// overwrites of a store of 256-byte values, and let small values, whose writes take more heap than their bytes,
    /// fill more than half of it. It never waits while checkpoints fail: the commits go on, and the log grows until one
    /// succeeds.
    void throttle() {
        if (!behind() || lastFailure != null || stopped != null) {
            return;
        }
        maybeStart();
        progressLock.lock();
        try {
            while (behind() && (scheduled.get() || queued != 0) && lastFailure == null && stopped == null) {
                progress.awaitUninterruptibly();
            }
        } finally {
            progressLock.unlock();
        }
    }

    /// {@return whether the log holds more than the trees may lack}
    private boolean behind() {
        var bytes = log.appendedBytes() - publishedBytes;
        if (bytes <= MIN_BACKLOG * every) {
            return false;
        }
        return bytes > MAX_BACKLOG * every || processBacklogHeap() > heapBudget;
    }

    /// {@return the heap the writes the trees of the store lack hold, by the log's estimate}
    private long backlogHeap() {
        return log.appendedHeap() - publishedHeap;
    }

    /// {@return the heap the writes the trees of every open store of the process lack hold, by their logs' estimates}
    private long processBacklogHeap() {
        var total = OPEN.contains(this) ? 0 : backlogHeap();
        for (var checkpointer : OPEN.toArray(Checkpointer[]::new)) {
            total += checkpointer.backlogHeap();
        }
        return total;
    }

    /// {@return the bytes of log queued since the cut of the last published checkpoint, which the trees lack}
    long backlog() {
        return log.appendedBytes() - publishedBytes;
    }

    /// {@return the failure of the last checkpoint started on its own thread, or `null` if one published since}
    RuntimeException lastFailure() {
        return lastFailure;
    }

    /// {@return the checkpoints built and not yet published and pruned, for tests}
    int queuedForTests() {
        return queued;
    }

    /// {@return the builds that started while the checkpoint they updated was not published, for tests}
    long overlappedForTests() {
        stageLock.lock();
        try {
            return overlapped;
        } finally {
            stageLock.unlock();
        }
    }

    /// Writes a checkpoint of every commit made so far, unless the last one holds them all, once the checkpoints built
    /// before it are durable.
    ///
    /// @param closing whether the store is closing, so no commit follows and every log segment becomes obsolete
    /// @throws LedgerIoException if the checkpoint cannot be written; the store goes on, and the next one retries
    void run(boolean closing) {
        lock.lock();
        try {
            if (closed && !closing) {
                return;
            }
            drain();
            if (stopped != null) {
                throw stopped;
            }
            var built = build(closing, beginBuild(false));
            if (built != null && !built.failed) {
                finish(built);
            }
        } finally {
            lock.unlock();
        }
    }

    /// Waits until every checkpoint built is durable or given up.
    private void drain() {
        stageLock.lock();
        try {
            while (!queue.isEmpty()) {
                stageChanged.awaitUninterruptibly();
            }
        } finally {
            stageLock.unlock();
        }
    }

    /// Marks a build as running, holding the build's lock, and takes what it builds on.
    ///
    /// @param own whether the store started it, which then builds only as [#mayBuildLocked()] allows
    /// @return what it builds on, or `null` if it may not build now
    private Base beginBuild(boolean own) {
        stageLock.lock();
        try {
            if (own && !mayBuildLocked()) {
                return null;
            }
            Built on = null;
            for (var built : queue) {
                if (!built.published && !built.failed) {
                    on = built;
                }
            }
            building = true;
            buildingOn = on;
            if (on != null) {
                overlapped++;
            }
            return on == null ? new Base(null, deltas, deltaTable) : new Base(on, on.deltas, on.deltaTable);
        } finally {
            stageLock.unlock();
        }
    }

    /// Marks the build as over, once it has queued its checkpoint or given its pages back.
    private void endBuild() {
        stageLock.lock();
        try {
            building = false;
            buildingOn = null;
            stageChanged.signalAll();
        } finally {
            stageLock.unlock();
        }
    }

    /// {@return whether a checkpoint the store starts may build now, as far as the reads without the stage lock tell}
    private boolean mayBuild() {
        if (failing) {
            return false;
        }
        var waiting = unpublished;
        return waiting == 0 || waiting == 1 && overlaps();
    }

    /// {@return whether a checkpoint the store starts may build now: none built waits to publish, or one does that is
    /// not being given up and [#overlap] lets the next one build beside it}
    private boolean mayBuildLocked() {
        var waiting = 0;
        for (var built : queue) {
            if (built.failed) {
                return false;
            }
            if (!built.published) {
                waiting++;
            }
        }
        return waiting == 0 || waiting == 1 && overlaps();
    }

    private boolean overlaps() {
        return switch (overlap) {
            case NEVER -> false;
            case BEHIND -> behind();
            case ALWAYS -> true;
        };
    }

    /// Counts the checkpoints of the queue again for the reads without the stage lock; holding it.
    private void recount() {
        var waiting = 0;
        var given = false;
        for (var built : queue) {
            given |= built.failed;
            if (!built.published && !built.failed) {
                waiting++;
            }
        }
        unpublished = waiting;
        failing = given;
        queued = queue.size();
    }

    /// Builds a checkpoint of every commit made so far on `base`, unless it holds them all, and queues it to be made
    /// durable; holding the build's lock, after [#beginBuild(boolean)].
    ///
    /// @return the checkpoint, queued; one given up with the checkpoint it was built on; or `null` if no commit was made
    ///         since `base`
    /// @throws LedgerIoException if the checkpoint cannot be written; its pages are free again
    private Built build(boolean closing, Base base) {
        try {
            var on = base.built();
            var cut = ledger.cutCheckpoint(() -> closing ? log.nextOpenSegment() : log.currentSegment(),
                    on == null ? null : on.trees, on == null ? Set.of() : on.taken);
            if (cut == null) {
                if (closing && !epochOnlyInLog()) {
                    // The trees hold every commit, so the segments hold nothing a reopen needs but the one kept for it
                    retire(log.nextOpenSegment(), SPARES);
                }
                return null;
            }
            return build(closing, cut, base);
        } finally {
            endBuild();
        }
    }

    private Built build(boolean closing, LiveLedger.Cut cut, Base base) {
        cutBytes = cut.logBytes();
        // Lagging once a second interval of log waits behind the cut: the commits will wait for this checkpoint
        var writer = pages.writer(random.nextLong(), () -> log.appendedBytes() - cutBytes > every);
        space.rewindRuns();
        var update = new TreeUpdate(pages, writer, space);
        var freed = new ArrayList<PageRef>();
        var adopted = new HashSet<PageRef>();
        try {
            var roots = build(cut, update, adopted);
            var nextDeltas = update.finishDeltas(base.deltas());
            freed.addAll(update.freed());
            if (base.deltaTable() != null) {
                freed.add(base.deltaTable());
            }
            PageRef nextDeltaTable = null;
            if (!nextDeltas.isEmpty()) {
                nextDeltaTable = writeDeltaTable(writer, update, nextDeltas);
            }
            if (cut.base() != null) {
                freed.add(cut.base().catalog);
                freed.add(cut.base().freeList);
                if (cut.base().blobFreeList != null) {
                    freed.add(cut.base().blobFreeList);
                }
            }
            var catalog = writeCatalog(cut, roots, writer, update);
            BlobFile.Runs blobRuns = null;
            PageRef blobFreeList = null;
            if (blobs != null) {
                var alsoFree = new ArrayList<>(update.freedExternal());
                alsoFree.addAll(cut.superseded());
                // The units the checkpoint built on takes stay in flight until it publishes
                Set<PageRef> taken = adopted;
                if (base.built() != null && !base.built().adopted.isEmpty()) {
                    taken = new HashSet<>(adopted);
                    taken.addAll(base.built().adopted);
                }
                blobRuns = blobs.durableRuns(taken, alsoFree);
                blobFreeList = writeBlobFreeList(writer, update, blobRuns);
            }
            var freeList = writeFreeList(writer, update, freed);
            // The pages the free list describes: a later build may take pages past them, which it leaves free
            var pageCount = space.pageCount();
            writer.flush();
            var number = (cut.base() == null ? 0 : cut.base().number) + 1;
            var built = new Built(cut, writer, update, number, roots, catalog, freeList, blobFreeList, pageCount, blobRuns,
                    nextDeltas, nextDeltaTable, freed, adopted, closing);
            if (!queue(built, base.built())) {
                abandon(cut, writer, update);
            }
            return built;
        } catch (IOException exception) {
            abandon(cut, writer, update);
            throw new LedgerIoException("The checkpoint could not be written", exception);
        } catch (RuntimeException | Error throwable) {
            abandon(cut, writer, update);
            throw throwable;
        } finally {
            ledger.readers().unregister(cut.sequence());
        }
    }

    /// Queues `built` to be made durable after the checkpoints built before it, and stages the units it frees, unless
    /// `on`, the checkpoint it was built on, was given up or no checkpoint may run any more.
    ///
    /// @return whether it queued it; otherwise it marks it given up
    private boolean queue(Built built, Built on) {
        stageLock.lock();
        try {
            if (stopped != null || (on != null && on.failed)) {
                built.failed = true;
                return false;
            }
            space.stage(built.freed);
            if (blobs != null) {
                blobs.stage(built.update.freedExternal());
            }
            queue.addLast(built);
            recount();
            return true;
        } finally {
            stageLock.unlock();
        }
    }

    /// Waits until every checkpoint built before `built` is durable or given up.
    ///
    /// @return whether `built` is still to be made durable, rather than given up with one before it
    private boolean awaitTurn(Built built) {
        stageLock.lock();
        try {
            while (!built.failed && queue.peekFirst() != built) {
                stageChanged.awaitUninterruptibly();
            }
            return !built.failed;
        } finally {
            stageLock.unlock();
        }
    }

    /// Makes `built`, the oldest checkpoint of the queue, durable, then publishes its trees, drops from memory what
    /// they hold, retires the log segments before its cut and trims the files.
    ///
    /// @throws LedgerIoException if it cannot be made durable; the checkpoints built on it are given up with it
    private void finish(Built built) {
        var cut = built.cut;
        if (stopped != null) {
            // The one before it could not be published: its superblock may be durable, and no other may follow it
            fail(built, null);
            throw stopped;
        }
        var superblockWritten = false;
        try {
            built.writer.awaitForce();
            if (blobs != null) {
                // The units the trees take were written by commits the log may not have forced yet
                blobs.force();
            }
            pages.force();
            superblockWritten = true;
            pages.writeSuperblock(new Superblock(built.number, cut.sequence(), cut.logStart(), built.pageCount, built.catalog,
                            built.freeList, built.blobRuns == null ? 0 : built.blobRuns.pageCount(), built.blobFreeList,
                            blobs == null ? 0 : blobs.epoch(), built.deltaTable),
                    built.writer.nonceHigh(), built.writer.nextNonce());
        } catch (IOException exception) {
            var failure = new LedgerIoException("The checkpoint could not be written", exception);
            // Once the superblock is being written, its pages stay in use: it may be durable
            fail(built, superblockWritten ? failure : null);
            throw failure;
        } catch (RuntimeException | Error throwable) {
            fail(built, superblockWritten
                    ? new LedgerIoException("The checkpoint could not be written", new IOException(throwable)) : null);
            throw throwable;
        }
        if (blobs != null) {
            durableEpoch = blobs.epoch();
        }
        try {
            ledger.publish(cut, built.number, built.roots, built.catalog, built.freeList, built.blobFreeList, built.freed,
                    built.update.freedExternal(), built.adopted);
            published(built);
            publishedBytes = cut.logBytes();
            publishedHeap = cut.logHeap();
            lastFailure = null;
            signalProgress();
            ledger.pruneCut(cut);
            // While the store runs, as many as a checkpoint interval fills, so the log seldom writes into a new file
            retire(cut.logStart(), built.closing ? SPARES : (int) Math.min(64, every / log.segmentSize() + 1));
            trim();
        } catch (RuntimeException | Error throwable) {
            stageLock.lock();
            try {
                if (!built.published) {
                    // The superblock is durable and the trees are not published: a later checkpoint would build on the
                    // trees before it, with its frees counted free
                    stopped = new LedgerIoException("The checkpoint could not be published", new IOException(throwable));
                }
            } finally {
                stageLock.unlock();
            }
            throw throwable;
        } finally {
            dequeue(built);
        }
    }

    /// Records that the trees of `built` are published: the next build updates them as the latest, and its delta
    /// pages are the durable ones.
    private void published(Built built) {
        stageLock.lock();
        try {
            deltas = built.deltas;
            deltaTable = built.deltaTable;
            built.published = true;
            recount();
            stageChanged.signalAll();
        } finally {
            stageLock.unlock();
        }
    }

    /// Takes `built` out of the queue, so the next checkpoint built is made durable.
    private void dequeue(Built built) {
        stageLock.lock();
        try {
            queue.remove(built);
            recount();
            stageChanged.signalAll();
        } finally {
            stageLock.unlock();
        }
        signalProgress();
    }

    /// Ends the files before their trailing free pages, unless a checkpoint is building, whose pages past the new end
    /// it would cut after the build wrote them.
    private void trim() {
        stageLock.lock();
        try {
            if (building) {
                return;
            }
            var end = space.trim();
            if (end > 0) {
                pages.truncate(end);
            }
            if (blobs != null) {
                blobs.trim();
            }
        } catch (IOException exception) {
            // The checkpoint is durable; trailing free pages wait for the next one
        } finally {
            stageLock.unlock();
        }
    }

    /// Gives up `built`, which failed before it was durable, and every checkpoint built on it; their keys go back to
    /// the next checkpoint.
    ///
    /// The pages `built` took go back once the build that reads its trees, if one runs, has given itself up.
    ///
    /// @param superblockFailure the failure of the superblock write, after which no checkpoint runs again and the
    ///                          pages of `built` stay in use; or `null` if it failed before it
    private void fail(Built built, LedgerIoException superblockFailure) {
        var given = new ArrayList<Built>();
        stageLock.lock();
        try {
            if (superblockFailure != null) {
                stopped = superblockFailure;
            }
            var after = false;
            for (var queued : queue) {
                after |= queued == built;
                if (after) {
                    queued.failed = true;
                    given.add(queued);
                }
            }
            if (!after) {
                built.failed = true;
                given.add(built);
            }
            recount();
            // No free list counts their units free any more: the trees before them still use them
            for (var queued : given) {
                space.unstage(queued.freed);
                if (blobs != null) {
                    blobs.unstage(queued.update.freedExternal());
                }
            }
            while (building && buildingOn == built) {
                stageChanged.awaitUninterruptibly();
            }
        } finally {
            stageLock.unlock();
        }
        try {
            for (var i = given.size() - 1; i >= 0; i--) {
                var queued = given.get(i);
                if (queued == built && superblockFailure != null) {
                    queued.writer.discard();
                    ledger.restoreCut(queued.cut);
                } else {
                    abandon(queued.cut, queued.writer, queued.update);
                }
            }
        } finally {
            stageLock.lock();
            try {
                queue.removeAll(given);
                recount();
                stageChanged.signalAll();
            } finally {
                stageLock.unlock();
            }
            signalProgress();
        }
    }

    /// {@return whether an epoch of the file of large values that units may have taken is recorded only by the log}
    ///
    /// Units are sealed under their epoch and a counter, so an open that reused an epoch would reuse their nonces: a
    /// unit written by a commit that failed, or by one a crash cut off, took its open's epoch without a checkpoint
    /// recording it, and the segments that hold its epoch frame stay until one does.
    private boolean epochOnlyInLog() {
        if (blobs == null) {
            return false;
        }
        return Integer.compareUnsigned(loggedEpoch, durableEpoch) > 0
                || (blobs.epochTaken() && Integer.compareUnsigned(blobs.epoch(), durableEpoch) > 0);
    }

    /// Retires the log segments before `first`, keeping `spares` of them to be prepared again.
    private void retire(long first, int spares) {
        try {
            files.retireBefore(first, spares);
        } catch (IOException exception) {
            // Obsolete segments wait for the next checkpoint
        }
    }

    private void abandon(LiveLedger.Cut cut, PageFile.Writer writer, TreeUpdate update) {
        writer.discard();
        for (var unit : update.allocated()) {
            space.release(unit.page(), unit.pages());
        }
        ledger.restoreCut(cut);
    }

    /// {@return the roots of every key space's tree after the cut, by id}
    ///
    /// @param adopted receives the units of the file of large values the trees take
    private TreeRoot[] build(LiveLedger.Cut cut, TreeUpdate update, Set<PageRef> adopted) throws IOException {
        var roots = cut.base() == null ? new TreeRoot[cut.maxId() + 1] : cut.base().roots(cut.maxId() + 1);
        var bound = Reads.boundOf(cut.sequence());
        var treeBound = cut.base() == null ? -1 : Reads.boundOf(cut.base().sequence);
        for (var keyspace : cut.keyspaces()) {
            var state = keyspace.state();
            // Listed in the order of their writes, off the publishing role: sorted here
            var chains = keyspace.dirty().toArray(Chain[]::new);
            ChainSort.sort(chains);
            var ops = new ArrayList<TreeUpdate.Op>(chains.length);
            var removals = keyspace.removals();
            Chain previous = null;
            for (var chain : chains) {
                if (previous != null && Chain.sameKey(previous, chain)) {
                    continue;
                }
                previous = chain;
                // The key's chain now: this one until it is dropped, as a key gets a new chain only once its chain is;
                // otherwise another one, or none
                var current = chain.released() ? state.chain(chain.key()) : chain;
                // The key, a segment of its own, only for the range removals to cover
                var key = removals.length == 0 ? null : chain.key();
                var version = KeyspaceState.resolve(current == null ? null : current.head, key, bound, removals);
                if (version == null && !state.resident) {
                    // No version at the cut in a paged key space: a commit that wrote the key rolled back, so the
                    // tree is right. A resident one holds every entry, so there the key is gone
                    continue;
                }
                // A blind write the cut takes, the chain's first and so at or below the cut, which sees a version of it:
                // the leaf the update merges tells whether the tree held the key
                var blind = current != null && current.blindState() == TreeSearch.UNKNOWN ? current : null;
                if (version == null || version == Version.HIDDEN) {
                    ops.add(new TreeUpdate.Op(chain, null, blind));
                } else {
                    ops.add(new TreeUpdate.Op(chain, version, blind));
                    var external = version.external();
                    if (external != null) {
                        adopted.add(external);
                    }
                }
            }
            var ranges = new ArrayList<TreeUpdate.Range>();
            for (var removal : keyspace.removals()) {
                if (removal.stamp() > treeBound && removal.stamp() <= bound) {
                    ranges.add(new TreeUpdate.Range(removal.from(), removal.to()));
                }
            }
            var base = roots[state.id];
            var root = update.apply(base, ops, TreeUpdate.merge(ranges));
            // The update settled the blind writes up to the cut but those of keys it skipped, looked up here
            var blind = state.blind.correction(cut.sequence(), key -> base != null && TreeSearch.find(pages, base, key) != null);
            var count = root == null ? 0 : root.count();
            if (count != keyspace.count() + blind) {
                throw new IllegalStateException("The checkpoint of " + state.keyspace.name() + " holds " + count
                        + " entries where the store counts " + (keyspace.count() + blind));
            }
            roots[state.id] = root;
        }
        return roots;
    }

    private PageRef writeCatalog(LiveLedger.Cut cut, TreeRoot[] roots, PageFile.Writer writer, TreeUpdate update) throws IOException {
        var unit = Catalog.encode(cut.maxId(), ledger.catalog(cut, roots));
        var unitPages = unit.length / PageFormat.SIZE;
        var page = allocate(unitPages, update);
        return writer.write(PageFormat.CATALOG, 0, page, unitPages, unit, Bytes.i32(unit, PageFormat.LENGTH), false);
    }

    /// Writes the free list of the file of large values the checkpoint makes durable, in pages of the data file.
    private PageRef writeBlobFreeList(PageFile.Writer writer, TreeUpdate update, BlobFile.Runs runs) throws IOException {
        var unitPages = PageFormat.pagesFor(Catalog.freeListLength(runs.runs().size()));
        var page = allocate(unitPages, update);
        var unit = Catalog.encodeFreeList(runs.runs(), unitPages);
        return writer.write(PageFormat.FREE_LIST, 0, page, unitPages, unit, Bytes.i32(unit, PageFormat.LENGTH), false);
    }

    /// Writes the table of the delta pages the checkpoint makes durable.
    private PageRef writeDeltaTable(PageFile.Writer writer, TreeUpdate update, DeltaTable table) throws IOException {
        var unitPages = PageFormat.pagesFor(table.length());
        var page = allocate(unitPages, update);
        var unit = table.encode(unitPages);
        return writer.write(PageFormat.DELTA_TABLE, 0, page, unitPages, unit, table.length(), false);
    }

    /// Writes the free list of the state the checkpoint makes durable, in pages taken from that list.
    private PageRef writeFreeList(PageFile.Writer writer, TreeUpdate update, List<PageRef> freed) throws IOException {
        var unitPages = PageFormat.pagesFor(Catalog.freeListLength(space.durableRuns(freed).size()));
        while (true) {
            var page = allocate(unitPages, update);
            // Taking the pages shortens or removes a run, so the list shrinks, or grows by at most one run
            var runs = space.durableRuns(freed);
            if (Catalog.freeListLength(runs.size()) <= PageFormat.capacity(unitPages)) {
                var unit = Catalog.encodeFreeList(runs, unitPages);
                return writer.write(PageFormat.FREE_LIST, 0, page, unitPages, unit, Bytes.i32(unit, PageFormat.LENGTH), false);
            }
            space.release(page, unitPages);
            update.allocated().removeLast();
            unitPages++;
        }
    }

    private int allocate(int unitPages, TreeUpdate update) {
        var page = space.allocate(unitPages);
        update.allocated().add(new PageRef(page, unitPages, 0, 0));
        return page;
    }

    /// What a build starts from.
    ///
    /// @param built the checkpoint built and not published whose trees it updates, or `null` to update the latest ones
    /// @param deltas the delta pages of the trees it updates
    /// @param deltaTable the blob holding their table, or `null` if there is none
    private record Base(Built built, DeltaTable deltas, PageRef deltaTable) {
    }

    /// A checkpoint built: its trees, catalog and free lists written, not yet durable.
    private static final class Built {
        final LiveLedger.Cut cut;
        final PageFile.Writer writer;
        final TreeUpdate update;
        final long number;
        final TreeRoot[] roots;
        final PageRef catalog;
        final PageRef freeList;
        // The blob of the free list of the file of large values, or null
        final PageRef blobFreeList;
        // The pages of the data file its free list describes
        final int pageCount;
        // The free pages of the file of large values it records, or null
        final BlobFile.Runs blobRuns;
        // The delta pages once its trees are durable, and the blob that holds their table, or null
        final DeltaTable deltas;
        final PageRef deltaTable;
        // The units of the data file its trees no longer use, staged in the free space until it publishes
        final List<PageRef> freed;
        // The units of the file of large values its trees take
        final Set<PageRef> adopted;
        // The units of the file of large values its publication takes out of flight: those its trees take, and those
        // the commits it holds replaced before a checkpoint took them
        final Set<PageRef> taken;
        // Its trees, as the next checkpoint builds on them
        final TreeVersion trees;
        // Whether the store closes after it
        final boolean closing;
        // Whether its trees are published, and whether it was given up; guarded by the stage lock
        boolean published;
        boolean failed;

        Built(LiveLedger.Cut cut, PageFile.Writer writer, TreeUpdate update, long number, TreeRoot[] roots, PageRef catalog,
              PageRef freeList, PageRef blobFreeList, int pageCount, BlobFile.Runs blobRuns, DeltaTable deltas,
              PageRef deltaTable, List<PageRef> freed, Set<PageRef> adopted, boolean closing) {
            this.cut = cut;
            this.writer = writer;
            this.update = update;
            this.number = number;
            this.roots = roots;
            this.catalog = catalog;
            this.freeList = freeList;
            this.blobFreeList = blobFreeList;
            this.pageCount = pageCount;
            this.blobRuns = blobRuns;
            this.deltas = deltas;
            this.deltaTable = deltaTable;
            this.freed = freed;
            this.adopted = adopted;
            var taken = new HashSet<PageRef>(adopted);
            taken.addAll(cut.superseded());
            this.taken = taken;
            this.trees = new TreeVersion(number, cut.sequence(), Long.MAX_VALUE, roots, catalog, freeList, blobFreeList, null);
            this.closing = closing;
        }
    }
}
