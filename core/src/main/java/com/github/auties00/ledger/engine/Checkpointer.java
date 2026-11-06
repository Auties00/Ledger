package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;

/// Writes the checkpoints of a store in a directory.
///
/// A checkpoint takes, holding the publishing role, the keys written since the last one and the range removals since, as
/// of the latest sequence ([LiveLedger#cutCheckpoint]); then, while commits go on, it applies them to the trees copy
/// on write ([TreeUpdate]), writes the catalog and the free list, forces the file, writes the superblock the last one
/// did not use and forces it. Only then are the new trees published, the log segments before the cut retired and the
/// versions the trees now hold dropped from memory. A crash at any point leaves the last durable superblock and
/// everything it uses whole: a checkpoint writes only pages no durable state uses.
///
/// The store starts one on a platform thread of its own whenever the log has grown by the checkpoint interval since
/// the last; [Ledger#checkpoint()][com.github.auties00.ledger.Ledger#checkpoint()] and
/// [Ledger#close()][com.github.auties00.ledger.Ledger#close()] run one on the caller's thread. One runs at a time.
///
/// Every unit of a checkpoint is sealed under a nonce made of 8 random bytes drawn for the checkpoint and a counter,
/// so no nonce repeats under the store's key, even across a checkpoint that a crash cut short.
///
/// A checkpoint that fails before its superblock gives its pages back, and the next one retries. One that fails while
/// it writes its superblock leaves it unknown whether that superblock is durable, so the pages it wrote stay in use and
/// no checkpoint runs again until the store reopens: overwriting either superblock could leave none that authenticates.
/// The log still holds every commit.
final class Checkpointer {
    /// The obsolete segments a closed store keeps for reuse: none. The next open writes into the segment the log kept
    /// for it ([LogWriter#nextOpenSegment()]), so a spare saves it nothing, and forcing a spare that a copy of the store
    /// left in the OS cache held the first durable commit after the open for tens of milliseconds.
    private static final int SPARES = 0;
    /// The checkpoint intervals of log the trees may lack before commits wait for a checkpoint, whatever the heap.
    private static final int MAX_BACKLOG = 6;
    /// The checkpoint intervals of log the trees may lack before commits wait for a checkpoint, if the writes behind it
    /// hold more heap than the store's budget: one being checkpointed and the next.
    private static final int MIN_BACKLOG = 2;
    // The epoch of the file of large values the last durable superblock records; guarded by the lock
    private int durableEpoch;
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
    private final ReentrantLock lock;
    private final AtomicBoolean scheduled;
    private final SecureRandom random;
    // The number of the last durable checkpoint, guarded by the lock
    private long number;
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
    // The delta pages of the last durable checkpoint, and the blob that holds their table, or null; guarded by the lock
    private DeltaTable deltas;
    private PageRef deltaTable;

    /// Creates the checkpointer of `ledger`.
    ///
    /// @param every the log growth that starts a checkpoint, in bytes
    /// @param number the number of the last durable checkpoint, or 0
    /// @param deltas the delta pages of the last durable checkpoint
    /// @param deltaTable the blob holding their table, or `null` if there is none
    /// @param failures receives the failures of the checkpoints started on their own threads
    /// @param durableEpoch the epoch of the file of large values the last superblock records
    /// @param loggedEpoch the highest epoch recovery found, in the superblock or in the log, which units may take
    Checkpointer(LiveLedger ledger, PageFile pages, FreeSpace space, BlobFile blobs, LogWriter log, LogFiles files, long every,
                 long number, DeltaTable deltas, PageRef deltaTable, Consumer<? super LedgerException> failures,
                 int durableEpoch, int loggedEpoch, long heapBudget) {
        this.ledger = ledger;
        this.pages = pages;
        this.space = space;
        this.blobs = blobs;
        this.log = log;
        this.files = files;
        this.every = every;
        this.number = number;
        this.deltas = deltas;
        this.deltaTable = deltaTable;
        this.lock = new ReentrantLock();
        this.scheduled = new AtomicBoolean();
        this.random = new SecureRandom();
        this.failures = failures;
        this.durableEpoch = durableEpoch;
        this.loggedEpoch = loggedEpoch;
        this.heapBudget = heapBudget;
        this.progressLock = new ReentrantLock();
        this.progress = progressLock.newCondition();
    }

    /// Counts the commits the open replayed, `bytes` of log holding `heap` by the log's estimate, as log the trees
    /// lack, as if the writer had queued them before it opened: the first checkpoint starts as soon as they are an
    /// interval, and commits wait for it while they hold the heap budget. Counted from 0, they let a crashed store
    /// that replayed six intervals take six more, and up to the whole budget, before it waited.
    void replayed(long bytes, long heap) {
        cutBytes = -bytes;
        publishedBytes = -bytes;
        publishedHeap = -heap;
    }

    /// Starts a checkpoint on a thread of its own if the log has grown enough since the last, and none is running.
    void maybeStart() {
        if (log.appendedBytes() - cutBytes < every || !scheduled.compareAndSet(false, true)) {
            return;
        }
        Thread.ofPlatform().daemon().name("ledger-checkpoint").start(() -> {
            try {
                run(false);
                lastFailure = null;
            } catch (RuntimeException failure) {
                // The log still holds every commit; the next checkpoint retries
                lastFailure = failure;
                report(failure);
            } finally {
                scheduled.set(false);
                signalProgress();
            }
        });
    }

    private void report(RuntimeException failure) {
        var reported = failure instanceof LedgerException known ? known
                : new com.github.auties00.ledger.LedgerCorruptedException("A checkpoint found the store inconsistent", failure);
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
    /// checkpoint is running to take them in: more than [#MAX_BACKLOG], or more than [#MIN_BACKLOG] while the writes
    /// behind them hold more than the heap budget.
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
            while (behind() && scheduled.get() && lastFailure == null) {
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
        return bytes > MAX_BACKLOG * every || log.appendedHeap() - publishedHeap > heapBudget;
    }

    /// {@return the bytes of log queued since the cut of the last published checkpoint, which the trees lack}
    long backlog() {
        return log.appendedBytes() - publishedBytes;
    }

    /// {@return the failure of the last checkpoint started on its own thread, or `null` if it succeeded}
    RuntimeException lastFailure() {
        return lastFailure;
    }

    /// Writes a checkpoint of every commit made so far, unless the last one holds them all.
    ///
    /// @param closing whether the store is closing, so no commit follows and every log segment becomes obsolete
    /// @throws LedgerIoException if the checkpoint cannot be written; the store goes on, and the next one retries
    void run(boolean closing) {
        lock.lock();
        try {
            if (stopped != null) {
                throw stopped;
            }
            var cut = ledger.cutCheckpoint(() -> closing ? log.nextOpenSegment() : log.currentSegment());
            if (cut == null) {
                if (closing && !epochOnlyInLog()) {
                    // The trees hold every commit, so the segments hold nothing a reopen needs but the one kept for it
                    retire(log.nextOpenSegment(), SPARES);
                }
                return;
            }
            cutBytes = cut.logBytes();
            // Lagging once a second interval of log waits behind the cut: the commits will wait for this checkpoint
            var writer = pages.writer(random.nextLong(), () -> log.appendedBytes() - cutBytes > every);
            space.rewindRuns();
            var update = new TreeUpdate(pages, writer, space);
            TreeRoot[] roots;
            PageRef catalog;
            PageRef freeList;
            PageRef blobFreeList = null;
            DeltaTable nextDeltas;
            PageRef nextDeltaTable = null;
            var freed = new ArrayList<PageRef>();
            var adopted = new HashSet<PageRef>();
            var superblockWritten = false;
            try {
                roots = build(cut, update, adopted);
                nextDeltas = update.finishDeltas(deltas);
                freed.addAll(update.freed());
                if (deltaTable != null) {
                    freed.add(deltaTable);
                }
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
                catalog = writeCatalog(cut, roots, writer, update);
                BlobFile.Runs blobRuns = null;
                if (blobs != null) {
                    var alsoFree = new ArrayList<>(update.freedExternal());
                    alsoFree.addAll(cut.superseded());
                    blobRuns = blobs.durableRuns(adopted, alsoFree);
                    blobFreeList = writeBlobFreeList(writer, update, blobRuns);
                }
                freeList = writeFreeList(writer, update, freed);
                writer.flush();
                writer.awaitForce();
                if (blobs != null) {
                    // The units the trees take were written by commits the log may not have forced yet
                    blobs.force();
                }
                pages.force();
                superblockWritten = true;
                pages.writeSuperblock(new Superblock(number + 1, cut.sequence(), cut.logStart(), space.pageCount(), catalog, freeList,
                                blobRuns == null ? 0 : blobRuns.pageCount(), blobFreeList, blobs == null ? 0 : blobs.epoch(),
                                nextDeltaTable),
                        writer.nonceHigh(), writer.nextNonce());
                number++;
                if (blobs != null) {
                    durableEpoch = blobs.epoch();
                }
                deltas = nextDeltas;
                deltaTable = nextDeltaTable;
            } catch (IOException exception) {
                var failure = new LedgerIoException("The checkpoint could not be written", exception);
                if (superblockWritten) {
                    // Its pages stay in use: the superblock may be durable
                    stopped = failure;
                    writer.discard();
                    ledger.restoreCut(cut);
                } else {
                    abandon(cut, writer, update);
                }
                throw failure;
            } catch (RuntimeException | Error throwable) {
                abandon(cut, writer, update);
                throw throwable;
            } finally {
                ledger.readers().unregister(cut.sequence());
            }
            ledger.publish(cut, number, roots, catalog, freeList, blobFreeList, freed, update.freedExternal(), adopted);
            publishedBytes = cut.logBytes();
            publishedHeap = cut.logHeap();
            signalProgress();
            ledger.pruneCut(cut);
            // While the store runs, as many as a checkpoint interval fills, so the log seldom writes into a new file
            retire(cut.logStart(), closing ? SPARES : (int) Math.min(64, every / log.segmentSize() + 1));
            try {
                var end = space.trim();
                if (end > 0) {
                    pages.truncate(end);
                }
                if (blobs != null) {
                    blobs.trim();
                }
            } catch (IOException exception) {
                // The checkpoint is durable; trailing free pages wait for the next one
            }
        } finally {
            lock.unlock();
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
}
