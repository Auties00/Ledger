package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/// Writes the checkpoints of a store in a directory.
///
/// A checkpoint takes, under the writer lock, the keys written since the last one and the range removals since, as
/// of the latest sequence ([LiveLedger#cutCheckpoint]); then, while commits go on, it applies them to the trees copy
/// on write ([TreeUpdate]), writes the catalog and the free list, forces the file, writes the superblock the last one
/// did not use and forces it. Only then are the new trees published, the log segments before the cut deleted and
/// the versions the trees now hold dropped from memory. A crash at any point leaves the last durable superblock and
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
    private final LiveLedger ledger;
    private final PageFile pages;
    private final FreeSpace space;
    private final LogWriter log;
    private final Path logDirectory;
    private final long every;
    private final ReentrantLock lock;
    private final AtomicBoolean scheduled;
    private final SecureRandom random;
    // The number of the last durable checkpoint, guarded by the lock
    private long number;
    // The log's queued bytes at the last cut
    private volatile long cutBytes;
    private volatile RuntimeException lastFailure;
    // Set when a superblock write failed: no checkpoint runs again in this open
    private volatile LedgerIoException stopped;

    /// Creates the checkpointer of `ledger`.
    ///
    /// @param every the log growth that starts a checkpoint, in bytes
    /// @param number the number of the last durable checkpoint, or 0
    Checkpointer(LiveLedger ledger, PageFile pages, FreeSpace space, LogWriter log, Path logDirectory, long every, long number) {
        this.ledger = ledger;
        this.pages = pages;
        this.space = space;
        this.log = log;
        this.logDirectory = logDirectory;
        this.every = every;
        this.number = number;
        this.lock = new ReentrantLock();
        this.scheduled = new AtomicBoolean();
        this.random = new SecureRandom();
    }

    /// Starts a checkpoint on a thread of its own if the log has grown enough since the last, and none is running.
    void maybeStart() {
        // TODO: make committers wait while the log grows past a few intervals during a checkpoint, so memory stays
        //  bounded under a write rate the checkpoints cannot keep up with
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
            } finally {
                scheduled.set(false);
            }
        });
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
            var cut = ledger.cutCheckpoint(() -> closing ? log.currentSegment() + 1 : log.currentSegment());
            if (cut == null) {
                return;
            }
            cutBytes = cut.logBytes();
            var writer = pages.writer(random.nextLong());
            var update = new TreeUpdate(pages, writer, space);
            TreeRoot[] roots;
            PageRef catalog;
            PageRef freeList;
            var freed = new ArrayList<PageRef>();
            var superblockWritten = false;
            try {
                roots = build(cut, update);
                freed.addAll(update.freed());
                if (cut.base() != null) {
                    freed.add(cut.base().catalog);
                    freed.add(cut.base().freeList);
                }
                catalog = writeCatalog(cut, roots, writer, update);
                freeList = writeFreeList(writer, update, freed);
                writer.flush();
                pages.force();
                superblockWritten = true;
                pages.writeSuperblock(new Superblock(number + 1, cut.sequence(), cut.logStart(), space.pageCount(), catalog, freeList),
                        writer.nonceHigh(), writer.nextNonce());
                number++;
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
            ledger.publish(cut, number, roots, catalog, freeList, freed);
            ledger.pruneCut(cut);
            try {
                LogRecovery.deleteSegmentsBefore(logDirectory, cut.logStart());
                var end = space.trim();
                if (end > 0) {
                    pages.truncate(end);
                }
            } catch (IOException exception) {
                // The checkpoint is durable; obsolete segments and trailing free pages wait for the next one
            }
        } finally {
            lock.unlock();
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
    private TreeRoot[] build(LiveLedger.Cut cut, TreeUpdate update) throws IOException {
        var roots = cut.base() == null ? new TreeRoot[cut.maxId() + 1] : cut.base().roots(cut.maxId() + 1);
        var bound = Reads.boundOf(cut.sequence());
        var treeBound = cut.base() == null ? -1 : Reads.boundOf(cut.base().sequence);
        for (var keyspace : cut.keyspaces()) {
            var state = keyspace.state();
            var ops = new ArrayList<TreeUpdate.Op>(keyspace.dirty().size());
            for (var key : keyspace.dirty()) {
                var version = KeyspaceState.visible(state.entries.get(key), key, bound, keyspace.removals());
                ops.add(new TreeUpdate.Op(key, version == null ? null : version.value));
            }
            var ranges = new ArrayList<TreeUpdate.Range>();
            for (var removal : keyspace.removals()) {
                if (removal.stamp() > treeBound && removal.stamp() <= bound) {
                    ranges.add(new TreeUpdate.Range(removal.from(), removal.to()));
                }
            }
            var root = update.apply(roots[state.id], ops, TreeUpdate.merge(ranges));
            var count = root == null ? 0 : root.count();
            if (count != keyspace.count()) {
                throw new IllegalStateException("The checkpoint of " + state.keyspace.name() + " holds " + count
                        + " entries where the store counts " + keyspace.count());
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
