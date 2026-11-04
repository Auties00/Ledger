package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;
import java.util.concurrent.locks.ReentrantLock;

/// Writes the checkpoints of a store in a directory.
///
/// A checkpoint takes, under the writer lock, the keys written since the last one and the range removals since, as
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
    /// The obsolete segments kept for reuse while the store runs; a closed store keeps none.
    private static final int SPARES = 1;
    /// The checkpoint intervals of log the trees may lack before commits wait for a checkpoint.
    private static final int BACKLOG = 3;

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
    // Signalled whenever a checkpoint ends, for the commits that wait on one
    private final ReentrantLock progressLock;
    private final Condition progress;
    // Set when a superblock write failed: no checkpoint runs again in this open
    private volatile LedgerIoException stopped;

    /// Creates the checkpointer of `ledger`.
    ///
    /// @param every the log growth that starts a checkpoint, in bytes
    /// @param number the number of the last durable checkpoint, or 0
    /// @param failures receives the failures of the checkpoints started on their own threads
    Checkpointer(LiveLedger ledger, PageFile pages, FreeSpace space, BlobFile blobs, LogWriter log, LogFiles files, long every,
                 long number, Consumer<? super LedgerException> failures) {
        this.ledger = ledger;
        this.pages = pages;
        this.space = space;
        this.blobs = blobs;
        this.log = log;
        this.files = files;
        this.every = every;
        this.number = number;
        this.lock = new ReentrantLock();
        this.scheduled = new AtomicBoolean();
        this.random = new SecureRandom();
        this.failures = failures;
        this.progressLock = new ReentrantLock();
        this.progress = progressLock.newCondition();
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

    /// Makes the caller wait while the log holds more than [#BACKLOG] checkpoint intervals the trees do not, and a
    /// checkpoint is running to take them in.
    ///
    /// It never waits while checkpoints fail: the commits go on, and the log grows until one succeeds.
    void throttle() {
        if (log.appendedBytes() - publishedBytes <= BACKLOG * every || lastFailure != null || stopped != null) {
            return;
        }
        maybeStart();
        progressLock.lock();
        try {
            while (log.appendedBytes() - publishedBytes > BACKLOG * every && scheduled.get() && lastFailure == null) {
                progress.awaitUninterruptibly();
            }
        } finally {
            progressLock.unlock();
        }
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
            var cut = ledger.cutCheckpoint(() -> closing ? log.currentSegment() + 1 : log.currentSegment());
            if (cut == null) {
                if (closing) {
                    // The trees hold every commit, so the segments hold nothing a reopen needs
                    retire(Long.MAX_VALUE, 0);
                }
                return;
            }
            cutBytes = cut.logBytes();
            var writer = pages.writer(random.nextLong());
            var update = new TreeUpdate(pages, writer, space);
            TreeRoot[] roots;
            PageRef catalog;
            PageRef freeList;
            PageRef blobFreeList = null;
            var freed = new ArrayList<PageRef>();
            var adopted = new HashSet<PageRef>();
            var superblockWritten = false;
            try {
                roots = build(cut, update, adopted);
                freed.addAll(update.freed());
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
                if (blobs != null) {
                    // The units the trees take were written by commits the log may not have forced yet
                    blobs.force();
                }
                pages.force();
                superblockWritten = true;
                pages.writeSuperblock(new Superblock(number + 1, cut.sequence(), cut.logStart(), space.pageCount(), catalog, freeList,
                                blobRuns == null ? 0 : blobRuns.pageCount(), blobFreeList, blobs == null ? 0 : blobs.epoch()),
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
            ledger.publish(cut, number, roots, catalog, freeList, blobFreeList, freed, update.freedExternal(), adopted);
            publishedBytes = cut.logBytes();
            signalProgress();
            ledger.pruneCut(cut);
            retire(cut.logStart(), closing ? 0 : SPARES);
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
            // Listed in the order of their writes, off the writer lock: sorted here, on every processor
            var chains = keyspace.dirty().toArray(Chain[]::new);
            Arrays.parallelSort(chains, Chain.BY_KEY);
            var ops = new ArrayList<TreeUpdate.Op>(chains.length);
            MemorySegment previous = null;
            for (var chain : chains) {
                var key = chain.key;
                if (previous != null && Segments.compare(previous, key) == 0) {
                    continue;
                }
                previous = key;
                // The key's chain now, which is another one if the writer dropped this one
                var version = KeyspaceState.resolve(state.head(key), key, bound, keyspace.removals());
                if (version == null && !state.resident) {
                    // No version at the cut in a paged key space: a commit that wrote the key rolled back, so the
                    // tree is right. A resident one holds every entry, so there the key is gone
                    continue;
                }
                if (version == null || version == Version.HIDDEN) {
                    ops.add(new TreeUpdate.Op(key, null));
                } else {
                    ops.add(new TreeUpdate.Op(key, version.value, version.external));
                    if (version.external != null) {
                        adopted.add(version.external);
                    }
                }
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

    /// Writes the free list of the file of large values the checkpoint makes durable, in pages of the data file.
    private PageRef writeBlobFreeList(PageFile.Writer writer, TreeUpdate update, BlobFile.Runs runs) throws IOException {
        var unitPages = PageFormat.pagesFor(Catalog.freeListLength(runs.runs().size()));
        var page = allocate(unitPages, update);
        var unit = Catalog.encodeFreeList(runs.runs(), unitPages);
        return writer.write(PageFormat.FREE_LIST, 0, page, unitPages, unit, Bytes.i32(unit, PageFormat.LENGTH), false);
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
