package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.locks.ReentrantLock;

/// The range removals of one key space whose count is not known yet.
///
/// A batch with a range removal runs alone, and the removal must say how many entries it takes away. Counting them
/// reads the nodes of the tree on the path to each bound of the range, which after an open the page cache holds none
/// of: several reads of the file before the commit could queue its frame. A removal of a batch of a store in a directory
/// counts later instead ([Removal]): the commit registers a read at the sequence before its own, which keeps the trees,
/// the versions in memory and the earlier removals it would have counted against, and the count runs on a thread of the
/// store's own beside the log's force, or on the first thread that needs it.
///
/// The count a reader at sequence `s` sees is the count of [KeyspaceState#counts], which leaves these removals out,
/// plus [#correction(long)] at `s`. The removals are listed in the order of their sequences by the publisher, holding
/// the publishing role, which also folds into one total those every reader sees and whose count is known; any thread
/// reads them.
final class PendingRanges {
    private static final int INITIAL_CAPACITY = 8;
    // The array of a run with no removal listed, which no one writes, as the next removal copies it
    private static final Removal[] NO_REMOVALS = new Removal[0];

    // The run the publisher appends to; a reader keeps the one it loaded, whose removals up to its end stay valid
    private volatile Run run;

    PendingRanges() {
        this.run = new Run(NO_REMOVALS, 0, 0, 0);
    }

    /// Lists `removal`; holding the publishing role, in the order of the sequences, before its commit is visible.
    void add(Removal removal) {
        var current = run;
        var end = current.end;
        if (end == current.removals.length) {
            current = current.copy(Math.max(INITIAL_CAPACITY, 2 * (end - current.start)));
            run = current;
            end = current.end;
        }
        current.removals[end] = removal;
        // A volatile store, after the slot: a reader that sees the end sees the removal
        current.end = end + 1;
    }

    /// Folds into one total the listed removals at or below `oldest` whose count is known, so the counts of the
    /// readers, none older than `oldest`, no longer walk them; holding the publishing role.
    ///
    /// @param oldest the sequence of the oldest read, or the latest sequence if none runs
    void fold(long oldest) {
        var current = run;
        var start = current.start;
        var end = current.end;
        var folded = current.folded;
        var next = start;
        while (next < end) {
            var removal = current.removals[next];
            if (removal.sequence > oldest || !removal.settled()) {
                break;
            }
            folded += removal.effect;
            next++;
        }
        if (next == start) {
            return;
        }
        // The array stays shared with the readers of the current run, which may still walk the removals folded here:
        // a settled removal holds only its effect
        run = next == end ? new Run(NO_REMOVALS, 0, 0, folded)
                : next > current.removals.length / 2
                ? new Run(current, next, Math.max(INITIAL_CAPACITY, 2 * (end - next)), folded)
                : new Run(current.removals, next, end, folded);
    }

    /// {@return the change the removals up to commit `sequence` make to the count of the key space}
    ///
    /// A removal whose count is not known yet is counted on the calling thread, or waited for if another thread is
    /// counting it; the count is kept, so the next call reads nothing.
    ///
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if a page the count reads fails authentication;
    ///         the removal stays pending, and the next call counts it again
    /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read; likewise
    long correction(long sequence) {
        var current = run;
        var end = current.end;
        var total = current.folded;
        for (var i = current.start; i < end; i++) {
            var removal = current.removals[i];
            if (removal.sequence > sequence) {
                break;
            }
            total += removal.settle();
        }
        return total;
    }

    /// {@return the removals listed and not folded yet}
    int listed() {
        var current = run;
        return current.end - current.start;
    }

    /// A range removal of a committed batch whose entries are counted after the commit.
    ///
    /// It holds a registration of the store's [ReaderTable] at the sequence before its commit's, made while the
    /// commit ran alone, so every commit before it was published and none after it was: the registration keeps the
    /// trees it counts in, their pages, the versions in memory at or below its bound and the earlier range removals
    /// within it, so the count it makes later is the one the commit would have made. The registration goes once the
    /// count is known, and stays if the count fails, for the next attempt.
    ///
    /// Counting it is safe for concurrent use: one thread counts at a time, the others wait for it without being
    /// interruptible, and a count that fails leaves it for the next thread.
    static final class Removal {
        /// The sequence of the commit that holds the removal.
        final long sequence;
        /// The key space it removes from.
        final KeyspaceState state;
        private final MemorySegment from;
        private final MemorySegment to;
        private final ReaderTable readers;
        // The sequence of the read the removal holds, the one before its commit's
        private final long registered;
        // Held while the entries are counted, so one thread counts and the others wait
        private final ReentrantLock counting;
        // What the count reads, until it is known; then null, so a settled removal keeps no older trees reachable;
        // guarded by counting
        private ReadPoint point;
        // The change the removal makes to the count, minus the entries it removed; written before settled
        private long effect;
        private volatile boolean settled;

        /// Creates the removal of `from` to `to` in `state` by commit `sequence`, whose entries `point` counts.
        ///
        /// @param point what the commit would have counted against: the bound of the stamp before the removal's, over
        ///              the trees of the registration at `registered`
        /// @param registered the sequence of the registration with `readers` the caller made for the removal, which
        ///                   the removal now holds
        Removal(long sequence, KeyspaceState state, MemorySegment from, MemorySegment to, ReadPoint point,
                ReaderTable readers, long registered) {
            this.sequence = sequence;
            this.state = state;
            this.from = from;
            this.to = to;
            this.point = point;
            this.readers = readers;
            this.registered = registered;
            this.counting = new ReentrantLock();
        }

        /// {@return whether the count is known}
        boolean settled() {
            return settled;
        }

        /// {@return the change the removal makes to the count of its key space, counted first if it is not known}
        ///
        /// The thread that counts it releases the registration once the count is known.
        ///
        /// @throws com.github.auties00.ledger.LedgerCorruptedException if a page the count reads fails
        ///         authentication; the removal keeps its registration, and the next call counts again
        /// @throws com.github.auties00.ledger.LedgerIoException if the file cannot be read; likewise
        long settle() {
            if (settled) {
                return effect;
            }
            counting.lock();
            try {
                if (!settled) {
                    effect = -Reads.count(state, from, to, point);
                    settled = true;
                    point = null;
                    // Once the count is visible: a count that read the trees is done with them
                    readers.unregister(registered);
                }
                return effect;
            } finally {
                counting.unlock();
            }
        }

        /// Releases the registration of a removal whose commit was taken back, before anyone could see it.
        void abandon() {
            readers.unregister(registered);
        }
    }

    /// The removals listed from `start` to `end` of an array, after those folded into `folded`.
    ///
    /// Immutable but for [#end], which only grows: the publisher replaces the run to grow or fold, and copies its
    /// end, so a reader of a replaced run misses only removals listed after it loaded the run, which are above the
    /// sequence it reads at.
    private static final class Run {
        final Removal[] removals;
        final int start;
        final long folded;
        volatile int end;

        Run(Removal[] removals, int start, int end, long folded) {
            this.removals = removals;
            this.start = start;
            this.end = end;
            this.folded = folded;
        }

        /// Copies the removals of `source` from `from` on into a new array of `capacity`.
        Run(Run source, int from, int capacity, long folded) {
            var end = source.end;
            this.removals = new Removal[capacity];
            System.arraycopy(source.removals, from, removals, 0, end - from);
            this.start = 0;
            this.end = end - from;
            this.folded = folded;
        }

        /// {@return a copy of the removals not folded into an array of `capacity`}
        Run copy(int capacity) {
            return new Run(this, start, capacity, folded);
        }
    }
}
