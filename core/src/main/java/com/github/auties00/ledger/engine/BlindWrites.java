package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.function.Predicate;

/// The writes of one key space whose effect on its count is not known yet.
///
/// A commit of the pipeline that writes a key memory holds no version of does not look the key up in its tree: with a
/// tree larger than the page cache the lookup reads the file, and a commit would wait for a read to write. Such a blind
/// write cannot tell whether a put adds an entry or a removal takes one away, which only the tree knows. The tree it
/// would have read holds the key exactly when the trees of every later read that does not take the write hold it,
/// since no version of the key was in memory and no range removal since the tree covered it; so the checkpoint that
/// takes the write, which reads the key's leaf anyway, records on the key's chain whether that leaf held it, and a
/// count that needs it before then looks the key up in its own trees.
///
/// The count a reader at sequence `s` sees is the count of [KeyspaceState#counts], which leaves the blind writes out,
/// plus [#correction(long, Predicate)] at `s`. The writes are listed in the order of their sequences by the
/// publisher, holding the publishing role, which also folds into one total those every reader sees and whose effect
/// is known; any thread reads them.
final class BlindWrites {
    private static final int INITIAL_CAPACITY = 64;

    // The run the publisher appends to; a reader keeps the one it loaded, whose writes up to its end stay valid
    private volatile Run run;

    BlindWrites() {
        this.run = new Run(new Chain[INITIAL_CAPACITY], new long[INITIAL_CAPACITY], 0, 0, 0);
    }

    /// Lists the blind write of `chain`'s key by commit `sequence`; holding the publishing role, in the order of the
    /// sequences, before the commit is visible.
    ///
    /// @param removes whether the write removes the key
    void add(Chain chain, long sequence, boolean removes) {
        var current = run;
        var end = current.end;
        if (end == current.chains.length) {
            current = current.copy(Math.max(INITIAL_CAPACITY, 2 * (end - current.start)), current.folded);
            run = current;
            end = current.end;
        }
        current.chains[end] = chain;
        current.writes[end] = sequence << 1 | (removes ? 1 : 0);
        // A volatile store, after the slot: a reader that sees the end sees the write
        current.end = end + 1;
    }

    /// Folds into one total the listed writes at or below `oldest` whose effect is known, so the counts of the
    /// readers, none older than `oldest`, no longer walk them; holding the publishing role.
    ///
    /// @param oldest the sequence of the oldest read, or the latest sequence if none runs
    void fold(long oldest) {
        var current = run;
        var start = current.start;
        var end = current.end;
        var folded = current.folded;
        var next = start;
        while (next < end && (current.writes[next] >>> 1) <= oldest) {
            var state = current.chains[next].blindState;
            if (state == TreeSearch.UNKNOWN) {
                break;
            }
            folded += effect(state, current.writes[next]);
            next++;
        }
        if (next == start) {
            return;
        }
        // The arrays stay shared with the readers of the current run, which may still walk the writes folded here
        var replacement = next > current.chains.length / 2
                ? new Run(current, next, Math.max(INITIAL_CAPACITY, 2 * (end - next)), folded)
                : new Run(current.chains, current.writes, next, end, folded);
        run = replacement;
    }

    /// {@return the change the blind writes up to commit `sequence` make to the count of the key space}
    ///
    /// A write whose effect is not known yet is resolved by `inTree`, which must look the key up in trees that do not
    /// take any write above the last checkpoint that took the write's key, as the trees of any read at or after the
    /// write do while the write is not known; the answer is kept, so the next count does not look it up again.
    ///
    /// @param inTree whether the trees of the caller hold a key
    long correction(long sequence, Predicate<MemorySegment> inTree) {
        var current = run;
        var end = current.end;
        var total = current.folded;
        for (var i = current.start; i < end; i++) {
            var write = current.writes[i];
            if ((write >>> 1) > sequence) {
                break;
            }
            var chain = current.chains[i];
            var state = chain.blindState;
            if (state == TreeSearch.UNKNOWN) {
                // Every thread that resolves the write finds the same answer
                state = inTree.test(chain.key) ? TreeSearch.FOUND : TreeSearch.ABSENT;
                chain.blindState = state;
            }
            total += effect(state, write);
        }
        return total;
    }

    /// {@return the blind writes listed and not folded yet}
    int listed() {
        var current = run;
        return current.end - current.start;
    }

    /// {@return the change a write makes to the count: a put adds an entry the tree did not hold, a removal takes
    /// away one it held}
    private static long effect(byte state, long write) {
        var removes = (write & 1) != 0;
        if (state == TreeSearch.FOUND) {
            return removes ? -1 : 0;
        }
        return removes ? 0 : 1;
    }

    /// The writes listed from `start` to `end` of two arrays, after those folded into `folded`.
    ///
    /// Immutable but for [#end], which only grows: the publisher replaces the run to grow or fold, and copies its
    /// end, so a reader of a replaced run misses only writes listed after it loaded the run, which are above the
    /// sequence it reads at.
    private static final class Run {
        final Chain[] chains;
        // The sequence of each write, shifted left by one, with the low bit set for a removal
        final long[] writes;
        final int start;
        final long folded;
        volatile int end;

        Run(Chain[] chains, long[] writes, int start, int end, long folded) {
            this.chains = chains;
            this.writes = writes;
            this.start = start;
            this.end = end;
            this.folded = folded;
        }

        /// Copies the writes of `source` from `from` on into new arrays of `capacity`.
        Run(Run source, int from, int capacity, long folded) {
            var end = source.end;
            this.chains = new Chain[capacity];
            this.writes = new long[capacity];
            System.arraycopy(source.chains, from, chains, 0, end - from);
            System.arraycopy(source.writes, from, writes, 0, end - from);
            this.start = 0;
            this.end = end - from;
            this.folded = folded;
        }

        /// {@return a copy of the writes not folded into arrays of `capacity`}
        Run copy(int capacity, long folded) {
            return new Run(this, start, capacity, folded);
        }
    }
}
