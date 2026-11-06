package com.github.auties00.ledger.engine;

/// The number of entries of a key space as of each commit that changed it, from the oldest a reader may need: the
/// counts of [KeyspaceState], leaving out the blind writes ([BlindWrites]).
///
/// Two arrays of `long`s, the sequences ascending and the counts beside them, in a run the publisher appends to and
/// replaces to grow or to drop the counts no reader needs, as [BlindWrites] keeps its writes. A skip list of boxed
/// numbers took 84 bytes a commit, which a checkpoint, holding the oldest read at its cut, kept for every commit it
/// lasted: 16 here.
///
/// The publisher, holding the store's publishing role, appends and drops; any thread reads.
final class CountHistory {
    private static final int INITIAL_CAPACITY = 16;

    // The run the publisher appends to; a reader keeps the one it loaded, whose counts up to its end stay valid
    private volatile Run run;

    /// Starts the history with `count` entries as of sequence `sequence`.
    CountHistory(long sequence, long count) {
        reset(sequence, count);
    }

    /// Forgets every count but `count`, the number of entries as of `sequence`; while the store opens.
    void reset(long sequence, long count) {
        var fresh = new Run(new long[INITIAL_CAPACITY], new long[INITIAL_CAPACITY], 0, 0);
        fresh.sequences[0] = sequence;
        fresh.counts[0] = count;
        fresh.end = 1;
        run = fresh;
    }

    /// Records that the key space holds `count` entries as of commit `sequence`, above every sequence recorded;
    /// holding the publishing role, before the commit is visible.
    void append(long sequence, long count) {
        var current = run;
        var end = current.end;
        if (end == current.sequences.length) {
            current = current.copy(Math.max(INITIAL_CAPACITY, 2 * (end - current.start)));
            run = current;
            end = current.end;
        }
        current.sequences[end] = sequence;
        current.counts[end] = count;
        // A volatile store, after the slots: a reader that sees the end sees the count
        current.end = end + 1;
    }

    /// {@return the number of entries as of the last commit recorded}
    long last() {
        var current = run;
        return current.counts[current.end - 1];
    }

    /// {@return the number of entries a reader at `sequence` sees: as of the last commit at or below it, 0 if none}
    long at(long sequence) {
        var current = run;
        var low = current.start;
        var high = current.end - 1;
        if (high < low || current.sequences[low] > sequence) {
            return 0;
        }
        while (low < high) {
            var middle = (low + high + 1) >>> 1;
            if (current.sequences[middle] <= sequence) {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        return current.counts[low];
    }

    /// Drops the counts no reader at or above `oldest` needs, keeping the one it reads at; holding the publishing role.
    void dropBelow(long oldest) {
        var current = run;
        var end = current.end;
        var keep = current.start;
        // The last count at or below oldest stays; the ones before it go
        while (keep + 1 < end && current.sequences[keep + 1] <= oldest) {
            keep++;
        }
        if (keep == current.start) {
            return;
        }
        // The arrays stay shared with the readers of the current run, which may still read the counts dropped here
        run = keep > current.sequences.length / 2
                ? new Run(current, keep, Math.max(INITIAL_CAPACITY, 2 * (end - keep)))
                : new Run(current.sequences, current.counts, keep, end);
    }

    /// {@return the counts recorded and not dropped; for tests}
    int sizeForTests() {
        var current = run;
        return current.end - current.start;
    }

    /// The counts from `start` to `end` of two arrays.
    ///
    /// Immutable but for [#end], which only grows: the publisher replaces the run to grow or to drop, and copies its
    /// end, so a reader of a replaced run misses only counts recorded after it loaded the run, which are above the
    /// sequence it reads at.
    private static final class Run {
        final long[] sequences;
        final long[] counts;
        final int start;
        volatile int end;

        Run(long[] sequences, long[] counts, int start, int end) {
            this.sequences = sequences;
            this.counts = counts;
            this.start = start;
            this.end = end;
        }

        /// Copies the counts of `source` from `from` on into new arrays of `capacity`.
        Run(Run source, int from, int capacity) {
            var end = source.end;
            this.sequences = new long[capacity];
            this.counts = new long[capacity];
            System.arraycopy(source.sequences, from, sequences, 0, end - from);
            System.arraycopy(source.counts, from, counts, 0, end - from);
            this.start = 0;
            this.end = end - from;
        }

        /// {@return a copy of the counts not dropped into arrays of `capacity`}
        Run copy(int capacity) {
            return new Run(this, start, capacity);
        }
    }
}
