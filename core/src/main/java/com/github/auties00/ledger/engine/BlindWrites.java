package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
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
/// A [RunFreezer] that drops the chain of such a write before either hands the write to the frozen [PackedRun] it froze
/// the chain into: the write leaves the list, and the run's entry counts it ([PackedRun#BLIND_PUT],
/// [PackedRun#BLIND_REMOVE]) against whether the tree under the run holds the key, which the checkpoint that merges the
/// run records, or a count looks up. The list and the run's counts are published together, as one [Run], so a count
/// counts such a write once, through its slot or through the run.
///
/// The count a reader at sequence `s` sees is the count of [KeyspaceState#counts], which leaves the blind writes out,
/// plus [#correction(long, Predicate)] at `s`. The writes are listed in the order of their sequences by the
/// publisher, holding the publishing role, which also folds into one total those every reader sees and whose effect
/// is known; any thread reads them.
final class BlindWrites {
    private static final int INITIAL_CAPACITY = 64;
    // The arrays of a run with no write listed, which no one writes, as the next write copies them: a key space with
    // no blind write unfolded keeps none
    private static final Chain[] NO_CHAINS = new Chain[0];
    private static final long[] NO_WRITES = new long[0];
    // Take the place of the chains of folded writes, whose effect they give the same: a folded slot stays in arrays a
    // reader of an older run may walk, and kept the chain, its key and its key's chunk reachable after the checkpoint
    // that took the key dropped it
    private static final Chain FOUND = settled(TreeSearch.FOUND);
    private static final Chain ABSENT = settled(TreeSearch.ABSENT);

    // The run the publisher appends to; a reader keeps the one it loaded, whose writes up to its end stay valid
    private volatile Run run;

    BlindWrites() {
        this.run = new Run(NO_CHAINS, NO_WRITES, 0, 0, 0, Frozen.NONE);
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
    /// readers, none older than `oldest`, no longer walk them, and the writes of a frozen run no reader reads any more
    /// once their count is known; holding the publishing role. It stops at the first write a freeze handed to its run
    /// ([Chain#frozen()]), which [#publishFrozen(Listing, PackedRun, byte\[\], boolean)] takes out of the list.
    ///
    /// @param oldest the sequence of the oldest read, or the latest sequence if none runs
    void fold(long oldest) {
        var current = run;
        var start = current.start;
        var end = current.end;
        var folded = current.folded;
        var frozen = current.frozen;
        if (frozen.released && frozen.known()) {
            folded += frozen.total;
            frozen = Frozen.NONE;
        }
        var next = start;
        while (next < end && (current.writes[next] >>> 1) <= oldest) {
            var chain = current.chains[next];
            var state = chain.blindState();
            // A write a freeze handed to its run stays until the publication that makes the run count it takes it out:
            // folded before, a count settled since, it would count in the total and in the run
            if (state == TreeSearch.UNKNOWN || chain.frozen()) {
                break;
            }
            folded += effect(state, current.writes[next]);
            current.chains[next] = state == TreeSearch.FOUND ? FOUND : ABSENT;
            next++;
        }
        if (next == start && frozen == current.frozen) {
            return;
        }
        // The arrays stay shared with the readers of the current run, which may still walk the writes folded here
        var replacement = next == end ? new Run(NO_CHAINS, NO_WRITES, 0, 0, folded, frozen)
                : next > current.chains.length / 2
                ? new Run(current, next, Math.max(INITIAL_CAPACITY, 2 * (end - next)), folded, frozen)
                : new Run(current.chains, current.writes, next, end, folded, frozen);
        run = replacement;
    }

    /// {@return the change the blind writes up to commit `sequence` make to the count of the key space, with those a
    /// frozen run counts}
    ///
    /// A write whose effect is not known yet is resolved by `inTree`, which must look the key up in trees that do not
    /// take any write above the last checkpoint that took the write's key, as the trees of any read at or after the
    /// write do while the write is not known, and never in a frozen run, which may hold the write's own entry; the
    /// answer is kept, so the next count does not look it up again. The writes a frozen run counts are older than
    /// every reader that can load the run they are listed in, so they count whatever `sequence`; an entry whose key
    /// the run does not know to be in its tree yet is looked up there, once.
    ///
    /// @param inTree whether the trees of the caller hold a key
    long correction(long sequence, Predicate<MemorySegment> inTree) {
        var current = run;
        var end = current.end;
        var total = current.folded + current.frozen.correction();
        for (var i = current.start; i < end; i++) {
            var write = current.writes[i];
            if ((write >>> 1) > sequence) {
                break;
            }
            var chain = current.chains[i];
            var state = chain.blindState();
            if (state == TreeSearch.UNKNOWN) {
                // Every thread that resolves the write finds the same answer
                var found = inTree.test(chain.key());
                chain.settle(found);
                state = found ? TreeSearch.FOUND : TreeSearch.ABSENT;
            }
            total += effect(state, write);
        }
        return total;
    }

    /// {@return what the list counts for each entry of `frozen`, the run it counts the writes of; `null` if it counts
    /// none, or another run}
    ///
    /// The array is the list's: no one changes it.
    byte[] effectsOf(PackedRun frozen) {
        var current = run.frozen;
        return current.run == frozen ? current.effects : null;
    }

    /// {@return the frozen run whose writes the list counts, or `null`}
    PackedRun frozenRun() {
        return run.frozen.run;
    }

    /// {@return the listed writes as they stand, but those a freeze handed to its run ([Chain#frozen()]), for
    /// [#publishFrozen(Listing, PackedRun, byte\[\], boolean)]; safe without the publishing role}
    Listing listing() {
        var current = run;
        var end = current.end;
        var chains = new Chain[Math.max(INITIAL_CAPACITY, 2 * (end - current.start))];
        var writes = new long[chains.length];
        var kept = 0;
        for (var i = current.start; i < end; i++) {
            var chain = current.chains[i];
            if (!chain.frozen()) {
                chains[kept] = chain;
                writes[kept++] = current.writes[i];
            }
        }
        return new Listing(current, end, chains, writes, kept);
    }

    /// Makes the list count the writes of `frozen` as `effects` says, in place of those of the frozen run it counted,
    /// leaving every write listed; holding the publishing role, once a freeze that handed no blind write over
    /// published `frozen`.
    ///
    /// `effects` is the list's from then on: no one changes it.
    void replaceFrozen(PackedRun frozen, byte[] effects) {
        var current = run;
        run = new Run(current.chains, current.writes, current.start, current.end, current.folded, new Frozen(frozen, effects));
    }

    /// Makes the list count the writes of `frozen` as `effects` says, in place of those of the frozen run it counted,
    /// and leaves out of it the blind writes a freeze handed to `frozen` ([Chain#frozen()]); holding the publishing
    /// role, after the freeze dropped the chains whose writes it counts there and marked those it hands over.
    ///
    /// `effects` is the list's from then on: no one changes it.
    ///
    /// @param prepared what [#listing()] collected without the role once the freeze marked every write it hands over
    /// @param force whether to walk the list again here, holding the role, if it changed since `prepared` but for the
    ///              writes listed after it, rather than return `false`
    /// @return whether it did; `false`, changing nothing, if the list changed and `force` is not set: the caller
    ///         collects it again
    boolean publishFrozen(Listing prepared, PackedRun frozen, byte[] effects, boolean force) {
        var current = run;
        var listing = prepared;
        if (prepared.source != current) {
            if (!force) {
                return false;
            }
            listing = listing();
        }
        var end = current.end;
        var chains = listing.chains;
        var writes = listing.writes;
        var kept = listing.kept;
        var tail = end - listing.end;
        if (kept + tail > chains.length) {
            chains = Arrays.copyOf(chains, Math.max(INITIAL_CAPACITY, 2 * (kept + tail)));
            writes = Arrays.copyOf(writes, chains.length);
        }
        // Listed since, by commits after the drops: none of their chains is one a freeze dropped
        for (var i = listing.end; i < end; i++) {
            chains[kept] = current.chains[i];
            writes[kept++] = current.writes[i];
        }
        run = new Run(chains, writes, 0, kept, current.folded, new Frozen(frozen, effects));
        return true;
    }

    /// Records that no reader reads `released` any more, so the writes of the frozen run the list counts, `released`
    /// or the one it replaced, which every tree that holds `released` holds as well, fold into the total once their
    /// count is known: at once if it is, or at a later [#fold(long)]; holding the publishing role.
    void releaseFrozen(PackedRun released) {
        var current = run;
        var frozen = current.frozen.run;
        if (frozen == null || current.frozen.released || frozen.maxStamp() > released.maxStamp()) {
            return;
        }
        var done = new Frozen(frozen, current.frozen.effects, current.frozen.total, true);
        var folded = current.folded;
        if (done.known()) {
            folded += done.total;
            done = Frozen.NONE;
        }
        run = new Run(current.chains, current.writes, current.start, current.end, folded, done);
    }

    /// {@return a chain of no key whose blind write's effect is known to be `state`}
    private static Chain settled(byte state) {
        var chain = new Chain(MemorySegment.ofArray(new byte[0]), 0);
        chain.settle(state == TreeSearch.FOUND);
        return chain;
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

    /// The listed writes a freeze collected without the publishing role, leaving out those it handed to its run.
    ///
    /// @param source the run they came from
    /// @param end the end of `source` they reach
    /// @param chains the chains of the writes kept, with room for more
    /// @param writes their writes
    /// @param kept how many it kept
    record Listing(Run source, int end, Chain[] chains, long[] writes, int kept) {
    }

    /// The frozen run whose blind writes a list counts, with what it counts for each of its entries.
    ///
    /// The total is counted at the first need and kept: the entries' effects never change, and whether the tree under
    /// the run holds a key is a fact a lookup and the checkpoint that merges the run find the same.
    private static final class Frozen {
        static final Frozen NONE = new Frozen(null, null, 0, false);
        // Not counted yet
        private static final long UNCOUNTED = Long.MIN_VALUE;

        final PackedRun run;
        final byte[] effects;
        // Whether every reader reads a tree that holds the run, so its total may fold
        final boolean released;
        volatile long total;

        Frozen(PackedRun run, byte[] effects) {
            this(run, effects, UNCOUNTED, false);
        }

        Frozen(PackedRun run, byte[] effects, long total, boolean released) {
            this.run = run;
            this.effects = effects;
            this.total = run == null ? 0 : total;
            this.released = released;
        }

        boolean known() {
            return total != UNCOUNTED;
        }

        /// {@return what the run's blind writes do to the count, counted at the first call}
        long correction() {
            var counted = total;
            if (counted != UNCOUNTED) {
                return counted;
            }
            var sum = 0L;
            for (var entry = 0; entry < effects.length; entry++) {
                var effect = effects[entry];
                if (effect == PackedRun.BLIND_PUT) {
                    sum += run.baseState(entry) == TreeSearch.FOUND ? 0 : 1;
                } else if (effect == PackedRun.BLIND_REMOVE) {
                    sum -= run.baseState(entry) == TreeSearch.FOUND ? 1 : 0;
                }
            }
            total = sum;
            return sum;
        }
    }

    /// The writes listed from `start` to `end` of two arrays, after those folded into `folded`, and the frozen run
    /// whose writes count with them.
    ///
    /// Immutable but for [#end], which only grows: the publisher replaces the run to grow or fold, and copies its
    /// end, so a reader of a replaced run misses only writes listed after it loaded the run, which are above the
    /// sequence it reads at.
    static final class Run {
        final Chain[] chains;
        // The sequence of each write, shifted left by one, with the low bit set for a removal
        final long[] writes;
        final int start;
        final long folded;
        final Frozen frozen;
        volatile int end;

        Run(Chain[] chains, long[] writes, int start, int end, long folded, Frozen frozen) {
            this.chains = chains;
            this.writes = writes;
            this.start = start;
            this.end = end;
            this.folded = folded;
            this.frozen = frozen;
        }

        /// Copies the writes of `source` from `from` on into new arrays of `capacity`.
        Run(Run source, int from, int capacity, long folded, Frozen frozen) {
            var end = source.end;
            this.chains = new Chain[capacity];
            this.writes = new long[capacity];
            System.arraycopy(source.chains, from, chains, 0, end - from);
            System.arraycopy(source.writes, from, writes, 0, end - from);
            this.start = 0;
            this.end = end - from;
            this.folded = folded;
            this.frozen = frozen;
        }

        /// {@return a copy of the writes not folded into arrays of `capacity`}
        Run copy(int capacity, long folded) {
            return new Run(this, start, capacity, folded, frozen);
        }
    }
}
