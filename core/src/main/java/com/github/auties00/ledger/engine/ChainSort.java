package com.github.auties00.ledger.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/// Sorts the chains a checkpoint takes as [Chain#BY_KEY] orders them, keeping chains of equal keys in the order they
/// came in.
///
/// A comparison sort reads two chains, scattered over the heap, per comparison: a checkpoint of a million dirty keys
/// spent a second of every processor sorting them. This one reads each chain once for each 8 bytes of its key it
/// needs. It sorts the chains by the first 8 bytes of their keys, which [Chain#word(long)] reads, with a radix sort over
/// arrays of their own; then each run of chains those bytes leave tied by the next 8 bytes, and so on until the keys
/// end. A short run, or one of keys that end within the bytes that tie them, goes to the comparator.
///
/// From [#PARALLEL] chains on, it reads the first 8 bytes on every processor, in the common fork-join pool, and sorts
/// each group of chains the first byte that tells them apart makes on a processor of its own: reading scattered chains
/// waits on memory, which a second processor waits on beside the first.
final class ChainSort {
    // A run this short goes to the comparator rather than to a radix pass of its own
    private static final int SMALL_RUN = 48;
    private static final int DIGIT_BITS = 8;
    private static final int BUCKETS = 1 << DIGIT_BITS;
    /// The chains from which a sort runs on every processor: the small checkpoints of a store that commits a little at
    /// a time stay on the checkpoint's thread.
    private static final int PARALLEL = 1 << 16;

    private ChainSort() {
    }

    /// Sorts `chains` in place.
    ///
    /// @return the first 8 bytes of the key of each chain, as [Chain#word(long)] reads them, in the order of the sorted
    ///         chains
    static long[] sort(Chain[] chains) {
        var length = chains.length;
        var words = new long[length];
        if (length <= SMALL_RUN) {
            Arrays.sort(chains, Chain.BY_KEY);
            gather(chains, words);
        } else if (length < PARALLEL || ForkJoinPool.getCommonPoolParallelism() <= 1) {
            gather(chains, words);
            sortRun(chains, words, 0, length, 0, new Scratch(length));
        } else {
            Arrays.parallelSetAll(words, i -> chains[i].word(0));
            sortInParallel(chains, words, new Scratch(length));
        }
        return words;
    }

    private static void gather(Chain[] chains, long[] words) {
        for (var i = 0; i < chains.length; i++) {
            words[i] = chains[i].word(0);
        }
    }

    /// Sorts all of `chains` by their keys, of which `words` holds the first 8 bytes, on every processor.
    ///
    /// A stable pass splits the chains by the first byte of `words` that differs, which orders the groups, and each
    /// group is sorted as a run of its own. Words that are all equal leave one group, sorted on this thread.
    private static void sortInParallel(Chain[] chains, long[] words, Scratch scratch) {
        var differing = 0L;
        for (var word : words) {
            differing |= word ^ words[0];
        }
        if (differing == 0) {
            tied(chains, words, 0, chains.length, Long.BYTES, scratch);
            return;
        }
        var shift = (Long.SIZE - 1 - Long.numberOfLeadingZeros(differing)) / DIGIT_BITS * DIGIT_BITS;
        var ends = scratch.counts;
        for (var word : words) {
            ends[(int) (word >>> shift) & (BUCKETS - 1)]++;
        }
        var position = 0;
        for (var digit = 0; digit < BUCKETS; digit++) {
            var count = ends[digit];
            ends[digit] = position;
            position += count;
        }
        for (var i = 0; i < chains.length; i++) {
            var at = ends[(int) (words[i] >>> shift) & (BUCKETS - 1)]++;
            scratch.words[at] = words[i];
            scratch.chains[at] = chains[i];
        }
        System.arraycopy(scratch.words, 0, words, 0, words.length);
        System.arraycopy(scratch.chains, 0, chains, 0, chains.length);
        var tasks = new ArrayList<ForkJoinTask<?>>(BUCKETS);
        var from = 0;
        for (var digit = 0; digit < BUCKETS; digit++) {
            var start = from;
            var end = ends[digit];
            if (end - start > 1) {
                tasks.add(ForkJoinTask.adapt(() -> sortRun(chains, words, start, end, 0, new Scratch(scratch))));
            }
            from = end;
        }
        ForkJoinTask.invokeAll(tasks);
    }

    /// Sorts `chains` from `from` to `to`, whose keys are equal before `offset`, by their keys from `offset`, which
    /// `words` holds 8 bytes of for each chain.
    private static void sortRun(Chain[] chains, long[] words, int from, int to, long offset, Scratch scratch) {
        radix(chains, words, from, to, scratch);
        var start = from;
        while (start < to) {
            var end = start + 1;
            while (end < to && words[end] == words[start]) {
                end++;
            }
            if (end - start > 1) {
                tied(chains, words, start, end, offset + Long.BYTES, scratch);
            }
            start = end;
        }
    }

    /// Sorts the run of chains from `from` to `to`, whose keys are equal before `offset` and whose words are equal.
    ///
    /// The words of the run are as they were when it returns, as those of a sorted run must be the first bytes of its
    /// keys.
    private static void tied(Chain[] chains, long[] words, int from, int to, long offset, Scratch scratch) {
        if (to - from <= SMALL_RUN) {
            Arrays.sort(chains, from, to, Chain.BY_KEY);
            return;
        }
        var word = words[from];
        var longer = false;
        for (var i = from; i < to; i++) {
            var chain = chains[i];
            longer |= chain.keyLength() > offset;
            words[i] = chain.word(offset);
        }
        if (!longer) {
            // Every key ends before offset, tied up to its end: only their lengths, or nothing, tell them apart
            Arrays.sort(chains, from, to, Chain.BY_KEY);
        } else {
            sortRun(chains, words, from, to, offset, scratch);
        }
        Arrays.fill(words, from, to, word);
    }

    /// Sorts the chains from `from` to `to` by `words`, as unsigned numbers, keeping chains of equal words in order: a
    /// least significant digit radix sort, which skips a digit every word shares. It uses only the positions from
    /// `from` to `to` of the arrays of `scratch`, so runs that do not overlap may share them.
    private static void radix(Chain[] chains, long[] words, int from, int to, Scratch scratch) {
        var length = to - from;
        var counts = scratch.counts;
        var inWords = words;
        var inChains = chains;
        var outWords = scratch.words;
        var outChains = scratch.chains;
        for (var shift = 0; shift < Long.SIZE; shift += DIGIT_BITS) {
            Arrays.fill(counts, 0);
            for (var i = from; i < to; i++) {
                counts[(int) (inWords[i] >>> shift) & (BUCKETS - 1)]++;
            }
            if (counts[(int) (inWords[from] >>> shift) & (BUCKETS - 1)] == length) {
                continue;
            }
            var position = from;
            for (var digit = 0; digit < BUCKETS; digit++) {
                var count = counts[digit];
                counts[digit] = position;
                position += count;
            }
            for (var i = from; i < to; i++) {
                var digit = (int) (inWords[i] >>> shift) & (BUCKETS - 1);
                var at = counts[digit]++;
                outWords[at] = inWords[i];
                outChains[at] = inChains[i];
            }
            var swapWords = inWords;
            var swapChains = inChains;
            inWords = outWords;
            inChains = outChains;
            outWords = swapWords;
            outChains = swapChains;
        }
        if (inWords != words) {
            System.arraycopy(inWords, from, words, from, length);
            System.arraycopy(inChains, from, chains, from, length);
        }
    }

    /// The arrays a radix pass moves the chains through, as long as the chains to sort, and the counts of its digits.
    private static final class Scratch {
        final long[] words;
        final Chain[] chains;
        final int[] counts;

        Scratch(int length) {
            this.words = new long[length];
            this.chains = new Chain[length];
            this.counts = new int[BUCKETS];
        }

        /// Makes the scratch of a task that sorts a run of its own: the arrays of `shared`, counts of its own.
        Scratch(Scratch shared) {
            this.words = shared.words;
            this.chains = shared.chains;
            this.counts = new int[BUCKETS];
        }
    }
}
