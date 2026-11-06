package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;

/// Sorts the chains a checkpoint takes as [Chain#BY_KEY] orders them, keeping chains of equal keys in the order they
/// came in.
///
/// A comparison sort reads two chains, scattered over the heap, per comparison: a checkpoint of a million dirty keys
/// spent a second of every processor sorting them. This one reads each chain once for each 8 bytes of its key it
/// needs. It sorts the chains by the first 8 bytes of their keys, which [Chain#prefix] holds, with a radix sort over
/// arrays of their own; then each run of chains those bytes leave tied by the next 8 bytes, and so on until the keys
/// end. A short run, or one of keys that end within the bytes that tie them, goes to the comparator.
final class ChainSort {
    // A run this short goes to the comparator rather than to a radix pass of its own
    private static final int SMALL_RUN = 48;
    private static final int DIGIT_BITS = 8;
    private static final int BUCKETS = 1 << DIGIT_BITS;
    private static final ValueLayout.OfLong BIG_ENDIAN_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private ChainSort() {
    }

    /// Sorts `chains` in place.
    static void sort(Chain[] chains) {
        var length = chains.length;
        if (length <= SMALL_RUN) {
            Arrays.sort(chains, Chain.BY_KEY);
            return;
        }
        var words = new long[length];
        for (var i = 0; i < length; i++) {
            words[i] = chains[i].prefix;
        }
        var scratch = new Scratch(length);
        sortRun(chains, words, 0, length, 0, scratch);
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

    /// Sorts the run of chains from `from` to `to`, whose keys are equal before `offset`.
    private static void tied(Chain[] chains, long[] words, int from, int to, long offset, Scratch scratch) {
        if (to - from <= SMALL_RUN) {
            Arrays.sort(chains, from, to, Chain.BY_KEY);
            return;
        }
        var longer = false;
        for (var i = from; i < to; i++) {
            var key = chains[i].key;
            longer |= key.byteSize() > offset;
            words[i] = word(key, offset);
        }
        if (!longer) {
            // Every key ends before offset, tied up to its end: only their lengths, or nothing, tell them apart
            Arrays.sort(chains, from, to, Chain.BY_KEY);
            return;
        }
        sortRun(chains, words, from, to, offset, scratch);
    }

    /// {@return the 8 bytes of `key` at `offset`, big-endian, zero past its end}
    private static long word(MemorySegment key, long offset) {
        var size = key.byteSize();
        if (offset + Long.BYTES <= size) {
            return key.get(BIG_ENDIAN_LONG, offset);
        }
        var word = 0L;
        for (var at = offset; at < size; at++) {
            word |= (key.get(ValueLayout.JAVA_BYTE, at) & 0xFFL) << (56 - 8 * (at - offset));
        }
        return word;
    }

    /// Sorts the chains from `from` to `to` by `words`, as unsigned numbers, keeping chains of equal words in order: a
    /// least significant digit radix sort, which skips a digit every word shares.
    private static void radix(Chain[] chains, long[] words, int from, int to, Scratch scratch) {
        var length = to - from;
        var counts = scratch.counts;
        var inWords = words;
        var inChains = chains;
        var inFrom = from;
        var outWords = scratch.words;
        var outChains = scratch.chains;
        var outFrom = 0;
        for (var shift = 0; shift < Long.SIZE; shift += DIGIT_BITS) {
            Arrays.fill(counts, 0);
            for (var i = inFrom; i < inFrom + length; i++) {
                counts[(int) (inWords[i] >>> shift) & (BUCKETS - 1)]++;
            }
            if (counts[(int) (inWords[inFrom] >>> shift) & (BUCKETS - 1)] == length) {
                continue;
            }
            var position = outFrom;
            for (var digit = 0; digit < BUCKETS; digit++) {
                var count = counts[digit];
                counts[digit] = position;
                position += count;
            }
            for (var i = inFrom; i < inFrom + length; i++) {
                var digit = (int) (inWords[i] >>> shift) & (BUCKETS - 1);
                var at = counts[digit]++;
                outWords[at] = inWords[i];
                outChains[at] = inChains[i];
            }
            var swapWords = inWords;
            var swapChains = inChains;
            var swapFrom = inFrom;
            inWords = outWords;
            inChains = outChains;
            inFrom = outFrom;
            outWords = swapWords;
            outChains = swapChains;
            outFrom = swapFrom;
        }
        if (inWords != words) {
            System.arraycopy(inWords, inFrom, words, from, length);
            System.arraycopy(inChains, inFrom, chains, from, length);
        }
    }

    /// The arrays a radix pass moves the chains through, as long as the chains to sort.
    private static final class Scratch {
        final long[] words;
        final Chain[] chains;
        final int[] counts;

        Scratch(int length) {
            this.words = new long[length];
            this.chains = new Chain[length];
            this.counts = new int[BUCKETS];
        }
    }
}
