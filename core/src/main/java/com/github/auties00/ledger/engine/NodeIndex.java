package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Arrays;

/// A search index the page cache keeps behind a leaf or a branch: the array of a cached node runs past its page, and
/// a node holding an index searches its keys on a few lines of memory.
///
/// A search of a plain node reads an offset and then a key at every step, and every entry of a leaf holding small
/// values sits on a line of its own: in a tree larger than the processor's cache, each step waits for memory. The first
/// bytes after the keys' common prefix, kept together, decide most steps of a search without reading a key.
///
/// The searched keys are the keys of a leaf and the first keys of a branch's children but the first, which only the
/// search of a child before a key compares. Past the [page][PageFormat#SIZE], in the byte order of the files:
///
/// ```text
/// 0  bytes of the prefix the searched keys share, up to MAX_PREFIX, 2 bytes
/// 2  searched keys, 2 bytes
/// 4  the prefix
///    the head of each searched key, 4 bytes each, in key order: the 4 bytes of the key after the prefix, then zeros
///    where the key ends
/// ```
///
/// Heads never decrease in key order, and of two keys sharing the prefix, the one with the lower head sorts first: a
/// padding zero stands only where its key ended, so that key is a prefix of the other or below it at the first byte
/// the heads differ in. Only keys with equal heads need a comparison, which makes every step of a search the same step
/// of the search of a plain node.
///
/// The index exists only in memory. A unit is sealed and authenticated by the page count of its reference, so bytes past
/// the page never reach the file, and a node is immutable once cached, so an index stays true for as long as its array
/// lives. A node read past the cache, one the cache takes as the first to evict, and one a checkpoint reads to replace
/// or writes, whole or as a delta record, have no index, and search the plain way: a checkpoint of random overwrites
/// caches a leaf per record, most of which no search reads before the next checkpoint replaces them.
final class NodeIndex {
    /// The prefix an index holds at most. A shorter prefix than the keys share is still a prefix of each: it only leaves
    /// the heads less to tell apart, and it bounds what long keys add to the cache.
    static final int MAX_PREFIX = 64;
    /// The fewest searched keys an index is built for: a search of fewer reads about as many lines without one.
    static final int MIN_HEADS = 4;
    private static final int PREFIX_LENGTH = PageFormat.SIZE;
    private static final int HEADS = PageFormat.SIZE + 2;
    private static final int PREFIX = PageFormat.SIZE + 4;
    private static final int HEAD = Integer.BYTES;
    private static final ValueLayout.OfInt BIG_ENDIAN_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private NodeIndex() {
    }

    /// {@return whether the node `unit` holds an index}
    static boolean isPresent(byte[] unit) {
        return unit.length > PageFormat.SIZE;
    }

    /// {@return the leaf or branch `node`, which authenticated, with an index, or `node` if an index would not help}
    ///
    /// An index does not help a node with few keys, nor one whose longest run of equal heads is over a quarter of its
    /// keys: a leaf holding the end of one chat and the start of another shares few bytes, and the heads of each chat's
    /// keys are its id's last bytes and the bytes all its message ids start with. The search would compare keys through
    /// most of its steps, and read the index on top of them.
    ///
    /// @param node a unit of one page, which stays as it is; any other unit is returned as it is
    /// @throws LedgerCorruptedException if the entries of the node run past it or its keys are out of order
    static byte[] attach(byte[] node) {
        var kind = node[PageFormat.KIND];
        if ((kind != PageFormat.LEAF && kind != PageFormat.BRANCH) || node.length != PageFormat.SIZE) {
            return node;
        }
        try {
            return kind == PageFormat.LEAF ? build(node, 0, TreeNodes.LEAF_KEY) : build(node, 1, TreeNodes.BRANCH_KEY);
        } catch (IndexOutOfBoundsException exception) {
            throw new LedgerCorruptedException("A node's entries run past its page", exception);
        }
    }

    /// {@return `node` with an index of its keys from entry `first`, each `keyAt` bytes into its entry, or `node`}
    private static byte[] build(byte[] node, int first, int keyAt) {
        var count = TreeNodes.count(node) - first;
        if (count < MIN_HEADS) {
            return node;
        }
        var firstKey = TreeNodes.entry(node, first) + keyAt;
        var prefix = commonPrefix(node, firstKey, TreeNodes.entry(node, first + count - 1) + keyAt);
        var heads = PREFIX + prefix;
        var indexed = new byte[heads + HEAD * count];
        var longestRun = 0;
        var run = 0;
        var previous = 0;
        for (var i = 0; i < count; i++) {
            var key = TreeNodes.entry(node, first + i) + keyAt;
            var length = Bytes.u16(node, key - 2);
            if (length < prefix || key + length > PageFormat.SIZE) {
                throw new LedgerCorruptedException("A node's keys are out of order or run past its page", null);
            }
            var head = head(node, key + prefix, length - prefix);
            run = i > 0 && head == previous ? run + 1 : 1;
            longestRun = Math.max(longestRun, run);
            previous = head;
            Bytes.i32(indexed, heads + HEAD * i, head);
        }
        if (longestRun * 4 > count) {
            return node;
        }
        System.arraycopy(node, 0, indexed, 0, PageFormat.SIZE);
        Bytes.u16(indexed, PREFIX_LENGTH, prefix);
        Bytes.u16(indexed, HEADS, count);
        System.arraycopy(node, firstKey, indexed, PREFIX, prefix);
        return indexed;
    }

    /// {@return the bytes the keys at `left` and `right` of `node` share, up to [#MAX_PREFIX]}
    ///
    /// The length of a key is in the two bytes before it.
    private static int commonPrefix(byte[] node, int left, int right) {
        var leftLength = Math.min(Bytes.u16(node, left - 2), MAX_PREFIX);
        var rightLength = Math.min(Bytes.u16(node, right - 2), MAX_PREFIX);
        var mismatch = Arrays.mismatch(node, left, left + leftLength, node, right, right + rightLength);
        return mismatch == -1 ? leftLength : mismatch;
    }

    /// {@return the head of what follows the prefix of a key: the `length` bytes at `offset` of `node`}
    private static int head(byte[] node, int offset, int length) {
        if (length >= HEAD) {
            return Bytes.i32(node, offset);
        }
        var head = 0;
        for (var i = 0; i < HEAD; i++) {
            head = head << 8 | (i < length ? node[offset + i] & 0xFF : 0);
        }
        return head;
    }

    /// {@return the index of the first entry of the leaf `unit`, which holds an index, whose key is at or after `key`,
    /// or its count}
    static int lowerBound(byte[] unit, MemorySegment key) {
        return search(unit, key, 0, TreeNodes.LEAF_KEY, false);
    }

    /// {@return the index of the child of the branch `unit`, which holds an index, whose subtree would hold `key`}
    static int childFor(byte[] unit, MemorySegment key) {
        return search(unit, key, 1, TreeNodes.BRANCH_KEY, true) - 1;
    }

    /// {@return the index of the last child of the branch `unit`, which holds an index, whose first key is before
    /// `key`, or -1}
    static int lastChildBefore(byte[] unit, MemorySegment key) {
        var after = search(unit, key, 1, TreeNodes.BRANCH_KEY, false);
        if (after > 1) {
            return after - 1;
        }
        // The first child's key is not a searched key: the prefix says nothing of it
        var entry = TreeNodes.entry(unit, 0) + TreeNodes.BRANCH_KEY;
        return TreeNodes.compare(unit, entry, Bytes.u16(unit, entry - 2), key) < 0 ? 0 : -1;
    }

    /// {@return the first of the searched entries of `unit`, from entry `first`, whose key is after `key`, or at it
    /// unless `through`; past the last searched entry if there is none}
    ///
    /// @param keyAt the bytes into an entry where its key starts
    private static int search(byte[] unit, MemorySegment key, int first, int keyAt, boolean through) {
        var prefix = Bytes.u16(unit, PREFIX_LENGTH);
        var count = Bytes.u16(unit, HEADS);
        var order = comparePrefix(unit, prefix, key);
        if (order != 0) {
            return order > 0 ? first : first + count;
        }
        var heads = PREFIX + prefix;
        var head = head(key, prefix);
        var low = 0;
        var high = count;
        while (low < high) {
            var middle = (low + high) >>> 1;
            var step = Integer.compareUnsigned(Bytes.i32(unit, heads + HEAD * middle), head);
            if (step == 0) {
                var entry = TreeNodes.entry(unit, first + middle) + keyAt;
                step = TreeNodes.compare(unit, entry, Bytes.u16(unit, entry - 2), key);
                if (step == 0) {
                    step = through ? -1 : 1;
                }
            }
            if (step < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return first + low;
    }

    /// {@return a negative number, zero or a positive number as the prefix of `unit`, of `prefix` bytes, sorts before
    /// `key`, starts it, or sorts after it}
    private static int comparePrefix(byte[] unit, int prefix, MemorySegment key) {
        var common = (int) Math.min(prefix, key.byteSize());
        var at = 0;
        for (; at + Long.BYTES <= common; at += Long.BYTES) {
            var mine = Bytes.i64(unit, PREFIX + at);
            var theirs = key.get(Segments.BIG_ENDIAN_LONG, at);
            if (mine != theirs) {
                return Long.compareUnsigned(mine, theirs);
            }
        }
        for (; at < common; at++) {
            var mine = unit[PREFIX + at];
            var theirs = key.get(ValueLayout.JAVA_BYTE, at);
            if (mine != theirs) {
                return Byte.compareUnsigned(mine, theirs);
            }
        }
        return common == prefix ? 0 : 1;
    }

    /// {@return the head of `key`, which starts with the `prefix` bytes of a node's prefix}
    private static int head(MemorySegment key, int prefix) {
        var length = key.byteSize() - prefix;
        if (length >= HEAD) {
            return key.get(BIG_ENDIAN_INT, prefix);
        }
        var head = 0;
        for (var i = 0; i < HEAD; i++) {
            head = head << 8 | (i < length ? key.get(ValueLayout.JAVA_BYTE, prefix + i) & 0xFF : 0);
        }
        return head;
    }
}
