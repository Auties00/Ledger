package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/// Point lookups and range counts in a key space's tree.
final class TreeSearch {
    /// A lookup through the page cache met a node the cache does not hold.
    static final byte UNKNOWN = 0;
    /// The tree holds the key.
    static final byte FOUND = 1;
    /// The tree does not hold the key.
    static final byte ABSENT = 2;
    /// A hint that the write does not look the key up: unless memory tells, it is a blind write ([BlindWrites]).
    static final byte BLIND = 3;

    private TreeSearch() {
    }

    /// {@return whether the tree under `root` holds `key`, as [#FOUND] or [#ABSENT], looked up through the page cache
    /// alone, or [#UNKNOWN] if the cache does not hold a node on the way}
    static byte lookupCached(PageFile pages, TreeRoot root, MemorySegment key) {
        var unit = pages.cached(root.ref(), PageFile.NODE);
        while (unit != null && !TreeNodes.isLeaf(unit)) {
            unit = pages.cachedNode(unit, TreeNodes.childFor(unit, key));
        }
        if (unit == null) {
            return UNKNOWN;
        }
        return entryOf(unit, key) != -1 ? FOUND : ABSENT;
    }

    /// {@return whether the tree under `root` holds `key`}
    static boolean contains(PageFile pages, TreeRoot root, MemorySegment key) {
        var unit = pages.read(root.ref(), PageFile.NODE, true);
        while (!TreeNodes.isLeaf(unit)) {
            unit = pages.node(unit, TreeNodes.childFor(unit, key));
        }
        return entryOf(unit, key) != -1;
    }

    /// {@return the leaf of the tree under `root` that would hold `key`}
    ///
    /// With [#entryOf(byte\[\], byte\[\])], the lookup of a read by key: the key in an array, which the nodes compare
    /// with their own word by word, and no object for what it finds.
    static byte[] leaf(PageFile pages, TreeRoot root, byte[] key) {
        var unit = pages.read(root.ref(), PageFile.NODE, true);
        while (!TreeNodes.isLeaf(unit)) {
            unit = pages.node(unit, TreeNodes.childFor(unit, key));
        }
        return unit;
    }

    /// {@return the offset of the entry of leaf `unit` holding `key`, or -1 if it holds none}
    static int entryOf(byte[] unit, byte[] key) {
        var index = TreeNodes.lowerBound(unit, key);
        if (index == TreeNodes.count(unit)) {
            return -1;
        }
        var entry = TreeNodes.entry(unit, index);
        return TreeNodes.compare(unit, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(unit, entry), key) == 0
                ? entry
                : -1;
    }

    /// {@return the offset of the entry of leaf `unit` holding `key`, or -1 if it holds none}
    private static int entryOf(byte[] unit, MemorySegment key) {
        var index = TreeNodes.lowerBound(unit, key);
        if (index == TreeNodes.count(unit)) {
            return -1;
        }
        var entry = TreeNodes.entry(unit, index);
        return TreeNodes.compare(unit, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(unit, entry), key) == 0
                ? entry
                : -1;
    }

    /// A leaf of a tree, as its parent names it.
    ///
    /// @param first the lowest key of the leaf, a read-only view of the parent, or `null` for a leaf with no parent
    record Leaf(PageRef ref, MemorySegment first) {
    }

    /// The leaves of a tree that may hold keys at or after a key, as far as the nodes on the path to that key name them.
    ///
    /// @param leaves the leaves of the lowest branch on the path, from the one that would hold the key on, in key
    ///               order; or the root alone, if it is a leaf
    /// @param rest   the subtrees after them, as lists of nodes of one level, each in key order: the nodes after the
    ///               path's own in its parent, a level at a time from the lowest, so every node of a list holds keys
    ///               after those of every list before it; empty lists for levels whose path node is its parent's
    ///               last
    record Path(List<Leaf> leaves, List<List<Leaf>> rest) {
    }

    /// {@return the leaves of the tree under `root` that the nodes on the path to `from` name, and the subtrees after
    /// them}
    ///
    /// It reads one node a level, through the cache, so the first leaves are known after two or three reads: reading
    /// every branch first made the leaves of a large tree wait for all of them.
    static Path path(PageFile pages, TreeRoot root, MemorySegment from) {
        var unit = pages.read(root.ref(), PageFile.NODE, true);
        if (TreeNodes.isLeaf(unit)) {
            return new Path(List.of(new Leaf(root.ref(), null)), List.of());
        }
        var rest = new ArrayList<List<Leaf>>();
        while (TreeNodes.level(unit) > 1) {
            var index = TreeNodes.childFor(unit, from);
            rest.add(children(unit, index + 1));
            unit = pages.node(unit, index);
        }
        return new Path(children(unit, TreeNodes.childFor(unit, from)), rest.reversed());
    }

    /// {@return the leaves under `nodes`, nodes of one level of a tree in key order, in key order}
    ///
    /// It reads the nodes a level at a time, with [PageFile#readAll(List, byte, int)] on up to `threads` threads: a
    /// store that just opened finds its file out of the OS cache as often as not, and reading the nodes of a level one
    /// after another made the reader wait for each read in turn.
    static List<Leaf> leavesUnder(PageFile pages, List<Leaf> nodes, int threads) {
        var level = nodes;
        while (true) {
            var units = pages.readAll(level.stream().map(Leaf::ref).toList(), PageFile.NODE, threads);
            var children = new ArrayList<Leaf>();
            for (var unit : units) {
                children.addAll(children(unit, 0));
            }
            if (TreeNodes.level(units[0]) == 1) {
                return children;
            }
            level = children;
        }
    }

    /// {@return the children of branch `unit` from `from` on}
    private static List<Leaf> children(byte[] unit, int from) {
        var children = new ArrayList<Leaf>(Math.max(0, TreeNodes.count(unit) - from));
        for (var i = from; i < TreeNodes.count(unit); i++) {
            children.add(new Leaf(TreeNodes.childRef(unit, i), TreeNodes.childKey(unit, i)));
        }
        return children;
    }

    /// {@return the read-only value of leaf entry `entry` of `unit`, read from its blob if it has one}
    static MemorySegment value(PageFile pages, byte[] unit, int entry) {
        return value(pages, unit, entry, true);
    }

    /// {@return the read-only value of leaf entry `entry` of `unit`, read from its blob if it has one}
    ///
    /// @param cached whether a blob read from the file goes to the cache
    static MemorySegment value(PageFile pages, byte[] unit, int entry, boolean cached) {
        return value(pages, unit, entry, cached, true);
    }

    /// {@return the read-only value of leaf entry `entry` of `unit`, read from its blob if it has one}
    ///
    /// @param cached whether a blob read from the file goes to the cache
    /// @param referenced whether a blob the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the leaf records a value longer than its blob's content
    static MemorySegment value(PageFile pages, byte[] unit, int entry, boolean cached, boolean referenced) {
        if (TreeNodes.isInline(unit, entry)) {
            return TreeNodes.inlineValue(unit, entry);
        }
        return MemorySegment.ofArray(blob(pages, unit, entry, cached, referenced))
                .asSlice(PageFormat.HEADER, TreeNodes.blobLength(unit, entry))
                .asReadOnly();
    }

    /// {@return the value of leaf entry `entry` of `unit` in a new array, read from its blob if it has one, which goes
    /// to the cache as for [#value(PageFile, byte\[\], int)]}
    ///
    /// It copies from the array that holds the value, so the new array is not zeroed first, as a copy of the segment
    /// [#value(PageFile, byte\[\], int)] returns would have it.
    ///
    /// @throws LedgerCorruptedException if the leaf records a value longer than its blob's content
    static byte[] valueBytes(PageFile pages, byte[] unit, int entry) {
        if (TreeNodes.isInline(unit, entry)) {
            return TreeNodes.inlineValueBytes(unit, entry);
        }
        return Segments.toBytes(blob(pages, unit, entry, true, true), PageFormat.HEADER, TreeNodes.blobLength(unit, entry));
    }

    /// {@return the array of the blob of leaf entry `entry` of `unit`, which has one, with the value after its header}
    ///
    /// @param cached whether a blob read from the file goes to the cache
    /// @param referenced whether a blob the cache takes counts as read, rather than as the first it evicts
    /// @throws LedgerCorruptedException if the leaf records a value longer than the blob's content
    private static byte[] blob(PageFile pages, byte[] unit, int entry, boolean cached, boolean referenced) {
        var ref = TreeNodes.blobRef(unit, entry);
        var blob = TreeNodes.isExternal(unit, entry) ? pages.blobs().read(ref, cached, referenced)
                : pages.read(ref, PageFormat.VALUE, cached, referenced);
        var length = TreeNodes.blobLength(unit, entry);
        // The cache may hold the blob's header and content alone, which a longer value would read past
        if (length > Bytes.i32(blob, PageFormat.LENGTH)) {
            throw new LedgerCorruptedException("A leaf records a value of " + length + " bytes in a unit holding "
                    + Bytes.i32(blob, PageFormat.LENGTH) + ", at page " + ref.page(), null);
        }
        return blob;
    }

    /// {@return the entries of the tree under `root` from `from`, included, to `to`, excluded, either `null` for no
    /// bound}
    ///
    /// It reads the nodes on the paths to the two bounds only: every subtree between them counts as its recorded
    /// entries.
    static long count(PageFile pages, TreeRoot root, MemorySegment from, MemorySegment to) {
        if (root == null) {
            return 0;
        }
        return count(pages, root.ref(), root.count(), null, null, from, to);
    }

    private static long count(PageFile pages, PageRef ref, long entries, MemorySegment low, MemorySegment high,
                              MemorySegment from, MemorySegment to) {
        if ((from == null || (low != null && Segments.compare(from, low) <= 0))
                && (to == null || (high != null && Segments.compare(high, to) <= 0))) {
            return entries;
        }
        var unit = pages.read(ref, PageFile.NODE, true);
        if (TreeNodes.isLeaf(unit)) {
            var start = from == null ? 0 : TreeNodes.lowerBound(unit, from);
            var end = to == null ? TreeNodes.count(unit) : TreeNodes.lowerBound(unit, to);
            return Math.max(0, end - start);
        }
        var total = 0L;
        var children = TreeNodes.count(unit);
        for (var i = 0; i < children; i++) {
            var childLow = TreeNodes.childKey(unit, i);
            if (to != null && Segments.compare(childLow, to) >= 0) {
                break;
            }
            var childHigh = i + 1 < children ? TreeNodes.childKey(unit, i + 1) : high;
            if (from != null && childHigh != null && Segments.compare(childHigh, from) <= 0) {
                continue;
            }
            total += count(pages, TreeNodes.childRef(unit, i), TreeNodes.childCount(unit, i), childLow, childHigh, from, to);
        }
        return total;
    }
}
