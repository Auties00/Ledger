package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// Point lookups and range counts in a key space's tree.
final class TreeSearch {
    private TreeSearch() {
    }

    /// A key found in a leaf.
    ///
    /// @param unit the leaf
    /// @param entry the offset of the entry in the leaf
    record Hit(byte[] unit, int entry) {
    }

    /// {@return where the tree under `root` holds `key`, or `null` if it does not}
    static Hit find(PageFile pages, TreeRoot root, MemorySegment key) {
        var unit = pages.read(root.ref(), PageFile.NODE, true);
        while (!TreeNodes.isLeaf(unit)) {
            unit = pages.read(TreeNodes.childRef(unit, TreeNodes.childFor(unit, key)), PageFile.NODE, true);
        }
        var index = TreeNodes.lowerBound(unit, key);
        if (index == TreeNodes.count(unit)) {
            return null;
        }
        var entry = TreeNodes.entry(unit, index);
        return TreeNodes.compare(unit, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(unit, entry), key) == 0
                ? new Hit(unit, entry)
                : null;
    }

    /// {@return the read-only value of leaf entry `entry` of `unit`, read from its blob if it has one}
    static MemorySegment value(PageFile pages, byte[] unit, int entry) {
        if (!TreeNodes.isBlob(unit, entry)) {
            return TreeNodes.inlineValue(unit, entry);
        }
        var blob = pages.read(TreeNodes.blobRef(unit, entry), PageFormat.VALUE, true);
        return MemorySegment.ofArray(blob).asSlice(PageFormat.HEADER, TreeNodes.blobLength(unit, entry)).asReadOnly();
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
