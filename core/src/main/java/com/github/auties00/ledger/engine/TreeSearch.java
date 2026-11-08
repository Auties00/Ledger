package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
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

    /// A key found in a leaf.
    ///
    /// @param unit the leaf
    /// @param entry the offset of the entry in the leaf
    record Hit(byte[] unit, int entry) {
    }

    /// {@return whether the tree under `root` holds `key`, as [#FOUND] or [#ABSENT], looked up through the page cache
    /// alone, or [#UNKNOWN] if the cache does not hold a node on the way}
    static byte lookupCached(PageFile pages, TreeRoot root, MemorySegment key) {
        var unit = pages.cached(root.ref(), PageFile.NODE);
        while (unit != null && !TreeNodes.isLeaf(unit)) {
            unit = pages.cached(TreeNodes.childRef(unit, TreeNodes.childFor(unit, key)), PageFile.NODE);
        }
        if (unit == null) {
            return UNKNOWN;
        }
        return entryOf(unit, key) != -1 ? FOUND : ABSENT;
    }

    /// {@return where the tree under `root` holds `key`, or `null` if it does not}
    static Hit find(PageFile pages, TreeRoot root, MemorySegment key) {
        var unit = pages.read(root.ref(), PageFile.NODE, true);
        while (!TreeNodes.isLeaf(unit)) {
            unit = pages.read(TreeNodes.childRef(unit, TreeNodes.childFor(unit, key)), PageFile.NODE, true);
        }
        var entry = entryOf(unit, key);
        return entry == -1 ? null : new Hit(unit, entry);
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

    /// {@return the leaves of the tree under `root` that may hold keys at or after `from`, in key order}
    ///
    /// It reads the branches a level at a time, with [PageFile#readAll(List, byte, int)] on up to `threads` threads,
    /// and none of the leaves: a store that just opened finds its file out of the OS cache as often as not, and
    /// reading the nodes of a level one after another made the reader wait for each read in turn.
    static List<Leaf> leaves(PageFile pages, TreeRoot root, MemorySegment from, int threads) {
        var top = pages.read(root.ref(), PageFile.NODE, true);
        if (TreeNodes.isLeaf(top)) {
            return List.of(new Leaf(root.ref(), null));
        }
        var level = List.of(top);
        while (true) {
            var children = new ArrayList<Leaf>();
            for (var node = 0; node < level.size(); node++) {
                var unit = level.get(node);
                // Only the first node of a level is on the path to `from`: those after it hold later keys alone
                for (var i = node == 0 ? TreeNodes.childFor(unit, from) : 0; i < TreeNodes.count(unit); i++) {
                    children.add(new Leaf(TreeNodes.childRef(unit, i), TreeNodes.childKey(unit, i)));
                }
            }
            if (TreeNodes.level(level.getFirst()) == 1) {
                return children;
            }
            level = Arrays.asList(pages.readAll(children.stream().map(Leaf::ref).toList(), PageFile.NODE, threads));
        }
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
    static MemorySegment value(PageFile pages, byte[] unit, int entry, boolean cached, boolean referenced) {
        if (TreeNodes.isInline(unit, entry)) {
            return TreeNodes.inlineValue(unit, entry);
        }
        if (TreeNodes.isExternal(unit, entry)) {
            var external = pages.blobs().read(TreeNodes.blobRef(unit, entry), cached, referenced);
            return MemorySegment.ofArray(external).asSlice(PageFormat.HEADER, TreeNodes.blobLength(unit, entry)).asReadOnly();
        }
        var blob = pages.read(TreeNodes.blobRef(unit, entry), PageFormat.VALUE, cached, referenced);
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
