package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/// Walks the entries of a key space's tree within a range, in either direction.
///
/// Copy-on-write nodes cannot link to their siblings, so the cursor keeps the path from the root to its leaf and
/// climbs it to reach the next leaf.
final class TreeCursor {
    private final PageFile pages;
    private final MemorySegment from;
    private final MemorySegment to;
    private final boolean descending;
    private byte[][] path;
    private int[] indexes;
    private int depth;
    private byte[] leaf;
    private int index;
    private int entry;
    private boolean started;
    private boolean exhausted;

    /// Creates a cursor over the entries of the tree under `root` from `from`, included, to `to`, excluded, either
    /// `null` for no bound.
    ///
    /// @param root the root, or `null` for an empty tree
    TreeCursor(PageFile pages, TreeRoot root, MemorySegment from, MemorySegment to, boolean descending) {
        this.pages = pages;
        this.from = from;
        this.to = to;
        this.descending = descending;
        this.path = new byte[8][];
        this.indexes = new int[8];
        if (root == null) {
            exhausted = true;
        } else {
            seek(pages.read(root.ref(), PageFile.NODE, true));
        }
    }

    private void seek(byte[] unit) {
        while (!TreeNodes.isLeaf(unit)) {
            int child;
            if (descending) {
                child = to == null ? TreeNodes.count(unit) - 1 : TreeNodes.lastChildBefore(unit, to);
                if (child < 0) {
                    exhausted = true;
                    return;
                }
            } else {
                child = from == null ? 0 : TreeNodes.childFor(unit, from);
            }
            push(unit, child);
            unit = pages.read(TreeNodes.childRef(unit, child), PageFile.NODE, true);
        }
        leaf = unit;
        if (descending) {
            index = (to == null ? TreeNodes.count(unit) : TreeNodes.lowerBound(unit, to)) - 1;
        } else {
            index = from == null ? 0 : TreeNodes.lowerBound(unit, from);
        }
    }

    private void push(byte[] unit, int child) {
        if (depth == path.length) {
            path = Arrays.copyOf(path, depth * 2);
            indexes = Arrays.copyOf(indexes, depth * 2);
        }
        path[depth] = unit;
        indexes[depth++] = child;
    }

    /// Moves to the next entry in the cursor's order.
    ///
    /// @return whether there is one
    boolean next() {
        if (exhausted) {
            return false;
        }
        if (started) {
            index += descending ? -1 : 1;
        } else {
            started = true;
        }
        while (index < 0 || index >= TreeNodes.count(leaf)) {
            if (!nextLeaf()) {
                exhausted = true;
                return false;
            }
        }
        entry = TreeNodes.entry(leaf, index);
        var offset = TreeNodes.leafKeyOffset(entry);
        var length = TreeNodes.leafKeyLength(leaf, entry);
        if (descending ? from != null && TreeNodes.compare(leaf, offset, length, from) < 0
                : to != null && TreeNodes.compare(leaf, offset, length, to) >= 0) {
            exhausted = true;
            return false;
        }
        return true;
    }

    private boolean nextLeaf() {
        while (depth > 0) {
            var unit = path[depth - 1];
            var child = indexes[depth - 1] + (descending ? -1 : 1);
            if (child >= 0 && child < TreeNodes.count(unit)) {
                indexes[depth - 1] = child;
                var node = pages.read(TreeNodes.childRef(unit, child), PageFile.NODE, true);
                while (!TreeNodes.isLeaf(node)) {
                    var edge = descending ? TreeNodes.count(node) - 1 : 0;
                    push(node, edge);
                    node = pages.read(TreeNodes.childRef(node, edge), PageFile.NODE, true);
                }
                leaf = node;
                index = descending ? TreeNodes.count(node) - 1 : 0;
                return true;
            }
            depth--;
        }
        return false;
    }

    /// {@return the key of the current entry, a read-only view of the leaf}
    MemorySegment key() {
        return TreeNodes.leafKey(leaf, entry);
    }

    /// {@return a negative number, zero or a positive number as the current entry's key sorts before, with or after
    /// `key`}
    int compareKey(MemorySegment key) {
        return TreeNodes.compare(leaf, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(leaf, entry), key);
    }

    /// {@return the value of the current entry, read-only, read from its blob if it has one}
    MemorySegment value() {
        return TreeSearch.value(pages, leaf, entry);
    }
}
