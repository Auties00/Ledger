package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/// Walks the entries of a key space's tree within a range, in either direction.
///
/// Copy-on-write nodes cannot link to their siblings, so the cursor keeps the path from the root to its leaf and
/// climbs it to reach the next leaf.
///
/// The first [#ADMITTED] leaves and blobs a cursor reads go to the page cache; past them it reads through the cache,
/// using the units the cache holds and adding the others only while the cache has room for them, as entries the cache
/// evicts before any other. A long scan reads each unit once, so caching what it reads would only evict the units
/// other reads use: the PostgreSQL buffer manager holds a large sequential scan to a small ring for the same reason. A
/// cache with room evicts nothing to take them, and keeps a key space small enough to fit for the next scan.
final class TreeCursor {
    /// The leaves and blobs a cursor adds to the page cache, enough for the pages of a chat a reader scrolls through.
    static final int ADMITTED = 64;

    private final PageFile pages;
    // The bounds in arrays, which the nodes compare with their keys word by word
    private final byte[] from;
    private final byte[] to;
    private final boolean descending;
    private byte[][] path;
    private int[] indexes;
    private int depth;
    private byte[] leaf;
    private int index;
    private int entry;
    private boolean started;
    private boolean exhausted;
    // The leaves and blobs read so far, up to ADMITTED
    private int admitted;

    /// Creates a cursor over the entries of the tree under `root` from `from`, included, to `to`, excluded, either
    /// `null` for no bound.
    ///
    /// @param root the root, or `null` for an empty tree
    /// @param from the lowest key, which the cursor keeps and no one may change while it is used
    /// @param to the first key past the range, which the cursor keeps and no one may change while it is used
    TreeCursor(PageFile pages, TreeRoot root, byte[] from, byte[] to, boolean descending) {
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
            unit = child(unit, child);
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
                var node = child(unit, child);
                while (!TreeNodes.isLeaf(node)) {
                    var edge = descending ? TreeNodes.count(node) - 1 : 0;
                    push(node, edge);
                    node = child(node, edge);
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
        if (TreeNodes.isInline(leaf, entry)) {
            return TreeNodes.inlineValue(leaf, entry);
        }
        if (admit()) {
            return TreeSearch.value(pages, leaf, entry, true);
        }
        var bytes = (long) PageFormat.pagesFor(TreeNodes.blobLength(leaf, entry)) * PageFormat.SIZE;
        return TreeSearch.value(pages, leaf, entry, pages.cache().hasRoom(bytes), false);
    }

    /// {@return child `index` of the branch `parent`, read through the cache}
    ///
    /// Branches are always cached: a scan reads one for every few hundred leaves, and every read starts from them.
    private byte[] child(byte[] parent, int index) {
        var ref = TreeNodes.childRef(parent, index);
        if (TreeNodes.level(parent) > 1 || admit()) {
            return pages.read(ref, PageFile.NODE, true);
        }
        return pages.read(ref, PageFile.NODE, pages.cache().hasRoom(PageFormat.SIZE), false);
    }

    /// {@return whether the leaf or blob the cursor reads next goes to the cache}
    private boolean admit() {
        if (admitted == ADMITTED) {
            return false;
        }
        admitted++;
        return true;
    }
}
