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
///
/// While the caller handles an entry, the processor fetches the memory the next ones need: the cursor touches it ahead
/// of its reads. Entering a leaf it reads the key length and value form of its next [#AHEAD] entries, and at each entry
/// those of the entry [#AHEAD] further, so the misses of their lines overlap instead of each waiting behind the caller's
/// work on the entry before, as entries laid out in ascending order defeat the hardware's prefetch going down. It looks
/// the next entry's blob up in the page cache, and on entering a leaf the leaf it reads after it, the next child of the
/// same parent, whose first entries it touches too. The lookups do not count as reads
/// ([PageCache#peek(long, long, int, int)]) and keep nothing, so the cache's clock, its idle ticker and the admission of
/// the units the cursor reads see only what it reads, as they would without the touches. A touch issues no I/O, changes
/// no state of the cursor and never throws: an entry the cursor never reaches is never reported damaged.
final class TreeCursor {
    /// The leaves and blobs a cursor adds to the page cache, enough for the pages of a chat a reader scrolls through.
    static final int ADMITTED = 64;
    /// How many entries past the current one the cursor touches: a leaf of a chat's messages holds about nine.
    static final int AHEAD = 8;
    /// A value no touch folds to, every touch folding bytes below 2^8; a field the JIT cannot fold, so that comparing
    /// what a touch read with it keeps the touch's loads in compiled code, with nothing waiting for them.
    private static int untouched = 1 << 16;
    /// Written only if a touch folds to [#untouched], which none does.
    private static int matched;

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
        touchLeaf();
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
        touchEntries(leaf, index + (descending ? -AHEAD : AHEAD), 1);
        touchNextBlob();
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
                touchLeaf();
                return true;
            }
            depth--;
        }
        return false;
    }

    /// {@return whether the current entry is the last of its leaf in the cursor's order, so that the next [#next()]
    /// reads another leaf}
    boolean lastOfLeaf() {
        return descending ? index == 0 : index == TreeNodes.count(leaf) - 1;
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
        var length = TreeNodes.blobLength(leaf, entry);
        var bytes = PageFormat.cachedLength(PageFormat.pagesFor(length), length);
        return TreeSearch.value(pages, leaf, entry, pages.cache().hasRoom(bytes), false);
    }

    /// {@return child `index` of the branch `parent`, read through the cache}
    ///
    /// Branches are always cached: a scan reads one for every few hundred leaves, and every read starts from them.
    private byte[] child(byte[] parent, int index) {
        if (TreeNodes.level(parent) > 1 || admit()) {
            return pages.node(parent, index);
        }
        var hit = pages.cachedNode(parent, index);
        if (hit != null) {
            return hit;
        }
        return pages.load(TreeNodes.childRef(parent, index), PageFile.NODE, pages.cache().hasRoom(PageFormat.SIZE), false);
    }

    /// {@return whether the leaf or blob the cursor reads next goes to the cache}
    private boolean admit() {
        if (admitted == ADMITTED) {
            return false;
        }
        admitted++;
        return true;
    }

    /// Touches the leaf the cursor entered from its current index on, and the leaf it reads after it.
    private void touchLeaf() {
        touchEntries(leaf, index, AHEAD);
        touchSibling();
    }

    /// Reads the key length and the value form of up to `entries` entries of the leaf `unit`, from index `from` on in
    /// the cursor's order, those of them it holds.
    ///
    /// Every offset it reads at is checked against the array, so a damaged leaf reads nothing past it.
    private void touchEntries(byte[] unit, int from, int entries) {
        var length = unit.length;
        if (length < PageFormat.HEADER + 2) {
            return;
        }
        var count = TreeNodes.count(unit);
        var step = descending ? -1 : 1;
        var folded = 0;
        for (var i = from; entries > 0 && i >= 0 && i < count; i += step, entries--) {
            var at = PageFormat.HEADER + 2 + 2 * i;
            if (at + 2 > length) {
                break;
            }
            var offset = Bytes.u16(unit, at);
            if (offset + 2 > length) {
                continue;
            }
            var field = offset + 2 + Bytes.u16(unit, offset);
            if (field < length) {
                folded ^= unit[field] & 0xFF;
            }
        }
        sink(folded);
    }

    /// Looks the blob of the entry after the current one in the cursor's order up in the page cache, if its value has
    /// one.
    private void touchNextBlob() {
        var unit = leaf;
        var next = index + (descending ? -1 : 1);
        try {
            if (next < 0 || next >= TreeNodes.count(unit)) {
                return;
            }
            var at = TreeNodes.entry(unit, next);
            if (!TreeNodes.isInline(unit, at)) {
                var blob = pages.peekBlob(unit, at);
                if (blob != null) {
                    sink(blob[PageFormat.LENGTH] & 0xFF);
                }
            }
        } catch (IndexOutOfBoundsException _) {
            // A damaged entry, which a read that reaches it reports
        }
    }

    /// Looks the leaf the cursor reads after its current one up in the page cache, if it is the next child of the same
    /// parent and may hold keys of the range, and touches its first entries.
    private void touchSibling() {
        if (depth == 0) {
            return;
        }
        var parent = path[depth - 1];
        var current = indexes[depth - 1];
        var sibling = current + (descending ? -1 : 1);
        try {
            if (sibling < 0 || sibling >= TreeNodes.count(parent)) {
                return;
            }
            // Going down, every key of the sibling sorts before the current child's first key; going up, none before
            // the sibling's
            if (descending ? from != null && TreeNodes.compareChildKey(parent, current, from) <= 0
                    : to != null && TreeNodes.compareChildKey(parent, sibling, to) >= 0) {
                return;
            }
            var unit = pages.peekNode(parent, sibling);
            if (unit != null && TreeNodes.isLeaf(unit)) {
                touchEntries(unit, descending ? TreeNodes.count(unit) - 1 : 0, AHEAD);
            }
        } catch (IndexOutOfBoundsException _) {
            // A damaged branch, which a read that reaches the child reports
        }
    }

    /// Keeps the loads that produced `folded` in compiled code, through a branch on it that is never taken.
    private static void sink(int folded) {
        if (folded == untouched) {
            matched++;
        }
    }
}
