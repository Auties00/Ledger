package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.HashMap;

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
/// A cursor made to gather reads ahead while the reads of the file are [cold][PageFile#coldReads()]. When it needs a
/// leaf that neither the cache nor its own reads ahead hold, it reads that leaf together with the next siblings under
/// the same parent, then the blobs of the values in its range that those leaves hold, each set of reads at once
/// ([PageFile#prefetch(PageRef\[\], int, byte)]); when it needs a blob of the current leaf that neither holds, it reads
/// the blobs of the rest of the leaf at once. A page of 50 messages read out of the device one unit after another
/// takes about 14 reads, at about 145 microseconds each on the Windows runner, which the reads ahead turn into a few
/// rounds of reads made at once. What it reads ahead stays with the cursor until the cursor reaches it, and then goes to the cache as
/// the cursor's own read would have, so the cache holds what it would have held without the reads ahead, and what the
/// cursor never reaches goes with it. The reads ahead run on the calling thread and on threads it waits for, so none
/// outlives the call that made it, and a unit they cannot read is read again, by the cursor, when it reaches it.
final class TreeCursor {
    /// The leaves and blobs a cursor adds to the page cache, enough for the pages of a chat a reader scrolls through.
    static final int ADMITTED = 64;
    /// The leaves the first gather of a cursor reads, the one it needs among them; each later one reads twice as many
    /// as the one before, up to [#GATHER_LEAVES]: a page of a chat takes about six leaves, and a short scan should not
    /// read many it never reaches.
    static final int FIRST_GATHER = 3;
    /// The most leaves a gather reads.
    static final int GATHER_LEAVES = 8;
    /// The most blobs a gather reads.
    static final int GATHER_VALUES = 64;
    /// The most pages of a blob a gather reads: a longer read keeps the device busy on its own.
    static final int GATHER_VALUE_PAGES = 16;
    /// The most pages of blobs a gather reads, which bounds what a cursor holds read ahead to a megabyte.
    static final int GATHER_PAGES = 256;

    private final PageFile pages;
    // The bounds in arrays, which the nodes compare with their keys word by word
    private final byte[] from;
    private final byte[] to;
    private final boolean descending;
    // The entries the scan is expected to take, or 0 for a cursor that never reads ahead
    private final long wanted;
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
    // The entries the cursor has moved to
    private long stepped;
    // The leaves the next gather reads
    private int window;
    // The leaves and blobs read ahead that the cursor has not used yet, by reference, or null before the first gather
    private HashMap<PageRef, byte[]> gathered;
    // Whether the blobs of the current leaf were gathered, which happens once a leaf
    private boolean leafGathered;
    // The entries a gather still reads values for, and the pages of blobs it still reads, as it collects them
    private long budget;
    private int pagesLeft;

    /// Creates a cursor over the entries of the tree under `root` from `from`, included, to `to`, excluded, either
    /// `null` for no bound, that never reads ahead.
    ///
    /// @param root the root, or `null` for an empty tree
    /// @param from the lowest key, which the cursor keeps and no one may change while it is used
    /// @param to the first key past the range, which the cursor keeps and no one may change while it is used
    TreeCursor(PageFile pages, TreeRoot root, byte[] from, byte[] to, boolean descending) {
        this(pages, root, from, to, descending, 0);
    }

    /// Creates a cursor over the entries of the tree under `root` from `from`, included, to `to`, excluded, either
    /// `null` for no bound.
    ///
    /// @param root the root, or `null` for an empty tree
    /// @param from the lowest key, which the cursor keeps and no one may change while it is used
    /// @param to the first key past the range, which the cursor keeps and no one may change while it is used
    /// @param wanted the entries the scan is expected to take, [Long#MAX_VALUE] if it may take them all, which bounds
    ///               the values the cursor reads ahead; or 0 for a cursor that never reads ahead
    TreeCursor(PageFile pages, TreeRoot root, byte[] from, byte[] to, boolean descending, long wanted) {
        this.pages = pages;
        this.from = from;
        this.to = to;
        this.descending = descending;
        this.wanted = wanted;
        this.window = FIRST_GATHER;
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
        index = first(unit);
    }

    /// {@return the index of the first entry of leaf `unit` in the cursor's range and order, which is out of the leaf
    /// if none is}
    private int first(byte[] unit) {
        if (descending) {
            return (to == null ? TreeNodes.count(unit) : TreeNodes.lowerBound(unit, to)) - 1;
        }
        return from == null ? 0 : TreeNodes.lowerBound(unit, from);
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
        if (pastRange(leaf, entry)) {
            exhausted = true;
            return false;
        }
        stepped++;
        return true;
    }

    /// {@return whether entry `entry` of leaf `unit` is past the cursor's range, in its order}
    private boolean pastRange(byte[] unit, int entry) {
        var offset = TreeNodes.leafKeyOffset(entry);
        var length = TreeNodes.leafKeyLength(unit, entry);
        return descending ? from != null && TreeNodes.compare(unit, offset, length, from) < 0
                : to != null && TreeNodes.compare(unit, offset, length, to) >= 0;
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
        var admit = admit();
        if (wanted > 0 && !TreeNodes.isExternal(leaf, entry)) {
            var ref = TreeNodes.blobRef(leaf, entry);
            var blob = pages.cached(ref, PageFormat.VALUE);
            if (blob == null) {
                blob = take(ref);
                if (blob == null && !leafGathered && pages.coldReads()) {
                    gatherValues();
                    blob = take(ref);
                }
                var cached = admit || pages.cache().hasRoom(blobBytes());
                if (blob == null) {
                    blob = pages.load(ref, PageFormat.VALUE, cached, admit);
                } else if (cached) {
                    pages.cache().put(ref, blob, admit);
                }
            }
            return TreeSearch.blobValue(leaf, entry, ref, blob);
        }
        if (admit) {
            return TreeSearch.value(pages, leaf, entry, true);
        }
        return TreeSearch.value(pages, leaf, entry, pages.cache().hasRoom(blobBytes()), false);
    }

    /// {@return the bytes the cache keeps of the blob of the current entry}
    private int blobBytes() {
        var length = TreeNodes.blobLength(leaf, entry);
        return PageFormat.cachedLength(PageFormat.pagesFor(length), length);
    }

    /// {@return child `index` of the branch `parent`, read through the cache}
    ///
    /// Branches are always cached: a scan reads one for every few hundred leaves, and every read starts from them.
    private byte[] child(byte[] parent, int index) {
        if (TreeNodes.level(parent) > 1) {
            return pages.node(parent, index);
        }
        leafGathered = false;
        var admit = admit();
        var hit = pages.cachedNode(parent, index);
        if (hit != null) {
            return hit;
        }
        var ref = TreeNodes.childRef(parent, index);
        var cached = admit || pages.cache().hasRoom(PageFormat.SIZE);
        if (wanted > 0) {
            var unit = take(ref);
            if (unit == null && !ref.isRecord() && pages.coldReads()) {
                gatherLeaves(parent, index, ref);
                unit = take(ref);
            }
            if (unit != null) {
                if (cached) {
                    pages.cache().put(ref, unit, admit);
                }
                return unit;
            }
        }
        return pages.load(ref, PageFile.NODE, cached, admit);
    }

    /// {@return whether the leaf or blob the cursor reads next goes to the cache}
    private boolean admit() {
        if (admitted == ADMITTED) {
            return false;
        }
        admitted++;
        return true;
    }

    /// {@return the unit `ref` names if the cursor read it ahead and has not used it, which it then forgets, or `null`}
    private byte[] take(PageRef ref) {
        return gathered == null || gathered.isEmpty() ? null : gathered.remove(ref);
    }

    /// Reads ahead child `index` of the branch `parent` of the leaves, which `ref` names and neither the cache nor the
    /// cursor holds, the next siblings in the cursor's order and range up to the window, and the blobs of the values in
    /// the range that they hold; what it read before and has not used is forgotten, as the cursor has passed it.
    private void gatherLeaves(byte[] parent, int index, PageRef ref) {
        if (gathered == null) {
            gathered = new HashMap<>();
        } else {
            gathered.clear();
        }
        var window = this.window;
        this.window = Math.min(window * 2, GATHER_LEAVES);
        // The leaves of the gather in the cursor's order, those the cache holds included, and the ones to read
        var leaves = new byte[window][];
        var reads = new PageRef[window];
        var positions = new int[window];
        var read = 0;
        reads[read++] = ref;
        var size = 1;
        var step = descending ? -1 : 1;
        var children = TreeNodes.count(parent);
        for (var i = index + step; size < window && i >= 0 && i < children; i += step, size++) {
            // A child holds keys from its first one to the first one of the next, excluded
            if (descending ? from != null && TreeNodes.compareChildKey(parent, i + 1, from) <= 0
                    : to != null && TreeNodes.compareChildKey(parent, i, to) >= 0) {
                break;
            }
            var sibling = TreeNodes.childRef(parent, i);
            if (sibling.isRecord()) {
                // Read when the cursor reaches it, with its delta page and base through the cache
                continue;
            }
            var cached = pages.cache().peek(sibling);
            if (cached != null) {
                leaves[size] = cached;
            } else {
                positions[read] = size;
                reads[read++] = sibling;
            }
        }
        var units = pages.prefetch(reads, read, PageFile.NODE);
        for (var i = 0; i < read; i++) {
            if (units[i] != null) {
                gathered.put(reads[i], units[i]);
                leaves[positions[i]] = units[i];
            }
        }
        budget = stepped < wanted ? wanted - stepped : Long.MAX_VALUE;
        pagesLeft = GATHER_PAGES;
        var values = new PageRef[GATHER_VALUES];
        var count = 0;
        for (var i = 0; i < size && budget > 0 && count < values.length; i++) {
            var unit = leaves[i];
            if (unit != null && TreeNodes.isLeaf(unit)) {
                count = collectValues(unit, first(unit), values, count);
            }
        }
        prefetchValues(values, count);
    }

    /// Reads ahead the blobs of the values in the range that the current leaf holds from the current entry on, which
    /// neither the cache nor the cursor holds.
    private void gatherValues() {
        leafGathered = true;
        if (gathered == null) {
            gathered = new HashMap<>();
        }
        // The current entry is counted already, and is the one the cursor needs
        budget = stepped <= wanted ? wanted - stepped + 1 : Long.MAX_VALUE;
        pagesLeft = GATHER_PAGES;
        var values = new PageRef[GATHER_VALUES];
        prefetchValues(values, collectValues(leaf, index, values, 0));
    }

    /// Adds to `values`, from `count` on, the blobs of the data file of the entries of leaf `unit` from `index` on, in
    /// the cursor's order and range, that neither the cache nor the cursor holds, while the budgets of entries and pages
    /// last and `values` has room; an entry past the range, or a blob past the pages left, ends the gather.
    ///
    /// @return the blobs `values` holds
    private int collectValues(byte[] unit, int index, PageRef[] values, int count) {
        var entries = TreeNodes.count(unit);
        var step = descending ? -1 : 1;
        for (var i = index; i >= 0 && i < entries && budget > 0 && count < values.length; i += step) {
            var entry = TreeNodes.entry(unit, i);
            if (pastRange(unit, entry)) {
                budget = 0;
                break;
            }
            budget--;
            if (!TreeNodes.isBlob(unit, entry)) {
                continue;
            }
            var ref = TreeNodes.blobRef(unit, entry);
            if (ref.pages() <= GATHER_VALUE_PAGES && !gathered.containsKey(ref) && pages.cache().peek(ref) == null) {
                if (ref.pages() > pagesLeft) {
                    budget = 0;
                    break;
                }
                pagesLeft -= ref.pages();
                values[count++] = ref;
            }
        }
        return count;
    }

    /// Reads the first `count` blobs of `values` at once, and keeps those it could read until the cursor uses them.
    private void prefetchValues(PageRef[] values, int count) {
        if (count == 0) {
            return;
        }
        var units = pages.prefetch(values, count, PageFormat.VALUE);
        for (var i = 0; i < count; i++) {
            if (units[i] != null) {
                gathered.put(values[i], units[i]);
            }
        }
    }
}
