package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
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
/// ([PageFile#prefetch(PageRef\[\], int, int\[\])]); when it needs a blob of the current leaf that neither holds, it
/// reads the blobs of the rest of the leaf at once. A page of 50 messages read out of the device one unit after another
/// takes about 14 reads, at about 145 microseconds each on the Windows runner, which the reads ahead turn into a few
/// rounds of reads made at once. What it reads ahead stays with the cursor until the cursor reaches it, and then goes
/// to the cache as the cursor's own read would have, so the cache holds what it would have held without the reads
/// ahead, and what the cursor never reaches goes with it. The leaves are opened as they arrive, as the blobs to read
/// are found in them; the blobs stay sealed, in arrays of the store's pool, until the cursor opens the one it reaches.
/// The array of a set of blobs read at once goes back to the pool once the cursor has opened all of them; that of the
/// blobs of one leaf also once the cursor leaves the leaf, and that of a gather of leaves at the next one or at
/// [#release()]. A cursor so holds the arrays of two sets of blobs at most, whatever the length of its scan, also when
/// the cache holds its leaves and none of its blobs, as it does after a checkpoint. A scan's reads ahead allocate about
/// what its own reads would have:
/// arrays of their own for each unit read ahead, read into and opened into, would make 1,000 cold chat pages allocate
/// 188 megabytes rather than 103, which the process keeps resident as heap until it collects. The reads ahead run on
/// the calling thread and on threads it waits for, so none outlives the call that made it, and a unit they cannot read
/// is read again, by the cursor, when it reaches it.
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
    /// The most pages of blobs a gather reads, a megabyte; a cursor holds the blobs of two gathers at most, its last
    /// gather of leaves and the gather of its current leaf.
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
    // The leaves read ahead that the cursor has not used yet, opened into an array of `held`, by reference; null
    // before the first gather
    private HashMap<PageRef, Opened> gathered;
    // The blobs read ahead that the cursor has not used yet, sealed in the array of `aheadValues` or `leafValues`, by
    // reference
    private HashMap<PageRef, Sealed> sealedValues;
    // The arrays of the store's pool that the leaves read ahead lie in, until the cursor gives them back
    private ArrayList<byte[]> held;
    // The blobs the last gather of leaves read, or null once none of them is left
    private Values aheadValues;
    // The blobs of the current leaf that its gather read, or null once none of them is left, and the references it
    // read them for
    private Values leafValues;
    private PageRef[] leafRefs;
    private int leafRefCount;
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
                blob = takeValue(ref);
                if (blob == null && !leafGathered && pages.coldReads()) {
                    gatherValues();
                    blob = takeValue(ref);
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
        if (leafValues != null) {
            dropLeafValues();
        }
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

    /// Gives back to the store's pool the arrays the cursor read leaves and blobs ahead into, which it no longer uses:
    /// once its scan is done, as it reads nothing afterwards.
    void release() {
        if (held != null) {
            letGo();
        }
    }

    /// {@return the leaf `ref` names, as the cache keeps it, in an array of its own, if the cursor read it ahead and
    /// has not used it, which it then forgets, or `null`}
    private byte[] take(PageRef ref) {
        if (gathered == null || gathered.isEmpty()) {
            return null;
        }
        var opened = gathered.remove(ref);
        return opened == null ? null : Arrays.copyOf(opened.array(), opened.length());
    }

    /// {@return the plaintext of the blob `ref` names if the cursor read it ahead and has not used it, which it then
    /// forgets, or `null`, also if it fails authentication}
    private byte[] takeValue(PageRef ref) {
        if (sealedValues == null || sealedValues.isEmpty()) {
            return null;
        }
        var sealed = sealedValues.remove(ref);
        if (sealed == null) {
            return null;
        }
        var values = sealed.values();
        var plain = pages.openPrefetched(ref, PageFormat.VALUE, values.array, sealed.offset());
        if (--values.left == 0) {
            // None of its blobs is left to open
            pages.releasePrefetched(values.array);
            if (values == aheadValues) {
                aheadValues = null;
            } else {
                leafValues = null;
                leafRefs = null;
                leafRefCount = 0;
            }
        }
        return plain;
    }

    /// Forgets the blobs of the current leaf that its gather read and the cursor has not used, as the cursor leaves the
    /// leaf and never comes back to it, and gives back the array they lie in.
    private void dropLeafValues() {
        for (var i = 0; i < leafRefCount; i++) {
            var sealed = sealedValues.get(leafRefs[i]);
            if (sealed != null && sealed.values() == leafValues) {
                sealedValues.remove(leafRefs[i]);
            }
        }
        pages.releasePrefetched(leafValues.array);
        leafValues = null;
        leafRefs = null;
        leafRefCount = 0;
    }

    /// {@return the bytes of the arrays of the store's pool that hold what the cursor read ahead; for tests}
    long heldBytesForTests() {
        var bytes = 0L;
        if (held != null) {
            for (var array : held) {
                bytes += array.length;
            }
        }
        if (aheadValues != null) {
            bytes += aheadValues.array.length;
        }
        if (leafValues != null) {
            bytes += leafValues.array.length;
        }
        return bytes;
    }

    /// Forgets what the cursor read ahead and has not used, and gives back the arrays it lies in.
    private void letGo() {
        if (held == null) {
            gathered = new HashMap<>();
            sealedValues = new HashMap<>();
            held = new ArrayList<>();
            return;
        }
        for (var array : held) {
            pages.releasePrefetched(array);
        }
        held.clear();
        gathered.clear();
        sealedValues.clear();
        if (aheadValues != null) {
            pages.releasePrefetched(aheadValues.array);
            aheadValues = null;
        }
        if (leafValues != null) {
            pages.releasePrefetched(leafValues.array);
            leafValues = null;
        }
        leafRefs = null;
        leafRefCount = 0;
    }

    /// Reads ahead child `index` of the branch `parent` of the leaves, which `ref` names and neither the cache nor the
    /// cursor holds, the next siblings in the cursor's order and range up to the window, and the blobs of the values in
    /// the range that they hold; what it read before and has not used is forgotten, as the cursor has passed it.
    private void gatherLeaves(byte[] parent, int index, PageRef ref) {
        letGo();
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
        var offsets = new int[read];
        var sealed = pages.prefetch(reads, read, offsets);
        try {
            // Opened into arrays of the pool, which the blobs to read are found in, and copied out as the cache keeps
            // them only for the leaves the cursor reaches
            for (var i = 0; i < read; i++) {
                if (offsets[i] < 0) {
                    continue;
                }
                var plain = pages.acquireOpened(reads[i]);
                var length = pages.openPrefetched(reads[i], PageFile.NODE, sealed, offsets[i], plain);
                if (length < 0) {
                    pages.releasePrefetched(plain);
                    continue;
                }
                held.add(plain);
                gathered.put(reads[i], new Opened(plain, length));
                leaves[positions[i]] = plain;
            }
        } finally {
            pages.releasePrefetched(sealed);
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
        aheadValues = prefetchValues(values, count);
    }

    /// Reads ahead the blobs of the values in the range that the current leaf holds from the current entry on, which
    /// neither the cache nor the cursor holds.
    private void gatherValues() {
        leafGathered = true;
        if (held == null) {
            letGo();
        } else if (leafValues != null) {
            dropLeafValues();
        }
        // The current entry is counted already, and is the one the cursor needs
        budget = stepped <= wanted ? wanted - stepped + 1 : Long.MAX_VALUE;
        pagesLeft = GATHER_PAGES;
        var values = new PageRef[GATHER_VALUES];
        var count = collectValues(leaf, index, values, 0);
        leafValues = prefetchValues(values, count);
        if (leafValues != null) {
            leafRefs = values;
            leafRefCount = count;
        }
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
            if (ref.pages() <= GATHER_VALUE_PAGES && !sealedValues.containsKey(ref) && pages.cache().peek(ref) == null) {
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

    /// Reads the first `count` blobs of `values` at once, and keeps those it could read, sealed, until the cursor uses
    /// them.
    ///
    /// @return the blobs read, or `null` if it could read none, whose array it then gave back
    private Values prefetchValues(PageRef[] values, int count) {
        if (count == 0) {
            return null;
        }
        var offsets = new int[count];
        var sealed = pages.prefetch(values, count, offsets);
        var read = new Values(sealed);
        for (var i = 0; i < count; i++) {
            if (offsets[i] >= 0) {
                sealedValues.put(values[i], new Sealed(read, offsets[i]));
                read.left++;
            }
        }
        if (read.left == 0) {
            pages.releasePrefetched(sealed);
            return null;
        }
        return read;
    }

    /// Blobs read ahead at once, sealed in `array`, of the store's pool, of which `left` are not used yet.
    private static final class Values {
        final byte[] array;
        int left;

        Values(byte[] array) {
            this.array = array;
        }
    }

    /// A blob read ahead, sealed at `offset` of the array of `values`.
    private record Sealed(Values values, int offset) {
    }

    /// A leaf read ahead, opened into `array`, whose first `length` bytes the cache keeps.
    private record Opened(byte[] array, int length) {
    }
}
