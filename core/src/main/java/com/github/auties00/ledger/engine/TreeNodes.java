package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/// The nodes of a key space's tree in the data file: copy-on-write B+tree nodes of one page each.
///
/// Content of a node, after the [PageFormat] header:
///
/// ```text
/// 0  entries, 2 bytes
/// 2  offset of each entry in the unit, 2 bytes each, in key order
///    the search index of the keys, in a node of format PageFormat.INDEXED (NodeIndex)
///    the entries
/// ```
///
/// A checkpoint writes every node with a search index, which its searches use; a node of the first format, which an
/// older version wrote, is searched through the offsets of its entries.
///
/// A leaf entry is a key and its value, inline, in a blob of its own, or in a unit of [BlobFile] a commit wrote:
///
/// ```text
/// key length (2), key, 0, value length (2), value
/// key length (2), key, 1, value length (4), reference of the value blob (20)
/// key length (2), key, 2, value length (4), reference of the value's unit in ledger.blobs (20)
/// ```
///
/// A branch entry is a child: its reference (page and nonce, its pages being 1), the entries of its subtree, flags,
/// and its first key, which is also the separator before it. The flags tell whether the subtree has values in blobs
/// or units, and whether the child is a leaf made of a base leaf and a delta record ([DeltaLeaves]), whose slot in the
/// delta page the reference names follows the key.
///
/// ```text
/// page (4), nonce (12), entries (8), flags (1), key length (2), key
/// page (4), nonce (12), entries (8), flags (1), key length (2), key, slot of the delta record (2)
/// ```
///
/// Every child's first key is the lowest key of its subtree, so a key below the first key of a branch's first child
/// still belongs to it.
final class TreeNodes {
    /// The bytes a node's entries may take, their offsets and heads included, as [LeafEntry#size()] and
    /// [Child#size()] count them: the content of a page but the count of the entries and the longest prefix of a search
    /// index ([NodeIndex]).
    static final int CAPACITY = PageFormat.capacity(1) - 2 - NodeIndex.MAX_PREFIX;
    /// The bytes of an entry of a node besides the entry itself: its offset and the head of its key.
    static final int SLOT = 2 + NodeIndex.HEAD;
    /// The largest entry, offset and head included, so that every leaf holds at least two.
    static final int MAX_ENTRY = CAPACITY / 2;
    /// The fill below which a node a checkpoint rewrote merges with a neighbour.
    static final int MIN_FILL = CAPACITY / 4;
    /// The fill to split leaves to that nothing is expected to grow, in sixteenths of [#CAPACITY].
    static final int TARGET_FULL = 16;
    /// The fill to split leaves to whose entries may grow a little, in sixteenths of [#CAPACITY].
    static final int TARGET_NEAR_FULL = 15;
    /// The fill to split leaves to that take new keys among their old ones, in sixteenths of [#CAPACITY].
    static final int TARGET_ROOMY = 14;
    private static final int BRANCH_FIXED = 4 + 8 + 4 + 8 + 1 + 2;
    /// The bytes into a leaf entry where its key starts.
    private static final int LEAF_KEY = 2;
    private static final byte INLINE = 0;
    private static final byte BLOB = 1;
    private static final byte EXTERNAL = 2;
    /// The form of an entry of a delta record that removes the key from the base leaf.
    private static final byte REMOVED = 3;
    private static final byte HAS_BLOBS = 1;
    private static final byte IS_DELTA = 2;
    /// The header byte that marks a leaf made of a base leaf and a delta record, whose header names the base.
    private static final int ORIGIN = 3;
    private static final byte FROM_DELTA = 1;

    private TreeNodes() {
    }

    static int count(byte[] unit) {
        return Bytes.u16(unit, PageFormat.HEADER);
    }

    static int entry(byte[] unit, int index) {
        return Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * index);
    }

    static int level(byte[] unit) {
        return unit[PageFormat.LEVEL];
    }

    static boolean isLeaf(byte[] unit) {
        return unit[PageFormat.KIND] == PageFormat.LEAF;
    }

    /// {@return a negative number, zero or a positive number as the `length` bytes at `offset` of `unit` sort before,
    /// with or after `key`}
    static int compare(byte[] unit, int offset, int length, MemorySegment key) {
        return Segments.compare(unit, offset, length, key);
    }

    /// {@return a negative number, zero or a positive number as the `length` bytes at `offset` of `unit` sort before,
    /// with or after `key`}
    static int compare(byte[] unit, int offset, int length, byte[] key) {
        return Segments.compare(unit, offset, length, key, 0, key.length);
    }

    static int leafKeyLength(byte[] unit, int entry) {
        return Bytes.u16(unit, entry);
    }

    static int leafKeyOffset(int entry) {
        return entry + 2;
    }

    /// {@return the index of the first entry of the leaf `unit` whose key is at or after `key`, or its count}
    static int lowerBound(byte[] unit, MemorySegment key) {
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            return NodeIndex.search(unit, key, LEAF_KEY, false);
        }
        var low = 0;
        var high = count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = entry(unit, middle);
            if (compare(unit, leafKeyOffset(entry), leafKeyLength(unit, entry), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    /// {@return the index of the first entry of the leaf `unit` whose key is at or after `key`, or its count}
    static int lowerBound(byte[] unit, byte[] key) {
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            return NodeIndex.search(unit, key, LEAF_KEY, false);
        }
        var low = 0;
        var high = count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = entry(unit, middle);
            if (compare(unit, leafKeyOffset(entry), leafKeyLength(unit, entry), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    /// {@return the read-only key of leaf entry `entry`}
    static MemorySegment leafKey(byte[] unit, int entry) {
        return MemorySegment.ofArray(unit).asSlice(leafKeyOffset(entry), leafKeyLength(unit, entry)).asReadOnly();
    }

    private static int valueField(byte[] unit, int entry) {
        return leafKeyOffset(entry) + leafKeyLength(unit, entry);
    }

    /// {@return whether the value of leaf entry `entry` is in a blob of the data file}
    static boolean isBlob(byte[] unit, int entry) {
        return unit[valueField(unit, entry)] == BLOB;
    }

    /// {@return whether the value of leaf entry `entry` is in a unit of [BlobFile]}
    static boolean isExternal(byte[] unit, int entry) {
        return unit[valueField(unit, entry)] == EXTERNAL;
    }

    /// {@return whether the value of leaf entry `entry` is in the leaf}
    static boolean isInline(byte[] unit, int entry) {
        return unit[valueField(unit, entry)] == INLINE;
    }

    /// {@return whether entry `entry` of a delta record removes its key}
    static boolean isRemoved(byte[] unit, int entry) {
        return unit[valueField(unit, entry)] == REMOVED;
    }

    /// {@return the bytes entry `entry` takes, without its offset}
    static int entryLength(byte[] unit, int entry) {
        var field = valueField(unit, entry);
        return field - entry + switch (unit[field]) {
            case INLINE -> 3 + Bytes.u16(unit, field + 1);
            case REMOVED -> 1;
            default -> 5 + PageRef.BYTES;
        };
    }

    /// {@return entry `entry` of `unit`, a view of it}
    static LeafEntry entryAt(byte[] unit, int entry) {
        var key = leafKey(unit, entry);
        if (isRemoved(unit, entry)) {
            return LeafEntry.removed(key);
        }
        if (isBlob(unit, entry) || isExternal(unit, entry)) {
            return new LeafEntry(key, null, blobRef(unit, entry), blobLength(unit, entry), isExternal(unit, entry));
        }
        var value = inlineValue(unit, entry);
        return new LeafEntry(key, value, null, (int) value.byteSize());
    }

    /// {@return the read-only inline value of leaf entry `entry`}
    static MemorySegment inlineValue(byte[] unit, int entry) {
        var field = valueField(unit, entry);
        return MemorySegment.ofArray(unit).asSlice(inlineValueOffset(field), inlineValueLength(unit, field)).asReadOnly();
    }

    /// {@return the inline value of leaf entry `entry`, in a new array}
    static byte[] inlineValueBytes(byte[] unit, int entry) {
        var field = valueField(unit, entry);
        return Segments.toBytes(unit, inlineValueOffset(field), inlineValueLength(unit, field));
    }

    // Where the inline value of the entry whose value field is at `field` starts
    private static int inlineValueOffset(int field) {
        return field + 3;
    }

    // The length of the inline value of the entry whose value field is at `field`
    private static int inlineValueLength(byte[] unit, int field) {
        return Bytes.u16(unit, field + 1);
    }

    static int blobLength(byte[] unit, int entry) {
        return Bytes.i32(unit, valueField(unit, entry) + 1);
    }

    static PageRef blobRef(byte[] unit, int entry) {
        return PageRef.read(unit, valueField(unit, entry) + 5);
    }

    /// {@return the index of the child of the branch `unit` whose subtree would hold `key`}
    static int childFor(byte[] unit, MemorySegment key) {
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            return Math.max(0, NodeIndex.search(unit, key, BRANCH_FIXED, true) - 1);
        }
        // The last child whose first key is at or before the key, or the first child
        var low = 1;
        var high = count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = entry(unit, middle);
            if (compare(unit, entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2), key) <= 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low - 1;
    }

    /// {@return the index of the child of the branch `unit` whose subtree would hold `key`}
    static int childFor(byte[] unit, byte[] key) {
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            return Math.max(0, NodeIndex.search(unit, key, BRANCH_FIXED, true) - 1);
        }
        // The last child whose first key is at or before the key, or the first child
        var low = 1;
        var high = count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = entry(unit, middle);
            if (compare(unit, entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2), key) <= 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low - 1;
    }

    /// {@return the index of the last child of the branch `unit` whose first key is before `key`, or -1}
    static int lastChildBefore(byte[] unit, byte[] key) {
        if (unit[PageFormat.FORMAT] == PageFormat.INDEXED) {
            return NodeIndex.search(unit, key, BRANCH_FIXED, false) - 1;
        }
        var low = 0;
        var high = count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = entry(unit, middle);
            if (compare(unit, entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low - 1;
    }

    static PageRef childRef(byte[] unit, int index) {
        var entry = entry(unit, index);
        return new PageRef(childPage(unit, entry), 1, childNonceHigh(unit, entry), childNonceLow(unit, entry), childSlot(unit, entry));
    }

    /// {@return the first page of the child at branch entry `entry`}
    static int childPage(byte[] unit, int entry) {
        return Bytes.i32(unit, entry);
    }

    /// {@return the first 8 bytes of the nonce of the child at branch entry `entry`}
    static long childNonceHigh(byte[] unit, int entry) {
        return Bytes.i64(unit, entry + 4);
    }

    /// {@return the last 4 bytes of the nonce of the child at branch entry `entry`}
    static int childNonceLow(byte[] unit, int entry) {
        return Bytes.i32(unit, entry + 12);
    }

    /// {@return the delta record of the child at branch entry `entry`, or -1 if the child is a unit of its own}
    static int childSlot(byte[] unit, int entry) {
        return (unit[entry + 24] & IS_DELTA) == 0 ? -1
                : Bytes.u16(unit, entry + BRANCH_FIXED + Bytes.u16(unit, entry + BRANCH_FIXED - 2));
    }

    static long childCount(byte[] unit, int index) {
        return Bytes.i64(unit, entry(unit, index) + 16);
    }

    static MemorySegment childKey(byte[] unit, int index) {
        var entry = entry(unit, index);
        return MemorySegment.ofArray(unit).asSlice(entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2)).asReadOnly();
    }

    /// {@return a negative number, zero or a positive number as the first key of child `index` of the branch `unit`
    /// sorts before, with or after `key`}
    static int compareChildKey(byte[] unit, int index, byte[] key) {
        var entry = entry(unit, index);
        return compare(unit, entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2), key);
    }

    /// {@return the entries of the leaf `unit`, as views of it}
    static List<LeafEntry> leafEntries(byte[] unit) {
        var count = count(unit);
        var entries = new ArrayList<LeafEntry>(count + 8);
        for (var i = 0; i < count; i++) {
            entries.add(entryAt(unit, entry(unit, i)));
        }
        return entries;
    }

    /// {@return whether `unit` is the leaf of a delta record, whose header names its base leaf rather than itself}
    static boolean madeFromDelta(byte[] unit) {
        return unit[ORIGIN] == FROM_DELTA;
    }

    /// {@return the leaf the leaf `unit` named by `ref` is made from: `ref` itself, or the base leaf of a delta}
    static PageRef base(byte[] unit, PageRef ref) {
        if (!madeFromDelta(unit)) {
            return ref;
        }
        return new PageRef(Bytes.i32(unit, PageFormat.PAGE), 1, Bytes.i64(unit, PageFormat.NONCE),
                Bytes.i32(unit, PageFormat.NONCE + 8));
    }

    /// Fills the header of `unit`, a leaf made of the base leaf `base` and a delta record, with `length` bytes of
    /// content; [#base(byte\[\], PageRef)] reads `base` back from it.
    static void deltaHeader(byte[] unit, PageRef base, int length) {
        PageFile.header(unit, PageFormat.LEAF, 0, base.page(), 1, length, base.nonceHigh(), base.nonceLow());
        unit[ORIGIN] = FROM_DELTA;
    }

    /// {@return the children of the branch `unit`, as views of it}
    static List<Child> children(byte[] unit) {
        var count = count(unit);
        var children = new ArrayList<Child>(count + 8);
        var level = level(unit) - 1;
        for (var i = 0; i < count; i++) {
            var entry = entry(unit, i);
            children.add(new Child(childKey(unit, i), childRef(unit, i), Bytes.i64(unit, entry + 16),
                    (unit[entry + 24] & HAS_BLOBS) != 0, level, -1));
        }
        return children;
    }

    /// {@return the bytes the entries or children whose sizes, as [LeafEntry#size()] and [Child#size()] give them, are
    /// those of `sizes` from `from` to `to` take in a node, as [#CAPACITY] bounds them}
    static int fill(int[] sizes, int from, int to) {
        var fill = 0;
        for (var i = from; i < to; i++) {
            fill += sizes[i];
        }
        return fill;
    }

    /// {@return the bytes the entries or children of the node `unit` take, as [#fill(int\[\], int, int)] counts them
    /// for a node it plans, whatever the format of `unit`}
    static int fill(byte[] unit) {
        var count = count(unit);
        var fill = SLOT * count;
        var leaf = isLeaf(unit);
        for (var i = 0; i < count; i++) {
            var entry = entry(unit, i);
            fill += leaf ? entryLength(unit, entry) : childLength(unit, entry);
        }
        return fill;
    }

    /// {@return the bytes the child at branch entry `entry` takes, without its offset and head}
    private static int childLength(byte[] unit, int entry) {
        return BRANCH_FIXED + Bytes.u16(unit, entry + BRANCH_FIXED - 2) + ((unit[entry + 24] & IS_DELTA) != 0 ? 2 : 0);
    }

    /// {@return a leaf of `entries` from `from` to `to`, with a search index, its content filled in and its header but
    /// for its format and index byte left to fill, in an array of its header and content}
    static byte[] writeLeaf(List<LeafEntry> entries, int from, int to) {
        return writeLeaf(entries, from, to, true);
    }

    /// {@return a leaf of `entries` from `from` to `to`, its content filled in and its header but for its format and
    /// index byte left to fill, in an array of its header and content}
    ///
    /// A leaf of entries that [#fill(int\[\], int, int)] counts at most [#CAPACITY] bytes for fits in a page.
    ///
    /// @param indexed whether the leaf holds a search index; one without is of the first format, and takes the bytes
    ///                its entries and their offsets take
    static byte[] writeLeaf(List<LeafEntry> entries, int from, int to, boolean indexed) {
        var count = to - from;
        var bytes = 0;
        for (var i = from; i < to; i++) {
            bytes += entries.get(i).bytes();
        }
        var position = PageFormat.HEADER + 2 + 2 * count;
        if (!indexed) {
            var unit = new byte[position + bytes];
            Bytes.u16(unit, PageFormat.HEADER, count);
            return writeEntries(unit, position, entries, from, count);
        }
        var first = count == 0 ? MemorySegment.NULL : entries.get(from).key();
        var prefix = count == 0 ? 0 : NodeIndex.prefix(first, entries.get(to - 1).key());
        var heads = new long[count];
        for (var i = 0; i < count; i++) {
            heads[i] = NodeIndex.head(entries.get(from + i).key(), prefix);
        }
        var wide = NodeIndex.wide(heads, position - PageFormat.HEADER + NodeIndex.length(count, prefix, true) + (long) bytes);
        var unit = new byte[position + NodeIndex.length(count, prefix, wide) + bytes];
        Bytes.u16(unit, PageFormat.HEADER, count);
        position = NodeIndex.write(unit, position, first, prefix, heads, wide);
        return writeEntries(unit, position, entries, from, count);
    }

    /// {@return `unit`, a leaf whose `count` entries from `from` of `entries` it writes from `position` on, and their
    /// offsets}
    private static byte[] writeEntries(byte[] unit, int position, List<LeafEntry> entries, int from, int count) {
        for (var i = 0; i < count; i++) {
            Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * i, position);
            position = writeEntry(unit, position, entries.get(from + i));
        }
        filled(unit, position);
        return unit;
    }

    /// Checks that a node written up to `position` fills `unit`, which the sizes of its entries or children gave.
    private static void filled(byte[] unit, int position) {
        if (position != unit.length) {
            throw new IllegalStateException("A node of " + (unit.length - PageFormat.HEADER) + " bytes of content took "
                    + (position - PageFormat.HEADER));
        }
    }

    /// Writes `entry` at `position` of `unit`, in the form a leaf or a delta record holds it.
    ///
    /// @return the position past it
    static int writeEntry(byte[] unit, int position, LeafEntry entry) {
        var target = MemorySegment.ofArray(unit);
        var keyLength = (int) entry.key().byteSize();
        Bytes.u16(unit, position, keyLength);
        MemorySegment.copy(entry.key(), 0, target, position + 2, keyLength);
        position += 2 + keyLength;
        if (entry.isRemoved()) {
            unit[position] = REMOVED;
            return position + 1;
        }
        if (entry.blob() == null) {
            unit[position] = INLINE;
            Bytes.u16(unit, position + 1, entry.length());
            if (entry.held() instanceof Version logged) {
                logged.readLogged(unit, position + 3);
            } else if (entry.held() instanceof byte[] array) {
                System.arraycopy(array, 0, unit, position + 3, entry.length());
            } else {
                MemorySegment.copy(entry.value(), 0, target, position + 3, entry.length());
            }
            return position + 3 + entry.length();
        }
        unit[position] = entry.external() ? EXTERNAL : BLOB;
        Bytes.i32(unit, position + 1, entry.length());
        entry.blob().write(unit, position + 5);
        return position + 5 + PageRef.BYTES;
    }

    /// {@return a branch of `children` from `from` to `to`, with a search index, its content filled in and its header
    /// but for its format and index byte left to fill, in an array of its header and content}
    ///
    /// A branch of children that [#fill(int\[\], int, int)] counts at most [#CAPACITY] bytes for fits in a page.
    static byte[] writeBranch(List<Child> children, int from, int to) {
        var count = to - from;
        var bytes = 0;
        for (var i = from; i < to; i++) {
            bytes += children.get(i).bytes();
        }
        var position = PageFormat.HEADER + 2 + 2 * count;
        var first = count == 0 ? MemorySegment.NULL : children.get(from).firstKey();
        var prefix = count == 0 ? 0 : NodeIndex.prefix(first, children.get(to - 1).firstKey());
        var heads = new long[count];
        for (var i = 0; i < count; i++) {
            heads[i] = NodeIndex.head(children.get(from + i).firstKey(), prefix);
        }
        var wide = NodeIndex.wide(heads, position - PageFormat.HEADER + NodeIndex.length(count, prefix, true) + (long) bytes);
        var unit = new byte[position + NodeIndex.length(count, prefix, wide) + bytes];
        Bytes.u16(unit, PageFormat.HEADER, count);
        position = NodeIndex.write(unit, position, first, prefix, heads, wide);
        var target = MemorySegment.ofArray(unit);
        for (var i = 0; i < count; i++) {
            var child = children.get(from + i);
            Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * i, position);
            Bytes.i32(unit, position, child.ref().page());
            Bytes.i64(unit, position + 4, child.ref().nonceHigh());
            Bytes.i32(unit, position + 12, child.ref().nonceLow());
            Bytes.i64(unit, position + 16, child.count());
            unit[position + 24] = (byte) ((child.blobs() ? HAS_BLOBS : 0) | (child.ref().isRecord() ? IS_DELTA : 0));
            var keyLength = (int) child.firstKey().byteSize();
            Bytes.u16(unit, position + 25, keyLength);
            MemorySegment.copy(child.firstKey(), 0, target, position + BRANCH_FIXED, keyLength);
            position += BRANCH_FIXED + keyLength;
            if (child.ref().isRecord()) {
                Bytes.u16(unit, position, child.ref().slot());
                position += 2;
            }
        }
        filled(unit, position);
        return unit;
    }

    /// {@return the runs, as pairs of first and last index, that split items of `sizes` evenly into nodes of
    /// [#CAPACITY] bytes}
    static List<int[]> split(int[] sizes, int count) {
        var total = 0L;
        for (var i = 0; i < count; i++) {
            total += sizes[i];
        }
        return spread(sizes, count, total, (int) ((total + CAPACITY - 1) / CAPACITY));
    }

    /// {@return the runs, as pairs of first and last index, that split items of `sizes` evenly into the nodes
    /// [#nodesFor(int\[\], int, int)] counts}
    ///
    /// @param target the sixteenths of [#CAPACITY] to fill a node to on average
    static List<int[]> split(int[] sizes, int count, int target) {
        var total = 0L;
        for (var i = 0; i < count; i++) {
            total += sizes[i];
        }
        return spread(sizes, count, total, nodesFor(sizes, count, target));
    }

    /// {@return the nodes that hold items of `sizes` at `target` sixteenths of [#CAPACITY] on average, and at least as
    /// many as filling each to its capacity in turn takes, so that an even spread leaves no small node at the end}
    static int nodesFor(int[] sizes, int count, int target) {
        var total = 0L;
        var greedy = 0;
        var fill = 0L;
        for (var i = 0; i < count; i++) {
            total += sizes[i];
            if (i == 0 || fill + sizes[i] > CAPACITY) {
                greedy++;
                fill = 0;
            }
            fill += sizes[i];
        }
        var divisor = (long) target * CAPACITY;
        return Math.max(greedy, Math.toIntExact((TARGET_FULL * total + divisor - 1) / divisor));
    }

    /// {@return the runs that spread items of `sizes`, `total` bytes, evenly over `pages` nodes, past which it goes on
    /// only if entries too large for the room left stop nodes short of their share; no node passes [#CAPACITY]}
    private static List<int[]> spread(int[] sizes, int count, long total, int pages) {
        var remainingPages = Math.max(1, pages);
        var remaining = total;
        var runs = new ArrayList<int[]>(remainingPages);
        var index = 0;
        while (index < count) {
            var goal = (remaining + remainingPages - 1) / remainingPages;
            var start = index;
            var fill = 0L;
            while (index < count && fill + sizes[index] <= CAPACITY && (fill < goal || index == start)) {
                fill += sizes[index++];
            }
            runs.add(new int[]{start, index});
            remaining -= fill;
            remainingPages = Math.max(1, remainingPages - 1);
        }
        return runs;
    }

    /// An entry of a leaf: a key and its value, inline, in a blob, or in a unit of [BlobFile]; in a delta record, also
    /// the removal of a key.
    ///
    /// An inline value is held as a read-only segment; as the array that is the value, for a version that keeps its
    /// value on an array of its own, which [#writeEntry(byte\[\], int, LeafEntry)] copies with an array copy and no view
    /// of it; or, for the value of a version that left the heap for the log ([SpillQueue]), as that version, whose value
    /// [#writeEntry(byte\[\], int, LeafEntry)] reads from the log straight into the unit it writes. A checkpoint then
    /// copies such a value once, into its leaf or delta record, with nothing of its own in between.
    ///
    /// @param key the key
    /// @param held the inline value: a read-only segment, an array no one writes that is the value whole, or a
    ///             [Version] whose value is in the log ([Version#logged()]); or `null` if the value is in a blob or a
    ///             unit, or the entry is a removal
    /// @param blob the blob or unit of the value, or `null` if it is inline, or the entry is a removal
    /// @param length the bytes of the value, or -1 for a removal
    /// @param external whether `blob` is a unit of [BlobFile] rather than a blob of the data file
    record LeafEntry(MemorySegment key, Object held, PageRef blob, int length, boolean external) {
        LeafEntry(MemorySegment key, Object held, PageRef blob, int length) {
            this(key, held, blob, length, false);
        }

        /// {@return the inline value, or `null` if it is in a blob or a unit, or the entry is a removal}
        ///
        /// A value held as an array is viewed anew at each call.
        ///
        /// @throws ClassCastException if the value is in the log ([#logged()]), which no segment holds
        MemorySegment value() {
            return held instanceof byte[] array ? MemorySegment.ofArray(array) : (MemorySegment) held;
        }

        /// {@return the version whose value, in the log, is the entry's inline value, or `null` if the value is not
        /// in the log}
        Version logged() {
            return held instanceof Version version ? version : null;
        }

        /// {@return the removal of `key`, as a delta record holds it}
        static LeafEntry removed(MemorySegment key) {
            return new LeafEntry(key, null, null, -1, false);
        }

        /// {@return whether the entry removes its key}
        boolean isRemoved() {
            return length < 0;
        }

        /// {@return whether the entry and `other` hold the same key and the same value, in the same form}
        boolean sameAs(LeafEntry other) {
            if (length != other.length || external != other.external || Segments.compare(key, other.key) != 0) {
                return false;
            }
            if (blob != null || other.blob != null) {
                return blob != null && blob.equals(other.blob);
            }
            if (held instanceof Version || other.held instanceof Version) {
                // Not read back to compare: a value in the log is that of a write since the last checkpoint, which
                // a delta record then holds even if it wrote the bytes the base holds
                return held == other.held;
            }
            if (held instanceof byte[] array && other.held instanceof byte[] otherArray) {
                return Arrays.equals(array, 0, length, otherArray, 0, length);
            }
            return held == null || MemorySegment.mismatch(value(), 0, length, other.value(), 0, length) == -1;
        }

        /// {@return the bytes the entry takes in a leaf, its offset and the head of its key included}
        int size() {
            return SLOT + bytes();
        }

        /// {@return the bytes the entry takes as a leaf or a delta record holds it, without an offset or a head}
        int bytes() {
            if (isRemoved()) {
                return 2 + (int) key.byteSize() + 1;
            }
            return 2 + (int) key.byteSize() + 1 + (blob == null ? 2 + length : 4 + PageRef.BYTES);
        }

        /// {@return whether a value of `length` bytes under a key of `keyLength` bytes is stored inline}
        static boolean fitsInline(long keyLength, long length) {
            return SLOT + 2 + keyLength + 1 + 2 + length <= MAX_ENTRY;
        }
    }

    /// A child of a branch, as a branch records it, and what a checkpoint knows of a node it wrote.
    ///
    /// @param firstKey the lowest key of the subtree
    /// @param ref the node
    /// @param count the entries of the subtree
    /// @param blobs whether a leaf of the subtree holds a value in a blob
    /// @param level the level of the node, 0 for a leaf
    /// @param fill the bytes the node's entries or children take, as [TreeNodes#fill(int\[\], int, int)] counts them,
    ///             if a checkpoint just wrote it, or -1
    record Child(MemorySegment firstKey, PageRef ref, long count, boolean blobs, int level, int fill) {
        /// {@return the bytes the child takes in a branch, its offset and the head of its key included}
        int size() {
            return SLOT + bytes();
        }

        /// {@return the bytes the child takes in a branch, without its offset and head}
        int bytes() {
            return BRANCH_FIXED + (int) firstKey.byteSize() + (ref.isRecord() ? 2 : 0);
        }
    }
}
