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
///    the entries
/// ```
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
    /// The bytes a node's entries and their offsets may take.
    static final int CAPACITY = PageFormat.capacity(1) - 2;
    /// The largest entry, offset included, so that every leaf holds at least two.
    static final int MAX_ENTRY = CAPACITY / 2;
    /// The fill below which a node a checkpoint rewrote merges with a neighbour.
    static final int MIN_FILL = CAPACITY / 4;
    private static final int BRANCH_FIXED = 4 + 8 + 4 + 8 + 1 + 2;
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
        return MemorySegment.ofArray(unit).asSlice(field + 3, Bytes.u16(unit, field + 1)).asReadOnly();
    }

    /// {@return the inline value of leaf entry `entry`, in an array of its own}
    static byte[] inlineBytes(byte[] unit, int entry) {
        var start = valueField(unit, entry) + 3;
        return Arrays.copyOfRange(unit, start, start + Bytes.u16(unit, start - 2));
    }

    static int blobLength(byte[] unit, int entry) {
        return Bytes.i32(unit, valueField(unit, entry) + 1);
    }

    static PageRef blobRef(byte[] unit, int entry) {
        return PageRef.read(unit, valueField(unit, entry) + 5);
    }

    /// {@return the index of the child of the branch `unit` whose subtree would hold `key`}
    static int childFor(byte[] unit, MemorySegment key) {
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
        var slot = (unit[entry + 24] & IS_DELTA) == 0 ? -1
                : Bytes.u16(unit, entry + BRANCH_FIXED + Bytes.u16(unit, entry + BRANCH_FIXED - 2));
        return new PageRef(Bytes.i32(unit, entry), 1, Bytes.i64(unit, entry + 4), Bytes.i32(unit, entry + 12), slot);
    }

    static long childCount(byte[] unit, int index) {
        return Bytes.i64(unit, entry(unit, index) + 16);
    }

    static MemorySegment childKey(byte[] unit, int index) {
        var entry = entry(unit, index);
        return MemorySegment.ofArray(unit).asSlice(entry + BRANCH_FIXED, Bytes.u16(unit, entry + BRANCH_FIXED - 2)).asReadOnly();
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

    /// {@return the leaf the leaf `unit` named by `ref` is made from: `ref` itself, or the base leaf of a delta}
    static PageRef base(byte[] unit, PageRef ref) {
        if (unit[ORIGIN] != FROM_DELTA) {
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

    /// Writes `entries` from `from` to `to` into `unit` as a leaf's content.
    ///
    /// @return the bytes of content
    static int writeLeaf(byte[] unit, List<LeafEntry> entries, int from, int to) {
        var count = to - from;
        Bytes.u16(unit, PageFormat.HEADER, count);
        var position = PageFormat.HEADER + 2 + 2 * count;
        for (var i = 0; i < count; i++) {
            Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * i, position);
            position = writeEntry(unit, position, entries.get(from + i));
        }
        return position - PageFormat.HEADER;
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
            MemorySegment.copy(entry.value(), 0, target, position + 3, entry.length());
            return position + 3 + entry.length();
        }
        unit[position] = entry.external() ? EXTERNAL : BLOB;
        Bytes.i32(unit, position + 1, entry.length());
        entry.blob().write(unit, position + 5);
        return position + 5 + PageRef.BYTES;
    }

    /// Writes `children` from `from` to `to` into `unit` as a branch's content.
    ///
    /// @return the bytes of content
    static int writeBranch(byte[] unit, List<Child> children, int from, int to) {
        var count = to - from;
        Bytes.u16(unit, PageFormat.HEADER, count);
        var position = PageFormat.HEADER + 2 + 2 * count;
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
        return position - PageFormat.HEADER;
    }

    /// {@return the runs, as pairs of first and last index, that split items of `sizes` evenly into nodes of
    /// [#CAPACITY] bytes}
    static List<int[]> split(int[] sizes, int count) {
        var total = 0L;
        for (var i = 0; i < count; i++) {
            total += sizes[i];
        }
        var remainingPages = Math.max(1, (int) ((total + CAPACITY - 1) / CAPACITY));
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
    /// @param key the key
    /// @param value the inline value, or `null` if it is in a blob or a unit, or the entry is a removal
    /// @param blob the blob or unit of the value, or `null` if it is inline, or the entry is a removal
    /// @param length the bytes of the value, or -1 for a removal
    /// @param external whether `blob` is a unit of [BlobFile] rather than a blob of the data file
    record LeafEntry(MemorySegment key, MemorySegment value, PageRef blob, int length, boolean external) {
        LeafEntry(MemorySegment key, MemorySegment value, PageRef blob, int length) {
            this(key, value, blob, length, false);
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
            return value == null || MemorySegment.mismatch(value, 0, length, other.value, 0, length) == -1;
        }

        /// {@return the bytes the entry takes in a leaf, offset included}
        int size() {
            if (isRemoved()) {
                return 2 + 2 + (int) key.byteSize() + 1;
            }
            return 2 + 2 + (int) key.byteSize() + 1 + (blob == null ? 2 + length : 4 + PageRef.BYTES);
        }

        /// {@return whether a value of `length` bytes under a key of `keyLength` bytes is stored inline}
        static boolean fitsInline(long keyLength, long length) {
            return 2 + 2 + keyLength + 1 + 2 + length <= MAX_ENTRY;
        }
    }

    /// A child of a branch, as a branch records it, and what a checkpoint knows of a node it wrote.
    ///
    /// @param firstKey the lowest key of the subtree
    /// @param ref the node
    /// @param count the entries of the subtree
    /// @param blobs whether a leaf of the subtree holds a value in a blob
    /// @param level the level of the node, 0 for a leaf
    /// @param fill the bytes of the node's content if a checkpoint just wrote it, or -1
    record Child(MemorySegment firstKey, PageRef ref, long count, boolean blobs, int level, int fill) {
        /// {@return the bytes the child takes in a branch, offset included}
        int size() {
            return 2 + BRANCH_FIXED + (int) firstKey.byteSize() + (ref.isRecord() ? 2 : 0);
        }
    }
}
