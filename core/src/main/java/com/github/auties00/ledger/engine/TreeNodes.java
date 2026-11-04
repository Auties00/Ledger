package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
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
/// A branch entry is a child: its reference (page and nonce, its pages being 1), the entries of its subtree, whether
/// the subtree has values in blobs or units, and its first key, which is also the separator before it.
///
/// ```text
/// page (4), nonce (12), entries (8), flags (1), key length (2), key
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
    private static final byte HAS_BLOBS = 1;

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
        var mismatch = MemorySegment.mismatch(MemorySegment.ofArray(unit), offset, offset + length, key, 0, key.byteSize());
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == length) {
            return -1;
        }
        if (mismatch == key.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(unit[offset + (int) mismatch], key.get(ValueLayout.JAVA_BYTE, mismatch));
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

    /// {@return the read-only inline value of leaf entry `entry`}
    static MemorySegment inlineValue(byte[] unit, int entry) {
        var field = valueField(unit, entry);
        return MemorySegment.ofArray(unit).asSlice(field + 3, Bytes.u16(unit, field + 1)).asReadOnly();
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

    /// {@return the index of the last child of the branch `unit` whose first key is before `key`, or -1}
    static int lastChildBefore(byte[] unit, MemorySegment key) {
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
        return new PageRef(Bytes.i32(unit, entry), 1, Bytes.i64(unit, entry + 4), Bytes.i32(unit, entry + 12));
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
            var entry = entry(unit, i);
            var key = leafKey(unit, entry);
            if (isBlob(unit, entry) || isExternal(unit, entry)) {
                entries.add(new LeafEntry(key, null, blobRef(unit, entry), blobLength(unit, entry), isExternal(unit, entry)));
            } else {
                var value = inlineValue(unit, entry);
                entries.add(new LeafEntry(key, value, null, (int) value.byteSize()));
            }
        }
        return entries;
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
        var target = MemorySegment.ofArray(unit);
        for (var i = 0; i < count; i++) {
            var entry = entries.get(from + i);
            Bytes.u16(unit, PageFormat.HEADER + 2 + 2 * i, position);
            var keyLength = (int) entry.key().byteSize();
            Bytes.u16(unit, position, keyLength);
            MemorySegment.copy(entry.key(), 0, target, position + 2, keyLength);
            position += 2 + keyLength;
            if (entry.blob() == null) {
                unit[position] = INLINE;
                Bytes.u16(unit, position + 1, entry.length());
                MemorySegment.copy(entry.value(), 0, target, position + 3, entry.length());
                position += 3 + entry.length();
            } else {
                unit[position] = entry.external() ? EXTERNAL : BLOB;
                Bytes.i32(unit, position + 1, entry.length());
                entry.blob().write(unit, position + 5);
                position += 5 + PageRef.BYTES;
            }
        }
        return position - PageFormat.HEADER;
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
            unit[position + 24] = child.blobs() ? HAS_BLOBS : 0;
            var keyLength = (int) child.firstKey().byteSize();
            Bytes.u16(unit, position + 25, keyLength);
            MemorySegment.copy(child.firstKey(), 0, target, position + BRANCH_FIXED, keyLength);
            position += BRANCH_FIXED + keyLength;
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

    /// An entry of a leaf: a key and its value, inline, in a blob, or in a unit of [BlobFile].
    ///
    /// @param key the key
    /// @param value the inline value, or `null` if it is in a blob or a unit
    /// @param blob the blob or unit of the value, or `null` if it is inline
    /// @param length the bytes of the value
    /// @param external whether `blob` is a unit of [BlobFile] rather than a blob of the data file
    record LeafEntry(MemorySegment key, MemorySegment value, PageRef blob, int length, boolean external) {
        LeafEntry(MemorySegment key, MemorySegment value, PageRef blob, int length) {
            this(key, value, blob, length, false);
        }

        /// {@return the bytes the entry takes in a leaf, offset included}
        int size() {
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
            return 2 + BRANCH_FIXED + (int) firstKey.byteSize();
        }
    }
}
