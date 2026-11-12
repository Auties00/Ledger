package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/// The search index a leaf or a branch of the [second format][PageFormat#INDEXED] holds: the prefix its keys share and
/// the first bytes of each key after it, its head, together between the offsets of its entries and the entries.
///
/// A search of a node without one reads an offset and then a key at every step, and every entry of a leaf holding
/// small values sits on a line of memory of its own: in a tree larger than the processor's cache, each step waits for
/// memory. The heads of the keys, side by side, decide the steps of a search on a few lines, which a search loads
/// together before it starts; a key is read only where the heads tie, and for the entry the search ends on.
///
/// The keys are those of a leaf's entries, or the first keys of a branch's children. Content of a node, after the
/// [PageFormat] header:
///
/// ```text
/// 0  entries, 2 bytes
/// 2  offset of each entry in the unit, 2 bytes each, in key order
///    the prefix the first and the last key share, up to MAX_PREFIX bytes
///    the head of each key, 4 or 8 bytes each, in key order: its bytes after the prefix, then zeros where it ends
///    the entries
/// ```
///
/// The [index byte][PageFormat#INDEX] of the header holds the length of the prefix in its low 4 bits, and in its high
/// bit whether the heads take 8 bytes rather than 4. The keys between the first and the last sort between them, so they
/// all start with the prefix. Heads never decrease in key order, and of two keys with different heads, the one with the
/// lower head sorts first: a padding zero stands only where its key ended, so that key is a prefix of the other or below
/// it at the first byte the heads differ in. A search compares a key with the prefix first, which places a key that
/// does not start with it before or after every key of the node, then compares heads as unsigned numbers, and only keys
/// whose heads tie its own.
///
/// The sizes a checkpoint plans its nodes by count a head of 4 bytes for each entry, and [TreeNodes#CAPACITY] keeps the
/// room for the longest prefix. Heads of 8 bytes take more room than that: a node takes them only if they fit, and only
/// when the keys of 4-byte heads tie in long runs, as those of a leaf holding the end of one chat and the start of the
/// next, which share no more than the top bytes of the chats' ids, and whose 4-byte heads are the id's last byte and the
/// first bytes every key of the chat shares.
final class NodeIndex {
    /// The bytes a prefix takes at most.
    static final int MAX_PREFIX = 15;
    /// The bytes of a head that the size of an entry counts.
    static final int HEAD = Integer.BYTES;
    /// The bytes of a wide head.
    private static final int WIDE_HEAD = Long.BYTES;
    /// The bit of the index byte that marks heads of 8 bytes.
    private static final int WIDE = 0x80;
    /// The bits of the index byte that hold the length of the prefix.
    private static final int PREFIX = 0x0F;
    /// The bits of the index byte no index sets.
    private static final int RESERVED = 0x70;
    /// The keys that may share a 4-byte head before a node takes 8-byte heads, if they fit and tie less.
    private static final int TIES = 2;
    /// The bytes of a line of memory, the stride at which a search touches the index before it starts.
    private static final int LINE = 64;
    /// A value the touched bytes of an index never take: non-final, so the compiler keeps the loads that feed the test.
    private static int untouched = Integer.MIN_VALUE;
    /// Where the touched bytes would go if they took the value of [#untouched]; never written.
    @SuppressWarnings("unused")
    private static int sink;

    private NodeIndex() {
    }

    /// {@return the bytes the keys `first` and `last` share, up to [#MAX_PREFIX]}
    static int prefix(MemorySegment first, MemorySegment last) {
        var firstLength = Math.min(first.byteSize(), MAX_PREFIX);
        var lastLength = Math.min(last.byteSize(), MAX_PREFIX);
        var mismatch = MemorySegment.mismatch(first, 0, firstLength, last, 0, lastLength);
        return (int) (mismatch == -1 ? firstLength : mismatch);
    }

    /// {@return the 8 bytes of `key` from `from`, then zeros where it ends, as a big-endian number}
    static long head(MemorySegment key, int from) {
        var length = key.byteSize() - from;
        if (length >= Long.BYTES) {
            return key.get(Segments.BIG_ENDIAN_LONG, from);
        }
        var head = 0L;
        for (var i = 0; i < Long.BYTES; i++) {
            head = head << 8 | (i < length ? key.get(ValueLayout.JAVA_BYTE, from + i) & 0xFF : 0);
        }
        return head;
    }

    /// {@return the 8 bytes of `key` from `from`, then zeros where it ends, as a big-endian number}
    private static long head(byte[] key, int from) {
        var length = key.length - from;
        if (length >= Long.BYTES) {
            return Bytes.i64(key, from);
        }
        var head = 0L;
        for (var i = 0; i < Long.BYTES; i++) {
            head = head << 8 | (i < length ? key[from + i] & 0xFF : 0);
        }
        return head;
    }

    /// {@return whether a node whose keys have the 8-byte `heads`, in key order, takes heads of 8 bytes}
    ///
    /// @param wideContent the bytes of content the node takes with heads of 8 bytes
    static boolean wide(long[] heads, long wideContent) {
        if (wideContent > PageFormat.capacity(1)) {
            return false;
        }
        var narrowRun = 1;
        var narrowLongest = 1;
        var wideRun = 1;
        var wideLongest = 1;
        for (var i = 1; i < heads.length; i++) {
            narrowRun = heads[i] >>> 32 == heads[i - 1] >>> 32 ? narrowRun + 1 : 1;
            narrowLongest = Math.max(narrowLongest, narrowRun);
            wideRun = heads[i] == heads[i - 1] ? wideRun + 1 : 1;
            wideLongest = Math.max(wideLongest, wideRun);
        }
        return narrowLongest > TIES && wideLongest < narrowLongest;
    }

    /// {@return the bytes an index of `count` keys with a prefix of `prefix` bytes takes}
    static int length(int count, int prefix, boolean wide) {
        return prefix + (wide ? WIDE_HEAD : HEAD) * count;
    }

    /// Writes the index of a node at `position` of `unit`, past the offsets of its entries, and marks the node's
    /// header as holding it.
    ///
    /// @param first the first key of the node, which starts with the prefix
    /// @param heads the 8-byte heads of the keys after the prefix, in key order
    /// @return the position past the index
    static int write(byte[] unit, int position, MemorySegment first, int prefix, long[] heads, boolean wide) {
        unit[PageFormat.FORMAT] = PageFormat.INDEXED;
        unit[PageFormat.INDEX] = (byte) (prefix | (wide ? WIDE : 0));
        MemorySegment.copy(first, ValueLayout.JAVA_BYTE, 0, unit, position, prefix);
        position += prefix;
        for (var head : heads) {
            if (wide) {
                Bytes.i64(unit, position, head);
                position += WIDE_HEAD;
            } else {
                Bytes.i32(unit, position, (int) (head >>> 32));
                position += HEAD;
            }
        }
        return position;
    }

    /// {@return the first of the entries of the node `unit`, which holds an index, whose key is after `key`, or at it
    /// unless `through`; the count of its entries if there is none}
    ///
    /// @param keyAt the bytes into an entry where its key starts, its length being in the 2 bytes before it
    /// @throws LedgerCorruptedException if the index does not fit in the node's content
    static int search(byte[] unit, byte[] key, int keyAt, boolean through) {
        var count = TreeNodes.count(unit);
        var index = unit[PageFormat.INDEX];
        var prefix = index & PREFIX;
        var width = (index & WIDE) != 0 ? WIDE_HEAD : HEAD;
        var at = PageFormat.HEADER + 2 + 2 * count;
        var heads = at + prefix;
        var end = heads + width * count;
        checkBounds(unit, index, end);
        var touched = touch(unit, end);
        var order = Segments.compare(unit, at, prefix, key, 0, Math.min(prefix, key.length));
        int found;
        if (order != 0) {
            found = order > 0 ? 0 : count;
        } else {
            var head = head(key, prefix);
            var low = 0;
            var high = count;
            if (width == HEAD) {
                var narrow = (int) (head >>> 32);
                while (low < high) {
                    var middle = (low + high) >>> 1;
                    var step = Integer.compareUnsigned(Bytes.i32(unit, heads + HEAD * middle), narrow);
                    if (step == 0) {
                        step = compareKey(unit, middle, keyAt, key, through);
                    }
                    if (step < 0) {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
            } else {
                while (low < high) {
                    var middle = (low + high) >>> 1;
                    var step = Long.compareUnsigned(Bytes.i64(unit, heads + WIDE_HEAD * middle), head);
                    if (step == 0) {
                        step = compareKey(unit, middle, keyAt, key, through);
                    }
                    if (step < 0) {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
            }
            found = low;
        }
        if (touched == untouched) {
            sink = touched;
        }
        return found;
    }

    /// {@return the first of the entries of the node `unit`, which holds an index, whose key is after `key`, or at it
    /// unless `through`; the count of its entries if there is none}
    ///
    /// @param keyAt the bytes into an entry where its key starts, its length being in the 2 bytes before it
    /// @throws LedgerCorruptedException if the index does not fit in the node's content
    static int search(byte[] unit, MemorySegment key, int keyAt, boolean through) {
        var count = TreeNodes.count(unit);
        var index = unit[PageFormat.INDEX];
        var prefix = index & PREFIX;
        var width = (index & WIDE) != 0 ? WIDE_HEAD : HEAD;
        var at = PageFormat.HEADER + 2 + 2 * count;
        var heads = at + prefix;
        var end = heads + width * count;
        checkBounds(unit, index, end);
        var touched = touch(unit, end);
        var order = Segments.compare(unit, at, prefix, key, Math.min(prefix, key.byteSize()));
        int found;
        if (order != 0) {
            found = order > 0 ? 0 : count;
        } else {
            var head = head(key, prefix);
            var low = 0;
            var high = count;
            if (width == HEAD) {
                var narrow = (int) (head >>> 32);
                while (low < high) {
                    var middle = (low + high) >>> 1;
                    var step = Integer.compareUnsigned(Bytes.i32(unit, heads + HEAD * middle), narrow);
                    if (step == 0) {
                        step = compareKey(unit, middle, keyAt, key, through);
                    }
                    if (step < 0) {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
            } else {
                while (low < high) {
                    var middle = (low + high) >>> 1;
                    var step = Long.compareUnsigned(Bytes.i64(unit, heads + WIDE_HEAD * middle), head);
                    if (step == 0) {
                        step = compareKey(unit, middle, keyAt, key, through);
                    }
                    if (step < 0) {
                        low = middle + 1;
                    } else {
                        high = middle;
                    }
                }
            }
            found = low;
        }
        if (touched == untouched) {
            sink = touched;
        }
        return found;
    }

    /// Checks that the index the header byte `index` describes, ending at `end`, sits in the content of `unit`.
    ///
    /// The header authenticates with the unit, so only a store written wrong fails it; a plain store's check sum
    /// passes any bytes it was written with.
    private static void checkBounds(byte[] unit, byte index, int end) {
        if ((index & RESERVED) != 0 || end > PageFormat.HEADER + Bytes.i32(unit, PageFormat.LENGTH)) {
            throw new LedgerCorruptedException("A node's search index runs past its content, at page "
                    + Bytes.i32(unit, PageFormat.PAGE), null);
        }
    }

    /// {@return the bytes of `unit` read on every line from the offsets of its entries to `end`, the end of its index}
    ///
    /// The loads do not wait for each other, so the lines the search reads one after another arrive together.
    private static int touch(byte[] unit, int end) {
        var touched = unit[end - 1];
        for (var i = PageFormat.HEADER + 2; i < end; i += LINE) {
            touched |= unit[i];
        }
        return touched;
    }

    /// {@return the order of the key of entry `index` of `unit` and `key`, where an equal key counts as before `key`
    /// if `through` and after it otherwise}
    private static int compareKey(byte[] unit, int index, int keyAt, byte[] key, boolean through) {
        var at = TreeNodes.entry(unit, index) + keyAt;
        var order = TreeNodes.compare(unit, at, Bytes.u16(unit, at - 2), key);
        return order != 0 ? order : through ? -1 : 1;
    }

    /// As [#compareKey(byte\[\], int, int, byte\[\], boolean)] for a key in a segment.
    private static int compareKey(byte[] unit, int index, int keyAt, MemorySegment key, boolean through) {
        var at = TreeNodes.entry(unit, index) + keyAt;
        var order = TreeNodes.compare(unit, at, Bytes.u16(unit, at - 2), key);
        return order != 0 ? order : through ? -1 : 1;
    }
}
