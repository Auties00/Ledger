package com.github.auties00.ledger.engine;

/// A large value a store keeps in memory as two arrays: a head that fills whole regions of the G1 collector, and the
/// few bytes after it.
///
/// The G1 collector gives an array of half a region or more regions of its own, and counts every byte of its last one
/// as used, so a value of 16 MiB on one array, 16 MiB and its header, takes nine regions of 2 MiB and leaves almost all
/// of the last one empty until the array dies. Split, the head takes eight regions exactly and the 64 bytes after it
/// are an ordinary young object. A version keeps such a value from its commit until a checkpoint has taken its unit of
/// [BlobFile], which holds the value as well: only the value of a put to a paged key space of byte arrays that goes to
/// that file is split, so a log frame never needs it whole.
///
/// A read copies the value out with one array copy per array into a new array, the head's copy first, which the
/// compiler fuses with the allocation: it zeroes only the part of the new array the tail fills, which [#headLength]
/// keeps to a thirty-second of the value, so the read costs about what one of a value on one array does
/// ([Segments#toBytes(byte\[\], int, int)]).
///
/// It is immutable: both arrays are written once, before the version that holds them is published, and never again.
final class SplitValue {
    // The region before anyone asked, and what a test sets when it lets the JVM answer
    private static final long UNKNOWN = -1;
    /// The most a tail may be of its value, as a shift: a thirty-second, the share of a read's new array the compiler
    /// zeroes besides the copies.
    private static final int TAIL_SHARE_SHIFT = 5;

    // The bytes of a region of G1, 0 if the JVM runs another collector or cannot say, or UNKNOWN until asked
    private static volatile long region = UNKNOWN;
    // The region tests force, or UNKNOWN for the JVM's
    private static volatile long forcedRegionForTests = UNKNOWN;

    /// The first bytes of the value, which fill whole regions with the array's header.
    final byte[] head;
    /// The rest of the value, fewer bytes than half a region.
    final byte[] tail;

    /// Makes a value of `head` followed by `tail`, arrays no one writes.
    SplitValue(byte[] head, byte[] tail) {
        this.head = head;
        this.tail = tail;
    }

    /// {@return the bytes of the value}
    int length() {
        return head.length + tail.length;
    }

    /// {@return the value in a new array}
    byte[] join() {
        return join(head, tail);
    }

    /// {@return `head` followed by `tail`, in a new array}
    ///
    /// The copy of `head` follows the allocation with nothing in between, so the compiler zeroes only the bytes after
    /// it, which the copy of `tail` fills.
    static byte[] join(byte[] head, byte[] tail) {
        var joined = new byte[head.length + tail.length];
        System.arraycopy(head, 0, joined, 0, head.length);
        System.arraycopy(tail, 0, joined, head.length, tail.length);
        return joined;
    }

    /// {@return the bytes of the head of a value of `length` bytes split for regions of `regionBytes`, or 0 if the
    /// value is kept on one array}
    ///
    /// The head's array, with the [headroom][ValueSlab#HEADROOM] a header and the alignment take, is as many whole
    /// regions as fit the value. A value is split only if its tail would be an ordinary object, below half a region,
    /// and at most a thirty-second of the value, the bytes a read zeroes besides its copies; a value whose array fills
    /// its last region already gains nothing. A value of half a region to one region is never split: a head below a
    /// region would be copied by the young collections, and the value takes one region whole either way.
    ///
    /// @param regionBytes the bytes of a region of G1, or 0 if the JVM runs another collector
    static int headLength(long length, long regionBytes) {
        if (regionBytes <= ValueSlab.HEADROOM || length < regionBytes) {
            return 0;
        }
        var regions = (length + ValueSlab.HEADROOM) / regionBytes;
        var head = regions * regionBytes - ValueSlab.HEADROOM;
        var tail = length - head;
        if (tail <= 0 || tail >= regionBytes / 2 || tail > length >>> TAIL_SHARE_SHIFT) {
            return 0;
        }
        return Math.toIntExact(head);
    }

    /// {@return the bytes of a region of G1, or 0 if the JVM runs another collector or cannot say}
    ///
    /// The first call asks the JVM, which loads the management classes, tens of milliseconds the first time: only a
    /// put of a value of a region or more, which writes as many bytes to the file of large values, ever asks. A
    /// question that fails gives 0, so no put fails on it.
    static long region() {
        var forced = forcedRegionForTests;
        if (forced != UNKNOWN) {
            return forced;
        }
        var known = region;
        if (known != UNKNOWN) {
            return known;
        }
        long asked;
        try {
            asked = ValueSlab.collectorRegion();
        } catch (Throwable _) {
            asked = 0;
        }
        region = asked;
        return asked;
    }

    /// Makes [#region()] give `regionBytes`, 0 for a JVM that splits nothing, or the JVM's answer if `-1`; for tests.
    static void forceRegionForTests(long regionBytes) {
        forcedRegionForTests = regionBytes;
    }
}
