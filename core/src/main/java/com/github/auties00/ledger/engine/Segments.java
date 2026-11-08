package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Comparator;

/// Operations on the memory segments that hold encoded keys and values.
final class Segments {
    // Mixed into every hash; drawn once per process
    private static final long HASH_SEED;
    private static final long HASH_KEY;

    static {
        var random = new java.security.SecureRandom();
        HASH_SEED = random.nextLong();
        // Odd, so its products keep every bit of the other factor
        HASH_KEY = random.nextLong() | 1;
    }
    /// Orders segments as unsigned bytes, a prefix before every longer segment it starts.
    static final Comparator<MemorySegment> UNSIGNED = Segments::compare;
    static final ValueLayout.OfLong BIG_ENDIAN_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    private Segments() {
    }

    /// {@return a negative number, zero or a positive number as `left` sorts before, with or after `right`}
    static int compare(MemorySegment left, MemorySegment right) {
        // The first 8 bytes decide most comparisons of keys that do not share a prefix, without the set-up of a
        // mismatch, which a walk down the ordered map of a large key space pays twenty times
        if (left.byteSize() >= Long.BYTES && right.byteSize() >= Long.BYTES) {
            var leftPrefix = left.get(BIG_ENDIAN_LONG, 0);
            var rightPrefix = right.get(BIG_ENDIAN_LONG, 0);
            if (leftPrefix != rightPrefix) {
                return Long.compareUnsigned(leftPrefix, rightPrefix);
            }
        }
        var mismatch = left.mismatch(right);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == left.byteSize()) {
            return -1;
        }
        if (mismatch == right.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(left.get(ValueLayout.JAVA_BYTE, mismatch), right.get(ValueLayout.JAVA_BYTE, mismatch));
    }

    /// {@return the order of the `leftLength` bytes at `leftFrom` of `left` and the `rightLength` bytes at `rightFrom`
    /// of `right`, as [#compare(MemorySegment, MemorySegment)] gives}
    static int compare(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength) {
        if (leftLength >= Long.BYTES && rightLength >= Long.BYTES) {
            var leftPrefix = left.get(BIG_ENDIAN_LONG, leftFrom);
            var rightPrefix = right.get(BIG_ENDIAN_LONG, rightFrom);
            if (leftPrefix != rightPrefix) {
                return Long.compareUnsigned(leftPrefix, rightPrefix);
            }
        }
        var mismatch = MemorySegment.mismatch(left, leftFrom, leftFrom + leftLength, right, rightFrom, rightFrom + rightLength);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == leftLength) {
            return -1;
        }
        if (mismatch == rightLength) {
            return 1;
        }
        return Byte.compareUnsigned(left.get(ValueLayout.JAVA_BYTE, leftFrom + mismatch), right.get(ValueLayout.JAVA_BYTE, rightFrom + mismatch));
    }

    /// {@return the first 8 of the `length` bytes at `from` of `segment`, big-endian, zero past their end}
    ///
    /// As unsigned numbers, the words of two keys that differ order them as [#compare(MemorySegment, MemorySegment)]
    /// does; words that are equal say nothing, as the keys may differ past them, or by trailing zero bytes alone.
    static long word(MemorySegment segment, long from, long length) {
        if (length >= Long.BYTES) {
            return segment.get(BIG_ENDIAN_LONG, from);
        }
        var word = 0L;
        for (var i = 0L; i < length; i++) {
            word |= (segment.get(ValueLayout.JAVA_BYTE, from + i) & 0xFFL) << (56 - 8 * i);
        }
        return word;
    }

    /// {@return whether the `leftLength` bytes at `leftFrom` of `left` are the `rightLength` bytes at `rightFrom` of
    /// `right`}
    static boolean equal(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength) {
        if (leftLength != rightLength) {
            return false;
        }
        if (leftLength >= Long.BYTES && left.get(ValueLayout.JAVA_LONG_UNALIGNED, leftFrom) != right.get(ValueLayout.JAVA_LONG_UNALIGNED, rightFrom)) {
            return false;
        }
        return MemorySegment.mismatch(left, leftFrom, leftFrom + leftLength, right, rightFrom, rightFrom + rightLength) == -1;
    }

    /// {@return whether `left` and `right` hold the same bytes}
    static boolean equal(MemorySegment left, MemorySegment right) {
        var length = left.byteSize();
        if (length != right.byteSize()) {
            return false;
        }
        // The first 8 bytes tell most keys apart without the set-up of a mismatch
        if (length >= Long.BYTES && left.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) != right.get(ValueLayout.JAVA_LONG_UNALIGNED, 0)) {
            return false;
        }
        return left.mismatch(right) == -1;
    }

    /// {@return a hash of the bytes of `segment`, under a seed of this process}
    ///
    /// Keys often share long prefixes, a chat's id before a message's, so every byte counts. The seed, random and never
    /// stored, keeps keys that others choose, as the senders of messages choose their ids, from being made to collide:
    /// the hash index probes a run of colliding keys one by one.
    static int hash(MemorySegment segment) {
        return hash(segment, 0, segment.byteSize());
    }

    /// {@return the hash of the `length` bytes at `from` of `segment`, as [#hash(MemorySegment)] gives it}
    static int hash(MemorySegment segment, long from, long length) {
        var hash = HASH_SEED ^ length * 0x9E3779B97F4A7C15L;
        var offset = 0L;
        for (; offset + Long.BYTES <= length; offset += Long.BYTES) {
            hash = fold(hash ^ segment.get(ValueLayout.JAVA_LONG_UNALIGNED, from + offset), HASH_KEY);
        }
        if (offset < length) {
            var tail = 0L;
            for (var shift = 0; offset < length; offset++, shift += 8) {
                tail |= (segment.get(ValueLayout.JAVA_BYTE, from + offset) & 0xFFL) << shift;
            }
            hash = fold(hash ^ tail, HASH_KEY);
        }
        hash = fold(hash, HASH_KEY ^ 0x9E3779B97F4A7C15L);
        return (int) (hash ^ (hash >>> 32));
    }

    // The 128-bit product of left and right, its halves folded into each other: every bit of the result depends on
    // every bit of both, so a difference in the bytes cannot be steered to cancel without the key
    private static long fold(long left, long right) {
        return Math.multiplyHigh(left, right) ^ (left * right);
    }

    /// {@return a read-only copy of `segment` in an array of its own}
    ///
    /// For a key that outlives the batch it came in: a slice would keep the batch's whole array reachable.
    static MemorySegment copy(MemorySegment segment) {
        return MemorySegment.ofArray(toBytes(segment)).asReadOnly();
    }

    /// {@return the bytes of `segment`, in a new array}
    ///
    /// `MemorySegment.toArray` does the same, but builds the name of the array class for its size check on every
    /// call, which costs more than copying a short value.
    static byte[] toBytes(MemorySegment segment) {
        var array = new byte[Math.toIntExact(segment.byteSize())];
        MemorySegment.copy(segment, 0, MemorySegment.ofArray(array), 0, array.length);
        return array;
    }

    /// {@return `key` encoded into a read-only segment of its own}
    ///
    /// @throws IllegalArgumentException if the key encodes to no bytes or more than [KeyCodec#MAX_LENGTH]
    static <K> MemorySegment encodeKey(KeyCodec<K> codec, K key) {
        var size = codec.sizeOf(key);
        if (size < 1 || size > KeyCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Keys are 1 to " + KeyCodec.MAX_LENGTH + " bytes: " + size);
        }
        var array = new byte[size];
        var segment = MemorySegment.ofArray(array);
        codec.encode(key, segment);
        return segment.asReadOnly();
    }
}
