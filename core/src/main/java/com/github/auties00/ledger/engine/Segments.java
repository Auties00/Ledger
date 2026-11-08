package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

/// Operations on the memory segments and byte arrays that hold encoded keys and values.
///
/// Every comparison here, whatever form its keys take, orders them as unsigned bytes, a prefix before every longer key
/// it starts: the order of the trees, of the ordered maps of the key spaces and of the range removals. It reads the
/// common bytes as big-endian words, whose unsigned order is the order of their bytes, and reads no byte outside the
/// keys it compares. The first 8 bytes alone leave undecided the keys of one chat, which share a chat's id and the top
/// of a timestamp there, and so most comparisons a walk down a tree or an ordered map makes; a mismatch decides them
/// too, but its set-up costs more than the words it reads.
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
    private static final ValueLayout.OfInt BIG_ENDIAN_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfShort BIG_ENDIAN_SHORT = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    // The words of an array in the order of the platform, as JAVA_LONG_UNALIGNED reads them out of a segment for the hash
    private static final VarHandle NATIVE_LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    /// The common bytes a comparison reads as words; past them a mismatch, whose set-up costs less than the words it
    /// would skip in keys that long, finds the first byte that differs.
    private static final int WORDS = 64;

    private Segments() {
    }

    /// {@return a negative number, zero or a positive number as `left` sorts before, with or after `right`}
    static int compare(MemorySegment left, MemorySegment right) {
        return compare(left, 0, left.byteSize(), right, 0, right.byteSize());
    }

    /// {@return the order of the `leftLength` bytes at `leftFrom` of `left` and the `rightLength` bytes at `rightFrom`
    /// of `right`, as [#compare(MemorySegment, MemorySegment)] gives}
    static int compare(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength) {
        var common = Math.min(leftLength, rightLength);
        var words = Math.min(common, WORDS) & -Long.BYTES;
        for (var at = 0L; at < words; at += Long.BYTES) {
            var leftWord = left.get(BIG_ENDIAN_LONG, leftFrom + at);
            var rightWord = right.get(BIG_ENDIAN_LONG, rightFrom + at);
            if (leftWord != rightWord) {
                return Long.compareUnsigned(leftWord, rightWord);
            }
        }
        return words == common
                ? Long.compare(leftLength, rightLength)
                : compareTail(left, leftFrom, leftLength, right, rightFrom, rightLength, common);
    }

    /// {@return the order of the `leftLength` bytes at `leftFrom` of `left` and the `rightLength` bytes at `rightFrom`
    /// of `right`, as [#compare(MemorySegment, MemorySegment)] gives}
    static int compare(byte[] left, int leftFrom, int leftLength, byte[] right, int rightFrom, int rightLength) {
        var common = Math.min(leftLength, rightLength);
        var words = Math.min(common, WORDS) & -Long.BYTES;
        for (var at = 0; at < words; at += Long.BYTES) {
            var leftWord = Bytes.i64(left, leftFrom + at);
            var rightWord = Bytes.i64(right, rightFrom + at);
            if (leftWord != rightWord) {
                return Long.compareUnsigned(leftWord, rightWord);
            }
        }
        return words == common
                ? Integer.compare(leftLength, rightLength)
                : compareTail(left, leftFrom, leftLength, right, rightFrom, rightLength, common);
    }

    /// {@return the order of the `leftLength` bytes at `leftFrom` of `left` and `right`, as
    /// [#compare(MemorySegment, MemorySegment)] gives}
    static int compare(byte[] left, int leftFrom, int leftLength, MemorySegment right) {
        var rightLength = right.byteSize();
        var common = (int) Math.min(leftLength, rightLength);
        var words = Math.min(common, WORDS) & -Long.BYTES;
        for (var at = 0; at < words; at += Long.BYTES) {
            var leftWord = Bytes.i64(left, leftFrom + at);
            var rightWord = right.get(BIG_ENDIAN_LONG, at);
            if (leftWord != rightWord) {
                return Long.compareUnsigned(leftWord, rightWord);
            }
        }
        return words == common
                ? Long.compare(leftLength, rightLength)
                : compareTail(left, leftFrom, leftLength, right, rightLength, common);
    }

    // What decides two ranges whose whole words are equal, kept apart from the words so the comparison the ordered maps
    // call at every step stays small enough to inline: the rest of keys longer than WORDS, or the bytes after the last
    // whole word, then the lengths. A word that ends the common bytes overlaps the last word read, whose bytes are equal
    private static int compareTail(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength, long common) {
        if (common > WORDS) {
            return compareRest(left, leftFrom + WORDS, leftLength - WORDS, right, rightFrom + WORDS, rightLength - WORDS);
        }
        long leftTail;
        long rightTail;
        if (common >= Long.BYTES) {
            leftTail = left.get(BIG_ENDIAN_LONG, leftFrom + common - Long.BYTES);
            rightTail = right.get(BIG_ENDIAN_LONG, rightFrom + common - Long.BYTES);
        } else {
            leftTail = tail(left, leftFrom, (int) common);
            rightTail = tail(right, rightFrom, (int) common);
        }
        return leftTail != rightTail ? Long.compareUnsigned(leftTail, rightTail) : Long.compare(leftLength, rightLength);
    }

    // As compareTail of two segments
    private static int compareTail(byte[] left, int leftFrom, int leftLength, byte[] right, int rightFrom, int rightLength, int common) {
        if (common > WORDS) {
            return Arrays.compareUnsigned(left, leftFrom + WORDS, leftFrom + leftLength, right, rightFrom + WORDS, rightFrom + rightLength);
        }
        long leftTail;
        long rightTail;
        if (common >= Long.BYTES) {
            leftTail = Bytes.i64(left, leftFrom + common - Long.BYTES);
            rightTail = Bytes.i64(right, rightFrom + common - Long.BYTES);
        } else {
            leftTail = tail(left, leftFrom, common);
            rightTail = tail(right, rightFrom, common);
        }
        return leftTail != rightTail ? Long.compareUnsigned(leftTail, rightTail) : Integer.compare(leftLength, rightLength);
    }

    // As compareTail of two segments, but in words past WORDS too: a mismatch would take a segment of the array, and a
    // read by key compares its key with the bounds of every range removal
    private static int compareTail(byte[] left, int leftFrom, int leftLength, MemorySegment right, long rightLength, int common) {
        for (var at = WORDS; at + Long.BYTES <= common; at += Long.BYTES) {
            var leftWord = Bytes.i64(left, leftFrom + at);
            var rightWord = right.get(BIG_ENDIAN_LONG, at);
            if (leftWord != rightWord) {
                return Long.compareUnsigned(leftWord, rightWord);
            }
        }
        long leftTail;
        long rightTail;
        if (common >= Long.BYTES) {
            leftTail = Bytes.i64(left, leftFrom + common - Long.BYTES);
            rightTail = right.get(BIG_ENDIAN_LONG, common - Long.BYTES);
        } else {
            leftTail = tail(left, leftFrom, common);
            rightTail = tail(right, 0, common);
        }
        return leftTail != rightTail ? Long.compareUnsigned(leftTail, rightTail) : Long.compare(leftLength, rightLength);
    }

    // The order of two ranges past the common bytes the words read
    private static int compareRest(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength) {
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

    /// {@return the 1 to 7 bytes at `from` of `segment`, as a big-endian number}
    private static long tail(MemorySegment segment, long from, int length) {
        var value = 0L;
        var at = from;
        if ((length & Integer.BYTES) != 0) {
            value = segment.get(BIG_ENDIAN_INT, at) & 0xFFFFFFFFL;
            at += Integer.BYTES;
        }
        if ((length & Short.BYTES) != 0) {
            value = value << Short.SIZE | segment.get(BIG_ENDIAN_SHORT, at) & 0xFFFF;
            at += Short.BYTES;
        }
        if ((length & 1) != 0) {
            value = value << Byte.SIZE | segment.get(ValueLayout.JAVA_BYTE, at) & 0xFF;
        }
        return value;
    }

    /// {@return the 1 to 7 bytes at `from` of `array`, as a big-endian number}
    private static long tail(byte[] array, int from, int length) {
        var value = 0L;
        var at = from;
        if ((length & Integer.BYTES) != 0) {
            value = Bytes.i32(array, at) & 0xFFFFFFFFL;
            at += Integer.BYTES;
        }
        if ((length & Short.BYTES) != 0) {
            value = value << Short.SIZE | Bytes.u16(array, at);
            at += Short.BYTES;
        }
        if ((length & 1) != 0) {
            value = value << Byte.SIZE | array[at] & 0xFF;
        }
        return value;
    }

    /// {@return whether the `leftLength` bytes at `leftFrom` of `left` are the `rightLength` bytes at `rightFrom` of
    /// `right`}
    static boolean equal(MemorySegment left, long leftFrom, long leftLength, MemorySegment right, long rightFrom, long rightLength) {
        return leftLength == rightLength && compare(left, leftFrom, leftLength, right, rightFrom, rightLength) == 0;
    }

    /// {@return whether `left` and `right` hold the same bytes}
    static boolean equal(MemorySegment left, MemorySegment right) {
        return equal(left, 0, left.byteSize(), right, 0, right.byteSize());
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
        return finish(hash);
    }

    /// {@return the hash of the bytes of `array`, the one [#hash(MemorySegment)] gives a segment of the same bytes}
    ///
    /// Chains are indexed by the hash of their segments, so a lookup by an array that hashed its bytes otherwise would
    /// miss them and read the tree.
    static int hash(byte[] array) {
        var length = array.length;
        var hash = HASH_SEED ^ length * 0x9E3779B97F4A7C15L;
        var offset = 0;
        for (; offset + Long.BYTES <= length; offset += Long.BYTES) {
            hash = fold(hash ^ (long) NATIVE_LONG.get(array, offset), HASH_KEY);
        }
        if (offset < length) {
            var tail = 0L;
            for (var shift = 0; offset < length; offset++, shift += 8) {
                tail |= (array[offset] & 0xFFL) << shift;
            }
            hash = fold(hash ^ tail, HASH_KEY);
        }
        return finish(hash);
    }

    private static int finish(long hash) {
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
        return toBytes(segment, 0, Math.toIntExact(segment.byteSize()));
    }

    /// {@return the `length` bytes at `offset` of `segment`, in a new array}
    ///
    /// @throws IndexOutOfBoundsException if `segment` holds fewer
    static byte[] toBytes(MemorySegment segment, long offset, int length) {
        var array = new byte[length];
        MemorySegment.copy(segment, offset, MemorySegment.ofArray(array), 0, length);
        return array;
    }

    /// {@return the `length` bytes at `offset` of `array`, in a new array}
    ///
    /// An array copy fills the new array whole, so the compiler leaves out the zeroing of its allocation; it cannot
    /// for a segment copy, which writes an array twice, as zeros and then as the value: for a value of a megabyte or
    /// more, whose array the allocation's slow path zeroes, that is a third of what a read of it costs. The bounds are
    /// checked first: past the end of `array`, [Arrays#copyOfRange(byte\[\], int, int)] would pad with zeros.
    ///
    /// @throws IndexOutOfBoundsException if `array` holds fewer
    static byte[] toBytes(byte[] array, int offset, int length) {
        Objects.checkFromIndexSize(offset, length, array.length);
        return Arrays.copyOfRange(array, offset, offset + length);
    }

    /// {@return `key` encoded into a read-only segment of its own}
    ///
    /// @throws IllegalArgumentException if the key encodes to no bytes or more than [KeyCodec#MAX_LENGTH]
    static <K> MemorySegment encodeKey(KeyCodec<K> codec, K key) {
        var array = new byte[checkedLength(codec.sizeOf(key))];
        var segment = MemorySegment.ofArray(array);
        codec.encode(key, segment);
        return segment.asReadOnly();
    }

    /// {@return `key` encoded into an array of its own}
    ///
    /// @throws IllegalArgumentException if the key encodes to no bytes or more than [KeyCodec#MAX_LENGTH]
    static <K> byte[] encodeKeyBytes(KeyCodec<K> codec, K key) {
        var array = new byte[checkedLength(codec.sizeOf(key))];
        codec.encode(key, MemorySegment.ofArray(array));
        return array;
    }

    /// {@return `key` encoded for a lookup that keeps nothing of it past the call: the caller's array itself for a key
    /// of [KeyCodec#bytes()], which is its own encoding and must not change during the call}
    ///
    /// @throws IllegalArgumentException if the key encodes to no bytes or more than [KeyCodec#MAX_LENGTH]
    static <K> byte[] lookupKey(KeyCodec<K> codec, K key) {
        if (codec == BuiltinCodecs.BYTES_KEY) {
            var array = (byte[]) key;
            checkedLength(array.length);
            return array;
        }
        return encodeKeyBytes(codec, key);
    }

    private static int checkedLength(int length) {
        if (length < 1 || length > KeyCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Keys are 1 to " + KeyCodec.MAX_LENGTH + " bytes: " + length);
        }
        return length;
    }
}
