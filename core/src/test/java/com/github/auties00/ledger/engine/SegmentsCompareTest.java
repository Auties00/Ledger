package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Every form of the key comparison against the order of [Arrays#compareUnsigned(byte\[\], int, int, byte\[\], int, int)],
/// and the two forms of the hash against each other. Keys sit inside larger arrays whose bytes around them would change
/// the answer of a comparison that read them, and are slices a read past their end throws on.
class SegmentsCompareTest {
    // Around word sizes and the point where the comparison falls back to a mismatch
    private static final int[] LENGTHS = {1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 63, 64, 65, 66, 71, 72, 73, 127, 128, 129, 1023, 1024};
    private static final int MARGIN = 19;

    private static void assertSameOrder(byte[] left, int leftFrom, int leftLength, byte[] right, int rightFrom, int rightLength) {
        var expected = Integer.signum(Arrays.compareUnsigned(left, leftFrom, leftFrom + leftLength, right, rightFrom, rightFrom + rightLength));
        var leftSegment = MemorySegment.ofArray(left);
        var rightSegment = MemorySegment.ofArray(right);
        var leftSlice = leftSegment.asSlice(leftFrom, leftLength).asReadOnly();
        var rightSlice = rightSegment.asSlice(rightFrom, rightLength).asReadOnly();
        var rightKey = Arrays.copyOfRange(right, rightFrom, rightFrom + rightLength);
        var name = "%s against %s".formatted(
                java.util.HexFormat.of().formatHex(left, leftFrom, leftFrom + leftLength),
                java.util.HexFormat.of().formatHex(right, rightFrom, rightFrom + rightLength));
        assertEquals(expected, Integer.signum(Segments.compare(leftSlice, rightSlice)), "slices: " + name);
        assertEquals(expected, Integer.signum(Segments.UNSIGNED.compare(leftSlice, rightSlice)), "comparator: " + name);
        assertEquals(expected, Integer.signum(Segments.compare(leftSegment, leftFrom, leftLength, rightSegment, rightFrom, rightLength)), "ranges: " + name);
        assertEquals(expected, Integer.signum(Segments.compare(leftSegment.asReadOnly(), leftFrom, leftLength, rightSegment.asReadOnly(), rightFrom, rightLength)), "read-only ranges: " + name);
        assertEquals(expected, Integer.signum(Segments.compare(left, leftFrom, leftLength, right, rightFrom, rightLength)), "arrays: " + name);
        assertEquals(expected, Integer.signum(Segments.compare(left, leftFrom, leftLength, rightSlice)), "array and slice: " + name);
        assertEquals(expected, Integer.signum(TreeNodes.compare(left, leftFrom, leftLength, rightSlice)), "node and slice: " + name);
        assertEquals(expected, Integer.signum(TreeNodes.compare(left, leftFrom, leftLength, rightKey)), "node and array: " + name);
        assertEquals(expected, Integer.signum(byMismatch(leftSlice, rightSlice)), "mismatch: " + name);
        assertEquals(expected == 0, Segments.equal(leftSlice, rightSlice), "equal slices: " + name);
        assertEquals(expected == 0, Segments.equal(leftSegment, leftFrom, leftLength, rightSegment, rightFrom, rightLength), "equal ranges: " + name);
    }

    // The order a whole mismatch gives, the reference apart from the JDK's array comparison
    private static int byMismatch(MemorySegment left, MemorySegment right) {
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
        return Byte.compareUnsigned(left.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE)[(int) mismatch],
                right.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE)[(int) mismatch]);
    }

    // `key` at `from` of an array whose other bytes are all `around`
    private static byte[] placed(byte[] key, int from, int around) {
        var array = new byte[from + key.length + MARGIN];
        Arrays.fill(array, (byte) around);
        System.arraycopy(key, 0, array, from, key.length);
        return array;
    }

    private static void assertBothWays(byte[] left, byte[] right, int leftFrom, int rightFrom) {
        // Past each key, bytes that sort below and above whatever the other key holds there
        for (var around : new int[][]{{0x00, 0xFF}, {0xFF, 0x00}}) {
            var placedLeft = placed(left, leftFrom, around[0]);
            var placedRight = placed(right, rightFrom, around[1]);
            assertSameOrder(placedLeft, leftFrom, left.length, placedRight, rightFrom, right.length);
            assertSameOrder(placedRight, rightFrom, right.length, placedLeft, leftFrom, left.length);
        }
    }

    private static byte[] filled(int length, long seed) {
        var bytes = new byte[length];
        new SplittableRandom(seed).nextBytes(bytes);
        return bytes;
    }

    @Test
    @DisplayName("orders a key before every key it starts, and equal keys as equal, at every length and offset")
    void prefixes() {
        for (var shorter : LENGTHS) {
            for (var longer : LENGTHS) {
                if (longer < shorter) {
                    continue;
                }
                var key = filled(longer, longer);
                assertBothWays(Arrays.copyOf(key, shorter), key, shorter % 3, (longer % 5) + 1);
            }
        }
    }

    @Test
    @DisplayName("finds a difference at any position of the common bytes, with the bytes as unsigned")
    void differences() {
        for (var leftLength : LENGTHS) {
            for (var rightLength : LENGTHS) {
                var common = Math.min(leftLength, rightLength);
                var base = filled(Math.max(leftLength, rightLength), common);
                for (var at : new int[]{0, 1, 6, 7, 8, 9, 15, 16, 17, 62, 63, 64, 65, 72, common - 2, common - 1}) {
                    if (at < 0 || at >= common) {
                        continue;
                    }
                    var left = Arrays.copyOf(base, leftLength);
                    var right = Arrays.copyOf(base, rightLength);
                    // Across the sign bit, which a signed comparison gets the wrong way round
                    left[at] = 0x7F;
                    right[at] = (byte) 0x80;
                    assertBothWays(left, right, at % 8, (at + 3) % 8);
                }
            }
        }
    }

    @Test
    @DisplayName("agrees on random keys of 1 to 1024 bytes sharing prefixes of any length, keys of a small alphabet at nonzero offsets")
    void random() {
        var random = new SplittableRandom(17);
        for (var round = 0; round < 40_000; round++) {
            var leftLength = 1 + (random.nextBoolean() ? random.nextInt(80) : random.nextInt(1024));
            var rightLength = 1 + (random.nextBoolean() ? random.nextInt(80) : random.nextInt(1024));
            var left = new byte[leftLength];
            var right = new byte[rightLength];
            random.nextBytes(left);
            var shared = random.nextInt(Math.min(leftLength, rightLength) + 1);
            System.arraycopy(left, 0, right, 0, shared);
            if (shared < rightLength) {
                var rest = new byte[rightLength - shared];
                random.nextBytes(rest);
                System.arraycopy(rest, 0, right, shared, rest.length);
            }
            if (random.nextInt(4) == 0) {
                // Few values, so runs repeat
                for (var i = 0; i < leftLength; i++) {
                    left[i] &= 1;
                }
                for (var i = 0; i < rightLength; i++) {
                    right[i] &= 1;
                }
            }
            assertBothWays(left, right, random.nextInt(9), random.nextInt(9));
        }
    }

    @Test
    @DisplayName("sorts as Arrays.compareUnsigned sorts, through the comparator of the ordered maps")
    void sorted() {
        var random = new SplittableRandom(23);
        var keys = new byte[2_000][];
        for (var i = 0; i < keys.length; i++) {
            keys[i] = new byte[1 + random.nextInt(40)];
            random.nextBytes(keys[i]);
            keys[i][0] &= 3;
        }
        var expected = keys.clone();
        Arrays.sort(expected, Arrays::compareUnsigned);
        var segments = new MemorySegment[keys.length];
        for (var i = 0; i < keys.length; i++) {
            segments[i] = MemorySegment.ofArray(keys[i]).asReadOnly();
        }
        Arrays.sort(segments, Segments.UNSIGNED);
        for (var i = 0; i < keys.length; i++) {
            assertArrayEquals(expected[i], Segments.toBytes(segments[i]));
        }
    }

    @Test
    @DisplayName("hashes an array as it hashes a segment or a slice of the same bytes")
    void hashes() {
        var random = new SplittableRandom(29);
        for (var length = 0; length <= 1024; length = length < 64 ? length + 1 : length * 2) {
            for (var round = 0; round < 200; round++) {
                var bytes = new byte[length];
                random.nextBytes(bytes);
                var offset = random.nextInt(16);
                var larger = placed(bytes, offset, random.nextInt(256));
                var expected = Segments.hash(MemorySegment.ofArray(bytes));
                assertEquals(expected, Segments.hash(bytes));
                assertEquals(expected, Segments.hash(MemorySegment.ofArray(bytes).asReadOnly()));
                assertEquals(expected, Segments.hash(MemorySegment.ofArray(larger).asSlice(offset, length)));
                assertEquals(expected, Segments.hash(MemorySegment.ofArray(larger), offset, length));
            }
        }
    }

    @Test
    @DisplayName("looks a key of byte arrays up by its own array, checked as an encoding is, and copies every other key")
    void lookupKeys() {
        var key = filled(32, 31);
        assertSame(key, Segments.lookupKey(KeyCodec.bytes(), key));
        assertNotSame(key, Segments.encodeKeyBytes(KeyCodec.bytes(), key));
        assertArrayEquals(key, Segments.encodeKeyBytes(KeyCodec.bytes(), key));
        assertArrayEquals(Segments.toBytes(Segments.encodeKey(KeyCodec.utf8(), "chat")), Segments.lookupKey(KeyCodec.utf8(), "chat"));
        assertArrayEquals(Segments.toBytes(Segments.encodeKey(KeyCodec.int64(), 5L)), Segments.lookupKey(KeyCodec.int64(), 5L));
        assertEquals(KeyCodec.MAX_LENGTH, Segments.lookupKey(KeyCodec.bytes(), new byte[KeyCodec.MAX_LENGTH]).length);
        assertThrows(IllegalArgumentException.class, () -> Segments.lookupKey(KeyCodec.bytes(), new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> Segments.lookupKey(KeyCodec.bytes(), new byte[KeyCodec.MAX_LENGTH + 1]));
        assertThrows(IllegalArgumentException.class, () -> Segments.lookupKey(KeyCodec.utf8(), ""));
        assertThrows(IllegalArgumentException.class, () -> Segments.encodeKey(KeyCodec.bytes(), new byte[0]));
    }
}
