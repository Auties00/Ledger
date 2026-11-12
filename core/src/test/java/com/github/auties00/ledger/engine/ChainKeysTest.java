package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Every key method of [Chain] against [Arrays#compareUnsigned(byte\[\], byte\[\])] and [Arrays#equals(byte\[\], byte\[\])]
/// on the plain bytes, over keys of 0 to 40 bytes held in the chain's own words, in a slab chunk at an offset past 0,
/// or in a segment of their own, compared with one another and with segments and arrays of every kind.
class ChainKeysTest {
    // Every length from the empty key to past the longest a chain holds in its words
    private static final int LONGEST = 40;

    @Test
    @DisplayName("picks the width of a chain's own words by the key's length, and keeps longer keys in a slab")
    void widths() {
        var slab = new KeySlab();
        for (var length = 0; length <= LONGEST; length++) {
            var key = MemorySegment.ofArray(new byte[length]);
            var chain = slab.chain(key, Segments.hash(key));
            assertEquals(length, chain.keyLength());
            assertEquals(length <= Chain.INLINE, chain.inlineKey(), "length " + length);
            if (length <= 8) {
                assertInstanceOf(Chain.Inline8.class, chain);
                assertFalse(chain instanceof Chain.Inline16, "length " + length);
            } else if (length <= 16) {
                assertInstanceOf(Chain.Inline16.class, chain);
                assertFalse(chain instanceof Chain.Inline24, "length " + length);
            } else if (length <= 24) {
                assertInstanceOf(Chain.Inline24.class, chain);
                assertFalse(chain instanceof Chain.Inline32, "length " + length);
            } else if (length <= 32) {
                assertInstanceOf(Chain.Inline32.class, chain);
            } else {
                assertFalse(chain instanceof Chain.Inline8, "length " + length);
            }
        }
        assertFalse(Chain.BOTTOM.inlineKey());
        assertThrows(IllegalArgumentException.class, () -> Chain.inline(MemorySegment.ofArray(new byte[Chain.INLINE + 1]), 0));
    }

    @Test
    @DisplayName("compares and matches keys as their bytes, whichever way each side holds its key")
    void againstBytes() {
        var random = new SplittableRandom(0x1EDBE7L);
        var keys = keys(random);
        var slab = new KeySlab();
        var chains = new ArrayList<List<Chain>>();
        var segments = new ArrayList<List<MemorySegment>>();
        for (var key : keys) {
            chains.add(forms(slab, key, random));
            segments.add(segments(key));
        }
        for (var i = 0; i < keys.size(); i++) {
            var left = keys.get(i);
            for (var leftChain : chains.get(i)) {
                checkAlone(leftChain, left);
                for (var j = 0; j < keys.size(); j++) {
                    var right = keys.get(j);
                    var expected = Integer.signum(Arrays.compareUnsigned(left, right));
                    var name = HexFormat.of().formatHex(left) + " against " + HexFormat.of().formatHex(right);
                    for (var segment : segments.get(j)) {
                        assertEquals(expected, Integer.signum(leftChain.compareKey(segment)), "compareKey: " + name);
                        assertEquals(expected == 0, leftChain.keyEquals(segment), "keyEquals: " + name);
                    }
                    assertEquals(expected == 0, leftChain.keyEquals(right.clone()), "keyEquals of an array: " + name);
                    for (var rightChain : chains.get(j)) {
                        assertEquals(expected, Integer.signum(Chain.compareKeys(leftChain, rightChain)), "compareKeys: " + name);
                        assertEquals(expected, Integer.signum(Chain.BY_KEY.compare(leftChain, rightChain)), "BY_KEY: " + name);
                        assertEquals(expected == 0, Chain.sameKey(leftChain, rightChain), "sameKey: " + name);
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}-byte keys")
    @ValueSource(ints = {0, 1, 7, 8, 9, 15, 16, 17, 23, 24, 25, 31, 32, 33, 40})
    @DisplayName("orders random keys of one length, and keys one byte past or short of it, as their bytes")
    void randomOfLength(int length) {
        var random = new SplittableRandom(length * 31L + 7);
        var slab = new KeySlab();
        for (var round = 0; round < 2_000; round++) {
            var left = bytes(random, length);
            var right = random.nextInt(4) == 0 ? left.clone() : bytes(random, Math.max(0, length + random.nextInt(3) - 1));
            if (right.length > 0 && random.nextBoolean()) {
                // Share all but the last byte, so only the tail word or the lengths decide
                System.arraycopy(left, 0, right, 0, Math.min(left.length, right.length - 1));
            }
            var expected = Integer.signum(Arrays.compareUnsigned(left, right));
            var leftChain = forms(slab, left, random).get(random.nextInt(3));
            var rightChain = forms(slab, right, random).get(random.nextInt(3));
            assertEquals(expected, Integer.signum(Chain.compareKeys(leftChain, rightChain)));
            assertEquals(expected, Integer.signum(leftChain.compareKey(MemorySegment.ofArray(right))));
            assertEquals(expected == 0, Chain.sameKey(leftChain, rightChain));
            assertEquals(expected == 0, leftChain.keyEquals(MemorySegment.ofArray(right)));
            checkAlone(leftChain, left);
        }
    }

    @Test
    @DisplayName("copies a key to a caller's array at an offset, and checks the bounds first")
    void copyKey() {
        var random = new SplittableRandom(5);
        var slab = new KeySlab();
        for (var length = 0; length <= LONGEST; length++) {
            var key = bytes(random, length);
            for (var chain : forms(slab, key, random)) {
                var target = new byte[length + 6];
                Arrays.fill(target, (byte) 0x5A);
                chain.copyKey(target, 3);
                assertArrayEquals(key, Arrays.copyOfRange(target, 3, 3 + length));
                for (var i : new int[]{0, 1, 2, length + 3, length + 4, length + 5}) {
                    assertEquals((byte) 0x5A, target[i], "byte " + i + " around a key of " + length);
                }
                var small = new byte[length + 2];
                assertThrows(IndexOutOfBoundsException.class, () -> chain.copyKey(small, 3));
            }
        }
    }

    @Test
    @DisplayName("gives the ordered map and walks a read-only copy of a key in a chain's own words, packed in the slab")
    void slabKeys() {
        var random = new SplittableRandom(11);
        var slab = new KeySlab();
        for (var round = 0; round < 3_000; round++) {
            var key = bytes(random, random.nextInt(LONGEST + 1));
            var segment = MemorySegment.ofArray(key);
            var chain = slab.chain(segment, Segments.hash(segment));
            var copy = slab.key(chain);
            assertArrayEquals(key, Segments.toBytes(copy));
            assertTrue(copy.isReadOnly());
            assertTrue(chain.keyEquals(copy));
            assertEquals(0, Segments.compare(copy, chain.key()));
        }
    }

    @Test
    @DisplayName("keeps no reference to the segment a chain was made of, so its bytes may be reused at once")
    void reusedInput() {
        var random = new SplittableRandom(13);
        var slab = new KeySlab();
        try (var arena = Arena.ofConfined()) {
            var buffer = arena.allocate(LONGEST + 9);
            for (var round = 0; round < 1_000; round++) {
                var key = bytes(random, random.nextInt(LONGEST + 1));
                // A native, unaligned slice that ends where the key does
                var at = random.nextInt(9);
                MemorySegment.copy(key, 0, buffer, ValueLayout.JAVA_BYTE, at, key.length);
                var slice = buffer.asSlice(at, key.length);
                var chain = slab.chain(slice, Segments.hash(slice));
                buffer.fill((byte) 0xA5);
                assertArrayEquals(key, Segments.toBytes(chain.key()));
                assertEquals(Segments.hash(MemorySegment.ofArray(key)), chain.hash);
            }
        }
    }

    @Test
    @DisplayName("lets threads that make chains, copy their keys and compare them at once agree with the bytes")
    void concurrent() throws Exception {
        var slab = new KeySlab();
        var threads = 8;
        var made = new ConcurrentHashMap<Chain, byte[]>();
        var start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(threads)) {
            var tasks = new ArrayList<java.util.concurrent.Future<?>>();
            for (var t = 0; t < threads; t++) {
                var seed = t;
                tasks.add(executor.submit(() -> {
                    start.await();
                    var random = new SplittableRandom(seed);
                    for (var i = 0; i < 20_000; i++) {
                        var key = bytes(random, random.nextInt(LONGEST + 1));
                        var segment = MemorySegment.ofArray(key);
                        var chain = slab.chain(segment, Segments.hash(segment));
                        var copy = slab.key(chain);
                        if (!Arrays.equals(key, Segments.toBytes(copy)) || !chain.keyEquals(key)) {
                            throw new AssertionError("a key changed on its way through the slab: " + HexFormat.of().formatHex(key));
                        }
                        if (i % 64 == 0) {
                            made.put(chain, key);
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (var task : tasks) {
                task.get();
            }
        }
        var entries = new ArrayList<>(made.entrySet());
        for (var i = 1; i < entries.size(); i++) {
            var left = entries.get(i - 1);
            var right = entries.get(i);
            assertEquals(Integer.signum(Arrays.compareUnsigned(left.getValue(), right.getValue())),
                    Integer.signum(Chain.compareKeys(left.getKey(), right.getKey())));
            assertArrayEquals(left.getValue(), Segments.toBytes(left.getKey().key()));
        }
    }

    // What a chain alone answers about its key: its length, its bytes, its words at every offset
    private static void checkAlone(Chain chain, byte[] key) {
        var name = HexFormat.of().formatHex(key) + (chain.inlineKey() ? " in words" : " in a segment");
        assertEquals(key.length, chain.keyLength(), name);
        var copy = chain.key();
        assertTrue(copy.isReadOnly(), "a key copy is writable: " + name);
        assertArrayEquals(key, Segments.toBytes(copy), name);
        for (var at = 0; at <= LONGEST + 4; at++) {
            assertEquals(word(key, at), chain.word(at), "word at " + at + " of " + name);
        }
    }

    // The 8 bytes at `at` of `key`, big-endian, zero past its end
    private static long word(byte[] key, int at) {
        var word = 0L;
        for (var i = 0; i < Long.BYTES; i++) {
            var b = at + i < key.length ? key[at + i] & 0xFFL : 0;
            word |= b << (56 - 8 * i);
        }
        return word;
    }

    // A chain of `key` in each form: in the slab (its own words up to INLINE bytes), at an offset of a shared
    // read-only chunk, and in a segment of its own
    private static List<Chain> forms(KeySlab slab, byte[] key, SplittableRandom random) {
        var segment = MemorySegment.ofArray(key.clone());
        var hash = Segments.hash(segment);
        var offset = random.nextInt(1, 64);
        var chunk = new byte[offset + key.length + random.nextInt(8)];
        Arrays.fill(chunk, (byte) random.nextInt(256));
        System.arraycopy(key, 0, chunk, offset, key.length);
        return List.of(
                slab.chain(segment, hash),
                new Chain(MemorySegment.ofArray(chunk).asReadOnly(), offset, key.length, hash),
                new Chain(MemorySegment.ofArray(key.clone()).asReadOnly(), hash));
    }

    // `key` as segments of every kind a key reaches a chain in: an array of its own, read-only, a slice of a larger
    // array whose bytes past it differ, and a native unaligned slice that ends with it
    private static List<MemorySegment> segments(byte[] key) {
        var larger = new byte[key.length + 11];
        Arrays.fill(larger, (byte) 0xFF);
        System.arraycopy(key, 0, larger, 3, key.length);
        var global = Arena.global().allocate(key.length + 1L);
        MemorySegment.copy(key, 0, global, ValueLayout.JAVA_BYTE, 1, key.length);
        return List.of(
                MemorySegment.ofArray(key.clone()),
                MemorySegment.ofArray(key.clone()).asReadOnly(),
                MemorySegment.ofArray(larger).asSlice(3, key.length),
                global.asSlice(1, key.length));
    }

    // The empty key, the word boundaries, keys equal but for trailing 0x00 or 0xFF bytes, and random keys sharing
    // prefixes
    private static List<byte[]> keys(SplittableRandom random) {
        var keys = new ArrayList<byte[]>();
        keys.add(new byte[0]);
        var base = bytes(random, LONGEST);
        for (var length = 1; length <= LONGEST; length++) {
            keys.add(Arrays.copyOf(base, length));
            var zeros = Arrays.copyOf(base, length);
            Arrays.fill(zeros, Math.max(0, length - 3), length, (byte) 0);
            keys.add(zeros);
            var ones = Arrays.copyOf(base, length);
            Arrays.fill(ones, Math.max(0, length - 2), length, (byte) 0xFF);
            keys.add(ones);
        }
        for (var length : new int[]{8, 16, 24, 32}) {
            keys.add(new byte[length]);
            var full = new byte[length];
            Arrays.fill(full, (byte) 0xFF);
            keys.add(full);
        }
        keys.add(new byte[]{'a', 'b'});
        keys.add(new byte[]{'a', 'b', 0});
        keys.add(new byte[]{'a', 'b', 0, 0, 0, 0, 0, 0, 0});
        keys.add(new byte[]{(byte) 0x80});
        keys.add(new byte[]{0x7F});
        for (var i = 0; i < 40; i++) {
            keys.add(bytes(random, random.nextInt(LONGEST + 1)));
        }
        return keys;
    }

    private static byte[] bytes(SplittableRandom random, int length) {
        var bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    @Test
    @DisplayName("finds a chain in the hash index by a segment or an array, whichever way the chain holds its key")
    void indexLookups() {
        var random = new SplittableRandom(17);
        var index = new ChainIndex();
        var slab = new KeySlab();
        var held = new ArrayList<Chain>();
        var bytes = new ArrayList<byte[]>();
        for (var i = 0; i < 5_000; i++) {
            var key = bytes(random, random.nextInt(1, LONGEST + 1));
            var forms = forms(slab, key, random);
            var chain = forms.get(random.nextInt(forms.size()));
            if (index.putIfAbsent(chain) == null) {
                held.add(chain);
                bytes.add(key);
            }
        }
        for (var i = 0; i < held.size(); i++) {
            var key = bytes.get(i);
            var hash = Segments.hash(MemorySegment.ofArray(key));
            for (var segment : segments(key)) {
                assertSame(held.get(i), index.get(segment, hash));
            }
            assertSame(held.get(i), index.get(key.clone(), hash));
            // Another form of the same key is the same key to the index
            for (var other : forms(slab, key, random)) {
                assertSame(held.get(i), index.putIfAbsent(other));
            }
        }
    }
}
