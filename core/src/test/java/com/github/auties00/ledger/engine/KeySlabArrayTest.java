package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The chain [KeySlab#chain(byte\[\], int, int, int)] makes of a key where it lies in a larger array, as the resident
/// loader installs keys from its leaves, against the chain [KeySlab#chain(MemorySegment, int)] makes of a segment of the
/// same bytes, for every length class: the chain's own words up to 8, 16, 24 and 32 bytes, a slab chunk, and an array of
/// its own past the longest a chunk takes.
class KeySlabArrayTest {
    // Past the longest key a chunk takes (a sixteenth of 4 KiB), so the last lengths get arrays of their own
    private static final int LONGEST = 300;

    @Test
    @DisplayName("a chain of a key in a larger array holds what a chain of a segment of the same bytes holds")
    void sameChains() {
        var random = new SplittableRandom(0xA22A7L);
        var slab = new KeySlab();
        for (var length = 0; length <= LONGEST; length++) {
            var key = new byte[length];
            random.nextBytes(key);
            var from = 1 + random.nextInt(7);
            var array = new byte[from + length + 3];
            random.nextBytes(array);
            System.arraycopy(key, 0, array, from, length);
            var segment = MemorySegment.ofArray(key.clone());
            var hash = Segments.hash(array, from, length);
            assertEquals(Segments.hash(segment), hash, "length " + length);
            var expected = slab.chain(segment, hash);
            var actual = slab.chain(array, from, length, hash);
            assertEquals(expected.getClass(), actual.getClass(), "length " + length);
            assertEquals(expected.inlineKey(), actual.inlineKey(), "length " + length);
            assertEquals(length, actual.keyLength());
            assertEquals(hash, actual.hash);
            var copied = new byte[length];
            actual.copyKey(copied, 0);
            assertArrayEquals(key, copied, "length " + length);
            assertTrue(actual.keyEquals(segment), "length " + length);
            assertTrue(actual.keyEquals(key), "length " + length);
            assertTrue(actual.keyEquals(array, from, length), "length " + length);
            assertEquals(0, actual.compareKey(expected.key()), "length " + length);
            // The chain holds a copy: the array may go back to a pool and be written over
            Arrays.fill(array, (byte) 0x5A);
            actual.copyKey(copied, 0);
            assertArrayEquals(key, copied, "length " + length);
            assertTrue(actual.keyEquals(segment), "length " + length);
        }
    }

    @Test
    @DisplayName("an index of chains made from arrays finds them by segment, by array and by a key in a larger array")
    void lookups() {
        var random = new SplittableRandom(0x10C4L);
        var slab = new KeySlab();
        var index = new ChainIndex();
        var keys = new ArrayList<byte[]>();
        var held = new HashSet<String>();
        var chains = new ArrayList<Chain>();
        for (var length = 0; length <= LONGEST; length++) {
            for (var copy = 0; copy < 3; copy++) {
                var key = new byte[length];
                random.nextBytes(key);
                if (length > 0) {
                    key[0] = (byte) copy;
                }
                var array = new byte[length + 9];
                System.arraycopy(key, 0, array, 5, length);
                var hash = Segments.hash(array, 5, length);
                var chain = slab.chain(array, 5, length, hash);
                if (index.putIfAbsent(chain) == null) {
                    keys.add(key);
                    held.add(HexFormat.of().formatHex(key));
                    chains.add(chain);
                }
            }
        }
        for (var i = 0; i < keys.size(); i++) {
            var key = keys.get(i);
            var hash = Segments.hash(key);
            var chain = chains.get(i);
            assertSame(chain, index.get(MemorySegment.ofArray(key), hash));
            assertSame(chain, index.get(key, hash));
            var array = new byte[key.length + 11];
            System.arraycopy(key, 0, array, 11, key.length);
            assertSame(chain, index.get(array, 11, key.length, hash));
            var other = Arrays.copyOf(key, key.length + 1);
            other[key.length] = (byte) 0xA5;
            if (!held.contains(HexFormat.of().formatHex(other))) {
                assertNull(index.get(other, Segments.hash(other)));
            }
        }
    }

    @Test
    @DisplayName("the key after a key in a larger array is the one after a segment of the same bytes")
    void after() {
        var random = new SplittableRandom(0xAF7E7L);
        for (var length = 0; length <= LONGEST; length += 7) {
            var key = new byte[length];
            random.nextBytes(key);
            var array = new byte[length + 4];
            System.arraycopy(key, 0, array, 2, length);
            var expected = ResidentLoader.after(MemorySegment.ofArray(key));
            var actual = ResidentLoader.after(array, 2, length);
            assertEquals(-1, expected.mismatch(actual));
            assertTrue(actual.isReadOnly());
            assertTrue(Segments.compare(MemorySegment.ofArray(key), actual) < 0);
        }
    }
}
