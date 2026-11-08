package com.github.auties00.ledger.engine;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The sort of a checkpoint's dirty chains and the page counts of its delta records, against a comparison sort and a
/// boxed map.
class ChainSortTest {
    static Stream<Arguments> shapes() {
        return Stream.of(
                Arguments.of("random 16-byte keys", (Function<SplittableRandom, byte[]>) random -> bytes(random, 16)),
                Arguments.of("keys of 0 to 12 bytes over a 3-letter alphabet", (Function<SplittableRandom, byte[]>) random -> {
                    var key = new byte[random.nextInt(13)];
                    for (var i = 0; i < key.length; i++) {
                        key[i] = (byte) random.nextInt(3);
                    }
                    return key;
                }),
                Arguments.of("a chat id of 40 bytes before a message id", (Function<SplittableRandom, byte[]>) random -> {
                    var chat = random.nextInt(4);
                    var key = new byte[40 + 1 + 8];
                    Arrays.fill(key, 0, 40, (byte) ('a' + chat));
                    var id = bytes(random, 8);
                    System.arraycopy(id, 0, key, 41, random.nextInt(9));
                    return Arrays.copyOf(key, 41 + random.nextInt(9));
                }),
                Arguments.of("keys ending in runs of zero bytes", (Function<SplittableRandom, byte[]>) random ->
                        new byte[random.nextInt(24)]),
                Arguments.of("one chat id of 20 bytes before a message id, so every first word is the same", (Function<SplittableRandom, byte[]>) random -> {
                    var key = new byte[20 + random.nextInt(9)];
                    Arrays.fill(key, 0, 20, (byte) 0xC3);
                    var id = bytes(random, key.length - 20);
                    System.arraycopy(id, 0, key, 20, id.length);
                    return key;
                }),
                Arguments.of("first bytes on both sides of 0x80, before 8 and 16 shared bytes or short ends", (Function<SplittableRandom, byte[]>) random -> {
                    var first = new byte[]{0x00, 0x7F, (byte) 0x80, (byte) 0xFF}[random.nextInt(4)];
                    var key = new byte[1 + random.nextInt(26)];
                    Arrays.fill(key, first);
                    if (key.length > 17) {
                        key[17] = (byte) random.nextInt(3);
                    }
                    if (key.length > 25) {
                        key[25] = (byte) random.nextInt(3);
                    }
                    return key;
                }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    @DisplayName("orders chains as the comparator does, chains of equal keys in the order they came in, and returns their first words")
    void matchesComparator(String shape, Function<SplittableRandom, byte[]> keys) {
        var random = new SplittableRandom(shape.hashCode());
        // The last two sizes straddle the size from which the sort runs on every processor
        for (var size : new int[]{0, 1, 2, 47, 48, 49, 500, 20_000, (1 << 16) - 1, 1 << 16, 90_000}) {
            var chains = new Chain[size];
            // Keys in shared chunks, at offsets past 0, beside keys of their own
            var slab = new KeySlab();
            for (var i = 0; i < size; i++) {
                // Some keys twice, as a dropped chain and the one that replaced it
                var key = i > 0 && random.nextInt(8) == 0 ? Segments.toBytes(chains[random.nextInt(i)].key()) : keys.apply(random);
                var segment = MemorySegment.ofArray(key);
                chains[i] = random.nextBoolean() ? slab.chain(segment, Segments.hash(segment))
                        : new Chain(segment.asReadOnly(), Segments.hash(segment));
            }
            var expected = chains.clone();
            // A stable sort: chains of equal keys keep their order
            Arrays.sort(expected, Chain.BY_KEY);
            var words = ChainSort.sort(chains);
            assertArrayEquals(expected, chains, shape + ", " + size + " chains");
            assertEquals(size, words.length);
            for (var i = 0; i < size; i++) {
                assertEquals(chains[i].word(0), words[i], shape + ", " + size + " chains, chain " + i);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    @DisplayName("the first words of two different keys, as unsigned numbers, order them as the comparator does, and an order is never read off equal words")
    void wordsOrderKeys(String shape, Function<SplittableRandom, byte[]> keys) {
        var random = new SplittableRandom(shape.hashCode() * 31L);
        for (var i = 0; i < 100_000; i++) {
            var left = MemorySegment.ofArray(keys.apply(random));
            var right = MemorySegment.ofArray(keys.apply(random));
            var leftWord = Segments.word(left, 0, left.byteSize());
            var rightWord = Segments.word(right, 0, right.byteSize());
            assertEquals(new Chain(left.asReadOnly(), 0).word(0), leftWord);
            if (leftWord != rightWord) {
                assertEquals(Integer.signum(Segments.compare(left, right)), Integer.signum(Long.compareUnsigned(leftWord, rightWord)));
            }
        }
        // Zero padding makes these words equal, though the keys differ
        assertEquals(Segments.word(MemorySegment.ofArray(new byte[]{1}), 0, 1), Segments.word(MemorySegment.ofArray(new byte[]{1, 0}), 0, 2));
    }

    @Test
    @DisplayName("a failure on a worker of a sort on every processor reaches the caller")
    void workerFailure() {
        var chains = new Chain[1 << 17];
        for (var i = 0; i < chains.length; i++) {
            var key = MemorySegment.ofArray(new byte[]{(byte) i, (byte) (i >>> 8), (byte) (i >>> 16)}).asReadOnly();
            chains[i] = new Chain(key, 0);
        }
        chains[chains.length / 2] = null;
        assertThrows(NullPointerException.class, () -> ChainSort.sort(chains));
    }

    @Test
    @DisplayName("counts records by page as a boxed map does")
    void pageCounts() {
        var random = new SplittableRandom(7);
        var counts = new PageCounts();
        var other = new PageCounts();
        var expected = new java.util.TreeMap<Integer, Integer>();
        for (var i = 0; i < 50_000; i++) {
            var page = random.nextInt(i < 25_000 ? 3_000 : 300_000);
            var change = random.nextInt(5) - 2;
            (random.nextBoolean() ? counts : other).add(page, change);
            expected.merge(page, change, Integer::sum);
        }
        counts.addAll(other);
        var pages = counts.pages();
        assertArrayEquals(expected.keySet().stream().mapToInt(Integer::intValue).toArray(), pages);
        for (var page : pages) {
            assertEquals(expected.get(page), counts.count(page), "page " + page);
        }
        counts.clear();
        assertTrue(counts.isEmpty());
        assertEquals(0, counts.count(pages[0]));
        counts.add(0, 3);
        assertEquals(List.of(0), Arrays.stream(counts.pages()).boxed().collect(java.util.stream.Collectors.toCollection(ArrayList::new)));
    }

    private static byte[] bytes(SplittableRandom random, int length) {
        var bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
