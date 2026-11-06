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
                        new byte[random.nextInt(24)]));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shapes")
    @DisplayName("orders chains as the comparator does, chains of equal keys in the order they came in")
    void matchesComparator(String shape, Function<SplittableRandom, byte[]> keys) {
        var random = new SplittableRandom(shape.hashCode());
        for (var size : new int[]{0, 1, 2, 47, 48, 49, 500, 20_000}) {
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
            ChainSort.sort(chains);
            assertArrayEquals(expected, chains, shape + ", " + size + " chains");
        }
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
