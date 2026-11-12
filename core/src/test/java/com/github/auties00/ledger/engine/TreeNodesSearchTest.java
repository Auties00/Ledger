package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The searches of a node by an array key, which load the lines of the probes ahead of their compares, against a linear
/// scan, the searches by a segment key and the plain binary searches they replace, on leaves and branches that
/// [TreeNodes#writeLeaf(List, int, int, int)] and [TreeNodes#writeBranch(List, int, int, int)] build: in arrays of
/// exactly their content, as the page cache keeps trimmed units, and padded to a whole page.
class TreeNodesSearchTest {
    // Both sides of the node size up to which a leaf loads every key up front, and of a second level of lookahead
    private static final int[] COUNTS = {0, 1, 2, 3, 15, 16, 17, 31, 32, 33, 64, 577};

    private enum Shape {
        // A chat and a message id, as the message id key space keys them: a shared prefix decided past the first line
        IDS,
        // Random bytes of random lengths
        RANDOM,
        // A small alphabet over short lengths, so that many keys are prefixes of others
        PREFIXES,
        // Keys of one or two bytes, with a few of the largest length among them, few enough to keep a node's offsets in
        // two bytes
        EDGES
    }

    static Stream<Arguments> nodes() {
        var arguments = new ArrayList<Arguments>();
        for (var shape : Shape.values()) {
            for (var count : COUNTS) {
                arguments.add(Arguments.of(shape, count));
            }
        }
        return arguments.stream();
    }

    @ParameterizedTest(name = "{0} keys, {1} entries")
    @MethodSource("nodes")
    @DisplayName("finds the same entry of a leaf as a linear scan and the plain searches, for keys at, between, below, above and prefixes of its keys")
    void leaves(Shape shape, int count) {
        var random = new SplittableRandom(31L * shape.ordinal() + count);
        var keys = keys(shape, count, random);
        var entries = new ArrayList<TreeNodes.LeafEntry>(keys.size());
        for (var i = 0; i < keys.size(); i++) {
            entries.add(leafEntry(keys.get(i), i, keys.size(), random));
        }
        var sizes = new int[entries.size()];
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = entries.get(i).size();
        }
        var exact = TreeNodes.writeLeaf(entries, 0, entries.size(), TreeNodes.contentLength(sizes, 0, sizes.length));
        for (var unit : List.of(exact, padded(exact))) {
            assertEquals(keys.size(), TreeNodes.count(unit));
            for (var probe : probes(keys, random)) {
                var expected = linearLowerBound(keys, probe);
                var name = shape + " " + count + " " + java.util.HexFormat.of().formatHex(probe);
                assertEquals(expected, TreeNodes.lowerBound(unit, probe), "array: " + name);
                assertEquals(expected, TreeNodes.lowerBound(unit, MemorySegment.ofArray(probe)), "segment: " + name);
                assertEquals(expected, plainLowerBound(unit, probe), "plain: " + name);
            }
        }
    }

    @ParameterizedTest(name = "{0} keys, {1} children")
    @MethodSource("nodes")
    @DisplayName("finds the same child of a branch as a linear scan and the plain searches, with children of their own units and delta records")
    void branches(Shape shape, int count) {
        var random = new SplittableRandom(17L * shape.ordinal() + count + 1);
        var keys = keys(shape, count, random);
        var children = new ArrayList<TreeNodes.Child>(keys.size());
        for (var key : keys) {
            var ref = new PageRef(random.nextInt(1, Integer.MAX_VALUE), 1, random.nextLong(), random.nextInt(),
                    random.nextBoolean() ? -1 : random.nextInt(0, 1 << 16));
            children.add(new TreeNodes.Child(MemorySegment.ofArray(key), ref, random.nextLong(1, 1L << 40),
                    random.nextBoolean(), 0, -1));
        }
        var sizes = new int[children.size()];
        for (var i = 0; i < sizes.length; i++) {
            sizes[i] = children.get(i).size();
        }
        var exact = TreeNodes.writeBranch(children, 0, children.size(), TreeNodes.contentLength(sizes, 0, sizes.length));
        for (var unit : List.of(exact, padded(exact))) {
            assertEquals(keys.size(), TreeNodes.count(unit));
            for (var i = 0; i < children.size(); i++) {
                assertEquals(children.get(i).ref(), TreeNodes.childRef(unit, i));
            }
            for (var probe : probes(keys, random)) {
                var name = shape + " " + count + " " + java.util.HexFormat.of().formatHex(probe);
                var expectedChild = linearChildFor(keys, probe);
                assertEquals(expectedChild, TreeNodes.childFor(unit, probe), "array: " + name);
                assertEquals(expectedChild, TreeNodes.childFor(unit, MemorySegment.ofArray(probe)), "segment: " + name);
                assertEquals(expectedChild, plainChildFor(unit, probe), "plain: " + name);
                var expectedBefore = linearLastChildBefore(keys, probe);
                assertEquals(expectedBefore, TreeNodes.lastChildBefore(unit, probe), "before, array: " + name);
                assertEquals(expectedBefore, plainLastChildBefore(unit, probe), "before, plain: " + name);
            }
        }
    }

    @Test
    @DisplayName("keeps the loads of a search behind a value no byte reaches, and never stores to the sink")
    void sentinelIsOutOfReach() throws ReflectiveOperationException {
        var untouched = TreeNodes.class.getDeclaredField("untouched");
        var sink = TreeNodes.class.getDeclaredField("touchSink");
        assertTrue(Modifier.isStatic(untouched.getModifiers()));
        assertFalse(Modifier.isFinal(untouched.getModifiers()), "a final sentinel lets the compiler drop the loads");
        untouched.setAccessible(true);
        sink.setAccessible(true);
        var value = untouched.getInt(null);
        assertTrue(value < Byte.MIN_VALUE || value > Byte.MAX_VALUE, "the XOR of sign-extended bytes reaches " + value);
        var random = new SplittableRandom(7);
        var keys = keys(Shape.RANDOM, 64, random);
        var entries = new ArrayList<TreeNodes.LeafEntry>();
        var sizes = new int[keys.size()];
        for (var i = 0; i < keys.size(); i++) {
            entries.add(leafEntry(keys.get(i), i, keys.size(), random));
            sizes[i] = entries.get(i).size();
        }
        var unit = TreeNodes.writeLeaf(entries, 0, entries.size(), TreeNodes.contentLength(sizes, 0, sizes.length));
        for (var probe : probes(keys, random)) {
            TreeNodes.lowerBound(unit, probe);
        }
        assertEquals(0, sink.getInt(null));
        assertEquals(value, untouched.getInt(null));
    }

    // Sorted, distinct keys of the shape
    private static List<byte[]> keys(Shape shape, int count, SplittableRandom random) {
        var keys = new TreeSet<byte[]>(Arrays::compareUnsigned);
        var attempts = 0;
        while (keys.size() < count && attempts++ < count * 1_000) {
            keys.add(switch (shape) {
                case IDS -> {
                    var key = new byte[24];
                    var chat = random.nextInt(3);
                    key[3] = (byte) chat;
                    key[4] = '3';
                    key[5] = 'E';
                    key[6] = 'B';
                    key[7] = '0';
                    for (var i = 8; i < key.length; i++) {
                        key[i] = (byte) "0123456789ABCDEF".charAt(random.nextInt(16));
                    }
                    yield key;
                }
                case RANDOM -> {
                    var key = new byte[random.nextInt(1, 41)];
                    random.nextBytes(key);
                    yield key;
                }
                case PREFIXES -> {
                    var key = new byte[random.nextInt(1, 21)];
                    for (var i = 0; i < key.length; i++) {
                        key[i] = (byte) switch (random.nextInt(3)) {
                            case 0 -> 0;
                            case 1 -> 1;
                            default -> 0xFF;
                        };
                    }
                    yield key;
                }
                case EDGES -> {
                    var key = new byte[random.nextInt(Math.max(8, count / 4)) == 0 ? KeyCodec.MAX_LENGTH - random.nextInt(2) : random.nextInt(1, 3)];
                    random.nextBytes(key);
                    yield key;
                }
            });
        }
        return new ArrayList<>(keys);
    }

    // An inline value, a blob, a unit of the blob file, or a removal, with the last entry a removal so that a short key
    // ends the unit
    private static TreeNodes.LeafEntry leafEntry(byte[] key, int index, int count, SplittableRandom random) {
        var segment = MemorySegment.ofArray(key);
        if (index == count - 1 && random.nextBoolean()) {
            return TreeNodes.LeafEntry.removed(segment);
        }
        return switch (random.nextInt(8)) {
            case 0 -> TreeNodes.LeafEntry.removed(segment);
            case 1 -> new TreeNodes.LeafEntry(segment, null, new PageRef(random.nextInt(1, 1 << 20), 2, random.nextLong(), random.nextInt()),
                    random.nextInt(1, 1 << 20), random.nextBoolean());
            default -> {
                var value = new byte[random.nextInt(0, 40)];
                random.nextBytes(value);
                yield new TreeNodes.LeafEntry(segment, MemorySegment.ofArray(value), null, value.length);
            }
        };
    }

    // The keys themselves and keys around each of them, below the first and above the last, and random keys
    private static List<byte[]> probes(List<byte[]> keys, SplittableRandom random) {
        var probes = new ArrayList<byte[]>();
        probes.add(new byte[]{0});
        var top = new byte[KeyCodec.MAX_LENGTH];
        Arrays.fill(top, (byte) 0xFF);
        probes.add(top);
        probes.add(new byte[]{(byte) 0xFF});
        for (var key : keys) {
            probes.add(key.clone());
            probes.add(Arrays.copyOf(key, key.length + 1));
            var after = Arrays.copyOf(key, key.length + 1);
            after[key.length] = (byte) 0xFF;
            probes.add(after);
            if (key.length > 1) {
                probes.add(Arrays.copyOf(key, key.length - 1));
                probes.add(Arrays.copyOf(key, 1));
            }
            var lower = key.clone();
            lower[lower.length - 1]--;
            probes.add(lower);
            var higher = key.clone();
            higher[higher.length - 1]++;
            probes.add(higher);
        }
        for (var i = 0; i < 64; i++) {
            var probe = new byte[random.nextInt(1, 30)];
            random.nextBytes(probe);
            probes.add(probe);
        }
        return probes;
    }

    // The unit in an array of a whole page, its padding nonzero, as a unit the page cache keeps whole
    private static byte[] padded(byte[] unit) {
        var padded = new byte[Math.max(unit.length, PageFormat.SIZE)];
        Arrays.fill(padded, (byte) 0x5A);
        System.arraycopy(unit, 0, padded, 0, unit.length);
        return padded;
    }

    private static int linearLowerBound(List<byte[]> keys, byte[] probe) {
        var index = 0;
        while (index < keys.size() && Arrays.compareUnsigned(keys.get(index), probe) < 0) {
            index++;
        }
        return index;
    }

    private static int linearChildFor(List<byte[]> keys, byte[] probe) {
        var index = 1;
        while (index < keys.size() && Arrays.compareUnsigned(keys.get(index), probe) <= 0) {
            index++;
        }
        return index - 1;
    }

    private static int linearLastChildBefore(List<byte[]> keys, byte[] probe) {
        var index = 0;
        while (index < keys.size() && Arrays.compareUnsigned(keys.get(index), probe) < 0) {
            index++;
        }
        return index - 1;
    }

    // The binary searches without lookahead, as they read before it
    private static int plainLowerBound(byte[] unit, byte[] key) {
        var low = 0;
        var high = TreeNodes.count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            var entry = TreeNodes.entry(unit, middle);
            if (TreeNodes.compare(unit, TreeNodes.leafKeyOffset(entry), TreeNodes.leafKeyLength(unit, entry), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    private static int plainChildFor(byte[] unit, byte[] key) {
        var low = 1;
        var high = TreeNodes.count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            if (compareChild(unit, middle, key) <= 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low - 1;
    }

    private static int plainLastChildBefore(byte[] unit, byte[] key) {
        var low = 0;
        var high = TreeNodes.count(unit);
        while (low < high) {
            var middle = (low + high) >>> 1;
            if (compareChild(unit, middle, key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low - 1;
    }

    private static int compareChild(byte[] unit, int index, byte[] key) {
        var childKey = TreeNodes.childKey(unit, index);
        return Segments.compare(childKey, MemorySegment.ofArray(key));
    }
}
