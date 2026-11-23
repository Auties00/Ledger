package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Tree cursors over ranges whose far bound falls on, just before or just after the first or last key of a leaf, in
/// both directions, over trees of many leaves with inline values, values in blobs of the data file and both: a cursor
/// skips the bound compare inside a leaf whose last entry in its order is within the range, so each such range must
/// pass exactly the entries a sorted map holds in it. Each tree is written straight through a [TreeUpdate].
class LeafBoundScanTest {
    private static final Comparator<byte[]> UNSIGNED = Arrays::compareUnsigned;
    private static final byte[] KEY = new byte[32];

    enum Values {INLINE, BLOB, MIXED}

    @ParameterizedTest(name = "{0}")
    @EnumSource(Values.class)
    @DisplayName("a range bounded at a leaf's first or last key passes exactly the entries in it, either way")
    void boundsAtLeafEdges(Values values, @TempDir Path dir) throws IOException {
        var random = new Random(values.ordinal() * 7919L + 17);
        var pages = PageFile.open(dir, new PageCipher(new SecretKeySpec(KEY, "AES")), 64L << 20, new HeapPool(16L << 20, false));
        try {
            var model = new TreeMap<byte[], byte[]>(UNSIGNED);
            for (var i = 0; i < 1_500; i++) {
                // Keys of different lengths, so a bound one byte past a key compares a longer key with a shorter one
                var key = ("k%05d".formatted(i * 3) + "-".repeat(random.nextInt(4))).getBytes(StandardCharsets.US_ASCII);
                model.put(key, value(values, random));
            }
            var root = write(pages, model);
            var lasts = leafLasts(pages, root);
            assertTrue(lasts.size() > 10, "the tree has too few leaves to test: " + lasts.size());
            var keys = new ArrayList<>(model.keySet());
            var bounds = new ArrayList<byte[]>();
            for (var last : lasts) {
                var position = indexOf(keys, last);
                bounds.add(last);
                bounds.add(append(last));
                bounds.add(increment(last));
                if (position + 1 < keys.size()) {
                    var first = keys.get(position + 1);
                    bounds.add(first);
                    bounds.add(append(first));
                }
            }
            bounds.add(keys.getFirst());
            bounds.add(keys.getLast());
            bounds.add(append(keys.getLast()));
            var checked = 0;
            for (var bound : bounds) {
                var position = Math.max(0, lowerIndex(keys, bound) - 40);
                for (var near : new byte[][]{null, keys.get(position)}) {
                    for (var wanted : new long[]{0, 3, Long.MAX_VALUE}) {
                        // Ascending, the bound is `to`, excluded
                        if (near == null || UNSIGNED.compare(near, bound) < 0) {
                            check(pages, root, model, near, bound, false, wanted);
                            checked++;
                        }
                        // Descending, the bound is `from`, included, and the near bound past it
                        var high = near == null ? null : keys.get(Math.min(keys.size() - 1, lowerIndex(keys, bound) + 40));
                        if (high == null || UNSIGNED.compare(bound, high) < 0) {
                            check(pages, root, model, bound, high, true, wanted);
                            checked++;
                        }
                    }
                }
            }
            assertTrue(checked > 500, "too few ranges checked: " + checked);
        } finally {
            pages.close();
        }
    }

    /// Walks a cursor over `from`, included, to `to`, excluded, and checks it passes what `model` holds there.
    private static void check(PageFile pages, TreeRoot root, TreeMap<byte[], byte[]> model, byte[] from, byte[] to,
                              boolean descending, long wanted) {
        NavigableMap<byte[], byte[]> expected = from == null && to == null ? model
                : from == null ? model.headMap(to, false)
                : to == null ? model.tailMap(from, true)
                : model.subMap(from, true, to, false);
        if (descending) {
            expected = expected.descendingMap();
        }
        var cursor = new TreeCursor(pages, root, from, to, descending, wanted, true);
        try {
            var range = describe(from, to, descending, wanted);
            for (Map.Entry<byte[], byte[]> entry : expected.entrySet()) {
                assertTrue(cursor.next(), "the cursor ended early over " + range);
                assertArrayEquals(entry.getKey(), cursor.key().toArray(ValueLayout.JAVA_BYTE), range);
                assertEquals(0, cursor.compareKey(MemorySegment.ofArray(entry.getKey())), range);
                assertArrayEquals(entry.getValue(), cursor.value().toArray(ValueLayout.JAVA_BYTE), range);
                assertTrue(cursor.key().isReadOnly() && cursor.value().isReadOnly(), range);
                cursor.took();
            }
            assertFalse(cursor.next(), "the cursor went past " + range);
            assertFalse(cursor.next(), "the cursor went past " + range);
        } finally {
            cursor.release();
        }
    }

    private static String describe(byte[] from, byte[] to, boolean descending, long wanted) {
        return "[" + text(from) + ", " + text(to) + ") descending=" + descending + " wanted=" + wanted;
    }

    private static String text(byte[] key) {
        return key == null ? "null" : Arrays.toString(key);
    }

    private static TreeRoot write(PageFile pages, TreeMap<byte[], byte[]> model) throws IOException {
        var ops = new ArrayList<TreeUpdate.Op>(model.size());
        model.forEach((key, value) -> ops.add(new TreeUpdate.Op(MemorySegment.ofArray(key.clone()).asReadOnly(),
                MemorySegment.ofArray(value.clone()).asReadOnly())));
        var writer = pages.writer(1);
        var update = new TreeUpdate(pages, writer, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet()));
        var root = update.apply(null, ops, TreeUpdate.merge(List.of()));
        update.finishDeltas(DeltaTable.EMPTY);
        writer.flush();
        return root;
    }

    /// {@return the last key of each leaf of the tree under `root`, in key order}
    private static List<byte[]> leafLasts(PageFile pages, TreeRoot root) {
        var lasts = new ArrayList<byte[]>();
        var cursor = new TreeCursor(pages, root, null, null, false);
        while (cursor.next()) {
            if (cursor.lastOfLeaf()) {
                lasts.add(cursor.key().toArray(ValueLayout.JAVA_BYTE));
            }
        }
        return lasts;
    }

    private static byte[] value(Values values, Random random) {
        var blob = switch (values) {
            case INLINE -> false;
            case BLOB -> true;
            case MIXED -> random.nextInt(3) == 0;
        };
        var value = new byte[blob ? 2_500 + random.nextInt(6_000) : 10 + random.nextInt(300)];
        random.nextBytes(value);
        return value;
    }

    private static int indexOf(List<byte[]> keys, byte[] key) {
        var index = lowerIndex(keys, key);
        assertTrue(index < keys.size() && Arrays.equals(keys.get(index), key));
        return index;
    }

    /// {@return the index of the first of `keys` at or after `key`, or their count}
    private static int lowerIndex(List<byte[]> keys, byte[] key) {
        var low = 0;
        var high = keys.size();
        while (low < high) {
            var middle = (low + high) >>> 1;
            if (UNSIGNED.compare(keys.get(middle), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    /// {@return the key right after `key`: `key` with a zero byte appended}
    private static byte[] append(byte[] key) {
        return Arrays.copyOf(key, key.length + 1);
    }

    /// {@return `key` with its last byte one higher, which the keys here never end with 0xFF}
    private static byte[] increment(byte[] key) {
        var next = key.clone();
        next[next.length - 1]++;
        return next;
    }
}
