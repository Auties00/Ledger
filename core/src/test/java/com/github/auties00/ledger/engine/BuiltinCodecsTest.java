package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BuiltinCodecsTest {
    private static <K> byte[] encode(KeyCodec<K> codec, K key) {
        var bytes = new byte[codec.sizeOf(key)];
        codec.encode(key, MemorySegment.ofArray(bytes));
        return bytes;
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ascii", "àèìòù", "日本語", "emoji 🌟", "unpaired \uD800 high", "unpaired \uDC00 low", "\uD800"})
    @DisplayName("UTF-8 encodes in place exactly as String.getBytes does")
    void utf8MatchesJdk(String text) {
        var expected = text.getBytes(StandardCharsets.UTF_8);
        assertEquals(expected.length, ValueCodec.utf8().sizeOf(text));
        var encoded = new byte[expected.length];
        ValueCodec.utf8().encode(text, MemorySegment.ofArray(encoded));
        assertArrayEquals(expected, encoded);
        assertEquals(new String(expected, StandardCharsets.UTF_8), ValueCodec.utf8().decode(MemorySegment.ofArray(encoded).asReadOnly()));
    }

    @Test
    @DisplayName("integer keys sort numerically as unsigned bytes, negative values first")
    void integerOrder() {
        var longs = List.of(Long.MIN_VALUE, -1_000L, -1L, 0L, 1L, 255L, 256L, Long.MAX_VALUE);
        for (var i = 1; i < longs.size(); i++) {
            assertTrue(Arrays.compareUnsigned(encode(KeyCodec.int64(), longs.get(i - 1)), encode(KeyCodec.int64(), longs.get(i))) < 0);
        }
        var ints = List.of(Integer.MIN_VALUE, -1, 0, 1, 128, Integer.MAX_VALUE);
        for (var i = 1; i < ints.size(); i++) {
            assertTrue(Arrays.compareUnsigned(encode(KeyCodec.int32(), ints.get(i - 1)), encode(KeyCodec.int32(), ints.get(i))) < 0);
        }
        assertEquals(-1_000L, KeyCodec.int64().decode(MemorySegment.ofArray(encode(KeyCodec.int64(), -1_000L))));
        assertEquals(128, KeyCodec.int32().decode(MemorySegment.ofArray(encode(KeyCodec.int32(), 128))));
    }

    @Test
    @DisplayName("a batch writes only its store's key spaces, reads its writes in place and keeps a small batch small")
    void batch() {
        var opened = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
        var other = Keyspace.paged("other", KeyCodec.utf8(), ValueCodec.bytes());
        var batch = new ArrayWriteBatch(this, new HeapPool(1 << 20), Map.of(opened.name(), opened));
        assertThrows(IllegalArgumentException.class, () -> batch.put(other, "k", new byte[1]));
        assertThrows(IllegalArgumentException.class, () -> batch.put(opened, "", new byte[1]));
        batch.put(opened, "key", new byte[]{1, 2, 3})
                .delete(opened, "gone")
                .deleteRange(opened, KeyRange.between("a", "b"));
        assertEquals(3, batch.size());
        assertEquals("key", new String(batch.key(0).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
        assertArrayEquals(new byte[]{1, 2, 3}, batch.value(0).toArray(ValueLayout.JAVA_BYTE));
        assertEquals(ArrayWriteBatch.DELETE, batch.kind(1));
        assertEquals(null, batch.value(1));
        assertEquals("b", new String(batch.value(2).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
        var encoded = batch.encodedBytes();
        // Below the pool's smallest array, the batch grew an array of its own by doubling from its first write
        var array = batch.viewForTests().byteSize();
        assertTrue(array >= encoded && array < 2 * encoded, () -> array + " bytes for " + encoded);
        batch.prepare(false, null, Map.of());
        assertEquals(encoded, batch.home(0).byteSize());
        batch.finish();
        assertTrue(batch.isEmpty());
    }

    @Test
    @DisplayName("an of codec decodes a copy it may keep; an ofTrusted codec reads the store's memory in place")
    void trustModel() {
        var kept = new MemorySegment[1];
        var storeMemory = new byte[]{1, 2, 3};
        var segment = MemorySegment.ofArray(storeMemory).asReadOnly();

        var copying = ValueCodec.<String>of(value -> 0, (value, target) -> { }, source -> {
            kept[0] = source;
            return "";
        });
        copying.decode(segment);
        storeMemory[0] = 9;
        assertEquals(1, kept[0].get(ValueLayout.JAVA_BYTE, 0));
        assertTrue(kept[0].isReadOnly());

        var trusting = ValueCodec.<String>ofTrusted(value -> 0, (value, target) -> { }, source -> {
            kept[0] = source;
            return "";
        });
        trusting.decode(segment);
        storeMemory[0] = 7;
        assertEquals(7, kept[0].get(ValueLayout.JAVA_BYTE, 0));
        assertTrue(ValueCodec.bytes().trusted() && KeyCodec.utf8().trusted());
    }

    @Test
    @DisplayName("the pool recycles arrays of its size classes and leaves others to the collector")
    void pool() {
        var pool = new HeapPool(1 << 20);
        var array = pool.acquire(5_000);
        assertEquals(8_192, array.length);
        pool.release(array);
        assertSame(array, pool.acquire(6_000));
        var large = pool.acquire((1 << 20) + 1);
        assertEquals((1 << 20) + 1, large.length);
    }
}
