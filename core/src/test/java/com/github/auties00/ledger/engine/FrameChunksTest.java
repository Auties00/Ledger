package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The bodies of log frames kept on chunks of a pool: the bytes they hold against a body of one array given the same
/// writes, whatever write a chunk boundary falls in, and the chunks they give back, once each.
///
/// Each pool here is a test's own, with room for every chunk it hands out, and no trimmer.
class FrameChunksTest {
    private static final int CHUNK = FrameBody.CHUNK_BYTES;
    private static final PageRef REF = new PageRef(0x01020304, 7, 0x1122334455667788L, 0x99AABBCC);

    /// The writes a boundary is placed in, by name.
    private static final List<Write> WRITES = List.of(
            new Write("a put's key", body -> body.put(5, bytes(40, 1), bytes(100, 2))),
            new Write("a put's value", body -> body.put(5, bytes(3, 3), bytes(300, 4))),
            new Write("a value of several chunks", body -> body.put(5, bytes(3, 3), bytes(2 * CHUNK + 1_000, 5))),
            new Write("a large key space id", body -> body.put(1_000_000_000, bytes(2, 6), bytes(2, 7))),
            new Write("an external put's reference", body -> body.putExternal(5, bytes(3, 8), 300_000, REF)),
            new Write("a removal", body -> body.delete(5, bytes(30, 9))),
            new Write("a range removal", body -> body.deleteRange(5, bytes(17, 10), null)),
            new Write("a catalog entry", body -> body.catalog(300, "a key space with a long name, over a boundary")));

    private record Write(String name, Consumer<FrameBody> apply) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Arguments> boundaries() {
        // From the boundary itself to past the room a write checks for before it, so a write that fits whole before
        // the boundary takes the path that checks no field, at the first and third chunk
        return WRITES.stream().flatMap(write -> IntStream.rangeClosed(0, 40)
                .mapToObj(before -> Arguments.of(write, before, before % 2 == 0 ? 1 : 3)));
    }

    /// {@return `length` bytes counting up from `seed`}
    private static MemorySegment bytes(int length, int seed) {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++) {
            bytes[i] = (byte) (seed * 31 + i);
        }
        return MemorySegment.ofArray(bytes);
    }

    /// Puts to `body` until it holds `size` bytes exactly, with a put whose value takes what is left.
    private static void fillTo(FrameBody body, int size) {
        var needed = size - body.size();
        // Kind, key space 5, key length and a key of one byte, then the value's length and the value
        for (var lengthBytes = 1; lengthBytes <= 3; lengthBytes++) {
            var length = needed - 4 - lengthBytes;
            if (length >= 0 && varIntBytes(length) == lengthBytes) {
                body.put(5, bytes(1, 11), bytes(length, 12));
                assertEquals(size, body.size());
                return;
            }
        }
        throw new AssertionError("no put fills " + needed + " bytes");
    }

    private static int varIntBytes(int value) {
        return value == 0 ? 1 : (38 - Integer.numberOfLeadingZeros(value)) / 7;
    }

    /// Checks that `body` holds `expected`, every chunk but the last full.
    private static void assertHolds(byte[] expected, FrameBody body) {
        assertEquals(expected.length, body.size());
        assertArrayEquals(expected, body.toArray());
        var count = body.chunkCount();
        for (var i = 0; i < count - 1; i++) {
            assertEquals(CHUNK, body.chunkLength(i), "chunk " + i + " of " + count);
            assertEquals(CHUNK, body.chunk(i).length);
        }
        assertTrue(count == 1 || expected.length > CHUNK, () -> count + " chunks for " + expected.length + " bytes");
    }

    /// {@return the arrays the body's bytes are in, by identity}
    private static List<byte[]> arrays(FrameBody body) {
        return IntStream.range(0, body.chunkCount()).mapToObj(body::chunk).toList();
    }

    /// Checks that `pool` holds each of `arrays` free exactly once, and no other array of a chunk.
    private static void assertFreeOnce(HeapPool pool, List<byte[]> arrays) {
        var free = pool.freeForTests(CHUNK);
        var seen = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
        for (var array : free) {
            assertTrue(seen.add(array), "an array is free twice");
        }
        assertEquals(arrays.size(), free.size(), "the free chunks");
        for (var array : arrays) {
            assertTrue(seen.contains(array), "a chunk did not go back");
        }
    }

    @ParameterizedTest(name = "{0}, {1} bytes before chunk {2} ends")
    @MethodSource("boundaries")
    @DisplayName("holds the bytes a body of one array holds, the chunk boundary anywhere in a write")
    void boundary(Write write, int before, int chunk) {
        var pool = new HeapPool(1L << 30, false);
        var chunked = new FrameBody(1_024, pool);
        var plain = new FrameBody(1_024);
        for (var body : List.of(chunked, plain)) {
            fillTo(body, chunk * CHUNK - before);
            write.apply().accept(body);
            // A write after it, which starts wherever the last one ended
            body.put(5, bytes(5, 13), bytes(7, 14));
        }
        assertHolds(Arrays.copyOf(plain.array(), plain.size()), chunked);
    }

    @Test
    @DisplayName("holds the bytes of a random run of writes of 300 KiB, 600 KiB and 1.5 MiB, from any start")
    void randomWrites() {
        var random = new Random(42);
        for (var target : new int[]{300 << 10, 600 << 10, 1_536 << 10}) {
            for (var capacity : new int[]{16, 256, CHUNK / 2, CHUNK, target}) {
                var pool = new HeapPool(1L << 30, false);
                var chunked = new FrameBody(capacity, pool);
                var plain = new FrameBody(16);
                while (plain.size() < target) {
                    var write = WRITES.get(random.nextInt(WRITES.size()));
                    var key = bytes(1 + random.nextInt(64), random.nextInt(100));
                    var value = bytes(random.nextInt(3_000), random.nextInt(100));
                    var keyspace = random.nextInt(1 << 20);
                    var listed = random.nextInt(4) == 0;
                    for (var body : List.of(chunked, plain)) {
                        if (listed) {
                            write.apply().accept(body);
                        } else {
                            body.put(keyspace, key, value);
                        }
                    }
                }
                assertHolds(Arrays.copyOf(plain.array(), plain.size()), chunked);
                var arrays = arrays(chunked);
                chunked.release();
                assertFreeOnce(pool, arrays);
            }
        }
    }

    @Test
    @DisplayName("a body started on a chunk that ends small is one array, which goes back as a run takes it")
    void overestimated() {
        var pool = new HeapPool(1L << 30, false);
        var body = new FrameBody(4 * CHUNK, pool);
        body.put(1, bytes(10, 1), bytes(1_000, 2));
        assertEquals(1, body.chunkCount());
        assertEquals(CHUNK, body.array().length);
        var plain = new FrameBody(16);
        plain.put(1, bytes(10, 1), bytes(1_000, 2));
        var entry = new byte[FrameBody.runEntryBytes(body.size())];
        var expected = new byte[entry.length];
        assertEquals(entry.length, body.writeRunEntry(entry, 0));
        plain.writeRunEntry(expected, 0);
        assertArrayEquals(expected, entry);
        var array = body.array();
        body.handOff();
        body.release();
        assertFreeOnce(pool, List.of(array));
    }

    @Test
    @DisplayName("a chunked body serves no single array, and a released one no bytes at all")
    void accessors() {
        var pool = new HeapPool(1L << 30, false);
        var body = new FrameBody(256, pool);
        fillTo(body, CHUNK + 100);
        assertEquals(2, body.chunkCount());
        assertThrows(IllegalStateException.class, body::array);
        assertThrows(IllegalStateException.class, () -> body.writeRunEntry(new byte[2 * CHUNK], 0));
        body.handOff();
        body.release();
        assertThrows(IllegalStateException.class, body::chunkCount);
        assertThrows(IllegalStateException.class, () -> body.chunk(0));
        assertThrows(IllegalStateException.class, () -> body.chunkLength(0));
        assertThrows(IllegalStateException.class, body::array);
        assertEquals(CHUNK + 100, body.size());
    }

    @Test
    @DisplayName("gives its chunks back once, however often it is released or discarded")
    void releasesOnce() {
        var pool = new HeapPool(1L << 30, false);
        var body = new FrameBody(CHUNK, pool);
        fillTo(body, 3 * CHUNK + 5);
        var arrays = arrays(body);
        assertEquals(4, arrays.size());
        body.handOff();
        // The log took it: a commit taken back after leaves it to the log
        body.discard();
        assertEquals(0, pool.freeForTests(CHUNK).size());
        body.release();
        body.release();
        body.discard();
        assertFreeOnce(pool, arrays);
        // The chunks come back with the pattern a released chunk is filled with, with assertions on
        var poisoned = new byte[CHUNK];
        Arrays.fill(poisoned, (byte) 0x5A);
        var assertions = false;
        assert assertions = true;
        if (assertions) {
            for (var array : arrays) {
                assertArrayEquals(poisoned, array);
            }
        }
    }

    @Test
    @DisplayName("a body taken back before the log took it gives its chunks back at once")
    void discarded() {
        var pool = new HeapPool(1L << 30, false);
        var body = new FrameBody(256, pool);
        fillTo(body, 2 * CHUNK + 1);
        var arrays = arrays(body);
        body.discard();
        body.discard();
        assertFreeOnce(pool, arrays);
        // A second body takes the same chunks back from the pool, and they hold its bytes alone
        var next = new FrameBody(CHUNK, pool);
        fillTo(next, 2 * CHUNK + 1);
        var plain = new FrameBody(16);
        fillTo(plain, 2 * CHUNK + 1);
        assertHolds(Arrays.copyOf(plain.array(), plain.size()), next);
        for (var array : arrays(next)) {
            assertTrue(arrays.stream().anyMatch(released -> released == array), "a chunk was not recycled");
        }
    }

    @Test
    @DisplayName("a body made without a pool keeps its one array, which its release leaves readable")
    void unpooled() {
        var body = new FrameBody(16);
        fillTo(body, 3 * CHUNK);
        var array = body.array();
        body.handOff();
        body.release();
        assertSame(array, body.array());
        assertEquals(1, body.chunkCount());
    }
}
