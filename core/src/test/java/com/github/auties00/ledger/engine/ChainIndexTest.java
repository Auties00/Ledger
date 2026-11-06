package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/// The hash index of a key space's chains, against a map of the JDK, alone and under concurrent writers and readers.
class ChainIndexTest {
    private static Chain chain(long key) {
        var bytes = MemorySegment.ofArray(ByteBuffer.allocate(12).putLong(key).putInt((int) (key * 31)).array());
        return new Chain(bytes, Segments.hash(bytes));
    }

    private static MemorySegment keyOf(long key) {
        return MemorySegment.ofArray(ByteBuffer.allocate(12).putLong(key).putInt((int) (key * 31)).array());
    }

    @Test
    @DisplayName("finds, adds once and removes by identity as a map of the chains would, through growth and tombstones")
    void model() {
        var index = new ChainIndex();
        var model = new HashMap<Long, Chain>();
        var random = new SplittableRandom(5);
        for (var step = 0; step < 400_000; step++) {
            var key = (long) random.nextInt(20_000);
            switch (random.nextInt(3)) {
                case 0 -> {
                    var created = chain(key);
                    var held = index.putIfAbsent(created);
                    var expected = model.get(key);
                    assertSame(expected, held, "put " + key);
                    if (expected == null) {
                        model.put(key, created);
                    }
                }
                case 1 -> {
                    var held = model.get(key);
                    // A chain of the same key that is not the one held stays
                    index.remove(held != null && random.nextBoolean() ? held : chain(key));
                    if (held != null && index.get(keyOf(key), held.hash) == null) {
                        model.remove(key);
                    }
                }
                default -> {
                    var expected = model.get(key);
                    var bytes = keyOf(key);
                    assertSame(expected, index.get(bytes, Segments.hash(bytes)), "get " + key);
                }
            }
        }
        assertEquals(model.size(), index.size());
        for (var entry : model.entrySet()) {
            var bytes = keyOf(entry.getKey());
            assertSame(entry.getValue(), index.get(bytes, Segments.hash(bytes)));
        }
        var absent = keyOf(-1);
        assertNull(index.get(absent, Segments.hash(absent)));
    }

    @Test
    @DisplayName("adds once and removes by identity with colliding hashes, keys of 1 to 11 bytes, probes that wrap and tables that shrink")
    void strictModel() {
        var random = new SplittableRandom(7);
        for (var trial = 0; trial < 10; trial++) {
            var index = new ChainIndex();
            var model = new HashMap<String, Chain>();
            // A few hashes for every key: equal hashes of different keys, probes from the last slots of a table
            var forced = new int[]{0x0000FFFF, 0x0000FFFE, 0x7C00000F, 0};
            for (var step = 0; step < 150_000; step++) {
                // Phases of many keys and of few, so the segments grow and then shrink
                var many = (step / 30_000) % 2 == 0;
                var id = random.nextInt(many ? 3_000 : 300);
                var length = 1 + id % 11;
                var bytes = new byte[length];
                for (var i = 0; i < length; i++) {
                    bytes[i] = (byte) (id >>> (8 * (i % 4)));
                }
                bytes[length - 1] ^= (byte) (id / 256 + length);
                var name = java.util.HexFormat.of().formatHex(bytes);
                var hash = forced[Math.floorMod(java.util.Arrays.hashCode(bytes), forced.length)];
                var key = MemorySegment.ofArray(bytes).asReadOnly();
                switch (random.nextInt(many ? 3 : 4)) {
                    case 0 -> {
                        var created = new Chain(key, hash);
                        var expected = model.get(name);
                        assertSame(expected, index.putIfAbsent(created), "put " + name);
                        if (expected == null) {
                            model.put(name, created);
                        }
                    }
                    case 1 -> {
                        // Another chain of the same key is not the one held: removing it changes nothing
                        index.remove(new Chain(key, hash));
                        assertSame(model.get(name), index.get(key, hash), "removing another chain of " + name);
                    }
                    default -> {
                        var held = model.remove(name);
                        if (held != null) {
                            index.remove(held);
                        }
                        assertNull(index.get(key, hash), "found " + name + " after its removal");
                    }
                }
                assertEquals(model.size(), index.size());
            }
            for (var held : model.values()) {
                assertSame(held, index.get(held.key, held.hash));
            }
        }
    }

    @Test
    @DisplayName("finds every chain a reader synchronized with the insertion of, while the segments grow")
    void published() throws Exception {
        var writers = 4;
        var index = new ChainIndex();
        var published = new java.util.concurrent.atomic.AtomicLongArray(writers);
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var threads = new ArrayList<Thread>();
        for (var w = 0; w < writers; w++) {
            var writer = w;
            threads.add(Thread.ofPlatform().start(() -> {
                for (var n = 1L; !stop.get() && n <= 1_000_000; n++) {
                    var created = chain(((long) writer << 40) | n);
                    if (index.putIfAbsent(created) != null) {
                        failure.compareAndSet(null, new AssertionError("a new key was held already"));
                        return;
                    }
                    // What a commit's publication does for the chains it wrote
                    published.setRelease(writer, n);
                }
            }));
        }
        for (var r = 0; r < 4; r++) {
            var seed = r;
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new SplittableRandom(seed);
                while (!stop.get()) {
                    var writer = random.nextInt(writers);
                    var last = published.getAcquire(writer);
                    for (var n = last; n > Math.max(0, last - 4); n--) {
                        var bytes = keyOf(((long) writer << 40) | n);
                        if (index.get(bytes, Segments.hash(bytes)) == null) {
                            failure.compareAndSet(null, new AssertionError("a reader missed " + writer + ":" + n));
                            stop.set(true);
                            return;
                        }
                    }
                }
            }));
        }
        Thread.sleep(2_000);
        stop.set(true);
        for (var thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    @Test
    @DisplayName("never hides a chain it holds from a reader while writers add and remove others and the segments grow")
    void concurrent() throws Exception {
        var index = new ChainIndex();
        // Keys every reader must always find, added before the readers start
        var stable = new ArrayList<Chain>();
        for (var key = 0L; key < 5_000; key++) {
            var created = chain(key);
            index.putIfAbsent(created);
            stable.add(created);
        }
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var start = new CountDownLatch(1);
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 4; t++) {
            var seed = t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    var random = new SplittableRandom(seed);
                    while (!stop.get()) {
                        // Writers race to add and remove the same keys; at most one chain of a key may be held
                        var key = 1_000_000L + random.nextInt(200_000);
                        if (random.nextInt(4) == 0) {
                            var bytes = keyOf(key);
                            var held = index.get(bytes, Segments.hash(bytes));
                            if (held != null) {
                                index.remove(held);
                            }
                        } else {
                            index.putIfAbsent(chain(key));
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
        }
        for (var t = 0; t < 4; t++) {
            var seed = 100 + t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    var random = new SplittableRandom(seed);
                    while (!stop.get()) {
                        var held = stable.get(random.nextInt(stable.size()));
                        if (index.get(held.key, held.hash) != held) {
                            throw new AssertionError("a reader missed a chain the index holds");
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
        }
        start.countDown();
        Thread.sleep(2_000);
        stop.set(true);
        for (var thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        for (var held : stable) {
            assertSame(held, index.get(held.key, held.hash));
        }
        // Each key found once accounts for one chain: a key held twice would make the size larger
        var found = stable.size();
        for (var key = 1_000_000L; key < 1_200_000L; key++) {
            var bytes = keyOf(key);
            if (index.get(bytes, Segments.hash(bytes)) != null) {
                found++;
            }
        }
        assertEquals(found, index.size());
    }
}
