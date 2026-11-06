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
