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
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The hash index of a key space's chains, against a map of the JDK, alone and under concurrent writers and readers.
class ChainIndexTest {
    // Keys at offsets of shared chunks, as a key space holds them
    private static final KeySlab SLAB = new KeySlab();

    private static Chain chain(long key) {
        var bytes = MemorySegment.ofArray(ByteBuffer.allocate(12).putLong(key).putInt((int) (key * 31)).array());
        return SLAB.chain(bytes, Segments.hash(bytes));
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
                    var array = Segments.toBytes(bytes);
                    assertSame(expected, index.get(array, Segments.hash(array)), "get of an array " + key);
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
                        var created = random.nextBoolean() ? SLAB.chain(key, hash) : new Chain(key, hash);
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
                        assertSame(model.get(name), index.get(bytes, hash), "removing another chain of " + name + ", by array");
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
                assertEquals(model.isEmpty(), index.holdsNone());
                if (step % 64 == 0) {
                    assertEquals(index.tablesForTests(), index.occupiedForTests());
                }
            }
            for (var held : model.values()) {
                assertSame(held, index.get(held.key(), held.hash));
                assertSame(held, index.get(Segments.toBytes(held.key()), held.hash));
                index.remove(held);
                assertEquals(index.tablesForTests(), index.occupiedForTests());
            }
            assertTrue(index.holdsNone());
            assertEquals(0, index.tablesForTests());
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
    @DisplayName("counts the segments that hold a table through storms of insertions and removals over every segment")
    void occupiedSegments() throws Exception {
        var index = new ChainIndex();
        assertTrue(index.holdsNone());
        var failure = new AtomicReference<Throwable>();
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 4; t++) {
            var seed = t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    var random = new SplittableRandom(seed);
                    var held = new ArrayList<Chain>();
                    for (var step = 0; step < 300_000; step++) {
                        // Few keys per thread, over and over down to none, so segments empty and fill
                        var phase = (step >> 10) & 1;
                        if (phase == 0 && held.size() < 200) {
                            var created = chain(((long) seed << 40) | random.nextInt(1_000));
                            if (index.putIfAbsent(created) == null) {
                                held.add(created);
                            }
                        } else if (!held.isEmpty()) {
                            index.remove(held.remove(random.nextInt(held.size())));
                        }
                    }
                    if (seed % 2 == 0) {
                        held.forEach(index::remove);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
        }
        for (var thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        assertEquals(index.tablesForTests(), index.occupiedForTests());
        assertEquals(index.size() == 0, index.holdsNone());
        var walk = new ChainIndex.Cursor();
        for (var chain = index.next(walk); chain != null; chain = index.next(walk)) {
            index.remove(chain);
        }
        assertEquals(0, index.size());
        assertEquals(0, index.tablesForTests());
        assertEquals(0, index.occupiedForTests());
        assertTrue(index.holdsNone());
    }

    @Test
    @DisplayName("never reports no chain to a reader synchronized with the insertion of one it still holds, while other segments empty and fill")
    void occupiedPublished() throws Exception {
        var index = new ChainIndex();
        var published = new java.util.concurrent.atomic.AtomicLong();
        var acknowledged = new java.util.concurrent.atomic.AtomicLong();
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var threads = new ArrayList<Thread>();
        threads.add(Thread.ofPlatform().start(() -> {
            for (var n = 1L; !stop.get(); n++) {
                var created = chain(n);
                index.putIfAbsent(created);
                // What a commit's publication does for the chains it wrote
                published.setRelease(n);
                while (acknowledged.getAcquire() < n) {
                    if (stop.get()) {
                        // A reader may still be looking for this chain, which stays
                        return;
                    }
                    Thread.onSpinWait();
                }
                // The chain stays until the reader looked, then leaves, often the last of the whole index
                index.remove(created);
            }
        }));
        threads.add(Thread.ofPlatform().start(() -> {
            var random = new SplittableRandom(11);
            while (!stop.get()) {
                var created = chain((1L << 50) | random.nextInt(4));
                if (index.putIfAbsent(created) == null) {
                    index.remove(created);
                }
            }
        }));
        threads.add(Thread.ofPlatform().start(() -> {
            var seen = 0L;
            while (!stop.get()) {
                var n = published.getAcquire();
                if (n == seen) {
                    Thread.onSpinWait();
                    continue;
                }
                seen = n;
                var array = Segments.toBytes(keyOf(n));
                if (index.holdsNone() || index.get(array, Segments.hash(array)) == null) {
                    failure.compareAndSet(null, new AssertionError("a reader missed " + n));
                    stop.set(true);
                    return;
                }
                acknowledged.setRelease(n);
            }
        }));
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
                        if (index.get(held.key(), held.hash) != held) {
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
            assertSame(held, index.get(held.key(), held.hash));
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

    @Test
    @DisplayName("a block of lookups finds what a lookup of each key finds, through colliding hashes, tombstones, growth and shrinking")
    void lookupsModel() {
        var random = new SplittableRandom(11);
        for (var trial = 0; trial < 6; trial++) {
            // Two key spaces' indexes, which one block looks up in at once
            var indexes = new ChainIndex[]{new ChainIndex(), new ChainIndex()};
            var models = java.util.List.of(new HashMap<String, Chain>(), new HashMap<String, Chain>());
            var forced = new int[]{0x0000FFFF, 0x0000FFFE, 0x0000FFEF, 0x7C00000F, 0};
            var lookups = new ChainIndex.Lookups(random.nextBoolean());
            for (var step = 0; step < 120_000; step++) {
                var many = (step / 30_000) % 2 == 0;
                var which = random.nextInt(2);
                var index = indexes[which];
                var model = models.get(which);
                var id = random.nextInt(many ? 3_000 : 300);
                var bytes = keyBytes(id);
                var name = java.util.HexFormat.of().formatHex(bytes);
                var hash = forced[Math.floorMod(java.util.Arrays.hashCode(bytes), forced.length)];
                var key = MemorySegment.ofArray(bytes).asReadOnly();
                if (random.nextInt(3) != 0) {
                    var created = SLAB.chain(key, hash);
                    if (index.putIfAbsent(created) == null) {
                        model.put(name, created);
                    }
                } else {
                    var held = model.remove(name);
                    if (held != null) {
                        index.remove(held);
                    }
                }
                if (step % 37 == 0) {
                    lookups.clear();
                    var count = 1 + random.nextInt(ChainIndex.Lookups.BLOCK);
                    var expected = new Chain[count];
                    for (var at = 0; at < count; at++) {
                        var into = random.nextInt(3);
                        var lookedUp = keyBytes(random.nextInt(many ? 3_200 : 320));
                        var lookedUpHash = forced[Math.floorMod(java.util.Arrays.hashCode(lookedUp), forced.length)];
                        var segment = MemorySegment.ofArray(lookedUp);
                        // A third of them in no index, as a key space that holds no chain looks up
                        var target = into == 2 ? null : indexes[into];
                        lookups.add(target, segment, lookedUpHash);
                        expected[at] = target == null ? null : target.get(segment, lookedUpHash);
                    }
                    lookups.run();
                    for (var at = 0; at < count; at++) {
                        assertSame(expected[at], lookups.found(at), "lookup " + at + " of " + count + " at step " + step);
                    }
                }
            }
        }
    }

    // The key of `id`: 1 to 11 bytes
    private static byte[] keyBytes(int id) {
        var length = 1 + id % 11;
        var bytes = new byte[length];
        for (var i = 0; i < length; i++) {
            bytes[i] = (byte) (id >>> (8 * (i % 4)));
        }
        bytes[length - 1] ^= (byte) (id / 256 + length);
        return bytes;
    }

    @Test
    @DisplayName("a block of lookups probes past a slot whose chain is not published yet, a tombstone and another key of the same tag")
    void lookupsPastCandidates() {
        var index = new ChainIndex();
        // The same segment, tag and first slot of a small table, and different hashes: bit 4 alone differs
        var hash = 0x00000001;
        var sameTag = 0x00000011;
        var inFlight = new Chain(MemorySegment.ofArray(new byte[]{1}).asReadOnly(), hash);
        var sameHash = new Chain(MemorySegment.ofArray(new byte[]{2}).asReadOnly(), hash);
        var otherHash = new Chain(MemorySegment.ofArray(new byte[]{3}).asReadOnly(), sameTag);
        var removed = new Chain(MemorySegment.ofArray(new byte[]{4}).asReadOnly(), hash);
        var behind = new Chain(MemorySegment.ofArray(new byte[]{5}).asReadOnly(), hash);
        // The first slot holds a tag whose chain is not published, as an insertion holds it between its two stores
        var slot = index.tagForTests(inFlight);
        for (var chain : new Chain[]{sameHash, otherHash, removed, behind}) {
            index.publishForTests(chain, index.tagForTests(chain));
        }
        index.remove(removed);
        var lookups = new ChainIndex.Lookups(true);
        var absent = MemorySegment.ofArray(new byte[]{6});
        for (var key : new Chain[]{inFlight, sameHash, otherHash, removed, behind}) {
            lookups.add(index, key.key(), key.hash);
        }
        lookups.add(index, absent, hash);
        lookups.run();
        assertNull(lookups.found(0), "a chain not published yet");
        assertSame(sameHash, lookups.found(1));
        assertSame(otherHash, lookups.found(2));
        assertNull(lookups.found(3), "a removed chain");
        assertSame(behind, lookups.found(4), "a chain behind a tombstone");
        assertNull(lookups.found(5));
        for (var at = 0; at < 6; at++) {
            var key = at == 5 ? absent : new Chain[]{inFlight, sameHash, otherHash, removed, behind}[at].key();
            assertSame(index.get(key, at == 2 ? sameTag : hash), lookups.found(at), "lookup " + at + " against get");
        }
        index.publishForTests(inFlight, slot);
        lookups.clear();
        lookups.add(index, inFlight.key(), hash);
        lookups.run();
        assertSame(inFlight, lookups.found(0));
        assertSame(inFlight, index.get(inFlight.key(), hash));
    }

    @Test
    @DisplayName("a block of lookups answers from the tables it read first while the index grows, shrinks and drops chains between its steps")
    void lookupsStaleTables() {
        var random = new SplittableRandom(13);
        var index = new ChainIndex();
        // Keys no change touches, which every lookup must find
        var stable = new ArrayList<Chain>();
        for (var key = 0L; key < 2_000; key++) {
            var created = chain(key);
            index.putIfAbsent(created);
            stable.add(created);
        }
        // Every chain each changing key ever had
        var held = new HashMap<Long, java.util.Set<Chain>>();
        var current = new HashMap<Long, Chain>();
        for (var key = 10_000L; key < 12_000; key++) {
            var created = chain(key);
            index.putIfAbsent(created);
            current.put(key, created);
            held.computeIfAbsent(key, _ -> java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>())).add(created);
        }
        var fresh = new long[]{1_000_000L};
        for (var round = 0; round < 300; round++) {
            var lookups = new ChainIndex.Lookups(random.nextBoolean());
            var looked = new long[ChainIndex.Lookups.BLOCK];
            for (var at = 0; at < looked.length; at++) {
                looked[at] = random.nextBoolean() ? random.nextInt(2_000) : 10_000L + random.nextInt(2_000);
                var bytes = keyOf(looked[at]);
                lookups.add(index, bytes, Segments.hash(bytes));
            }
            lookups.runForTests(() -> {
                // Drop and recreate changing keys, then grow the segments past a rebuild and shrink them back
                for (var i = 0; i < 300; i++) {
                    var key = 10_000L + random.nextInt(2_000);
                    var chain = current.remove(key);
                    if (chain != null) {
                        index.remove(chain);
                    } else {
                        var created = chain(key);
                        index.putIfAbsent(created);
                        current.put(key, created);
                        held.get(key).add(created);
                    }
                }
                var added = new ArrayList<Chain>();
                var count = random.nextInt(6_000);
                for (var i = 0; i < count; i++) {
                    var created = chain(fresh[0]++);
                    index.putIfAbsent(created);
                    added.add(created);
                }
                for (var chain : added) {
                    index.remove(chain);
                }
            });
            for (var at = 0; at < looked.length; at++) {
                var found = lookups.found(at);
                if (looked[at] < 2_000) {
                    assertSame(stable.get((int) looked[at]), found, "a stable key in round " + round);
                } else {
                    assertTrue(found == null || held.get(looked[at]).contains(found), "a chain the key never had in round " + round);
                }
            }
        }
    }

    @Test
    @DisplayName("blocks of lookups never miss a chain the index holds while writers add and remove others and the segments grow")
    void lookupsConcurrent() throws Exception {
        var index = new ChainIndex();
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
        for (var t = 0; t < 3; t++) {
            var seed = t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    var random = new SplittableRandom(seed);
                    while (!stop.get()) {
                        var key = 1_000_000L + random.nextInt(200_000);
                        if (random.nextInt(3) == 0) {
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
        for (var t = 0; t < 3; t++) {
            var seed = 100 + t;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    var random = new SplittableRandom(seed);
                    var lookups = new ChainIndex.Lookups(seed % 2 == 0);
                    var expected = new Chain[ChainIndex.Lookups.BLOCK];
                    var keys = new MemorySegment[ChainIndex.Lookups.BLOCK];
                    while (!stop.get()) {
                        lookups.clear();
                        var count = 1 + random.nextInt(ChainIndex.Lookups.BLOCK);
                        for (var at = 0; at < count; at++) {
                            if (random.nextInt(4) == 0) {
                                // A key the writers add and remove: a lookup finds one of its chains or none
                                keys[at] = keyOf(1_000_000L + random.nextInt(200_000));
                                expected[at] = null;
                            } else {
                                var held = stable.get(random.nextInt(stable.size()));
                                keys[at] = held.key();
                                expected[at] = held;
                            }
                            lookups.add(index, keys[at], Segments.hash(keys[at]));
                        }
                        lookups.run();
                        for (var at = 0; at < count; at++) {
                            var found = lookups.found(at);
                            if (expected[at] != null && found != expected[at]) {
                                throw new AssertionError("a block of lookups missed a chain the index holds");
                            }
                            if (found != null && !found.keyEquals(keys[at])) {
                                throw new AssertionError("a block of lookups found the chain of another key");
                            }
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
    }
}
