package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Log frame bodies that refer to the arrays the store's versions keep instead of copying their values: which commits
/// refer to what, that no referred array is one the pool can hand out, and that the store recovers every durable
/// commit whatever its batches do with their arrays while its frames wait in the queue. The pool of the process
/// scribbles over every array it takes back while a store test runs, so a frame or version left on one fails the test.
class FrameReferencesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    // A resident key space whose values are not byte arrays the codec hands back, so the store copies them
    private static final Keyspace<String, byte[]> NOTES = Keyspace.resident("notes", KeyCodec.utf8(), ValueCodec.of(
            value -> value.length, (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
            source -> source.toArray(ValueLayout.JAVA_BYTE)));
    private static final List<Keyspace<String, byte[]>> KEYSPACES = List.of(MESSAGES, SESSIONS, NOTES);
    private static final Map<String, Keyspace<?, ?>> BY_NAME = Map.of(MESSAGES.name(), MESSAGES, SESSIONS.name(), SESSIONS, NOTES.name(), NOTES);
    // Both sides of the shortest values the bodies refer to, and of a slice of the log writer
    private static final int[] LENGTHS = {0, 1, 100, 255, 256, 257, 1_400, 2_047, 2_048, 4_096, 10_000, 65_537, 131_072};

    private static byte[] pattern(Random random, int length) {
        var bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    private static int length(Random random) {
        return random.nextInt(4) == 0 ? LENGTHS[random.nextInt(LENGTHS.length)] : random.nextInt(2_000);
    }

    private static MemorySegment utf8(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }

    /// {@return the arrays `body` refers to, by identity}
    private static Set<byte[]> referenced(FrameBody body) {
        var arrays = Collections.<byte[]>newSetFromMap(new IdentityHashMap<>());
        for (var reference = 0; reference < body.references(); reference++) {
            arrays.add(body.referredArrayForTests(reference));
        }
        return arrays;
    }

    /// The ways a prepared batch hands its values to the store's versions.
    enum Homes {
        // Little beside the values: the batch's own array, compacted into one of their size
        OWN,
        // Its own array, pool-sized and too full to compact
        OWN_FULL,
        // Enough beside the values to copy them into an array of their own
        CHUNKS,
        // Past a chunk of the batch: the chunks of its rope, the last one copied
        ROPE,
        // Values of half a chunk or more, on arrays of their own
        OWN_VALUES
    }

    /// {@return a prepared batch whose values reach the store's versions as `homes` says, from `pool`}
    private ArrayWriteBatch batch(Homes homes, HeapPool pool, Random random) {
        var batch = new ArrayWriteBatch(this, pool, BY_NAME);
        switch (homes) {
            case OWN -> batch.put(MESSAGES, "single", pattern(random, 1_000));
            case OWN_FULL -> {
                // 8 writes of 1,002 bytes: the array grows to a pooled 8 KiB one and holds 8,016 bytes
                for (var i = 0; i < 8; i++) {
                    batch.put(MESSAGES, "k" + i, pattern(random, 1_000));
                }
            }
            case CHUNKS -> {
                for (var i = 0; i < 12; i++) {
                    batch.put(MESSAGES, "message" + i, pattern(random, length(random) % 20_000));
                }
                batch.put(SESSIONS, "session", pattern(random, 5_000));
            }
            case ROPE -> {
                for (var i = 0; i < 400; i++) {
                    batch.put(MESSAGES, "message" + i, pattern(random, 1_000 + random.nextInt(400)));
                }
                batch.put(SESSIONS, "session", pattern(random, 3_000));
            }
            case OWN_VALUES -> {
                batch.put(MESSAGES, "large", pattern(random, 200_000));
                batch.put(MESSAGES, "small", pattern(random, 3_000));
                batch.put(SESSIONS, "session", pattern(random, 150_000));
            }
        }
        batch.prepare(false, null, Map.of());
        return batch;
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Homes.class)
    @DisplayName("refers only to arrays the pool never hands out, which keep the values after the batch starts again")
    void notPooled(Homes homes) {
        var random = new Random(homes.ordinal() * 31L + 7);
        var pool = new HeapPool(1L << 28, false);
        var batch = batch(homes, pool, random);
        var body = new FrameBody(64, 1);
        var copied = new FrameBody();
        for (var i = 0; i < batch.size(); i++) {
            var own = batch.ownValue(i);
            var array = own != null ? own : batch.homeArray(i);
            if (array == null) {
                // A resident value the store copies
                assertTrue(batch.keyspace(i).residency() == Keyspace.Residency.RESIDENT, "write " + i + " has no home");
                continue;
            }
            var offset = own != null ? 0 : batch.homeOffset(i);
            var length = (int) batch.valueBytes(i);
            assertEquals(-1, MemorySegment.ofArray(array).asSlice(offset, length).mismatch(batch.value(i)), "the value of write " + i);
            if (own == null) {
                assertEquals(-1, batch.home(i).asSlice(offset, length).mismatch(batch.value(i)), "the home of write " + i);
            }
            body.put(0, batch.key(i), array, offset, length);
            copied.put(0, batch.key(i), batch.value(i));
        }
        var expected = copied.toBytes();
        assertArrayEquals(expected, body.toBytes());
        var arrays = referenced(body);
        assertFalse(arrays.isEmpty());
        batch.finish();
        // Whatever the pool holds now is what the next batches write over
        for (var shift = 12; shift <= 20; shift++) {
            for (var taken = 0; taken < 64; taken++) {
                var array = pool.acquire(1 << shift);
                for (var kept : arrays) {
                    assertNotSame(kept, array, "a referred array is free in the pool");
                }
                Arrays.fill(array, (byte) 0x5A);
            }
        }
        // And the batch writes over its own arrays again
        for (var i = 0; i < 50; i++) {
            batch.put(MESSAGES, "again" + i, new byte[2_000]);
        }
        assertArrayEquals(expected, body.toBytes());
    }

    @ParameterizedTest(name = "log idle: {0}")
    @CsvSource({"false", "true"})
    @DisplayName("of a commit the log seals whole leaves its long values out of its array, whether the log is idle or not")
    void forCommit(boolean idle) {
        var random = new Random(3);
        var pool = new HeapPool(1L << 26, false);
        var single = new ArrayWriteBatch(this, pool, BY_NAME);
        single.put(MESSAGES, "single", pattern(random, 1_000));
        single.put(MESSAGES, "small", pattern(random, FrameBody.RUN_REFERABLE - 1));
        single.prepare(false, null, Map.of());
        var body = FrameBody.forCommit(single, null, null, idle);
        assertEquals(FrameBody.RUN_REFERABLE, body.referable());
        var estimate = FrameBody.capacityFor(single, null);
        assertTrue(estimate > 1_000);
        assertEquals(Math.max(16, estimate - 1_000), body.chunk(0).length, "the room of the body");

        // The value whose unit holds it is no reference
        var units = new PageRef[]{new PageRef(1, 1, 0, 0, 0), null};
        assertEquals(Math.max(16, FrameBody.capacityFor(single, units)), FrameBody.forCommit(single, units, null, idle).chunk(0).length);

        // Past a run, but with fewer bytes of values than a large body refers to: never worth the pieces
        var large = new ArrayWriteBatch(this, pool, BY_NAME);
        for (var i = 0; i < 20; i++) {
            large.put(MESSAGES, "message" + i, pattern(random, 1_000));
        }
        large.prepare(false, null, Map.of());
        var largeBody = FrameBody.forCommit(large, null, null, idle);
        assertEquals(FrameBody.NOT_REFERABLE, largeBody.referable());
        assertEquals(FrameBody.capacityFor(large, null), largeBody.chunk(0).length);
        assertTrue(FrameBody.capacityFor(large, null) > LogWriter.RUN_MAX_BODY);
    }

    @ParameterizedTest(name = "log idle: {0}")
    @CsvSource({"false", "true"})
    @DisplayName("of a commit of a few chunks of values refers to those of 64 bytes or more while the log is idle, and to nothing while it is busy")
    void forLargeCommit(boolean idle) {
        var random = new Random(5);
        var pool = new HeapPool(1L << 28, false);
        var batch = new ArrayWriteBatch(this, pool, BY_NAME);
        var referable = 0L;
        var count = 0;
        for (var i = 0; i < 2_400; i++) {
            var length = i % 5 == 0 ? random.nextInt(FrameBody.LARGE_REFERABLE) : FrameBody.LARGE_REFERABLE + random.nextInt(1_500);
            batch.put(MESSAGES, "message" + i, pattern(random, length));
            if (length >= FrameBody.LARGE_REFERABLE) {
                referable += length;
                count++;
            }
            if (i % 50 == 0) {
                batch.delete(MESSAGES, "gone" + i);
            }
        }
        batch.put(SESSIONS, "session", pattern(random, 3_000));
        referable += 3_000;
        count++;
        batch.prepare(false, null, Map.of());
        assertTrue(referable >= FrameBody.LARGE_REFERRED_BYTES);
        var capacity = FrameBody.capacityFor(batch, null);
        // With a pool, the body starts on a chunk whether it copies the values or holds only the rest, which takes less
        var pooled = FrameBody.forCommit(batch, null, pool, idle);
        assertTrue(capacity - referable < FrameBody.CHUNK_BYTES);
        assertEquals(idle ? FrameBody.LARGE_REFERABLE : FrameBody.NOT_REFERABLE, pooled.referable());
        assertEquals(1, pooled.chunkCount());
        assertEquals(FrameBody.CHUNK_BYTES, pooled.chunk(0).length, "the room of the pooled body");
        assertTrue(pooled.holdsPoolForTests());
        pooled.release();
        assertFalse(pooled.holdsPoolForTests());
        var body = FrameBody.forCommit(batch, null, null, idle);
        if (!idle) {
            assertEquals(FrameBody.NOT_REFERABLE, body.referable());
            assertEquals(capacity, body.chunk(0).length, "the room of the body");
            return;
        }
        assertEquals(FrameBody.LARGE_REFERABLE, body.referable());
        assertEquals(capacity - referable, body.chunk(0).length, "the room of the body");
        // Every put of a value that long refers to it, as the applier offers each on an array, and the rest are copied
        var copied = new FrameBody();
        var referred = 0;
        for (var i = 0; i < batch.size(); i++) {
            if (batch.kind(i) != ArrayWriteBatch.PUT) {
                body.delete(0, batch.key(i));
                copied.delete(0, batch.key(i));
                continue;
            }
            var own = batch.ownValue(i);
            var array = own != null ? own : batch.homeArray(i);
            var offset = own != null ? 0 : batch.homeOffset(i);
            if (array == null) {
                array = batch.value(i).toArray(ValueLayout.JAVA_BYTE);
                offset = 0;
            }
            body.put(0, batch.key(i), array, offset, (int) batch.valueBytes(i));
            copied.put(0, batch.key(i), batch.value(i));
            referred += batch.valueBytes(i) >= FrameBody.LARGE_REFERABLE ? 1 : 0;
        }
        assertEquals(count, referred);
        assertEquals(referred, body.references());
        assertEquals(copied.size(), body.size());
        assertArrayEquals(copied.toBytes(), body.toBytes());
        // Its room held what it copied, so it never grew
        assertEquals(capacity - referable, body.chunk(0).length, "the room of the filled body");

        // A value in its unit is no reference, nor counts toward the chunk of values
        var units = new PageRef[batch.size()];
        for (var i = 0; i < units.length; i++) {
            if (batch.kind(i) == ArrayWriteBatch.PUT && i % 4 == 0) {
                units[i] = new PageRef(1, 1, 0, 0, 0);
            }
        }
        var inUnits = FrameBody.forCommit(batch, units, null, true);
        var left = 0L;
        for (var i = 0; i < units.length; i++) {
            if (units[i] == null && batch.kind(i) == ArrayWriteBatch.PUT && batch.valueBytes(i) >= FrameBody.LARGE_REFERABLE) {
                left += batch.valueBytes(i);
            }
        }
        if (left >= FrameBody.LARGE_REFERRED_BYTES) {
            assertEquals(FrameBody.capacityFor(batch, units) - left, inUnits.chunk(0).length);
        } else {
            assertEquals(FrameBody.NOT_REFERABLE, inUnits.referable());
        }
    }

    @ParameterizedTest(name = "log idle: {0}")
    @CsvSource({"false", "true"})
    @DisplayName("of a batch of a hundred messages, sessions and chats, past a chunk of values, copies them on a pooled chunk whether the log is idle or not")
    void forSteadyBatch(boolean idle) {
        var random = new Random(7);
        var pool = new HeapPool(1L << 28, false);
        for (var round = 0; round < 20; round++) {
            var batch = new ArrayWriteBatch(this, pool, BY_NAME);
            var referable = 0L;
            for (var i = 0; i < 100; i++) {
                // Message bodies of up to a kilobyte and a half, one in seven a document of up to 12 KB, beside a session
                // and a chat
                var body = random.nextInt(7) == 0 ? 2_500 + random.nextInt(9_500) : 400 + random.nextInt(1_200);
                batch.put(MESSAGES, "message" + round + ":" + i, pattern(random, body));
                batch.put(SESSIONS, "session" + i, pattern(random, 1_536));
                batch.put(NOTES, "chat" + i % 20, pattern(random, 320));
                referable += body + 1_536 + 320;
            }
            batch.prepare(false, null, Map.of());
            assertTrue(referable >= FrameBody.CHUNK_BYTES && referable < FrameBody.LARGE_REFERRED_BYTES, "values: " + referable);
            var body = FrameBody.forCommit(batch, null, pool, idle);
            assertEquals(FrameBody.NOT_REFERABLE, body.referable());
            assertEquals(0, body.references());
            assertEquals(FrameBody.CHUNK_BYTES, body.chunk(0).length, "the room of the body");
            body.release();
        }
    }

    @Test
    @DisplayName("of a body checks a write against its longest length, the values it refers to included")
    void limit() {
        var body = new FrameBody(16, 1);
        // A reference the body is never gathered from, as long as a body may be but for a few bytes
        body.put(0, utf8("k"), new byte[0], 0, FrameBody.MAX_BYTES - 200);
        var room = FrameBody.MAX_BYTES - FrameBody.WRITE_FIELDS - body.size();
        assertDoesNotThrow(() -> body.checkRoom(room));
        assertThrows(IllegalArgumentException.class, () -> body.checkRoom(room + 1));
        assertThrows(IllegalArgumentException.class, () -> body.checkRoom(Long.MAX_VALUE / 2));
    }

    /// Scribbles over every array the pool of the process takes back, and keeps the arrays it holds free, while open.
    private static final class ScribblingPool implements HeapPool.Watcher, AutoCloseable {
        private final Set<byte[]> free = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        ScribblingPool() {
            HeapPool.process().watchForTests(this);
        }

        @Override
        public void acquired(byte[] array) {
            free.remove(array);
        }

        @Override
        public void released(byte[] array) {
            Arrays.fill(array, (byte) 0x5A);
            free.add(array);
        }

        /// Checks that no frame `ledger` has queued refers to an array the pool holds free.
        void assertNoneFree(Ledger ledger) {
            var queued = ((LiveLedger) ledger).logForTests().queuedForTests();
            for (var body : queued) {
                for (var array : referenced(body)) {
                    assertFalse(free.contains(array), "a queued frame refers to an array free in the pool");
                }
            }
        }

        @Override
        public void close() {
            HeapPool.process().watchForTests(null);
        }
    }

    private static Ledger open(Path directory, boolean encrypted, long largeValues) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NOTES).largeValues(largeValues))
                .segmentSize(64 << 10)
                .build();
    }

    private static String id(Keyspace<?, ?> keyspace, String key) {
        return keyspace.name() + "\0" + key;
    }

    /// Makes `rounds` commits of every kind whose values a frame may refer to or copy, recording them in `model`.
    ///
    /// @return the last commit
    private static Commit work(Ledger ledger, Random random, Map<String, byte[]> model, int rounds) {
        Commit last = null;
        var keys = 120;
        for (var round = 0; round < rounds; round++) {
            switch (random.nextInt(7)) {
                case 0 -> {
                    var key = "m" + random.nextInt(keys);
                    var value = pattern(random, length(random));
                    last = ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
                    model.put(id(MESSAGES, key), value);
                }
                case 1 -> {
                    // Enough beside the values to send them to chunks, the longest to arrays of their own
                    var batch = ledger.newBatch();
                    for (var write = random.nextInt(random.nextBoolean() ? 4 : 30); write >= 0; write--) {
                        var key = "m" + random.nextInt(keys);
                        var value = pattern(random, length(random));
                        batch.put(MESSAGES, key, value);
                        model.put(id(MESSAGES, key), value);
                    }
                    var session = "s" + random.nextInt(keys);
                    var state = pattern(random, random.nextInt(6_000));
                    batch.put(SESSIONS, session, state);
                    model.put(id(SESSIONS, session), state);
                    last = ledger.commit(batch);
                }
                case 2 -> {
                    var key = "s" + random.nextInt(keys);
                    var value = pattern(random, length(random));
                    last = ledger.commit(ledger.newBatch().put(SESSIONS, key, value));
                    model.put(id(SESSIONS, key), value);
                }
                case 3 -> {
                    // A resident key space on a value of its own, or on the batch's array
                    var batch = ledger.newBatch();
                    for (var write = random.nextInt(4); write >= 0; write--) {
                        var key = "n" + random.nextInt(keys);
                        var value = pattern(random, length(random));
                        batch.put(NOTES, key, value);
                        model.put(id(NOTES, key), value);
                    }
                    if (random.nextBoolean()) {
                        var key = "m" + random.nextInt(keys);
                        var value = pattern(random, 1_000);
                        batch.put(MESSAGES, key, value);
                        model.put(id(MESSAGES, key), value);
                    }
                    last = ledger.commit(batch);
                }
                case 4 -> {
                    // A commit that runs alone, for its range removal
                    var key = "m" + random.nextInt(keys);
                    var value = pattern(random, length(random));
                    var batch = ledger.newBatch().put(MESSAGES, key, value).deleteRange(NOTES, KeyRange.atLeast("n9"));
                    model.put(id(MESSAGES, key), value);
                    model.keySet().removeIf(entry -> entry.startsWith(NOTES.name() + "\0n9"));
                    last = ledger.commit(batch);
                }
                default -> {
                    // A transaction, with its values on arrays of their own
                    var message = "m" + random.nextInt(keys);
                    var messageValue = pattern(random, length(random));
                    var encoded = "m" + random.nextInt(keys);
                    var encodedValue = pattern(random, length(random));
                    var note = "n" + random.nextInt(keys);
                    var noteValue = pattern(random, length(random));
                    var session = "s" + random.nextInt(keys);
                    var sessionValue = pattern(random, length(random));
                    var removed = "m" + random.nextInt(keys);
                    last = ledger.update(transaction -> {
                        transaction.put(MESSAGES, message, messageValue);
                        transaction.putEncoded(MESSAGES, utf8(encoded), MemorySegment.ofArray(encodedValue));
                        transaction.put(NOTES, note, noteValue);
                        transaction.put(SESSIONS, session, sessionValue);
                        transaction.delete(MESSAGES, removed);
                    });
                    model.put(id(MESSAGES, message), messageValue);
                    model.put(id(MESSAGES, encoded), encodedValue);
                    model.put(id(NOTES, note), noteValue);
                    model.put(id(SESSIONS, session), sessionValue);
                    // Its last write
                    model.remove(id(MESSAGES, removed));
                }
            }
        }
        return last;
    }

    private static void verify(Ledger ledger, Map<String, byte[]> model) {
        var counts = new TreeMap<String, Long>();
        for (var entry : model.entrySet()) {
            var separator = entry.getKey().indexOf('\0');
            var name = entry.getKey().substring(0, separator);
            var key = entry.getKey().substring(separator + 1);
            var keyspace = KEYSPACES.stream().filter(candidate -> candidate.name().equals(name)).findFirst().orElseThrow();
            assertArrayEquals(entry.getValue(), ledger.get(keyspace, key).orElseThrow(), entry.getKey());
            counts.merge(name, 1L, Long::sum);
        }
        for (var keyspace : KEYSPACES) {
            assertEquals(counts.getOrDefault(keyspace.name(), 0L), ledger.size(keyspace), keyspace.name());
        }
    }

    /// Runs `test` with the pool of the process scribbling, and the slab forced hot if `hot`, so small commits put
    /// their values on its chunks.
    private static void withSlab(boolean hot, PoolTest test) throws Exception {
        if (hot) {
            ValueSlab.forceNewHotForTests(1L << 20);
        }
        try (var pool = new ScribblingPool()) {
            test.run(pool);
        } finally {
            if (hot) {
                ValueSlab.forceNewHotForTests(0);
            }
        }
    }

    /// A test that runs while the pool of the process scribbles.
    @FunctionalInterface
    private interface PoolTest {
        void run(ScribblingPool pool) throws Exception;
    }

    static Stream<Arguments> recoveries() {
        return IntStream.range(0, 16).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0, seed % 3 == 0, seed % 4 < 2, seed % 8 < 4 ? 64L << 10 : 1L << 20));
    }

    @ParameterizedTest(name = "seed {0}, encrypted: {1}, process crash: {2}, hot slab: {3}, large values from {4}")
    @MethodSource("recoveries")
    @DisplayName("recovers the exact bytes of every durable commit after a crash")
    void recovers(int seed, boolean encrypted, boolean processCrash, boolean hot, long largeValues) throws Exception {
        withSlab(hot, _ -> {
            var random = new Random(seed * 131L + 1);
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(directory, encrypted, largeValues)) {
                work(ledger, random, model, 80).awaitDurable();
                verify(ledger, model);
                survivor = processCrash ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(seed));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, largeValues)) {
                verify(ledger, model);
                // And writes on: the log after recovery takes frames that refer to values again
                work(ledger, random, model, 20).awaitDurable();
                verify(ledger, model);
            }
        });
    }

    /// Holds every write of the log's segments until the latch it returns opens, once `reached` counted down, so the
    /// frames committed meanwhile stay queued.
    private static CountDownLatch stallLog(CrashFileSystem disk, CountDownLatch reached) {
        var open = new CountDownLatch(1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".log")) {
                reached.countDown();
                try {
                    assertTrue(open.await(2, TimeUnit.MINUTES));
                } catch (InterruptedException exception) {
                    throw new IOException(exception);
                }
            }
        });
        return open;
    }

    @ParameterizedTest(name = "encrypted: {0}, hot slab: {1}")
    @CsvSource({"true, false", "false, false", "true, true", "false, true"})
    @DisplayName("logs the values of queued frames while their batches start again on pooled arrays, and recovers them")
    void reusedBatches(boolean encrypted, boolean hot) throws Exception {
        withSlab(hot, pool -> {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new ConcurrentHashMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(directory, encrypted, 1L << 20)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var lasts = new ConcurrentLinkedQueue<Commit>();
                try {
                    // A commit that parks the writer on the stall, so every frame after it stays queued
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    model.put(id(MESSAGES, "first"), new byte[300]);
                    assertTrue(reached.await(2, TimeUnit.MINUTES));
                    var threads = new ArrayList<Thread>();
                    for (var t = 0; t < 4; t++) {
                        var thread = t;
                        threads.add(Thread.ofPlatform().start(() -> {
                            try {
                                var random = new Random(thread * 17L + 3);
                                var batch = ledger.newBatch();
                                Commit last = null;
                                for (var round = 0; round < 40; round++) {
                                    // Beside a session: the messages go to an array of their own, the batch's to the pool
                                    for (var write = 0; write < 4; write++) {
                                        var key = "m" + thread + ":" + round + ":" + write;
                                        var value = pattern(random, 1_400);
                                        batch.put(MESSAGES, key, value);
                                        model.put(id(MESSAGES, key), value);
                                    }
                                    var session = "s" + thread;
                                    var state = pattern(random, 3_000);
                                    batch.put(SESSIONS, session, state);
                                    model.put(id(SESSIONS, session), state);
                                    last = ledger.commit(batch);
                                    // Alone: the value stays on the array the batch gives up
                                    var key = "o" + thread + ":" + round;
                                    var value = pattern(random, 5_000);
                                    batch.put(MESSAGES, key, value);
                                    model.put(id(MESSAGES, key), value);
                                    last = ledger.commit(batch);
                                    // Pool-sized and too full to compact: the store keeps the pooled array
                                    for (var write = 0; write < 8; write++) {
                                        var full = "f" + thread + ":" + round + ":" + write;
                                        var fullValue = pattern(random, 1_000);
                                        batch.put(MESSAGES, full, fullValue);
                                        model.put(id(MESSAGES, full), fullValue);
                                    }
                                    last = ledger.commit(batch);
                                }
                                lasts.add(last);
                            } catch (Throwable failure) {
                                failures.add(failure);
                            }
                        }));
                    }
                    for (var thread : threads) {
                        thread.join();
                    }
                    assertTrue(failures.isEmpty(), failures::toString);
                    assertFalse(((LiveLedger) ledger).logForTests().queuedForTests().isEmpty(), "no frame is queued");
                    pool.assertNoneFree(ledger);
                    verify(ledger, model);
                } finally {
                    open.countDown();
                }
                for (var last : lasts) {
                    last.awaitDurable();
                }
                survivor = disk.afterPowerLoss(new Random(5));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                verify(ledger, model);
            }
        });
    }

    /// Opens a store as [#open(Path, boolean, long)] does, whose log takes itself for idle whatever it queues, so the
    /// frames of every large commit refer to their values.
    private static Ledger openIdle(Path directory, boolean encrypted) {
        LogWriter.forceIdleNewForTests(true);
        try {
            return open(directory, encrypted, 1L << 20);
        } finally {
            LogWriter.forceIdleNewForTests(false);
        }
    }

    /// Fills `batch` with a large commit of shape `shape`, whose values reach the store's versions in each way a batch
    /// hands them over, recording them in `model`.
    private static void largeBatch(WriteBatchFiller batch, int shape, String prefix, Random random, Map<String, byte[]> model) {
        switch (shape) {
            case 0 -> {
                // Past a few chunks of the batch: the chunks of its rope, the last one copied
                for (var i = 0; i < 1_600; i++) {
                    batch.put(MESSAGES, prefix + "r" + i, pattern(random, FrameBody.LARGE_REFERABLE + random.nextInt(1_400)), model);
                }
                batch.put(SESSIONS, prefix + "s", pattern(random, 3_000), model);
            }
            case 1 -> {
                // Little beside the values, just past what a large body refers to
                for (var i = 0; i < 800; i++) {
                    batch.put(MESSAGES, prefix + "o" + i, pattern(random, 1_320 + random.nextInt(60)), model);
                }
            }
            case 2 -> {
                // Enough beside the values to copy them into arrays of their own, and resident values the store copies
                for (var i = 0; i < 450; i++) {
                    batch.put(MESSAGES, prefix + "c" + i, pattern(random, 2_400 + random.nextInt(1_000)), model);
                }
                for (var i = 0; i < 40; i++) {
                    batch.put(NOTES, prefix + "n" + i, pattern(random, 1_000), model);
                }
            }
            case 3 -> {
                // Values of half a chunk or more, on arrays of their own, beside short ones
                batch.put(MESSAGES, prefix + "l0", pattern(random, 150_000), model);
                batch.put(MESSAGES, prefix + "l1", pattern(random, 140_000), model);
                batch.put(MESSAGES, prefix + "l2", pattern(random, 3_000), model);
                batch.put(MESSAGES, prefix + "l3", pattern(random, 40), model);
                batch.put(MESSAGES, prefix + "l4", pattern(random, 200_000), model);
                batch.put(MESSAGES, prefix + "l5", pattern(random, 260_000), model);
                batch.put(MESSAGES, prefix + "l6", pattern(random, 160_000), model);
                batch.put(SESSIONS, prefix + "ls", pattern(random, 150_000), model);
            }
            case 4 -> {
                // More writes than a chunk holds the keys and lengths of: a body on chunks of its own beside its values
                for (var i = 0; i < 25_000; i++) {
                    batch.put(MESSAGES, prefix + "t" + i, pattern(random, FrameBody.LARGE_REFERABLE + random.nextInt(80)), model);
                }
            }
            default -> {
                // Resident byte arrays on copies of their own, and resident values the store copies
                for (var i = 0; i < 600; i++) {
                    batch.put(SESSIONS, prefix + "b" + i, pattern(random, 900), model);
                    batch.put(NOTES, prefix + "m" + i, pattern(random, 900), model);
                }
            }
        }
    }

    /// A batch being filled, which records each put in the model.
    private record WriteBatchFiller(WriteBatch batch) {
        void put(Keyspace<String, byte[]> keyspace, String key, byte[] value, Map<String, byte[]> model) {
            batch.put(keyspace, key, value);
            model.put(id(keyspace, key), value);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}, hot slab: {1}")
    @CsvSource({"true, false", "false, false", "true, true", "false, true"})
    @DisplayName("logs the values of queued large frames that refer to them while their batches start again on pooled arrays, and recovers them after a loss of power")
    void largeFramesQueued(boolean encrypted, boolean hot) throws Exception {
        withSlab(hot, pool -> {
            var disk = CrashFileSystem.create();
            var model = new ConcurrentHashMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = openIdle(disk.getPath("/store"), encrypted)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var lasts = new ConcurrentLinkedQueue<Commit>();
                try {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    model.put(id(MESSAGES, "first"), new byte[300]);
                    assertTrue(reached.await(2, TimeUnit.MINUTES));
                    var threads = new ArrayList<Thread>();
                    for (var t = 0; t < 3; t++) {
                        var thread = t;
                        threads.add(Thread.ofPlatform().start(() -> {
                            try {
                                var random = new Random(thread * 29L + 11);
                                // One batch, started again after each commit on the arrays it gets back
                                var batch = ledger.newBatch();
                                var filler = new WriteBatchFiller(batch);
                                Commit last = null;
                                for (var round = 0; round < 10; round++) {
                                    largeBatch(filler, (round + thread) % 6, thread + ":" + round + ":", random, model);
                                    last = ledger.commit(batch);
                                    // And small commits between them on arrays of the same pool
                                    var key = "small" + thread + ":" + round;
                                    var value = pattern(random, 1_400);
                                    batch.put(MESSAGES, key, value);
                                    model.put(id(MESSAGES, key), value);
                                    last = ledger.commit(batch);
                                }
                                lasts.add(last);
                            } catch (Throwable failure) {
                                failures.add(failure);
                            }
                        }));
                    }
                    for (var thread : threads) {
                        thread.join();
                    }
                    assertTrue(failures.isEmpty(), failures::toString);
                    var queued = ((LiveLedger) ledger).logForTests().queuedForTests();
                    var large = queued.stream().filter(body -> body.size() > LogWriter.RUN_MAX_BODY && body.references() > 0).count();
                    assertTrue(large >= 15, "large frames that refer to values: " + large);
                    assertTrue(queued.stream().anyMatch(body -> body.chunkCount() > 1 && body.references() > 0), "a chunked frame that refers to values");
                    pool.assertNoneFree(ledger);
                    verify(ledger, model);
                } finally {
                    open.countDown();
                }
                for (var last : lasts) {
                    last.awaitDurable();
                }
                assertTrue(((LiveLedger) ledger).logForTests().piecewiseFramesForTests() >= 15);
                survivor = disk.afterPowerLoss(new Random(23));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                verify(ledger, model);
            }
        });
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("refers from the frame of a large commit to its values while the log is idle, and copies them while the log holds such a frame")
    void largeFramesWhileBusy(boolean encrypted) throws Exception {
        withSlab(false, pool -> {
            var random = new Random(31);
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(disk.getPath("/store"), encrypted, 1L << 20)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                var queue = ((LiveLedger) ledger).logForTests();
                Commit last;
                try {
                    // A small frame the writer holds, which leaves the log idle
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    model.put(id(MESSAGES, "first"), new byte[300]);
                    assertTrue(reached.await(2, TimeUnit.MINUTES));
                    assertTrue(queue.idle());

                    var batch = ledger.newBatch();
                    largeBatch(new WriteBatchFiller(batch), 0, "a", random, model);
                    ledger.commit(batch);
                    var referring = queue.queuedForTests().getLast();
                    assertEquals(FrameBody.LARGE_REFERABLE, referring.referable());
                    assertTrue(referring.references() >= 300, "references: " + referring.references());
                    assertFalse(queue.idle());

                    // Behind it, the next large commit copies its values
                    largeBatch(new WriteBatchFiller(batch), 0, "b", random, model);
                    ledger.commit(batch);
                    var copying = queue.queuedForTests().getLast();
                    assertEquals(FrameBody.NOT_REFERABLE, copying.referable());
                    assertEquals(0, copying.references());
                    last = ledger.commit(ledger.newBatch().put(MESSAGES, "after", new byte[100]));
                    model.put(id(MESSAGES, "after"), new byte[100]);
                    pool.assertNoneFree(ledger);
                } finally {
                    open.countDown();
                }
                last.awaitDurable();
                survivor = disk.afterPowerLoss(new Random(37));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                verify(ledger, model);
            }
        });
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("a commit estimated past a run whose values hold less than a chunk refers to none of them on an idle log, and recovers")
    void largeEstimateFewValues(boolean encrypted) throws Exception {
        withSlab(false, _ -> {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = openIdle(disk.getPath("/store"), encrypted)) {
                var value = pattern(new Random(41), 100);
                ledger.commit(ledger.newBatch().put(MESSAGES, "x", value).put(MESSAGES, "y", value));
                model.put(id(MESSAGES, "x"), value);
                model.put(id(MESSAGES, "y"), value);
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                var queue = ((LiveLedger) ledger).logForTests();
                Commit last;
                try {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    model.put(id(MESSAGES, "first"), new byte[300]);
                    assertTrue(reached.await(2, TimeUnit.MINUTES));
                    // 1,000 removals of keys of one character and one put of 100 bytes: estimated past a run, logged
                    // in one
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 1_000; i++) {
                        batch.delete(MESSAGES, String.valueOf((char) ('a' + i % 26)));
                    }
                    var put = pattern(new Random(43), 100);
                    batch.put(MESSAGES, "x", put);
                    model.put(id(MESSAGES, "x"), put);
                    last = ledger.commit(batch);
                    var frame = queue.queuedForTests().getLast();
                    assertTrue(frame.size() <= LogWriter.RUN_MAX_BODY, "the body takes " + frame.size() + " bytes");
                    assertEquals(0, frame.references());
                } finally {
                    open.countDown();
                }
                last.awaitDurable();
                model.remove(id(MESSAGES, "y"));
                ledger.commit(ledger.newBatch().delete(MESSAGES, "y")).awaitDurable();
                survivor = disk.afterPowerLoss(new Random(47));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                verify(ledger, model);
            }
        });
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("refers from the frame of a large durable commit made right after an empty store opens, as an app-state snapshot is, and recovers it")
    void largeCommitAfterOpen(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, byte[]>();
        CrashFileSystem survivor;
        try (var ledger = open(disk.getPath("/store"), encrypted, 1L << 20)) {
            var random = new Random(53);
            var batch = ledger.newBatch();
            for (var i = 0; i < 20_000; i++) {
                var key = "%032x".formatted(i * 0x9E3779B97F4A7C15L);
                var value = pattern(random, 100 + Math.floorMod(i * 31, 201));
                batch.put(MESSAGES, key, value);
                model.put(id(MESSAGES, key), value);
            }
            ledger.commit(batch).awaitDurable();
            assertEquals(1, ((LiveLedger) ledger).logForTests().piecewiseFramesForTests(), "the snapshot's frame sealed piece by piece");
            verify(ledger, model);
            survivor = disk.afterProcessCrash();
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
            verify(ledger, model);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("logs a valid frame for a resident byte array whose instance a reader writes before the seal, and the commits after it")
    void residentInstance(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var committed = pattern(new Random(11), 4_000);
        var after = pattern(new Random(12), 4_000);
        CrashFileSystem survivor;
        try (var ledger = open(directory, encrypted, 1L << 20)) {
            ledger.awaitDurable();
            var reached = new CountDownLatch(1);
            var open = stallLog(disk, reached);
            Commit commit;
            try {
                ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                assertTrue(reached.await(2, TimeUnit.MINUTES));
                ledger.commit(ledger.newBatch().put(SESSIONS, "session", committed.clone()));
                // Against the contract of a resident key space: the change may reach the log, but never breaks it
                var instance = ledger.get(SESSIONS, "session").orElseThrow();
                Arrays.fill(instance, (byte) 0);
                commit = ledger.commit(ledger.newBatch().put(MESSAGES, "after", after));
            } finally {
                open.countDown();
            }
            commit.awaitDurable();
            survivor = disk.afterProcessCrash();
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
            var session = ledger.get(SESSIONS, "session").orElseThrow();
            assertTrue(Arrays.equals(committed, session) || Arrays.equals(new byte[committed.length], session));
            assertArrayEquals(after, ledger.get(MESSAGES, "after").orElseThrow());
        }
    }

    private static void assertFrame(FrameBody frame, String name, int references, int maxOwn, int size) {
        assertEquals(references, frame.references(), name + ": references");
        assertTrue(frame.chunk(0).length <= maxOwn, name + ": copies on an array of " + frame.chunk(0).length + " bytes");
        assertTrue(frame.size() >= size, name + ": holds " + frame.size() + " bytes");
    }

    @ParameterizedTest(name = "hot slab: {0}")
    @CsvSource({"false", "true"})
    @DisplayName("refers from the frames of its commits to the values its versions keep, whichever way a commit reached them")
    void refers(boolean hot) throws Exception {
        withSlab(hot, _ -> {
            var random = new Random(13);
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), true, 1L << 20)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                Commit last;
                var queue = ((LiveLedger) ledger).logForTests();
                try {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    assertTrue(reached.await(2, TimeUnit.MINUTES));

                    ledger.commit(ledger.newBatch().put(MESSAGES, "own", pattern(random, 4_000)));
                    assertFrame(queue.queuedForTests().getLast(), "a value of its own", 1, 64, 4_000);
                    ledger.commit(ledger.newBatch().put(MESSAGES, "at", pattern(random, FrameBody.RUN_REFERABLE)));
                    assertFrame(queue.queuedForTests().getLast(), "at the threshold", 1, 64, FrameBody.RUN_REFERABLE);
                    ledger.commit(ledger.newBatch().put(MESSAGES, "under", pattern(random, FrameBody.RUN_REFERABLE - 1)));
                    assertFrame(queue.queuedForTests().getLast(), "under the threshold", 0, 512, FrameBody.RUN_REFERABLE - 1);

                    // Enough beside the messages to copy them into an array of their own
                    var chunks = ledger.newBatch();
                    for (var i = 0; i < 8; i++) {
                        chunks.put(MESSAGES, "chunk" + i, pattern(random, 1_400));
                    }
                    chunks.put(SESSIONS, "session", pattern(random, 2_000));
                    ledger.commit(chunks);
                    assertFrame(queue.queuedForTests().getLast(), "chunks", 9, 512, 13_000);

                    // A body past what the writer seals whole refers to nothing
                    var large = ledger.newBatch();
                    for (var i = 0; i < 20; i++) {
                        large.put(MESSAGES, "large" + i, pattern(random, 1_400));
                    }
                    ledger.commit(large);
                    assertFrame(queue.queuedForTests().getLast(), "large", 0, 64 << 10, 28_000);

                    // Resident values: a byte array the store copies, another codec's copy
                    ledger.commit(ledger.newBatch().put(SESSIONS, "copied", pattern(random, 4_000)));
                    assertFrame(queue.queuedForTests().getLast(), "resident byte array", 1, 64, 4_000);
                    ledger.commit(ledger.newBatch().put(NOTES, "note", pattern(random, 4_000)));
                    assertFrame(queue.queuedForTests().getLast(), "resident", 1, 64, 4_000);

                    // A commit that runs alone
                    var alone = ledger.newBatch();
                    alone.put(MESSAGES, "alone", pattern(random, 4_000));
                    alone.deleteRange(NOTES, KeyRange.atLeast("z"));
                    ledger.commit(alone);
                    assertFrame(queue.queuedForTests().getLast(), "alone", 1, 128, 4_000);

                    // A transaction refers to its values of 2 KiB or more
                    var messageValue = pattern(random, 4_000);
                    var encodedValue = pattern(random, 3_000);
                    var noteValue = pattern(random, 4_000);
                    var sessionValue = pattern(random, 1_000);
                    last = ledger.update(transaction -> {
                        transaction.put(MESSAGES, "transaction", messageValue);
                        transaction.putEncoded(MESSAGES, utf8("encoded"), MemorySegment.ofArray(encodedValue));
                        transaction.put(NOTES, "transaction", noteValue);
                        transaction.put(SESSIONS, "transaction", sessionValue);
                    });
                    assertFrame(queue.queuedForTests().getLast(), "transaction", 3, 2_048, 12_000);
                } finally {
                    open.countDown();
                }
                last.awaitDurable();
            }
        });
    }

    // Reads a private field of the engine: a transaction's frame is nowhere else to see before the log takes it
    private static Object field(Class<?> owner, Object instance, String name) throws ReflectiveOperationException {
        var field = owner.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
    }

    private static FrameBody frame(Transaction transaction) throws ReflectiveOperationException {
        var applier = field(LiveTransaction.class, transaction, "applier");
        return (FrameBody) field(LiveLedger.Applier.class, applier, "frame");
    }

    /// Makes the frame of `transaction` claim to be so long that only `room` more bytes of keys and values fit.
    private static void fill(Transaction transaction, int room) {
        try {
            var frame = frame(transaction);
            var key = utf8("filler");
            frame.put(1, key, new byte[0], 0, FrameBody.MAX_BYTES - 2 * FrameBody.WRITE_FIELDS - frame.size() - (int) key.byteSize() - room);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    static Stream<Arguments> oversized() {
        var longKey = "k".repeat(100);
        var writes = List.<Arguments>of(
                Arguments.of("a value it would refer to", (Consumer<Transaction>) transaction -> transaction.put(MESSAGES, "over", new byte[3_000])),
                Arguments.of("a value it would copy", (Consumer<Transaction>) transaction -> transaction.put(MESSAGES, "over", new byte[100])),
                Arguments.of("an encoded value", (Consumer<Transaction>) transaction -> transaction.putEncoded(MESSAGES, utf8("over"), MemorySegment.ofArray(new byte[100]))),
                Arguments.of("a resident byte array", (Consumer<Transaction>) transaction -> transaction.put(SESSIONS, "over", new byte[100])),
                Arguments.of("a removal", (Consumer<Transaction>) transaction -> transaction.delete(MESSAGES, longKey)),
                Arguments.of("a range removal", (Consumer<Transaction>) transaction -> transaction.deleteRange(MESSAGES, KeyRange.atLeast(longKey))));
        return writes.stream().flatMap(write -> Stream.of(true, false).map(encrypted -> Arguments.of(write.get()[0], write.get()[1], encrypted)));
    }

    @ParameterizedTest(name = "{0}, encrypted: {2}")
    @MethodSource("oversized")
    @DisplayName("takes back a transaction whose write its frame has no room for, and goes on healthy")
    void oversizedTransaction(String ignored, Consumer<Transaction> write, boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var random = new Random(17);
        var before = pattern(random, 3_000);
        var after = pattern(random, 3_000);
        var longKey = "k".repeat(100);
        CrashFileSystem survivor;
        try (var ledger = open(directory, encrypted, 1L << 20)) {
            ledger.commit(ledger.newBatch().put(MESSAGES, "before", before).put(MESSAGES, longKey, before)).awaitDurable();
            assertThrows(IllegalArgumentException.class, () -> ledger.update(transaction -> {
                transaction.put(MESSAGES, "rolled back", new byte[300]);
                transaction.put(NOTES, "rolled back", new byte[300]);
                fill(transaction, 20);
                write.accept(transaction);
            }));
            assertTrue(ledger.get(MESSAGES, "rolled back").isEmpty());
            assertTrue(ledger.get(NOTES, "rolled back").isEmpty());
            assertArrayEquals(before, ledger.get(MESSAGES, "before").orElseThrow());
            assertEquals(2, ledger.size(MESSAGES));
            assertEquals(0, ledger.size(NOTES));
            ledger.commit(ledger.newBatch().put(MESSAGES, "after", after)).awaitDurable();
            assertEquals(3, ledger.size(MESSAGES));
            survivor = disk.afterPowerLoss(new Random(19));
        }
        try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
            assertArrayEquals(before, ledger.get(MESSAGES, "before").orElseThrow());
            assertArrayEquals(after, ledger.get(MESSAGES, "after").orElseThrow());
            assertArrayEquals(before, ledger.get(MESSAGES, longKey).orElseThrow());
            assertTrue(ledger.get(MESSAGES, "rolled back").isEmpty());
            assertEquals(3, ledger.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("lets a transaction catch a write its frame has no room for, and find nothing of that write")
    void caughtInTransaction() {
        var disk = CrashFileSystem.create();
        try (var ledger = open(disk.getPath("/store"), true, 1L << 20)) {
            ledger.commit(ledger.newBatch().put(MESSAGES, "kept", new byte[300]).put(MESSAGES, "other", new byte[300]));
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                fill(transaction, 20);
                FrameBody frame;
                try {
                    frame = frame(transaction);
                } catch (ReflectiveOperationException exception) {
                    throw new AssertionError(exception);
                }
                var size = frame.size();
                var references = frame.references();
                assertThrows(IllegalArgumentException.class, () -> transaction.put(MESSAGES, "over", new byte[3_000]));
                assertTrue(transaction.get(MESSAGES, "over").isEmpty());
                assertThrows(IllegalArgumentException.class, () -> transaction.delete(MESSAGES, "k".repeat(100)));
                assertThrows(IllegalArgumentException.class, () -> transaction.deleteRange(MESSAGES, KeyRange.atLeast("k".repeat(100))));
                assertTrue(transaction.get(MESSAGES, "kept").isPresent());
                assertEquals(size, frame.size());
                assertEquals(references, frame.references());
                // The filler is no value a writer could read, so this frame must not reach the log
                transaction.delete(MESSAGES, "other");
                assertTrue(transaction.get(MESSAGES, "other").isEmpty());
                throw new IllegalStateException("taken back");
            }));
            assertEquals(2, ledger.size(MESSAGES));
        }
    }
}
