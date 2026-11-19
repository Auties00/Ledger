package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

    /// {@return the arrays `body` refers to, by identity: those of its pieces, or of a split body's section}
    private static Set<byte[]> referenced(FrameBody body) {
        var arrays = Collections.<byte[]>newSetFromMap(new IdentityHashMap<>());
        if (body.split()) {
            for (var reference = 0; reference < body.references(); reference++) {
                arrays.add(body.referenceArray(reference));
            }
            return arrays;
        }
        for (var piece = 1; piece < body.pieces(); piece += 2) {
            arrays.add(body.pieceArray(piece));
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

    @Test
    @DisplayName("of a commit the log seals whole leaves its long values out of its array; of a larger one refers to nothing")
    void forCommit() {
        var random = new Random(3);
        var pool = new HeapPool(1L << 26, false);
        var single = new ArrayWriteBatch(this, pool, BY_NAME);
        single.put(MESSAGES, "single", pattern(random, 1_000));
        single.put(MESSAGES, "small", pattern(random, FrameBody.RUN_REFERABLE - 1));
        single.prepare(false, null, Map.of());
        var body = FrameBody.forCommit(single, null, null);
        assertEquals(FrameBody.RUN_REFERABLE, body.referable());
        var estimate = FrameBody.capacityFor(single, null);
        assertTrue(estimate > 1_000);
        assertEquals(Math.max(16, estimate - 1_000), body.pieceArray(0).length, "the room of the body");

        // The value whose unit holds it is no reference
        var units = new PageRef[]{new PageRef(1, 1, 0, 0, 0), null};
        assertEquals(Math.max(16, FrameBody.capacityFor(single, units)), FrameBody.forCommit(single, units, null).pieceArray(0).length);

        var large = new ArrayWriteBatch(this, pool, BY_NAME);
        for (var i = 0; i < 20; i++) {
            large.put(MESSAGES, "message" + i, pattern(random, 1_000));
        }
        large.prepare(false, null, Map.of());
        var largeBody = FrameBody.forCommit(large, null, null);
        assertEquals(FrameBody.NOT_REFERABLE, largeBody.referable());
        assertEquals(FrameBody.capacityFor(large, null), largeBody.pieceArray(0).length);
        assertTrue(FrameBody.capacityFor(large, null) > LogWriter.RUN_MAX_BODY);
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
        assertTrue(frame.pieceArray(0).length <= maxOwn, name + ": copies on an array of " + frame.pieceArray(0).length + " bytes");
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
                var pieces = frame.pieces();
                assertThrows(IllegalArgumentException.class, () -> transaction.put(MESSAGES, "over", new byte[3_000]));
                assertTrue(transaction.get(MESSAGES, "over").isEmpty());
                assertThrows(IllegalArgumentException.class, () -> transaction.delete(MESSAGES, "k".repeat(100)));
                assertThrows(IllegalArgumentException.class, () -> transaction.deleteRange(MESSAGES, KeyRange.atLeast("k".repeat(100))));
                assertTrue(transaction.get(MESSAGES, "kept").isPresent());
                assertEquals(size, frame.size());
                assertEquals(pieces, frame.pieces());
                // The filler is no value a writer could read, so this frame must not reach the log
                transaction.delete(MESSAGES, "other");
                assertTrue(transaction.get(MESSAGES, "other").isEmpty());
                throw new IllegalStateException("taken back");
            }));
            assertEquals(2, ledger.size(MESSAGES));
        }
    }

    /// A write of a commit body as recovery reads it: its key space, key, and value or `null`, with the value frame
    /// that holds the value (-1 for the body itself) and where the value lies in that frame's body.
    private record Read(int keyspace, byte[] key, byte[] value, int frame, long at) {
    }

    /// The frames of a split commit as the log writer seals them, before sealing: its head, the own part, and the body of
    /// each of its value frames.
    private record SplitFrames(byte[] head, byte[][] values) {
        /// {@return the value frames as recovery hands them over}
        FrameBody.ValueFrames valueFrames() {
            var frames = new FrameBody.ValueFrames(values.length);
            for (var i = 0; i < values.length; i++) {
                frames.add(values[i], values[i].length, 1_000L * (i + 1));
            }
            return frames;
        }
    }

    /// {@return the frames the log writer seals for the ended split body `body`: its own bytes, and each value frame's
    /// references gathered}
    private static SplitFrames frames(FrameBody body) {
        var head = new byte[body.headBytes()];
        var at = 0;
        for (var chunk = 0; chunk < body.chunkCount(); chunk++) {
            System.arraycopy(body.chunk(chunk), 0, head, at, body.chunkLength(chunk));
            at += body.chunkLength(chunk);
        }
        assertEquals(head.length, at);
        var values = new byte[body.valueFrames()][];
        for (var frame = 0; frame < values.length; frame++) {
            values[frame] = new byte[body.valueFrameBytes(frame)];
            var offset = 0;
            for (var reference = body.valueFrameFirst(frame); reference < body.valueFrameEnd(frame); reference++) {
                var length = body.referenceLength(reference);
                System.arraycopy(body.referenceArray(reference), body.referenceOffset(reference), values[frame], offset, length);
                offset += length;
            }
            assertEquals(values[frame].length, offset);
        }
        return new SplitFrames(head, values);
    }

    /// {@return the puts and removals of the commit body `bytes`, read as recovery reads them, with the value frames
    /// `values` of a split body, or `null`}
    private static List<Read> read(byte[] bytes, FrameBody.ValueFrames values) {
        var reads = new ArrayList<Read>();
        var view = MemorySegment.ofArray(bytes).asReadOnly();
        FrameBody.readCommit(bytes, view, 0, bytes.length, values, new FrameBody.Visitor() {
            @Override
            public void put(int keyspace, int keyAt, int keyLength, int valueAt, int valueLength) {
                reads.add(new Read(keyspace, Arrays.copyOfRange(bytes, keyAt, keyAt + keyLength),
                        Arrays.copyOfRange(bytes, valueAt, valueAt + valueLength), -1, valueAt));
            }

            @Override
            public void putSection(int keyspace, int keyAt, int keyLength, int frame, int valueAt, int valueLength) {
                var body = values.body(frame);
                assertTrue(valueAt + valueLength <= values.length(frame));
                reads.add(new Read(keyspace, Arrays.copyOfRange(bytes, keyAt, keyAt + keyLength),
                        Arrays.copyOfRange(body, valueAt, valueAt + valueLength), frame, valueAt));
            }

            @Override
            public void delete(int keyspace, int keyAt, int keyLength) {
                reads.add(new Read(keyspace, Arrays.copyOfRange(bytes, keyAt, keyAt + keyLength), null, -1, -1));
            }

            @Override
            public void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
                throw new AssertionError("no range removal was written");
            }

            @Override
            public void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
                throw new AssertionError("no unit was written");
            }
        });
        return reads;
    }

    /// {@return the length of the writes a split body's head starts with}
    private static int writesLength(byte[] bytes) {
        return (bytes[0] & 0xFF) | (bytes[1] & 0xFF) << 8 | (bytes[2] & 0xFF) << 16 | (bytes[3] & 0xFF) << 24;
    }

    @Test
    @DisplayName("a split body takes the paged values of a rope batch as a reference per run of a chunk, as value frames that never split a reference, and reads back every write from them")
    void splitBody() {
        var random = new Random(41);
        var pool = new HeapPool(1L << 28, false);
        var batch = new ArrayWriteBatch(this, pool, BY_NAME);
        var values = new ArrayList<byte[]>();
        for (var i = 0; i < 400; i++) {
            // One value on an array of its own in the middle of the rope's values, and a resident one
            var value = pattern(random, i == 200 ? 200_000 : 1_000 + random.nextInt(400));
            batch.put(i == 300 ? SESSIONS : MESSAGES, "message" + i, value);
            values.add(value);
        }
        batch.prepare(false, null, Map.of());
        assertTrue(batch.valuesInOrder());
        assertTrue(batch.chunksForTests() > 1, "the batch is no rope");
        var body = FrameBody.forCommit(batch, null, pool);
        assertTrue(body.split());
        assertEquals(FrameBody.COMMIT_SPLIT, body.commitType());
        assertEquals(FrameBody.sectionBytes(batch, null), values.stream().mapToLong(value -> value.length).sum() - values.get(300).length);
        // Where each run of the section would break: a new array, or a value that does not follow the last on it
        var runs = 0;
        byte[] last = null;
        var end = 0;
        var section = 0L;
        for (var i = 0; i < batch.size(); i++) {
            var key = batch.key(i);
            var length = (int) batch.valueBytes(i);
            if (batch.keyspace(i).residency() == Keyspace.Residency.RESIDENT) {
                body.put(1, key, batch.value(i));
                continue;
            }
            var own = batch.ownValue(i);
            var array = own != null ? own : batch.homeArray(i);
            var offset = own != null ? 0 : batch.homeOffset(i);
            if (array != last || offset != end) {
                runs++;
            }
            last = array;
            end = offset + length;
            body.putSection(0, key, array, offset, length);
            assertEquals(-1 - section, body.sectionOffset(length));
            section += length;
        }
        body.finishSplit();
        assertEquals(runs, body.references());
        // The own value breaks the run of its chunk, and the rope's last chunk is a copy
        assertTrue(runs <= batch.chunksForTests() + 2, runs + " references for " + batch.chunksForTests() + " chunks");
        assertEquals(section + body.headBytes(), body.size());
        // Each reference of 64 KiB or more is a value frame of its own, shorter ones share one of at most a chunk
        var frameBytes = 0L;
        for (var frame = 0; frame < body.valueFrames(); frame++) {
            var first = body.valueFrameFirst(frame);
            var references = body.valueFrameEnd(frame) - first;
            assertTrue(references >= 1, "an empty value frame");
            var bytes = 0;
            for (var reference = first; reference < body.valueFrameEnd(frame); reference++) {
                bytes += body.referenceLength(reference);
                if (body.referenceLength(reference) >= FrameBody.VALUES_ALONE) {
                    assertEquals(1, references, "a long reference shares value frame " + frame);
                }
            }
            assertEquals(bytes, body.valueFrameBytes(frame));
            assertTrue(references == 1 || bytes <= FrameBody.CHUNK_BYTES, "value frame " + frame + " of " + bytes + " bytes");
            frameBytes += bytes;
        }
        assertEquals(section, frameBytes);
        assertEquals(body.headBytes() + section + (1 + body.valueFrames()) * (LogWriter.FRAME_HEADER + 16L),
                body.sealedBytes(16));
        var expected = frames(body);
        var writes = writesLength(expected.head());
        assertArrayEquals(FrameBody.splitTable(expected.head(), 0, expected.head().length),
                Arrays.stream(expected.values()).mapToInt(frame -> frame.length).toArray());
        var reads = read(expected.head(), expected.valueFrames());
        assertEquals(values.size(), reads.size());
        var frame = 0;
        var next = 0L;
        for (var i = 0; i < reads.size(); i++) {
            var read = reads.get(i);
            assertArrayEquals(values.get(i), read.value(), "write " + i);
            assertArrayEquals(("message" + i).getBytes(StandardCharsets.UTF_8), read.key());
            if (i == 300) {
                assertEquals(-1, read.frame(), "the resident value is in the head");
                assertTrue(read.at() < writes, "the resident value is among the writes");
                continue;
            }
            // The values of the section, back to back, each in the value frame that holds it
            if (next == expected.values()[frame].length) {
                frame++;
                next = 0;
            }
            assertEquals(frame, read.frame(), "the value frame of write " + i);
            assertEquals(next, read.at(), "where write " + i + " lies");
            next += read.value().length;
        }
        assertEquals(expected.values().length - 1, frame);
        // The batch starts again on chunks of the pool, which never are the section's
        batch.finish();
        for (var i = 0; i < 400; i++) {
            batch.put(MESSAGES, "again" + i, new byte[1_200]);
        }
        var again = frames(body);
        assertArrayEquals(expected.head(), again.head());
        for (var i = 0; i < expected.values().length; i++) {
            assertArrayEquals(expected.values()[i], again.values()[i], "value frame " + i);
        }
        body.release();
        assertEquals(0, body.references(), "a released body keeps the arrays it referred to");
    }

    @Test
    @DisplayName("a released split body made without a pool lets go of its section's arrays too")
    void releasedSplitWithoutPool() {
        var body = FrameBody.forSplit(16, null);
        body.putSection(0, utf8("a"), new byte[100_000], 0, 100_000);
        body.finishSplit();
        assertEquals(1, body.references());
        body.release();
        assertEquals(0, body.references());
        assertTrue(body.split());
    }

    @Test
    @DisplayName("a section extends its last reference only by a value that follows it on the same array, and gathers short references into a value frame")
    void sectionRuns() {
        var first = new byte[100];
        var second = new byte[100];
        new Random(5).nextBytes(first);
        new Random(6).nextBytes(second);
        var body = FrameBody.forSplit(16, null);
        body.putSection(0, utf8("a"), first, 0, 10);
        // An inline put between two values of the section is in the head, which breaks no run
        body.put(1, utf8("inline"), utf8("held"));
        body.putSection(0, utf8("b"), first, 10, 5);
        assertEquals(1, body.references());
        // A gap
        body.putSection(0, utf8("c"), first, 20, 5);
        assertEquals(2, body.references());
        // Another array at the offset the last reference ends at
        body.putSection(0, utf8("d"), second, 25, 5);
        assertEquals(3, body.references());
        body.putSection(0, utf8("e"), second, 30, 0);
        body.putSection(0, utf8("f"), second, 30, 7);
        assertEquals(3, body.references());
        // An array before the last one, at the end of its reference
        body.putSection(0, utf8("g"), first, 25, 3);
        assertEquals(4, body.references());
        body.finishSplit();
        // Four short references, one value frame
        assertEquals(1, body.valueFrames());
        assertEquals(10 + 5 + 5 + 5 + 7 + 3, body.valueFrameBytes(0));
        var frames = frames(body);
        var reads = read(frames.head(), frames.valueFrames());
        assertEquals(List.of("a", "inline", "b", "c", "d", "e", "f", "g"),
                reads.stream().map(read -> new String(read.key(), StandardCharsets.UTF_8)).toList());
        assertArrayEquals(Arrays.copyOfRange(first, 0, 10), reads.get(0).value());
        assertArrayEquals("held".getBytes(StandardCharsets.UTF_8), reads.get(1).value());
        assertArrayEquals(Arrays.copyOfRange(first, 10, 15), reads.get(2).value());
        assertArrayEquals(Arrays.copyOfRange(first, 20, 25), reads.get(3).value());
        assertArrayEquals(Arrays.copyOfRange(second, 25, 30), reads.get(4).value());
        assertArrayEquals(new byte[0], reads.get(5).value());
        assertArrayEquals(Arrays.copyOfRange(second, 30, 37), reads.get(6).value());
        assertArrayEquals(Arrays.copyOfRange(first, 25, 28), reads.get(7).value());
        // The writes, then a table of one value frame
        assertTrue(writesLength(frames.head()) < frames.head().length);
    }

    @Test
    @DisplayName("value frames: a reference of 64 KiB or more alone, short ones gathered up to a chunk, never splitting one")
    void valueFrameGrouping() {
        var arrays = new ArrayList<byte[]>();
        var body = FrameBody.forSplit(16, null);
        // Lengths of the references in order, each on an array of its own
        int[] lengths = {1_000, 2_000, FrameBody.VALUES_ALONE, 500, FrameBody.CHUNK_BYTES - 600, 700, 300,
                FrameBody.VALUES_ALONE - 1, FrameBody.VALUES_ALONE - 1, FrameBody.VALUES_ALONE - 1, FrameBody.VALUES_ALONE - 1,
                FrameBody.VALUES_ALONE - 1, 3 << 20};
        for (var i = 0; i < lengths.length; i++) {
            var array = new byte[lengths[i]];
            Arrays.fill(array, (byte) i);
            arrays.add(array);
            body.putSection(0, utf8("k" + i), array, 0, lengths[i]);
        }
        body.finishSplit();
        assertEquals(lengths.length, body.references());
        var frames = new ArrayList<List<Integer>>();
        for (var frame = 0; frame < body.valueFrames(); frame++) {
            var references = new ArrayList<Integer>();
            for (var reference = body.valueFrameFirst(frame); reference < body.valueFrameEnd(frame); reference++) {
                references.add(reference);
            }
            frames.add(references);
        }
        // 1,000 and 2,000 gather; 64 KiB alone; 500 alone as a long one follows it; then 700 and 300 gather with three
        // references just short of 64 KiB, the fourth passing a chunk, which starts the next; the 3 MiB one alone
        assertEquals(List.of(List.of(0, 1), List.of(2), List.of(3), List.of(4), List.of(5, 6, 7, 8, 9), List.of(10, 11),
                List.of(12)), frames);
        var split = frames(body);
        var reads = read(split.head(), split.valueFrames());
        for (var i = 0; i < lengths.length; i++) {
            assertArrayEquals(arrays.get(i), reads.get(i).value(), "write " + i);
        }
    }

    @Test
    @DisplayName("a split body whose section stays empty is its head alone, with a table of no value frame, and an empty value left to it reads as a put of the head")
    void emptySection() {
        var body = FrameBody.forSplit(16, new HeapPool(1L << 24, false));
        body.put(1, utf8("session"), utf8("resident"));
        body.delete(0, utf8("removed"));
        body.putSection(0, utf8("empty"), new byte[10], 4, 0);
        body.finishSplit();
        assertEquals(0, body.references());
        assertEquals(0, body.valueFrames());
        var frames = frames(body);
        assertEquals(0, frames.values().length);
        assertEquals(0, FrameBody.splitTable(frames.head(), 0, frames.head().length).length);
        var reads = read(frames.head(), frames.valueFrames());
        assertEquals(3, reads.size());
        assertArrayEquals("resident".getBytes(StandardCharsets.UTF_8), reads.get(0).value());
        assertEquals(null, reads.get(1).value());
        // Recovery passes it on as a put whose value lies in no value frame, which a replay could not index
        assertArrayEquals("empty".getBytes(StandardCharsets.UTF_8), reads.get(2).key());
        assertArrayEquals(new byte[0], reads.get(2).value());
        assertEquals(-1, reads.get(2).frame());
    }

    /// {@return the frames of a split body of three puts that leave their values to the section, one long enough for a
    /// value frame of its own, and one inline}
    private static SplitFrames splitFrames() {
        var body = FrameBody.forSplit(16, null);
        var values = new byte[64];
        new Random(9).nextBytes(values);
        var alone = new byte[FrameBody.VALUES_ALONE];
        new Random(10).nextBytes(alone);
        body.putSection(0, utf8("a"), values, 0, 30);
        body.put(1, utf8("b"), utf8("inline"));
        body.putSection(0, utf8("c"), values, 30, 34);
        body.putSection(0, utf8("d"), alone, 0, alone.length);
        body.finishSplit();
        assertEquals(2, body.valueFrames());
        return frames(body);
    }

    static Stream<Arguments> malformedSplits() {
        var valid = splitFrames();
        var head = valid.head();
        var writes = writesLength(head);
        var values = valid.values();
        return Stream.of(
                Arguments.of("a writes length below its own four bytes", withWritesLength(head, 3), values),
                Arguments.of("a writes length past the head", withWritesLength(head, head.length + 1), values),
                Arguments.of("a negative writes length", withWritesLength(head, -1), values),
                Arguments.of("a head shorter than its writes length", Arrays.copyOf(head, 3), values),
                Arguments.of("writes that end inside a write", withWritesLength(head, writes - 1), values),
                Arguments.of("a table a byte late", withWritesLength(head, writes + 1), values),
                Arguments.of("bytes after the table", Arrays.copyOf(head, head.length + 1), values),
                Arguments.of("a value frame fewer than the table", head, Arrays.copyOf(values, 1)),
                Arguments.of("a value frame more than the table", head, new byte[][]{values[0], values[1], new byte[4]}),
                Arguments.of("a value frame shorter than the table", head, new byte[][]{Arrays.copyOf(values[0], values[0].length - 1), values[1]}),
                Arguments.of("a value that spans two value frames", withTable(head, writes, values[0].length - 1, values[1].length + 1),
                        new byte[][]{Arrays.copyOf(values[0], values[0].length - 1),
                                concat(Arrays.copyOfRange(values[0], values[0].length - 1, values[0].length), values[1])}),
                Arguments.of("values that end before the last value frame", withTable(head, writes, values[0].length, values[1].length + 1),
                        new byte[][]{values[0], Arrays.copyOf(values[1], values[1].length + 1)}));
    }

    private static byte[] withWritesLength(byte[] bytes, int writes) {
        var copy = bytes.clone();
        copy[0] = (byte) writes;
        copy[1] = (byte) (writes >>> 8);
        copy[2] = (byte) (writes >>> 16);
        copy[3] = (byte) (writes >>> 24);
        return copy;
    }

    /// {@return `head` with its table replaced by one of value frames of `lengths`}
    private static byte[] withTable(byte[] head, int writes, int... lengths) {
        var table = new java.io.ByteArrayOutputStream();
        table.writeBytes(Arrays.copyOf(head, writes));
        writeVarInt(table, lengths.length);
        for (var length : lengths) {
            writeVarInt(table, length);
        }
        return table.toByteArray();
    }

    private static void writeVarInt(java.io.ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        var bytes = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, bytes, first.length, second.length);
        return bytes;
    }

    /// {@return value frames of the bodies `values`, as recovery hands them over}
    private static FrameBody.ValueFrames valueFrames(byte[][] values) {
        return new SplitFrames(null, values).valueFrames();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedSplits")
    @DisplayName("recovery refuses a malformed split commit")
    void malformedSplit(String ignored, byte[] head, byte[][] values) {
        assertThrows(LedgerCorruptedException.class, () -> read(head, valueFrames(values)));
    }

    @Test
    @DisplayName("the valid frames of a malformed split's cases read back, so each case fails on its damage alone")
    void validSplit() {
        var valid = splitFrames();
        assertEquals(4, read(valid.head(), valid.valueFrames()).size());
    }

    @Test
    @DisplayName("recovery refuses a section put in a body that is not split")
    void sectionOutsideSplit() {
        var valid = splitFrames();
        var writes = writesLength(valid.head());
        // The writes of the head, read as the body of a frame of type COMMIT
        var bytes = Arrays.copyOfRange(valid.head(), 4, writes);
        assertThrows(LedgerCorruptedException.class, () -> read(bytes, null));
        assertThrows(LedgerCorruptedException.class, () -> FrameBody.readCommitKinds(bytes, 0, bytes.length, false,
                new FrameBody.KindVisitor() {
                    @Override
                    public void put(int keyspace) {
                    }

                    @Override
                    public void remove(int keyspace) {
                    }

                    @Override
                    public void putExternal(int keyspace, long length, PageRef ref) {
                    }
                }));
        // And a split head read as a body that is not
        assertThrows(LedgerCorruptedException.class, () -> read(valid.head(), null));
    }

    static Stream<Arguments> splitStores() {
        return Stream.of(Arguments.of(true, 1L << 20), Arguments.of(false, 1L << 20), Arguments.of(true, 64L << 10),
                Arguments.of(false, 64L << 10));
    }

    @ParameterizedTest(name = "encrypted: {0}, large values from {1}")
    @MethodSource("splitStores")
    @DisplayName("queues a split frame for a rope batch, which keeps its values while the batch starts again at once on pooled chunks, and recovers them")
    void splitFrames(boolean encrypted, long largeValues) throws Exception {
        withSlab(false, pool -> {
            var random = new Random(largeValues + (encrypted ? 1 : 0));
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(disk.getPath("/store"), encrypted, largeValues)) {
                ledger.awaitDurable();
                // A unit first, whose epoch frame is then durable: a commit with a unit waits for it
                var unit = pattern(random, 100_000);
                ledger.commit(ledger.newBatch().put(MESSAGES, "unit", unit)).awaitDurable();
                model.put(id(MESSAGES, "unit"), unit);
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                var queue = ((LiveLedger) ledger).logForTests();
                Commit last = null;
                try {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    model.put(id(MESSAGES, "first"), new byte[300]);
                    assertTrue(reached.await(2, TimeUnit.MINUTES));
                    var batch = ledger.newBatch();
                    for (var round = 0; round < 4; round++) {
                        for (var i = 0; i < 300; i++) {
                            var key = "r" + round + ":" + i;
                            // A value of 200 KiB on an array of its own, below the default large values; values of
                            // 64 to 128 KiB on the rope, which the smaller large values send to units
                            var length = i == 150 ? 200 << 10 : i % 97 == 5 ? 70_000 + random.nextInt(50_000) : 1_000 + random.nextInt(1_000);
                            var value = pattern(random, length);
                            batch.put(MESSAGES, key, value);
                            model.put(id(MESSAGES, key), value);
                        }
                        var session = "s" + round;
                        var state = pattern(random, 3_000);
                        batch.put(SESSIONS, session, state);
                        model.put(id(SESSIONS, session), state);
                        // The same batch starts again right after, on chunks of the pool, while the frame waits
                        last = ledger.commit(batch);
                        var frame = queue.queuedForTests().getLast();
                        assertTrue(frame.split(), "the frame of round " + round + " is not split");
                        assertTrue(frame.references() < 40, frame.references() + " references");
                        assertTrue(frame.valueFrames() > 0 && frame.valueFrames() <= frame.references(), frame.valueFrames() + " value frames");
                        assertTrue(frame.headBytes() < 64 << 10, "the frame copies its values");
                    }
                    pool.assertNoneFree(ledger);
                    verify(ledger, model);
                } finally {
                    open.countDown();
                }
                last.awaitDurable();
                verify(ledger, model);
                survivor = disk.afterPowerLoss(new Random(7));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, largeValues)) {
                verify(ledger, model);
                ledger.checkpoint();
                verify(ledger, model);
            }
        });
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a split commit's group seals into exactly the bytes its body says its head and value frames take, as the writer counts them for its rollover and its write through")
    void splitSealedBytes(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store"))
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NOTES))
                .segmentSize(64L << 20)
                .build()) {
            ledger.commit(ledger.newBatch().put(MESSAGES, "warm", new byte[10])).awaitDurable();
            var log = ledger.logForTests();
            var reached = new CountDownLatch(1);
            var open = stallLog(disk, reached);
            Commit last;
            List<FrameBody> bodies;
            try {
                // A commit whose group's write holds the writer, so the next frame waits in the queue
                ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                assertTrue(reached.await(2, TimeUnit.MINUTES));
                var random = new Random(3);
                var batch = ledger.newBatch();
                for (var i = 0; i < 2_000; i++) {
                    // Values of the rope, a few long enough for a value frame of their own, and resident ones between
                    var length = i % 333 == 7 ? 90_000 : 200 + random.nextInt(800);
                    batch.put(i % 50 == 0 ? SESSIONS : MESSAGES, "k" + i, pattern(random, length));
                }
                last = ledger.commit(batch);
                bodies = log.queuedForTests();
            } finally {
                open.countDown();
            }
            assertEquals(1, bodies.size());
            var split = bodies.getFirst();
            assertTrue(split.split());
            assertTrue(split.valueFrames() > 2, split.valueFrames() + " value frames");
            var overhead = encrypted ? LogWriter.TAG_BYTES : Integer.BYTES;
            var sealed = split.sealedBytes(overhead);
            last.awaitDurable();
            // The split commit's group, the last, holds its frames alone
            assertEquals(sealed, log.groupEnd() - log.groupStart(), "the bytes the group sealed into");
        }
    }

    @Test
    @DisplayName("a split body whose own part passes the largest body of a run starts on a chunk of the pool and gives it back on release; a smaller one, or one without a pool, takes an array of its own")
    void splitOwnPartOnChunk() {
        var pool = new HeapPool(1L << 26, false);
        var large = FrameBody.forSplit(LogWriter.RUN_MAX_BODY + 1, pool);
        assertEquals(FrameBody.CHUNK_BYTES, large.chunk(0).length);
        assertTrue(large.holdsPoolForTests());
        large.finishSplit();
        large.handOff();
        large.release();
        assertFalse(large.holdsPoolForTests());
        var small = FrameBody.forSplit(LogWriter.RUN_MAX_BODY - 8, pool);
        assertTrue(small.chunk(0).length < FrameBody.CHUNK_BYTES);
        var unpooled = FrameBody.forSplit(LogWriter.RUN_MAX_BODY + 1, null);
        assertTrue(unpooled.chunk(0).length < FrameBody.CHUNK_BYTES || unpooled.chunk(0).length == LogWriter.RUN_MAX_BODY + 1 + Integer.BYTES);
    }
}
