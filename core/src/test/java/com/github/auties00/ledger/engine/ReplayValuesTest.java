package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What the versions of the commits an open replays keep: copies of their paged values in shared chunks or arrays of
/// their own, never the decrypted frame bodies, with the values, counts and range removals unchanged through reads,
/// scans, checkpoints, later writes, concurrent readers and further crashes.
class ReplayValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    /// The lengths around the bounds of a shared value and of a quarter of a chunk.
    private static final int[] BOUNDARY_LENGTHS = {0, 1, ReplayValues.SHARED_BYTES - 1, ReplayValues.SHARED_BYTES,
            ReplayValues.SHARED_BYTES + 1, 64 * 1024, 64 * 1024 + 1, ReplayValues.CHUNK_BYTES - 1, ReplayValues.CHUNK_BYTES};

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a replayed paged value is on a chunk of its own replay or an array of its own, never on its frame body, and reads, scans and checkpoints return it unchanged")
    void replayedValuesLeaveTheBody(boolean encrypted, @TempDir Path directory) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var expected = expectedValues();
        try (var ledger = (LiveLedger) open(directory, encryption)) {
            for (var commit = 0; commit < 200; commit++) {
                var batch = ledger.newBatch().put(SESSIONS, new byte[]{(byte) commit}, filled(1_500, commit));
                for (var i = 0; i < 10; i++) {
                    var key = messageKey(commit, i);
                    var value = filled(1_400, commit * 10 + i);
                    batch.put(MESSAGES, key, value);
                    expected.put(ByteBuffer.wrap(key), value);
                }
                ledger.commit(batch).awaitDurable();
            }
            for (var i = 0; i < BOUNDARY_LENGTHS.length; i++) {
                var key = boundaryKey(i);
                var value = filled(BOUNDARY_LENGTHS[i], 100 + i);
                ledger.commit(ledger.newBatch().put(MESSAGES, key, value).put(SESSIONS, new byte[]{-1}, filled(64, i)))
                        .awaitDurable();
                expected.put(ByteBuffer.wrap(key), value);
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = (LiveLedger) open(directory, encryption)) {
            var chunks = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
            for (var entry : expected.entrySet()) {
                var key = entry.getKey().array();
                var length = entry.getValue().length;
                var version = head(ledger, key);
                var source = version.sourceForTests();
                if (ReplayValues.shared(length)) {
                    var chunk = assertInstanceOf(MemorySegment.class, source, "a shared value of " + length + " bytes");
                    assertEquals(ReplayValues.CHUNK_BYTES, chunk.byteSize(), "the chunk of a value of " + length + " bytes");
                    assertTrue(chunk.isReadOnly());
                    chunks.add(chunk);
                } else {
                    var own = assertInstanceOf(byte[].class, source, "a value of " + length + " bytes");
                    assertEquals(length, own.length);
                }
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, key).orElseThrow());
            }
            // 2,000 values of 1,400 bytes, 187 to a chunk, and the boundary values that share one
            assertTrue(chunks.size() <= 13, "the replayed values took " + chunks.size() + " chunks");
            assertContents(ledger, expected);
            assertEquals(201, ledger.size(SESSIONS));
            ledger.checkpoint();
            assertContents(ledger, expected);
        }
        try (var ledger = open(directory, encryption)) {
            assertContents(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, encrypted);
    }

    @Test
    @DisplayName("the replayed values stay as written while later batches take and fill the pool's arrays")
    void replayedValuesSurvivePooledArrays(@TempDir Path directory) throws Exception {
        var expected = expectedValues();
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            for (var commit = 0; commit < 50; commit++) {
                var batch = ledger.newBatch().put(SESSIONS, new byte[]{(byte) commit}, filled(3_000, commit));
                for (var i = 0; i < 20; i++) {
                    var key = messageKey(commit, i);
                    var value = filled(2_000, commit + i);
                    batch.put(MESSAGES, key, value);
                    expected.put(ByteBuffer.wrap(key), value);
                }
                ledger.commit(batch).awaitDurable();
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            for (var round = 0; round < 50; round++) {
                // Each batch takes arrays of the pool and fills them with other bytes
                var other = ledger.newBatch().put(SESSIONS, new byte[]{1, (byte) round}, filled(3_000, 0x7F));
                for (var i = 0; i < 100; i++) {
                    other.put(MESSAGES, new byte[]{9, (byte) round, (byte) i}, filled(2_000, 0x55));
                }
                ledger.commit(other).awaitDurable();
            }
            for (var entry : expected.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, entry.getKey().array()).orElseThrow());
            }
            ledger.checkpoint();
            for (var entry : expected.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, entry.getKey().array()).orElseThrow());
            }
        }
    }

    @Test
    @DisplayName("a replayed overwrite is not sampled: the key space's overwrite counters start empty after a recovery")
    void replayDoesNotSampleOverwrites(@TempDir Path directory) throws Exception {
        long first;
        long last;
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            // In the trees, so the replay finds versions to overwrite only among its own
            ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{0}, new byte[8])).awaitDurable();
            ledger.checkpoint();
            first = ledger.latest() + 1;
            for (var commit = 0; commit < 400; commit++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{(byte) (commit % 20)}, filled(300, commit))).awaitDurable();
            }
            last = ledger.latest();
            ledger.closeWithoutCheckpoint();
        }
        var sampled = 0;
        for (var sequence = first; sequence <= last; sequence++) {
            if (ValueSlab.Overwrites.sampled(sequence)) {
                sampled++;
            }
        }
        assertTrue(sampled > 0, "no replayed commit has a sampled sequence");
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            var overwrites = ledger.state(MESSAGES).overwrites;
            assertEquals(0, overwrites.sampledPutsForTests(), "the replay sampled its puts");
            assertTrue(overwrites.worthwhile());
            for (var key = 0; key < 20; key++) {
                var round = 380 + key;
                assertArrayEquals(filled(300, round), ledger.get(MESSAGES, new byte[]{(byte) key}).orElseThrow());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a replayed range removal keeps removing its range, and counting it, after the replay, a checkpoint and a reopen")
    void replayedRangeRemoval(boolean encrypted, @TempDir Path directory) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        try (var ledger = (LiveLedger) open(directory, encryption)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 40; i++) {
                batch.put(MESSAGES, new byte[]{5, (byte) i}, filled(500, i));
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            var replayed = ledger.newBatch();
            for (var i = 40; i < 60; i++) {
                replayed.put(MESSAGES, new byte[]{5, (byte) i}, filled(500, i));
            }
            ledger.commit(replayed).awaitDurable();
            ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, filled(2_000, 1))
                    .deleteRange(MESSAGES, KeyRange.between(new byte[]{5, 10}, new byte[]{5, 50}))).awaitDurable();
            ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{5, 20}, filled(500, 77))).awaitDurable();
            ledger.closeWithoutCheckpoint();
        }
        for (var round = 0; round < 3; round++) {
            try (var ledger = (LiveLedger) open(directory, encryption)) {
                assertEquals(21, ledger.size(MESSAGES), "round " + round);
                for (var i = 0; i < 60; i++) {
                    var value = ledger.get(MESSAGES, new byte[]{5, (byte) i});
                    if (i == 20) {
                        assertArrayEquals(filled(500, 77), value.orElseThrow());
                    } else if (i >= 10 && i < 50) {
                        assertFalse(value.isPresent(), "key 5," + i + " in round " + round);
                    } else {
                        assertArrayEquals(filled(500, i), value.orElseThrow(), "key 5," + i + " in round " + round);
                    }
                }
                assertEquals(21, ledger.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (_, _) -> true));
                if (round == 1) {
                    ledger.checkpoint();
                }
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a replayed range removal keeps its bounds on arrays of their own, so its commit's frame body is not pinned until a checkpoint")
    void replayedRangeRemovalBoundsLeaveTheBody(boolean encrypted, @TempDir Path directory) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var commits = 30;
        try (var ledger = (LiveLedger) open(directory, encryption)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                batch.put(MESSAGES, new byte[]{7, (byte) i}, filled(300, i));
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            for (var commit = 0; commit < commits; commit++) {
                // Each body also carries a resident value and a paged one, so a bound sliced from it would pin kilobytes
                var replayed = ledger.newBatch()
                        .put(SESSIONS, new byte[]{(byte) commit}, filled(2_000, commit))
                        .put(MESSAGES, new byte[]{8, (byte) commit}, filled(1_000, commit));
                replayed = switch (commit % 3) {
                    case 0 -> replayed.deleteRange(MESSAGES, KeyRange.between(new byte[]{7, (byte) commit}, new byte[]{7, (byte) (commit + 1), 0}));
                    case 1 -> replayed.deleteRange(MESSAGES, KeyRange.atLeast(new byte[]{9, (byte) commit}));
                    default -> replayed.deleteRange(MESSAGES, KeyRange.below(new byte[]{0, (byte) commit}));
                };
                ledger.commit(replayed).awaitDurable();
            }
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = (LiveLedger) open(directory, encryption)) {
            var removals = ledger.state(MESSAGES).removals;
            assertEquals(commits, removals.length, "the replay's range removals before a checkpoint");
            var bounds = 0;
            for (var removal : removals) {
                for (var bound : new MemorySegment[]{removal.from(), removal.to()}) {
                    if (bound == null) {
                        continue;
                    }
                    // A read-only segment hides its array; a slice of a body never starts it, as the body's encoding precedes it
                    assertFalse(bound.isNative());
                    assertEquals(0, bound.address(), "a bound that starts past the beginning of its array");
                    assertTrue(bound.isReadOnly());
                    bounds++;
                }
            }
            assertEquals(commits / 3 * 4, bounds);
            assertEquals(100 - commits / 3 * 2 + commits, ledger.size(MESSAGES));
            ledger.checkpoint();
            assertEquals(100 - commits / 3 * 2 + commits, ledger.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("once a commit holding a range removal, a resident put and a paged put replays, nothing it left in memory keeps its body reachable")
    void replayedRangeRemovalDropsTheBody(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 20; i++) {
                batch.put(MESSAGES, new byte[]{7, (byte) i}, filled(300, i));
            }
            ledger.commit(batch).awaitDurable();
            var body = replayRangeRemoval(ledger);
            for (var attempt = 0; attempt < 10 && !body.refersTo(null); attempt++) {
                System.gc();
                Thread.sleep(20);
            }
            assertTrue(body.refersTo(null), "the replayed commit still keeps its body reachable");
            assertEquals(1, ledger.state(MESSAGES).removals.length);
            assertEquals(20 - 10 + 1, ledger.size(MESSAGES));
            assertFalse(ledger.get(MESSAGES, new byte[]{7, 5}).isPresent());
            assertArrayEquals(filled(300, 15), ledger.get(MESSAGES, new byte[]{7, 15}).orElseThrow());
            assertArrayEquals(filled(1_000, 3), ledger.get(MESSAGES, new byte[]{8}).orElseThrow());
            assertArrayEquals(filled(2_000, 2), ledger.get(SESSIONS, new byte[]{1}).orElseThrow());
            ledger.closeWithoutCheckpoint();
        }
    }

    /// Replays, as the open after a crash does, the next commit of `ledger`: a removal of the keys `7,5` to `7,14`, a
    /// resident put and a paged put, all in one body.
    ///
    /// @return a weak reference to the body, which nothing else holds once this returns
    private static WeakReference<byte[]> replayRangeRemoval(LiveLedger ledger) {
        var frame = FrameBody.forTransaction(null);
        frame.deleteRange(ledger.state(MESSAGES).id, MemorySegment.ofArray(new byte[]{7, 5}), MemorySegment.ofArray(new byte[]{7, 15}));
        frame.put(ledger.state(SESSIONS).id, MemorySegment.ofArray(new byte[]{1}), MemorySegment.ofArray(filled(2_000, 2)));
        frame.put(ledger.state(MESSAGES).id, MemorySegment.ofArray(new byte[]{8}), MemorySegment.ofArray(filled(1_000, 3)));
        var body = frame.toBytes();
        assertTrue(ledger.replay(ledger.latest() + 1, body, new ReplayValues()));
        return new WeakReference<>(body);
    }

    @Test
    @Timeout(120)
    @DisplayName("readers, writers that overwrite replayed keys, and checkpoints that free their chunks, run beside each other on what a replay left")
    void concurrentWorkOnReplayedValues(@TempDir Path directory) throws Exception {
        var keys = 3_000;
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            for (var commit = 0; commit < keys / 10; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 10; i++) {
                    var key = commit * 10 + i;
                    batch.put(MESSAGES, intKey(key), stamped(key, 0));
                }
                ledger.commit(batch).awaitDurable();
            }
            ledger.closeWithoutCheckpoint();
        }
        var writers = 2;
        var last = new int[keys];
        var failure = new AtomicReference<Throwable>();
        var stop = new AtomicBoolean();
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            var threads = new ArrayList<Thread>();
            for (var writer = 0; writer < writers; writer++) {
                var number = writer;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        // Each writer owns the keys of its parity, so the last round of each key is known
                        for (var round = 1; !stop.get(); round++) {
                            var key = (round * 7 % (keys / writers)) * writers + number;
                            ledger.commit(ledger.newBatch().put(MESSAGES, intKey(key), stamped(key, round)));
                            last[key] = round;
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            for (var reader = 0; reader < 2; reader++) {
                var seed = reader;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new Random(seed);
                        while (!stop.get()) {
                            try (var snapshot = ledger.snapshot()) {
                                var key = random.nextInt(keys);
                                var first = snapshot.get(MESSAGES, intKey(key)).orElseThrow();
                                checkStamped(key, first);
                                assertEquals(keys, snapshot.size(MESSAGES));
                                assertArrayEquals(first, snapshot.get(MESSAGES, intKey(key)).orElseThrow());
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            var deadline = System.nanoTime() + 2_000_000_000L;
            while (System.nanoTime() < deadline && failure.get() == null) {
                ledger.checkpoint();
                Thread.sleep(20);
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            for (var key = 0; key < keys; key++) {
                assertArrayEquals(stamped(key, last[key]), ledger.get(MESSAGES, intKey(key)).orElseThrow(), "key " + key);
            }
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {1, 2, 3, 4, 5, 6})
    @DisplayName("a store that replayed its log, then took writes over the replayed keys, recovers after the power fails again before a checkpoint")
    void powerLossAfterReplay(int seed) throws Exception {
        var random = new Random(seed);
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var expected = expectedValues();
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            for (var commit = 0; commit < 60; commit++) {
                var batch = ledger.newBatch().put(SESSIONS, new byte[]{(byte) commit}, filled(1_000, commit));
                for (var i = 0; i < 10; i++) {
                    var key = messageKey(commit, i);
                    var value = filled(1 + random.nextInt(3_000), commit + i);
                    batch.put(MESSAGES, key, value);
                    expected.put(ByteBuffer.wrap(key), value);
                }
                ledger.commit(batch).awaitDurable();
            }
            disk = disk.afterPowerLoss(new Random(random.nextLong()));
            ledger.closeWithoutCheckpoint();
        }
        directory = disk.getPath("/store");
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            assertContents(ledger, expected);
            // Overwrites and removals of replayed keys, whose replaced versions are on the replay's chunks
            for (var commit = 0; commit < 30; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 5; i++) {
                    var key = messageKey(random.nextInt(60), random.nextInt(10));
                    if (random.nextInt(4) == 0) {
                        batch.delete(MESSAGES, key);
                        expected.remove(ByteBuffer.wrap(key));
                    } else {
                        var value = filled(1 + random.nextInt(3_000), 200 + commit);
                        batch.put(MESSAGES, key, value);
                        expected.put(ByteBuffer.wrap(key), value);
                    }
                }
                ledger.commit(batch).awaitDurable();
            }
            assertContents(ledger, expected);
            disk = disk.afterPowerLoss(new Random(random.nextLong()));
            ledger.closeWithoutCheckpoint();
        }
        directory = disk.getPath("/store");
        try (var ledger = (LiveLedger) open(directory, Encryption.key(KEY))) {
            assertContents(ledger, expected);
            ledger.checkpoint();
            assertContents(ledger, expected);
        }
        try (var ledger = open(directory, Encryption.key(KEY))) {
            assertContents(ledger, expected);
        }
        CheckpointTest.verifyPages(directory, true);
    }

    private static Ledger open(Path directory, Encryption encryption) {
        return Ledger.builder().directory(directory).encryption(encryption).keyspace(SESSIONS).keyspace(MESSAGES)
                .checkpointEvery(1L << 40).build();
    }

    private static void assertContents(Ledger ledger, TreeMap<ByteBuffer, byte[]> expected) {
        assertEquals(expected.size(), ledger.size(MESSAGES));
        var scanned = new ArrayList<byte[]>();
        var keys = new ArrayList<ByteBuffer>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            keys.add(ByteBuffer.wrap(key));
            scanned.add(value);
            return true;
        });
        assertTrue(List.copyOf(expected.keySet()).equals(keys), "the scan returned other keys or another order");
        var index = 0;
        for (var value : expected.values()) {
            assertArrayEquals(value, scanned.get(index++));
        }
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, entry.getKey().array()).orElseThrow());
        }
    }

    /// {@return an empty map of the values the key space should hold, ordered as the store orders keys}
    private static TreeMap<ByteBuffer, byte[]> expectedValues() {
        return new TreeMap<>((left, right) -> Arrays.compareUnsigned(left.array(), right.array()));
    }

    private static Version head(LiveLedger ledger, byte[] key) {
        return ledger.state(MESSAGES).chain(MemorySegment.ofArray(key)).head;
    }

    private static byte[] messageKey(int commit, int index) {
        return new byte[]{1, (byte) (commit >>> 8), (byte) commit, (byte) index};
    }

    private static byte[] boundaryKey(int index) {
        return new byte[]{2, (byte) index};
    }

    private static byte[] intKey(int key) {
        return ByteBuffer.allocate(4).putInt(key).array();
    }

    /// {@return a value of 600 bytes that names `key` and `round` at both ends}
    private static byte[] stamped(int key, int round) {
        var value = ByteBuffer.allocate(600).putInt(key).putInt(round);
        value.position(592);
        return value.putInt(key).putInt(round).array();
    }

    private static void checkStamped(int key, byte[] value) {
        var buffer = ByteBuffer.wrap(value);
        assertEquals(600, value.length);
        assertEquals(key, buffer.getInt(0));
        assertEquals(key, buffer.getInt(592));
        assertEquals(buffer.getInt(4), buffer.getInt(596));
    }

    private static byte[] filled(int length, int value) {
        var bytes = new byte[length];
        for (var i = 0; i < length; i++) {
            bytes[i] = (byte) (value + i * 31);
        }
        return bytes;
    }
}
