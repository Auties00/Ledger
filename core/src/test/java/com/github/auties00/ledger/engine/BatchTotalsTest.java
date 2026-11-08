package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The totals a batch keeps of its writes, which decide whether a commit passes over the writes at all, checked against
/// the writes after every change to a batch; and the commits they steer: a batch committed again after a failed commit,
/// a batch reused after a range removal, values put encoded to resident key spaces, large values under thresholds of
/// their own, and large batches of threads writing the same keys.
class BatchTotalsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> FILES = Keyspace.paged("files", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    // Its encoder refuses a value starting with "refused" once the batch reserved its bytes, its decoder one starting
    // with "undecodable"
    private static final Keyspace<String, String> NAMES = Keyspace.resident("names", KeyCodec.utf8(), ValueCodec.of(
            value -> value.getBytes(StandardCharsets.UTF_8).length,
            (value, target) -> {
                if (value.startsWith("refused")) {
                    throw new IllegalStateException("refused");
                }
                MemorySegment.copy(MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)), 0, target, 0, target.byteSize());
            },
            source -> {
                var value = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
                if (value.startsWith("undecodable")) {
                    throw new IllegalStateException("undecodable");
                }
                return value;
            }));
    private static final List<Keyspace<byte[], byte[]>> BYTE_KEYSPACES = List.of(MESSAGES, FILES, SESSIONS);

    private static Ledger.Options options(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(FILES).keyspace(SESSIONS).keyspace(NAMES)
                .largeValues(FILES, 4_096);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static MemorySegment segment(String text) {
        return MemorySegment.ofArray(bytes(text));
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    @Nested
    @DisplayName("totals")
    class Totals {
        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8})
        @DisplayName("match the writes after every put, removal, failed encoding, clear, preparation and finish")
        void match(int seed) {
            try (var ledger = Ledger.builder().memory().keyspace(MESSAGES).keyspace(FILES).keyspace(SESSIONS).keyspace(NAMES).build()) {
                var batch = (ArrayWriteBatch) ledger.newBatch();
                var random = new Random(seed);
                // Whether a codec threw once the batch reserved the bytes of its write, which the frame estimate counts
                var reservedUnused = false;
                for (var step = 0; step < 1_500; step++) {
                    switch (random.nextInt(24)) {
                        case 0 -> {
                            batch.clear();
                            reservedUnused = false;
                        }
                        case 1 -> {
                            batch.finish();
                            reservedUnused = false;
                        }
                        case 2, 3 -> {
                            // A commit that fails after the preparation leaves the writes, and one again prepares them again
                            for (var round = 0; round < 2; round++) {
                                batch.prepare(false);
                                checkHomes(batch);
                            }
                        }
                        case 4, 5 -> batch.deleteRange(pick(random), range(random));
                        case 6, 7 -> batch.delete(pick(random), key(random));
                        case 8 -> {
                            assertThrows(IllegalStateException.class, () -> batch.put(NAMES, "n" + random.nextInt(50), "refused"));
                            reservedUnused = true;
                        }
                        case 9 -> batch.put(NAMES, "n" + random.nextInt(50), "name" + random.nextInt());
                        case 10 -> batch.putEncoded(NAMES, segment("n" + random.nextInt(50)), segment("encoded" + random.nextInt()));
                        case 11 -> batch.putEncoded(pick(random), MemorySegment.ofArray(key(random)), MemorySegment.ofArray(value(random)));
                        default -> batch.put(pick(random), key(random), value(random));
                    }
                    check(batch, reservedUnused, random);
                }
            }
        }

        private static Keyspace<byte[], byte[]> pick(Random random) {
            return BYTE_KEYSPACES.get(random.nextInt(BYTE_KEYSPACES.size()));
        }

        private static byte[] key(Random random) {
            return bytes("k" + random.nextInt(200));
        }

        private static KeyRange<byte[]> range(Random random) {
            return switch (random.nextInt(4)) {
                case 0 -> KeyRange.all();
                case 1 -> KeyRange.atLeast(key(random));
                case 2 -> KeyRange.below(key(random));
                default -> KeyRange.between(bytes("k1"), bytes("k5"));
            };
        }

        private static byte[] value(Random random) {
            var draw = random.nextInt(100);
            int length;
            if (draw < 5) {
                length = 0;
            } else if (draw < 75) {
                length = random.nextInt(300);
            } else if (draw < 90) {
                length = 300 + random.nextInt(20_000);
            } else {
                // Around half a chunk, where a value moves from a shared chunk to an array of its own
                length = ArrayWriteBatch.CHUNK_BYTES / 2 - 2 + random.nextInt(5);
            }
            var value = new byte[length];
            random.nextBytes(value);
            return value;
        }

        /// Checks the totals of `batch` against its writes, and the frame estimate they give.
        private static void check(ArrayWriteBatch batch, boolean reservedUnused, Random random) {
            var ranges = false;
            var residentPuts = 0;
            var largest = 0L;
            var spans = 0L;
            for (var i = 0; i < batch.size(); i++) {
                var key = batch.key(i);
                var value = batch.value(i);
                spans += (key == null ? 0 : key.byteSize()) + (value == null ? 0 : value.byteSize());
                if (batch.kind(i) == ArrayWriteBatch.PUT) {
                    if (batch.keyspace(i).residency() == Keyspace.Residency.RESIDENT) {
                        residentPuts++;
                    }
                    largest = Math.max(largest, value.byteSize());
                } else if (batch.kind(i) == ArrayWriteBatch.DELETE_RANGE) {
                    ranges = true;
                }
            }
            assertEquals(ranges, batch.removesRanges(), "range removals");
            assertEquals(residentPuts, batch.residentPuts(), "puts to resident key spaces");
            assertEquals(largest, batch.largestValueBytes(), "largest value");
            // Some puts with their value in a unit of the file of large values
            PageRef[] units = null;
            var unitBytes = 0L;
            if (batch.size() > 0 && random.nextBoolean()) {
                units = new PageRef[batch.size()];
                for (var i = 0; i < batch.size(); i++) {
                    if (batch.kind(i) == ArrayWriteBatch.PUT && random.nextBoolean()) {
                        units[i] = new PageRef(1, 1, 0, 0, 0);
                        unitBytes += PageRef.BYTES - batch.valueBytes(i);
                    }
                }
            }
            var expected = (int) Math.min(16 + 16L * batch.size() + spans + unitBytes, 64L << 20);
            var estimate = FrameBody.capacityFor(batch, units);
            if (reservedUnused) {
                assertTrue(estimate >= expected, "frame estimate " + estimate + " below " + expected);
            } else {
                assertEquals(expected, estimate, "frame estimate");
            }
        }

        /// Checks that the preparation gave each put the home the batch's rule decides from its writes, holding the
        /// value's bytes.
        private static void checkHomes(ArrayWriteBatch batch) {
            var paged = 0L;
            var resident = 0L;
            var ownBytes = 0L;
            for (var i = 0; i < batch.size(); i++) {
                if (batch.kind(i) == ArrayWriteBatch.PUT) {
                    if (batch.ownValue(i) != null) {
                        ownBytes += batch.valueBytes(i);
                    } else if (batch.keyspace(i).residency() == Keyspace.Residency.PAGED) {
                        paged += batch.valueBytes(i);
                    } else {
                        resident += batch.valueBytes(i);
                    }
                }
            }
            var kept = paged > 0 ? paged : resident;
            var used = batch.encodedBytes() - ownBytes;
            // The rule of ArrayWriteBatch.prepare: no homes, the batch's own array (64 bytes of slack), or chunks
            var own = kept > 0 && used <= ArrayWriteBatch.CHUNK_BYTES && used - kept <= Math.max(64, used / 16);
            var chunks = kept > 0 && !own && paged > 0;
            for (var i = 0; i < batch.size(); i++) {
                if (batch.kind(i) != ArrayWriteBatch.PUT) {
                    continue;
                }
                var home = batch.home(i);
                var length = batch.valueBytes(i);
                if (batch.ownValue(i) != null) {
                    assertTrue(length >= ArrayWriteBatch.CHUNK_BYTES / 2, "a value of " + length + " bytes on an array of its own");
                    assertEquals(length, batch.ownValue(i).length, "a value of half a chunk or more shares its array");
                    assertNull(home, "write " + i + " has a home besides its array");
                    continue;
                }
                assertTrue(length < ArrayWriteBatch.CHUNK_BYTES / 2, "a value of " + length + " bytes shares an array");
                if (own) {
                    assertSame(batch.viewForTests(), home, "write " + i + " not on the batch's array");
                    assertEquals(batch.valueArrayOffset(i), batch.homeOffset(i));
                } else if (chunks && batch.keyspace(i).residency() == Keyspace.Residency.PAGED) {
                    assertNotNull(home, "write " + i + " has no chunk");
                    assertNotSame(batch.viewForTests(), home);
                    assertTrue(home.byteSize() <= ArrayWriteBatch.CHUNK_BYTES, "a chunk of " + home.byteSize() + " bytes");
                } else {
                    assertNull(home, "write " + i + " has a home");
                    continue;
                }
                assertEquals(-1, home.asSlice(batch.homeOffset(i), length).mismatch(batch.value(i)), "write " + i + " moved");
            }
        }
    }

    @Nested
    @DisplayName("commits")
    class Commits {
        @Test
        @DisplayName("a batch committed again after its large value failed to reach its file packs its values again, durably")
        void recommit() throws Exception {
            var disk = CrashFileSystem.create();
            var failing = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(BlobFile.FILE)) {
                    throw new IOException("No space left on device");
                }
            });
            var expected = new LinkedHashMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = options(disk.getPath("/store")).build()) {
                var batch = ledger.newBatch();
                // Values below half a chunk, which share chunks, and above, which get arrays of their own
                for (var i = 0; i < 40; i++) {
                    expected.put("m" + i, value(i, 100 + i * 5));
                }
                for (var i = 0; i < 3; i++) {
                    expected.put("big" + i, value(100 + i, ArrayWriteBatch.CHUNK_BYTES / 2 + 1_000 * i));
                }
                for (var i = 0; i < 2; i++) {
                    expected.put("mid" + i, value(200 + i, 60_000));
                }
                for (var entry : expected.entrySet()) {
                    batch.put(MESSAGES, bytes(entry.getKey()), entry.getValue());
                }
                var large = value(300, 10_000);
                batch.put(FILES, bytes("large"), large);
                batch.put(SESSIONS, bytes("session"), value(301, 200));
                batch.put(NAMES, "name", "kept");
                failing.set(true);
                assertThrows(LedgerIoException.class, () -> ledger.commit(batch));
                assertEquals(Optional.empty(), ledger.get(MESSAGES, bytes("m0")));
                assertEquals(0, ledger.size(MESSAGES));
                failing.set(false);
                ledger.commit(batch).awaitDurable();
                survivor = disk.afterPowerLoss(new Random(7));
                verify(ledger, expected, large);
            }
            try (var ledger = options(survivor.getPath("/store")).build()) {
                verify(ledger, expected, value(300, 10_000));
                ledger.checkpoint();
                verify(ledger, expected, value(300, 10_000));
            }
            CheckpointTest.verifyPages(survivor.getPath("/store"), true);
        }

        private static void verify(Ledger ledger, Map<String, byte[]> expected, byte[] large) {
            for (var entry : expected.entrySet()) {
                assertArrayEquals(entry.getValue(), ledger.get(MESSAGES, bytes(entry.getKey())).orElseThrow(), entry.getKey());
            }
            assertEquals(expected.size(), ledger.size(MESSAGES));
            assertArrayEquals(large, ledger.get(FILES, bytes("large")).orElseThrow());
            assertArrayEquals(value(301, 200), ledger.get(SESSIONS, bytes("session")).orElseThrow());
            assertEquals(Optional.of("kept"), ledger.get(NAMES, "name"));
        }

        @Test
        @DisplayName("a batch whose range removal committed, or was cleared, commits its next puts through the pipeline")
        void afterRangeRemoval(@TempDir Path directory) throws Exception {
            var value = value(1, 100);
            try (var ledger = (LiveLedger) options(directory).build()) {
                ledger.commit(ledger.newBatch().put(MESSAGES, bytes("a1"), value).put(MESSAGES, bytes("a2"), value)).awaitDurable();
                // With trees, a commit of the pipeline writes a key memory holds no version of blind; one that runs
                // alone looks it up
                ledger.checkpoint();
                var blind = ledger.state(MESSAGES).blind;
                var listed = blind.listed();
                var batch = (ArrayWriteBatch) ledger.newBatch();
                batch.deleteRange(MESSAGES, KeyRange.between(bytes("a"), bytes("b"))).put(MESSAGES, bytes("c"), value);
                assertTrue(batch.removesRanges());
                ledger.commit(batch);
                assertFalse(batch.removesRanges());
                assertEquals(listed, blind.listed(), "the batch with a range removal did not run alone");
                batch.put(MESSAGES, bytes("d"), value);
                ledger.commit(batch);
                assertEquals(listed + 1, blind.listed(), "the batch of a put did not go through the pipeline");
                batch.deleteRange(MESSAGES, KeyRange.all());
                batch.clear();
                assertFalse(batch.removesRanges());
                batch.put(MESSAGES, bytes("e"), value);
                ledger.commit(batch).awaitDurable();
                assertEquals(listed + 2, blind.listed(), "the cleared batch did not go through the pipeline");
                checkAfterRangeRemoval(ledger, value);
            }
            try (var ledger = options(directory).build()) {
                checkAfterRangeRemoval(ledger, value);
            }
        }

        private static void checkAfterRangeRemoval(Ledger ledger, byte[] value) {
            assertEquals(Optional.empty(), ledger.get(MESSAGES, bytes("a1")));
            assertEquals(Optional.empty(), ledger.get(MESSAGES, bytes("a2")));
            for (var key : List.of("c", "d", "e")) {
                assertArrayEquals(value, ledger.get(MESSAGES, bytes(key)).orElseThrow(), key);
            }
            assertEquals(3, ledger.size(MESSAGES));
        }

        @Test
        @DisplayName("a value put encoded to a resident key space is decoded at the commit, through the pipeline or alone")
        void residentEncoded(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                ledger.commit(ledger.newBatch().put(MESSAGES, bytes("m"), value(1, 100))
                        .putEncoded(NAMES, segment("pipelined"), segment("through the pipeline"))
                        .put(SESSIONS, bytes("s"), value(2, 100))).awaitDurable();
                ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.all())
                        .putEncoded(NAMES, segment("alone"), segment("alone"))).awaitDurable();
                assertEquals(Optional.of("through the pipeline"), ledger.get(NAMES, "pipelined"));
                assertEquals(Optional.of("alone"), ledger.get(NAMES, "alone"));
                assertEquals(0, ledger.size(MESSAGES));
            }
            try (var ledger = options(directory).build()) {
                assertEquals(Optional.of("through the pipeline"), ledger.get(NAMES, "pipelined"));
                assertEquals(Optional.of("alone"), ledger.get(NAMES, "alone"));
                assertArrayEquals(value(2, 100), ledger.get(SESSIONS, bytes("s")).orElseThrow());
            }
        }

        @ParameterizedTest(name = "range removal: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a value put encoded that its resident key space cannot decode stops the batch before it has a sequence")
        void undecodable(boolean alone, @TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                var first = ledger.commit(ledger.newBatch().put(MESSAGES, bytes("m"), value(1, 100))).sequence();
                var batch = ledger.newBatch().put(MESSAGES, bytes("lost"), value(2, 100))
                        .putEncoded(NAMES, segment("n"), segment("undecodable"))
                        .put(SESSIONS, bytes("lost"), value(3, 100));
                if (alone) {
                    batch.deleteRange(FILES, KeyRange.all());
                }
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertEquals(Optional.empty(), ledger.get(MESSAGES, bytes("lost")));
                assertEquals(Optional.empty(), ledger.get(SESSIONS, bytes("lost")));
                assertEquals(Optional.empty(), ledger.get(NAMES, "n"));
                assertEquals(1, ledger.size(MESSAGES));
                assertEquals(0, ledger.size(SESSIONS));
                var next = ledger.commit(ledger.newBatch().put(MESSAGES, bytes("after"), value(4, 100)));
                assertEquals(first + 1, next.sequence());
                next.awaitDurable();
            }
            try (var ledger = options(directory).build()) {
                assertEquals(Optional.empty(), ledger.get(MESSAGES, bytes("lost")));
                assertArrayEquals(value(4, 100), ledger.get(MESSAGES, bytes("after")).orElseThrow());
                assertEquals(2, ledger.size(MESSAGES));
                assertEquals(0, ledger.size(NAMES));
            }
        }

        @Test
        @DisplayName("a batch whose codec threw on one write commits the others")
        void codecThrew(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory).build()) {
                var batch = ledger.newBatch().put(MESSAGES, bytes("m"), value(1, 100));
                assertThrows(IllegalStateException.class, () -> batch.put(NAMES, "refused", "refused"));
                batch.put(NAMES, "kept", "kept").put(SESSIONS, bytes("s"), value(2, 100));
                ledger.commit(batch).awaitDurable();
            }
            try (var ledger = options(directory).build()) {
                assertArrayEquals(value(1, 100), ledger.get(MESSAGES, bytes("m")).orElseThrow());
                assertEquals(Optional.empty(), ledger.get(NAMES, "refused"));
                assertEquals(Optional.of("kept"), ledger.get(NAMES, "kept"));
                assertEquals(1, ledger.size(NAMES));
                assertArrayEquals(value(2, 100), ledger.get(SESSIONS, bytes("s")).orElseThrow());
            }
        }

        @Test
        @DisplayName("a value at its key space's threshold below the store's goes to the file of large values, one below every threshold does not")
        void thresholds(@TempDir Path directory) throws Exception {
            var options = options(directory).largeValues(MESSAGES, Long.MAX_VALUE);
            var huge = value(1, 2 << 20);
            var below = value(2, 4_095);
            var at = value(3, 4_096);
            var session = value(4, (1 << 20) + 1);
            try (var ledger = (LiveLedger) options.build()) {
                var blobs = ledger.blobsForTests();
                // Above the smallest threshold, but below those of its key spaces
                ledger.commit(ledger.newBatch().put(MESSAGES, bytes("huge"), huge).put(FILES, bytes("below"), below)).awaitDurable();
                assertEquals(0, blobs.unadoptedForTests());
                ledger.commit(ledger.newBatch().put(MESSAGES, bytes("small"), value(5, 10)).put(FILES, bytes("at"), at)).awaitDurable();
                assertEquals(1, blobs.unadoptedForTests());
                // At the store's threshold, in a key space without one of its own
                ledger.commit(ledger.newBatch().put(SESSIONS, bytes("session"), session)).awaitDurable();
                assertEquals(2, blobs.unadoptedForTests());
            }
            try (var ledger = options.build()) {
                assertArrayEquals(huge, ledger.get(MESSAGES, bytes("huge")).orElseThrow());
                assertArrayEquals(below, ledger.get(FILES, bytes("below")).orElseThrow());
                assertArrayEquals(at, ledger.get(FILES, bytes("at")).orElseThrow());
                assertArrayEquals(session, ledger.get(SESSIONS, bytes("session")).orElseThrow());
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("pipeline")
    class Pipeline {
        private static final int THREADS = 4;
        private static final int COMMITS = 12;
        private static final int WRITES = 1_200;
        private static final int KEYS = 1_500;

        /// A write of the model: `value` `null` for a removal.
        private record Op(int keyspace, String key, String value) {
        }

        /// A committed batch of a thread, by its sequence.
        private record Recorded(long sequence, List<Op> ops) {
        }

        @ParameterizedTest(name = "on disk: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("large batches of threads that write the same keys apply in the order of their sequences")
        void overlapping(boolean disk, @TempDir Path directory) throws Exception {
            var recorded = new ConcurrentLinkedQueue<Recorded>();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            try (var ledger = disk ? options(directory).build()
                    : Ledger.builder().memory().keyspace(MESSAGES).keyspace(FILES).keyspace(SESSIONS).keyspace(NAMES).build()) {
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < THREADS; t++) {
                    var id = t;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            work(ledger, id, recorded);
                        } catch (Throwable failure) {
                            failures.add(failure);
                        }
                    }));
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertTrue(failures.isEmpty(), () -> "a committer failed: " + failures.peek());
                var model = model(recorded);
                check(ledger, model);
                if (disk) {
                    ledger.checkpoint();
                    check(ledger, model);
                }
            }
            if (disk) {
                try (var ledger = options(directory).build()) {
                    check(ledger, model(recorded));
                }
            }
        }

        private static void work(Ledger ledger, int id, ConcurrentLinkedQueue<Recorded> recorded) throws Exception {
            var random = new Random(id * 7919L + 1);
            for (var commit = 0; commit < COMMITS; commit++) {
                var batch = ledger.newBatch();
                var ops = new ArrayList<Op>(WRITES);
                for (var i = 0; i < WRITES; i++) {
                    var keyspace = random.nextInt(3);
                    var key = "k" + random.nextInt(KEYS);
                    if (random.nextInt(5) == 0) {
                        switch (keyspace) {
                            case 0 -> batch.delete(MESSAGES, bytes(key));
                            case 1 -> batch.delete(SESSIONS, bytes(key));
                            default -> batch.delete(NAMES, key);
                        }
                        ops.add(new Op(keyspace, key, null));
                        continue;
                    }
                    var value = "t" + id + "c" + commit + "i" + i;
                    switch (keyspace) {
                        case 0 -> batch.put(MESSAGES, bytes(key), bytes(value));
                        case 1 -> batch.put(SESSIONS, bytes(key), bytes(value));
                        default -> {
                            if (random.nextBoolean()) {
                                batch.putEncoded(NAMES, segment(key), segment(value));
                            } else {
                                batch.put(NAMES, key, value);
                            }
                        }
                    }
                    ops.add(new Op(keyspace, key, value));
                }
                var done = ledger.commit(batch);
                recorded.add(new Recorded(done.sequence(), ops));
                if (commit == COMMITS - 1) {
                    done.awaitDurable();
                }
            }
        }

        /// {@return the contents of each key space once the recorded batches apply in the order of their sequences}
        private static List<Map<String, String>> model(ConcurrentLinkedQueue<Recorded> recorded) {
            var model = List.<Map<String, String>>of(new HashMap<>(), new HashMap<>(), new HashMap<>());
            var ordered = new ArrayList<>(recorded);
            ordered.sort(Comparator.comparingLong(Recorded::sequence));
            for (var i = 1; i < ordered.size(); i++) {
                assertTrue(ordered.get(i).sequence() > ordered.get(i - 1).sequence(), "two commits share a sequence");
            }
            for (var batch : ordered) {
                for (var op : batch.ops()) {
                    if (op.value() == null) {
                        model.get(op.keyspace()).remove(op.key());
                    } else {
                        model.get(op.keyspace()).put(op.key(), op.value());
                    }
                }
            }
            return model;
        }

        private static void check(Ledger ledger, List<Map<String, String>> model) {
            for (var n = 0; n < KEYS; n++) {
                var key = "k" + n;
                assertEquals(Optional.ofNullable(model.get(0).get(key)), ledger.get(MESSAGES, bytes(key)).map(v -> new String(v, StandardCharsets.UTF_8)), "messages " + key);
                assertEquals(Optional.ofNullable(model.get(1).get(key)), ledger.get(SESSIONS, bytes(key)).map(v -> new String(v, StandardCharsets.UTF_8)), "sessions " + key);
                assertEquals(Optional.ofNullable(model.get(2).get(key)), ledger.get(NAMES, key), "names " + key);
            }
            assertEquals(model.get(0).size(), ledger.size(MESSAGES), "messages count");
            assertEquals(model.get(1).size(), ledger.size(SESSIONS), "sessions count");
            assertEquals(model.get(2).size(), ledger.size(NAMES), "names count");
        }
    }
}
