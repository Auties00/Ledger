package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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

/// Byte arrays put to a resident key space of byte arrays, which a batch clones at the put from
/// `ArrayWriteBatch.RESIDENT_OWN_BYTES` on: the batch's arrays, the one copy the store's version keeps, failed and
/// concurrent commits, and what recovery reads back after a crash.
class ResidentBytesOwnTest {
    private static final byte[] KEY = new byte[32];
    private static final int THRESHOLD = ArrayWriteBatch.RESIDENT_OWN_BYTES;
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());

    private static Ledger.Options options(Path directory, Encryption encryption) {
        return Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NAMES);
    }

    private static ArrayWriteBatch batch() {
        return new ArrayWriteBatch(new Object(), new HeapPool(1 << 24, false), Map.of(
                MESSAGES.name(), MESSAGES, SESSIONS.name(), SESSIONS, NAMES.name(), NAMES));
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static byte[] key(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    @Nested
    @DisplayName("a batch")
    class Batch {
        @ParameterizedTest(name = "{0} bytes")
        @ValueSource(ints = {0, 1, THRESHOLD - 1, THRESHOLD, THRESHOLD + 1, 1_536, 4_096, 20_000})
        @DisplayName("clones a resident byte array from the threshold on, holding only the key on its array, and leaves a smaller one there")
        void threshold(int length) {
            var batch = batch();
            var caller = value(length, length);
            var expected = caller.clone();
            batch.put(SESSIONS, new byte[]{1, 2, 3}, caller);
            if (length > 0) {
                caller[0]++;
            }
            assertEquals(3 + length, batch.encodedBytes());
            assertArrayEquals(expected, batch.value(0).toArray(ValueLayout.JAVA_BYTE));
            assertTrue(batch.value(0).isReadOnly());
            assertEquals(1, batch.residentPuts());
            assertEquals(length, batch.largestValueBytes());
            var own = batch.ownValue(0);
            if (length < THRESHOLD) {
                assertNull(own);
                assertEquals(3 + length, batch.viewForTests().byteSize());
                return;
            }
            assertNotNull(own);
            assertNotSame(caller, own);
            assertArrayEquals(expected, own);
            assertSame(own, batch.valueArray(0));
            assertEquals(0, batch.valueArrayOffset(0));
            // The batch's array holds the key alone
            assertEquals(3, batch.viewForTests().byteSize());
            // Nothing is left for the store to keep on the batch's array
            batch.prepare(false, null, Map.of());
            assertNull(batch.home(0));
            assertEquals(0, batch.homeOffset(0));
            assertArrayEquals(expected, batch.value(0).toArray(ValueLayout.JAVA_BYTE));
        }

        @ParameterizedTest(name = "{0} bytes")
        @ValueSource(ints = {THRESHOLD - 1, THRESHOLD, 1_536})
        @DisplayName("clones an encoded resident byte array from the threshold on, whatever segment it comes in")
        void encoded(int length) {
            var bytes = value(length, length);
            var wider = new byte[length + 9];
            System.arraycopy(bytes, 0, wider, 4, length);
            for (var segment : new MemorySegment[]{MemorySegment.ofArray(bytes.clone()),
                    MemorySegment.ofArray(wider).asSlice(4, length), MemorySegment.ofArray(bytes.clone()).asReadOnly()}) {
                var batch = batch();
                batch.putEncoded(SESSIONS, MemorySegment.ofArray(new byte[]{7}), segment);
                assertEquals(length >= THRESHOLD, batch.ownValue(0) != null);
                if (!segment.isReadOnly()) {
                    segment.set(ValueLayout.JAVA_BYTE, 0, (byte) (bytes[0] + 1));
                }
                assertArrayEquals(bytes, batch.value(0).toArray(ValueLayout.JAVA_BYTE));
                assertNull(batch.decodedValue(0));
            }
        }

        @Test
        @DisplayName("leaves values of a paged key space, or of a resident key space of another codec, on its array")
        void otherKeyspaces() {
            var batch = batch();
            batch.put(MESSAGES, new byte[]{1}, value(1, 1_536)).put(NAMES, new byte[]{2}, "x".repeat(1_536));
            assertNull(batch.ownValue(0));
            assertNull(batch.ownValue(1));
            assertEquals(2 + 2 * 1_536, batch.viewForTests().byteSize());
        }

        @Test
        @DisplayName("beside a paged value, keeps the paged value on its array, which holds no resident bytes, and the session on its own")
        void mixed() {
            var batch = batch();
            var message = value(2, 300);
            var session = value(3, 1_536);
            batch.put(MESSAGES, new byte[]{1}, message).put(SESSIONS, new byte[]{2}, session);
            assertEquals(1 + 300 + 1 + 1_536, batch.encodedBytes());
            batch.prepare(false, null, Map.of());
            // Compacted to the two keys and the message
            assertEquals(1 + 300 + 1, batch.viewForTests().byteSize());
            assertNull(batch.ownValue(0));
            var home = batch.home(0);
            assertNotNull(home);
            assertArrayEquals(message, home.asSlice(batch.homeOffset(0), 300).toArray(ValueLayout.JAVA_BYTE));
            assertTrue(home.byteSize() - 300 <= 64, "the home holds " + home.byteSize() + " bytes for 300");
            assertNull(batch.home(1));
            assertArrayEquals(session, batch.ownValue(1));
            // A second prepare, as a commit that failed makes on its retry, finds the same homes
            batch.prepare(false, null, Map.of());
            assertSame(home, batch.home(0));
            assertNull(batch.home(1));
        }

        @Test
        @DisplayName("keeps its own values through the growth of its writes and a move to a rope, and forgets them when cleared")
        void growth() {
            var batch = batch();
            var count = 600;
            for (var i = 0; i < count; i++) {
                var bytes = new byte[]{(byte) (i >>> 8), (byte) i};
                if (i % 2 == 0) {
                    batch.put(SESSIONS, bytes, value(i, 1_000));
                } else {
                    batch.put(MESSAGES, bytes, value(i, 1_000));
                }
            }
            // The messages alone outgrow a chunk
            assertTrue(batch.chunksForTests() > 0);
            for (var i = 0; i < count; i++) {
                assertEquals(i % 2 == 0, batch.ownValue(i) != null, "write " + i);
                assertArrayEquals(value(i, 1_000), batch.value(i).toArray(ValueLayout.JAVA_BYTE), "write " + i);
            }
            batch.clear();
            batch.put(SESSIONS, new byte[]{1}, value(1, 10));
            assertNull(batch.ownValue(0));
            batch.put(SESSIONS, new byte[]{2}, value(2, 1_000));
            assertNotNull(batch.ownValue(1));
            assertNull(batch.ownValue(0));
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        @ParameterizedTest(name = "alone: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("keeps the batch's clone as the version's one array, which reads return, never the caller's")
        void oneCopy(boolean alone, @TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) options(directory, Encryption.key(KEY)).build()) {
                var caller = value(1, 1_536);
                var expected = caller.clone();
                var batch = (ArrayWriteBatch) ledger.newBatch().put(SESSIONS, key("session"), caller);
                if (alone) {
                    // A range removal makes the commit run alone
                    batch.deleteRange(MESSAGES, KeyRange.all());
                }
                var own = batch.ownValue(0);
                caller[0]++;
                ledger.commit(batch).awaitDurable();
                var version = head(ledger, SESSIONS, key("session"));
                assertSame(own, version.sourceForTests());
                assertSame(own, version.decoded);
                var read = ledger.get(SESSIONS, key("session")).orElseThrow();
                assertSame(own, read);
                assertArrayEquals(expected, read);
                ledger.checkpoint();
                assertArrayEquals(expected, ledger.get(SESSIONS, key("session")).orElseThrow());
            }
            try (var ledger = options(directory, Encryption.key(KEY)).build()) {
                assertArrayEquals(value(1, 1_536), ledger.get(SESSIONS, key("session")).orElseThrow());
            }
        }

        @Test
        @DisplayName("keeps an encoded session on the batch's clone, without decoding it")
        void encoded(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) options(directory, Encryption.key(KEY)).build()) {
                var bytes = value(2, 800);
                var batch = (ArrayWriteBatch) ledger.newBatch().putEncoded(SESSIONS, MemorySegment.ofArray(key("s")), MemorySegment.ofArray(bytes));
                var own = batch.ownValue(0);
                assertNotNull(own);
                ledger.commit(batch).awaitDurable();
                assertSame(own, head(ledger, SESSIONS, key("s")).decoded);
                assertSame(own, ledger.get(SESSIONS, key("s")).orElseThrow());
                assertArrayEquals(bytes, own);
            }
        }

        @Test
        @DisplayName("a store in memory keeps the batch's clone too, and a small session on a copy of its own")
        void memoryStore() {
            try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(MESSAGES).keyspace(SESSIONS).build()) {
                var batch = (ArrayWriteBatch) ledger.newBatch().put(SESSIONS, key("large"), value(1, 1_536))
                        .put(SESSIONS, key("small"), value(2, 20)).put(MESSAGES, key("m"), value(3, 200));
                var own = batch.ownValue(0);
                ledger.commit(batch);
                assertSame(own, ledger.get(SESSIONS, key("large")).orElseThrow());
                var small = head(ledger, SESSIONS, key("small"));
                assertEquals(20, small.sourceBytesForTests());
                assertSame(small.sourceForTests(), small.decoded);
                assertArrayEquals(value(2, 20), ledger.get(SESSIONS, key("small")).orElseThrow());
                assertArrayEquals(value(3, 200), ledger.get(MESSAGES, key("m")).orElseThrow());
            }
        }

        @Test
        @DisplayName("a commit whose decoder throws leaves a session's clone in the batch, which the retry keeps")
        void failedCommit(@TempDir Path directory) throws Exception {
            var fails = new AtomicBoolean(true);
            var picky = Keyspace.resident("picky", KeyCodec.bytes(), ValueCodec.<byte[]>of(value -> value.length,
                    (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
                    source -> {
                        if (fails.get()) {
                            throw new IllegalStateException("no");
                        }
                        return source.toArray(ValueLayout.JAVA_BYTE);
                    }));
            try (var ledger = (LiveLedger) options(directory, Encryption.key(KEY)).keyspace(picky).build()) {
                var session = value(1, 1_536);
                var batch = (ArrayWriteBatch) ledger.newBatch().put(SESSIONS, key("s"), session)
                        .put(MESSAGES, key("m"), value(2, 300))
                        .putEncoded(picky, MemorySegment.ofArray(key("p")), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                var own = batch.ownValue(0);
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(SESSIONS, key("s")).isEmpty());
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(SESSIONS, key("s")).isEmpty());
                fails.set(false);
                batch.put(SESSIONS, key("t"), value(3, 600));
                ledger.commit(batch).awaitDurable();
                assertSame(own, ledger.get(SESSIONS, key("s")).orElseThrow());
                assertArrayEquals(session, own);
                assertArrayEquals(value(2, 300), ledger.get(MESSAGES, key("m")).orElseThrow());
                assertArrayEquals(value(3, 600), ledger.get(SESSIONS, key("t")).orElseThrow());
                assertArrayEquals(new byte[]{1, 2, 3}, ledger.get(picky, key("p")).orElseThrow());
                assertEquals(2, ledger.size(SESSIONS));
            }
        }

        @Test
        @DisplayName("concurrent committers of sessions and messages read back what they wrote, beside checkpoints, and after a reopen")
        void concurrent(@TempDir Path directory) throws Exception {
            var threads = 6;
            var rounds = 250;
            var failures = new ConcurrentLinkedQueue<Throwable>();
            try (var ledger = options(directory, Encryption.key(KEY)).build()) {
                var start = new CountDownLatch(1);
                var stop = new AtomicBoolean();
                var workers = new ArrayList<Thread>();
                for (var t = 0; t < threads; t++) {
                    var thread = t;
                    workers.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var round = 0; round < rounds; round++) {
                                var number = thread * rounds + round;
                                var session = value(number, 50 + number % 3_000);
                                var batch = ledger.newBatch().put(SESSIONS, key("s" + thread), session)
                                        .put(SESSIONS, key("k" + number), session)
                                        .put(MESSAGES, key("m" + number), value(-number, 200));
                                session[0]++;
                                ledger.commit(batch);
                                var read = ledger.get(SESSIONS, key("s" + thread)).orElseThrow();
                                assertArrayEquals(value(number, 50 + number % 3_000), read, "session " + number);
                                assertNotSame(session, read);
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                var checkpointer = Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get()) {
                            ledger.checkpoint();
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                });
                start.countDown();
                for (var worker : workers) {
                    worker.join();
                }
                stop.set(true);
                checkpointer.join();
                if (!failures.isEmpty()) {
                    throw new AssertionError(failures.peek());
                }
                verify(ledger, threads, rounds);
            }
            try (var ledger = options(directory, Encryption.key(KEY)).build()) {
                verify(ledger, threads, rounds);
            }
        }

        private static void verify(Ledger ledger, int threads, int rounds) {
            for (var t = 0; t < threads; t++) {
                var last = t * rounds + rounds - 1;
                assertArrayEquals(value(last, 50 + last % 3_000), ledger.get(SESSIONS, key("s" + t)).orElseThrow());
            }
            for (var number = 0; number < threads * rounds; number++) {
                assertArrayEquals(value(number, 50 + number % 3_000), ledger.get(SESSIONS, key("k" + number)).orElseThrow(), "session " + number);
                assertArrayEquals(value(-number, 200), ledger.get(MESSAGES, key("m" + number)).orElseThrow(), "message " + number);
            }
            assertEquals(threads + threads * rounds, ledger.size(SESSIONS));
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        // Lengths around the threshold, a session's and one past the pool's smallest array
        private static final int[] LENGTHS = {0, 1, THRESHOLD - 1, THRESHOLD, 700, 1_536, 5_000};

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
        @DisplayName("recovers every commit up to the last durable one, whole, and none of a later one but whole, wherever the power fails")
        void powerLoss(int seed) throws Exception {
            var encryption = seed % 2 == 0 ? Encryption.key(KEY) : Encryption.none();
            var dry = CrashFileSystem.create();
            new Workload(seed, encryption).run(dry.getPath("/store"));
            var disk = CrashFileSystem.create();
            var random = new Random(seed * 7_919L + 3);
            if (random.nextBoolean()) {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            } else {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            }
            var workload = new Workload(seed, encryption);
            workload.run(disk.getPath("/store"));
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = options(survivor.getPath("/store"), encryption).build()) {
                var recovered = 0;
                while (recovered < workload.commits && ledger.get(SESSIONS, key("s" + recovered)).isPresent()) {
                    recovered++;
                }
                assertTrue(recovered >= workload.durable, "recovered " + recovered + " of " + workload.durable + " durable commits");
                for (var i = 0; i < workload.commits; i++) {
                    var session = ledger.get(SESSIONS, key("s" + i));
                    var message = ledger.get(MESSAGES, key("m" + i));
                    if (i < recovered) {
                        assertArrayEquals(Workload.session(i), session.orElseThrow(), "session " + i);
                        assertArrayEquals(value(-i, 120), message.orElseThrow(), "message " + i);
                    } else {
                        assertTrue(session.isEmpty(), "session " + i + " after the last commit recovered");
                        assertTrue(message.isEmpty(), "message " + i + " after the last commit recovered");
                    }
                }
                var hot = ledger.get(SESSIONS, key("hot"));
                if (recovered == 0) {
                    assertTrue(hot.isEmpty());
                } else {
                    assertArrayEquals(Workload.session(recovered - 1), hot.orElseThrow(), "the session every commit overwrites");
                }
                assertEquals(recovered == 0 ? 0 : recovered + 1, ledger.size(SESSIONS));
                // The store writes on after the recovery
                ledger.commit(ledger.newBatch().put(SESSIONS, key("after"), value(9, 1_536))).awaitDurable();
                assertArrayEquals(value(9, 1_536), ledger.get(SESSIONS, key("after")).orElseThrow());
            }
        }

        /// Commits a session, the session every commit overwrites and a message per commit, some awaited to be durable,
        /// with checkpoints and a reopen among them, until it ends or the stopped file system fails the store.
        private static final class Workload {
            private final Random random;
            private final Encryption encryption;
            // The commits started, and the commits durable when the last one awaited returned
            int commits;
            int durable;

            Workload(int seed, Encryption encryption) {
                this.random = new Random(seed);
                this.encryption = encryption;
            }

            static byte[] session(int commit) {
                return value(commit, LENGTHS[commit % LENGTHS.length]);
            }

            void run(Path directory) {
                Ledger ledger = null;
                try {
                    ledger = options(directory, encryption).build();
                    for (var step = 0; step < 40; step++) {
                        var choice = random.nextInt(12);
                        if (choice == 0) {
                            ledger.checkpoint();
                            durable = commits;
                        } else if (choice == 1) {
                            ledger.close();
                            durable = commits;
                            ledger = options(directory, encryption).build();
                        } else {
                            var number = commits;
                            var batch = ledger.newBatch().put(SESSIONS, key("s" + number), session(number))
                                    .put(SESSIONS, key("hot"), session(number))
                                    .put(MESSAGES, key("m" + number), value(-number, 120));
                            // Counted before it is made: a commit that throws may have reached the log
                            commits++;
                            var commit = ledger.commit(batch);
                            if (choice < 6) {
                                commit.awaitDurable();
                                durable = commits;
                            }
                        }
                    }
                    ledger.close();
                    durable = commits;
                } catch (RuntimeException | InterruptedException failure) {
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (RuntimeException _) {
                            // The file system stopped: closing only lets go of the threads
                        }
                    }
                }
            }
        }
    }
}
