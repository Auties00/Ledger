package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Frame bodies that refer to the arrays the store's versions keep instead of copying their values: the bytes the log
/// holds are those of a body that copies everything, whichever way the values reached it, whatever the batches do with
/// their arrays while the frames wait in the queue.
class FrameReferencesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> NOTES = Keyspace.resident("notes", KeyCodec.utf8(), ValueCodec.of(
            value -> value.length, (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
            source -> source.toArray(ValueLayout.JAVA_BYTE)));
    private static final List<Keyspace<String, byte[]>> KEYSPACES = List.of(MESSAGES, SESSIONS, NOTES);
    private static final Map<String, Keyspace<?, ?>> BY_NAME = Map.of(MESSAGES.name(), MESSAGES, SESSIONS.name(), SESSIONS, NOTES.name(), NOTES);
    // Both sides of the threshold, and of a slice of the writer, both with and without a whole number of AES blocks
    private static final int[] LENGTHS = {0, 1, 15, 100, 255, 256, 257, 1_400, 4_096, 65_535, 65_536, 65_537, 131_072, 200_000};

    private static byte[] pattern(Random random, int length) {
        var bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    private static int length(Random random) {
        return random.nextInt(4) == 0 ? LENGTHS[random.nextInt(LENGTHS.length)] : random.nextInt(2_000);
    }

    private static byte[] gather(FrameBody body) {
        var bytes = new ByteArrayOutputStream();
        for (var piece = 0; piece < body.pieces(); piece++) {
            bytes.write(body.pieceArray(piece), body.pieceOffset(piece), body.pieceLength(piece));
        }
        assertEquals(body.size(), bytes.size());
        return bytes.toByteArray();
    }

    private static MemorySegment utf8(String text) {
        return MemorySegment.ofArray(text.getBytes(java.nio.charset.StandardCharsets.UTF_8)).asReadOnly();
    }

    @Nested
    @DisplayName("a body")
    class Body {
        // Puts each value of a prepared batch as a commit does for a key space of paged values
        private static void applyPaged(ArrayWriteBatch batch, FrameBody body) {
            for (var i = 0; i < batch.size(); i++) {
                var length = (int) batch.valueBytes(i);
                var array = batch.homeArray(i);
                assertEquals(-1, MemorySegment.ofArray(array).asSlice(batch.homeOffset(i), length).mismatch(batch.value(i)));
                body.put(0, batch.key(i), array, batch.homeOffset(i), length);
            }
        }

        private static Set<byte[]> referenced(FrameBody body) {
            var arrays = Collections.<byte[]>newSetFromMap(new IdentityHashMap<>());
            for (var piece = 1; piece < body.pieces(); piece += 2) {
                arrays.add(body.pieceArray(piece));
            }
            return arrays;
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {1, 2, 3, 4, 5})
        @DisplayName("holds the bytes of a body that copies everything")
        void sameBytes(int seed) {
            var random = new Random(seed);
            var copying = new FrameBody();
            var referring = new FrameBody();
            var referred = 0;
            for (var write = 0; write < 200; write++) {
                var key = utf8("key" + write);
                switch (random.nextInt(4)) {
                    case 0 -> {
                        copying.delete(1, key);
                        referring.delete(1, key);
                    }
                    case 1 -> {
                        copying.deleteRange(2, key, null);
                        referring.deleteRange(2, key, null);
                    }
                    default -> {
                        var length = length(random);
                        var offset = random.nextInt(32);
                        var array = pattern(random, offset + length + random.nextInt(32));
                        copying.put(3, key, MemorySegment.ofArray(array).asSlice(offset, length));
                        referring.put(3, key, array, offset, length);
                        referred += length >= FrameBody.MIN_REFERENCED ? 1 : 0;
                    }
                }
            }
            assertEquals(1, copying.pieces());
            assertEquals(2 * referred + 1, referring.pieces());
            assertEquals(copying.size(), referring.size());
            assertArrayEquals(gather(copying), gather(referring));
        }

        @Test
        @DisplayName("of a batch of messages starts on an array of keys and lengths alone")
        void sizedWithoutValues() {
            var random = new Random(7);
            var batch = new ArrayWriteBatch(this, new HeapPool(1 << 26), BY_NAME);
            var valueBytes = 0L;
            for (var i = 0; i < 1_000; i++) {
                var value = pattern(random, 1_400);
                batch.put(MESSAGES, "message" + i, value);
                batch.put(MESSAGES, "id" + i, pattern(random, 32));
                valueBytes += value.length;
            }
            batch.prepare(false);
            var body = FrameBody.forCommit(batch, null);
            assertTrue(body.pieceArray(0).length < 100_000, "starts on " + body.pieceArray(0).length + " bytes");
            var first = body.pieceArray(0);
            applyPaged(batch, body);
            assertEquals(first, body.pieceArray(0), "grew past its estimate");
            assertEquals(2_001, body.pieces());
            assertTrue(body.size() > valueBytes);
        }

        @Test
        @DisplayName("of a commit that refers to nothing has no tables, and keeps a byte array of a resident key space")
        void copied() {
            var random = new Random(8);
            var small = new ArrayWriteBatch(this, new HeapPool(1 << 26), BY_NAME);
            small.put(MESSAGES, "small", pattern(random, FrameBody.MIN_REFERENCED - 1));
            small.prepare(false);
            var body = FrameBody.forCommit(small, null);
            assertEquals(1, body.pieces());
            assertTrue(body.pieceArray(0).length >= FrameBody.MIN_REFERENCED - 1);

            var session = new ArrayWriteBatch(this, new HeapPool(1 << 26), BY_NAME);
            session.put(SESSIONS, "session", pattern(random, 10_000));
            session.prepare(false);
            assertTrue(FrameBody.forCommit(session, null).pieceArray(0).length >= 10_000);
        }

        @ParameterizedTest(name = "{0} writes of {1} bytes")
        @CsvSource({"1, 3000", "1, 200000", "40, 1400", "3, 200000"})
        @DisplayName("never refers to an array the pool can hand out")
        void notPooled(int writes, int length) {
            var random = new Random(writes * 31L + length);
            var pool = new HeapPool(1 << 28);
            var batch = new ArrayWriteBatch(this, pool, BY_NAME);
            for (var i = 0; i < writes; i++) {
                batch.put(MESSAGES, "key" + i, pattern(random, length));
            }
            if (writes > 1) {
                // Enough beside the values to send them to chunks of their own
                batch.put(SESSIONS, "session", pattern(random, 100_000));
            }
            batch.prepare(false);
            var body = new FrameBody();
            for (var i = 0; i < writes; i++) {
                body.put(0, batch.key(i), batch.homeArray(i), batch.homeOffset(i), (int) batch.valueBytes(i));
            }
            var expected = gather(body);
            var arrays = referenced(body);
            assertFalse(arrays.isEmpty());
            batch.finish();
            // Whatever the pool holds now is what a batch would write over next
            var scribbled = 0;
            for (var shift = 12; shift <= 24; shift++) {
                for (var taken = 0; taken < 3; taken++) {
                    var array = pool.acquire(1 << shift);
                    for (var kept : arrays) {
                        assertNotSame(kept, array);
                    }
                    Arrays.fill(array, (byte) 0x5A);
                    scribbled++;
                }
            }
            assertTrue(scribbled > 0);
            assertArrayEquals(expected, gather(body));
        }
    }

    @Nested
    @DisplayName("the log writer")
    class Writer {
        private static byte[] storeId(Random random) {
            return pattern(random, 16);
        }

        // Queues random frames through both kinds of body: returns what each commit's body must be
        private static List<byte[]> writeFrames(LogWriter writer, Random random) throws InterruptedException {
            var bodies = new ArrayList<byte[]>();
            for (var sequence = 1L; sequence <= 80; sequence++) {
                var copying = new FrameBody();
                var referring = new FrameBody();
                var writes = 1 + random.nextInt(random.nextBoolean() ? 2 : 30);
                for (var write = 0; write < writes; write++) {
                    var key = utf8("key" + sequence + ":" + write);
                    if (random.nextInt(6) == 0) {
                        copying.delete(1, key);
                        referring.delete(1, key);
                        continue;
                    }
                    var length = length(random);
                    var offset = random.nextInt(32);
                    var array = pattern(random, offset + length);
                    copying.put(3, key, MemorySegment.ofArray(array).asSlice(offset, length));
                    referring.put(3, key, array, offset, length);
                }
                var expected = gather(copying);
                assertArrayEquals(expected, gather(referring));
                bodies.add(expected);
                writer.append(FrameBody.COMMIT, sequence, referring);
                if (random.nextInt(4) == 0) {
                    writer.awaitDurable(sequence);
                }
            }
            writer.awaitDurable(bodies.size());
            return bodies;
        }

        // Seals each frame found in the segments with a single call over the bytes of a copying body, and compares
        private static void compareSealed(LogFiles files, StoreKeys keys, byte[] storeId, List<byte[]> bodies) throws Exception {
            var seen = 0;
            var segments = 0;
            for (var number : files.segments()) {
                var bytes = Files.readAllBytes(files.segment(number));
                if (bytes.length <= LogWriter.SEGMENT_HEADER) {
                    continue;
                }
                segments++;
                var salt = Arrays.copyOfRange(bytes, LogWriter.SALT_OFFSET, LogWriter.SALT_OFFSET + LogWriter.SALT_BYTES);
                var position = LogWriter.SEGMENT_HEADER;
                while (position + LogWriter.FRAME_HEADER <= bytes.length) {
                    if (bytes[position] == 0) {
                        // The padding after a group, up to the block where the next one starts
                        position = (position / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
                        continue;
                    }
                    var header = ByteBuffer.wrap(bytes, position, LogWriter.FRAME_HEADER);
                    assertEquals(LogWriter.MAGIC, header.getInt(position));
                    var sealedLength = header.getInt(position + 4);
                    var sequence = header.getLong(position + 8);
                    var useTag = header.getLong(position + LogWriter.USE_TAG_OFFSET);
                    var expected = bodies.get((int) sequence - 1);
                    var sealedAt = position + LogWriter.FRAME_HEADER;
                    byte[] sealed;
                    if (keys.encrypted()) {
                        var cipher = Cipher.getInstance("AES/GCM/NoPadding");
                        cipher.init(Cipher.ENCRYPT_MODE, keys.segmentKey(storeId, number, salt, useTag),
                                new GCMParameterSpec(LogWriter.TAG_BYTES * 8, LogWriter.nonce(position)));
                        cipher.updateAAD(bytes, position, LogWriter.FRAME_HEADER);
                        sealed = cipher.doFinal(expected);
                    } else {
                        var crc = new java.util.zip.CRC32C();
                        crc.update(bytes, position, LogWriter.FRAME_HEADER);
                        crc.update(expected);
                        sealed = ByteBuffer.allocate(expected.length + Integer.BYTES).put(expected).putInt((int) crc.getValue()).array();
                    }
                    assertArrayEquals(sealed, Arrays.copyOfRange(bytes, sealedAt, sealedAt + sealedLength), "commit " + sequence);
                    seen++;
                    position = sealedAt + sealedLength;
                }
            }
            assertEquals(bodies.size(), seen);
            assertTrue(segments > 1, "no frame crossed into another segment");
        }

        @ParameterizedTest(name = "seed {0}, encrypted: {1}")
        @CsvSource({"1, true", "2, true", "3, true", "4, true", "1, false", "2, false", "3, false", "4, false"})
        @DisplayName("seals what a single call over a copying body seals, and recovery reads it back")
        void sealed(int seed, boolean encrypted) throws Exception {
            var random = new Random(seed);
            var directory = CrashFileSystem.create().getPath("/log");
            Files.createDirectories(directory);
            var keys = StoreKeys.derive(encrypted ? EncryptionSpec.key(KEY) : EncryptionSpec.NONE, pattern(random, 16), 0);
            var storeId = storeId(random);
            var files = new LogFiles(directory, keys, storeId);
            List<byte[]> bodies;
            try (var executor = Executors.newFixedThreadPool(2)) {
                var writer = new LogWriter(files, keys, storeId, 1, 0, 256 << 10, executor, Duration.ZERO, null);
                try {
                    bodies = writeFrames(writer, random);
                } finally {
                    writer.close();
                }
            }
            compareSealed(files, keys, storeId, bodies);
            var recovered = new ArrayList<byte[]>();
            LogRecovery.replay(files, keys, storeId, 1, 0, new LogRecovery.Sink() {
                @Override
                public void catalog(int keyspace, String name) {
                }

                @Override
                public boolean commit(long sequence, byte[] body) {
                    recovered.add(body);
                    return true;
                }

                @Override
                public void epoch(int epoch) {
                }
            });
            assertEquals(bodies.size(), recovered.size());
            for (var i = 0; i < bodies.size(); i++) {
                assertArrayEquals(bodies.get(i), recovered.get(i), "commit " + (i + 1));
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        private static Ledger open(Path directory, boolean encrypted, long largeValues) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                    .keyspace(MESSAGES).keyspace(SESSIONS).keyspace(NOTES).largeValues(largeValues))
                    .segmentSize(64 << 10)
                    .build();
        }

        private static String id(Keyspace<?, ?> keyspace, String key) {
            return keyspace.name() + "\0" + key;
        }

        // Every kind of commit whose values a frame may refer to, or copy; returns the last commit
        private static Commit work(Ledger ledger, Random random, Map<String, byte[]> model, int rounds) {
            Commit last = null;
            var keys = 120;
            for (var round = 0; round < rounds; round++) {
                switch (random.nextInt(6)) {
                    case 0 -> {
                        // Little beside its value: the version keeps the batch's own array
                        var key = "m" + random.nextInt(keys);
                        var value = pattern(random, length(random));
                        last = ledger.commit(ledger.newBatch().put(MESSAGES, key, value));
                        model.put(id(MESSAGES, key), value);
                    }
                    case 1 -> {
                        // Enough beside the values to send them to chunks, the longest to arrays of their own
                        var batch = ledger.newBatch();
                        for (var write = random.nextInt(30); write >= 0; write--) {
                            var key = "m" + random.nextInt(keys);
                            var value = pattern(random, length(random));
                            batch.put(MESSAGES, key, value);
                            model.put(id(MESSAGES, key), value);
                        }
                        var session = "s" + random.nextInt(keys);
                        var state = pattern(random, 20_000);
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
                        last = ledger.update(transaction -> {
                            transaction.put(MESSAGES, message, messageValue);
                            transaction.putEncoded(MESSAGES, utf8(encoded), MemorySegment.ofArray(encodedValue));
                            transaction.put(NOTES, note, noteValue);
                            transaction.put(SESSIONS, session, sessionValue);
                        });
                        model.put(id(MESSAGES, message), messageValue);
                        model.put(id(MESSAGES, encoded), encodedValue);
                        model.put(id(NOTES, note), noteValue);
                        model.put(id(SESSIONS, session), sessionValue);
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

        static Stream<Arguments> cases() {
            return IntStream.range(0, 24).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0, seed % 3 == 0, seed % 4 < 2 ? 64L << 10 : 1L << 20));
        }

        @ParameterizedTest(name = "seed {0}, encrypted: {1}, process crash: {2}, large values from {3}")
        @MethodSource("cases")
        @DisplayName("recovers the exact bytes of every durable commit after a failure")
        void recovers(int seed, boolean encrypted, boolean processCrash, long largeValues) throws Exception {
            var random = new Random(seed * 131L + 1);
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, byte[]>();
            CrashFileSystem survivor;
            try (var ledger = open(directory, encrypted, largeValues)) {
                work(ledger, random, model, 60).awaitDurable();
                verify(ledger, model);
                survivor = processCrash ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(seed));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, largeValues)) {
                verify(ledger, model);
            }
        }

        // Holds the first write of the log's segments until the latch opens, so the frames committed meanwhile stay queued
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

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("logs the values of queued frames while their batches write other values over pooled arrays")
        void reusedBatches(boolean encrypted) throws Exception {
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
                // One commit to park the writer on the stall, so every frame after it stays queued
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
                            for (var round = 0; round < 60; round++) {
                                for (var write = 0; write < 4; write++) {
                                    var key = "m" + thread + ":" + round + ":" + write;
                                    var value = pattern(random, 1_400);
                                    batch.put(MESSAGES, key, value);
                                    model.put(id(MESSAGES, key), value);
                                }
                                // Beside the messages: their values go to chunks and the batch's array to the pool
                                var session = "s" + thread;
                                var state = pattern(random, 30_000);
                                batch.put(SESSIONS, session, state);
                                model.put(id(SESSIONS, session), state);
                                last = ledger.commit(batch);
                                // Some of them alone: their values stay on the array the batch gives up
                                var key = "o" + thread + ":" + round;
                                var value = pattern(random, 5_000);
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
                assertTrue(failures.isEmpty(), () -> failures.toString());
                verify(ledger, model);
                open.countDown();
                for (var last : lasts) {
                    last.awaitDurable();
                }
                survivor = disk.afterPowerLoss(new Random(5));
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                verify(ledger, model);
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("logs a resident byte array as committed, even if a reader writes the instance it received before the seal")
        void residentInstance(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var committed = pattern(new Random(11), 4_000);
            CrashFileSystem survivor;
            try (var ledger = open(directory, encrypted, 1L << 20)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                assertTrue(reached.await(2, TimeUnit.MINUTES));
                var commit = ledger.commit(ledger.newBatch().put(SESSIONS, "session", committed.clone()));
                var instance = ledger.get(SESSIONS, "session").orElseThrow();
                Arrays.fill(instance, (byte) 0);
                open.countDown();
                commit.awaitDurable();
                survivor = disk.afterProcessCrash();
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                assertArrayEquals(committed, ledger.get(SESSIONS, "session").orElseThrow());
            }
        }
    }
}
