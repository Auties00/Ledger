package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Transaction;
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
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        @Test
        @DisplayName("of the longest a value may make it holds that value, and not a byte more")
        void limit() {
            var key = utf8("k");
            var longest = FrameBody.MAX_BYTES - FrameBody.WRITE_FIELDS - 1;
            assertThrows(IllegalArgumentException.class, () -> new FrameBody().put(0, key, new byte[0], 0, longest + 1));
            var body = new FrameBody();
            body.put(0, key, new byte[0], 0, longest);
            assertTrue(body.size() > longest);
            assertTrue(body.size() <= FrameBody.MAX_BYTES, "holds " + body.size());
        }

        static Stream<Arguments> writes() {
            var key = utf8("k".repeat(50));
            var inline = MemorySegment.ofArray(new byte[50]);
            return Stream.of(
                    Arguments.of("a put it refers to", (Consumer<FrameBody>) body -> body.put(0, key, new byte[0], 0, 1_000)),
                    Arguments.of("a put of an array it copies", (Consumer<FrameBody>) body -> body.put(0, key, new byte[50], 0, 50)),
                    Arguments.of("a put of a segment", (Consumer<FrameBody>) body -> body.put(0, key, inline)),
                    Arguments.of("a put of a large value", (Consumer<FrameBody>) body -> body.putExternal(0, key, 1L << 40, new PageRef(0, 1, 0L, 0))),
                    Arguments.of("a removal", (Consumer<FrameBody>) body -> body.delete(0, key)),
                    Arguments.of("a range removal", (Consumer<FrameBody>) body -> body.deleteRange(0, key, key)),
                    Arguments.of("a name", (Consumer<FrameBody>) body -> body.catalog(0, "k".repeat(50))));
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("writes")
        @DisplayName("that a write would take past its limit throws and stays as it was, its length never wrapping")
        void past(String ignored, Consumer<FrameBody> write) {
            var body = new FrameBody();
            // Room for a few dozen bytes of keys and values
            body.put(0, utf8("k"), new byte[0], 0, FrameBody.MAX_BYTES - 2 * FrameBody.WRITE_FIELDS - 1 - 10);
            var size = body.size();
            var pieces = body.pieces();
            assertThrows(IllegalArgumentException.class, () -> write.accept(body));
            assertEquals(size, body.size());
            assertEquals(pieces, body.pieces());
            body.delete(0, utf8("0123456789"));
            assertTrue(body.size() > size);
            assertTrue(body.size() <= FrameBody.MAX_BYTES, "holds " + body.size());
            assertThrows(IllegalArgumentException.class, () -> body.delete(0, utf8("0123456789".repeat(4))));
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

        // Reads a private field of the engine: a commit's frame is nowhere else to see before the log writes it
        private static Object field(Class<?> owner, Object instance, String name) throws ReflectiveOperationException {
            var field = owner.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(instance);
        }

        private static LiveLedger.Applier applier(Transaction transaction) throws ReflectiveOperationException {
            return (LiveLedger.Applier) field(LiveTransaction.class, transaction, "applier");
        }

        private static FrameBody frame(Transaction transaction) throws ReflectiveOperationException {
            return (FrameBody) field(LiveLedger.Applier.class, applier(transaction), "frame");
        }

        // Makes the frame of `transaction` claim to be so long that only `room` more bytes of keys and values fit
        private static void fill(Transaction transaction, int room) throws ReflectiveOperationException {
            var frame = frame(transaction);
            var key = utf8("filler");
            frame.put(1, key, new byte[0], 0, FrameBody.MAX_BYTES - 2 * FrameBody.WRITE_FIELDS - frame.size() - (int) key.byteSize() - room);
        }

        // The frames queued and not taken by the log writer yet, whose latest is that of the latest commit
        private static List<FrameBody> queued(Ledger ledger) throws ReflectiveOperationException {
            var log = field(LiveLedger.class, ledger, "log");
            var body = Class.forName(LogWriter.class.getName() + "$Queued").getDeclaredMethod("body");
            body.setAccessible(true);
            var lock = (java.util.concurrent.locks.ReentrantLock) field(LogWriter.class, log, "queueLock");
            var bodies = new ArrayList<FrameBody>();
            lock.lock();
            try {
                for (var frame : (List<?>) field(LogWriter.class, log, "queue")) {
                    bodies.add((FrameBody) body.invoke(frame));
                }
            } finally {
                lock.unlock();
            }
            return bodies;
        }

        private static void assertFrame(FrameBody frame, String name, int pieces, int maxCopied, int size) {
            assertEquals(pieces, frame.pieces(), name + ": pieces");
            assertTrue(frame.pieceArray(0).length <= maxCopied, name + ": copies on an array of " + frame.pieceArray(0).length + " bytes");
            assertTrue(frame.size() >= size, name + ": holds " + frame.size() + " bytes");
        }

        @Test
        @DisplayName("refers from the frames of its commits to the values its versions keep, whichever way a commit reached them")
        void refers() throws Exception {
            var random = new Random(13);
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), true, 1L << 20)) {
                ledger.awaitDurable();
                var reached = new CountDownLatch(1);
                var open = stallLog(disk, reached);
                Commit last;
                try {
                    // Parks the writer at a write of the log: every frame after this one stays queued
                    ledger.commit(ledger.newBatch().put(MESSAGES, "first", new byte[300]));
                    assertTrue(reached.await(2, TimeUnit.MINUTES));

                    // A single value: the version keeps the batch's own array
                    ledger.commit(ledger.newBatch().put(MESSAGES, "own", pattern(random, 4_000)));
                    assertFrame(queued(ledger).getLast(), "own array", 3, 256, 4_000);

                    // At the threshold, and just under it
                    ledger.commit(ledger.newBatch().put(MESSAGES, "at", pattern(random, FrameBody.MIN_REFERENCED)));
                    assertFrame(queued(ledger).getLast(), "at the threshold", 3, 256, FrameBody.MIN_REFERENCED);
                    ledger.commit(ledger.newBatch().put(MESSAGES, "under", pattern(random, FrameBody.MIN_REFERENCED - 1)));
                    assertFrame(queued(ledger).getLast(), "under the threshold", 1, 1_024, FrameBody.MIN_REFERENCED - 1);

                    // Enough beside the messages to send them to chunks; a resident byte array is always copied
                    var chunks = ledger.newBatch();
                    for (var i = 0; i < 10; i++) {
                        chunks.put(MESSAGES, "chunk" + i, pattern(random, 1_400));
                    }
                    chunks.put(SESSIONS, "session", pattern(random, 30_000));
                    ledger.commit(chunks);
                    assertFrame(queued(ledger).getLast(), "chunks", 21, 32_000, 44_000);

                    // A value of half a chunk or more gets an array of its own
                    var own = ledger.newBatch();
                    own.put(MESSAGES, "large", pattern(random, 200_000));
                    own.put(SESSIONS, "beside", pattern(random, 30_000));
                    ledger.commit(own);
                    assertFrame(queued(ledger).getLast(), "array of its own", 3, 32_000, 230_000);

                    // A resident key space with its own codec, on a value of its own, and on the batch's array
                    ledger.commit(ledger.newBatch().put(NOTES, "note", pattern(random, 4_000)));
                    assertFrame(queued(ledger).getLast(), "resident", 3, 256, 4_000);
                    var notes = ledger.newBatch();
                    notes.put(NOTES, "note1", pattern(random, 1_000));
                    notes.put(MESSAGES, "beside", pattern(random, 30_000));
                    ledger.commit(notes);
                    assertFrame(queued(ledger).getLast(), "resident beside a message", 5, 256, 31_000);

                    ledger.commit(ledger.newBatch().put(SESSIONS, "copied", pattern(random, 4_000)));
                    assertFrame(queued(ledger).getLast(), "resident byte array", 1, 8_192, 4_000);

                    // A commit that runs alone
                    var alone = ledger.newBatch();
                    alone.put(MESSAGES, "alone", pattern(random, 4_000));
                    alone.deleteRange(NOTES, KeyRange.atLeast("z"));
                    ledger.commit(alone);
                    assertFrame(queued(ledger).getLast(), "alone", 3, 256, 4_000);

                    // A transaction: each of its values on an array of its own, but for a resident byte array
                    var messageValue = pattern(random, 4_000);
                    var encodedValue = pattern(random, 4_000);
                    var noteValue = pattern(random, 4_000);
                    var sessionValue = pattern(random, 200);
                    last = ledger.update(transaction -> {
                        transaction.put(MESSAGES, "transaction", messageValue);
                        transaction.putEncoded(MESSAGES, utf8("encoded"), MemorySegment.ofArray(encodedValue));
                        transaction.put(NOTES, "transaction", noteValue);
                        transaction.put(SESSIONS, "transaction", sessionValue);
                    });
                    assertFrame(queued(ledger).getLast(), "transaction", 7, 512, 12_200);
                } finally {
                    open.countDown();
                }
                last.awaitDurable();
            }
        }

        // A write a transaction makes once its frame has little room left, far more than it holds beside its key
        static Stream<Arguments> oversized() {
            var longKey = "k".repeat(100);
            var writes = List.<Arguments>of(
                    Arguments.of("a value it would refer to", (Consumer<Transaction>) transaction -> transaction.put(MESSAGES, "over", new byte[1_000])),
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
                    try {
                        fill(transaction, 20);
                    } catch (ReflectiveOperationException exception) {
                        throw new AssertionError(exception);
                    }
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

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("takes back the write a commit's frame finds no room for once the version is attached, as a batch's thread does")
        void oversizedCommit(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var before = pattern(new Random(23), 3_000);
            CrashFileSystem survivor;
            try (var ledger = open(directory, encrypted, 1L << 20)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "replaced", before)).awaitDurable();
                var live = (LiveLedger) ledger;
                // A batch needs 2 GiB of values to get here: this is what its commit calls, in the failing order
                assertThrows(IllegalArgumentException.class, () -> ledger.update(transaction -> {
                    try {
                        fill(transaction, 20);
                        var applier = applier(transaction);
                        var state = live.state(MESSAGES);
                        // The first fits and replaces a value, the second finds no room once its version is attached
                        for (var length : List.of(20, 1_000)) {
                            var encoded = utf8(length == 20 ? "replaced" : "new");
                            var value = new byte[length];
                            applier.put(state, applier.claim(state, encoded), encoded, MemorySegment.ofArray(value).asReadOnly(), null, value, 0, null, TreeSearch.UNKNOWN, null);
                        }
                    } catch (ReflectiveOperationException exception) {
                        throw new AssertionError(exception);
                    }
                }));
                assertArrayEquals(before, ledger.get(MESSAGES, "replaced").orElseThrow());
                assertTrue(ledger.get(MESSAGES, "new").isEmpty());
                assertEquals(1, ledger.size(MESSAGES));
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", before)).awaitDurable();
                survivor = disk.afterProcessCrash();
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 1L << 20)) {
                assertArrayEquals(before, ledger.get(MESSAGES, "replaced").orElseThrow());
                assertArrayEquals(before, ledger.get(MESSAGES, "after").orElseThrow());
                assertEquals(2, ledger.size(MESSAGES));
            }
        }

        @Test
        @DisplayName("lets a transaction catch a write its frame has no room for, and find nothing of that write")
        void caughtInTransaction() {
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), true, 1L << 20)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "kept", new byte[300]).put(MESSAGES, "other", new byte[300]));
                assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                    try {
                        fill(transaction, 20);
                        var size = frame(transaction).size();
                        var pieces = frame(transaction).pieces();
                        assertThrows(IllegalArgumentException.class, () -> transaction.put(MESSAGES, "over", new byte[1_000]));
                        assertTrue(transaction.get(MESSAGES, "over").isEmpty());
                        assertThrows(IllegalArgumentException.class, () -> transaction.delete(MESSAGES, "k".repeat(100)));
                        assertThrows(IllegalArgumentException.class, () -> transaction.deleteRange(MESSAGES, KeyRange.atLeast("k".repeat(100))));
                        assertTrue(transaction.get(MESSAGES, "kept").isPresent());
                        assertEquals(size, frame(transaction).size());
                        assertEquals(pieces, frame(transaction).pieces());
                        // The filler is no value a writer could read, so this frame must not reach the log
                        transaction.delete(MESSAGES, "other");
                        assertTrue(transaction.get(MESSAGES, "other").isEmpty());
                    } catch (ReflectiveOperationException exception) {
                        throw new AssertionError(exception);
                    }
                    throw new IllegalStateException("taken back");
                }));
                assertEquals(2, ledger.size(MESSAGES));
            }
        }
    }
}
