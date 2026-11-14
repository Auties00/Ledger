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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Byte arrays bound for the file of large values, which a batch borrows from the caller instead of copying until the
/// commit ends: which puts borrow, what the commit seals, that the store keeps no reference to the caller's array once
/// the commit returns (reads, snapshots, checkpoints, recovery and a crash all see the bytes of the commit), how a
/// commit that fails keeps the array for a retry, and how concurrent writers that reuse their arrays share the store
/// with readers and checkpoints.
class BorrowedValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final int OWN = ArrayWriteBatch.CHUNK_BYTES / 2;
    private static final int EXACT = 2 << 20;
    private static final int ODD = (3 << 20) + 1_000;
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    // Values below 8 MiB stay in the log, so none is borrowed
    private static final Keyspace<String, byte[]> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> TEXTS = Keyspace.paged("texts", KeyCodec.utf8(), ValueCodec.utf8());

    private static Ledger.Options options(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(FILES).keyspace(NOTES).keyspace(SESSIONS).keyspace(TEXTS).largeValues(NOTES, 8L << 20);
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static Version head(Ledger ledger, Keyspace<?, ?> keyspace, String key) {
        return ((LiveLedger) ledger).state(keyspace).chain(MemorySegment.ofArray(key.getBytes())).head;
    }

    private static void assertReads(Ledger ledger, String key, byte[] expected) {
        assertArrayEquals(expected, ledger.get(FILES, key).orElseThrow());
        assertArrayEquals(expected, ledger.readEncoded(FILES, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow());
        var ranged = ledger.range(FILES, KeyRange.between(key, key + "\0"), 2);
        assertEquals(1, ranged.size());
        assertArrayEquals(expected, ranged.getFirst().value());
        var encoded = new ArrayList<byte[]>();
        ledger.forEachEncoded(FILES, KeyRange.between(key, key + "\0"), (_, value) -> encoded.add(value.toArray(ValueLayout.JAVA_BYTE)));
        assertEquals(1, encoded.size());
        assertArrayEquals(expected, encoded.getFirst());
    }

    /// Writes over every byte of `array`, as a caller that reuses its buffer does.
    private static void scribble(byte[] array) {
        for (var i = 0; i < array.length; i++) {
            array[i] = (byte) ~array[i];
        }
    }

    @Nested
    @DisplayName("a batch")
    class Batch {
        private ArrayWriteBatch batch(long borrowFrom) {
            return new ArrayWriteBatch(new Object(), new HeapPool(1 << 24, false), Map.of(FILES.name(), FILES, TEXTS.name(), TEXTS),
                    keyspace -> keyspace == FILES ? borrowFrom : Long.MAX_VALUE);
        }

        @Test
        @DisplayName("borrows a large byte array whole, counts it, reads it through a read-only view, and lets go of it when cleared")
        void borrows() {
            var batch = batch(1L << 20);
            var caller = value(2, ODD);
            batch.put(FILES, "a", caller);
            assertEquals(1 + ODD, batch.encodedBytes());
            assertEquals(ODD, batch.valueBytes(0));
            assertEquals(ODD, batch.largestValueBytes());
            assertTrue(batch.borrowed(0));
            assertSame(caller, batch.ownValue(0));
            assertSame(caller, batch.valueArray(0));
            assertEquals(0, batch.valueArrayOffset(0));
            var view = batch.value(0);
            assertTrue(view.isReadOnly());
            caller[7]++;
            assertEquals(caller[7], view.get(ValueLayout.JAVA_BYTE, 7));
            batch.prepare(false, null, Map.of());
            assertNull(batch.home(0));
            assertNull(batch.homeArray(0));
            batch.clear();
            batch.put(FILES, "b", value(3, 100));
            assertNull(batch.ownValue(0));
            assertFalse(batch.borrowed(0));
        }

        @Test
        @DisplayName("borrows from half a chunk on, at the threshold, and from a threshold below half a chunk")
        void threshold() {
            var at = batch(OWN);
            var caller = value(4, OWN);
            at.put(FILES, "a", caller).put(FILES, "b", value(5, OWN - 1));
            assertTrue(at.borrowed(0));
            assertSame(caller, at.ownValue(0));
            // Below half a chunk a value stays on the batch's array, which the commit seals from
            assertFalse(at.borrowed(1));
            assertNull(at.ownValue(1));
            var low = batch(1_000);
            var small = value(6, 10_000);
            low.put(FILES, "a", small);
            assertFalse(low.borrowed(0));
            assertNotSame(small, low.valueArray(0));
        }

        @Test
        @DisplayName("copies a value below the threshold, of a key space that never borrows, or of a batch that borrows none")
        void copies() {
            var below = batch(4L << 20);
            var caller = value(7, ODD);
            below.put(FILES, "a", caller);
            assertFalse(below.borrowed(0));
            assertNotSame(caller, below.ownValue(0));
            assertArrayEquals(caller, below.ownValue(0));
            var other = batch(1L << 20);
            other.put(TEXTS, "a", "x".repeat(ODD));
            assertFalse(other.borrowed(0));
            var none = new ArrayWriteBatch(new Object(), new HeapPool(1 << 24, false), Map.of(FILES.name(), FILES));
            none.put(FILES, "a", caller);
            assertFalse(none.borrowed(0));
            assertNotSame(caller, none.ownValue(0));
        }

        @Test
        @DisplayName("copies an encoded value, whichever memory it is on, onto one array of its own")
        void copiesEncoded() {
            var expected = value(8, EXACT);
            try (var arena = Arena.ofConfined()) {
                var native_ = arena.allocate(EXACT);
                MemorySegment.copy(expected, 0, native_, ValueLayout.JAVA_BYTE, 0, EXACT);
                var padded = new byte[EXACT + 10];
                System.arraycopy(expected, 0, padded, 5, EXACT);
                var sources = List.of(MemorySegment.ofArray(expected), MemorySegment.ofArray(expected).asReadOnly(), native_,
                        MemorySegment.ofArray(padded).asSlice(5, EXACT));
                for (var source : sources) {
                    var batch = batch(1L << 20);
                    batch.putEncoded(FILES, MemorySegment.ofArray(new byte[]{1}), source);
                    assertFalse(batch.borrowed(0), source.toString());
                    assertNotSame(expected, batch.ownValue(0), source.toString());
                    assertArrayEquals(expected, batch.ownValue(0), source.toString());
                }
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("keeps no reference to a borrowed array once the commit returns: reads, a snapshot, a checkpoint and a reopen see the committed bytes")
        void reusedAfterCommit(boolean encrypted, @TempDir Path directory) throws Exception {
            var exact = value(10, EXACT);
            var odd = value(11, ODD);
            var buffer = exact.clone();
            try (var ledger = options(directory, encrypted).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "exact", buffer)).awaitDurable();
                assertNotSame(buffer, head(ledger, FILES, "exact").sourceForTests());
                scribble(buffer);
                assertReads(ledger, "exact", exact);
                buffer = odd.clone();
                try (var snapshot = ledger.snapshot()) {
                    ledger.commit(ledger.newBatch().put(FILES, "exact", buffer)).awaitDurable();
                    scribble(buffer);
                    assertArrayEquals(exact, snapshot.get(FILES, "exact").orElseThrow());
                    assertReads(ledger, "exact", odd);
                    ledger.checkpoint();
                    assertArrayEquals(exact, snapshot.get(FILES, "exact").orElseThrow());
                }
                assertReads(ledger, "exact", odd);
                buffer = exact.clone();
                // Not waited for: the commit returns before its frame is sealed, and the frame names the unit alone
                ledger.commit(ledger.newBatch().put(FILES, "late", buffer));
                scribble(buffer);
                assertReads(ledger, "late", exact);
            }
            CheckpointTest.verifyPages(directory, encrypted);
            try (var ledger = options(directory, encrypted).build()) {
                assertReads(ledger, "exact", odd);
                assertReads(ledger, "late", exact);
            }
        }

        @Test
        @DisplayName("replays a borrowed value from its unit after a close without a checkpoint, whatever the caller wrote after the commit")
        void replays(@TempDir Path directory) throws Exception {
            var exact = value(12, EXACT);
            var buffer = exact.clone();
            var ledger = options(directory, true).build();
            ledger.commit(ledger.newBatch().put(FILES, "exact", buffer).put(FILES, "small", new byte[]{1})).awaitDurable();
            scribble(buffer);
            assertReads(ledger, "exact", exact);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            try (var reopened = options(directory, true).build()) {
                assertReads(reopened, "exact", exact);
                assertArrayEquals(new byte[]{1}, reopened.get(FILES, "small").orElseThrow());
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("stores a borrowed array as it is when the commit seals it, and a copied one as it was at the put")
        void sealsAtCommit(@TempDir Path directory) throws Exception {
            var caller = value(13, ODD);
            var encoded = value(14, ODD);
            var expectedEncoded = encoded.clone();
            byte[] sealed;
            try (var ledger = options(directory, true).build()) {
                var batch = ledger.newBatch().put(FILES, "borrowed", caller)
                        .putEncoded(FILES, MemorySegment.ofArray("copied".getBytes()), MemorySegment.ofArray(encoded));
                scribble(caller);
                scribble(encoded);
                sealed = caller.clone();
                ledger.commit(batch).awaitDurable();
                assertReads(ledger, "borrowed", sealed);
                assertReads(ledger, "copied", expectedEncoded);
                ledger.checkpoint();
                assertReads(ledger, "borrowed", sealed);
            }
            try (var ledger = options(directory, true).build()) {
                assertReads(ledger, "borrowed", sealed);
                assertReads(ledger, "copied", expectedEncoded);
            }
        }

        @Test
        @DisplayName("lets the collector take a borrowed array once the commit returns, though the store and the batch live on")
        void unreachable(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory, true).build()) {
                var batch = ledger.newBatch();
                var borrowed = putAndCommit(ledger, batch);
                for (var i = 0; i < 50 && borrowed.get() != null; i++) {
                    System.gc();
                    Thread.sleep(10);
                }
                assertNull(borrowed.get(), "the store or the batch still refers to the caller's array");
                assertReads(ledger, "a", value(15, EXACT));
                // The batch is reused
                ledger.commit(batch.put(FILES, "b", new byte[]{2})).awaitDurable();
                assertArrayEquals(new byte[]{2}, ledger.get(FILES, "b").orElseThrow());
            }
        }

        // Puts and commits an array no frame of the caller refers to afterwards
        private static WeakReference<byte[]> putAndCommit(Ledger ledger, com.github.auties00.ledger.WriteBatch batch)
                throws InterruptedException {
            var caller = value(15, EXACT);
            ledger.commit(batch.put(FILES, "a", caller)).awaitDurable();
            assertTrue(((ArrayWriteBatch) batch).isEmpty());
            return new WeakReference<>(caller);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a commit whose unit cannot be written keeps the borrowed array for a retry of the same batch, and frees the pages")
        void failedWrite(boolean encrypted) throws Exception {
            try (var disk = CrashFileSystem.create()) {
                var directory = disk.getPath("/store");
                var odd = value(16, ODD);
                var buffer = odd.clone();
                var failing = new AtomicBoolean(true);
                try (var ledger = options(directory, encrypted).build()) {
                    disk.intercept((path, kind) -> {
                        if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(BlobFile.FILE)) {
                            throw new IOException("No space left on device");
                        }
                    });
                    var batch = (ArrayWriteBatch) ledger.newBatch().put(FILES, "odd", buffer).put(FILES, "small", new byte[]{1});
                    assertThrows(LedgerIoException.class, () -> ledger.commit(batch));
                    assertTrue(ledger.get(FILES, "odd").isEmpty());
                    assertTrue(ledger.get(FILES, "small").isEmpty());
                    assertSame(buffer, batch.ownValue(0));
                    assertTrue(batch.borrowed(0));
                    failing.set(false);
                    ledger.commit(batch).awaitDurable();
                    assertTrue(batch.isEmpty());
                    scribble(buffer);
                    assertReads(ledger, "odd", odd);
                    ledger.checkpoint();
                    assertReads(ledger, "odd", odd);
                    assertArrayEquals(new byte[]{1}, ledger.get(FILES, "small").orElseThrow());
                }
                try (var ledger = options(directory, encrypted).build()) {
                    assertReads(ledger, "odd", odd);
                }
            }
        }

        @Test
        @DisplayName("a commit that fails after writing the unit of a borrowed value leaves the batch to retry, and its pages free")
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
            var odd = value(19, ODD);
            var buffer = odd.clone();
            try (var ledger = options(directory, true).keyspace(picky).build()) {
                var batch = ledger.newBatch().put(FILES, "odd", buffer)
                        .putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(FILES, "odd").isEmpty());
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                fails.set(false);
                ledger.commit(batch).awaitDurable();
                scribble(buffer);
                assertReads(ledger, "odd", odd);
                ledger.checkpoint();
                assertReads(ledger, "odd", odd);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("applies a borrowed value in a batch that removes a range, which runs alone")
        void alone(@TempDir Path directory) throws Exception {
            var exact = value(13, EXACT);
            var buffer = exact.clone();
            try (var ledger = options(directory, true).build()) {
                ledger.commit(ledger.newBatch().put(FILES, "a", new byte[]{1}).put(FILES, "b", new byte[]{2})).awaitDurable();
                ledger.commit(ledger.newBatch().deleteRange(FILES, KeyRange.between("a", "c")).put(FILES, "c", buffer)).awaitDurable();
                scribble(buffer);
                assertTrue(ledger.get(FILES, "a").isEmpty());
                assertReads(ledger, "c", exact);
                ledger.checkpoint();
                assertReads(ledger, "c", exact);
            }
        }

        @Test
        @DisplayName("copies what it does not borrow: the log, a resident or decoded key space, a transaction, a store in memory")
        void copiesTheRest(@TempDir Path directory) throws Exception {
            var note = value(15, ODD);
            var session = value(16, ODD);
            var transaction = value(17, ODD);
            try (var ledger = options(directory, true).build()) {
                var batch = (ArrayWriteBatch) ledger.newBatch().put(NOTES, "note", note).put(SESSIONS, "session", session)
                        .put(TEXTS, "text", "y".repeat(ODD));
                for (var i = 0; i < batch.size(); i++) {
                    assertFalse(batch.borrowed(i));
                }
                assertNotSame(note, batch.ownValue(0));
                assertNotSame(session, batch.ownValue(1));
                ledger.commit(batch).awaitDurable();
                ledger.update(update -> update.put(FILES, "transaction", transaction)).awaitDurable();
                assertArrayEquals(value(15, ODD), ledger.get(NOTES, "note").orElseThrow());
                assertArrayEquals(value(16, ODD), ledger.get(SESSIONS, "session").orElseThrow());
                assertNotSame(session, ledger.get(SESSIONS, "session").orElseThrow());
                assertEquals("y".repeat(ODD), ledger.get(TEXTS, "text").orElseThrow());
                assertArrayEquals(value(17, ODD), ledger.get(FILES, "transaction").orElseThrow());
            }
            try (var memory = Ledger.builder().memory().keyspace(FILES).build()) {
                var batch = (ArrayWriteBatch) memory.newBatch().put(FILES, "memory", value(18, ODD));
                assertFalse(batch.borrowed(0));
                memory.commit(batch).awaitDurable();
                assertArrayEquals(value(18, ODD), memory.get(FILES, "memory").orElseThrow());
                assertInstanceOf(byte[].class, head(memory, FILES, "memory").sourceForTests());
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("recovers, wherever the power fails, the values of the commits from the last durable one to the last visible one, from a buffer reused after each commit")
        void crashes(boolean encrypted) throws Exception {
            var dry = CrashFileSystem.create();
            new CrashWorkload(encrypted).run(dry.getPath("/store"));
            for (var force = 0; force <= dry.forces(); force++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(force);
                var workload = new CrashWorkload(encrypted);
                workload.run(disk.getPath("/store"));
                var survivor = disk.afterPowerLoss(new Random(force));
                try (var ledger = options(survivor.getPath("/store"), encrypted).build()) {
                    var recovered = 0;
                    while (recovered < CrashWorkload.COMMITS && ledger.get(FILES, "k" + recovered).isPresent()) {
                        assertArrayEquals(CrashWorkload.value(recovered), ledger.get(FILES, "k" + recovered).orElseThrow(), "k" + recovered);
                        recovered++;
                    }
                    for (var later = recovered; later < CrashWorkload.COMMITS; later++) {
                        assertTrue(ledger.get(FILES, "k" + later).isEmpty(), "k" + later + " after a gap at force " + force);
                    }
                    assertTrue(recovered >= workload.durable && recovered <= workload.visible,
                            recovered + " recovered, durable " + workload.durable + ", visible " + workload.visible + ", force " + force);
                    // The store keeps working on what it recovered
                    ledger.commit(ledger.newBatch().put(FILES, "after", value(20, EXACT))).awaitDurable();
                    ledger.checkpoint();
                    assertArrayEquals(value(20, EXACT), ledger.get(FILES, "after").orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("serves concurrent writers that reuse their buffers, readers and checkpoints whole values")
        void concurrent(@TempDir Path directory) throws Exception {
            var writers = 4;
            var rounds = 6;
            var lengths = new int[]{EXACT, EXACT + 3_000, (3 << 20) / 2, 200_000};
            // Each value by its first 16 bytes, which a reader checks the rest against
            var known = new ConcurrentHashMap<String, byte[]>();
            var failure = new AtomicReference<Throwable>();
            try (var ledger = options(directory, true).build()) {
                var start = new CountDownLatch(1);
                var running = new AtomicBoolean(true);
                var threads = new ArrayList<Thread>();
                for (var writer = 0; writer < writers; writer++) {
                    var id = writer;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            // One buffer per length, written over once each commit returns
                            var buffers = new HashMap<Integer, byte[]>();
                            for (var round = 0; round < rounds; round++) {
                                var seed = id * 1_000 + round;
                                var written = value(seed, lengths[(id + round) % lengths.length]);
                                known.put(Arrays.toString(Arrays.copyOf(written, 16)), written);
                                var buffer = buffers.computeIfAbsent(written.length, byte[]::new);
                                System.arraycopy(written, 0, buffer, 0, written.length);
                                var commit = ledger.commit(ledger.newBatch().put(FILES, "w" + id + "-" + round % 2, buffer));
                                scribble(buffer);
                                if (round % 2 == 0) {
                                    commit.awaitDurable();
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
                for (var reader = 0; reader < 2; reader++) {
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            var random = new Random();
                            while (running.get()) {
                                var key = "w" + random.nextInt(writers) + "-" + random.nextInt(2);
                                var read = random.nextBoolean()
                                        ? ledger.get(FILES, key).orElse(null)
                                        : ledger.readEncoded(FILES, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElse(null);
                                if (read != null) {
                                    var expected = known.get(Arrays.toString(Arrays.copyOf(read, 16)));
                                    if (expected == null || !Arrays.equals(expected, read)) {
                                        throw new AssertionError("a read of " + key + " returned a value no writer committed");
                                    }
                                }
                            }
                        } catch (Throwable throwable) {
                            failure.compareAndSet(null, throwable);
                        }
                    }));
                }
                var checkpoints = Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        while (running.get()) {
                            ledger.checkpoint();
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                });
                start.countDown();
                for (var writer = 0; writer < writers; writer++) {
                    threads.get(writer).join();
                }
                running.set(false);
                for (var thread : threads) {
                    thread.join();
                }
                checkpoints.join();
                if (failure.get() != null) {
                    throw new AssertionError(failure.get());
                }
                var expected = new HashMap<String, byte[]>();
                for (var writer = 0; writer < writers; writer++) {
                    for (var round = 0; round < rounds; round++) {
                        expected.put("w" + writer + "-" + round % 2, value(writer * 1_000 + round, lengths[(writer + round) % lengths.length]));
                    }
                }
                for (var entry : expected.entrySet()) {
                    assertArrayEquals(entry.getValue(), ledger.get(FILES, entry.getKey()).orElseThrow(), entry.getKey());
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    /// Commits a borrowed value per commit from one buffer, written over once each commit returns, the second half of
    /// them without waiting for durability, until the file system stops.
    private static final class CrashWorkload {
        static final int COMMITS = 4;
        private final boolean encrypted;
        // The commits acknowledged durable, and those made visible
        int durable;
        int visible;

        CrashWorkload(boolean encrypted) {
            this.encrypted = encrypted;
        }

        static byte[] value(int index) {
            return BorrowedValuesTest.value(100 + index, ODD);
        }

        void run(Path directory) throws InterruptedException {
            Ledger ledger;
            try {
                ledger = options(directory, encrypted).build();
            } catch (RuntimeException _) {
                return;
            }
            var buffer = new byte[ODD];
            try {
                for (var index = 0; index < COMMITS; index++) {
                    System.arraycopy(value(index), 0, buffer, 0, ODD);
                    // A commit that throws may still have reached the log
                    visible = index + 1;
                    var commit = ledger.commit(ledger.newBatch().put(FILES, "k" + index, buffer));
                    scribble(buffer);
                    if (index < COMMITS / 2) {
                        commit.awaitDurable();
                        durable = index + 1;
                    }
                }
                ledger.close();
                durable = visible;
            } catch (RuntimeException _) {
                // The file system stopped where it was told to
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // It stays stopped
                }
            }
        }
    }
}
