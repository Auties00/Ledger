package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
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
import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What memory a store keeps reachable: the arrays of the batches resident values came in, the last commit of the
/// pipeline, and how keys that differ only where a hash could be made to cancel spread over the index.
class MemoryRetentionTest {
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());

    @Test
    @DisplayName("a resident byte array is held once; another resident value takes an array of its own beside large values, and shares its batch's alone")
    void residentValueOwnsItsBytes(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES).build()) {
            var session = new byte[1_500];
            session[7] = 7;
            var name = "n".repeat(1_500);
            var batch = ledger.newBatch().put(SESSIONS, new byte[]{1}, session).put(NAMES, new byte[]{1}, name);
            for (var i = 0; i < 10; i++) {
                batch.put(MESSAGES, new byte[]{2, (byte) i}, new byte[4_096]);
            }
            ledger.commit(batch).awaitDurable();
            ledger.commit(ledger.newBatch().put(NAMES, new byte[]{9}, name)).awaitDurable();
            // A byte array is its own encoding: the store keeps one copy, which reads return and checkpoints write
            var held = head(ledger, SESSIONS, new byte[]{1});
            assertEquals(1_500, held.sourceBytesForTests());
            assertTrue(held.decoded != session, "the store holds the caller's array");
            assertEquals(7, ledger.get(SESSIONS, new byte[]{1}).orElseThrow()[7]);
            // Another value would keep the 40 KiB of messages reachable on the batch's array
            assertEquals(1_500, head(ledger, NAMES, new byte[]{1}).sourceBytesForTests());
            // Alone in its batch, it shares the batch's array, which holds little else
            var alone = head(ledger, NAMES, new byte[]{9}).sourceBytesForTests();
            assertTrue(alone > 1_500 && alone <= 2 * 1_500 + 64, "a value alone in its batch keeps " + alone + " bytes");
        }
    }

    @Test
    @DisplayName("a paged value is copied, alone, off a batch that holds other bytes, into arrays below half a G1 region; one alone in its batch shares it")
    void pagedValueLeavesItsBatch(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            ledger.valueSlab().forceColdForTests();
            ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[1_500]).put(MESSAGES, new byte[]{1}, new byte[1_500]))
                    .awaitDurable();
            // The session's bytes, which the store holds a copy of, are not kept for the message: the session took an
            // array of its own at the put, and the message keeps the batch's array, with the two keys beside it
            assertEquals(1_502, head(ledger, MESSAGES, new byte[]{1}).sourceBytesForTests());
            ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{2}, new byte[1_500])).awaitDurable();
            assertEquals(1_501, head(ledger, MESSAGES, new byte[]{2}).sourceBytesForTests());
            var batch = ledger.newBatch().put(SESSIONS, new byte[]{3}, new byte[64]);
            for (var i = 0; i < 400; i++) {
                batch.put(MESSAGES, new byte[]{3, (byte) (i >>> 8), (byte) i}, new byte[2_000]);
            }
            batch.put(MESSAGES, new byte[]{4}, new byte[ArrayWriteBatch.CHUNK_BYTES]);
            ledger.commit(batch).awaitDurable();
            var chunks = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Object, Boolean>());
            for (var i = 0; i < 400; i++) {
                var version = head(ledger, MESSAGES, new byte[]{3, (byte) (i >>> 8), (byte) i});
                var bytes = version.sourceBytesForTests();
                assertTrue(bytes <= ArrayWriteBatch.CHUNK_BYTES, "a value of a large batch keeps " + bytes + " bytes");
                chunks.add(version.sourceForTests());
            }
            assertTrue(chunks.size() >= 4, "800,000 bytes of values took " + chunks.size() + " arrays");
            // A value of half a chunk or more takes an array of its own
            assertEquals(ArrayWriteBatch.CHUNK_BYTES, head(ledger, MESSAGES, new byte[]{4}).sourceBytesForTests());
        }
    }

    @Test
    @DisplayName("the values a commit keeps, and the bounds of its range removal, survive the pool handing its batch's array to another batch")
    void reusedBatchArray(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            for (var i = 0; i < 10; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{5, (byte) i}, new byte[]{(byte) i})).awaitDurable();
            }
            // In the tree, so reads of them consult the removal's bounds rather than versions it cut
            ledger.checkpoint();
            // Large enough for the pool's arrays, with a resident value so the paged ones are copied off it
            var first = ledger.newBatch().put(SESSIONS, new byte[]{1}, filled(3_000, 1))
                    .deleteRange(MESSAGES, com.github.auties00.ledger.KeyRange.between(new byte[]{5, 2}, new byte[]{5, 6}));
            for (var i = 0; i < 4; i++) {
                first.put(MESSAGES, new byte[]{6, (byte) i}, filled(2_000, 10 + i));
            }
            ledger.commit(first).awaitDurable();
            for (var round = 0; round < 8; round++) {
                // Each batch takes an array of the same size from the pool and fills it with other bytes
                var other = ledger.newBatch().put(SESSIONS, new byte[]{2}, filled(3_000, 0x7F));
                for (var i = 0; i < 4; i++) {
                    other.put(MESSAGES, new byte[]{7, (byte) round, (byte) i}, filled(2_000, 0x55));
                }
                ledger.commit(other).awaitDurable();
            }
            assertEquals(3_000, ledger.get(SESSIONS, new byte[]{1}).orElseThrow().length);
            assertEquals(1, ledger.get(SESSIONS, new byte[]{1}).orElseThrow()[2_999]);
            for (var i = 0; i < 4; i++) {
                var value = ledger.get(MESSAGES, new byte[]{6, (byte) i}).orElseThrow();
                assertEquals(2_000, value.length);
                assertEquals(10 + i, value[0]);
                assertEquals(10 + i, value[1_999]);
            }
            for (var i = 0; i < 10; i++) {
                assertEquals(i < 2 || i >= 6, ledger.get(MESSAGES, new byte[]{5, (byte) i}).isPresent(), "key 5," + i);
            }
        }
    }

    @Test
    @DisplayName("a store in memory keeps each paged value on an array of its own, not on its batch's")
    void memoryStoreOwnsItsValues() throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(MESSAGES).build()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 50; i++) {
                batch.put(MESSAGES, new byte[]{8, (byte) i}, new byte[200]);
            }
            ledger.commit(batch);
            assertEquals(200, head(ledger, MESSAGES, new byte[]{8, 0}).sourceBytesForTests());
        }
    }

    @Test
    @DisplayName("the chains the sweep drops after a checkpoint, which only commits take out of the ordered map, stay reachable through the links of a descending scan within a thousand")
    void sweptChainsLeaveTheLinks(@TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 30).build()) {
            var count = 8_000;
            for (var batch = 0; batch < count / 1_000; batch++) {
                var writes = ledger.newBatch();
                for (var i = 0; i < 1_000; i++) {
                    writes.put(MESSAGES, ByteBuffer.allocate(4).putInt(batch * 1_000 + i).array(), new byte[16]);
                }
                ledger.commit(writes).awaitDurable();
            }
            // Orders the chains and links them
            assertEquals(count, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            var references = new java.util.ArrayList<java.lang.ref.WeakReference<Chain>>();
            var state = ledger.state(MESSAGES);
            for (var i = 0; i < count; i++) {
                references.add(new java.lang.ref.WeakReference<>(state.chain(MemorySegment.ofArray(ByteBuffer.allocate(4).putInt(i).array()))));
            }
            // A reader of the tree before the checkpoint keeps every chain through its prune; the sweep drops them after
            try (var _ = ledger.snapshot()) {
                ledger.checkpoint();
            }
            for (var i = 0; i < 400; i++) {
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[]{(byte) i})).awaitDurable();
            }
            assertEquals(0, state.chains(), "the sweep left chains in memory");
            var retained = references.size();
            for (var attempt = 0; attempt < 10 && retained > 1_300; attempt++) {
                System.gc();
                Thread.sleep(20);
                retained = 0;
                for (var reference : references) {
                    if (reference.get() != null) {
                        retained++;
                    }
                }
            }
            assertTrue(retained <= 1_300, retained + " of the " + count + " chains the sweep dropped stayed reachable");
            assertEquals(count, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
        }
    }

    @ParameterizedTest(name = "hot slab: {0}")
    @ValueSource(booleans = {false, true})
    @Timeout(120)
    @DisplayName("what a snapshot kept through a checkpoint stays for it, and a tick sweeps it off once it closed, with no commit to do so")
    void tickSweepsAQuietStore(boolean hot, @TempDir Path directory) throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40).build()) {
            var checkpointer = ledger.checkpointer();
            IdleTicker.unwatch(checkpointer);
            if (hot) {
                ledger.valueSlab().forceHotForTests(1 << 20);
            }
            var count = 8_000;
            for (var i = 0; i < count; i++) {
                var durable = ledger.commit(ledger.newBatch().put(MESSAGES, ByteBuffer.allocate(4).putInt(i).array(), new byte[]{(byte) i}));
                if (i == count - 1) {
                    durable.awaitDurable();
                }
            }
            var state = ledger.state(MESSAGES);
            try (var snapshot = ledger.snapshot()) {
                ledger.checkpoint();
                checkpointer.tick();
                while (checkpointer.sweepingForTests()) {
                    Thread.sleep(1);
                }
                assertEquals(count, state.chains(), "the sweep dropped what the snapshot may read");
                assertEquals(count, snapshot.size(MESSAGES));
            }
            checkpointer.tick();
            while (state.chains() != 0) {
                Thread.sleep(1);
            }
            for (var i = 0; i < count; i++) {
                assertArrayEquals(new byte[]{(byte) i}, ledger.get(MESSAGES, ByteBuffer.allocate(4).putInt(i).array()).orElseThrow());
            }
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("sweeps of the ticks beside commits, snapshots and checkpoints leave every snapshot reading what it first read, and every key as last written across a reopen")
    void tickSweepsBesideCommits(@TempDir Path directory) throws Exception {
        var keys = 1_000;
        var writers = 3;
        var stop = new AtomicBoolean();
        var last = new int[writers][keys];
        var failure = new AtomicReference<Throwable>();
        var builder = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40);
        try (var ledger = (LiveLedger) builder.build()) {
            var checkpointer = ledger.checkpointer();
            IdleTicker.unwatch(checkpointer);
            var threads = new ArrayList<Thread>();
            for (var writer = 0; writer < writers; writer++) {
                var number = writer;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var round = 1; !stop.get(); round++) {
                            var key = round % keys;
                            ledger.commit(ledger.newBatch().put(MESSAGES, sweepKey(number, key), ByteBuffer.allocate(4).putInt(round).array()));
                            last[number][key] = round;
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            for (var reader = 0; reader < 2; reader++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new java.util.Random(7);
                        while (!stop.get()) {
                            try (var snapshot = ledger.snapshot()) {
                                var key = sweepKey(random.nextInt(writers), random.nextInt(keys));
                                var first = snapshot.get(MESSAGES, key);
                                var count = snapshot.size(MESSAGES);
                                Thread.sleep(2);
                                assertEquals(first.map(ByteBuffer::wrap), snapshot.get(MESSAGES, key).map(ByteBuffer::wrap));
                                assertEquals(count, snapshot.size(MESSAGES));
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            var deadline = System.nanoTime() + 3_000_000_000L;
            for (var round = 0; System.nanoTime() < deadline && failure.get() == null; round++) {
                checkpointer.tick();
                Thread.sleep(2);
                if (round % 50 == 49) {
                    ledger.checkpoint();
                }
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
        }
        try (var ledger = builder.build()) {
            for (var writer = 0; writer < writers; writer++) {
                for (var key = 0; key < keys; key++) {
                    if (last[writer][key] != 0) {
                        assertArrayEquals(ByteBuffer.allocate(4).putInt(last[writer][key]).array(), ledger.get(MESSAGES, sweepKey(writer, key)).orElseThrow());
                    }
                }
            }
        }
    }

    private static byte[] sweepKey(int writer, int key) {
        return ByteBuffer.allocate(8).putInt(writer).putInt(key).array();
    }

    @Test
    @DisplayName("while checkpoints fail, the chains a range removal empties, which only commits take out of the ordered map, stay on the links within a thousand")
    void removedChainsLeaveTheLinksWhileCheckpointsFail() throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new java.util.concurrent.atomic.AtomicBoolean(true);
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                    && path.getFileName().toString().equals(PageFile.FILE)) {
                throw new java.io.IOException("No space left on device");
            }
        });
        var failures = new java.util.concurrent.ConcurrentLinkedQueue<LedgerException>();
        try (var ledger = (LiveLedger) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 30).onBackgroundFailure(failures::add).build()) {
            var count = 8_000;
            for (var batch = 0; batch < count / 1_000; batch++) {
                var writes = ledger.newBatch();
                for (var i = 0; i < 1_000; i++) {
                    writes.put(MESSAGES, ByteBuffer.allocate(4).putInt(batch * 1_000 + i).array(), new byte[16]);
                }
                ledger.commit(writes).awaitDurable();
            }
            var above = ByteBuffer.allocate(4).putInt(1_000_000).array();
            ledger.commit(ledger.newBatch().put(MESSAGES, above, new byte[16])).awaitDurable();
            // Orders the chains and links them
            assertEquals(count + 1, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            var state = ledger.state(MESSAGES);
            assertThrows(LedgerException.class, ledger::checkpoint);
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, com.github.auties00.ledger.KeyRange.between(
                    ByteBuffer.allocate(4).putInt(0).array(), ByteBuffer.allocate(4).putInt(count).array()))).awaitDurable();
            for (var i = 0; i < 400; i++) {
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{1}, new byte[]{(byte) i})).awaitDurable();
            }
            assertEquals(1, state.chains(), "the range removal left chains in memory");
            // The failed checkpoint's keys hold the chains as well: what the links add is what they reach
            var onLinks = 0;
            for (var chain = state.chain(MemorySegment.ofArray(above)).below(); chain != null && chain != Chain.BOTTOM; chain = chain.below()) {
                onLinks++;
            }
            assertTrue(onLinks <= 1_300, onLinks + " of the " + count + " chains the removal emptied stayed on the links");
            assertEquals(1, ledger.forEach(MESSAGES, com.github.auties00.ledger.KeyRange.<byte[]>all().reversed(), (_, _) -> true));
            failing.set(false);
        }
    }

    @ParameterizedTest(name = "in a directory: {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(60)
    @DisplayName("a commit of a thousand and twenty-four writes or more lets go of its batch once published, with no tick and no later commit, and the next commit follows it")
    void largeCommitForgetsItself(boolean directory, @TempDir Path path) throws Exception {
        try (var ledger = forgetting(directory, path)) {
            var batch = commitWrites(ledger, 1, LiveLedger.FORGET_AFTER_WRITES);
            var latest = ledger.latest();
            assertTrue(collected(batch), "the pipeline kept a commit of " + LiveLedger.FORGET_AFTER_WRITES + " writes");
            assertEquals(latest, ledger.latest());
            ledger.commit(ledger.newBatch().put(MESSAGES, forgetKey(2, 0), new byte[]{9})).awaitDurable();
            assertEquals(latest + 1, ledger.latest());
            assertEquals(LiveLedger.FORGET_AFTER_WRITES + 1, ledger.size(MESSAGES));
            for (var i = 0; i < LiveLedger.FORGET_AFTER_WRITES; i++) {
                assertArrayEquals(new byte[]{(byte) i}, ledger.get(MESSAGES, forgetKey(1, i)).orElseThrow());
            }
            assertArrayEquals(new byte[]{9}, ledger.get(MESSAGES, forgetKey(2, 0)).orElseThrow());
            // A second large commit, after the empty start the first left, lets go of itself too
            assertTrue(collected(commitWrites(ledger, 3, 2_000)), "the pipeline kept a commit of 2,000 writes");
            assertEquals(latest + 2, ledger.latest());
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("a commit of fewer writes stays the pipeline's last until a tick lets go of it")
    void smallCommitWaitsForATick(@TempDir Path path) throws Exception {
        try (var ledger = forgetting(true, path)) {
            var batch = commitWrites(ledger, 1, LiveLedger.FORGET_AFTER_WRITES - 1);
            assertFalse(collected(batch), "a commit of " + (LiveLedger.FORGET_AFTER_WRITES - 1) + " writes let go of itself");
            ledger.checkpointer().tick();
            assertTrue(collected(batch), "the tick kept the last commit");
            assertEquals(LiveLedger.FORGET_AFTER_WRITES - 1, ledger.size(MESSAGES));
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("large commits that let go of themselves beside small ones on the same keys, range removals, snapshots, checkpoints and ticks keep every count exact and every sequence taken, across a reopen")
    void largeCommitsBesideOthers(@TempDir Path path) throws Exception {
        var writers = 3;
        var ownKeys = 4_000;
        var shared = 50;
        var stop = new AtomicBoolean();
        var failure = new AtomicReference<Throwable>();
        var commits = new AtomicLong();
        var written = new BitSet[writers];
        var builder = Ledger.builder().directory(path).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40);
        long expected;
        try (var ledger = (LiveLedger) builder.build()) {
            var checkpointer = ledger.checkpointer();
            IdleTicker.unwatch(checkpointer);
            var start = ledger.latest();
            var threads = new ArrayList<Thread>();
            for (var writer = 0; writer < writers; writer++) {
                var number = writer;
                var keys = written[writer] = new BitSet(ownKeys);
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new Random(number);
                        for (var round = 0; !stop.get() || round < 3; round++) {
                            var batch = ledger.newBatch();
                            var value = ByteBuffer.allocate(4).putInt(round).array();
                            if (round % 3 == 0) {
                                // Half of the writer's keys, which its small commits write as well, and keys every writer writes
                                var from = round / 3 % 2 * (ownKeys / 2);
                                for (var i = from; i < from + ownKeys / 2; i++) {
                                    batch.put(MESSAGES, forgetKey(number, i), value);
                                    keys.set(i);
                                }
                                for (var i = 0; i < shared; i++) {
                                    batch.put(MESSAGES, forgetKey(0xFF, i), value);
                                }
                            } else {
                                for (var i = 0; i < 10; i++) {
                                    var key = random.nextInt(ownKeys);
                                    batch.put(MESSAGES, forgetKey(number, key), value);
                                    keys.set(key);
                                }
                                batch.put(SESSIONS, new byte[]{(byte) number}, value);
                            }
                            var commit = ledger.commit(batch);
                            commits.incrementAndGet();
                            if (round % 7 == 0) {
                                commit.awaitDurable();
                            }
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    var removed = KeyRange.between(forgetKey(0xFE, 0), forgetKey(0xFE, 100));
                    while (!stop.get()) {
                        var batch = ledger.newBatch();
                        for (var i = 0; i < 20; i++) {
                            batch.put(MESSAGES, forgetKey(0xFE, i), new byte[]{1});
                        }
                        ledger.commit(batch);
                        commits.incrementAndGet();
                        ledger.commit(ledger.newBatch().deleteRange(MESSAGES, removed));
                        commits.incrementAndGet();
                        assertTrue(ledger.get(MESSAGES, forgetKey(0xFE, 3)).isEmpty(), "the range removal left a key");
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    var random = new Random(11);
                    while (!stop.get()) {
                        try (var snapshot = ledger.snapshot()) {
                            var key = forgetKey(random.nextInt(writers), random.nextInt(ownKeys));
                            var first = snapshot.get(MESSAGES, key);
                            var count = snapshot.size(MESSAGES);
                            Thread.sleep(1);
                            assertEquals(first.map(ByteBuffer::wrap), snapshot.get(MESSAGES, key).map(ByteBuffer::wrap));
                            assertEquals(count, snapshot.size(MESSAGES));
                        }
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            var deadline = System.nanoTime() + 3_000_000_000L;
            for (var round = 0; System.nanoTime() < deadline && failure.get() == null; round++) {
                checkpointer.tick();
                Thread.sleep(2);
                if (round % 40 == 39) {
                    ledger.checkpoint();
                }
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertEquals(start + commits.get(), ledger.latest(), "a sequence was skipped or taken twice");
            var own = 0L;
            for (var keys : written) {
                own += keys.cardinality();
            }
            expected = own + shared;
            assertEquals(expected, ledger.size(MESSAGES));
            assertEquals(writers, ledger.size(SESSIONS));
            // The pipeline still lets go of a large commit once quiet
            assertTrue(collected(commitWrites(ledger, 0xFD, 2_000)), "the pipeline kept a commit of 2,000 writes");
            expected += 2_000;
            assertEquals(expected, ledger.size(MESSAGES));
        }
        try (var ledger = builder.build()) {
            assertEquals(expected, ledger.size(MESSAGES));
            assertEquals(writers, ledger.size(SESSIONS));
            for (var writer = 0; writer < writers; writer++) {
                for (var key = 0; key < ownKeys; key++) {
                    assertEquals(written[writer].get(key), ledger.get(MESSAGES, forgetKey(writer, key)).isPresent(), "key " + writer + "," + key);
                }
            }
            assertTrue(ledger.get(MESSAGES, forgetKey(0xFE, 0)).isEmpty());
        }
    }

    @ParameterizedTest(name = "process crash: {0}")
    @ValueSource(booleans = {false, true})
    @Timeout(60)
    @DisplayName("a store that let go of large commits and made small ones after them recovers every durable commit from its log, counts included")
    void recoversAfterForgottenCommits(boolean processCrash) throws Exception {
        var disk = CrashFileSystem.create();
        CrashFileSystem survivor;
        long latest;
        try (var ledger = (LiveLedger) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40).build()) {
            IdleTicker.unwatch(ledger.checkpointer());
            assertTrue(collected(commitWrites(ledger, 1, 3_000)), "the pipeline kept a commit of 3,000 writes");
            for (var i = 0; i < 20; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, forgetKey(1, i), new byte[]{(byte) (100 + i)})
                        .put(SESSIONS, new byte[]{1}, new byte[]{(byte) i})).awaitDurable();
            }
            assertTrue(collected(commitWrites(ledger, 2, 2_000)), "the pipeline kept a commit of 2,000 writes");
            ledger.commit(ledger.newBatch().put(MESSAGES, forgetKey(3, 0), new byte[]{7})).awaitDurable();
            latest = ledger.latest();
            survivor = processCrash ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(5));
        }
        try (var ledger = (LiveLedger) Ledger.builder().directory(survivor.getPath("/store")).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).build()) {
            assertEquals(latest, ledger.latest());
            assertEquals(3_000 + 2_000 + 1, ledger.size(MESSAGES));
            assertEquals(1, ledger.size(SESSIONS));
            assertArrayEquals(new byte[]{19}, ledger.get(SESSIONS, new byte[]{1}).orElseThrow());
            for (var i = 0; i < 3_000; i++) {
                assertArrayEquals(new byte[]{(byte) (i < 20 ? 100 + i : i)}, ledger.get(MESSAGES, forgetKey(1, i)).orElseThrow());
            }
            for (var i = 0; i < 2_000; i++) {
                assertArrayEquals(new byte[]{(byte) i}, ledger.get(MESSAGES, forgetKey(2, i)).orElseThrow());
            }
            assertArrayEquals(new byte[]{7}, ledger.get(MESSAGES, forgetKey(3, 0)).orElseThrow());
            assertEquals(3_000 + 2_000 + 1, ledger.forEach(MESSAGES, KeyRange.<byte[]>all().reversed(), (_, _) -> true));
        }
    }

    /// Opens a store whose last commit only a commit's own size or an explicit tick lets go of: in a directory, one the
    /// [IdleTicker] does not tick, with no checkpoint due.
    private static LiveLedger forgetting(boolean directory, Path path) {
        if (!directory) {
            return (LiveLedger) Ledger.builder().memory().keyspace(SESSIONS).keyspace(MESSAGES).build();
        }
        var ledger = (LiveLedger) Ledger.builder().directory(path).encryption(Encryption.key(new byte[32]))
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40).build();
        IdleTicker.unwatch(ledger.checkpointer());
        return ledger;
    }

    /// Commits `count` puts of keys under `prefix` and waits for them to be durable, keeping no strong reference to the
    /// batch.
    private static WeakReference<WriteBatch> commitWrites(LiveLedger ledger, int prefix, int count) throws InterruptedException {
        var batch = ledger.newBatch();
        for (var i = 0; i < count; i++) {
            batch.put(MESSAGES, forgetKey(prefix, i), new byte[]{(byte) i});
        }
        ledger.commit(batch).awaitDurable();
        return new WeakReference<>(batch);
    }

    /// {@return whether the collector took what `reference` refers to, within a few collections}
    private static boolean collected(WeakReference<?> reference) throws InterruptedException {
        for (var attempt = 0; attempt < 20 && reference.get() != null; attempt++) {
            System.gc();
            Thread.sleep(10);
        }
        return reference.get() == null;
    }

    private static byte[] forgetKey(int prefix, int key) {
        return new byte[]{(byte) prefix, (byte) (key >>> 8), (byte) key};
    }

    private static byte[] filled(int length, int value) {
        var bytes = new byte[length];
        java.util.Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    @Test
    @DisplayName("keys whose words differ only in bits a seeded multiply-and-rotate hash cancels get different hashes")
    void craftedKeysSpread() {
        // Flipping the top bit of one word and bit 28 of the next left a multiply-and-rotate state unchanged for every
        // seed: 16 keys that collided under any seed
        var hashes = new HashSet<Integer>();
        for (var pattern = 0; pattern < 16; pattern++) {
            // Word p carries the flip of pair p, word p + 1 the bit that cancels it
            var words = new long[5];
            for (var pair = 0; pair < 4; pair++) {
                if ((pattern >>> pair & 1) != 0) {
                    words[pair] ^= Long.MIN_VALUE;
                    words[pair + 1] ^= 1L << 28;
                }
            }
            var key = ByteBuffer.allocate(40).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            for (var word : words) {
                key.putLong(word);
            }
            var bytes = key.array();
            hashes.add(Segments.hash(MemorySegment.ofArray(bytes)));
        }
        assertTrue(hashes.size() > 12, "the crafted keys share " + (16 - hashes.size() + 1) + " hashes");
    }
}
