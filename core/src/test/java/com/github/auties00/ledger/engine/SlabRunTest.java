package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Batches of many values that a store would keep on an array of their size, placed on a chunk of a hot [ValueSlab] as
/// one run instead: where each value lands, when the run falls back to the array, what a commit that failed and its
/// retry leave, and that the values read back beside concurrent commits, checkpoints and chunk drops and after a crash.
///
/// One thread commits, so every run of a test but the concurrent one goes to the same stripe, whose chunk the test can
/// fill to a known point first.
class SlabRunTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], String> NAMES = Keyspace.resident("names", KeyCodec.bytes(), ValueCodec.utf8());
    private static final long REGION = 1L << 20;
    private static final int CHUNK = (int) REGION - 64;

    private static Ledger.Options store(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(SESSIONS).keyspace(MESSAGES).keyspace(NAMES);
    }

    private static LiveLedger hot(Path directory) {
        var ledger = (LiveLedger) store(directory).build();
        ledger.valueSlab().forceHotForTests(REGION);
        return ledger;
    }

    private static Version head(LiveLedger ledger, Keyspace<?, ?> keyspace, byte[] key) {
        return ledger.state(keyspace).chain(MemorySegment.ofArray(key)).head;
    }

    private static byte[] filled(int length, int seed) {
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (seed + i * 31);
        }
        return value;
    }

    private static byte[] key(int prefix, int index) {
        return new byte[]{(byte) prefix, (byte) index};
    }

    // `count` puts of `length` bytes under keys of `prefix`, the value of each seeded by `seed` plus its index
    private static WriteBatch puts(LiveLedger ledger, int prefix, int count, int length, int seed) {
        var batch = ledger.newBatch();
        for (var i = 0; i < count; i++) {
            batch.put(MESSAGES, key(prefix, i), filled(length, seed + i));
        }
        return batch;
    }

    private static void assertValues(LiveLedger ledger, int prefix, int count, int length, int seed) {
        for (var i = 0; i < count; i++) {
            assertArrayEquals(filled(length, seed + i), ledger.get(MESSAGES, key(prefix, i)).orElseThrow(), "key " + prefix + "," + i);
        }
    }

    private static Object sharedSource(LiveLedger ledger, int prefix, int count) {
        var source = head(ledger, MESSAGES, key(prefix, 0)).sourceForTests();
        for (var i = 0; i < count; i++) {
            assertEquals(source, head(ledger, MESSAGES, key(prefix, i)).sourceForTests(), "the source of key " + prefix + "," + i);
        }
        return source;
    }

    @Nested
    @DisplayName("placement")
    class Placement {
        @Test
        @DisplayName("a batch of a hundred values goes to the slab's chunk as one run, which the batches after it, on the arrays it gave back, leave intact")
        void run(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                ledger.commit(puts(ledger, 1, 100, 256, 0)).awaitDurable();
                assertEquals(1, ledger.valueSlab().heldForTests());
                assertEquals(CHUNK, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                sharedSource(ledger, 1, 100);
                assertValues(ledger, 1, 100, 256, 0);
                for (var round = 2; round < 10; round++) {
                    ledger.commit(puts(ledger, round, 100, 256, round * 1_000)).awaitDurable();
                }
                assertValues(ledger, 1, 100, 256, 0);
                for (var round = 2; round < 10; round++) {
                    assertValues(ledger, round, 100, 256, round * 1_000);
                }
            }
        }

        @Test
        @DisplayName("a run that does not fit what its stripe's chunk has left goes, whole, to a new chunk, and the old one's tail is counted")
        void newChunk(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var slab = ledger.valueSlab();
                // Ten of these leave 48,512 bytes of the chunk, less than the run of 60,038 bytes below
                for (var i = 0; i < 10; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, key(1, i), filled(100_000, i)));
                }
                var first = head(ledger, MESSAGES, key(1, 9)).sourceForTests();
                ledger.commit(puts(ledger, 2, 20, 3_000, 50)).awaitDurable();
                var second = sharedSource(ledger, 2, 20);
                assertNotEquals(first, second);
                assertEquals(CHUNK, head(ledger, MESSAGES, key(2, 0)).sourceBytesForTests());
                assertEquals(2L * CHUNK, slab.chunkBytes());
                assertEquals(1, slab.heldForTests());
                assertEquals(48_512 + (long) CHUNK, slab.slackBytes());
                assertValues(ledger, 2, 20, 3_000, 50);
                for (var i = 0; i < 10; i++) {
                    assertArrayEquals(filled(100_000, i), ledger.get(MESSAGES, key(1, i)).orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("a run longer than a sixteenth of a chunk stays on an array of its size, and makes no chunk")
        void tooLong(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                // 80,038 bytes from the first value to the end of the last, above 65,532
                ledger.commit(puts(ledger, 1, 20, 4_000, 7)).awaitDurable();
                assertEquals(0, ledger.valueSlab().heldForTests());
                assertEquals(0, ledger.valueSlab().chunkBytes());
                assertEquals(20 * 4_002, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                sharedSource(ledger, 1, 20);
                assertValues(ledger, 1, 20, 4_000, 7);
                // Just within the bound: 20 values of 3,270 bytes take 65,438
                ledger.commit(puts(ledger, 2, 20, 3_270, 8)).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, key(2, 0)).sourceBytesForTests());
                assertValues(ledger, 2, 20, 3_270, 8);
            }
        }

        @Test
        @DisplayName("a resident value among the run's takes an array of its own, and a removal's key does not keep the run off the slab")
        void mixed(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(3, 0), filled(10, 3))).awaitDurable();
                var batch = ledger.newBatch();
                for (var i = 0; i < 20; i++) {
                    batch.put(MESSAGES, key(1, i), filled(200, i));
                    if (i == 10) {
                        batch.put(NAMES, key(2, 0), "n".repeat(100));
                        batch.delete(MESSAGES, key(3, 0));
                    }
                }
                ledger.commit(batch).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                sharedSource(ledger, 1, 20);
                assertEquals(100, head(ledger, NAMES, key(2, 0)).sourceBytesForTests());
                assertEquals("n".repeat(100), ledger.get(NAMES, key(2, 0)).orElseThrow());
                assertTrue(ledger.get(MESSAGES, key(3, 0)).isEmpty());
                assertValues(ledger, 1, 20, 200, 0);
            }
        }

        @Test
        @DisplayName("a run of eight values or more goes to the slab whatever its key space overwrites; a smaller one only if the key space is judged worthwhile")
        void judged(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                // Seven puts and ten removals: more writes than the slab takes value by value
                ledger.commit(withRemovals(puts(ledger, 1, 7, 200, 0))).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                var overwrites = ledger.state(MESSAGES).overwrites;
                for (var i = 0; i < 64; i++) {
                    overwrites.sample(Version.ofOwn(Long.MAX_VALUE, new byte[1], null, null, null), 1_024);
                }
                assertTrue(!overwrites.worthwhile());
                ledger.commit(withRemovals(puts(ledger, 2, 7, 200, 100))).awaitDurable();
                assertEquals(7 * 202 + 10 * 2, head(ledger, MESSAGES, key(2, 0)).sourceBytesForTests());
                sharedSource(ledger, 2, 7);
                ledger.commit(withRemovals(puts(ledger, 3, 8, 200, 200))).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, key(3, 0)).sourceBytesForTests());
                sharedSource(ledger, 3, 8);
                assertValues(ledger, 1, 7, 200, 0);
                assertValues(ledger, 2, 7, 200, 100);
                assertValues(ledger, 3, 8, 200, 200);
            }
        }

        private static WriteBatch withRemovals(WriteBatch batch) {
            for (var i = 0; i < 10; i++) {
                batch.delete(MESSAGES, key(9, i));
            }
            return batch;
        }

        @Test
        @DisplayName("a commit whose values are a run counts none of the view, session and array a commit of its own allocates")
        void heap(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var log = ledger.log();
                // The first commit takes the frames the open deferred to its group
                ledger.commit(ledger.newBatch().put(MESSAGES, key(0, 0), new byte[100])).awaitDurable();
                var before = log.appendedHeap();
                ledger.commit(puts(ledger, 1, 20, 200, 0)).awaitDurable();
                var shared = log.appendedHeap() - before;
                assertEquals(CHUNK, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                ledger.valueSlab().forceColdForTests();
                before = log.appendedHeap();
                ledger.commit(puts(ledger, 2, 20, 200, 0)).awaitDurable();
                var own = log.appendedHeap() - before;
                assertTrue(head(ledger, MESSAGES, key(2, 0)).sourceBytesForTests() < CHUNK);
                assertEquals(LogWriter.COMMIT_HEAP, own - shared);
            }
        }
    }

    @Nested
    @DisplayName("measurement")
    class Measurement {
        private static final long WINDOW = 250_000_000L;
        private static final long MEASURED_REGION = 2L << 20;
        private static final int MEASURED_CHUNK = (int) MEASURED_REGION - 64;

        private final AtomicLong now = new AtomicLong(1_000);

        private LiveLedger measured(Path directory) {
            ValueSlab.measureNewForTests(now::get, () -> MEASURED_REGION);
            try {
                return (LiveLedger) store(directory).build();
            } finally {
                ValueSlab.measureNewForTests(null, null);
            }
        }

        // Six windows of `batches` batches of twenty values of `length` bytes each, or fewer if the slab turns hot
        private void windows(LiveLedger ledger, int batches, int length) {
            var slab = ledger.valueSlab();
            for (var window = 0; window < 6 && !slab.hot(); window++) {
                now.addAndGet(WINDOW);
                for (var i = 0; i < batches; i++) {
                    ledger.commit(puts(ledger, 1, 20, length, window * 1_000 + i));
                }
            }
        }

        @Test
        @Timeout(120)
        @DisplayName("runs longer than a sixteenth of a chunk, committed fast enough to learn the region, never turn the slab hot, so a small commit after them makes no chunk")
        void tooLongStaysCold(@TempDir Path directory) throws Exception {
            try (var ledger = measured(directory)) {
                var slab = ledger.valueSlab();
                // Twenty values of 7,000 bytes run 140,038 bytes, above 131,068; forty a window are 5.6 MB in 250 ms
                windows(ledger, 40, 7_000);
                assertEquals(MEASURED_CHUNK, slab.capacity());
                assertTrue(!slab.hot());
                assertEquals(0, slab.chunkBytes());
                ledger.commit(ledger.newBatch().put(MESSAGES, key(2, 0), filled(100, 2))).awaitDurable();
                assertTrue(head(ledger, MESSAGES, key(2, 0)).sourceBytesForTests() < MEASURED_CHUNK);
                assertEquals(0, slab.heldForTests());
                assertEquals(0, slab.chunkBytes());
                assertArrayEquals(filled(100, 2), ledger.get(MESSAGES, key(2, 0)).orElseThrow());
            }
        }

        @Test
        @Timeout(120)
        @DisplayName("runs within a sixteenth of a chunk, committed as fast, turn the slab hot and land on its chunks")
        void withinTurnsHot(@TempDir Path directory) throws Exception {
            try (var ledger = measured(directory)) {
                var slab = ledger.valueSlab();
                // Twenty values of 3,000 bytes run 60,038 bytes; ninety a window are 5.4 MB in 250 ms
                windows(ledger, 90, 3_000);
                assertTrue(slab.hot());
                ledger.commit(puts(ledger, 3, 20, 3_000, 9)).awaitDurable();
                assertEquals(MEASURED_CHUNK, head(ledger, MESSAGES, key(3, 0)).sourceBytesForTests());
                sharedSource(ledger, 3, 20);
                assertValues(ledger, 3, 20, 3_000, 9);
            }
        }
    }

    @Nested
    @DisplayName("failures")
    class Failures {
        @Test
        @DisplayName("a commit that failed on the batch's own array, retried as a run, leaves that array to the versions it made, through a clear and a refill")
        void retry(@TempDir Path directory) throws Exception {
            var fails = new AtomicBoolean(true);
            var picky = Keyspace.resident("picky", KeyCodec.bytes(), ValueCodec.<byte[]>of(value -> value.length,
                    (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
                    source -> {
                        if (fails.get()) {
                            throw new IllegalStateException("no");
                        }
                        return source.toArray(ValueLayout.JAVA_BYTE);
                    }));
            try (var ledger = (LiveLedger) store(directory).keyspace(picky).build()) {
                var slab = ledger.valueSlab();
                slab.forceColdForTests();
                // A range removal runs the batch alone, so its puts are applied, then rolled back, before the decoder
                // throws; the 4,096 bytes of the batch are a size the pool recycles, so the copy the first prepare
                // exposes would go to the next batch that asks for one, were it given back
                var batch = (ArrayWriteBatch) puts(ledger, 1, 28, 144, 0)
                        .deleteRange(MESSAGES, KeyRange.between(key(9, 0), key(9, 9)));
                batch.putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertEquals(4_096, batch.encodedBytes());
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertTrue(ledger.get(MESSAGES, key(1, 0)).isEmpty());
                var exposed = batch.valueArray(0);
                assertEquals(4_096, exposed.length);
                var before = exposed.clone();
                slab.forceHotForTests(REGION);
                fails.set(false);
                ledger.commit(batch).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, key(1, 0)).sourceBytesForTests());
                sharedSource(ledger, 1, 28);
                assertValues(ledger, 1, 28, 144, 0);
                for (var round = 0; round < 4; round++) {
                    batch.clear();
                    // A first put of 4,096 bytes takes an array of that size from the pool
                    batch.put(MESSAGES, key(2 + round, 0), filled(4_094, 0x55 + round));
                    for (var i = 1; i < 20; i++) {
                        batch.put(MESSAGES, key(2 + round, i), filled(144, 0x55 + round));
                    }
                    ledger.commit(batch).awaitDurable();
                    assertTrue(batch.isEmpty());
                    assertValues(ledger, 1, 28, 144, 0);
                }
                assertArrayEquals(before, exposed, "the array the failed commit's versions sliced is as it was");
                assertArrayEquals(new byte[]{1, 2, 3}, ledger.get(picky, new byte[]{3}).orElseThrow());
            }
        }

        @Test
        @DisplayName("a run retried after a failed commit claims new bytes, leaving those of the first attempt as they were")
        void retryOnSlab(@TempDir Path directory) throws Exception {
            var fails = new AtomicBoolean(true);
            var picky = Keyspace.resident("picky", KeyCodec.bytes(), ValueCodec.<byte[]>of(value -> value.length,
                    (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
                    source -> {
                        if (fails.get()) {
                            throw new IllegalStateException("no");
                        }
                        return source.toArray(ValueLayout.JAVA_BYTE);
                    }));
            try (var ledger = (LiveLedger) store(directory).keyspace(picky).build()) {
                ledger.valueSlab().forceHotForTests(REGION);
                var batch = puts(ledger, 1, 20, 200, 0).deleteRange(MESSAGES, KeyRange.between(key(9, 0), key(9, 9)))
                        .putEncoded(picky, MemorySegment.ofArray(new byte[]{3}), MemorySegment.ofArray(new byte[]{1, 2, 3}));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
                var claimed = ledger.valueSlab().slackBytes();
                fails.set(false);
                ledger.commit(batch).awaitDurable();
                assertValues(ledger, 1, 20, 200, 0);
                // One chunk held throughout, so its slack does not move
                assertEquals(claimed, ledger.valueSlab().slackBytes());
                // The three attempts claimed three runs of the same bytes, from the first value to the end of the last,
                // one after the other from the start of a fresh chunk; the first two stay as they were copied
                var chunk = (MemorySegment) head(ledger, MESSAGES, key(1, 0)).sourceForTests();
                var run = 20 * 202 - 2;
                var last = chunk.asSlice(2L * run, run).toArray(ValueLayout.JAVA_BYTE);
                assertArrayEquals(last, chunk.asSlice(0, run).toArray(ValueLayout.JAVA_BYTE));
                assertArrayEquals(last, chunk.asSlice(run, run).toArray(ValueLayout.JAVA_BYTE));
                assertArrayEquals(filled(200, 0), chunk.asSlice(0, 200).toArray(ValueLayout.JAVA_BYTE));
                ledger.commit(puts(ledger, 2, 20, 200, 77)).awaitDurable();
                assertValues(ledger, 1, 20, 200, 0);
                assertValues(ledger, 2, 20, 200, 77);
                assertEquals(1, ledger.valueSlab().heldForTests());
            }
        }
    }

    @Nested
    @DisplayName("lifetime")
    class Lifetime {
        @Test
        @DisplayName("a long-lived snapshot reads run values across chunk drops, overwrites and checkpoints, and a reopen reads the last ones")
        void snapshot(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                for (var prefix = 0; prefix < 20; prefix++) {
                    ledger.commit(puts(ledger, prefix, 100, 400, prefix * 100));
                }
                try (var snapshot = ledger.snapshot()) {
                    for (var round = 1; round <= 3; round++) {
                        ledger.valueSlab().tick(true);
                        assertEquals(0, ledger.valueSlab().heldForTests());
                        for (var prefix = 0; prefix < 20; prefix++) {
                            ledger.commit(puts(ledger, prefix, 100, 400, prefix * 100 + round * 10_000));
                        }
                        ledger.checkpoint();
                        for (var prefix = 0; prefix < 20; prefix++) {
                            for (var i = 0; i < 100; i++) {
                                assertArrayEquals(filled(400, prefix * 100 + i), snapshot.get(MESSAGES, key(prefix, i)).orElseThrow());
                            }
                            assertValues(ledger, prefix, 100, 400, prefix * 100 + round * 10_000);
                        }
                    }
                }
            }
            try (var ledger = (LiveLedger) store(directory).build()) {
                for (var prefix = 0; prefix < 20; prefix++) {
                    assertValues(ledger, prefix, 100, 400, prefix * 100 + 30_000);
                }
            }
        }

        @Test
        @Timeout(120)
        @DisplayName("writers committing runs beside readers, checkpoints and ticks that drop the chunks never see a byte of another value")
        void concurrent(@TempDir Path directory) throws Exception {
            var writers = 4;
            var keys = 64;
            var rounds = 150;
            var length = 200;
            var done = new AtomicIntegerArray(writers);
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            try (var ledger = hot(directory)) {
                var threads = new ArrayList<Thread>();
                for (var t = 0; t < writers; t++) {
                    var writer = t;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            for (var round = 1; round <= rounds; round++) {
                                var batch = ledger.newBatch();
                                for (var k = 0; k < keys; k++) {
                                    batch.put(MESSAGES, key(writer, k), stamped(length, round, writer, k));
                                }
                                ledger.commit(batch);
                                done.set(writer, round);
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                var others = new ArrayList<Thread>();
                for (var r = 0; r < 2; r++) {
                    others.add(Thread.ofPlatform().start(() -> {
                        try {
                            var random = ThreadLocalRandom.current();
                            while (!stop.get()) {
                                var writer = random.nextInt(writers);
                                var k = random.nextInt(keys);
                                var least = done.get(writer);
                                var value = ledger.get(MESSAGES, key(writer, k)).orElse(null);
                                if (value == null) {
                                    assertEquals(0, least, "a key written once is never absent");
                                    continue;
                                }
                                var round = ByteBuffer.wrap(value).getInt();
                                assertTrue(round >= least, "read round " + round + " after round " + least + " was published");
                                assertArrayEquals(stamped(length, round, writer, k), value);
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                others.add(Thread.ofPlatform().start(() -> {
                    try {
                        var turn = 0;
                        while (!stop.get()) {
                            if (turn++ % 4 == 0) {
                                ledger.checkpoint();
                            } else {
                                ledger.valueSlab().tick(turn % 2 == 0);
                            }
                            Thread.sleep(2);
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
                for (var thread : threads) {
                    thread.join();
                }
                stop.set(true);
                for (var thread : others) {
                    thread.join();
                }
                assertEquals(List.of(), List.copyOf(failures));
                for (var t = 0; t < writers; t++) {
                    for (var k = 0; k < keys; k++) {
                        assertArrayEquals(stamped(length, rounds, t, k), ledger.get(MESSAGES, key(t, k)).orElseThrow());
                    }
                }
                assertTrue(ledger.valueSlab().chunkBytes() > 0, "the runs went to the slab");
            }
            try (var ledger = (LiveLedger) store(directory).build()) {
                for (var t = 0; t < writers; t++) {
                    for (var k = 0; k < keys; k++) {
                        assertArrayEquals(stamped(length, rounds, t, k), ledger.get(MESSAGES, key(t, k)).orElseThrow());
                    }
                }
            }
        }

        // A value that says which round wrote it, and whose other bytes follow from the round, writer and key
        private static byte[] stamped(int length, int round, int writer, int k) {
            var value = filled(length, round * 7_919 + writer * 131 + k);
            ByteBuffer.wrap(value).putInt(round);
            return value;
        }

        @Test
        @DisplayName("a checkpoint takes the values of runs on several chunks into the tree, whose reads find them once their chains are gone")
        void released(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var checkpointer = ledger.checkpointer();
                IdleTicker.unwatch(checkpointer);
                for (var prefix = 0; prefix < 50; prefix++) {
                    ledger.commit(puts(ledger, prefix, 100, 256, prefix));
                }
                var chunks = Collections.newSetFromMap(new IdentityHashMap<Object, Boolean>());
                for (var prefix = 0; prefix < 50; prefix++) {
                    chunks.add(sharedSource(ledger, prefix, 100));
                }
                assertTrue(chunks.size() >= 2, "1.3 MB of runs took " + chunks.size() + " chunks");
                ledger.checkpoint();
                ledger.valueSlab().tick(true);
                var state = ledger.state(MESSAGES);
                for (var i = 0; i < 1_000 && state.chains() != 0; i++) {
                    Thread.sleep(10);
                }
                assertEquals(0, state.chains());
                for (var prefix = 0; prefix < 50; prefix++) {
                    assertValues(ledger, prefix, 100, 256, prefix);
                }
            }
        }
    }

    @Nested
    @DisplayName("crashes")
    class Crashes {
        private static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
        private static final Keyspace<String, String> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.utf8());

        static IntStream seeds() {
            return IntStream.range(0, 40);
        }

        private static Ledger open(Path directory, boolean encrypted) {
            var ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                    .keyspace(PAGED).keyspace(RESIDENT))
                    .segmentSize(65_536)
                    .build();
            ((LiveLedger) ledger).valueSlab().forceHotForTests(REGION);
            return ledger;
        }

        @ParameterizedTest(name = "seed {0}")
        @MethodSource("seeds")
        @DisplayName("a store whose batches went to the slab as runs recovers a state between the last durable commit and the last visible one")
        void recovers(int seed) throws Exception {
            var encrypted = seed % 2 == 0;
            var dry = CrashFileSystem.create();
            var dryRun = new Workload(seed);
            dryRun.run(dry.getPath("/store"), encrypted);
            assertTrue(dryRun.runsOnSlab(), "the workload commits runs to the slab");
            var disk = CrashFileSystem.create();
            var random = new Random(seed * 7_919L + 3);
            if (random.nextBoolean()) {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            } else {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            }
            var workload = new Workload(seed);
            workload.run(disk.getPath("/store"), encrypted);
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                var recovered = new TreeMap<String, TreeMap<String, String>>();
                for (var keyspace : List.of(PAGED, RESIDENT)) {
                    var entries = new TreeMap<String, String>();
                    ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                        entries.put(key, value);
                        return true;
                    });
                    recovered.put(keyspace.name(), entries);
                }
                var match = false;
                for (var commit = workload.durable; commit <= workload.visible && !match; commit++) {
                    match = workload.history.get(commit).equals(recovered);
                }
                if (!match) {
                    fail("The store recovered a state no commit from " + workload.durable + " to " + workload.visible + " left");
                }
                ledger.commit(ledger.newBatch().put(PAGED, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
                assertEquals("recovery", ledger.get(PAGED, "after").orElseThrow());
            }
        }

        /// Batches of 10 to 60 puts of values of 50 to 400 characters, now and then beside a removal or a small resident
        /// value, with checkpoints and reopens between them.
        private static final class Workload {
            private final Random random;
            final List<Map<String, TreeMap<String, String>>> history;
            final TreeMap<String, TreeMap<String, String>> state;
            int visible;
            int durable;
            private boolean onSlab;

            Workload(int seed) {
                this.random = new Random(seed);
                this.history = new ArrayList<>();
                this.state = new TreeMap<>();
                state.put(PAGED.name(), new TreeMap<>());
                state.put(RESIDENT.name(), new TreeMap<>());
                history.add(copy(state));
            }

            boolean runsOnSlab() {
                return onSlab;
            }

            void run(Path directory, boolean encrypted) {
                Ledger ledger = null;
                try {
                    ledger = open(directory, encrypted);
                    for (var step = 0; step < 40; step++) {
                        var choice = random.nextInt(20);
                        if (choice == 0) {
                            ledger.checkpoint();
                            durable = visible;
                        } else if (choice == 1) {
                            ledger.close();
                            durable = visible;
                            ledger = open(directory, encrypted);
                        } else {
                            batch(ledger, choice < 6);
                        }
                    }
                    ledger.close();
                    durable = visible;
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

            private void batch(Ledger ledger, boolean awaitDurable) throws InterruptedException {
                var next = copy(state);
                var batch = ledger.newBatch();
                var writes = 10 + random.nextInt(51);
                String first = null;
                for (var i = 0; i < writes; i++) {
                    var key = "k%03d".formatted(random.nextInt(300));
                    var kind = random.nextInt(40);
                    if (kind == 0) {
                        batch.delete(PAGED, key);
                        next.get(PAGED.name()).remove(key);
                    } else if (kind == 1) {
                        var value = "r" + random.nextInt(1_000);
                        batch.put(RESIDENT, key, value);
                        next.get(RESIDENT.name()).put(key, value);
                    } else {
                        var value = value();
                        batch.put(PAGED, key, value);
                        next.get(PAGED.name()).put(key, value);
                        if (first == null) {
                            first = key;
                        }
                    }
                }
                var commit = ledger.commit(batch);
                if (first != null && writes > 16) {
                    var head = ((LiveLedger) ledger).state(PAGED).head(MemorySegment.ofArray(first.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                    onSlab |= head != null && head.sourceBytesForTests() == CHUNK;
                }
                state.clear();
                state.putAll(next);
                visible++;
                history.add(copy(state));
                if (awaitDurable) {
                    commit.awaitDurable();
                    durable = visible;
                }
            }

            private String value() {
                var length = 50 + random.nextInt(351);
                var builder = new StringBuilder(length);
                for (var i = 0; i < length; i++) {
                    builder.append((char) ('a' + random.nextInt(26)));
                }
                return builder.toString();
            }

            private static TreeMap<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
                var copy = new TreeMap<String, TreeMap<String, String>>();
                state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
                return copy;
            }
        }
    }
}
