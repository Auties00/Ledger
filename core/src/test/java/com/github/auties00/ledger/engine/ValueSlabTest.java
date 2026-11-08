package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The chunks a store's fastest commits keep their small values on: when the slab takes a value, that what a version
/// holds there stays intact, and what turns it hot and cold, with a clock of the test's so no timing is involved.
class ValueSlabTest {
    private static final Keyspace<byte[], byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final byte[] KEY = new byte[32];
    private static final long MIB = 1L << 20;
    private static final long REGION = MIB;
    private static final int CHUNK = (int) REGION - 64;

    private static Ledger.Options store(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(SESSIONS).keyspace(MESSAGES);
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

    @Nested
    @DisplayName("in a store")
    class InAStore {
        @Test
        @DisplayName("a cold slab leaves a value on its batch's array")
        void cold(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) store(directory).build()) {
                ledger.valueSlab().forceColdForTests();
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, new byte[100])).awaitDurable();
                assertEquals(101, head(ledger, MESSAGES, new byte[]{1}).sourceBytesForTests());
                assertEquals(0, ledger.valueSlab().heldForTests());
            }
        }

        @Test
        @DisplayName("a store of a few commits stays cold, and asks the collector nothing")
        void staysCold(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) store(directory).build()) {
                for (var i = 0; i < 100; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{(byte) i}, new byte[100]));
                }
                assertFalse(ledger.valueSlab().hot());
                assertEquals(0, ledger.valueSlab().capacity());
                assertEquals(0, ledger.valueSlab().chunkBytes());
            }
        }

        @Test
        @DisplayName("a hot slab takes a paged value alone in its batch, and one beside a resident value, which stays off it")
        void takes(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, filled(100, 1))).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, new byte[]{1}).sourceBytesForTests());
                ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{2}, filled(1_500, 2)).put(MESSAGES, new byte[]{2}, filled(200, 3))).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, new byte[]{2}).sourceBytesForTests());
                assertEquals(1_500, head(ledger, SESSIONS, new byte[]{2}).sourceBytesForTests());
                assertArrayEquals(filled(100, 1), ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
                assertArrayEquals(filled(200, 3), ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                assertArrayEquals(filled(1_500, 2), ledger.get(SESSIONS, new byte[]{2}).orElseThrow());
            }
        }

        @Test
        @DisplayName("a batch of many writes, a value of half a batch chunk or more, an empty one and one for the file of large values stay off a hot slab")
        void declines(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) store(directory).largeValues(64 << 10).build()) {
                ledger.valueSlab().forceHotForTests(REGION);
                var many = ledger.newBatch();
                for (var i = 0; i < 17; i++) {
                    many.put(MESSAGES, new byte[]{1, (byte) i}, filled(100, i));
                }
                ledger.commit(many).awaitDurable();
                assertEquals(0, ledger.valueSlab().heldForTests());
                assertTrue(head(ledger, MESSAGES, new byte[]{1, 0}).sourceBytesForTests() < CHUNK);
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{2}, new byte[0])).awaitDurable();
                assertEquals(0, ledger.valueSlab().heldForTests());
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{3}, filled(70_000, 3))).awaitDurable();
                assertEquals(0, ledger.valueSlab().heldForTests());
                assertTrue(head(ledger, MESSAGES, new byte[]{3}).external() != null, "the value has a unit");
                assertArrayEquals(filled(70_000, 3), ledger.get(MESSAGES, new byte[]{3}).orElseThrow());
                assertArrayEquals(new byte[0], ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
            }
        }

        @Test
        @DisplayName("in one batch, the values a hot slab takes go to it and the one it cannot take to an array of its own")
        void partly(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var large = ArrayWriteBatch.CHUNK_BYTES;
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, filled(300, 1)).put(MESSAGES, new byte[]{2}, filled(large, 2))
                        .put(MESSAGES, new byte[]{3}, filled(400, 3))).awaitDurable();
                assertEquals(CHUNK, head(ledger, MESSAGES, new byte[]{1}).sourceBytesForTests());
                assertEquals(large, head(ledger, MESSAGES, new byte[]{2}).sourceBytesForTests());
                assertEquals(CHUNK, head(ledger, MESSAGES, new byte[]{3}).sourceBytesForTests());
                assertArrayEquals(filled(300, 1), ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
                assertArrayEquals(filled(large, 2), ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                assertArrayEquals(filled(400, 3), ledger.get(MESSAGES, new byte[]{3}).orElseThrow());
            }
        }

        @Test
        @DisplayName("values on chunks survive the pool handing their batches' arrays to other batches, a checkpoint and a reopen")
        void survives(@TempDir Path directory) throws Exception {
            var count = 2_000;
            try (var ledger = hot(directory)) {
                for (var i = 0; i < count; i++) {
                    // Above the pool's smallest array with the resident value, so the arrays go round
                    ledger.commit(ledger.newBatch().put(SESSIONS, new byte[]{9}, new byte[5_000])
                            .put(MESSAGES, new byte[]{1, (byte) (i >>> 8), (byte) i}, filled(1_000, i)));
                }
                for (var i = 0; i < count; i++) {
                    assertArrayEquals(filled(1_000, i), ledger.get(MESSAGES, new byte[]{1, (byte) (i >>> 8), (byte) i}).orElseThrow(), "key " + i);
                }
                assertTrue(ledger.valueSlab().chunkBytes() >= CHUNK * 2L, "the values filled more than one chunk");
                ledger.checkpoint();
                for (var i = 0; i < count; i++) {
                    assertArrayEquals(filled(1_000, i), ledger.get(MESSAGES, new byte[]{1, (byte) (i >>> 8), (byte) i}).orElseThrow(), "key " + i);
                }
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{2}, filled(50, 7))).awaitDurable();
            }
            try (var ledger = (LiveLedger) store(directory).build()) {
                assertArrayEquals(filled(50, 7), ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                for (var i = 0; i < count; i++) {
                    assertArrayEquals(filled(1_000, i), ledger.get(MESSAGES, new byte[]{1, (byte) (i >>> 8), (byte) i}).orElseThrow(), "key " + i);
                }
            }
        }

        @Test
        @Timeout(60)
        @DisplayName("values of the largest size the slab takes end, at the smallest region")
        void largestValues(@TempDir Path directory) throws Exception {
            var largest = ArrayWriteBatch.CHUNK_BYTES / 2 - 1;
            assertEquals(131_071, largest);
            try (var ledger = hot(directory)) {
                for (var i = 0; i < 40; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{(byte) i}, filled(largest, i)));
                }
                for (var i = 0; i < 40; i++) {
                    assertEquals(CHUNK, head(ledger, MESSAGES, new byte[]{(byte) i}).sourceBytesForTests());
                    assertArrayEquals(filled(largest, i), ledger.get(MESSAGES, new byte[]{(byte) i}).orElseThrow());
                }
            }
        }

        @Test
        @Timeout(60)
        @DisplayName("a value larger than a chunk of a small region falls back to the batch's chunks, and never loops")
        void largerThanChunk(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) store(directory).build()) {
                ledger.valueSlab().forceHotForTests(64 << 10);
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, filled(100_000, 1))).awaitDurable();
                assertEquals(0, ledger.valueSlab().heldForTests());
                assertArrayEquals(filled(100_000, 1), ledger.get(MESSAGES, new byte[]{1}).orElseThrow());
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{2}, filled(60_000, 2))).awaitDurable();
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{3}, filled(60_000, 3))).awaitDurable();
                assertArrayEquals(filled(60_000, 2), ledger.get(MESSAGES, new byte[]{2}).orElseThrow());
                assertArrayEquals(filled(60_000, 3), ledger.get(MESSAGES, new byte[]{3}).orElseThrow());
            }
        }

        @Test
        @DisplayName("a long-lived snapshot reads its values across chunk drops, idle ticks, overwrites and checkpoints")
        void snapshot(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                for (var i = 0; i < 200; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{(byte) i}, filled(4_000, i)));
                }
                try (var snapshot = ledger.snapshot()) {
                    for (var round = 1; round <= 3; round++) {
                        ledger.valueSlab().tick(true);
                        assertEquals(0, ledger.valueSlab().heldForTests());
                        for (var i = 0; i < 200; i++) {
                            ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{(byte) i}, filled(4_000, i + round * 1_000)));
                        }
                        ledger.checkpoint();
                        for (var i = 0; i < 200; i++) {
                            assertArrayEquals(filled(4_000, i), snapshot.get(MESSAGES, new byte[]{(byte) i}).orElseThrow(), "key " + i);
                            assertArrayEquals(filled(4_000, i + round * 1_000), ledger.get(MESSAGES, new byte[]{(byte) i}).orElseThrow(), "key " + i);
                        }
                    }
                }
            }
        }

        @Test
        @Timeout(60)
        @DisplayName("a store that went idle checkpoints values below the idle backlog that hold a whole chunk, and lets go of it")
        void idleCheckpoint(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var key = new byte[]{1};
                ledger.commit(ledger.newBatch().put(MESSAGES, key, new byte[100])).awaitDurable();
                assertEquals(1, ledger.valueSlab().heldForTests());
                // One tick to see the commit, one to find the store idle
                var checkpointer = ledger.checkpointer();
                IdleTicker.unwatch(checkpointer);
                checkpointer.tick();
                checkpointer.tick();
                assertEquals(0, ledger.valueSlab().heldForTests());
                while (ledger.state(MESSAGES).head(MemorySegment.ofArray(key)) != null) {
                    Thread.sleep(10);
                }
                assertArrayEquals(new byte[100], ledger.get(MESSAGES, key).orElseThrow());
            }
        }

        @Test
        @DisplayName("a closed store's slab holds no chunk and makes none")
        void close(@TempDir Path directory) throws Exception {
            ValueSlab slab;
            try (var ledger = hot(directory)) {
                slab = ledger.valueSlab();
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, new byte[100])).awaitDurable();
                assertEquals(1, slab.heldForTests());
            }
            assertEquals(0, slab.heldForTests());
            assertFalse(slab.hot());
            assertFalse(place(slab, new byte[10]));
        }

        @Test
        @DisplayName("the heap estimate of the backlog counts a held chunk whole, and a dropped one's tail")
        void slack(@TempDir Path directory) throws Exception {
            try (var ledger = hot(directory)) {
                var slab = ledger.valueSlab();
                assertEquals(0, slab.slackBytes());
                ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{1}, new byte[100])).awaitDurable();
                assertEquals(CHUNK, slab.slackBytes());
                assertEquals(CHUNK, slab.chunkBytes());
                assertEquals(0, slab.retiredChunkBytes());
                slab.tick(true);
                assertEquals(CHUNK - 100, slab.slackBytes());
                assertEquals(CHUNK, slab.retiredChunkBytes());
            }
        }
    }

    private static boolean place(ValueSlab slab, byte[] value) {
        return slab.place(value, 0, value.length, new MemorySegment[1], new int[1], 0);
    }

    @Nested
    @DisplayName("through a store's commits")
    class ThroughCommits {
        private static final long WINDOW = 250_000_000L;
        private static final int LARGE_CHUNK = (int) (2 * MIB) - 64;

        private final AtomicLong now = new AtomicLong(1_000);
        private long next;

        private LiveLedger measured(Path directory) {
            ValueSlab.measureNewForTests(now::get, () -> 2 * MIB);
            try {
                return (LiveLedger) store(directory).build();
            } finally {
                ValueSlab.measureNewForTests(null, null);
            }
        }

        private static byte[] key(long number) {
            return ByteBuffer.allocate(Long.BYTES).putLong(number).array();
        }

        private byte[] commitFresh(LiveLedger ledger, int length) {
            var key = key(next++);
            ledger.commit(ledger.newBatch().put(MESSAGES, key, new byte[length]));
            return key;
        }

        // The number of commits holds at least 64 samples, so the checkpoint after them counts
        private void freshInterval(LiveLedger ledger) {
            for (var i = 0; i < 8_000; i++) {
                commitFresh(ledger, 1_024);
            }
        }

        private long sourceBytesOfFresh(LiveLedger ledger, int length) throws InterruptedException {
            var key = key(next++);
            ledger.commit(ledger.newBatch().put(MESSAGES, key, new byte[length])).awaitDurable();
            return head(ledger, MESSAGES, key).sourceBytesForTests();
        }

        @Test
        @Timeout(120)
        @DisplayName("fast commits turn the slab hot, overwrites of a few keys turn it down for the key space, and checkpoints of other writes bring it back")
        void activates(@TempDir Path directory) throws Exception {
            try (var ledger = measured(directory)) {
                var slab = ledger.valueSlab();
                var overwrites = ledger.state(MESSAGES).overwrites;
                assertFalse(slab.hot());
                assertEquals(0, slab.capacity());
                for (var window = 0; window < 6 && !slab.hot(); window++) {
                    now.addAndGet(WINDOW);
                    // 20 MiB a second, above the 16 of chunks of 2 MiB
                    for (var i = 0; i < 1_280; i++) {
                        commitFresh(ledger, 4_096);
                    }
                }
                assertTrue(slab.hot());
                assertEquals(LARGE_CHUNK, slab.capacity());
                assertEquals(LARGE_CHUNK, sourceBytesOfFresh(ledger, 4_096));
                assertTrue(overwrites.worthwhile());
                ledger.checkpoint();
                assertTrue(overwrites.worthwhile());

                for (var i = 0; i < 40_000 && overwrites.worthwhile(); i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, new byte[]{9, (byte) (i % 4)}, new byte[1_024]));
                }
                assertFalse(overwrites.worthwhile());
                assertTrue(sourceBytesOfFresh(ledger, 1_024) < LARGE_CHUNK);

                ledger.checkpoint();
                assertFalse(overwrites.worthwhile());
                var checkpoints = 0;
                while (!overwrites.worthwhile()) {
                    assertTrue(++checkpoints <= 6, "the share came back within a few checkpoints");
                    freshInterval(ledger);
                    assertFalse(overwrites.worthwhile(), "the share settled at a checkpoint holds until the next");
                    ledger.checkpoint();
                }
                assertTrue(slab.hot());
                assertEquals(LARGE_CHUNK, sourceBytesOfFresh(ledger, 1_024));
            }
        }

        @Test
        @Timeout(120)
        @DisplayName("a key space whose deliveries of 1-8 KB are removed 64 later never turns the slab hot, at the rate of a webhook inbox")
        void removedSoon(@TempDir Path directory) throws Exception {
            var inFlight = 64;
            try (var ledger = measured(directory)) {
                var slab = ledger.valueSlab();
                for (var window = 0; window < 8; window++) {
                    now.addAndGet(WINDOW);
                    // 12,000 deliveries a second, averaging 4.6 KB
                    for (var i = 0; i < 3_000; i++) {
                        var number = next++;
                        var size = 1_024 + (int) Math.floorMod(number * 7_919, 7_169);
                        ledger.commit(ledger.newBatch().put(MESSAGES, key(number), new byte[size]));
                        if (number >= inFlight) {
                            ledger.commit(ledger.newBatch().delete(MESSAGES, key(number - inFlight)));
                        }
                    }
                }
                assertFalse(ledger.state(MESSAGES).overwrites.worthwhile());
                assertFalse(slab.hot());
                assertEquals(0, slab.chunkBytes());
            }
        }

        @Test
        @Timeout(120)
        @DisplayName("a key space that keeps its deliveries turns the slab hot at the same rate")
        void kept(@TempDir Path directory) throws Exception {
            try (var ledger = measured(directory)) {
                var slab = ledger.valueSlab();
                for (var window = 0; window < 8; window++) {
                    now.addAndGet(WINDOW);
                    for (var i = 0; i < 3_000; i++) {
                        var number = next++;
                        var size = 1_024 + (int) Math.floorMod(number * 7_919, 7_169);
                        ledger.commit(ledger.newBatch().put(MESSAGES, key(number), new byte[size]));
                    }
                }
                assertTrue(ledger.state(MESSAGES).overwrites.worthwhile());
                assertTrue(slab.hot());
                assertTrue(slab.chunkBytes() > 0);
            }
        }
    }

    @Nested
    @DisplayName("alone")
    class Alone {
        @Test
        @Timeout(120)
        @DisplayName("threads claiming from small chunks, while a tick drops them, never share a byte")
        void concurrent() throws Exception {
            record Span(MemorySegment view, int offset, int length, byte seed) {
                void check() {
                    assertTrue(view.isReadOnly());
                    for (var i = 0; i < length; i++) {
                        assertEquals(seed, view.get(ValueLayout.JAVA_BYTE, offset + i));
                    }
                }
            }
            var slab = new ValueSlab(System::nanoTime, () -> 64 << 10, new ValueSlab.Budget(Long.MAX_VALUE));
            slab.forceHotForTests(64 << 10);
            var taken = new AtomicLong();
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var maxHeld = new AtomicInteger();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 8; t++) {
                var seed = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        var random = new java.util.Random(seed);
                        var homes = new MemorySegment[1];
                        var offsets = new int[1];
                        // The last spans only: a tick wastes most of a chunk, and every span keeps its chunk reachable
                        var window = new ArrayDeque<Span>();
                        for (var i = 0; i < 40_000; i++) {
                            var value = new byte[1 + random.nextInt(600)];
                            var stamp = (byte) random.nextInt();
                            Arrays.fill(value, stamp);
                            if (slab.place(value, 0, value.length, homes, offsets, 0)) {
                                window.add(new Span(homes[0], offsets[0], value.length, stamp));
                                taken.incrementAndGet();
                                if (window.size() > 256) {
                                    window.remove().check();
                                }
                            }
                            maxHeld.accumulateAndGet(slab.heldForTests(), Math::max);
                        }
                        window.forEach(Span::check);
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
            }
            var ticker = Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    slab.tick(true);
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException exception) {
                        failures.add(exception);
                        return;
                    }
                }
            });
            for (var thread : threads) {
                thread.join();
            }
            stop.set(true);
            ticker.join();
            assertEquals(List.of(), List.copyOf(failures));
            assertTrue(taken.get() > 0);
            assertTrue(maxHeld.get() <= 8, "held " + maxHeld.get() + " chunks");
            slab.close();
            assertEquals(0, slab.heldForTests());
        }

        @Test
        @Timeout(60)
        @DisplayName("a slab over the process's budget declines at once, and has its chunk in the window after another slab closes")
        void budget() {
            var now = new AtomicLong(1_000);
            var budget = new ValueSlab.Budget(REGION);
            var first = new ValueSlab(now::get, () -> REGION, budget);
            var second = new ValueSlab(now::get, () -> REGION, budget);
            first.forceHotForTests(REGION);
            second.forceHotForTests(REGION);
            assertTrue(place(first, new byte[100]));
            assertFalse(place(second, new byte[100]));
            assertEquals(0, second.heldForTests());
            first.close();
            now.addAndGet(250_000_000L);
            second.note(1L << 18);
            assertTrue(place(second, new byte[100]));
            assertEquals(1, second.heldForTests());
        }

        @Test
        @DisplayName("a slab refused a chunk asks the budget, under its lock, once a window rather than once a value, and tells a commit before it makes room for homes")
        void refusalCost() {
            var now = new AtomicLong(1_000);
            var budget = new ValueSlab.Budget(REGION);
            var first = new ValueSlab(now::get, () -> REGION, budget);
            var second = new ValueSlab(now::get, () -> REGION, budget);
            first.forceHotForTests(REGION);
            second.forceHotForTests(REGION);
            assertTrue(place(first, new byte[100]));
            assertTrue(second.accepting());
            var before = budget.reservesForTests();
            for (var i = 0; i < 10_000; i++) {
                assertFalse(place(second, new byte[1]));
            }
            assertEquals(before + 1, budget.reservesForTests());
            assertFalse(second.accepting());
            assertTrue(first.accepting(), "a slab holding the chunk is not refused");
            now.addAndGet(250_000_000L);
            second.note(1L << 18);
            assertTrue(second.accepting());
            for (var i = 0; i < 10_000; i++) {
                assertFalse(place(second, new byte[1]));
            }
            assertEquals(before + 2, budget.reservesForTests());
        }

        @Test
        @Timeout(60)
        @DisplayName("every thread, whatever its id, has a stripe that may hold a chunk")
        void everyThreadHasAStripe() throws Exception {
            var budget = new ValueSlab.Budget(Long.MAX_VALUE);
            var slab = new ValueSlab(System::nanoTime, () -> REGION, budget);
            slab.forceHotForTests(REGION);
            var declined = new AtomicInteger();
            // One after the other, so no thread finds its stripe's chunk being made
            for (var t = 0; t < 256; t++) {
                Thread.ofPlatform().start(() -> {
                    if (!place(slab, new byte[1])) {
                        declined.incrementAndGet();
                    }
                }).join();
            }
            assertEquals(0, declined.get());
            assertTrue(slab.heldForTests() <= 8, "held " + slab.heldForTests() + " chunks");
            assertEquals(slab.heldForTests(), budget.reservesForTests(), "no thread was refused");
            slab.close();
        }

        @Test
        @DisplayName("the JVM's answer is no region, or a power of two of at least 1 MiB")
        void collector() {
            var region = ValueSlab.collectorRegion();
            assertTrue(region == 0 || region >= MIB && Long.bitCount(region) == 1, "region " + region);
        }

        @Nested
        @DisplayName("measuring")
        class Measuring {
            private final AtomicLong now = new AtomicLong(1_000);
            private final AtomicInteger asked = new AtomicInteger();

            private ValueSlab slab(long region) {
                return new ValueSlab(now::get, () -> {
                    asked.incrementAndGet();
                    return region;
                }, new ValueSlab.Budget(32 * MIB));
            }

            // One window of 250 ms over which `bytes` of values went by
            private void window(ValueSlab slab, long bytes) {
                now.addAndGet(250_000_000L);
                slab.note(bytes);
            }

            private void warm(ValueSlab slab) {
                window(slab, 8 * MIB);
                window(slab, 8 * MIB);
                window(slab, 8 * MIB);
                assertTrue(slab.hot());
            }

            @Test
            @DisplayName("turns hot from the rate of values alone, before it has any chunk, and asks for the region only once fast")
            void warmsUp() {
                var slab = slab(2 * MIB);
                window(slab, MIB);
                assertEquals(0, asked.get());
                assertFalse(slab.hot());
                window(slab, 4 * MIB);
                assertEquals(1, asked.get());
                assertEquals(2 * MIB - 64, slab.capacity());
                assertFalse(slab.hot());
                // Below 16 MiB a second, the rate of this region
                window(slab, 3 * MIB);
                window(slab, 3 * MIB);
                assertFalse(slab.hot());
                window(slab, 8 * MIB);
                assertFalse(slab.hot());
                assertEquals(0, slab.chunkBytes());
                window(slab, 8 * MIB);
                assertTrue(slab.hot());
                assertEquals(1, asked.get());
                assertTrue(place(slab, new byte[100]));
                assertEquals(2 * MIB - 64, slab.chunkBytes());
            }

            @Test
            @DisplayName("a burst that fills one window does not turn it hot")
            void burst() {
                var slab = slab(2 * MIB);
                window(slab, 8 * MIB);
                window(slab, 8 * MIB);
                window(slab, MIB);
                window(slab, 8 * MIB);
                window(slab, MIB);
                assertFalse(slab.hot());
            }

            @Test
            @DisplayName("turns cold after eight slow windows in a row, and lets go of its chunks; a stall between fast windows does not")
            void cools() {
                var slab = slab(2 * MIB);
                warm(slab);
                assertTrue(place(slab, new byte[100]));
                for (var i = 0; i < 7; i++) {
                    window(slab, 1L << 18);
                }
                assertTrue(slab.hot());
                window(slab, 8 * MIB);
                for (var i = 0; i < 7; i++) {
                    window(slab, 1L << 18);
                }
                assertTrue(slab.hot());
                window(slab, 1L << 18);
                assertFalse(slab.hot());
                assertEquals(0, slab.heldForTests());
                warm(slab);
                // A checkpoint holds the commits back for seconds: one long window
                now.addAndGet(5_000_000_000L);
                slab.note(1L << 18);
                window(slab, 8 * MIB);
                assertTrue(slab.hot());
            }

            @Test
            @DisplayName("a rate between a quarter and all of the fast one keeps it as it is")
            void between() {
                var slab = slab(2 * MIB);
                warm(slab);
                for (var i = 0; i < 20; i++) {
                    window(slab, 2 * MIB);
                }
                assertTrue(slab.hot());
            }

            @ParameterizedTest(name = "region {0}")
            @CsvSource({"0", "64", "68157440"})
            @DisplayName("turns off for good under another collector, or when a chunk takes more than the budget")
            void off(long region) {
                var slab = slab(region);
                window(slab, 8 * MIB);
                assertTrue(slab.off());
                window(slab, 8 * MIB);
                window(slab, 8 * MIB);
                window(slab, 8 * MIB);
                assertFalse(slab.hot());
                assertEquals(1, asked.get());
            }

            @ParameterizedTest(name = "{0}")
            @ValueSource(classes = {NoClassDefFoundError.class, OutOfMemoryError.class, IllegalStateException.class})
            @DisplayName("turns off for good when asking for the region fails, without failing the commit that asked")
            void failedQuestion(Class<? extends Throwable> failure) throws Exception {
                var thrown = failure.getDeclaredConstructor().newInstance();
                var slab = new ValueSlab(now::get, () -> {
                    asked.incrementAndGet();
                    if (thrown instanceof Error error) {
                        throw error;
                    }
                    throw (RuntimeException) thrown;
                }, new ValueSlab.Budget(32 * MIB));
                for (var i = 0; i < 5; i++) {
                    window(slab, 8 * MIB);
                }
                assertTrue(slab.off());
                assertFalse(slab.hot());
                assertEquals(0, slab.capacity());
                assertEquals(1, asked.get());
            }

            @Test
            @DisplayName("a tick of a store still filling chunks lets go of one that took nothing since the last tick, as a thread that stopped leaves it")
            void unused() {
                var slab = slab(2 * MIB);
                warm(slab);
                assertTrue(place(slab, new byte[100]));
                slab.tick(false);
                assertTrue(place(slab, new byte[100]));
                slab.tick(false);
                assertEquals(1, slab.heldForTests());
                slab.tick(false);
                assertEquals(0, slab.heldForTests());
                assertTrue(slab.hot());
                assertEquals(2 * MIB - 64 - 200, slab.slackBytes());
                assertTrue(place(slab, new byte[100]));
                assertEquals(1, slab.heldForTests());
            }

            @Test
            @DisplayName("a tick cools a store that is idle or went silent, not one still filling chunks")
            void ticks() {
                var slab = slab(2 * MIB);
                warm(slab);
                assertTrue(place(slab, new byte[100]));
                slab.tick(false);
                assertTrue(slab.hot());
                assertEquals(1, slab.heldForTests());
                slab.tick(true);
                assertFalse(slab.hot());
                assertEquals(0, slab.heldForTests());
                warm(slab);
                assertTrue(place(slab, new byte[100]));
                now.addAndGet(15_000_000_000L);
                slab.tick(false);
                assertFalse(slab.hot());
                assertEquals(0, slab.heldForTests());
                assertFalse(place(slab, new byte[100]));
            }

            @Test
            @DisplayName("a chunk that fills is replaced by another, and its tail counted")
            void replaces() {
                var slab = slab(2 * MIB);
                warm(slab);
                var large = new byte[(int) (MIB + 1)];
                assertTrue(place(slab, large));
                assertTrue(place(slab, large));
                assertEquals(1, slab.heldForTests());
                assertEquals(2 * (2 * MIB - 64), slab.chunkBytes());
                assertEquals(2 * MIB - 64 - large.length + 2 * MIB - 64, slab.slackBytes());
            }
        }
    }

    @Nested
    @DisplayName("overwrites")
    class OverwriteShare {
        private static Version version(long stamp) {
            return Version.ofOwn(stamp, new byte[1], null, null, null);
        }

        // An interval of `samples` puts of `length` bytes, the first `overwritten` of which replace a version in no tree
        private static void interval(ValueSlab.Overwrites overwrites, int samples, int overwritten, int length, long bound) {
            for (var i = 0; i < samples; i++) {
                overwrites.sample(i < overwritten ? version(bound + 1) : null, length);
            }
        }

        @ParameterizedTest(name = "{1} of 1000 values of {0} bytes overwritten: {2}")
        @CsvSource({"128,430,true", "128,600,true", "128,740,false", "256,450,true", "256,550,false", "1024,150,true", "1024,430,false"})
        @DisplayName("a key space's share is judged against the size of its values, at the cut")
        void share(int length, int overwritten, boolean worthwhile) {
            var overwrites = new ValueSlab.Overwrites();
            assertTrue(overwrites.worthwhile());
            // Overwrites last, so the share so far passes no limit before the cut
            for (var i = 0; i < 1_000; i++) {
                overwrites.sample(i >= 1_000 - overwritten ? version(1) : null, length);
            }
            overwrites.cut(0);
            assertEquals(worthwhile, overwrites.worthwhile());
        }

        @Test
        @DisplayName("a share too high stops the slab before the interval ends")
        void early() {
            var overwrites = new ValueSlab.Overwrites();
            for (var i = 0; i < 63; i++) {
                overwrites.sample(version(1), 1_024);
            }
            assertTrue(overwrites.worthwhile());
            overwrites.sample(version(1), 1_024);
            assertFalse(overwrites.worthwhile());
        }

        @Test
        @DisplayName("a burst of overwrites decays over the next intervals")
        void decays() {
            var overwrites = new ValueSlab.Overwrites();
            interval(overwrites, 1_000, 1_000, 128, 0);
            overwrites.cut(1_000);
            assertFalse(overwrites.worthwhile());
            interval(overwrites, 1_000, 0, 128, 1_000);
            overwrites.cut(2_000);
            assertTrue(overwrites.worthwhile());
        }

        @Test
        @DisplayName("a version a checkpoint took, and a removal, count as no overwrite; an interval of few samples does not count")
        void counts() {
            var overwrites = new ValueSlab.Overwrites();
            overwrites.cut(100);
            for (var i = 0; i < 1_000; i++) {
                overwrites.sample(version(100), 1_024);
                overwrites.sample(new Version(200, (MemorySegment) null, null, null), 1_024);
            }
            overwrites.cut(200);
            assertTrue(overwrites.worthwhile());
            interval(overwrites, 10, 10, 1_024, 200);
            overwrites.cut(300);
            assertTrue(overwrites.worthwhile());
        }

        @Test
        @DisplayName("a removal of a version in no tree counts as an overwrite; one of a version a checkpoint took, or of a removal, does not")
        void removals() {
            var dying = new ValueSlab.Overwrites();
            dying.cut(100);
            for (var i = 0; i < 64; i++) {
                dying.sample(null, 4_096);
                dying.sampleRemoval(version(101));
            }
            assertFalse(dying.worthwhile());
            var kept = new ValueSlab.Overwrites();
            kept.cut(100);
            for (var i = 0; i < 64; i++) {
                kept.sample(null, 4_096);
                kept.sampleRemoval(version(100));
                kept.sampleRemoval(new Version(200, (MemorySegment) null, null, null));
                kept.sampleRemoval(null);
            }
            assertTrue(kept.worthwhile());
            kept.cut(200);
            assertTrue(kept.worthwhile());
        }

        @ParameterizedTest(name = "every {0}th commit from {1}")
        @CsvSource({"1,0", "2,0", "2,1", "3,1", "4,3", "64,0", "64,32"})
        @DisplayName("the sample does not follow a pattern of the commits")
        void sampling(int stride, int start) {
            var samples = 0;
            for (var i = 0; i < 1 << 16; i++) {
                if (ValueSlab.Overwrites.sampled(start + (long) stride * i)) {
                    samples++;
                }
            }
            assertTrue(samples > 512 && samples < 2_048, samples + " samples");
        }

        @Test
        @Timeout(60)
        @DisplayName("appliers that race on the counters only blur the estimate")
        void racing() throws Exception {
            var overwrites = new ValueSlab.Overwrites();
            var threads = new ArrayList<Thread>();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            for (var t = 0; t < 8; t++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var i = 0; i < 200_000; i++) {
                            overwrites.sample(version(1L << 40), 128);
                            if (i % 10_000 == 0) {
                                overwrites.cut(0);
                            }
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
            }
            for (var thread : threads) {
                thread.join();
            }
            assertEquals(List.of(), List.copyOf(failures));
            assertFalse(overwrites.worthwhile());
        }
    }
}
