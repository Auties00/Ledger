package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Bulk commits appending to one run while the spill moves earlier commits' values to the log from two drainers beside
/// its own thread, with pipelined commits, checkpoints and segment retirements running: snapshots held across all of
/// it read exactly the values of their commits, by lookup, encoded read and scan, and so does the store after a crash
/// and a reopen.
class BulkCommitSpillReadsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> TEXTS = Keyspace.paged("texts", KeyCodec.bytes(), ValueCodec.bytes());
    private static final int BULK = LiveLedger.BULK_RUN_WRITES + 300;
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 200, 300, 700, 1_500, 3_000};

    private static LiveLedger open(Path directory, boolean encrypted) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).commitWindow(Duration.ZERO)
                .largeValues(MESSAGES, 64 << 10))
                .segmentSize(1L << 20)
                .spillBudget(0)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static byte[] value(int message) {
        var length = LENGTHS[Math.floorMod(message * 7 + message / BULK, LENGTHS.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 31 + i * 13 + 5);
        }
        return value;
    }

    // Checks that `view` holds exactly the messages of its first n bulk commits, n read from its count
    private static int check(View view, int stride, int offset) {
        var size = view.size(MESSAGES);
        assertEquals(0, size % BULK, "a count that is not whole bulk commits: " + size);
        var commits = (int) (size / BULK);
        for (var message = offset % stride; message < size; message += stride) {
            var m = message;
            assertArrayEquals(value(message), view.get(MESSAGES, key(message)).orElseThrow(() -> new AssertionError("missing " + m)),
                    "message " + message);
            assertArrayEquals(value(message), view.readEncoded(MESSAGES, key(message),
                    segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow(), "encoded message " + message);
        }
        assertFalse(view.get(MESSAGES, key((int) size)).isPresent(), "a message past the count");
        var next = new int[1];
        view.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            var message = ByteBuffer.wrap(key).getInt();
            assertEquals(next[0], message, "scan order");
            if (message % 13 == offset % 13) {
                assertArrayEquals(value(message), value, "scanned message " + message);
            }
            next[0]++;
            return true;
        });
        assertEquals(size, next[0], "scanned");
        return commits;
    }

    // Checks the messages below a count of the store while commits run: its count only grows, so every message below
    // one it read is there, though a later commit may have added more since
    private static void checkLive(Ledger ledger, int stride, int offset) {
        var size = ledger.size(MESSAGES);
        for (var message = offset % stride; message < size; message += stride) {
            var m = message;
            assertArrayEquals(value(message), ledger.get(MESSAGES, key(message)).orElseThrow(() -> new AssertionError("missing " + m)),
                    "message " + message);
        }
    }

    @ParameterizedTest(name = "encrypted {0}, checkpoints {1}")
    @CsvSource({"true,false", "true,true", "false,false", "false,true"})
    @DisplayName("snapshots held across appends, two drainers spilling the run, checkpoints and retirements read every value exactly, through a crash and a reopen")
    void appendsUnderSpill(boolean encrypted, boolean checkpoints, @TempDir Path directory) throws Exception {
        var committed = 0;
        var maxAppendedSize = new AtomicInteger();
        try (var ledger = open(directory, encrypted)) {
            ledger.state(MESSAGES).overwrites.forceSpillsForTests();
            var stop = new AtomicBoolean();
            var failure = new AtomicReference<Throwable>();
            var threads = new ArrayList<Thread>();
            var checks = new AtomicLong();
            // Pipelined commits beside the bulk ones
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(1);
                try {
                    var i = 0;
                    while (!stop.get()) {
                        var batch = ledger.newBatch();
                        for (var w = 0; w < 4; w++) {
                            var v = new byte[100 + random.nextInt(400)];
                            random.nextBytes(v);
                            batch.put(TEXTS, key(i++ % 5000), v);
                        }
                        ledger.commit(batch);
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            }));
            // Two drainers beside the queue's own thread
            for (var d = 0; d < 2; d++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get()) {
                            ledger.spills().drain(ledger.log()::durableSequence);
                            Thread.onSpinWait();
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            // Readers holding snapshots across later appends, spills and checkpoints
            for (var r = 0; r < 2; r++) {
                var seed = r;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    try {
                        while (!stop.get()) {
                            try (var snapshot = ledger.snapshot()) {
                                var first = check(snapshot, 37, random.nextInt(37));
                                for (var again = 0; again < 3 && !stop.get(); again++) {
                                    Thread.sleep(3);
                                    assertEquals(first, check(snapshot, 41, random.nextInt(41)), "a snapshot's commits changed");
                                }
                            }
                            checkLive(ledger, 53, random.nextInt(53));
                            checks.incrementAndGet();
                        }
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            var checkpointer = Thread.ofPlatform().start(() -> {
                try {
                    while (checkpoints && !stop.get()) {
                        Thread.sleep(15);
                        ledger.checkpoint();
                    }
                } catch (Throwable throwable) {
                    failure.compareAndSet(null, throwable);
                }
            });
            for (var bulk = 0; bulk < 14 && failure.get() == null; bulk++) {
                var batch = ledger.newBatch();
                for (var message = bulk * BULK; message < (bulk + 1) * BULK; message++) {
                    batch.put(MESSAGES, key(message), value(message));
                }
                ledger.commit(batch).awaitDurable();
                committed++;
                var run = ledger.state(MESSAGES).run;
                if (run != null && run.committed()) {
                    maxAppendedSize.accumulateAndGet(run.size(), Math::max);
                }
                Thread.sleep(10);
            }
            Thread.sleep(50);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            checkpointer.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertTrue(checks.get() > 0, "no reader check ran");
            if (!checkpoints) {
                assertTrue(maxAppendedSize.get() > BULK, "no bulk commit appended: " + maxAppendedSize.get());
            }
            assertEquals(committed, check(ledger, 1, 0));
            ledger.closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted)) {
            assertEquals(committed, check(ledger, 1, 0));
            ledger.checkpoint();
            assertEquals(committed, check(ledger, 1, 0));
        }
        try (var ledger = open(directory, encrypted)) {
            assertEquals(committed, check(ledger, 1, 0));
        }
    }
}
