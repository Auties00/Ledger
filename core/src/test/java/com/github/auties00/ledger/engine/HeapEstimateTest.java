package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The log's estimate of the heap the writes the trees lack hold: a chain counts once for each interval between cuts
/// a commit lists it in, a write its version, through pipelined commits, failed and racing checkpoints and a replay.
/// Every store here checkpoints only when a test asks, on a cold slab, so each commit's estimate is the same for the
/// same frame.
class HeapEstimateTest {
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> CHATS = Keyspace.resident("chats", KeyCodec.bytes(), ValueCodec.bytes());
    private static final byte[] VALUE = new byte[100];

    private static LiveLedger open(Path directory) throws Exception {
        var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(MESSAGES).keyspace(CHATS).checkpointEvery(1L << 40).build();
        IdleTicker.unwatch(ledger.checkpointer());
        ledger.valueSlab().forceColdForTests();
        // The first commit takes the frame the open deferred to its group
        ledger.commit(ledger.newBatch().put(CHATS, new byte[]{-1}, VALUE)).awaitDurable();
        return ledger;
    }

    private static Keyspace<byte[], byte[]> keyspace(boolean resident) {
        return resident ? CHATS : MESSAGES;
    }

    private static byte[] key(int index) {
        return ByteBuffer.allocate(8).putInt(0x6b6b6b6b).putInt(index).array();
    }

    /// {@return what committing `keys` of `keyspace`, one put each, added to the log's estimate}
    private static long put(LiveLedger ledger, Keyspace<byte[], byte[]> keyspace, int... keys) throws Exception {
        var before = ledger.log().appendedHeap();
        var batch = ledger.newBatch();
        for (var key : keys) {
            batch.put(keyspace, key(key), VALUE);
        }
        ledger.commit(batch).awaitDurable();
        return ledger.log().appendedHeap() - before;
    }

    private static int dirty(LiveLedger ledger) {
        return ledger.state(MESSAGES).dirtyCountForTests() + ledger.state(CHATS).dirtyCountForTests();
    }

    @Nested
    @DisplayName("chains")
    class Chains {
        @ParameterizedTest(name = "resident {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("an overwrite of a key written since the cut counts its version, not its chain again")
        void overwrite(boolean resident, @TempDir Path directory) throws Exception {
            try (var ledger = open(directory)) {
                var keyspace = keyspace(resident);
                var fresh = put(ledger, keyspace, 1);
                var overwrite = put(ledger, keyspace, 1);
                assertEquals(LogWriter.CHAIN_HEAP, fresh - overwrite);
                assertEquals(overwrite, put(ledger, keyspace, 1));
                assertEquals(fresh, put(ledger, keyspace, 2));
            }
        }

        @Test
        @DisplayName("a key a commit writes twice counts its chain once, and keys of two key spaces count one each")
        void repeatsInCommit(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory)) {
                var distinct = put(ledger, MESSAGES, 1, 2);
                var repeated = put(ledger, MESSAGES, 3, 3);
                assertEquals(LogWriter.CHAIN_HEAP, distinct - repeated);
                var before = ledger.log().appendedHeap();
                ledger.commit(ledger.newBatch().put(MESSAGES, key(4), VALUE).put(CHATS, key(4), VALUE)).awaitDurable();
                assertEquals(distinct, ledger.log().appendedHeap() - before);
            }
        }

        @ParameterizedTest(name = "resident {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a key written again after a cut counts its chain again, whether a snapshot kept the chain or not")
        void afterCut(boolean resident, @TempDir Path directory) throws Exception {
            try (var ledger = open(directory)) {
                var keyspace = keyspace(resident);
                var fresh = put(ledger, keyspace, 1);
                put(ledger, keyspace, 2);
                try (var _ = ledger.snapshot()) {
                    ledger.checkpoint();
                    assertEquals(fresh, put(ledger, keyspace, 1));
                }
                ledger.checkpoint();
                assertEquals(fresh, put(ledger, keyspace, 2));
                assertEquals(fresh - LogWriter.CHAIN_HEAP, put(ledger, keyspace, 2));
            }
        }

        @ParameterizedTest(name = "same key {0}")
        @ValueSource(booleans = {false, true})
        @Timeout(60)
        @DisplayName("commits that publish in one pass count each chain they list once, one key for them all or a key each")
        void pipelined(boolean sameKey, @TempDir Path directory) throws Exception {
            try (var ledger = open(directory)) {
                put(ledger, MESSAGES, 0);
                var overwrite = put(ledger, MESSAGES, 0);
                ledger.checkpoint();
                var threads = 8;
                var commits = 500;
                var before = ledger.log().appendedHeap();
                var start = new CountDownLatch(1);
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var workers = new ArrayList<Thread>();
                for (var t = 0; t < threads; t++) {
                    var thread = t;
                    workers.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var i = 0; i < commits; i++) {
                                var key = sameKey ? 0 : 1 + thread * commits + i;
                                ledger.commit(ledger.newBatch().put(MESSAGES, key(key), VALUE));
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                start.countDown();
                for (var worker : workers) {
                    worker.join();
                }
                ledger.awaitDurable();
                assertTrue(failures.isEmpty(), () -> "failed: " + failures.peek());
                var writes = (long) threads * commits;
                var chains = sameKey ? 1 : writes;
                assertEquals(writes * overwrite + chains * LogWriter.CHAIN_HEAP, ledger.log().appendedHeap() - before);
                assertEquals(chains, dirty(ledger));
            }
        }
    }

    @Nested
    @DisplayName("checkpoints")
    class Checkpoints {
        @Test
        @Timeout(120)
        @DisplayName("checkpoints cut beside racing commits leave every chain listed since the last one counted in the backlog")
        void racingCuts(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory)) {
                var checkpointer = ledger.checkpointer();
                var running = new AtomicBoolean(true);
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var workers = new ArrayList<Thread>();
                for (var t = 0; t < 6; t++) {
                    var seed = t;
                    workers.add(Thread.ofPlatform().start(() -> {
                        var random = new Random(seed);
                        try {
                            while (running.get()) {
                                var batch = ledger.newBatch();
                                for (var i = 0; i < 1 + random.nextInt(8); i++) {
                                    batch.put(random.nextBoolean() ? MESSAGES : CHATS, key(random.nextInt(2_000)), VALUE);
                                }
                                ledger.commit(batch);
                            }
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    }));
                }
                for (var round = 0; round < 40; round++) {
                    Thread.sleep(10);
                    ledger.checkpoint();
                }
                running.set(false);
                for (var worker : workers) {
                    worker.join();
                }
                ledger.awaitDurable();
                assertTrue(failures.isEmpty(), () -> "failed: " + failures.peek());
                var listed = (long) dirty(ledger) * LogWriter.CHAIN_HEAP;
                var backlog = checkpointer.backlogHeap();
                assertTrue(backlog >= listed, () -> backlog + " bytes of backlog for " + listed + " of listed chains");
                ledger.checkpoint();
                assertEquals(0, checkpointer.backlogHeap());
                assertEquals(0, dirty(ledger));
            }
        }

        @Test
        @DisplayName("a checkpoint that fails keeps its chains counted, the overwrites after it count their versions, and the next one takes them all")
        void failedCheckpoint() throws Exception {
            var disk = CrashFileSystem.create();
            var failing = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                        && path.getFileName().toString().equals(PageFile.FILE)) {
                    throw new IOException("No space left on device");
                }
            });
            try (var ledger = open(disk.getPath("/store"))) {
                var checkpointer = ledger.checkpointer();
                ledger.checkpoint();
                var keys = new int[100];
                for (var i = 0; i < keys.length; i++) {
                    keys[i] = i;
                }
                var fresh = put(ledger, MESSAGES, keys);
                var backlog = checkpointer.backlogHeap();
                failing.set(true);
                assertThrows(LedgerException.class, ledger::checkpoint);
                assertEquals(backlog, checkpointer.backlogHeap());
                assertEquals(keys.length, dirty(ledger));
                // The failed cut gave its chains back to the interval it counted them in
                assertEquals(fresh - (long) keys.length * LogWriter.CHAIN_HEAP, put(ledger, MESSAGES, keys));
                failing.set(false);
                ledger.checkpoint();
                assertEquals(0, checkpointer.backlogHeap());
                assertEquals(fresh, put(ledger, MESSAGES, keys));
            }
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        @ParameterizedTest(name = "resident {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a replay counts every write's chain, no less than the estimate before the crash, and an overwrite after it counts its version")
        void replay(boolean resident, @TempDir Path directory) throws Exception {
            var keyspace = keyspace(resident);
            long estimate;
            long overwrite;
            var writes = 0L;
            try (var ledger = open(directory)) {
                ledger.checkpoint();
                put(ledger, keyspace, 1);
                overwrite = put(ledger, keyspace, 1);
                writes += 2;
                for (var round = 0; round < 20; round++) {
                    var keys = new int[10];
                    for (var i = 0; i < keys.length; i++) {
                        keys[i] = i;
                    }
                    put(ledger, keyspace, keys);
                    writes += keys.length;
                }
                estimate = ledger.checkpointer().backlogHeap();
                ledger.closeWithoutCheckpoint();
            }
            try (var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                    .keyspace(MESSAGES).keyspace(CHATS).checkpointEvery(1L << 40).build()) {
                IdleTicker.unwatch(ledger.checkpointer());
                ledger.valueSlab().forceColdForTests();
                var replayed = ledger.replayedHeap();
                var observed = writes;
                assertTrue(replayed >= observed * LogWriter.WRITE_HEAP, () -> replayed + " bytes for " + observed + " writes");
                assertTrue(replayed >= estimate, () -> replayed + " bytes replayed, " + estimate + " estimated before the crash");
                assertTrue(ledger.checkpointer().backlogHeap() >= estimate);
                // The first commit after the open takes the frame the open deferred to its group
                ledger.commit(ledger.newBatch().put(CHATS, new byte[]{-1}, VALUE)).awaitDurable();
                assertEquals(overwrite, put(ledger, keyspace, 1));
                ledger.checkpoint();
                assertEquals(overwrite + LogWriter.CHAIN_HEAP, put(ledger, keyspace, 1));
            }
        }
    }

    @Nested
    @DisplayName("calibration")
    class Calibration {
        private static long heapUsed() throws InterruptedException {
            var memory = ManagementFactory.getMemoryMXBean();
            var used = Long.MAX_VALUE;
            for (var i = 0; i < 6; i++) {
                System.gc();
                Thread.sleep(50);
                used = Math.min(used, memory.getHeapMemoryUsage().getUsed());
            }
            return used;
        }

        @ParameterizedTest(name = "pinned {0}")
        @ValueSource(booleans = {false, true})
        @Timeout(120)
        @DisplayName("batches of a hundred new keys of a paged key space count a chain a write, and the estimate of overwrites after them is no less than the heap they hold, a snapshot holding their older versions or not")
        void overwrites(boolean pinned, @TempDir Path directory) throws Exception {
            var keys = 20_000;
            var batch = 100;
            var random = new Random(7);
            try (var ledger = open(directory)) {
                var checkpointer = ledger.checkpointer();
                ledger.checkpoint();
                var log = ledger.log();
                var bytesBefore = log.appendedBytes();
                var logHeapBefore = log.appendedHeap();
                for (var i = 0; i < keys; i += batch) {
                    var writes = ledger.newBatch();
                    for (var j = i; j < i + batch; j++) {
                        var value = new byte[VALUE.length];
                        random.nextBytes(value);
                        writes.put(MESSAGES, key(j), value);
                    }
                    ledger.commit(writes);
                }
                ledger.awaitDurable();
                // Each write of a new key lists its chain, and each commit on the cold slab has a view of its own
                var freshOverhead = (log.appendedHeap() - logHeapBefore) - (log.appendedBytes() - bytesBefore);
                assertEquals((long) keys * LogWriter.WRITE_HEAP + (long) (keys / batch) * LogWriter.COMMIT_HEAP, freshOverhead);
                var heapFresh = heapUsed();
                var estimateFresh = checkpointer.backlogHeap();
                var snapshot = pinned ? ledger.snapshot() : null;
                try {
                    for (var round = 0; round < 4; round++) {
                        for (var i = 0; i < keys; i += batch) {
                            var writes = ledger.newBatch();
                            for (var j = 0; j < batch; j++) {
                                var value = new byte[VALUE.length];
                                random.nextBytes(value);
                                writes.put(MESSAGES, key(random.nextInt(keys)), value);
                            }
                            ledger.commit(writes);
                        }
                    }
                    ledger.awaitDurable();
                    var held = heapUsed() - heapFresh;
                    var estimated = checkpointer.backlogHeap() - estimateFresh;
                    assertTrue(estimated >= held, () -> "overwrites estimated at " + estimated + " bytes held " + held);
                } finally {
                    if (snapshot != null) {
                        snapshot.close();
                    }
                }
            }
        }
    }
}
