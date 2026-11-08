package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A checkpoint started on the store's own thread that holds its forces back while the log writes: when it forces,
/// what ends the hold, and what a crash during it leaves. The log is kept busy by holding its forces in the file
/// system's interceptor, so whether it has work does not depend on how fast the test's threads run.
class ForceDeferralTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final String VALUE = "v".repeat(64 << 10);
    private static final int EVERY = 1 << 20;
    private static final Duration LONG_HOLD = Duration.ofSeconds(60);
    // A quarter of an interval of log, half of what ends a hold whatever the last forces took
    private static final int FEW_VALUES = 4;

    /// Holds every force of a log segment while closed, holds the writes of the data file while told to, delays its
    /// writes and forces as told, and records when the data file is first forced once armed.
    private static final class LogGate implements ChangeInterceptor {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicLong firstDataForce = new AtomicLong();
        private final AtomicLong dataWriteDelay = new AtomicLong();
        private final AtomicBoolean dataWritesClosed = new AtomicBoolean();
        private final AtomicBoolean dataWriteHeld = new AtomicBoolean();
        private final AtomicLong dataForceDelay = new AtomicLong();

        @Override
        public void beforeChange(Path path, ChangeKind kind) throws IOException {
            var name = path.getFileName() == null ? "" : path.getFileName().toString();
            if (kind == ChangeKind.WRITE && name.equals(PageFile.FILE) && dataWriteDelay.get() > 0) {
                sleep(dataWriteDelay.get());
            }
            if (kind == ChangeKind.WRITE && name.equals(PageFile.FILE)) {
                while (dataWritesClosed.get()) {
                    dataWriteHeld.set(true);
                    sleep(1);
                }
            }
            if (kind != ChangeKind.FORCE) {
                return;
            }
            if (name.endsWith(".log")) {
                while (closed.get()) {
                    sleep(1);
                }
            } else if (name.equals(PageFile.FILE)) {
                if (armed.get()) {
                    firstDataForce.compareAndSet(0, System.nanoTime());
                }
                if (dataForceDelay.get() > 0) {
                    sleep(dataForceDelay.get());
                }
            }
        }

        private static void sleep(long millis) throws IOException {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in the interceptor", exception);
            }
        }
    }

    private static LiveLedger open(CrashFileSystem disk, Duration deferral) {
        return open(disk, deferral, Long.MAX_VALUE);
    }

    private static LiveLedger open(CrashFileSystem disk, Duration deferral, long heapBudget) {
        var options = (LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).checkpointEvery(EVERY);
        var ledger = (LiveLedger) options.forceDeferral(deferral).checkpointHeap(heapBudget).build();
        // Ticked by no one: an idle tick between two steps would start a checkpoint of its own
        IdleTicker.unwatch(ledger.checkpointer());
        return ledger;
    }

    private static Ledger reopen(CrashFileSystem disk) {
        return Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(MESSAGES).build();
    }

    /// Commits `count` values of 64 KiB under `m<from>` onwards, without waiting for them to be durable.
    private static void commit(Ledger ledger, int from, int count) {
        for (var i = from; i < from + count; i++) {
            ledger.commit(ledger.newBatch().put(MESSAGES, key(i), VALUE));
        }
    }

    /// Commits values of 64 KiB under `m<from>` onwards, without waiting for them to be durable, until the log the
    /// trees lack is an interval, so the last one starts a checkpoint and none follows its cut.
    ///
    /// @return the index after the last one committed
    private static int fill(LiveLedger ledger, int from) {
        var next = from;
        while (ledger.checkpointer().backlog() < EVERY) {
            ledger.commit(ledger.newBatch().put(MESSAGES, key(next++), VALUE));
        }
        return next;
    }

    private static String key(int index) {
        return "m%05d".formatted(index);
    }

    private static void await(BooleanSupplier condition, String failure) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, failure);
            Thread.sleep(5);
        }
    }

    @Test
    @DisplayName("a checkpoint on its own thread forces its pages only once the log has nothing left to write")
    void forcesInTheLull() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            gate.armed.set(true);
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            Thread.sleep(200);
            assertTrue(checkpointer.deferringForTests(), "the checkpoint stopped holding its forces back while the log wrote");
            assertEquals(0, gate.firstDataForce.get(), "the data file was forced while the log wrote");
            assertTrue(ledger.tree() == null, "the checkpoint was published before its forces");
            var opened = System.nanoTime();
            gate.closed.set(false);
            await(() -> ledger.tree() != null, "the checkpoint did not end once the log went quiet");
            assertTrue(gate.firstDataForce.get() > opened, "the data file was forced before the log went quiet");
            assertFalse(checkpointer.deferringForTests());
            ledger.awaitDurable();
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
            assertEquals(Optional.of(VALUE), reopened.get(MESSAGES, key(committed - 1)));
        }
        CheckpointTest.verifyPages(disk.getPath("/store"), true);
    }

    @Test
    @DisplayName("a checkpoint on the caller's thread ends a held checkpoint at once and forces its own while the log writes")
    void explicitCheckpoint() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            // Commits the held checkpoint does not hold, which the caller's one takes
            commit(ledger, committed, 4);
            committed += 4;
            var start = System.nanoTime();
            ledger.checkpoint();
            var took = System.nanoTime() - start;
            assertTrue(took < TimeUnit.SECONDS.toNanos(20), () -> "the checkpoint took " + took / 1_000_000 + " ms");
            assertFalse(checkpointer.deferringForTests());
            assertTrue(ledger.tree() != null);
            // The log still writes nothing: the caller's checkpoint never held its forces, and took every commit
            assertTrue(gate.closed.get());
            assertEquals(0, checkpointer.backlog());
            assertEquals(committed, ledger.size(MESSAGES));
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
        CheckpointTest.verifyPages(disk.getPath("/store"), true);
    }

    @ParameterizedTest(name = "final checkpoint {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a close ends a held checkpoint before it waits for the log, and the store loses nothing")
    void close(boolean checkpoint) throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        Thread closer = null;
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            var failure = new AtomicReference<Throwable>();
            closer = Thread.ofPlatform().start(() -> {
                try {
                    if (checkpoint) {
                        ledger.close();
                    } else {
                        ledger.closeWithoutCheckpoint();
                    }
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            // The close waits for the log, which writes nothing yet: only the close itself can end the hold
            await(() -> !checkpointer.deferringForTests(), "a close did not end the held checkpoint");
            await(() -> ledger.tree() != null, "the held checkpoint did not end once the close began");
            assertTrue(gate.closed.get());
            gate.closed.set(false);
            closer.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(closer.isAlive(), "the close did not end");
            assertTrue(failure.get() == null, () -> "the close failed: " + failure.get());
        } finally {
            gate.closed.set(false);
            if (closer == null) {
                ledger.close();
            } else {
                closer.join();
            }
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
            assertEquals(Optional.of(VALUE), reopened.get(MESSAGES, key(committed - 1)));
        }
        CheckpointTest.verifyPages(disk.getPath("/store"), true);
    }

    @Test
    @DisplayName("a checkpoint forces once it has held its forces back for the longest it may, even while the log writes")
    void bounded() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var hold = Duration.ofMillis(300);
        var ledger = open(disk, hold);
        int committed;
        try {
            gate.closed.set(true);
            committed = fill(ledger, 0);
            var started = System.nanoTime();
            await(() -> ledger.tree() != null, "the checkpoint held its forces back past the longest it may");
            var took = System.nanoTime() - started;
            assertTrue(took >= hold.toNanos(), () -> "the checkpoint ended " + took / 1_000_000 + " ms after its start, holding nothing back");
            assertTrue(gate.closed.get());
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("a checkpoint forces at once, without holding back, once half an interval of log waits behind its cut")
    void halfInterval() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            Thread.sleep(100);
            assertTrue(ledger.tree() == null, "the checkpoint ended while the log wrote");
            // Half an interval behind the cut, with the log still writing
            commit(ledger, committed, EVERY / 2 / VALUE.length() + 1);
            committed += EVERY / 2 / VALUE.length() + 1;
            await(() -> ledger.tree() != null, "the checkpoint went on holding its forces back past half an interval");
            assertTrue(gate.closed.get());
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("a held checkpoint forces once a few commits behind its cut, grown at their rate for as long as the last checkpoint's forces took, would pass half an interval")
    void projectedFromLastForces() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            ledger.commit(ledger.newBatch().put(MESSAGES, key(0), VALUE)).awaitDurable();
            gate.dataForceDelay.set(3_000);
            ledger.checkpoint();
            gate.dataForceDelay.set(0);
            gate.closed.set(true);
            committed = fill(ledger, 1);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            commit(ledger, committed, FEW_VALUES);
            committed += FEW_VALUES;
            await(() -> !checkpointer.deferringForTests(), "the checkpoint went on holding its forces back past the log its last forces projected");
            await(() -> checkpointer.backlog() < EVERY, "the held checkpoint was not published");
            assertTrue(gate.closed.get());
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("the same few commits behind the cut do not end a hold before any checkpoint of this open has forced")
    void noLastForcesToProject() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            commit(ledger, committed, FEW_VALUES);
            committed += FEW_VALUES;
            Thread.sleep(200);
            assertTrue(checkpointer.deferringForTests(), "the checkpoint stopped holding its forces back below half an interval");
            assertTrue(ledger.tree() == null, "the checkpoint ended while the log wrote");
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
    }

    @Test
    @DisplayName("a commit that reaches the backlog bound ends a held checkpoint and waits only for its forces, while the log still writes")
    void throttledCommitEndsHold() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        Thread explicit = null;
        Thread committer = null;
        var committed = 1;
        try {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            gate.dataWritesClosed.set(true);
            commit(ledger, 0, 1);
            // Cuts one value and stays in its writes while the backlog grows, so the checkpoint started on its own
            // thread queues behind it and cuts late, with the log it takes just short of the bound still unpublished
            explicit = Thread.ofPlatform().start(ledger::checkpoint);
            await(gate.dataWriteHeld::get, "the checkpoint on the caller's thread did not reach its writes");
            while (checkpointer.backlog() < 5.7 * EVERY) {
                commit(ledger, committed++, 1);
            }
            gate.dataWritesClosed.set(false);
            explicit.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(explicit.isAlive(), "the checkpoint on the caller's thread did not end");
            await(checkpointer::deferringForTests, "the checkpoint that cut late did not hold its forces back");
            var from = committed;
            var failure = new AtomicReference<Throwable>();
            committer = Thread.ofPlatform().start(() -> {
                try {
                    // Half an interval, past the bound from anywhere the loop above stopped
                    commit(ledger, from, 8);
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            committed += 8;
            // Well short of the minute of LONG_HOLD; the log writes nothing, so only the waiting commit can end the hold
            committer.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(committer.isAlive(), "the commits waited out the hold");
            assertTrue(failure.get() == null, () -> "a commit failed: " + failure.get());
            assertFalse(checkpointer.deferringForTests());
            assertTrue(checkpointer.backlog() < EVERY, () -> checkpointer.backlog() + " bytes of log the trees lack after the hold ended");
            assertTrue(gate.closed.get());
        } finally {
            gate.closed.set(false);
            gate.dataWritesClosed.set(false);
            if (explicit != null) {
                explicit.join();
            }
            if (committer != null) {
                committer.join();
            }
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
        CheckpointTest.verifyPages(disk.getPath("/store"), true);
    }

    @ParameterizedTest(name = "heap budget {0} bytes")
    @ValueSource(longs = {Long.MAX_VALUE, 1})
    @DisplayName("the log the trees lack stays within the backlog bounds under commits that outrun the checkpoints")
    void backlogBounds(long heapBudget) throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        gate.dataWriteDelay.set(1);
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD, heapBudget);
        try {
            var checkpointer = ledger.checkpointer();
            var largest = 0L;
            for (var i = 0; i < 200; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(i), VALUE));
                largest = Math.max(largest, checkpointer.backlog());
            }
            var bound = (heapBudget == 1 ? 2L : 6L) * EVERY + 2L * VALUE.length();
            var observed = largest;
            assertTrue(observed <= bound, () -> observed + " bytes of log the trees lacked, past " + bound);
            assertEquals(200, ledger.size(MESSAGES));
        } finally {
            ledger.close();
        }
    }

    @Test
    @DisplayName("a checkpoint forces at once when another store's writes take the heap budget of the process past it")
    void otherStoreBudget() throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD, 16L << 20);
        var otherDisk = CrashFileSystem.create();
        int committed;
        try (var other = Ledger.builder().directory(otherDisk.getPath("/store")).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).checkpointEvery(1L << 30).build()) {
            var checkpointer = ledger.checkpointer();
            gate.closed.set(true);
            committed = fill(ledger, 0);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            assertTrue(ledger.tree() == null, "the checkpoint ended while the log wrote");
            // About 26 MB of writes in the other store, which checkpoints nothing, past the budget this store's alone fit
            commit(other, 0, 400);
            await(() -> ledger.tree() != null, "the checkpoint went on holding its forces back past the budget of the process");
            assertTrue(gate.closed.get());
        } finally {
            gate.closed.set(false);
            ledger.close();
        }
        try (var reopened = reopen(disk)) {
            assertEquals(committed, reopened.size(MESSAGES));
        }
    }

    @ParameterizedTest(name = "seed {0}, {1}")
    @CsvSource({"1, power loss", "2, power loss", "3, power loss", "4, process crash", "5, process crash", "6, power loss"})
    @DisplayName("a crash while a checkpoint holds its forces back leaves the commits made durable before it, and a state some commit left")
    void crash(int seed, String failure) throws Exception {
        var disk = CrashFileSystem.create();
        var gate = new LogGate();
        disk.intercept(gate);
        var ledger = open(disk, LONG_HOLD);
        CrashFileSystem survivor;
        int committed;
        try {
            var checkpointer = ledger.checkpointer();
            // Durable and checkpointed, so the held checkpoint writes beside a durable superblock
            for (var i = 0; i < 10; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, key(i), VALUE)).awaitDurable();
            }
            ledger.checkpoint();
            gate.closed.set(true);
            committed = fill(ledger, 10);
            await(checkpointer::deferringForTests, "the checkpoint did not hold its forces back while the log wrote");
            survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(seed));
        } finally {
            gate.closed.set(false);
            ledger.closeWithoutCheckpoint();
        }
        try (var reopened = reopen(survivor)) {
            var size = (int) reopened.size(MESSAGES);
            assertTrue(size >= 10 && size <= committed, () -> size + " messages recovered");
            for (var i = 0; i < committed; i++) {
                assertEquals(i < size ? Optional.of(VALUE) : Optional.empty(), reopened.get(MESSAGES, key(i)), key(i));
            }
            // The recovered store takes commits and checkpoints
            reopened.commit(reopened.newBatch().put(MESSAGES, "after", "crash")).awaitDurable();
            reopened.checkpoint();
        }
        try (var reopened = reopen(survivor)) {
            assertEquals(Optional.of("crash"), reopened.get(MESSAGES, "after"));
        }
        CheckpointTest.verifyPages(survivor.getPath("/store"), true);
    }
}
