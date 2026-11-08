package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The spares of a running store: how many a checkpoint keeps, and the checkpoint the log asks for when it would write
/// a new segment with zeros, which runs on every system here through the builder's switch.
///
/// Stores live on the in-memory file system with a force of the log slowed down, as a real device's is: segments then
/// fill slower than the rate below which a new file gets zeros, deterministically. With segments of 64 KiB, of 15
/// durable commits each, and an interval of 1 MiB, the log may ask once 17 segments passed, for at most 8 keys.
class LogSparesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final long SEGMENT = 64 << 10;
    private static final long EVERY = 1 << 20;
    private static final long INTERVAL_SEGMENTS = EVERY / SEGMENT + 1;

    private static Ledger open(Path directory, boolean checkpointForSpares) {
        return open(directory, checkpointForSpares, _ -> {
        });
    }

    private static Ledger open(Path directory, boolean checkpointForSpares, java.util.function.Consumer<LedgerException> failures) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES)
                .checkpointEvery(EVERY).onBackgroundFailure(failures))
                .segmentSize(SEGMENT)
                .checkpointForSpares(checkpointForSpares)
                .build();
    }

    /// {@return a file system whose forces of log segments take a moment}
    private static CrashFileSystem slowLogDisk() {
        var disk = CrashFileSystem.create();
        disk.intercept(LogSparesTest::slowLogForce);
        return disk;
    }

    private static void slowLogForce(Path path, ChangeInterceptor.ChangeKind kind) {
        if (kind == ChangeInterceptor.ChangeKind.FORCE && path.getFileName() != null && LogFiles.segmentNumber(path.getFileName().toString()) >= 0) {
            LockSupport.parkNanos(200_000);
        }
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static List<Path> files(Path directory, String suffix) throws IOException {
        var found = new ArrayList<Path>();
        try (var files = Files.list(directory.resolve("log"))) {
            files.filter(file -> file.getFileName().toString().endsWith(suffix)).forEach(found::add);
        }
        return found;
    }

    /// Commits one of the first `keys` keys durably, by commit number.
    private static void commit(Ledger ledger, int number, int keys) throws InterruptedException {
        ledger.commit(ledger.newBatch().put(NAMES, "k" + number % keys, "v" + number)).awaitDurable();
    }

    /// {@return the number of the next commit, once the log's preparation of a segment waits for a checkpoint}
    private static int untilDeferring(Ledger ledger, int number) throws InterruptedException {
        var log = log(ledger);
        var deferrals = log.deferrals();
        while (log.deferrals() == deferrals) {
            assertTrue(log.currentSegment() < 500, "the log never waited for a checkpoint");
            commit(ledger, number++, 4);
        }
        return number;
    }

    /// Commits as slowly as a checkpoint needs: the commit after a request starts it, and the next one after it ended
    /// starts the preparation.
    ///
    /// @return the number of the next commit
    private static int awaitSpares(Ledger ledger, int number) throws InterruptedException {
        var log = log(ledger);
        while (log.deferring()) {
            var ended = log.checkpointsEnded();
            commit(ledger, number++, 4);
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (log.deferring() && log.checkpointsEnded() == ended && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
        }
        return number;
    }

    @Nested
    @DisplayName("a checkpoint the log asks for")
    class Requested {
        @Test
        @DisplayName("spares a loop over a few keys every new file once an interval's segments passed")
        void hotSet() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                var number = untilDeferring(ledger, 0);
                assertTrue(log.currentSegment() - 1 >= INTERVAL_SEGMENTS, () -> "asked at segment " + log.currentSegment());
                number = awaitSpares(ledger, number);
                var fresh = log.freshFiles();
                var first = log.currentSegment();
                var deferrals = log.deferrals();
                while (log.currentSegment() < first + 3 * (INTERVAL_SEGMENTS + 1)) {
                    commit(ledger, number++, 4);
                    number = awaitSpares(ledger, number);
                }
                assertEquals(fresh, log.freshFiles(), "new files after the first interval");
                assertTrue(log.deferrals() >= deferrals + 2, () -> log.deferrals() + " requests");
                assertEquals(4, ledger.size(NAMES));
            }
        }

        @Test
        @DisplayName("starts the next commit, whose group after it ended prepares the segment from its spares")
        void endWakesPreparation() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                var number = untilDeferring(ledger, 0);
                assertTrue(log.deferring());
                var ended = log.checkpointsEnded();
                var fresh = log.freshFiles();
                commit(ledger, number++, 4);
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (log.checkpointsEnded() == ended && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                commit(ledger, number, 4);
                assertFalse(log.deferring());
                assertEquals(fresh, log.freshFiles());
            }
        }

        @Test
        @DisplayName("is not made where a checkpoint would write many keys")
        void largeDirtySet() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                var number = 0;
                while (log.currentSegment() < INTERVAL_SEGMENTS + 10) {
                    commit(ledger, number++, Integer.MAX_VALUE);
                }
                assertEquals(0, log.deferrals());
                assertEquals(log.appendedBytes(), ((LiveLedger) ledger).checkpointer().backlog(), "a checkpoint ran");
                assertTrue(log.freshFiles() >= INTERVAL_SEGMENTS + 10, () -> log.freshFiles() + " new files");
            }
        }

        @Test
        @DisplayName("is not made where no zeros would be written: a log no commit waits for prepares after the first group")
        void noZeros() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                var number = 0;
                while (log.currentSegment() < INTERVAL_SEGMENTS + 10) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + number % 4, "v".repeat(1_000)));
                    number++;
                }
                assertEquals(0, log.deferrals());
            }
        }

        @Test
        @DisplayName("is not made in the first segments after an open, which finds no spares")
        void afterReopen() throws Exception {
            var disk = slowLogDisk();
            var directory = disk.getPath("/store");
            int number;
            try (var ledger = open(directory, true)) {
                number = untilDeferring(ledger, 0);
                number = awaitSpares(ledger, number);
            }
            try (var ledger = open(directory, true)) {
                var log = log(ledger);
                commit(ledger, number++, 4);
                var start = log.currentSegment();
                while (log.currentSegment() < start + INTERVAL_SEGMENTS - 2) {
                    commit(ledger, number++, 4);
                }
                assertEquals(0, log.deferrals());
            }
        }

        @Test
        @DisplayName("is not made with the switch off")
        void switchedOff() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), false)) {
                var log = log(ledger);
                var number = 0;
                while (log.currentSegment() < INTERVAL_SEGMENTS + 10) {
                    commit(ledger, number++, 4);
                }
                assertEquals(0, log.deferrals());
            }
        }

        @Test
        @DisplayName("that throws leaves the log healthy, preparing the segment as usual")
        void throwingSource() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                var asked = new AtomicBoolean();
                log.useCheckpointForSpares(() -> {
                    asked.set(true);
                    throw new IllegalStateException("thrown on the flush task");
                });
                var number = 0;
                while (log.currentSegment() < 10) {
                    commit(ledger, number++, 4);
                }
                assertTrue(asked.get());
                assertEquals(0, log.deferrals());
                log.checkHealthy();
            }
        }

        @Test
        @DisplayName("gives way once half the segment filled, if no checkpoint ends")
        void halfSegment() throws Exception {
            var disk = slowLogDisk();
            try (var ledger = open(disk.getPath("/store"), true)) {
                var log = log(ledger);
                // Claims a checkpoint will come, where none does
                log.useCheckpointForSpares(() -> true);
                var number = untilDeferring(ledger, 0);
                var segment = log.currentSegment();
                var fresh = log.freshFiles();
                var ended = log.checkpointsEnded();
                assertTrue(log.deferring());
                while (log.deferring()) {
                    commit(ledger, number++, 4);
                }
                assertEquals(segment, log.currentSegment(), "it should prepare before the rollover");
                assertEquals(ended, log.checkpointsEnded());
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (log.freshFiles() == fresh && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                assertEquals(fresh + 1, log.freshFiles());
            }
        }

        @Test
        @DisplayName("a group past the segment's end prepares inline while waiting, and the commits go on")
        void largeGroupWhileDeferring() throws Exception {
            var disk = slowLogDisk();
            var directory = disk.getPath("/store");
            var value = "v".repeat((int) (2 * SEGMENT));
            try (var ledger = open(directory, true)) {
                var log = log(ledger);
                log.useCheckpointForSpares(() -> true);
                var number = untilDeferring(ledger, 0);
                assertTrue(log.deferring());
                var segment = log.currentSegment();
                ledger.commit(ledger.newBatch().put(NAMES, "large", value)).awaitDurable();
                assertTrue(log.currentSegment() > segment);
                commit(ledger, number, 4);
                log.checkHealthy();
            }
            try (var ledger = open(directory, false)) {
                assertEquals(Optional.of(value), ledger.get(NAMES, "large"));
            }
        }

        @Test
        @DisplayName("that fails has the segment prepared before the rollover, and the log asks no more until one succeeds")
        void failedCheckpoint() throws Exception {
            var disk = slowLogDisk();
            var failing = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                slowLogForce(path, kind);
                if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName() != null
                        && path.getFileName().toString().equals(PageFile.FILE)) {
                    throw new IOException("No space left on device");
                }
            });
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var directory = disk.getPath("/store");
            int number;
            try (var ledger = open(directory, true, failures::add)) {
                var log = log(ledger);
                number = untilDeferring(ledger, 0);
                assertTrue(log.deferring());
                var segment = log.currentSegment();
                var fresh = log.freshFiles();
                failing.set(true);
                number = awaitSpares(ledger, number);
                assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures);
                assertEquals(segment, log.currentSegment(), "it should prepare before the rollover");
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (log.freshFiles() == fresh && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                assertEquals(fresh + 1, log.freshFiles());
                var deferrals = log.deferrals();
                while (log.currentSegment() < segment + 5) {
                    commit(ledger, number++, 4);
                }
                assertEquals(deferrals, log.deferrals());
                failing.set(false);
                ledger.checkpoint();
                commit(ledger, number++, 4);
            }
            try (var ledger = open(directory, false)) {
                assertEquals(Optional.of("v" + (number - 1)), ledger.get(NAMES, "k" + (number - 1) % 4));
            }
        }

        @Test
        @DisplayName("a close while the preparation waits leaves one empty segment of two blocks, which the next open writes into")
        void closeWhileDeferring() throws Exception {
            var disk = slowLogDisk();
            var directory = disk.getPath("/store");
            int number;
            long current;
            try (var ledger = open(directory, true)) {
                var log = log(ledger);
                number = untilDeferring(ledger, 0);
                assertTrue(log.deferring());
                current = log.currentSegment();
            }
            var left = files(directory, "");
            assertEquals(1, left.size(), () -> "files left in the log: " + left);
            var kept = LogFiles.segmentNumber(left.getFirst().getFileName().toString());
            assertEquals(current + 1, kept);
            assertEquals(LogWriter.KEPT_LENGTH, Files.size(left.getFirst()));
            try (var ledger = open(directory, true)) {
                assertEquals(4, ledger.size(NAMES));
                assertEquals(Optional.of("v" + (number - 1)), ledger.get(NAMES, "k" + (number - 1) % 4));
                commit(ledger, number, 4);
                assertEquals(kept, log(ledger).currentSegment());
                assertEquals(0, log(ledger).freshFiles());
            }
        }
    }

    @Nested
    @DisplayName("the spares a checkpoint keeps")
    class Kept {
        @Test
        @DisplayName("cover what the interval really fills when commits take more log than they count")
        void smallCommits() throws Exception {
            var disk = slowLogDisk();
            var directory = disk.getPath("/store");
            try (var ledger = open(directory, false)) {
                var log = log(ledger);
                var number = 0;
                while (log.currentSegment() < INTERVAL_SEGMENTS + 20) {
                    commit(ledger, number++, 4);
                }
                var current = log.currentSegment();
                ledger.checkpoint();
                assertEquals(current, log.currentSegment());
                var spares = files(directory, ".spare");
                assertEquals(current - 1, spares.size(), () -> "spares: " + spares);
            }
            assertEquals(List.of(), files(directory, ".spare"));
            assertEquals(1, files(directory, "").size());
        }

        @Test
        @DisplayName("are about an interval's bytes when commits take the log they count")
        void largeCommits() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var segment = 1L << 20;
            var every = 4L << 20;
            var value = "v".repeat(60_000);
            try (var ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES)
                    .checkpointEvery(every)).segmentSize(segment).checkpointForSpares(false).build()) {
                for (var number = 0; number < 400; number++) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + number % 4, value)).awaitDurable();
                }
                ledger.checkpoint();
                var spares = files(directory, ".spare");
                assertFalse(spares.isEmpty());
                assertTrue(spares.size() <= every / segment + 2, () -> "spares: " + spares);
            }
        }
    }

    @Nested
    @DisplayName("a crash")
    class Crashes {
        private static final int WRITERS = 3;
        private static final int COMMITS = 400;

        /// A hot set of six keys, two for each of three writers, and what they know of their commits: a key's value is
        /// the number of the writer's commit that wrote it.
        private static final class Workload {
            final AtomicLongArray issued = new AtomicLongArray(WRITERS);
            final AtomicLongArray durable = new AtomicLongArray(WRITERS);
            long deferrals;

            void run(Path directory) throws InterruptedException {
                Ledger ledger;
                try {
                    ledger = open(directory, true);
                } catch (RuntimeException _) {
                    return;
                }
                var threads = new ArrayList<Thread>();
                for (var writer = 0; writer < WRITERS; writer++) {
                    var id = writer;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            for (var commit = 1; commit <= COMMITS; commit++) {
                                issued.set(id, commit);
                                ledger.commit(ledger.newBatch().put(NAMES, "w" + id + (commit % 2), "" + commit)).awaitDurable();
                                durable.set(id, commit);
                            }
                        } catch (RuntimeException | InterruptedException _) {
                            // The file system stopped
                        }
                    }));
                }
                for (var thread : threads) {
                    thread.join();
                }
                deferrals = log(ledger).deferrals();
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // The file system stopped: closing only lets go of the threads
                }
            }

            void check(Ledger ledger) {
                for (var writer = 0; writer < WRITERS; writer++) {
                    var newest = 0L;
                    for (var key = 0; key < 2; key++) {
                        newest = Math.max(newest, ledger.get(NAMES, "w" + writer + key).map(Long::parseLong).orElse(0L));
                    }
                    assertTrue(newest >= durable.get(writer), "writer " + writer + " lost a durable commit: " + newest + " of " + durable.get(writer));
                    assertTrue(newest <= issued.get(writer), "writer " + writer + " recovered a commit it never made: " + newest);
                }
            }
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
        @DisplayName("of concurrent writers over few keys loses no durable commit, wherever it strikes")
        void concurrentWriters(int seed) throws Exception {
            var random = new Random(seed);
            var dry = slowLogDisk();
            var dryRun = new Workload();
            dryRun.run(dry.getPath("/store"));
            assertTrue(dryRun.deferrals > 0, "the workload never reached a preparation that waits");
            var disk = slowLogDisk();
            if (seed % 2 == 0) {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            } else {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            }
            var workload = new Workload();
            workload.run(disk.getPath("/store"));
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            var directory = survivor.getPath("/store");
            try (var ledger = open(directory, true)) {
                workload.check(ledger);
                ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
            }
            try (var ledger = open(directory, true)) {
                assertEquals(Optional.of("recovery"), ledger.get(NAMES, "after"));
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("while the preparation waits, and while the checkpoint it waits for runs, recovers every durable commit")
        void whileDeferring() throws Exception {
            for (var duringCheckpoint = 0; duringCheckpoint < 2; duringCheckpoint++) {
                var disk = slowLogDisk();
                var directory = disk.getPath("/store");
                var ledger = open(directory, true);
                int number;
                try {
                    number = untilDeferring(ledger, 0);
                    assertTrue(log(ledger).deferring());
                    if (duringCheckpoint == 1) {
                        // Starts it, and the file system stops at one of its forces, or at one of the next group's
                        disk.stopAtForce(2);
                        try {
                            commit(ledger, number, 4);
                            number++;
                        } catch (RuntimeException _) {
                            // The file system stopped under the commit, which may be lost
                        }
                    } else {
                        disk.stopAfter(0);
                    }
                } finally {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
                var survivor = disk.afterPowerLoss(new Random(duringCheckpoint));
                try (var reopened = open(survivor.getPath("/store"), true)) {
                    assertEquals(4, reopened.size(NAMES));
                    var last = number - 1;
                    assertEquals(Optional.of("v" + last), reopened.get(NAMES, "k" + last % 4));
                    commit(reopened, number, 4);
                }
                CheckpointTest.verifyPages(survivor.getPath("/store"), true);
            }
        }
    }
}
