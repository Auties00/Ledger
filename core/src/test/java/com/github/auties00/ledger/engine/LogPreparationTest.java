package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// When the log prepares the segment after the current one: late unless the next one takes zeros, at the latest at a
/// close, and what a crash at any of those points leaves to the next open.
///
/// Each group of a small commit takes one block of a segment, so a segment of [#SEGMENT] bytes reaches its late start
/// at the 16th group, and a batch of [#WIDE] values crosses it at once, so the preparation runs beside a first group
/// that is not durable yet.
class LogPreparationTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final long SEGMENT = 256 << 10;
    private static final int WIDE = 100;

    private static Ledger open(Path directory, long segmentSize) {
        return open(directory, segmentSize, Encryption.key(KEY));
    }

    private static Ledger open(Path directory, long segmentSize, Encryption encryption) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES))
                .segmentSize(segmentSize)
                .build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static Path segment(Path directory, long number) {
        return directory.resolve("log").resolve(LogWriter.segmentName(number));
    }

    /// Waits for `file`, which a thread of its own prepares, to take its name.
    private static void awaitFile(Path file) throws InterruptedException {
        var deadline = System.nanoTime() + 30_000_000_000L;
        while (!Files.exists(file)) {
            assertTrue(System.nanoTime() < deadline, () -> file + " was never prepared");
            Thread.sleep(1);
        }
    }

    private static void closeQuietly(Ledger ledger) {
        try {
            ledger.close();
        } catch (RuntimeException _) {
            // The file system stopped, or the log failed: closing only lets go of the threads
        }
    }

    /// {@return the store in `directory` closed once, so its next open writes into the segment the close kept}
    private static long closedOnce(Path directory) throws Exception {
        return closedOnce(directory, Encryption.key(KEY));
    }

    private static long closedOnce(Path directory, Encryption encryption) throws Exception {
        try (var ledger = open(directory, SEGMENT, encryption)) {
            ledger.commit(ledger.newBatch().put(NAMES, "first", "1")).awaitDurable();
        }
        try (var files = Files.list(directory.resolve("log"))) {
            return LogFiles.segmentNumber(files.toList().getFirst().getFileName().toString());
        }
    }

    @Test
    @DisplayName("the first commits after an open create no file, until a quarter of the segment is written")
    void lateStart(@TempDir Path directory) throws Exception {
        var kept = closedOnce(directory);
        try (var ledger = open(directory, SEGMENT)) {
            var commits = 0;
            for (; commits < 12; commits++) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + commits, "v")).awaitDurable();
                assertEquals(kept, log(ledger).currentSegment());
                assertFalse(Files.exists(segment(directory, kept + 1)), "prepared after " + (commits + 1) + " commits");
            }
            for (; commits < 20; commits++) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + commits, "v")).awaitDurable();
            }
            awaitFile(segment(directory, kept + 1));
            assertEquals(kept, log(ledger).currentSegment());
            while (log(ledger).currentSegment() == kept) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + commits++, "v")).awaitDurable();
            }
            assertEquals(kept + 1, log(ledger).currentSegment());
        }
    }

    @Test
    @DisplayName("a close prepares and keeps the next segment when no commit started it, for the next open to write into")
    void closePrepares(@TempDir Path directory) throws Exception {
        var kept = closedOnce(directory);
        for (var round = 0; round < 3; round++) {
            try (var ledger = open(directory, SEGMENT)) {
                ledger.commit(ledger.newBatch().put(NAMES, "r" + round, "v")).awaitDurable();
                assertEquals(kept + round, log(ledger).currentSegment());
                assertFalse(Files.exists(segment(directory, kept + round + 1)));
            }
            try (var files = Files.list(directory.resolve("log"))) {
                assertEquals(List.of(segment(directory, kept + round + 1)), files.toList());
            }
            assertEquals(LogWriter.KEPT_LENGTH, Files.size(segment(directory, kept + round + 1)));
        }
        try (var ledger = open(directory, SEGMENT)) {
            assertEquals(4, ledger.size(NAMES));
        }
    }

    @Test
    @DisplayName("an interrupted close still keeps the next segment, and the thread keeps its interrupt")
    void interruptedClose(@TempDir Path directory) throws Exception {
        var kept = closedOnce(directory);
        var ledger = open(directory, SEGMENT);
        ledger.commit(ledger.newBatch().put(NAMES, "a", "1")).awaitDurable();
        Thread.currentThread().interrupt();
        try {
            ledger.close();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(LogWriter.KEPT_LENGTH, Files.size(segment(directory, kept + 1)));
        try (var reopened = open(directory, SEGMENT)) {
            assertEquals(kept + 1, log(reopened).currentSegment());
            assertEquals(Optional.of("1"), reopened.get(NAMES, "a"));
        }
    }

    @Test
    @DisplayName("a close whose preparation fails loses nothing, and the next open prepares its own segment")
    void closePreparationFails() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var ledger = open(directory, SEGMENT);
        ledger.commit(ledger.newBatch().put(NAMES, "a", "1")).awaitDurable();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.CREATE && path.toString().endsWith(".prep")) {
                throw new IOException("No space left on device");
            }
        });
        ledger.close();
        disk.intercept((_, _) -> {
        });
        try (var reopened = open(directory, SEGMENT)) {
            assertEquals(Optional.of("1"), reopened.get(NAMES, "a"));
            reopened.commit(reopened.newBatch().put(NAMES, "b", "2")).awaitDurable();
        }
        try (var reopened = open(directory, SEGMENT)) {
            assertEquals(2, reopened.size(NAMES));
        }
    }

    @Test
    @DisplayName("a close after the log failed creates no file on the failing device")
    void stoppedWriter() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var ledger = open(directory, SEGMENT);
        ledger.commit(ledger.newBatch().put(NAMES, "a", "1")).awaitDurable();
        var created = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                throw new IOException("Input/output error");
            }
            if (kind == ChangeInterceptor.ChangeKind.CREATE && path.toString().endsWith(".prep")) {
                created.incrementAndGet();
            }
        });
        var failed = ledger.commit(ledger.newBatch().put(NAMES, "b", "2"));
        assertThrows(RuntimeException.class, failed::awaitDurable);
        closeQuietly(ledger);
        assertEquals(0, created.get());
        disk.intercept((_, _) -> {
        });
        try (var reopened = open(disk.afterProcessCrash().getPath("/store"), SEGMENT)) {
            assertEquals(Optional.of("1"), reopened.get(NAMES, "a"));
            reopened.commit(reopened.newBatch().put(NAMES, "c", "3")).awaitDurable();
        }
    }

    @Test
    @DisplayName("a crash before the late start leaves no next segment, and the next open prepares and writes one")
    void crashBeforeLateStart() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var ledger = open(directory, SEGMENT);
        long current;
        try {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "1")).awaitDurable();
            current = log(ledger).currentSegment();
            assertFalse(Files.exists(segment(directory, current + 1)));
        } finally {
            disk.stopAfter(0);
            closeQuietly(ledger);
        }
        var survivor = disk.afterPowerLoss(new Random(1));
        var recovered = survivor.getPath("/store");
        try (var reopened = open(recovered, SEGMENT)) {
            assertEquals(Optional.of("1"), reopened.get(NAMES, "a"));
            reopened.commit(reopened.newBatch().put(NAMES, "b", "2")).awaitDurable();
            assertEquals(current + 1, log(reopened).currentSegment());
        }
        try (var reopened = open(recovered, SEGMENT)) {
            assertEquals(2, reopened.size(NAMES));
        }
    }

    /// Commits a few values and closes, twice over.
    private static void session(Path directory, List<String> durable) {
        Ledger ledger = null;
        try {
            for (var round = 0; round < 2; round++) {
                ledger = open(directory, SEGMENT);
                for (var i = 0; i < 3; i++) {
                    var key = "r" + round + "-" + i;
                    ledger.commit(ledger.newBatch().put(NAMES, key, "v")).awaitDurable();
                    durable.add(key);
                }
                ledger.close();
            }
        } catch (RuntimeException | InterruptedException _) {
            if (ledger != null) {
                closeQuietly(ledger);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"process crash", "power loss"})
    @DisplayName("a stop at any change of a short session, the preparation at its close included, recovers every durable commit")
    void everyChange(String failure) throws Exception {
        var dry = CrashFileSystem.create();
        session(dry.getPath("/store"), new ArrayList<>());
        for (var stop = 0L; stop <= dry.changes(); stop++) {
            var disk = CrashFileSystem.create();
            disk.stopAfter(stop);
            var durable = new ArrayList<String>();
            session(disk.getPath("/store"), durable);
            var survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(stop));
            var recovered = survivor.getPath("/store");
            try (var ledger = open(recovered, SEGMENT)) {
                for (var key : durable) {
                    assertEquals(Optional.of("v"), ledger.get(NAMES, key), "after change " + stop);
                }
                ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
            }
            try (var ledger = open(recovered, SEGMENT)) {
                assertEquals(Optional.of("recovery"), ledger.get(NAMES, "after"), "after change " + stop);
            }
        }
    }

    /// Queues one batch of [#WIDE] values, a frame of 100 KB, past the late start of a segment of [#SEGMENT] bytes.
    private static Commit commitWide(Ledger ledger) {
        var batch = ledger.newBatch();
        for (var i = 0; i < WIDE; i++) {
            batch.put(NAMES, "wide" + i, "v".repeat(1_000));
        }
        return ledger.commit(batch);
    }

    /// {@return how many values of the batch of [#commitWide(Ledger)] `ledger` holds, which must be all or none}
    private static int wideValues(Ledger ledger, String context) {
        var present = 0;
        for (var i = 0; i < WIDE; i++) {
            present += ledger.get(NAMES, "wide" + i).isPresent() ? 1 : 0;
        }
        assertTrue(present == 0 || present == WIDE, context + ": " + present + " values of one batch");
        return present;
    }

    /// Asserts that a second recovery finds the commit made after the first, and counts it with the `expected` values
    /// before it.
    private static void reopensWithExactCount(Path directory, Encryption encryption, int expected, String context) {
        try (var ledger = open(directory, SEGMENT, encryption)) {
            assertEquals(expected + 1, ledger.size(NAMES), context);
            assertEquals(expected + 1, ledger.range(NAMES, KeyRange.all(), expected + 2).size(), context);
            assertEquals(Optional.of("recovery"), ledger.get(NAMES, "after"), context);
        }
    }

    /// What one recovery of a first group cut short found.
    ///
    /// @param prepared whether the next segment survived under its name
    /// @param unwritten whether the first frame of the segment the group went to is all zeros
    /// @param wide how many values of the group recovery replayed
    private record CutShort(boolean prepared, boolean unwritten, int wide) {
    }

    /// Recovers a store whose segment `kept` took a first group of [#WIDE] values that a force never completed, commits
    /// once more and recovers again.
    private static CutShort recoverCutShort(CrashFileSystem survivor, long kept, Encryption encryption, String context) throws Exception {
        var directory = survivor.getPath("/store");
        // The state recovery reads, before it writes anything
        var prepared = Files.exists(segment(directory, kept + 1));
        var bytes = Files.readAllBytes(segment(directory, kept));
        var unwritten = true;
        for (var i = LogWriter.SEGMENT_HEADER; i < Math.min(bytes.length, LogWriter.SEGMENT_HEADER + LogWriter.FRAME_HEADER); i++) {
            unwritten &= bytes[i] == 0;
        }
        int wide;
        try (var ledger = open(directory, SEGMENT, encryption)) {
            wide = wideValues(ledger, context);
            assertEquals(Optional.of("1"), ledger.get(NAMES, "first"), context);
            assertEquals(1 + wide, ledger.size(NAMES), context);
            ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
            assertTrue(log(ledger).currentSegment() >= (prepared ? kept + 1 : kept), context);
        }
        reopensWithExactCount(directory, encryption, 1 + wide, context);
        return new CutShort(prepared, unwritten, wide);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a first group that started the next segment's preparation and never became durable is recovered whole or not at all, and the next commit follows it")
    void firstGroupCutShort(boolean encrypted) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var kept = closedOnce(directory, encryption);
        var logDirectory = directory.resolve("log").toString();
        var forcing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var beforeDirectoryForce = new AtomicLong(Long.MAX_VALUE);
        disk.intercept((path, kind) -> {
            if (kind != ChangeInterceptor.ChangeKind.FORCE) {
                return;
            }
            if (path.toString().equals(logDirectory)) {
                beforeDirectoryForce.set(disk.forces());
            } else if (path.toString().endsWith(".log")) {
                forcing.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        var ledger = open(directory, SEGMENT, encryption);
        try {
            commitWide(ledger);
            forcing.await();
            awaitFile(segment(directory, kept + 1));
            // The name of the next segment durable too, so a loss of power keeps it
            var deadline = System.nanoTime() + 30_000_000_000L;
            while (disk.forces() <= beforeDirectoryForce.get()) {
                assertTrue(System.nanoTime() < deadline, "the preparation never forced its directory");
                Thread.sleep(1);
            }
            assertEquals(kept, log(ledger).currentSegment());

            var crashed = recoverCutShort(disk.afterProcessCrash(), kept, encryption, "process crash");
            assertEquals(WIDE, crashed.wide(), "a crash of the process keeps what was written");

            var unwritten = 0;
            var torn = 0;
            var whole = 0;
            for (var seed = 0; seed < 300; seed++) {
                var state = recoverCutShort(disk.afterPowerLoss(new Random(seed)), kept, encryption, "seed " + seed);
                if (state.wide() != 0) {
                    whole++;
                } else if (state.prepared() && state.unwritten()) {
                    unwritten++;
                } else if (state.prepared()) {
                    torn++;
                }
            }
            assertTrue(unwritten > 0, "no draw left an unwritten segment before the prepared one");
            assertTrue(torn > 0, "no draw left a torn first group before the prepared one");
            assertTrue(whole > 0, "no draw kept the whole group");
        } finally {
            disk.stopAfter(0);
            release.countDown();
            closeQuietly(ledger);
        }
    }

    @Test
    @DisplayName("an open makes durable the name of a segment a crashed process prepared beside its first group, and only then, not after a close")
    void preparedNameForced() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var kept = closedOnce(directory);
        var keptFile = segment(directory, kept).toString();
        var logDirectory = directory.resolve("log").toString();
        var writing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var forcesAfterClose = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().equals(logDirectory)) {
                forcesAfterClose.incrementAndGet();
            }
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().equals(keptFile)) {
                writing.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        var ledger = open(directory, SEGMENT);
        CrashFileSystem survivor;
        try {
            assertEquals(0, forcesAfterClose.get(), "an open after a close forces no directory");
            commitWide(ledger);
            writing.await();
            awaitFile(segment(directory, kept + 1));
            // A crash of the process with the segment renamed, nothing written to the one before it, and possibly its
            // directory not forced: a later loss of power could drop the name with an acknowledged commit in the file,
            // and the image of a process crash, durable as a whole, cannot show that, so the force is what is checked
            survivor = disk.afterProcessCrash();
        } finally {
            disk.stopAfter(0);
            release.countDown();
            closeQuietly(ledger);
        }
        var recovered = survivor.getPath("/store");
        var forces = new AtomicInteger();
        survivor.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().equals(logDirectory)) {
                forces.incrementAndGet();
            }
        });
        try (var reopened = open(recovered, SEGMENT)) {
            assertTrue(forces.get() > 0, "the open left the name of the segment it writes into to the cache");
            assertEquals(1, reopened.size(NAMES));
            reopened.commit(reopened.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
            assertEquals(kept + 1, log(reopened).currentSegment());
        }
        reopensWithExactCount(recovered, Encryption.key(KEY), 1, "second recovery");
    }

    /// Runs a session whose first commit after an open crosses the late start, after one that kept its segment.
    private static void wideSession(Path directory, Encryption encryption, List<String> acknowledged) {
        Ledger ledger = null;
        try {
            ledger = open(directory, SEGMENT, encryption);
            ledger.commit(ledger.newBatch().put(NAMES, "first", "1")).awaitDurable();
            acknowledged.add("first");
            ledger.close();
            ledger = open(directory, SEGMENT, encryption);
            commitWide(ledger).awaitDurable();
            acknowledged.add("wide");
            ledger.commit(ledger.newBatch().put(NAMES, "last", "1")).awaitDurable();
            acknowledged.add("last");
            ledger.close();
        } catch (RuntimeException | InterruptedException _) {
            if (ledger != null) {
                closeQuietly(ledger);
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}, {1}")
    @CsvSource({"true, process crash", "true, power loss", "false, process crash", "false, power loss"})
    @DisplayName("a stop at any change of a session whose first commit crosses the late start keeps the commits in order, each whole, and every durable one")
    void wideFirstCommit(boolean encrypted, String failure) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        var dry = CrashFileSystem.create();
        wideSession(dry.getPath("/store"), encryption, new ArrayList<>());
        for (var stop = 0L; stop <= dry.changes(); stop++) {
            var disk = CrashFileSystem.create();
            disk.stopAfter(stop);
            var acknowledged = new ArrayList<String>();
            wideSession(disk.getPath("/store"), encryption, acknowledged);
            var survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(stop));
            var recovered = survivor.getPath("/store");
            var context = "after change " + stop;
            int values;
            try (var ledger = open(recovered, SEGMENT, encryption)) {
                var wide = wideValues(ledger, context);
                var first = ledger.get(NAMES, "first").isPresent();
                var last = ledger.get(NAMES, "last").isPresent();
                assertTrue(first || !acknowledged.contains("first"), context);
                assertTrue(wide == WIDE || !acknowledged.contains("wide"), context);
                assertTrue(last || !acknowledged.contains("last"), context);
                assertTrue(first || wide == 0, context + ": a commit without the one before it");
                assertTrue(wide == WIDE || !last, context + ": a commit without the one before it");
                values = (first ? 1 : 0) + wide + (last ? 1 : 0);
                assertEquals(values, ledger.size(NAMES), context);
                ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
            }
            reopensWithExactCount(recovered, encryption, values, context);
        }
    }

    @Test
    @DisplayName("a group that crosses whole segments from before the late start rolls over through each, and survives a crash")
    void wideGroup() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var segmentSize = 64L << 10;
        var armed = new AtomicBoolean();
        var forcing = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && armed.compareAndSet(true, false)) {
                forcing.countDown();
                try {
                    release.await();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        var ledger = open(directory, segmentSize);
        var value = "v".repeat(1_000);
        var first = ledger.commit(ledger.newBatch().put(NAMES, "start", "1"));
        first.awaitDurable();
        var written = log(ledger).currentSegment();
        // The commits below queue behind this one's force, and the group after it holds them all, 600 KB
        armed.set(true);
        ledger.commit(ledger.newBatch().put(NAMES, "blocked", "1"));
        forcing.await();
        var last = first;
        for (var i = 0; i < 300; i++) {
            last = ledger.commit(ledger.newBatch().put(NAMES, "k" + i, value));
        }
        var huge = ledger.newBatch();
        for (var i = 0; i < 300; i++) {
            huge.put(NAMES, "h" + i, value);
        }
        last = ledger.commit(huge);
        release.countDown();
        last.awaitDurable();
        assertTrue(log(ledger).currentSegment() >= written + 6, () -> "only reached segment " + log(ledger).currentSegment());
        disk.stopAfter(0);
        closeQuietly(ledger);
        try (var reopened = open(disk.afterPowerLoss(new Random(3)).getPath("/store"), segmentSize)) {
            assertEquals(602, reopened.size(NAMES));
            assertEquals(Optional.of(value), reopened.get(NAMES, "k299"));
            assertEquals(Optional.of(value), reopened.get(NAMES, "h299"));
        }
    }

    @Test
    @DisplayName("concurrent committers and checkpoints roll over through many late starts, and a reopen finds every commit")
    void concurrent(@TempDir Path directory) throws Exception {
        var segmentSize = 128L << 10;
        var threads = 8;
        var perThread = 400;
        var value = "v".repeat(500);
        try (var ledger = open(directory, segmentSize); var pool = Executors.newFixedThreadPool(threads + 1)) {
            var stop = new AtomicBoolean();
            var checkpoints = pool.submit(() -> {
                while (!stop.get()) {
                    ledger.checkpoint();
                    Thread.sleep(5);
                }
                return null;
            });
            var committers = new ArrayList<Future<?>>();
            for (var thread = 0; thread < threads; thread++) {
                var id = thread;
                committers.add(pool.submit(() -> {
                    for (var i = 0; i < perThread; i++) {
                        var commit = ledger.commit(ledger.newBatch().put(NAMES, id + "-" + i, value));
                        if (i % 4 == 0) {
                            commit.awaitDurable();
                        }
                    }
                    return null;
                }));
            }
            for (var committer : committers) {
                committer.get();
            }
            stop.set(true);
            checkpoints.get();
            assertTrue(log(ledger).currentSegment() > 5, () -> "only reached segment " + log(ledger).currentSegment());
        }
        try (var ledger = open(directory, segmentSize)) {
            assertEquals(threads * perThread, ledger.size(NAMES));
            assertEquals(threads * perThread, ledger.range(NAMES, KeyRange.all(), threads * perThread + 1).size());
        }
        try (var files = Files.list(directory.resolve("log"))) {
            assertEquals(1, files.count());
        }
    }

    /// Commits small values until the log writes segment `number`, one group at a time, slowly enough that the one before
    /// it fills slower than the rate zeros need, and asking only whether each is durable: a wait for it would count.
    private static void fillWithoutWaiting(Ledger ledger, long number) throws InterruptedException {
        var commits = 0;
        while (log(ledger).currentSegment() < number) {
            var commit = ledger.commit(ledger.newBatch().put(NAMES, "w" + number + "-" + commits++, "v"));
            while (!commit.isDurable()) {
                Thread.sleep(1);
            }
            Thread.sleep(1);
        }
    }

    /// {@return a file system whose forces of a segment take a millisecond, so a commit always finds its force under way
    /// when it waits for it, and a segment of 64 KiB, of 15 groups, fills slower than the 977 microseconds it takes at the
    /// rate zeros need}
    private static CrashFileSystem slowForces() {
        var disk = CrashFileSystem.create();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        return disk;
    }

    @Test
    @DisplayName("a late preparation takes no zeros, even if the commits began to wait for the forces after the segment's first group")
    void frozenDecision() throws Exception {
        var segmentSize = 64L << 10;
        var directory = slowForces().getPath("/store");
        try (var ledger = open(directory, segmentSize)) {
            var log = log(ledger);
            var second = log.currentSegment() + 1;
            fillWithoutWaiting(ledger, second);
            var commits = 0;
            while (!Files.exists(segment(directory, second + 1)) && commits < 32) {
                ledger.commit(ledger.newBatch().put(NAMES, "s" + commits++, "v")).awaitDurable();
            }
            awaitFile(segment(directory, second + 1));
            assertEquals(second, log.currentSegment());
            assertEquals(LogWriter.KEPT_LENGTH, Files.size(segment(directory, second + 1)));
        }
    }

    @Test
    @DisplayName("a segment after one filled slowly by waiting commits starts its successor's zeros at its first group, where the file system takes them")
    void zerosAtFirstGroup() throws Exception {
        var segmentSize = 64L << 10;
        var directory = slowForces().getPath("/store");
        try (var ledger = open(directory, segmentSize)) {
            var log = log(ledger);
            var third = log.currentSegment() + 2;
            var commits = 0;
            while (log.currentSegment() < third) {
                ledger.commit(ledger.newBatch().put(NAMES, "s" + commits++, "v")).awaitDurable();
            }
            if (LogFiles.ZERO_FILL) {
                awaitFile(segment(directory, third + 1));
                assertEquals(segmentSize, Files.size(segment(directory, third + 1)));
            } else {
                assertFalse(Files.exists(segment(directory, third + 1)));
            }
        }
    }
}
