package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What the log depends on beyond the frames it writes: the name of the segment a group lands in, and what a process
/// that crashed left in the cache of the operating system, neither of which a group written through forces.
///
/// A process crash on the simulated file system keeps the cache at risk, so a loss of power after it finds what the
/// recovery left unforced. The interceptors hold the preparation of the next segment, whose forces would otherwise
/// cover the store a moment later.
class LogNamesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Fifteen groups of a block each to a segment
    private static final long SEGMENT = 64 << 10;
    private static final int DRAWS = 8;

    private static Ledger open(Path directory, WriteThrough.Mode mode, long segmentSize) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(segmentSize)
                .writeThrough(mode)
                .build();
    }

    private static Ledger open(Path directory, WriteThrough.Mode mode) {
        return open(directory, mode, SEGMENT);
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key) throws InterruptedException {
        var value = "v".repeat(500) + key;
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    private static TreeMap<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    /// Checks that every state a loss of power now could leave of `disk` holds `model`.
    private static void survivesPowerLoss(CrashFileSystem disk, Map<String, String> model) {
        for (var seed = 0; seed < DRAWS; seed++) {
            try (var ledger = open(disk.afterPowerLoss(new Random(seed)).getPath("/store"), WriteThrough.Mode.OFF)) {
                var recovered = contents(ledger);
                for (var entry : model.entrySet()) {
                    assertEquals(entry.getValue(), recovered.get(entry.getKey()), "seed " + seed + ", key " + entry.getKey());
                }
            }
        }
    }

    private static boolean isLogDirectory(Path path) {
        return path.toString().endsWith("/log");
    }

    private static boolean isPreparing(Path path) {
        var name = path.toString();
        return name.endsWith(".prep") || name.endsWith(".spare");
    }

    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(60, TimeUnit.SECONDS)) {
                throw new IOException("Held for too long");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
        }
    }

    /// Holds every force of a segment being prepared on `disk`, from the first write of a group, until `hold` counts
    /// down, so no preparation flushes what a test looks at, and the one the first group waits for still finishes.
    private static void holdPreparations(CrashFileSystem disk, CountDownLatch hold) {
        var written = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".log")) {
                written.set(true);
            } else if (kind == ChangeInterceptor.ChangeKind.FORCE && written.get() && isPreparing(path)) {
                await(hold);
            }
        });
    }

    @Nested
    @DisplayName("after a process crash that left the cache as it was")
    class ProcessCrash {
        /// Commits, some waiting for durability and some not, across segments and a checkpoint, then a close and an
        /// open, on `disk` until it stops.
        private static void workload(CrashFileSystem disk, WriteThrough.Mode mode) {
            var commits = 0;
            try {
                try (var ledger = open(disk.getPath("/store"), mode)) {
                    for (var index = 0; index < 22; index++) {
                        var pending = ledger.commit(ledger.newBatch().put(NAMES, "k%03d".formatted(commits++), "v".repeat(500)));
                        if (index % 3 != 2) {
                            pending.awaitDurable();
                        }
                        if (index == 18) {
                            ledger.checkpoint();
                        }
                    }
                }
                try (var ledger = open(disk.getPath("/store"), mode)) {
                    for (var index = 0; index < 3; index++) {
                        ledger.commit(ledger.newBatch().put(NAMES, "k%03d".formatted(commits++), "v".repeat(500)));
                    }
                }
            } catch (RuntimeException _) {
                // The simulated process stopped
            } catch (InterruptedException exception) {
                throw new IllegalStateException(exception);
            }
        }

        @ParameterizedTest(name = "write through: {0}")
        @EnumSource(value = WriteThrough.Mode.class, names = {"DSYNC", "OFF"})
        @DisplayName("everything recovery found and the next durable commit survive a loss of power, whichever change the crash came at")
        void everyChange(WriteThrough.Mode mode) throws Exception {
            var dry = CrashFileSystem.create();
            workload(dry, mode);
            for (var stop = 0L; stop <= dry.changes(); stop++) {
                var disk = CrashFileSystem.create();
                disk.stopAfter(stop);
                workload(disk, mode);
                var crashed = disk.afterProcessCrash();
                var hold = new CountDownLatch(1);
                try (var ledger = open(crashed.getPath("/store"), mode)) {
                    holdPreparations(crashed, hold);
                    Map<String, String> recovered = new HashMap<>(contents(ledger));
                    commit(ledger, recovered, "after");
                    survivesPowerLoss(crashed, recovered);
                    // Before the close, which waits for the preparation
                    hold.countDown();
                } catch (AssertionError failure) {
                    throw new AssertionError("Crash at change " + stop + " of " + dry.changes(), failure);
                } finally {
                    hold.countDown();
                }
            }
        }

        @ParameterizedTest(name = "write through: {0}")
        @EnumSource(value = WriteThrough.Mode.class, names = {"DSYNC", "OFF"})
        @DisplayName("the name of a segment a process prepared and never forced is forced before a commit depends on it")
        void nameOfEmptySegment(WriteThrough.Mode mode) throws Exception {
            var disk = CrashFileSystem.create();
            var prepared = new CountDownLatch(1);
            // The force of the directory after the rename of the first segment, where the process stops
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && isLogDirectory(path)) {
                    prepared.countDown();
                    throw new IOException("The process stopped here");
                }
            });
            var stopped = open(disk.getPath("/store"), mode);
            await(prepared);
            var crashed = disk.afterProcessCrash();
            disk.intercept((_, _) -> {
            });
            try {
                stopped.close();
            } catch (RuntimeException _) {
                // The preparation failed, which the close reports
            }
            var hold = new CountDownLatch(1);
            var model = new TreeMap<String, String>();
            try (var ledger = open(crashed.getPath("/store"), mode)) {
                crashed.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.FORCE && (isLogDirectory(path) || isPreparing(path))) {
                        await(hold);
                    }
                });
                try {
                    for (var index = 0; index < 5; index++) {
                        commit(ledger, model, "w" + index);
                    }
                    survivesPowerLoss(crashed, model);
                } finally {
                    hold.countDown();
                }
            }
        }
    }

    @Nested
    @DisplayName("the segment a close keeps")
    class Kept {
        @ParameterizedTest(name = "write through: {0}")
        @EnumSource(value = WriteThrough.Mode.class, names = {"DSYNC", "OFF"})
        @DisplayName("spares the next open every force, and its commits still survive a loss of power")
        void cleanOpenForcesNothing(WriteThrough.Mode mode) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), mode)) {
                commit(ledger, model, "first");
            }
            var forces = disk.forces();
            try (var ledger = open(disk.getPath("/store"), mode)) {
                assertEquals(forces, disk.forces(), "a clean open forced a file");
                commit(ledger, model, "second");
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("has its name forced by an open that finds no mark, as a loss of power after the close may leave it")
        void markLost() throws Exception {
            var model = new TreeMap<String, String>();
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                commit(ledger, model, "first");
            }
            var unmarked = 0;
            for (var seed = 0; seed < 64; seed++) {
                var survivor = disk.afterPowerLoss(new Random(seed));
                try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.OFF)) {
                    unmarked += survivor.forces() > 0 ? 1 : 0;
                    var key = "second" + seed;
                    commit(ledger, model, key);
                    survivesPowerLoss(survivor, Map.of("first", model.get("first"), key, model.get(key)));
                }
            }
            assertTrue(unmarked > 0, "no loss of power dropped the mark");
        }

        @Test
        @DisplayName("takes the first group of the next open through")
        void firstGroupThrough() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                commit(ledger, model, "first");
                // The catalog of a new store may make a group before the commit's
                assertTrue(log(ledger).writtenThrough() > 0);
            }
            var hold = new CountDownLatch(1);
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                holdPreparations(disk, hold);
                try {
                    commit(ledger, model, "second");
                    assertEquals(1, log(ledger).writtenThrough());
                    survivesPowerLoss(disk, model);
                } finally {
                    hold.countDown();
                }
            }
        }

        @Test
        @DisplayName("moves the log one way down when the file system refuses the first write through, which no probe tried, and the group is still durable")
        void firstWriteRefused() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var refused = new AtomicBoolean();
            // The first force of a segment under its name is the force of that first write through
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && refused.compareAndSet(false, true)) {
                    throw new IOException("Invalid argument");
                }
            });
            var way = new WriteThrough(WriteThrough.Kind.DIRECT, null);
            try (var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES))
                    .segmentSize(SEGMENT)
                    .writeThrough(way)
                    .build()) {
                commit(ledger, model, "first");
                assertTrue(refused.get());
                assertEquals(WriteThrough.Kind.DSYNC, log(ledger).writeThroughKind());
                assertTrue(log(ledger).writtenThrough() > 0);
                survivesPowerLoss(disk, model);
            }
        }
    }

    @Nested
    @DisplayName("a store just opened")
    class JustOpened {
        // Far more groups to a segment than the test commits, so only a move between groups reaches the next one
        private static final long LONG_SEGMENT = 4L << 20;

        @ParameterizedTest(name = "reopened: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("writes through its first group, then every group once it moves to the next segment, which it does as soon as that is ready")
        void reachesWriteThrough(boolean reopened) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            if (reopened) {
                try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                    commit(ledger, model, "before");
                }
            }
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                var log = log(ledger);
                commit(ledger, model, "first");
                assertTrue(log.writtenThrough() > 0, "the first group took the first block");
                var segment = log.currentSegment();
                var commits = 0;
                while (log.currentSegment() == segment && commits < 200) {
                    commit(ledger, model, "w" + commits++);
                    Thread.sleep(5);
                }
                assertEquals(segment + 1, log.currentSegment(), "the log never moved to the next segment");
                var through = log.writtenThrough();
                commit(ledger, model, "x0");
                commit(ledger, model, "x1");
                assertEquals(through + 2, log.writtenThrough());
                assertEquals(segment + 1, log.currentSegment(), "the log moved on twice");
                survivesPowerLoss(disk, model);
            }
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                assertEquals(model, contents(ledger));
            }
        }

        @Test
        @DisplayName("that only commits without waiting stays on its segment and takes no zeros")
        void noWaitNoMove() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                var log = log(ledger);
                var segment = log.currentSegment();
                for (var index = 0; index < 40; index++) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + index, "v".repeat(500)));
                    Thread.sleep(1);
                }
                assertEquals(segment, log.currentSegment());
                var longest = 0L;
                try (var files = Files.list(disk.getPath("/store/log"))) {
                    for (var file : (Iterable<Path>) files::iterator) {
                        longest = Math.max(longest, Files.size(file));
                    }
                }
                assertTrue(longest < LONG_SEGMENT, "a segment was written with zeros");
            }
        }

        @Test
        @DisplayName("is closed without waiting for the zeros of a segment the close would cut off")
        void closeStopsFill() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                commit(ledger, model, "before");
            }
            var writing = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            var writes = new AtomicInteger();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".prep") && writes.getAndIncrement() == 0) {
                    writing.countDown();
                    await(release);
                }
            });
            var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT);
            commit(ledger, model, "first");
            await(writing);
            var closer = Thread.ofVirtual().start(ledger::close);
            // The close reached the preparation, so it told the zeros to stop
            while (closer.isAlive() && Arrays.stream(closer.getStackTrace()).noneMatch(frame -> frame.getMethodName().equals("keepNext"))) {
                Thread.sleep(1);
            }
            release.countDown();
            closer.join();
            // The write of the zeros it was held at, and the first blocks
            assertEquals(2, writes.get());
            try (var reopened = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC, LONG_SEGMENT)) {
                assertEquals(model, contents(reopened));
            }
        }
    }

    @Nested
    @DisplayName("a segment prepared from a spare")
    class Spares {
        private static LogFiles files(Path directory) throws IOException {
            Files.createDirectories(directory);
            return new LogFiles(directory, StoreKeys.derive(EncryptionSpec.NONE, new byte[16], 0), new byte[16]);
        }

        @Test
        @DisplayName("is written with zeros to its full length when asked, whatever length the spare was")
        void shortSpare() throws Exception {
            var directory = CrashFileSystem.create().getPath("/log");
            var files = files(directory);
            Files.write(directory.resolve("%016x.spare".formatted(1L)), new byte[3 * LogWriter.ALIGNMENT + 100]);
            var prepared = files.prepare(2, SEGMENT, true, true, () -> false);
            assertEquals(SEGMENT, prepared.valid());
            assertEquals(SEGMENT, Files.size(prepared.path()));
        }

        @Test
        @DisplayName("keeps only its first two blocks when a close stops the zeros")
        void stopped() throws Exception {
            var directory = CrashFileSystem.create().getPath("/log");
            var files = files(directory);
            Files.write(directory.resolve("%016x.spare".formatted(1L)), new byte[3 * LogWriter.ALIGNMENT]);
            var prepared = files.prepare(2, SEGMENT, true, true, () -> true);
            assertEquals(LogWriter.KEPT_LENGTH, prepared.valid());
            assertEquals(LogWriter.KEPT_LENGTH, Files.size(prepared.path()));
        }

        @Test
        @DisplayName("keeps its length when no zeros are asked for")
        void noZeros() throws Exception {
            var directory = CrashFileSystem.create().getPath("/log");
            var files = files(directory);
            Files.write(directory.resolve("%016x.spare".formatted(1L)), new byte[5 * LogWriter.ALIGNMENT]);
            assertEquals(5L * LogWriter.ALIGNMENT, files.prepare(2, SEGMENT, false, true, () -> false).valid());
        }
    }

    @Nested
    @DisplayName("on a file system that journals names as NTFS does")
    class Ntfs {
        /// Runs a store that writes through into spares, stops the process at the change `stop` lets through and then
        /// fails, once the preparation of the next segment is armed, and returns what the crash left.
        private static CrashFileSystem crash(CrashFileSystem disk, Map<String, String> model, ChangeInterceptor stop,
                                             CountDownLatch stopped) throws Exception {
            var armed = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (armed.get()) {
                    stop.beforeChange(path, kind);
                }
            });
            var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC);
            var log = log(ledger);
            var commits = 0;
            while (log.currentSegment() < 8) {
                commit(ledger, model, "w" + commits++);
            }
            ledger.checkpoint();
            armed.set(true);
            var segment = log.currentSegment();
            try {
                // The first group of the next segment starts the preparation of the one after it
                while (log.currentSegment() == segment && stopped.getCount() != 0) {
                    commit(ledger, model, "w" + commits++);
                }
            } catch (RuntimeException _) {
                // The preparation that stopped was the one this commit rolled over to
            }
            await(stopped);
            var crashed = disk.afterProcessCrash();
            disk.intercept((_, _) -> {
            });
            ledger.close();
            return crashed;
        }

        @Test
        @DisplayName("a group written through a segment prepared at the open survives a loss of power, its name included")
        void preparedAtOpen() throws Exception {
            var model = new TreeMap<String, String>();
            var stopped = new CountDownLatch(1);
            // The rename of a preparation, so the crash leaves its spare and no empty segment
            var crashed = crash(CrashFileSystem.createNtfs(), model, (path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.RENAME && isPreparing(path)) {
                    stopped.countDown();
                    throw new IOException("The process stopped here");
                }
            }, stopped);
            var renamed = new AtomicBoolean();
            var hold = new CountDownLatch(1);
            crashed.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.RENAME && isPreparing(path)) {
                    renamed.set(true);
                } else if (kind == ChangeInterceptor.ChangeKind.FORCE && renamed.get() && isPreparing(path)) {
                    await(hold);
                }
            });
            try (var ledger = open(crashed.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                try {
                    commit(ledger, model, "after");
                    assertEquals(1, log(ledger).writtenThrough());
                    survivesPowerLoss(crashed, model);
                } finally {
                    hold.countDown();
                }
            }
        }

        @Test
        @DisplayName("a segment a crash left prepared has its first group forced, whatever bytes it has to write it through")
        void leftPrepared() throws Exception {
            var model = new TreeMap<String, String>();
            var stopped = new CountDownLatch(1);
            var renamed = new AtomicBoolean();
            // The force of a segment after its rename, so the crash leaves it empty with no force behind its name
            var crashed = crash(CrashFileSystem.createNtfs(), model, (path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.RENAME && isPreparing(path)) {
                    renamed.set(true);
                } else if (kind == ChangeInterceptor.ChangeKind.FORCE && renamed.get() && path.toString().endsWith(".log")) {
                    stopped.countDown();
                    throw new IOException("The process stopped here");
                }
            }, stopped);
            var hold = new CountDownLatch(1);
            try (var ledger = open(crashed.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                holdPreparations(crashed, hold);
                try {
                    commit(ledger, model, "after");
                    assertEquals(0, log(ledger).writtenThrough(), "the group that names the segment was written through");
                    survivesPowerLoss(crashed, model);
                    commit(ledger, model, "after2");
                    assertEquals(1, log(ledger).writtenThrough());
                } finally {
                    hold.countDown();
                }
            }
        }

        @Test
        @DisplayName("a store that writes through keeps the name of the segment its close keeps durable, and the next open writes through it")
        void keptThrough() throws Exception {
            var disk = CrashFileSystem.createNtfs();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                commit(ledger, model, "first");
            }
            var hold = new CountDownLatch(1);
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                holdPreparations(disk, hold);
                try {
                    commit(ledger, model, "second");
                    assertEquals(1, log(ledger).writtenThrough());
                    survivesPowerLoss(disk, model);
                } finally {
                    hold.countDown();
                }
            }
        }

        @Test
        @DisplayName("a store that forces keeps a segment no force named, and the first group into it is forced, which names it")
        void keptForced() throws Exception {
            var disk = CrashFileSystem.createNtfs();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                commit(ledger, model, "first");
            }
            var forces = disk.forces();
            var hold = new CountDownLatch(1);
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                assertEquals(forces, disk.forces(), "a clean open forced a file");
                holdPreparations(disk, hold);
                try {
                    commit(ledger, model, "second");
                    assertEquals(0, log(ledger).writtenThrough());
                    survivesPowerLoss(disk, model);
                } finally {
                    hold.countDown();
                }
            }
        }
    }
}
