package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import com.sun.nio.file.ExtendedOpenOption;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The log's writes through: a group that one write holds is durable once the write returns, with no force after
/// it, on the file system that simulates failures and on the real one; an interrupt of the write has it written
/// again, and a write the file system refuses moves the store down a way or leaves it to forces.
///
/// The simulated file system makes the writes of a channel opened with `DSYNC` durable as they return, so a store
/// there writes through only when asked for [WriteThrough.Mode#DSYNC]. Past the first group of an open a store writes
/// through only into a segment that holds more than its first block, a spare or a segment written with zeros, so each
/// test first fills segments, checkpoints to turn them into spares, and commits until the log writes into one.
class WriteThroughTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Fifteen groups of a block each to a segment
    private static final long SEGMENT = 64 << 10;
    private static final int DRAWS = 12;

    private static Ledger open(Path directory, WriteThrough.Mode mode) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(mode)
                .build();
    }

    private static Ledger open(Path directory, WriteThrough way) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(way)
                .build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    /// Commits `key` with a value that seals into one block, durably, and records it in `model`.
    private static void commit(Ledger ledger, Map<String, String> model, String key) throws InterruptedException {
        var value = "v".repeat(500) + key;
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    /// Fills segments, makes them spares, and commits until the log writes a group through into one of them.
    ///
    /// @return the commits made, whose keys are `w0` onwards
    private static int reachWriteThrough(Ledger ledger, Map<String, String> model) throws InterruptedException {
        var log = log(ledger);
        var commits = 0;
        // Segments enough for a checkpoint to leave spares
        while (log.currentSegment() < 12) {
            commit(ledger, model, "w" + commits++);
        }
        ledger.checkpoint();
        for (var attempt = 0; attempt < 200 && log.writtenThrough() == 0; attempt++) {
            commit(ledger, model, "w" + commits++);
        }
        assertTrue(log.writtenThrough() > 0, "no group was written through");
        return commits;
    }

    private static Map<String, String> contents(Ledger ledger) {
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
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.OFF)) {
                var recovered = contents(ledger);
                for (var entry : model.entrySet()) {
                    assertEquals(Optional.of(entry.getValue()), Optional.ofNullable(recovered.get(entry.getKey())),
                            "seed " + seed + ", key " + entry.getKey());
                }
            }
        }
    }

    /// Commits the keys `k00000` onwards in order to a store that writes through, enough first to make spares and
    /// reach one, then commits of which two in three wait for durability; and what it learned while it ran: the
    /// commits it made and how many of them it knows durable. A stopped file system ends it.
    private static final class Workload {
        private final CrashFileSystem disk;
        int made;
        int durable;
        // The forces once the store writes into a spare, from which a stop is worth trying
        long warmForces;
        long writtenThrough;

        Workload(CrashFileSystem disk) {
            this.disk = disk;
        }

        /// {@return the contents after the first `commits` commits}
        static Map<String, String> state(int commits) {
            var state = new TreeMap<String, String>();
            for (var i = 0; i < commits; i++) {
                state.put(key(i), value(i));
            }
            return state;
        }

        private static String key(int commit) {
            return "k%05d".formatted(commit);
        }

        private static String value(int commit) {
            return "v".repeat(commit % 7 * 300) + commit;
        }

        void run() {
            Ledger ledger = null;
            try {
                ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC);
                var log = log(ledger);
                while (log.currentSegment() < 6) {
                    commit(ledger, true);
                }
                ledger.checkpoint();
                warmForces = disk.forces();
                // Forty that wait are forty groups even if each that does not wait shares the group of the next: the
                // segment prepared before the checkpoint left spares, and the rest of the current one, hold 29
                for (var i = 0; i < 60; i++) {
                    commit(ledger, i % 3 != 0);
                }
                writtenThrough = log.writtenThrough();
                ledger.close();
                durable = made;
            } catch (RuntimeException | InterruptedException _) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }

        private void commit(Ledger ledger, boolean awaitDurable) throws InterruptedException {
            var commit = ledger.commit(ledger.newBatch().put(NAMES, key(made), value(made)));
            made++;
            if (awaitDurable) {
                commit.awaitDurable();
                durable = made;
            }
        }
    }

    /// {@return a store that writes through and keeps values of 4 KiB and more in the file of large values}
    private static Ledger openLarge(Path directory) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).largeValues(4_096))
                .segmentSize(SEGMENT)
                .writeThrough(WriteThrough.Mode.DSYNC)
                .build();
    }

    /// Commits the keys `k00000` onwards durably, each value in the file of large values, to a store that writes
    /// through, enough first to make spares and reach one; and what it learned while it ran. A stopped file system ends
    /// it.
    private static final class LargeValues {
        private final CrashFileSystem disk;
        int made;
        int durable;
        long warmForces;
        long largeThrough;

        LargeValues(CrashFileSystem disk) {
            this.disk = disk;
        }

        /// {@return the contents after the first `commits` commits}
        static Map<String, String> state(int commits) {
            var state = new TreeMap<String, String>();
            for (var i = 0; i < commits; i++) {
                state.put(key(i), value(i));
            }
            return state;
        }

        private static String key(int commit) {
            return "k%05d".formatted(commit);
        }

        private static String value(int commit) {
            return commit + "x".repeat(5_000);
        }

        void run() {
            Ledger ledger = null;
            try {
                ledger = openLarge(disk.getPath("/store"));
                var log = log(ledger);
                while (log.currentSegment() < 6) {
                    commit(ledger);
                }
                ledger.checkpoint();
                warmForces = disk.forces();
                var through = log.writtenThrough();
                // Two segments and more, past the one prepared before the checkpoint left spares
                for (var i = 0; i < 40; i++) {
                    commit(ledger);
                }
                largeThrough = log.writtenThrough() - through;
                ledger.close();
                durable = made;
            } catch (RuntimeException | InterruptedException _) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // The file system stopped: closing only lets go of the threads
                    }
                }
            }
        }

        private void commit(Ledger ledger) throws InterruptedException {
            var commit = ledger.commit(ledger.newBatch().put(NAMES, key(made), value(made)));
            made++;
            commit.awaitDurable();
            durable = made;
        }
    }

    @Nested
    @DisplayName("on a file system that simulates failures")
    class Simulated {
        @Test
        @DisplayName("a group written through survives a loss of power right after its commit returns, with no force after it")
        void durableAtReturn() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                reachWriteThrough(ledger, model);
                // Only into a spare or a segment written with zeros, which the speed of the commits decides
                var written = 0;
                for (var i = 0; i < 200 && written < 4; i++) {
                    var through = log.writtenThrough();
                    var forces = log.forces();
                    commit(ledger, model, "last" + i);
                    if (log.writtenThrough() > through) {
                        written++;
                        assertEquals(forces + 1, log.forces(), "a group written through counts as one force");
                        survivesPowerLoss(disk, model);
                    }
                }
                assertEquals(4, written, "groups of the last commits written through");
            }
        }

        @Test
        @DisplayName("a group written through with a large value is durable with the file of large values, whichever force the power fails at")
        void largeValues() throws Exception {
            var dry = CrashFileSystem.create();
            var planned = new LargeValues(dry);
            planned.run();
            assertTrue(planned.largeThrough > 0, "no group with a large value was written through");
            for (var stop = planned.warmForces; stop <= dry.forces(); stop++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(stop);
                var workload = new LargeValues(disk);
                workload.run();
                var survivor = disk.afterPowerLoss(new Random(stop));
                try (var ledger = openLarge(survivor.getPath("/store"))) {
                    var recovered = contents(ledger);
                    var at = stop;
                    assertTrue(recovered.size() >= workload.durable && recovered.size() <= workload.made,
                            () -> "stop at force " + at + ": " + recovered.size() + " commits recovered, " + workload.durable + " to " + workload.made + " expected");
                    assertEquals(LargeValues.state(recovered.size()), recovered, "stop at force " + stop);
                }
            }
        }

        @Test
        @DisplayName("a store that forces its groups writes none through")
        void off() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 4) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                for (var i = 0; i < 60; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(0, log.writtenThrough());
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a store on a file system it knows nothing of writes through only when asked to")
        void autoOnOtherFileSystems() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.AUTO)) {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 4) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                for (var i = 0; i < 60; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(0, log.writtenThrough());
            }
            assertFalse(WriteThrough.of(WriteThrough.Mode.AUTO, disk.getPath("/store")).enabled());
            assertTrue(WriteThrough.of(WriteThrough.Mode.DIRECT, disk.getPath("/store")).enabled());
        }

        @Test
        @DisplayName("a commit whose write through an interrupt cuts short is durable, and the log goes on")
        void interrupted() throws Exception {
            var disk = CrashFileSystem.create();
            var committer = Thread.currentThread();
            var armed = new AtomicBoolean();
            var interrupts = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                // A write through is a write and then a force of the segment, on the thread of a commit that leads
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")
                        && Thread.currentThread() == committer && armed.compareAndSet(true, false)) {
                    // Not waited for with a join, which the interrupt would end before it reached the channel
                    var interrupter = Thread.ofPlatform().start(committer::interrupt);
                    while (interrupter.isAlive()) {
                        Thread.onSpinWait();
                    }
                    interrupts.incrementAndGet();
                }
            });
            var released = new CountDownLatch(1);
            try (var pool = IoPool.create(1);
                 var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool))
                         .segmentSize(SEGMENT)
                         .writeThrough(WriteThrough.Mode.DSYNC)
                         .build()) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                for (var i = 0; i < 60; i++) {
                    armed.set(true);
                    var key = "w" + commits++;
                    var value = "v".repeat(500) + key;
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                    try {
                        commit.awaitDurable();
                    } catch (InterruptedException _) {
                        // The pool wrote the group, and the commit took the interrupt while it waited
                        commit.awaitDurable();
                    }
                    assertTrue(commit.isDurable());
                    model.put(key, value);
                    Thread.interrupted();
                }
                armed.set(false);
                // A group written through well inside its segment, so the next ones go through too, each in one write
                while (true) {
                    var before = log.writtenThrough();
                    commit(ledger, model, "w" + commits++);
                    if (log.writtenThrough() > before && log.groupEnd() < SEGMENT / 2) {
                        break;
                    }
                }
                // A commit writes its own group only if it starts the flush before the pool's thread does, which all the
                // commits above can lose in a warm JVM
                occupy(pool, released);
                for (var i = 0; i < 4; i++) {
                    armed.set(true);
                    var key = "w" + commits++;
                    var value = "v".repeat(500) + key;
                    var through = log.writtenThrough();
                    var before = interrupts.get();
                    ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                    model.put(key, value);
                    Thread.interrupted();
                    assertEquals(before + 1, interrupts.get(), "the commit left its group to the pool");
                    assertEquals(through + 1, log.writtenThrough(), "the interrupt left the group to a force");
                }
                armed.set(false);
                released.countDown();
                var through = log.writtenThrough();
                for (var i = 0; i < 20 && log.writtenThrough() == through; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertTrue(log.writtenThrough() > through, "the interrupts moved the log to forces");
                survivesPowerLoss(disk, model);
            }
            Thread.interrupted();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                assertEquals(model, contents(ledger));
            }
        }

        /// Keeps the only thread of `pool` from any other task until `released` opens, and returns once it runs, with every
        /// task queued before it done. The thread takes its next task after a minute at most, so a commit that left its
        /// group to the pool fails an assertion rather than hanging the test.
        private static void occupy(IoPool pool, CountDownLatch released) throws InterruptedException {
            var occupied = new CountDownLatch(1);
            ((PlatformIoPool) pool).executor().execute(() -> {
                occupied.countDown();
                try {
                    released.await(1, TimeUnit.MINUTES);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            });
            occupied.await();
        }

        @Test
        @DisplayName("a write through the file system refuses has its group forced, and the store forces from then on")
        void refusedWrite() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var refused = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && armed.compareAndSet(true, false)) {
                    refused.incrementAndGet();
                    throw new IOException("Invalid argument");
                }
            });
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                // A group written through well inside its segment, so the next one goes through too
                while (true) {
                    var before = log.writtenThrough();
                    commit(ledger, model, "w" + commits++);
                    if (log.writtenThrough() > before && log.groupEnd() < SEGMENT / 2) {
                        break;
                    }
                }
                var through = log.writtenThrough();
                armed.set(true);
                commit(ledger, model, "refused");
                assertEquals(1, refused.get());
                assertEquals(through, log.writtenThrough());
                survivesPowerLoss(disk, model);
                for (var i = 0; i < 40; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(through, log.writtenThrough(), "a group was written through after a refused write");
                survivesPowerLoss(disk, model);
            }
        }

        @ParameterizedTest(name = "{0} refused")
        @CsvSource({"1, DSYNC, true", "2, , false"})
        @DisplayName("a first write through refused moves the log down one way, O_DIRECT to O_DSYNC to forces, with its group durable by the way that takes it")
        void downgrades(int refusals, WriteThrough.Kind expected, boolean writesThrough) throws Exception {
            var disk = CrashFileSystem.create();
            var refused = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                // The first force of a segment under its name is the first group's write through, whichever frames it
                // holds, as a segment is forced under another name while it is prepared; the next way down writes that
                // group next
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && refused.get() < refusals) {
                    refused.incrementAndGet();
                    throw new IOException("Invalid argument");
                }
            });
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DIRECT, null))) {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 4) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                var segment = log.currentSegment();
                while (log.currentSegment() < segment + 4) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(refusals, refused.get(), "first writes refused");
                assertEquals(expected, log.writeThroughKind());
                assertEquals(writesThrough, log.writtenThrough() > 0);
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a segment no group can be written through into takes no second handle, and the first group that can opens it")
        void opensOnDemand() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                // Too long for the first block of the first segment, which a group of the frames of the open may have taken
                var value = "v".repeat(5_000);
                ledger.commit(ledger.newBatch().put(NAMES, "past", value)).awaitDurable();
                model.put("past", value);
                var opened = way.opensForTests();
                var through = log.writtenThrough();
                assertEquals(through, opened);
                var commits = 0;
                // The first segment has no bytes past its first block, nor does the next, prepared without zeros
                while (log.currentSegment() < 2) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(opened, way.opensForTests());
                assertEquals(through, log.writtenThrough());
                var segments = new HashSet<Long>();
                while (log.currentSegment() < 6) {
                    commitNoting(ledger, model, "w" + commits++, segments);
                }
                ledger.checkpoint();
                for (var i = 0; i < 200 && segments.size() < 2; i++) {
                    commitNoting(ledger, model, "w" + commits++, segments);
                }
                assertFalse(segments.isEmpty(), "no group was written through");
                assertEquals(opened + segments.size(), way.opensForTests());
            }
            survivesPowerLoss(disk, model);
        }

        /// Commits `key` and adds the segment its group went to `segments`, if it was
        /// written through.
        private static void commitNoting(Ledger ledger, Map<String, String> model, String key, Set<Long> segments) throws InterruptedException {
            var log = log(ledger);
            var through = log.writtenThrough();
            commit(ledger, model, key);
            if (log.writtenThrough() > through) {
                segments.add(log.currentSegment());
            }
        }

        @Test
        @DisplayName("only groups reach a segment durably: a way is tried by the first group it writes, with no write of its own in any segment")
        void noOtherWrites() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var forced = new AtomicInteger();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                    forced.incrementAndGet();
                }
            });
            Ledger opened;
            // No zeros, whose force of a segment under its name would count, so the spares of the checkpoint have the bytes
            var previous = System.getProperty("ledger.log.zeroFill");
            System.setProperty("ledger.log.zeroFill", "off");
            try {
                opened = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC);
            } finally {
                if (previous == null) {
                    System.clearProperty("ledger.log.zeroFill");
                } else {
                    System.setProperty("ledger.log.zeroFill", previous);
                }
            }
            try (var ledger = opened) {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 8) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                var segment = log.currentSegment();
                var forces = log.forces();
                var seen = forced.get();
                var through = log.writtenThrough();
                while (log.currentSegment() < segment + 4) {
                    commit(ledger, model, "w" + commits++);
                }
                assertTrue(log.writtenThrough() > through, "no group was written through");
                assertEquals(log.forces() - forces, forced.get() - seen, "forces of the segments beside those of the groups");
            }
            survivesPowerLoss(disk, model);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Kind.class, names = {"DIRECT", "DSYNC"})
        @DisplayName("a first write through with no native memory for its buffer fails its way and every way below, not the log: the group is forced")
        void bufferRefused(WriteThrough.Kind kind) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var way = new WriteThrough(kind, null);
            way.allocatorForTests = _ -> {
                throw new OutOfMemoryError("Cannot reserve direct buffer memory");
            };
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                commit(ledger, model, "first");
                assertEquals(0, log.writtenThrough());
                assertNull(log.writeThroughKind());
                survivesPowerLoss(disk, model);
                commit(ledger, model, "second");
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a later write through with no native memory for a larger buffer leaves the store to forces, and the log goes on")
        void largerBufferRefused() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                way.allocatorForTests = _ -> {
                    throw new OutOfMemoryError("Cannot reserve direct buffer memory");
                };
                // A group larger than the smallest output, which its buffer follows, once one lands where it is written through
                var value = "v".repeat(20_000);
                for (var i = 0; i < 200 && log.writeThroughKind() != null; i++) {
                    var key = "w" + commits++;
                    ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                    model.put(key, value);
                }
                assertNull(log.writeThroughKind());
                var through = log.writtenThrough();
                commit(ledger, model, "after");
                assertEquals(through, log.writtenThrough());
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("a way nothing refuses stays the way the log writes through in as segments roll")
        void keepsWay() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DIRECT, null))) {
                reachWriteThrough(ledger, model);
                assertEquals(WriteThrough.Kind.DIRECT, log(ledger).writeThroughKind());
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a device that does not prove a write through holds has every group forced, and is asked once, only when a segment needs the answer")
        void unproven() throws Exception {
            var disk = CrashFileSystem.create();
            var asked = new AtomicInteger();
            var model = new TreeMap<String, String>();
            // Reopened after a commit, as a new store queues a frame of its key spaces, which the pool may write before
            // the open returns; a close with no commit writes no checkpoint and retires that frame's segment, so the
            // reopen would queue the frame again
            try (var created = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                commit(created, model, "created");
            }
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DSYNC, () -> {
                asked.incrementAndGet();
                return false;
            }))) {
                var log = log(ledger);
                assertEquals(0, log.appendedBytes(), "the reopened store queued a frame");
                assertEquals(0, asked.get(), "the device was asked by opening a store");
                var commits = 0;
                while (log.currentSegment() < 4) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                var segment = log.currentSegment();
                while (log.currentSegment() < segment + 4) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(1, asked.get());
                assertEquals(0, log.writtenThrough());
                assertNull(log.writeThroughKind());
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a device that proves a write through holds has groups written through")
        void proven() throws Exception {
            var disk = CrashFileSystem.create();
            var asked = new AtomicInteger();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DSYNC, () -> {
                asked.incrementAndGet();
                return true;
            }))) {
                reachWriteThrough(ledger, model);
                assertEquals(1, asked.get());
                assertEquals(WriteThrough.Kind.DSYNC, log(ledger).writeThroughKind());
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("recovers a prefix of the commits from the last one acknowledged durable, whichever force or write through the power fails at")
        void everyForce() throws Exception {
            var dry = CrashFileSystem.create();
            var planned = new Workload(dry);
            planned.run();
            assertTrue(planned.writtenThrough > 0, "the workload wrote no group through");
            for (var stop = planned.warmForces; stop <= dry.forces(); stop++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(stop);
                var workload = new Workload(disk);
                workload.run();
                var survivor = disk.afterPowerLoss(new Random(stop));
                try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                    var recovered = contents(ledger);
                    var count = recovered.size();
                    var at = stop;
                    assertTrue(count >= workload.durable && count <= workload.made,
                            () -> "stop at force " + at + ": " + count + " commits recovered, " + workload.durable + " to " + workload.made + " expected");
                    assertEquals(Workload.state(count), recovered, "stop at force " + stop);
                    ledger.commit(ledger.newBatch().put(NAMES, "~after", "v")).awaitDurable();
                }
                try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                    assertEquals(Optional.of("v"), ledger.get(NAMES, "~after"));
                }
            }
        }

        @Test
        @DisplayName("concurrent committers share writes through as they share forces, and every acknowledged commit survives")
        void concurrent() throws Exception {
            var disk = CrashFileSystem.create();
            // A millisecond a force of the log, so the commits that arrive meanwhile make the next group
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            var model = new ConcurrentHashMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var warm = new TreeMap<String, String>();
                reachWriteThrough(ledger, warm);
                model.putAll(warm);
                var through = log.writtenThrough();
                var forces = log.forces();
                var results = new ArrayList<Future<?>>();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (var committer = 0; committer < 8; committer++) {
                        var name = "c" + committer + "-";
                        results.add(executor.submit(() -> {
                            for (var i = 0; i < 60; i++) {
                                var key = name + i;
                                var value = "x".repeat(i * 7) + key;
                                ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                                model.put(key, value);
                            }
                            return null;
                        }));
                    }
                }
                for (var result : results) {
                    result.get();
                }
                assertTrue(log.writtenThrough() > through, "no group of the committers was written through");
                var made = log.forces() - forces;
                assertTrue(made < 8 * 60, () -> made + " forces for " + 8 * 60 + " commits");
                survivesPowerLoss(disk, new TreeMap<>(model));
            }
        }
    }

    @Nested
    @DisplayName("on the default file system")
    class Real {
        @ParameterizedTest(name = "{0}")
        @EnumSource(WriteThrough.Mode.class)
        @DisplayName("commits written through or forced survive a crash and a close, and the platform writes through where it can")
        void survives(WriteThrough.Mode mode, @TempDir Path directory) throws Exception {
            var model = new TreeMap<String, String>();
            var ledger = open(directory, mode);
            long through;
            try {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 4) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                var segment = log.currentSegment();
                while (log.currentSegment() < segment + 3) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.commit(ledger.newBatch().put(NAMES, "large", "L".repeat(200_000))).awaitDurable();
                model.put("large", "L".repeat(200_000));
                for (var i = 0; i < 10; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                through = log.writtenThrough();
            } finally {
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            if (expectsWriteThrough(mode, directory)) {
                assertTrue(through > 0, "no group was written through");
            } else {
                assertEquals(0, through);
            }
            try (var reopened = open(directory, mode)) {
                assertEquals(model, contents(reopened));
                commit(reopened, model, "after");
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows a segment is opened unbuffered and written through, and its writes land where they name")
        void unbuffered(@TempDir Path directory) throws Throwable {
            Assumptions.assumeTrue(WriteThrough.class.getModule().isNativeAccessEnabled(), "the tests run without native access");
            var file = directory.resolve("segment");
            Files.write(file, new byte[16_384]);
            var handle = WriteThrough.of(WriteThrough.Mode.DIRECT, directory).open(file);
            try {
                var field = handle.getClass().getDeclaredField("handle");
                field.setAccessible(true);
                var opened = (MemorySegment) field.get(handle);
                var query = Linker.nativeLinker().downcallHandle(
                        SymbolLookup.libraryLookup("ntdll", Arena.global()).findOrThrow("NtQueryInformationFile"),
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
                try (var arena = Arena.ofConfined()) {
                    var status = arena.allocate(16, 8);
                    var information = arena.allocate(ValueLayout.JAVA_INT);
                    // FileModeInformation, class 16: FILE_WRITE_THROUGH is 0x2, FILE_NO_INTERMEDIATE_BUFFERING 0x8
                    assertEquals(0, (int) query.invokeExact(opened, status, information, 4, 16));
                    var mode = information.get(ValueLayout.JAVA_INT, 0);
                    assertEquals(0x2, mode & 0x2, "written through");
                    assertEquals(0x8, mode & 0x8, "unbuffered");
                }
                var bytes = new byte[8_192];
                Arrays.fill(bytes, (byte) 7);
                handle.write(bytes, bytes.length, 16_384, 4_096);
            } finally {
                handle.close();
            }
            var written = Files.readAllBytes(file);
            assertEquals(16_384, written.length);
            assertEquals(0, written[4_095]);
            assertEquals(7, written[4_096]);
            assertEquals(7, written[12_287]);
            assertEquals(0, written[12_288]);
        }

        @Test
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows the default mode writes through exactly where the device under the store proves it holds")
        void windowsProof(@TempDir Path directory) {
            Assumptions.assumeTrue(WriteThrough.class.getModule().isNativeAccessEnabled(), "the tests run without native access");
            var cache = WriteThrough.WriteCache.of(directory);
            // The answer of the runner's device says what the default arm of a benchmark there can show
            System.out.println("The write cache of the device under " + directory + ": " + cache);
            var proved = cache != null && cache.provesWriteThrough();
            assertEquals(proved, WriteThrough.of(WriteThrough.Mode.AUTO, directory).enabled());
            assertTrue(WriteThrough.of(WriteThrough.Mode.DIRECT, directory).enabled(), "direct does not ask the device");
        }

        @Test
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows a store that forces takes no zeros for its new segments")
        void windowsForcesWithoutZeros(@TempDir Path directory) throws Exception {
            var model = new TreeMap<String, String>();
            try (var ledger = open(directory, new WriteThrough(null, null))) {
                var log = log(ledger);
                var commits = 0;
                while (log.currentSegment() < 6) {
                    commit(ledger, model, "w" + commits++);
                }
                commit(ledger, model, "w" + commits);
                var current = directory.resolve("log").resolve(LogWriter.segmentName(log.currentSegment()));
                assertTrue(Files.size(current) < SEGMENT, "the segment was written with zeros");
                assertEquals(0, log.writtenThrough());
            }
        }

        @Test
        @DisabledOnOs(OS.MAC)
        @DisplayName("a store whose commits wait writes through into new segments written with zeros, before any checkpoint leaves a spare")
        void freshSegments(@TempDir Path directory) throws Exception {
            var model = new TreeMap<String, String>();
            // Direct asks no device, so this runs whatever the device under the temporary directory reports
            try (var ledger = open(directory, WriteThrough.Mode.DIRECT)) {
                var log = log(ledger);
                Assumptions.assumeTrue(log.writeThroughKind() != null, "the tests run without native access");
                var commits = 0;
                while (log.currentSegment() < 8) {
                    commit(ledger, model, "w" + commits++);
                }
                assertTrue(log.writtenThrough() > 0, "no group was written through");
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux the default mode opens a segment with O_DIRECT and O_DSYNC, and the way down with O_DSYNC alone")
        void linuxHandles(@TempDir Path directory) throws Exception {
            Assumptions.assumeTrue(acceptsDirect(directory), "the file system refuses O_DIRECT");
            var file = directory.resolve("segment");
            Files.write(file, new byte[16_384]);
            var through = WriteThrough.of(WriteThrough.Mode.AUTO, directory);
            assertEquals(WriteThrough.Kind.DIRECT, through.kind());
            var handle = through.open(file);
            try {
                var flags = openFlags(file);
                assertEquals(1, flags.size());
                assertEquals(oDirect(), flags.getFirst() & oDirect(), "O_DIRECT");
                assertEquals(O_DSYNC, flags.getFirst() & O_DSYNC, "O_DSYNC");
            } finally {
                handle.close();
            }
            through.downgrade();
            assertEquals(WriteThrough.Kind.DSYNC, through.kind());
            handle = through.open(file);
            try {
                var flags = openFlags(file);
                assertEquals(1, flags.size());
                assertEquals(0, flags.getFirst() & oDirect(), "O_DIRECT");
                assertEquals(O_DSYNC, flags.getFirst() & O_DSYNC, "O_DSYNC");
            } finally {
                handle.close();
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux the default mode stays on O_DIRECT as a store rolls segments, and the segment it writes is open that way")
        void linuxStore(@TempDir Path directory) throws Exception {
            Assumptions.assumeTrue(acceptsDirect(directory), "the file system refuses O_DIRECT");
            var model = new TreeMap<String, String>();
            try (var ledger = open(directory, WriteThrough.Mode.AUTO)) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                var rolled = log.currentSegment();
                while (log.currentSegment() < rolled + 3) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(WriteThrough.Kind.DIRECT, log.writeThroughKind(), "the log left O_DIRECT");
                var checked = false;
                for (var attempt = 0; attempt < 200 && !checked; attempt++) {
                    var through = log.writtenThrough();
                    var segment = log.currentSegment();
                    commit(ledger, model, "w" + commits++);
                    if (log.writtenThrough() > through && log.currentSegment() == segment) {
                        var file = directory.resolve("log").resolve(LogWriter.segmentName(segment));
                        checked = openFlags(file).stream().anyMatch(flags -> (flags & oDirect()) != 0 && (flags & O_DSYNC) != 0);
                        assertTrue(checked, "the segment written through is not open with O_DIRECT and O_DSYNC");
                    }
                }
                assertTrue(checked, "no group was written through inside a segment");
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        // The octal values of <fcntl.h>, as /proc/<pid>/fdinfo prints them
        private static final long O_DSYNC = 0_10000;

        private static long oDirect() {
            return System.getProperty("os.arch", "").equals("aarch64") ? 0_200000 : 0_40000;
        }

        /// {@return whether a file in `directory` takes an aligned write with `O_DIRECT`}
        private static boolean acceptsDirect(Path directory) {
            var file = directory.resolve("direct-probe");
            try {
                Files.write(file, new byte[8_192]);
                try (var channel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC, ExtendedOpenOption.DIRECT)) {
                    var block = ByteBuffer.allocateDirect(2 * LogWriter.ALIGNMENT).alignedSlice(LogWriter.ALIGNMENT).limit(LogWriter.ALIGNMENT);
                    channel.write(block, 0);
                }
                Files.delete(file);
                return true;
            } catch (IOException | UnsupportedOperationException _) {
                return false;
            }
        }

        /// {@return the flags this process holds `file` open with, one for each descriptor}
        private static List<Long> openFlags(Path file) throws IOException {
            var target = file.toRealPath();
            var flags = new ArrayList<Long>();
            try (var descriptors = Files.list(Path.of("/proc/self/fd"))) {
                for (var descriptor : descriptors.toList()) {
                    Path link;
                    try {
                        link = Files.readSymbolicLink(descriptor);
                    } catch (IOException _) {
                        // The descriptor of the listing itself, or one closed meanwhile
                        continue;
                    }
                    if (link.equals(target)) {
                        for (var line : Files.readAllLines(Path.of("/proc/self/fdinfo").resolve(descriptor.getFileName()))) {
                            if (line.startsWith("flags:")) {
                                flags.add(Long.parseLong(line.substring("flags:".length()).trim(), 8));
                            }
                        }
                    }
                }
            }
            return flags;
        }

        /// {@return whether this platform writes through in `mode` on the default file system, in `directory`}
        private static boolean expectsWriteThrough(WriteThrough.Mode mode, Path directory) {
            var os = System.getProperty("os.name", "");
            if (mode == WriteThrough.Mode.OFF || os.startsWith("Mac")) {
                return false;
            }
            if (os.startsWith("Windows")) {
                if (mode == WriteThrough.Mode.DSYNC) {
                    return true;
                }
                var nativeAccess = WriteThrough.class.getModule().isNativeAccessEnabled();
                return nativeAccess && (mode == WriteThrough.Mode.DIRECT || WriteThrough.WriteCache.proves(directory));
            }
            return os.equals("Linux") || mode != WriteThrough.Mode.AUTO;
        }
    }

    @Nested
    @DisplayName("what a Windows device reports")
    class Reports {
        @ParameterizedTest(name = "cache {0}, write through {1}, non-volatile {2}, {3} bytes: {4}")
        @CsvSource({
                // Unknown, unknown, volatile
                "0, 0, false, 28, false",
                // A volatile cache on, forced unit access unknown or unsupported
                "2, 0, false, 28, false",
                "2, 1, false, 28, false",
                // Forced unit access supported
                "2, 2, false, 28, true",
                "0, 2, false, 28, true",
                // The cache off
                "1, 0, false, 28, true",
                "1, 1, false, 28, true",
                // The cache non-volatile
                "2, 1, true, 28, true",
                // A structure cut before the non-volatile flag proves nothing with it
                "2, 1, true, 26, false",
                "2, 2, false, 24, true",
                // Too short to say anything
                "1, 2, true, 23, false",
                "1, 2, true, 0, false"
        })
        @DisplayName("proves a write through holds only for forced unit access, a cache off or a non-volatile cache")
        void proves(int enabled, int writeThrough, boolean nonVolatile, int returned, boolean expected) {
            try (var arena = Arena.ofConfined()) {
                var property = arena.allocate(28, Integer.BYTES);
                property.set(ValueLayout.JAVA_INT, 12, enabled);
                property.set(ValueLayout.JAVA_INT, 20, writeThrough);
                property.set(ValueLayout.JAVA_BYTE, 26, (byte) (nonVolatile ? 1 : 0));
                var cache = WriteThrough.WriteCache.parse(property, returned);
                assertEquals(expected, cache != null && cache.provesWriteThrough());
            }
        }
    }

    @Nested
    @DisplayName("the ways down")
    class Ways {
        @Test
        @DisplayName("go from O_DIRECT to O_DSYNC to forces, and open nothing after the last")
        void direct() {
            var through = new WriteThrough(WriteThrough.Kind.DIRECT, null);
            assertEquals(WriteThrough.Kind.DIRECT, through.kind());
            through.downgrade();
            assertEquals(WriteThrough.Kind.DSYNC, through.kind());
            assertTrue(through.enabled());
            through.downgrade();
            assertNull(through.kind());
            assertFalse(through.enabled());
            assertThrows(IllegalStateException.class, () -> through.open(Path.of("segment")));
        }

        @Test
        @DisplayName("go from unbuffered on Windows straight to forces: nothing below it writes through the drive's cache")
        void unbuffered() {
            var through = new WriteThrough(WriteThrough.Kind.UNBUFFERED, null);
            through.downgrade();
            assertNull(through.kind());
            assertFalse(through.enabled());
        }

        @Test
        @DisplayName("are given up unasked when the proof answers no, and the proof is asked once")
        void proof() {
            var asked = new AtomicInteger();
            var through = new WriteThrough(WriteThrough.Kind.UNBUFFERED, () -> {
                asked.incrementAndGet();
                return false;
            });
            assertEquals(0, asked.get());
            assertFalse(through.enabled());
            assertFalse(through.enabled());
            assertEquals(1, asked.get());
            assertThrows(IllegalStateException.class, () -> through.open(Path.of("segment")));
        }
    }

    @Nested
    @DisplayName("the zeros of a new segment")
    class Zeros {
        private static boolean zeroFills(String value, Path directory, boolean writesThrough) {
            var previous = System.getProperty("ledger.log.zeroFill");
            try {
                if (value == null) {
                    System.clearProperty("ledger.log.zeroFill");
                } else {
                    System.setProperty("ledger.log.zeroFill", value);
                }
                return new LogFiles(directory, null, null).zeroFills(writesThrough);
            } finally {
                if (previous == null) {
                    System.clearProperty("ledger.log.zeroFill");
                } else {
                    System.setProperty("ledger.log.zeroFill", previous);
                }
            }
        }

        @ParameterizedTest(name = "writes through: {0}")
        @CsvSource({"true", "false"})
        @DisplayName("follow ledger.log.zeroFill, and by default on Windows only a log that writes through")
        void policy(boolean writesThrough, @TempDir Path directory) throws Exception {
            var windows = System.getProperty("os.name", "").startsWith("Windows");
            var expected = !windows || writesThrough;
            assertEquals(expected, zeroFills(null, directory, writesThrough));
            assertEquals(expected, zeroFills("auto", directory, writesThrough));
            assertTrue(zeroFills("everywhere", directory, writesThrough));
            assertFalse(zeroFills("off", directory, writesThrough));
            assertTrue(zeroFills(null, CrashFileSystem.create().getPath("/store"), writesThrough), "a simulated file system runs the same everywhere");
            assertThrows(IllegalArgumentException.class, () -> zeroFills("always", directory, writesThrough));
        }
    }

    @Nested
    @DisplayName("the mode")
    class Modes {
        @Test
        @DisplayName("is read from ledger.log.writeThrough, auto when it is unset, and refuses a value it does not know")
        void property() {
            var previous = System.getProperty("ledger.log.writeThrough");
            try {
                System.clearProperty("ledger.log.writeThrough");
                assertEquals(WriteThrough.Mode.AUTO, WriteThrough.Mode.fromProperty());
                Set<WriteThrough.Mode> read = new HashSet<>();
                for (var value : new String[]{"auto", "OFF", " dsync ", "Direct"}) {
                    System.setProperty("ledger.log.writeThrough", value);
                    read.add(WriteThrough.Mode.fromProperty());
                }
                assertEquals(Set.of(WriteThrough.Mode.values()), read);
                System.setProperty("ledger.log.writeThrough", "always");
                assertThrows(IllegalArgumentException.class, WriteThrough.Mode::fromProperty);
            } finally {
                if (previous == null) {
                    System.clearProperty("ledger.log.writeThrough");
                } else {
                    System.setProperty("ledger.log.writeThrough", previous);
                }
            }
        }
    }

    @Nested
    @DisplayName("the answers of devices")
    class DeviceAnswers {
        // WRITE_CACHE_ENABLE: 1 disabled, 2 enabled; WRITE_THROUGH: 1 not supported, 2 supported
        private static final WriteThrough.WriteCache FUA = new WriteThrough.WriteCache(2, 2, false);
        private static final WriteThrough.WriteCache NON_VOLATILE = new WriteThrough.WriteCache(2, 1, true);
        private static final WriteThrough.WriteCache VOLATILE = new WriteThrough.WriteCache(2, 1, false);
        private static final WriteThrough.WriteCache TURNED_OFF = new WriteThrough.WriteCache(1, 1, false);

        private record Counted(WriteThrough.Answers answers, Map<String, AtomicInteger> asked) {
        }

        /// Answers whose directories are named after their volumes, the first name of their path, but `unnamed`.
        private static Counted answers(Map<String, WriteThrough.WriteCache> devices) {
            var asked = new ConcurrentHashMap<String, AtomicInteger>();
            var answers = new WriteThrough.Answers(directory -> {
                var name = directory.getName(0).toString();
                return name.equals("unnamed") ? null : name;
            }, volume -> {
                asked.computeIfAbsent(volume, _ -> new AtomicInteger()).incrementAndGet();
                return devices.get(volume);
            });
            return new Counted(answers, asked);
        }

        private static int asked(Counted counted, String volume) {
            var count = counted.asked().get(volume);
            return count == null ? 0 : count.get();
        }

        @Test
        @DisplayName("keep for the process an answer that rests on what the device is, or that proves nothing")
        void lasting() {
            var counted = answers(Map.of("fua", FUA, "nonVolatile", NON_VOLATILE, "volatile", VOLATILE));
            for (var store = 0; store < 3; store++) {
                assertTrue(counted.answers().proves(Path.of("fua", "store" + store)));
                assertTrue(counted.answers().proves(Path.of("nonVolatile", "store" + store)));
                assertFalse(counted.answers().proves(Path.of("volatile", "store" + store)));
            }
            assertEquals(1, asked(counted, "fua"));
            assertEquals(1, asked(counted, "nonVolatile"));
            assertEquals(1, asked(counted, "volatile"));
            assertEquals(3, counted.answers().asks());
        }

        @Test
        @DisplayName("ask again for each store where only a cache turned off proves it, or the device did not answer")
        void asksAgain() {
            var devices = new ConcurrentHashMap<String, WriteThrough.WriteCache>();
            devices.put("off", TURNED_OFF);
            var counted = answers(devices);
            for (var store = 0; store < 3; store++) {
                assertTrue(counted.answers().proves(Path.of("off", "store" + store)));
                assertFalse(counted.answers().proves(Path.of("silent", "store" + store)));
            }
            assertEquals(3, asked(counted, "off"));
            assertEquals(3, asked(counted, "silent"));
            // The cache turned back on, or a device that answers at last, reaches the next store
            devices.put("off", VOLATILE);
            devices.put("silent", FUA);
            assertFalse(counted.answers().proves(Path.of("off", "store")));
            assertTrue(counted.answers().proves(Path.of("silent", "store")));
            assertTrue(counted.answers().proves(Path.of("silent", "store")));
            assertEquals(4, asked(counted, "silent"));
        }

        @Test
        @DisplayName("prove nothing for a directory whose volume cannot be named, without asking any device")
        void unnamed() {
            var counted = answers(Map.of("fua", FUA));
            assertFalse(counted.answers().proves(Path.of("unnamed", "store")));
            assertEquals(0, counted.answers().asks());
        }

        @Test
        @DisplayName("keep each volume's answer apart, and ask again once they are forgotten")
        void perVolume() {
            var counted = answers(Map.of("fua", FUA, "volatile", VOLATILE));
            assertTrue(counted.answers().proves(Path.of("fua", "a")));
            assertFalse(counted.answers().proves(Path.of("volatile", "b")));
            counted.answers().clear();
            assertEquals(2, counted.answers().asks());
            assertTrue(counted.answers().proves(Path.of("fua", "c")));
            assertEquals(2, asked(counted, "fua"));
            assertEquals(1, asked(counted, "volatile"));
        }

        @Test
        @DisplayName("give stores asking beside each other the same answer, and keep it for the later ones")
        void concurrent() throws Exception {
            var counted = answers(Map.of("fua", FUA));
            var start = new CountDownLatch(1);
            try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
                var answers = new ArrayList<Future<Boolean>>();
                for (var store = 0; store < 16; store++) {
                    var directory = Path.of("fua", "store" + store);
                    answers.add(threads.submit(() -> {
                        start.await();
                        return counted.answers().proves(directory);
                    }));
                }
                start.countDown();
                for (var answer : answers) {
                    assertTrue(answer.get(60, TimeUnit.SECONDS));
                }
            }
            var before = asked(counted, "fua");
            assertTrue(before >= 1 && before <= 16, "asked " + before + " times");
            assertTrue(counted.answers().proves(Path.of("fua", "later")));
            assertEquals(before, asked(counted, "fua"));
        }

        @Test
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows a device whose answer lasts is asked once by the stores of a process, and only when a segment needs it")
        void windowsOnce(@TempDir Path directory) {
            Assumptions.assumeTrue(WriteThrough.class.getModule().isNativeAccessEnabled(), "the tests run without native access");
            var volume = WriteThrough.WriteCache.volumeOf(directory);
            Assumptions.assumeTrue(volume != null, "the volume under the temporary directory has no name");
            assertTrue(volume.startsWith("\\\\?\\Volume{") && volume.endsWith("}"), volume);
            var cache = WriteThrough.WriteCache.ofVolume(volume);
            Assumptions.assumeTrue(cache != null, "the device under the temporary directory does not answer");
            WriteThrough.WriteCache.clearForTests();
            var asks = WriteThrough.WriteCache.asksForTests();
            var first = WriteThrough.of(WriteThrough.Mode.AUTO, directory);
            var second = WriteThrough.of(WriteThrough.Mode.AUTO, directory);
            assertEquals(asks, WriteThrough.WriteCache.asksForTests(), "the device was asked before a segment needed it");
            assertEquals(cache.provesWriteThrough(), first.enabled());
            assertEquals(cache.provesWriteThrough(), second.enabled());
            assertEquals(cache.lastsForProcess() ? 1 : 2, WriteThrough.WriteCache.asksForTests() - asks);
        }
    }

    @Nested
    @DisplayName("the output")
    class Output {
        // Room for a group of the most one write takes, and a megabyte of filler, after a spare's first group
        private static final long SEGMENT = 4 << 20;
        private static final int WRITES = 256;

        private static Ledger open(Path directory, boolean encrypted, WriteThrough.Mode mode) {
            var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none()).keyspace(NAMES);
            return ((LedgerBuilder) builder).segmentSize(SEGMENT).writeThrough(mode).build();
        }

        /// Commits `writes` values of `length` bytes, the last `extra` bytes longer, under the keys `prefix000`
        /// onwards, in one batch, durably, and records them in `model`.
        private static void commitBatch(Ledger ledger, Map<String, String> model, String prefix, int writes, int length, int extra) throws InterruptedException {
            var batch = ledger.newBatch();
            for (var i = 0; i < writes; i++) {
                var key = prefix + "%03d".formatted(i);
                var value = "v".repeat(i == writes - 1 ? length + extra : length);
                batch.put(NAMES, key, value);
                model.put(key, value);
            }
            ledger.commit(batch).awaitDurable();
        }

        /// {@return the bytes the frames of the last group sealed into}
        private static long lastGroup(LogWriter log) {
            return log.groupEnd() - log.groupStart();
        }

        /// Commits until the next group starts in a segment it may be written through into with a group of
        /// [LogWriter#ONE_WRITE] bytes: a small group was just written through, and the segment holds that many more,
        /// their padding and room to spare.
        private static void reachRoom(Ledger ledger, Map<String, String> model, String prefix) throws InterruptedException {
            var log = log(ledger);
            for (var attempt = 0; attempt < 400; attempt++) {
                var through = log.writtenThrough();
                commit(ledger, model, prefix + attempt);
                var next = (log.groupEnd() + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT;
                // A spare holds the bytes of its last use, which may end a group of filler short of the segment's size
                if (log.writtenThrough() > through && next + LogWriter.ONE_WRITE + 2L * LogWriter.ALIGNMENT <= SEGMENT - (512 << 10)) {
                    return;
                }
                // On to the next segment, and to spares again once those the checkpoint left are taken
                commitBatch(ledger, model, prefix + attempt + "-", 128, 4_000, 0);
                if (attempt % 16 == 15) {
                    ledger.checkpoint();
                }
            }
            throw new AssertionError("no segment left room to write a group of the most one write takes through");
        }

        /// Checks that every state a loss of power now could leave of `disk` holds `model`.
        private static void survivesPowerLoss(CrashFileSystem disk, boolean encrypted, Map<String, String> model) {
            for (var seed = 0; seed < 4; seed++) {
                var survivor = disk.afterPowerLoss(new Random(seed));
                try (var ledger = open(survivor.getPath("/store"), encrypted, WriteThrough.Mode.OFF)) {
                    assertEquals(model, contents(ledger), "seed " + seed);
                }
            }
        }

        @Test
        @DisplayName("is a multiple of a block below a megabyte by its header, and holds a group of the most one write takes with its padding and the cipher's last reservation")
        void bounds() {
            assertEquals(0, LogWriter.OUTPUT % LogWriter.ALIGNMENT);
            assertEquals(0, LogWriter.ONE_WRITE % LogWriter.ALIGNMENT, "a group padded to its block ends past the bound");
            // 16 bytes: the header of a byte array without compact object headers, the larger of the two
            assertTrue(LogWriter.OUTPUT + 16 <= 1 << 20, "the output is humongous in a G1 region of 2 MiB");
            assertTrue(LogWriter.ONE_WRITE + 2 * LogWriter.TAG_BYTES <= LogWriter.OUTPUT, "the last call of the cipher drains a group of one write");
            assertTrue(LogWriter.ONE_WRITE >= LogWriter.FRAME_HEADER + LogWriter.RUN_BYTES + LogWriter.TAG_BYTES, "a run does not fit in one write");
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @CsvSource({"true", "false"})
        @DisplayName("writes through a group that seals into exactly the most one write takes, forces one a byte larger, and both survive a loss of power")
        void boundary(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), encrypted, WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var fills = 0;
                while (log.currentSegment() < 4) {
                    commitBatch(ledger, model, "f" + fills++ + "-", 64, 4_000, 0);
                }
                ledger.checkpoint();

                // A frame of WRITES puts under keys of four bytes seals into its values and this many bytes more
                commitBatch(ledger, model, "c", WRITES, 3_000, 0);
                var fixed = lastGroup(log) - (long) WRITES * 3_000;
                var length = (int) ((LogWriter.ONE_WRITE - fixed) / WRITES);
                var extra = (int) ((LogWriter.ONE_WRITE - fixed) % WRITES);

                reachRoom(ledger, model, "p");
                var through = log.writtenThrough();
                var forces = log.forces();
                commitBatch(ledger, model, "a", WRITES, length, extra);
                assertEquals(LogWriter.ONE_WRITE, lastGroup(log));
                assertEquals(through + 1, log.writtenThrough(), "the group of the most one write takes was not written through");
                assertEquals(forces + 1, log.forces());
                assertEquals(LogWriter.OUTPUT, log.outputLength());
                survivesPowerLoss(disk, encrypted, model);

                reachRoom(ledger, model, "q");
                through = log.writtenThrough();
                forces = log.forces();
                commitBatch(ledger, model, "b", WRITES, length, extra + 1);
                assertEquals(LogWriter.ONE_WRITE + 1, lastGroup(log));
                assertEquals(through, log.writtenThrough(), "a group past the most one write takes was written through");
                assertEquals(forces + 1, log.forces(), "a group past the most one write takes was not forced once");
                assertEquals(LogWriter.OUTPUT, log.outputLength());
                survivesPowerLoss(disk, encrypted, model);
            }
            try (var reopened = open(disk.getPath("/store"), encrypted, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }
    }
}
