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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
            // Reopened, as a new store queues a frame of its key spaces, which the pool may write before the open returns
            open(disk.getPath("/store"), WriteThrough.Mode.OFF).close();
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DSYNC, () -> {
                asked.incrementAndGet();
                return false;
            }))) {
                var log = log(ledger);
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

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Chunks.class, names = {"OFF", "DSYNC"})
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows a group written in chunks lands block by block where its chunks name, whole")
        void unbufferedChunks(WriteThrough.Chunks chunks, @TempDir Path directory) throws Throwable {
            Assumptions.assumeTrue(WriteThrough.class.getModule().isNativeAccessEnabled(), "the tests run without native access");
            var file = directory.resolve("segment");
            Files.write(file, new byte[64 << 10]);
            var way = new WriteThrough(WriteThrough.Kind.UNBUFFERED, null, chunks);
            var handle = way.open(file);
            var bytes = new byte[40_960];
            for (var block = 0; block < bytes.length / LogWriter.ALIGNMENT; block++) {
                Arrays.fill(bytes, block * LogWriter.ALIGNMENT, (block + 1) * LogWriter.ALIGNMENT, (byte) (block + 1));
            }
            int wrote;
            try {
                wrote = handle.write(bytes, bytes.length, bytes.length, 8_192, WriteThrough.MAX_CHUNKS);
                // The same handle writes a group of one block after the chunks, and again in chunks after that
                handle.write(bytes, LogWriter.ALIGNMENT, bytes.length, 53_248, WriteThrough.MAX_CHUNKS);
                handle.write(bytes, 2 * LogWriter.ALIGNMENT, bytes.length, 57_344, WriteThrough.MAX_CHUNKS);
            } finally {
                handle.close();
            }
            assertEquals(chunks == WriteThrough.Chunks.OFF ? 1 : WriteThrough.MAX_CHUNKS, wrote);
            var written = Files.readAllBytes(file);
            assertEquals(64 << 10, written.length);
            for (var offset = 0; offset < written.length; offset++) {
                byte expected;
                if (offset >= 8_192 && offset < 8_192 + bytes.length) {
                    expected = bytes[offset - 8_192];
                } else if (offset >= 53_248 && offset < 57_344) {
                    expected = 1;
                } else if (offset >= 57_344 && offset < 65_536) {
                    expected = bytes[offset - 57_344];
                } else {
                    expected = 0;
                }
                assertEquals(expected, written[offset], "byte " + offset);
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Chunks.class, names = {"DSYNC", "DIRECT"})
        @DisabledOnOs(OS.MAC)
        @DisplayName("a store that writes its groups of several blocks in chunks recovers them after a crash and a close")
        void chunkedStore(WriteThrough.Chunks chunks, @TempDir Path directory) throws Exception {
            var kind = WriteThrough.of(WriteThrough.Mode.DIRECT, directory).kind();
            Assumptions.assumeTrue(kind != null, "the tests run without native access");
            var model = new TreeMap<String, String>();
            var way = chunkedWay(kind, chunks, true);
            var ledger = open(directory, way);
            long inChunks;
            try {
                var log = log(ledger);
                var commits = 0;
                // Fresh segments written with zeros once the commits have waited, and spares after the checkpoint
                while (log.currentSegment() < 8) {
                    commitLarge(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                // Unless the file system refused O_DIRECT, which leaves the chunks to a channel of the cache
                var linuxForced = System.getProperty("os.name", "").equals("Linux") && chunks == WriteThrough.Chunks.DIRECT
                        && log.writeThroughKind() == WriteThrough.Kind.DIRECT;
                var checked = !linuxForced;
                for (var i = 0; i < 400 && (log.writtenInChunks() < 8 || !checked); i++) {
                    var before = log.writtenInChunks();
                    var segment = log.currentSegment();
                    commitLarge(ledger, model, "w" + commits++);
                    if (!checked && log.writtenInChunks() > before && log.currentSegment() == segment) {
                        var file = directory.resolve("log").resolve(LogWriter.segmentName(segment));
                        // The channel the chunks are forced through, beside the segment's own and its O_DSYNC one
                        assertTrue(openFlags(file).stream().anyMatch(flags -> (flags & oDirect()) != 0 && (flags & O_DSYNC) == 0),
                                "no channel of the segment is open with O_DIRECT alone");
                        checked = true;
                    }
                }
                assertTrue(checked, "no group was written in chunks inside a segment");
                inChunks = log.writtenInChunks();
            } finally {
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            assertTrue(inChunks > 0, "no group was written in chunks");
            awaitNoChunkWriters(way);
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux the device under the store is asked how it writes through, and the answer names the chunks")
        void linuxQueue(@TempDir Path directory) throws Exception {
            var device = (long) Files.getAttribute(directory, "unix:dev");
            var queue = WriteThrough.DeviceQueue.queueOf(device);
            // What the runner's device reports says which chunks the default arm of a benchmark there runs
            if (queue == null) {
                System.out.println("The device under " + directory + " has no queue to read");
            } else {
                System.out.println("The queue of the device under " + directory + " (" + queue + "): fua "
                        + Files.readString(queue.resolve("fua")).trim() + ", write_cache " + Files.readString(queue.resolve("write_cache")).trim());
            }
            var through = WriteThrough.DeviceQueue.writesThrough(directory);
            assertEquals(queue != null && (Files.readString(queue.resolve("fua")).trim().equals("1")
                    || Files.readString(queue.resolve("write_cache")).trim().equals("write through")), through);
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

    /// {@return a way that starts at `kind` and writes groups of several blocks in `chunks`, those of every group if
    /// `everyGroup`}
    private static WriteThrough chunkedWay(WriteThrough.Kind kind, WriteThrough.Chunks chunks, boolean everyGroup) {
        var way = new WriteThrough(kind, null, chunks);
        way.chunkEveryGroupForTests = everyGroup;
        return way;
    }

    /// Commits `key` with a value that seals into three blocks, durably, and records it in `model`.
    private static void commitLarge(Ledger ledger, Map<String, String> model, String key) throws InterruptedException {
        var value = "c".repeat(9_000) + key;
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    /// Commits until a group of several blocks is written in chunks well inside its segment, so the next one is too.
    ///
    /// @return the commits made, from `first`
    private static int reachChunks(Ledger ledger, Map<String, String> model, int first) throws InterruptedException {
        var log = log(ledger);
        var commits = first;
        for (var attempt = 0; attempt < 400; attempt++) {
            var before = log.writtenInChunks();
            commitLarge(ledger, model, "w" + commits++);
            if (log.writtenInChunks() > before && log.groupEnd() < SEGMENT / 2) {
                return commits;
            }
        }
        throw new AssertionError("no group was written in chunks inside a segment");
    }

    /// {@return `count` blocks, block `i` filled with `first + i`}
    private static byte[] numberedBlocks(int count, int first) {
        var bytes = new byte[count * LogWriter.ALIGNMENT];
        for (var block = 0; block < count; block++) {
            Arrays.fill(bytes, block * LogWriter.ALIGNMENT, (block + 1) * LogWriter.ALIGNMENT, (byte) (first + block));
        }
        return bytes;
    }

    /// Waits up to ten seconds for `count` threads of `way` to look for a chunk, spinning rather than parked.
    private static void awaitLooking(WriteThrough way, int count) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (way.lookingChunkWritersForTests() != count && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(count, way.lookingChunkWritersForTests(), "threads looking for a chunk");
    }

    /// Waits up to ten seconds for the threads of `way` that write chunks to end, as they do soon after a close.
    private static void awaitNoChunkWriters(WriteThrough way) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (way.chunkWritersForTests() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(0, way.chunkWritersForTests(), "a thread writing chunks outlived the store");
    }

    /// {@return whether this thread writes chunks of a group past the first}
    private static boolean onChunkThread() {
        return Thread.currentThread().getName().startsWith("ledger-io-chunk-");
    }

    /// Commits the keys `k00000` onwards in order, each value sealing into two to four blocks, to a store that writes
    /// every group of several blocks in chunks, enough first to make spares and reach one; and what it learned while it
    /// ran. A stopped file system ends it.
    private static final class ChunkedWorkload {
        private final CrashFileSystem disk;
        private final WriteThrough.Chunks chunks;
        int made;
        int durable;
        long warmForces;
        long inChunks;

        ChunkedWorkload(CrashFileSystem disk, WriteThrough.Chunks chunks) {
            this.disk = disk;
            this.chunks = chunks;
        }

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
            return "v".repeat(5_000 + commit % 3 * 4_000) + commit;
        }

        void run() {
            Ledger ledger = null;
            try {
                ledger = open(disk.getPath("/store"), chunkedWay(WriteThrough.Kind.DSYNC, chunks, true));
                var log = log(ledger);
                while (log.currentSegment() < 6) {
                    commit(ledger, true);
                }
                ledger.checkpoint();
                warmForces = disk.forces();
                var before = log.writtenInChunks();
                for (var i = 0; i < 24; i++) {
                    commit(ledger, i % 4 != 0);
                }
                inChunks = log.writtenInChunks() - before;
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

    @Nested
    @DisplayName("groups written in chunks")
    class Chunked {
        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Chunks.class, names = {"DSYNC", "DIRECT"})
        @DisplayName("a group written in chunks survives a loss of power right after its commit returns")
        void durableAtReturn(WriteThrough.Chunks chunks) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), chunkedWay(WriteThrough.Kind.DSYNC, chunks, true))) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                commits = reachChunks(ledger, model, commits);
                var written = 0;
                for (var i = 0; i < 200 && written < 4; i++) {
                    var inChunks = log.writtenInChunks();
                    var forces = log.forces();
                    commitLarge(ledger, model, "w" + commits++);
                    if (log.writtenInChunks() > inChunks) {
                        written++;
                        assertEquals(forces + 1, log.forces(), "a group written in chunks counts as one force");
                        survivesPowerLoss(disk, model);
                    }
                }
                assertEquals(4, written, "groups of the last commits written in chunks");
            }
            survivesPowerLoss(disk, model);
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Chunks.class, names = {"DSYNC", "DIRECT"})
        @DisplayName("recovers a prefix of the commits from the last one acknowledged durable, whichever chunk or force the power fails at")
        void everyForce(WriteThrough.Chunks chunks) throws Exception {
            var dry = CrashFileSystem.create();
            var planned = new ChunkedWorkload(dry, chunks);
            planned.run();
            assertTrue(planned.inChunks > 0, "the workload wrote no group in chunks");
            for (var stop = planned.warmForces; stop <= dry.forces(); stop++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(stop);
                var workload = new ChunkedWorkload(disk, chunks);
                workload.run();
                // Two draws, so a stop between chunks loses some blocks of the group written after it and keeps others
                for (var seed = 0; seed < 2; seed++) {
                    var survivor = disk.afterPowerLoss(new Random(stop * 31 + seed));
                    try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                        var recovered = contents(ledger);
                        var count = recovered.size();
                        var at = stop;
                        assertTrue(count >= workload.durable && count <= workload.made,
                                () -> "stop at force " + at + ": " + count + " commits recovered, " + workload.durable + " to " + workload.made + " expected");
                        assertEquals(ChunkedWorkload.state(count), recovered, "stop at force " + stop);
                        ledger.commit(ledger.newBatch().put(NAMES, "~after", "v")).awaitDurable();
                    }
                    try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                        assertEquals(Optional.of("v"), ledger.get(NAMES, "~after"));
                    }
                }
            }
        }

        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Chunks.class, names = {"DSYNC", "DIRECT"})
        @DisplayName("a chunk the file system refuses has the group forced once every chunk returned, and the store forces from then on")
        void refusedChunk(WriteThrough.Chunks chunks) throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var refused = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".log") && onChunkThread()
                        && armed.compareAndSet(true, false)) {
                    refused.incrementAndGet();
                    throw new IOException("Input/output error");
                }
            });
            try (var ledger = open(disk.getPath("/store"), chunkedWay(WriteThrough.Kind.DSYNC, chunks, true))) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                commits = reachChunks(ledger, model, commits);
                var through = log.writtenThrough();
                armed.set(true);
                commitLarge(ledger, model, "refused");
                assertEquals(1, refused.get());
                assertEquals(through, log.writtenThrough(), "the refused group counts as written through");
                assertNull(log.writeThroughKind());
                survivesPowerLoss(disk, model);
                for (var i = 0; i < 20; i++) {
                    commitLarge(ledger, model, "w" + commits++);
                }
                assertEquals(through, log.writtenThrough(), "a group was written through after a refused chunk");
            }
            survivesPowerLoss(disk, model);
        }

        @ParameterizedTest(name = "interrupted on {0}")
        @CsvSource({"the flush, DSYNC", "the flush, DIRECT", "a chunk's thread, DSYNC", "a chunk's thread, DIRECT"})
        @DisplayName("an interrupt that closes the channel under a chunk has every chunk of the group written again, and the store goes on writing chunks")
        void interrupted(String target, WriteThrough.Chunks chunks) throws Exception {
            var disk = CrashFileSystem.create();
            var committer = Thread.currentThread();
            var onFlush = target.equals("the flush");
            var armed = new AtomicBoolean();
            var interrupts = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                // Inside the write of a chunk, whose channel the interrupt closes before it writes
                if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith(".log")
                        && (onFlush ? Thread.currentThread() == committer : onChunkThread()) && armed.compareAndSet(true, false)) {
                    Thread.currentThread().interrupt();
                    interrupts.incrementAndGet();
                }
            });
            var way = chunkedWay(WriteThrough.Kind.DSYNC, chunks, true);
            try (var pool = IoPool.create(1);
                 var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool))
                         .segmentSize(SEGMENT)
                         .writeThrough(way)
                         .build()) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                for (var round = 0; round < 3; round++) {
                    commits = reachChunks(ledger, model, commits);
                    var through = log.writtenThrough();
                    var inChunks = log.writtenInChunks();
                    var before = interrupts.get();
                    // The commit then writes its own group, on this thread
                    var released = new CountDownLatch(1);
                    Simulated.occupy(pool, released);
                    armed.set(true);
                    var key = "w" + commits++;
                    var value = "c".repeat(9_000) + key;
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                    try {
                        commit.awaitDurable();
                    } catch (InterruptedException _) {
                        // The interrupt the flush kept was given back to this commit's thread
                        commit.awaitDurable();
                    } finally {
                        released.countDown();
                    }
                    Thread.interrupted();
                    model.put(key, value);
                    armed.set(false);
                    assertEquals(before + 1, interrupts.get(), "no chunk of the group was interrupted");
                    assertEquals(through + 1, log.writtenThrough(), "the interrupted group was left to a force");
                    assertEquals(inChunks + 1, log.writtenInChunks(), "the interrupted group was not written again in chunks");
                    survivesPowerLoss(disk, model);
                }
                var inChunks = log.writtenInChunks();
                for (var i = 0; i < 20 && log.writtenInChunks() == inChunks; i++) {
                    commitLarge(ledger, model, "w" + commits++);
                }
                assertTrue(log.writtenInChunks() > inChunks, "the interrupts stopped the chunks");
                survivesPowerLoss(disk, model);
            } finally {
                Thread.interrupted();
            }
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                assertEquals(model, contents(ledger));
            }
        }

        @Test
        @DisplayName("concurrent committers have their groups of several blocks written in chunks by the pool's flush, and every acknowledged commit survives")
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
            var way = chunkedWay(WriteThrough.Kind.DSYNC, WriteThrough.Chunks.DSYNC, false);
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                var warm = new TreeMap<String, String>();
                reachWriteThrough(ledger, warm);
                model.putAll(warm);
                assertEquals(0, log.writtenInChunks(), "a commit's own group was written in chunks");
                var results = new ArrayList<Future<?>>();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (var committer = 0; committer < 8; committer++) {
                        var name = "c" + committer + "-";
                        results.add(executor.submit(() -> {
                            for (var i = 0; i < 60; i++) {
                                var key = name + i;
                                var value = "x".repeat(1_500 + i * 7) + key;
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
                assertTrue(log.writtenInChunks() > 0, "no group of the committers was written in chunks");
                survivesPowerLoss(disk, new TreeMap<>(model));
            }
            awaitNoChunkWriters(way);
        }

        @Test
        @DisplayName("the threads that write chunks end when the store closes, idle, and the next store starts them again")
        void closeEndsThreads() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var way = chunkedWay(WriteThrough.Kind.DSYNC, WriteThrough.Chunks.DSYNC, true);
            try (var ledger = open(disk.getPath("/store"), way)) {
                var commits = reachWriteThrough(ledger, model);
                reachChunks(ledger, model, commits);
                assertTrue(way.chunkWritersForTests() > 0, "no thread writes chunks");
            }
            awaitNoChunkWriters(way);
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                // Segments of its own, which a checkpoint turns into spares
                var commits = 10_000;
                var first = log.currentSegment();
                while (log.currentSegment() < first + 8) {
                    commit(ledger, model, "w" + commits++);
                }
                ledger.checkpoint();
                reachChunks(ledger, model, commits);
                assertTrue(way.chunkWritersForTests() > 0, "the threads were not started again");
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("threads a prime woke look for the next group's chunks until it is written, which then wakes none, and park soon after it")
        void primedThreadsLook() throws Exception {
            var disk = CrashFileSystem.create();
            var file = disk.getPath("/segment");
            Files.write(file, new byte[64 << 10]);
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null, WriteThrough.Chunks.DSYNC);
            var handle = way.open(file);
            var first = numberedBlocks(4, 1);
            var second = numberedBlocks(4, 11);
            try {
                assertEquals(WriteThrough.MAX_CHUNKS, handle.write(first, first.length, first.length, 0, WriteThrough.MAX_CHUNKS));
                // Long enough that a loaded machine still posts the chunks while the threads look for them
                handle.prime(WriteThrough.MAX_CHUNKS, TimeUnit.SECONDS.toNanos(60));
                awaitLooking(way, WriteThrough.MAX_CHUNKS - 1);
                var posted = way.postsToLookingForTests();
                assertEquals(WriteThrough.MAX_CHUNKS, handle.write(second, second.length, second.length, 16_384, WriteThrough.MAX_CHUNKS));
                assertEquals(posted + WriteThrough.MAX_CHUNKS - 1, way.postsToLookingForTests(), "a chunk of the primed group was posted to a parked thread");
                // The prime ends with the group it announced, well before its minute
                awaitLooking(way, 0);
            } finally {
                handle.close();
                way.release();
            }
            var written = Files.readAllBytes(file);
            assertArrayEquals(first, Arrays.copyOfRange(written, 0, 16_384));
            assertArrayEquals(second, Arrays.copyOfRange(written, 16_384, 32_768));
            awaitNoChunkWriters(way);
        }

        @Test
        @DisplayName("a group whose chunks return after the flush stopped looking for them returns only once every chunk has landed")
        void slowChunks() throws Exception {
            var disk = CrashFileSystem.create();
            // Far past the time the flush looks for the other chunks before it parks
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && path.toString().endsWith("segment") && onChunkThread()) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException exception) {
                        throw new AssertionError("a thread writing chunks was interrupted", exception);
                    }
                }
            });
            var file = disk.getPath("/segment");
            Files.write(file, new byte[64 << 10]);
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null, WriteThrough.Chunks.DSYNC);
            var handle = way.open(file);
            try {
                for (var round = 0; round < 4; round++) {
                    var bytes = numberedBlocks(4, 1 + round * 4);
                    var at = round * 16_384L;
                    assertEquals(WriteThrough.MAX_CHUNKS, handle.write(bytes, bytes.length, bytes.length, at, WriteThrough.MAX_CHUNKS));
                    var written = Files.readAllBytes(file);
                    assertArrayEquals(bytes, Arrays.copyOfRange(written, (int) at, (int) at + bytes.length), "round " + round);
                }
            } finally {
                handle.close();
                way.release();
            }
            awaitNoChunkWriters(way);
        }

        @Test
        @DisplayName("a store that writes groups whole, or whose way writes only with O_DSYNC, writes no group in chunks")
        void whole() throws Exception {
            for (var way : List.of(chunkedWay(WriteThrough.Kind.DSYNC, WriteThrough.Chunks.OFF, true),
                    chunkedWay(WriteThrough.Kind.DSYNC, WriteThrough.Chunks.AUTO, true))) {
                var disk = CrashFileSystem.create();
                var model = new TreeMap<String, String>();
                try (var ledger = open(disk.getPath("/store"), way)) {
                    var log = log(ledger);
                    var commits = reachWriteThrough(ledger, model);
                    for (var i = 0; i < 30; i++) {
                        commitLarge(ledger, model, "w" + commits++);
                    }
                    assertTrue(log.writtenThrough() > 0);
                    assertEquals(0, log.writtenInChunks());
                    assertEquals(0, way.chunkWritersForTests());
                }
                survivesPowerLoss(disk, model);
            }
        }

        @ParameterizedTest(name = "{0} bytes")
        @CsvSource({"4096, 1", "8192, 2", "12288, 3", "16384, 4", "40960, 4", "131072, 4", "135168, 1"})
        @DisplayName("split a group into up to four chunks of whole blocks, up to 128 KiB")
        void split(int length, int expected) {
            var count = WriteThrough.chunksFor(length);
            assertEquals(expected, count);
            var blocks = new HashSet<Integer>();
            for (var index = 0; index < count; index++) {
                var start = WriteThrough.chunkStart(length, count, index);
                var end = WriteThrough.chunkStart(length, count, index + 1);
                assertEquals(0, start % LogWriter.ALIGNMENT);
                assertTrue(end > start, "an empty chunk");
                for (var block = start; block < end; block += LogWriter.ALIGNMENT) {
                    assertTrue(blocks.add(block));
                }
            }
            assertEquals(length / LogWriter.ALIGNMENT, blocks.size(), "blocks left out of every chunk");
            assertEquals(length, WriteThrough.chunkStart(length, count, count));
        }

        @ParameterizedTest(name = "{0}:{1}")
        @CsvSource({"8, 1", "8, 0", "259, 3", "253, 300", "4095, 1048575", "4096, 1048576", "65536, 7"})
        @DisplayName("read a Linux device number as glibc's makedev joins its major and minor numbers")
        void deviceNumbers(long major, long minor) {
            var device = ((major & 0xFFFFF000L) << 32) | ((major & 0xFFFL) << 8) | ((minor & 0xFFFFFF00L) << 12) | (minor & 0xFFL);
            var split = WriteThrough.DeviceQueue.majorMinor(device);
            assertEquals(major, split[0]);
            assertEquals(minor, split[1]);
        }

        @Test
        @DisplayName("are read from ledger.log.chunks, auto when it is unset, and refuse a value they do not know")
        void property() {
            var previous = System.getProperty("ledger.log.chunks");
            try {
                System.clearProperty("ledger.log.chunks");
                assertEquals(WriteThrough.Chunks.AUTO, WriteThrough.Chunks.fromProperty());
                Set<WriteThrough.Chunks> read = new HashSet<>();
                for (var value : new String[]{"auto", "OFF", " dsync ", "Direct"}) {
                    System.setProperty("ledger.log.chunks", value);
                    read.add(WriteThrough.Chunks.fromProperty());
                }
                assertEquals(Set.of(WriteThrough.Chunks.values()), read);
                System.setProperty("ledger.log.chunks", "always");
                assertThrows(IllegalArgumentException.class, WriteThrough.Chunks::fromProperty);
            } finally {
                if (previous == null) {
                    System.clearProperty("ledger.log.chunks");
                } else {
                    System.setProperty("ledger.log.chunks", previous);
                }
            }
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
}
