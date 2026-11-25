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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
        @DisplayName("a way nothing refuses stays the way the log writes through in as segments roll, through a channel on a file system other than the default one")
        void keepsWay() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), new WriteThrough(WriteThrough.Kind.DIRECT, null))) {
                var commits = reachWriteThrough(ledger, model);
                assertEquals(WriteThrough.Kind.DIRECT, log(ledger).writeThroughKind());
                for (var i = 0; i < 200 && log(ledger).throughHandleForTests() == null; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertTrue(log(ledger).throughHandleForTests() instanceof WriteThrough.ChannelHandle,
                        "the segment of another file system is not written through a channel");
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
                handle.write(bytes, bytes.length, 4_096);
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

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux a virtual thread that leads writes its groups through the C library's descriptor, opened with O_DIRECT and O_DSYNC, and they survive a close")
        void linuxDescriptor(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var model = new TreeMap<String, String>();
            var released = new CountDownLatch(1);
            try (var pool = IoPool.create(1); var ledger = openPooled(directory, pool)) {
                var log = log(ledger);
                onVirtualThread(() -> {
                    var commits = reachWriteThrough(ledger, model);
                    // A commit writes its own group only if it starts the flush before the pool's thread does
                    hold(pool, released);
                    var checked = false;
                    for (var attempt = 0; attempt < 200 && !checked; attempt++) {
                        var through = log.writtenThrough();
                        var segment = log.currentSegment();
                        commit(ledger, model, "w" + commits++);
                        if (log.writtenThrough() > through && log.currentSegment() == segment
                                && log.throughHandleForTests() instanceof WriteThrough.PosixHandle handle && handle.descriptorForTests() >= 0) {
                            var before = handle.channelWritesForTests();
                            var written = log.writtenThrough();
                            for (var i = 0; i < 4 && log.currentSegment() == segment; i++) {
                                commit(ledger, model, "w" + commits++);
                            }
                            if (log.currentSegment() == segment) {
                                assertEquals(before, handle.channelWritesForTests(), "a virtual leader wrote through the channel");
                                assertTrue(log.writtenThrough() > written, "no group was written through");
                                var file = directory.resolve("log").resolve(LogWriter.segmentName(segment));
                                var flags = openFlags(file);
                                assertTrue(flags.stream().filter(open -> (open & oDirect()) != 0 && (open & O_DSYNC) != 0).count() >= 2,
                                        "the channel and the descriptor are not both open with O_DIRECT and O_DSYNC: " + flags);
                                checked = true;
                            }
                        }
                    }
                    assertTrue(checked, "no group was written through a descriptor");
                    released.countDown();
                    return null;
                });
                assertEquals(WriteThrough.Kind.DIRECT, log.writeThroughKind(), "the log left O_DIRECT");
            } finally {
                released.countDown();
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux the segments of a store whose groups only platform threads write are channels alone, and the first group a virtual thread writes through opens its segment with a descriptor in place of the channel")
        void linuxDescriptorOnFirstVirtualWrite(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var model = new TreeMap<String, String>();
            var released = new CountDownLatch(1);
            var way = WriteThrough.of(WriteThrough.Mode.DIRECT, directory);
            try (var pool = IoPool.create(1);
                 var ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool))
                         .segmentSize(SEGMENT)
                         .writeThrough(way)
                         .build()) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                var through = log.writtenThrough();
                var segment = log.currentSegment();
                for (var i = 0; i < 400 && (log.writtenThrough() < through + 20 || log.currentSegment() < segment + 2); i++) {
                    commit(ledger, model, "w" + commits++);
                    assertFalse(log.throughHandleForTests() instanceof WriteThrough.PosixHandle,
                            "a segment that only platform threads wrote was opened with a descriptor");
                }
                assertTrue(log.writtenThrough() >= through + 20 && log.currentSegment() >= segment + 2,
                        "the platform threads wrote too few groups through");
                assertTrue(way.opensForTests() - way.closesForTests() <= 1, "a handle of a segment left behind is open");
                var from = commits;
                onVirtualThread(() -> {
                    // A commit writes its own group only if it starts the flush before the pool's thread does
                    hold(pool, released);
                    var next = from;
                    var described = false;
                    for (var attempt = 0; attempt < 400 && !described; attempt++) {
                        commit(ledger, model, "w" + next++);
                        described = log.throughHandleForTests() instanceof WriteThrough.PosixHandle handle && handle.descriptorForTests() >= 0;
                    }
                    assertTrue(described, "no group of the virtual thread was written through a descriptor");
                    assertEquals(1, way.opensForTests() - way.closesForTests(), "the channel the descriptor replaced is open");
                    released.countDown();
                    return null;
                });
                assertEquals(WriteThrough.Kind.DIRECT, log.writeThroughKind(), "the log left O_DIRECT");
            } finally {
                released.countDown();
            }
            assertEquals(way.opensForTests(), way.closesForTests(), "the close left a handle open");
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux a channel that refuses a write once a descriptor took the way's writes moves the log to O_DSYNC, not to forces")
        void linuxChannelRefusesAfterDescriptor(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var model = new TreeMap<String, String>();
            var released = new CountDownLatch(1);
            var way = WriteThrough.of(WriteThrough.Mode.DIRECT, directory);
            try (var pool = IoPool.create(1);
                 var ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool))
                         .segmentSize(SEGMENT)
                         .writeThrough(way)
                         .build()) {
                var log = log(ledger);
                var commits = onVirtualThread(() -> reachDescriptor(ledger, model, pool, released));
                assertEquals(WriteThrough.Kind.DIRECT, log.writeThroughKind());
                // A file system whose reported block size is larger than the device's logical block refuses the
                // channel's writes, which the writes of platform threads take
                way.channelRefusesForTests = true;
                released.countDown();
                for (var i = 0; i < 400 && log.writeThroughKind() == WriteThrough.Kind.DIRECT; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertEquals(WriteThrough.Kind.DSYNC, log.writeThroughKind(), "the refusal did not move the log one way down");
                var through = log.writtenThrough();
                for (var i = 0; i < 200 && log.writtenThrough() == through; i++) {
                    commit(ledger, model, "w" + commits++);
                }
                assertTrue(log.writtenThrough() > through, "the log forces its groups after the refusal");
                assertEquals(WriteThrough.Kind.DSYNC, log.writeThroughKind());
            } finally {
                released.countDown();
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux an interrupt of a virtual thread writing through a descriptor leaves its commit durable and the interrupt to the thread")
        void linuxDescriptorInterrupted(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var model = new TreeMap<String, String>();
            var released = new CountDownLatch(1);
            var armed = new AtomicBoolean();
            var interrupts = new AtomicInteger();
            WriteThrough.Posix.writingForTests = _ -> {
                if (armed.compareAndSet(true, false)) {
                    Thread.currentThread().interrupt();
                    interrupts.incrementAndGet();
                }
            };
            try (var pool = IoPool.create(1); var ledger = openPooled(directory, pool)) {
                var log = log(ledger);
                onVirtualThread(() -> {
                    var commits = reachDescriptor(ledger, model, pool, released);
                    for (var i = 0; i < 4; i++) {
                        var key = "w" + commits++;
                        var value = "v".repeat(500) + key;
                        var through = log.writtenThrough();
                        var before = interrupts.get();
                        armed.set(true);
                        var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                        commit.awaitDurable();
                        assertTrue(commit.isDurable(), "the commit is not durable");
                        model.put(key, value);
                        armed.set(false);
                        assertTrue(Thread.interrupted(), "the interrupt was lost");
                        assertEquals(before + 1, interrupts.get(), "the group was not written through a descriptor");
                        assertEquals(through + 1, log.writtenThrough(), "the interrupt left the group to a force");
                    }
                    var through = log.writtenThrough();
                    for (var i = 0; i < 20 && log.writtenThrough() == through; i++) {
                        commit(ledger, model, "w" + commits++);
                    }
                    assertTrue(log.writtenThrough() > through, "the interrupts moved the log to forces");
                    released.countDown();
                    return null;
                });
            } finally {
                WriteThrough.Posix.writingForTests = null;
                released.countDown();
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux at most the limit of virtual threads write through descriptors at a time, and the next writes through its channel")
        void linuxDescriptorLimit(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var limit = WriteThrough.Posix.LIMIT;
            var pools = new ArrayList<IoPool>();
            var ledgers = new ArrayList<Ledger>();
            var released = new CountDownLatch(1);
            var held = new CountDownLatch(1);
            var arrivals = new AtomicInteger();
            var most = new AtomicInteger();
            try {
                for (var i = 0; i <= limit; i++) {
                    var pool = IoPool.create(1);
                    pools.add(pool);
                    var ledger = openPooled(directory.resolve("store" + i), pool);
                    ledgers.add(ledger);
                    onVirtualThread(() -> reachDescriptor(ledger, new TreeMap<>(), pool, released));
                }
                WriteThrough.Posix.writingForTests = writing -> {
                    most.accumulateAndGet(writing, Math::max);
                    if (arrivals.incrementAndGet() <= limit) {
                        try {
                            held.await(1, TimeUnit.MINUTES);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                };
                var writers = new ArrayList<Thread>();
                for (var i = 0; i < limit; i++) {
                    var ledger = ledgers.get(i);
                    writers.add(Thread.ofVirtual().start(() -> {
                        try {
                            ledger.commit(ledger.newBatch().put(NAMES, "held", "v".repeat(500))).awaitDurable();
                        } catch (InterruptedException exception) {
                            throw new RuntimeException(exception);
                        }
                    }));
                }
                for (var i = 0; i < 600 && arrivals.get() < limit; i++) {
                    Thread.sleep(100);
                }
                assertEquals(limit, arrivals.get(), "the held writes did not all reach their descriptors");
                var last = ledgers.get(limit);
                var log = log(last);
                onVirtualThread(() -> {
                    var handle = (WriteThrough.PosixHandle) log.throughHandleForTests();
                    var channelWrites = handle.channelWritesForTests();
                    var through = log.writtenThrough();
                    last.commit(last.newBatch().put(NAMES, "next", "v".repeat(500))).awaitDurable();
                    assertEquals(through + 1, log.writtenThrough(), "the group past the limit was not written through");
                    if (log.throughHandleForTests() == handle) {
                        assertEquals(channelWrites + 1, handle.channelWritesForTests(), "the group past the limit did not go through the channel");
                    }
                    return null;
                });
                assertEquals(limit, arrivals.get(), "a virtual thread past the limit wrote through a descriptor");
                held.countDown();
                for (var writer : writers) {
                    writer.join();
                }
                assertTrue(most.get() <= limit, "more virtual threads than the limit wrote through descriptors: " + most.get());
            } finally {
                WriteThrough.Posix.writingForTests = null;
                held.countDown();
                released.countDown();
                for (var ledger : ledgers) {
                    ledger.close();
                }
                for (var pool : pools) {
                    pool.close();
                }
            }
        }

        @Test
        @EnabledOnOs(OS.LINUX)
        @DisplayName("on Linux a handle whose descriptor is closed refuses a write, and one dropped without a close has its descriptor closed")
        void linuxDescriptorClosed(@TempDir Path directory) throws Exception {
            assumeDescriptors(directory);
            var file = directory.resolve("segment");
            Files.write(file, new byte[16_384]);
            var target = file.toRealPath();
            var through = WriteThrough.of(WriteThrough.Mode.DIRECT, directory);
            var first = new byte[4_096];
            Arrays.fill(first, (byte) 7);
            var second = new byte[4_096];
            Arrays.fill(second, (byte) 9);
            var alone = through.open(file);
            assertTrue(alone instanceof WriteThrough.ChannelHandle, "a segment was opened with a descriptor before a virtual thread wrote");
            var handle = (WriteThrough.PosixHandle) through.descriptorsFor(alone, file, false);
            alone.close();
            onVirtualThread(() -> {
                handle.write(first, first.length, 0);
                assertTrue(handle.descriptorForTests() >= 0, "the virtual thread opened no descriptor");
                handle.close();
                assertFalse(handle.isOpen());
                assertThrows(ClosedChannelException.class, () -> handle.write(second, second.length, 4_096));
                return null;
            });
            assertEquals(List.of(), openFlags(file), "the closed handle left a descriptor open");
            var written = Files.readAllBytes(file);
            assertEquals(7, written[4_095]);
            assertEquals(0, written[4_096], "a write after the close reached the file");
            var number = onVirtualThread(() -> {
                var dropped = (WriteThrough.PosixHandle) through.open(file);
                dropped.write(second, second.length, 4_096);
                return dropped.descriptorForTests();
            });
            assertTrue(number >= 0, "the virtual thread opened no descriptor");
            var link = Path.of("/proc/self/fd/" + number);
            var closed = false;
            for (var i = 0; i < 200 && !closed; i++) {
                System.gc();
                Thread.sleep(50);
                try {
                    closed = !Files.readSymbolicLink(link).equals(target);
                } catch (IOException _) {
                    closed = true;
                }
            }
            assertTrue(closed, "the descriptor of a dropped handle stayed open");
        }

        /// Skips the test unless this process writes through descriptors of the C library into `directory`, linking
        /// their calls first.
        private static void assumeDescriptors(Path directory) {
            Assumptions.assumeTrue(WriteThrough.Posix.ELIGIBLE, "the process cannot write through descriptors of the C library");
            Assumptions.assumeTrue(acceptsDirect(directory), "the file system refuses O_DIRECT");
            assertTrue(WriteThrough.Posix.link(), "the calls of the C library did not link");
        }

        private static Ledger openPooled(Path directory, IoPool pool) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool))
                    .segmentSize(SEGMENT)
                    .writeThrough(WriteThrough.Mode.DIRECT)
                    .build();
        }

        /// Commits on the calling virtual thread until a group is written through the descriptor of a segment, with
        /// the thread of `pool` held until `released` opens so that each commit writes its own group.
        ///
        /// @return the commits made, whose keys are `w0` onwards
        private static int reachDescriptor(Ledger ledger, Map<String, String> model, IoPool pool, CountDownLatch released) throws Exception {
            var log = log(ledger);
            var commits = reachWriteThrough(ledger, model);
            hold(pool, released);
            for (var attempt = 0; attempt < 400; attempt++) {
                commit(ledger, model, "w" + commits++);
                if (log.throughHandleForTests() instanceof WriteThrough.PosixHandle handle && handle.descriptorForTests() >= 0
                        && log.groupEnd() < SEGMENT / 2) {
                    return commits;
                }
            }
            return fail("no segment was written through a descriptor");
        }

        /// Keeps the only thread of `pool` from any other task until `released` opens, and returns once it runs, with
        /// every task queued before it done; for five minutes at most, so a commit that left its group to the pool fails
        /// an assertion rather than hanging the test.
        private static void hold(IoPool pool, CountDownLatch released) throws InterruptedException {
            var occupied = new CountDownLatch(1);
            ((PlatformIoPool) pool).executor().execute(() -> {
                occupied.countDown();
                try {
                    released.await(5, TimeUnit.MINUTES);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            });
            occupied.await();
        }

        /// {@return what `work` returns, run on a virtual thread}
        ///
        /// @throws Exception what `work` throws
        private static <T> T onVirtualThread(Callable<T> work) throws Exception {
            var task = new FutureTask<>(work);
            Thread.ofVirtual().start(task);
            try {
                return task.get(5, TimeUnit.MINUTES);
            } catch (ExecutionException exception) {
                switch (exception.getCause()) {
                    case Exception cause -> throw cause;
                    case Error cause -> throw cause;
                    case null, default -> throw exception;
                }
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

        @Test
        @DisplayName("are refused by a descriptor handle's channel that fails a write before it took one, whatever the descriptor took, and not after")
        void channelRefusal() throws Exception {
            var disk = CrashFileSystem.create();
            var refuse = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && refuse.get()) {
                    throw new IOException("Invalid argument");
                }
            });
            var file = disk.getPath("/segment");
            Files.write(file, new byte[4 * LogWriter.ALIGNMENT]);
            refuse.set(true);
            var way = new WriteThrough(WriteThrough.Kind.DIRECT, null);
            var block = new byte[LogWriter.ALIGNMENT];
            // The test's own thread is a platform thread, which writes through the channel
            var handle = new WriteThrough.PosixHandle(FileChannel.open(file, StandardOpenOption.WRITE), file, way);
            try {
                assertThrows(WriteThrough.Refused.class, () -> handle.write(block, block.length, 0));
                refuse.set(false);
                handle.write(block, block.length, 0);
                refuse.set(true);
                var failure = assertThrows(IOException.class, () -> handle.write(block, block.length, LogWriter.ALIGNMENT));
                assertFalse(failure instanceof WriteThrough.Refused, "a channel that took a write refused the way");
            } finally {
                handle.close();
            }
            // The channel of the next segment's handle took a write of the way already
            var next = new WriteThrough.PosixHandle(FileChannel.open(file, StandardOpenOption.WRITE), file, way);
            try {
                var failure = assertThrows(IOException.class, () -> next.write(block, block.length, 2L * LogWriter.ALIGNMENT));
                assertFalse(failure instanceof WriteThrough.Refused, "a channel of a proven way refused it");
                refuse.set(false);
                assertThrows(ClosedChannelException.class, () -> {
                    next.close();
                    next.write(block, block.length, 0);
                });
            } finally {
                next.close();
            }
        }

        @Test
        @DisplayName("leave a carrier free of descriptor writes, so a scheduler of one carrier writes every group through the channel")
        void descriptorLimit() {
            assertEquals(0, WriteThrough.Posix.limitOf(1));
            assertEquals(1, WriteThrough.Posix.limitOf(2));
            assertEquals(3, WriteThrough.Posix.limitOf(4));
            assertTrue(WriteThrough.Posix.LIMIT > 0 || !WriteThrough.Posix.ELIGIBLE,
                    "a process with no carrier to spare writes through descriptors");
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
        @DisplayName("know nothing without asking, and name no volume for it, until some answer is kept")
        void knownWithoutAsking() {
            var named = new AtomicInteger();
            var answers = new WriteThrough.Answers(directory -> {
                named.incrementAndGet();
                return directory.getName(0).toString();
            }, volume -> volume.equals("fua") ? FUA : null);
            assertFalse(answers.anyKnown());
            assertNull(answers.provesIfKnown(Path.of("fua", "store")));
            assertEquals(0, named.get(), "a volume was named while no answer was kept");
            assertEquals(0, answers.asks());
            assertTrue(answers.proves(Path.of("fua", "store")));
            assertTrue(answers.anyKnown());
            assertEquals(Boolean.TRUE, answers.provesIfKnown(Path.of("fua", "other")));
            assertNull(answers.provesIfKnown(Path.of("silent", "store")));
            assertEquals(1, answers.asks());
            answers.clear();
            assertFalse(answers.anyKnown());
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
        // The key space of the batches, resident, so their frames hold their values: a paged key space's would leave
        // them to value frames, whose number moves with the lengths of the values
        private static final Keyspace<String, String> ROWS = Keyspace.resident("rows", KeyCodec.utf8(), ValueCodec.utf8());

        private static Ledger open(Path directory, boolean encrypted, WriteThrough.Mode mode) {
            var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                    .keyspace(NAMES).keyspace(ROWS);
            return ((LedgerBuilder) builder).segmentSize(SEGMENT).writeThrough(mode).build();
        }

        /// {@return what `ledger` holds, of both key spaces}
        private static Map<String, String> contents(Ledger ledger) {
            var entries = new TreeMap<>(WriteThroughTest.contents(ledger));
            ledger.forEach(ROWS, KeyRange.all(), (key, value) -> {
                entries.put(key, value);
                return true;
            });
            return entries;
        }

        /// Commits `writes` values of `length` bytes, the last `extra` bytes longer, under the keys `prefix000`
        /// onwards, in one batch, durably, and records them in `model`.
        private static void commitBatch(Ledger ledger, Map<String, String> model, String prefix, int writes, int length, int extra) throws InterruptedException {
            var batch = ledger.newBatch();
            for (var i = 0; i < writes; i++) {
                var key = prefix + "%03d".formatted(i);
                var value = "v".repeat(i == writes - 1 ? length + extra : length);
                batch.put(ROWS, key, value);
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
        // The group was written from the largest output, which it gave back once durable with the queue empty
        private static void assertLargestGivenBack(LogWriter log) {
            assertEquals(LogWriter.ALIGNMENT, log.outputLength(), "the largest output was not given back");
            var spare = log.spareOutputForTests();
            assertNotNull(spare, "the group was not written from the largest output");
            assertEquals(LogWriter.OUTPUT, spare.length);
        }

        private static void survivesPowerLoss(CrashFileSystem disk, boolean encrypted, Map<String, String> model) {
            for (var seed = 0; seed < 4; seed++) {
                var survivor = disk.afterPowerLoss(new Random(seed));
                try (var ledger = open(survivor.getPath("/store"), encrypted, WriteThrough.Mode.OFF)) {
                    assertEquals(model, contents(ledger), "seed " + seed);
                }
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @CsvSource({"true", "false"})
        @DisplayName("keeps the write through's buffer across a group too large for one write and the small group after it, and replaces it at most once for medium groups")
        void bufferFollows(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var sizes = new ArrayList<Long>();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            way.allocatorForTests = size -> {
                synchronized (sizes) {
                    sizes.add(size);
                }
                return Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
            };
            var builder = Ledger.builder().directory(disk.getPath("/store")).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                    .keyspace(NAMES).keyspace(ROWS);
            try (var ledger = ((LedgerBuilder) builder).segmentSize(SEGMENT).writeThrough(way).build()) {
                var log = log(ledger);
                var fills = 0;
                while (log.currentSegment() < 4) {
                    commitBatch(ledger, model, "f" + fills++ + "-", 64, 4_000, 0);
                }
                ledger.checkpoint();
                reachRoom(ledger, model, "p");
                // The filler of a quarter of a megabyte or more grew the output, and the buffer of the groups written through with it
                var allocated = sizes.size();
                var filled = log.outputLength();
                assertTrue(filled > 250_000, "the filler made an output of " + filled + " bytes");
                assertTrue(allocated > 0, "no group of the filler was written through");
                // A group too large for one write, forced: the output goes back to a block, and the buffer is not used
                commitBatch(ledger, model, "o", 262, 4_000, 0);
                assertEquals(LogWriter.ALIGNMENT, log.outputLength());
                assertEquals(allocated, sizes.size());
                // A small group written through keeps the buffer, whatever the output it was written from
                var through = log.writtenThrough();
                commit(ledger, model, "s");
                assertEquals(through + 1, log.writtenThrough(), "the small group was not written through");
                assertEquals(allocated, sizes.size(), "the buffer was replaced for a small group");
                // Medium groups written through once the collector took the spare output: the output grows to the power
                // of two at or above their bound, and the buffer, if smaller, grows once
                log.dropSpareOutputForTests();
                commitBatch(ledger, model, "m", 32, 4_000, 0);
                assertEquals(through + 2, log.writtenThrough(), "the medium group was not written through");
                var medium = log.outputLength();
                assertEquals(128 << 10, medium, "a medium group made an output of " + medium + " bytes");
                assertTrue(sizes.size() <= allocated + 1, "buffers allocated: " + sizes);
                var afterMedium = sizes.size();
                assertTrue(way.bufferSizeForTests() >= 128_000, "a buffer of " + way.bufferSizeForTests() + " bytes");
                commitBatch(ledger, model, "n", 32, 4_000, 0);
                assertEquals(through + 3, log.writtenThrough(), "the second medium group was not written through");
                assertEquals(medium, log.outputLength());
                assertEquals(afterMedium, sizes.size());
                survivesPowerLoss(disk, encrypted, model);
            }
        }

        @Test
        @DisplayName("grows the write through's buffer by a quarter at least for a larger group, keeps it for smaller ones, and takes it back to their size only after 256 in a row that took at most half of it")
        void bufferHysteresis() throws Exception {
            var disk = CrashFileSystem.create();
            var file = disk.getPath("/segment");
            Files.write(file, new byte[64 << 10]);
            var sizes = new ArrayList<Long>();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            way.allocatorForTests = size -> {
                sizes.add(size);
                return Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
            };
            var handle = way.open(file);
            var random = new Random(5);
            try {
                // Group sizes in blocks, and the buffer each write leaves, in blocks
                int[][] steps = {{2, 2}, {1, 2}, {4, 4}, {3, 4}, {5, 5}, {6, 7}, {2, 7}};
                for (var step : steps) {
                    write(handle, file, random, step[0]);
                    assertEquals((long) step[1] * LogWriter.ALIGNMENT, way.bufferSizeForTests(), "after a group of " + step[0] + " blocks");
                }
                assertEquals(4, sizes.size(), "buffers allocated: " + sizes);
                // 256 groups in a row of at most half of it, the first above: it goes back to the largest of them
                for (var i = 1; i < 255; i++) {
                    write(handle, file, random, 3);
                    assertEquals(7L * LogWriter.ALIGNMENT, way.bufferSizeForTests(), "after " + i + " small groups");
                }
                write(handle, file, random, 3);
                assertEquals(3L * LogWriter.ALIGNMENT, way.bufferSizeForTests(), "after 256 small groups");
                assertEquals(5, sizes.size(), "buffers allocated: " + sizes);
                // Groups that alternate between one block and more than half of it keep it
                write(handle, file, random, 4);
                for (var i = 0; i < 600; i++) {
                    write(handle, file, random, i % 2 == 0 ? 1 : 3);
                }
                assertEquals(4L * LogWriter.ALIGNMENT, way.bufferSizeForTests());
                assertEquals(6, sizes.size(), "buffers allocated: " + sizes);
            } finally {
                handle.close();
            }
        }

        private static void write(WriteThrough.Handle handle, Path file, Random random, int blocks) throws IOException {
            var length = blocks * LogWriter.ALIGNMENT;
            var bytes = new byte[length + random.nextInt(2) * LogWriter.ALIGNMENT];
            random.nextBytes(bytes);
            handle.write(bytes, length, 0);
            var written = new byte[length];
            try (var channel = FileChannel.open(file, StandardOpenOption.READ)) {
                channel.read(ByteBuffer.wrap(written), 0);
            }
            assertTrue(Arrays.equals(bytes, 0, length, written, 0, length), "the bytes of a group of " + blocks + " blocks");
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
                log.probe().keepSpare = true;
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
                assertLargestGivenBack(log);
                survivesPowerLoss(disk, encrypted, model);

                reachRoom(ledger, model, "q");
                through = log.writtenThrough();
                forces = log.forces();
                commitBatch(ledger, model, "b", WRITES, length, extra + 1);
                assertEquals(LogWriter.ONE_WRITE + 1, lastGroup(log));
                assertEquals(through, log.writtenThrough(), "a group past the most one write takes was written through");
                assertEquals(forces + 1, log.forces(), "a group past the most one write takes was not forced once");
                assertLargestGivenBack(log);
                survivesPowerLoss(disk, encrypted, model);
            }
            try (var reopened = open(disk.getPath("/store"), encrypted, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        /// Commits sixteen paged values of `length` bytes, the last `extra` bytes longer, under the keys `prefix00`
        /// onwards, in one batch, durably, and records them in `model`: four of them fill each of four chunks of the
        /// batch's rope, so the commit's frame is split into its head and four value frames, one per chunk, whatever
        /// the lengths of a fifth of a chunk to a quarter.
        private static void commitSplit(Ledger ledger, Map<String, String> model, String prefix, int length, int extra) throws InterruptedException {
            var batch = ledger.newBatch();
            for (var i = 0; i < 16; i++) {
                var key = prefix + "%02d".formatted(i);
                var value = "s".repeat(i == 15 ? length + extra : length);
                batch.put(NAMES, key, value);
                model.put(key, value);
            }
            ledger.commit(batch).awaitDurable();
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @CsvSource({"true", "false"})
        @DisplayName("counts every frame of a split commit, its head and its value frames, against the most one write takes: writes through a split commit that seals into exactly that, forces one a byte larger, and both survive a loss of power")
        void splitBoundary(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), encrypted, WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var fills = 0;
                while (log.currentSegment() < 4) {
                    commitBatch(ledger, model, "f" + fills++ + "-", 64, 4_000, 0);
                }
                ledger.checkpoint();

                commitSplit(ledger, model, "c", 60_000, 0);
                var fixed = lastGroup(log) - 16L * 60_000;
                // The head and four value frames, each with a header
                assertTrue(fixed >= 5L * LogWriter.FRAME_HEADER, "the commit was not split: " + fixed);
                var length = (int) ((LogWriter.ONE_WRITE - fixed) / 16);
                var extra = (int) ((LogWriter.ONE_WRITE - fixed) % 16);
                assertTrue(4L * length + extra + 1 <= FrameBody.CHUNK_BYTES, "four values do not fit a rope chunk");

                reachRoom(ledger, model, "p");
                var through = log.writtenThrough();
                var forces = log.forces();
                commitSplit(ledger, model, "a", length, extra);
                assertEquals(LogWriter.ONE_WRITE, lastGroup(log));
                assertEquals(through + 1, log.writtenThrough(), "the split commit of the most one write takes was not written through");
                assertEquals(forces + 1, log.forces());
                survivesPowerLoss(disk, encrypted, model);

                reachRoom(ledger, model, "q");
                through = log.writtenThrough();
                forces = log.forces();
                commitSplit(ledger, model, "b", length, extra + 1);
                assertEquals(LogWriter.ONE_WRITE + 1, lastGroup(log));
                assertEquals(through, log.writtenThrough(), "a split commit past the most one write takes was written through");
                assertEquals(forces + 1, log.forces(), "a split commit past the most one write takes was not forced once");
                survivesPowerLoss(disk, encrypted, model);
            }
            try (var reopened = open(disk.getPath("/store"), encrypted, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }
    }

    @Nested
    @DisplayName("the first commits after an open")
    class Linger {
        // Far longer than any test waits, so a linger that ends within the wait ended early
        private static final long LONG_LINGER = TimeUnit.SECONDS.toNanos(30);
        private static final long WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);

        /// Writes a few durable commits into a new store at `directory` and closes it, so the next open writes into the
        /// segment the close kept.
        private static void seed(Path directory, WriteThrough.Mode mode, Map<String, String> model) throws InterruptedException {
            try (var ledger = open(directory, mode)) {
                for (var i = 0; i < 4; i++) {
                    commit(ledger, model, "s" + i);
                }
            }
        }

        private static Ledger reopen(Path directory, WriteThrough.Mode mode, long linger) {
            var ledger = open(directory, mode);
            log(ledger).probe().lingerNanos = linger;
            return ledger;
        }

        private static void await(BooleanSupplier condition, String message) {
            var deadline = System.nanoTime() + WAIT_NANOS;
            while (!condition.getAsBoolean()) {
                assertTrue(System.nanoTime() < deadline, message);
                Thread.onSpinWait();
            }
        }

        private static void deferred(Ledger ledger, Map<String, String> model, String key) {
            ledger.commit(ledger.newBatch().put(NAMES, key, "d" + key));
            model.put(key, "d" + key);
        }

        @Test
        @DisplayName("a deferred commit and the durable one after it make one group, written through by the durable commit's thread, and the linger ends with it")
        void oneGroup() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = reopen(directory, WriteThrough.Mode.DSYNC, LONG_LINGER)) {
                var log = log(ledger);
                ledger.commit(ledger.newBatch().deleteRange(NAMES, KeyRange.between("s0", "s2")));
                model.remove("s0");
                model.remove("s1");
                await(log::lingeringForTests, "the flush did not linger after the deferred commit");
                ledger.commit(ledger.newBatch().deleteRange(NAMES, KeyRange.between("s2", "s3"))).awaitDurable();
                model.remove("s2");
                assertEquals(1, log.forces(), "the two commits made more than one group");
                assertEquals(1, log.writtenThrough(), "their group was not written through");
                assertEquals(1, log.ledGroupsForTests(), "the durable commit did not write the group itself");
                assertEquals(1, log.lingersForTests());
                await(() -> !log.lingeringForTests(), "the linger outlasted the group that took its frames");
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("a deferred commit and the durable one after it make one group on the platform's file system")
        void oneGroupOnDisk(@TempDir Path directory) throws Exception {
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.AUTO, model);
            try (var ledger = reopen(directory, WriteThrough.Mode.AUTO, LONG_LINGER)) {
                var log = log(ledger);
                deferred(ledger, model, "a");
                await(log::lingeringForTests, "the flush did not linger after the deferred commit");
                commit(ledger, model, "b");
                assertEquals(1, log.forces(), "the two commits made more than one group");
                assertEquals(1, log.ledGroupsForTests(), "the durable commit did not write the group itself");
            }
            try (var reopened = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @DisplayName("a deferred commit no other follows is durable once the linger runs out, which it does once")
        void lone() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = open(directory, WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var commit = ledger.commit(ledger.newBatch().put(NAMES, "a", "da"));
                model.put("a", "da");
                await(commit::isDurable, "a lone deferred commit was never made durable");
                assertEquals(1, log.lingersForTests());
                assertEquals(1, log.forces());
                assertEquals(0, log.ledGroupsForTests());
                Thread.sleep(50);
                assertEquals(1, log.lingersForTests(), "the flush lingered again over a durable frame");
                assertFalse(log.lingeringForTests());
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("a durable commit that waits with a timeout, and so leaves its group to the flush, ends the linger")
        void timedWait() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = reopen(directory, WriteThrough.Mode.DSYNC, LONG_LINGER)) {
                var log = log(ledger);
                deferred(ledger, model, "a");
                await(log::lingeringForTests, "the flush did not linger after the deferred commit");
                var start = System.nanoTime();
                assertTrue(ledger.commit(ledger.newBatch().put(NAMES, "b", "db")).awaitDurable(Duration.ofNanos(2 * LONG_LINGER)));
                model.put("b", "db");
                assertTrue(System.nanoTime() - start < WAIT_NANOS, "the timed wait waited out the linger");
                assertEquals(1, log.forces(), "the two commits made more than one group");
                assertEquals(0, log.ledGroupsForTests(), "a timed wait wrote its own group");
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("the first large value after an open, whose epoch waits in the linger's queue, ends the linger")
        void epochGate(@TempDir Path directory) throws Exception {
            try (var ledger = openLarge(directory)) {
                ledger.commit(ledger.newBatch().put(NAMES, "s", "s")).awaitDurable();
            }
            try (var ledger = openLarge(directory)) {
                var log = log(ledger);
                log.probe().lingerNanos = LONG_LINGER;
                ledger.commit(ledger.newBatch().put(NAMES, "a", "da"));
                await(log::lingeringForTests, "the flush did not linger after the deferred commit");
                var start = System.nanoTime();
                ledger.commit(ledger.newBatch().put(NAMES, "large", "L".repeat(10_000))).awaitDurable();
                assertTrue(System.nanoTime() - start < WAIT_NANOS, "the large value waited out the linger");
                assertEquals(Optional.of("L".repeat(10_000)), ledger.get(NAMES, "large"));
            }
        }

        @Test
        @DisplayName("a close ends the linger and makes its frames durable")
        void close() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            var ledger = reopen(directory, WriteThrough.Mode.DSYNC, LONG_LINGER);
            var log = log(ledger);
            deferred(ledger, model, "a");
            await(log::lingeringForTests, "the flush did not linger after the deferred commit");
            var start = System.nanoTime();
            ledger.close();
            assertTrue(System.nanoTime() - start < WAIT_NANOS, "the close waited out the linger");
            assertEquals(1, log.forces());
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("a loss of power during the linger keeps the commits before it, and one after the durable commit keeps both")
        void powerLoss() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = reopen(directory, WriteThrough.Mode.DSYNC, LONG_LINGER)) {
                var log = log(ledger);
                var before = new TreeMap<>(model);
                deferred(ledger, model, "a");
                await(log::lingeringForTests, "the flush did not linger after the deferred commit");
                for (var seed = 0; seed < DRAWS; seed++) {
                    try (var survivor = open(disk.afterPowerLoss(new Random(seed)).getPath("/store"), WriteThrough.Mode.OFF)) {
                        assertEquals(before, contents(survivor), "seed " + seed);
                    }
                }
                commit(ledger, model, "b");
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("a stream of commits, durable or not, lingers at most once, before the open's first group, and writes a group per durable commit")
        void steady() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = open(directory, WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                // The open's first group, after which a durable commit's group holds its frame alone
                commit(ledger, model, "first");
                var groups = log.groupsForTests();
                for (var i = 0; i < 300; i++) {
                    commit(ledger, model, "c" + i);
                }
                assertEquals(300, log.groupsForTests() - groups, "the durable commits did not make a group each");
                for (var i = 0; i < 300; i++) {
                    deferred(ledger, model, "d" + i);
                }
                ledger.awaitDurable();
                assertTrue(log.lingersForTests() <= 1, log.lingersForTests() + " lingers in one open");
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("once the open's first group is written, a commit after an idle spell, deferred or durable, never lingers")
        void afterIdle() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = new TreeMap<String, String>();
            seed(directory, WriteThrough.Mode.DSYNC, model);
            try (var ledger = open(directory, WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                commit(ledger, model, "first");
                var lingers = log.lingersForTests();
                for (var i = 0; i < 20; i++) {
                    Thread.sleep(Duration.ofNanos(4 * LogWriter.LINGER_NANOS));
                    if (i % 2 == 0) {
                        var commit = ledger.commit(ledger.newBatch().put(NAMES, "d" + i, "dd" + i));
                        model.put("d" + i, "dd" + i);
                        await(commit::isDurable, "a deferred commit after the first group was never made durable");
                    } else {
                        commit(ledger, model, "c" + i);
                    }
                    assertFalse(log.lingeringForTests(), "a flush lingered after the open's first group");
                }
                assertEquals(lingers, log.lingersForTests(), "a flush lingered after the open's first group");
                survivesPowerLoss(disk, model);
            }
        }
    }
}
