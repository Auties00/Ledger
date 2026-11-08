package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The log's writes through: a group that one write holds is durable once the write returns, with no force after
/// it, on the file system that simulates failures and on the real one; an interrupt of the write has it written
/// again, and a write or a probe the file system refuses leaves the store to forces.
///
/// The simulated file system makes the writes of a channel opened with `DSYNC` durable as they return, so a store
/// there writes through only when asked for [WriteThrough.Mode#DSYNC]. A store writes through only into a segment
/// that holds more than its first block, a spare or a segment written with zeros, so each test first fills segments,
/// checkpoints to turn them into spares, and commits until the log writes into one.
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
                for (var i = 0; i < 40; i++) {
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
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
                var log = log(ledger);
                var commits = reachWriteThrough(ledger, model);
                var cutThrough = 0;
                for (var i = 0; i < 60; i++) {
                    armed.set(true);
                    var key = "w" + commits++;
                    var value = "v".repeat(500) + key;
                    var through = log.writtenThrough();
                    var before = interrupts.get();
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
                    if (interrupts.get() > before && log.writtenThrough() > through) {
                        cutThrough++;
                    }
                }
                armed.set(false);
                assertTrue(cutThrough > 0, "no interrupt cut a commit's own write through short");
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

        @Test
        @DisplayName("a probe the file system refuses leaves the store to forces before any group depends on it")
        void refusedProbe() throws Exception {
            var disk = CrashFileSystem.create();
            var seen = ConcurrentHashMap.<String>newKeySet();
            var refused = new AtomicInteger();
            var model = new TreeMap<String, String>();
            disk.intercept((path, kind) -> {
                // Until one is refused, the first force of a segment longer than two blocks is the probe of its write
                // through: a segment is forced under another name while it is prepared, and groups of a block each
                // leave a new one at two blocks until its first force
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && seen.add(path.toString())
                        && refused.get() == 0 && Files.size(path) > LogWriter.KEPT_LENGTH) {
                    refused.incrementAndGet();
                    throw new IOException("Invalid argument");
                }
            });
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
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
                assertEquals(1, refused.get(), "no segment was probed");
                assertEquals(0, log.writtenThrough());
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
            if (expectsWriteThrough(mode)) {
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
            var handle = WriteThrough.of(WriteThrough.Mode.AUTO, directory).open(file);
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

        /// {@return whether this platform writes through in `mode` on the default file system}
        private static boolean expectsWriteThrough(WriteThrough.Mode mode) {
            var os = System.getProperty("os.name", "");
            if (mode == WriteThrough.Mode.OFF || os.startsWith("Mac")) {
                return false;
            }
            if (os.startsWith("Windows")) {
                return mode == WriteThrough.Mode.DSYNC || WriteThrough.class.getModule().isNativeAccessEnabled();
            }
            return os.equals("Linux") || mode != WriteThrough.Mode.AUTO;
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
