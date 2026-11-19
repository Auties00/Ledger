package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The size of the buffer the log seals its groups into: one block for a store of small commits, grown for larger
/// groups to the bound of the group in one step and taken back, after a run of small ones, to the size that run needed,
/// so groups of two sizes neither keep a large buffer nor make it, or the native buffer of a write through that follows
/// it, change size group after group; and given back once a group too large for one write is durable, to be taken back
/// by the next one while the collector has not.
class LogOutputSizeTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Small segments, so a checkpoint leaves spares a group may be written through into
    private static final long SEGMENT = 64 << 10;
    // The small groups in a row after which the buffer goes back to their size
    private static final int SHRINK_AFTER = 256;

    private static Ledger open(Path directory, boolean encrypted, WriteThrough way) {
        var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none()).keyspace(NAMES);
        return ((LedgerBuilder) builder).segmentSize(SEGMENT).writeThrough(way).build();
    }

    // Segments that hold groups of several megabytes without a rollover
    private static Ledger openLarge(Path directory, boolean encrypted, IoPool pool) {
        var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none()).keyspace(NAMES);
        if (pool != null) {
            builder.ioPool(pool);
        }
        return ((LedgerBuilder) builder).segmentSize(16 << 20).writeThrough(new WriteThrough(null, null)).build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key, int length) throws InterruptedException {
        var value = "v".repeat(length);
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    /// Commits `writes` values of about `length` bytes under the keys `prefix0` onwards, in one batch, durably, and
    /// records them in `model`.
    private static void commitBatch(Ledger ledger, Map<String, String> model, String prefix, int writes, int length) throws InterruptedException {
        var batch = ledger.newBatch();
        for (var i = 0; i < writes; i++) {
            var key = prefix + i;
            var value = "w".repeat(length) + i;
            batch.put(NAMES, key, value);
            model.put(key, value);
        }
        ledger.commit(batch).awaitDurable();
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    private static void survivesPowerLoss(CrashFileSystem disk, boolean encrypted, Map<String, String> model) {
        for (var seed = 0; seed < 4; seed++) {
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = open(survivor.getPath("/store"), encrypted, new WriteThrough(null, null))) {
                assertEquals(model, contents(ledger), "seed " + seed);
            }
        }
    }

    private static void survivesPowerLossLarge(CrashFileSystem disk, boolean encrypted, Map<String, String> model) {
        for (var seed = 0; seed < 4; seed++) {
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = openLarge(survivor.getPath("/store"), encrypted, null)) {
                assertEquals(model, contents(ledger), "seed " + seed);
            }
        }
    }

    /// Keeps the only thread of `pool` from any other task until `released` opens, so a commit writes its own group;
    /// the thread takes its next task after a minute at most.
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

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("grows once, to the bound of a group of one value of 256 KiB, and keeps that size for the next ones")
    void boundedByGroup(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = openLarge(disk.getPath("/store"), encrypted, null)) {
            var log = log(ledger);
            commit(ledger, model, "first", 500);
            var allocations = log.outputAllocationsForTests();
            commit(ledger, model, "large0", 256 << 10);
            var length = log.outputLength();
            assertTrue(length >= 256 << 10 && length <= (256 << 10) + 2 * LogWriter.ALIGNMENT,
                    "the output holds " + length + " bytes after a group of 256 KiB, where doubling made 512 KiB");
            assertEquals(0, length % LogWriter.ALIGNMENT);
            assertEquals(allocations + 1, log.outputAllocationsForTests(), "the output grew more than once for one group");
            for (var i = 1; i < 6; i++) {
                commit(ledger, model, "large" + i, 256 << 10);
                assertEquals(length, log.outputLength());
            }
            assertEquals(allocations + 1, log.outputAllocationsForTests(), "groups of the same size grew the output again");
            assertEquals(1, log.mostGroupGrowthsForTests());
        }
        survivesPowerLossLarge(disk, encrypted, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("grows at most once for a group of many small commits sealed as runs, whatever the groups the flush takes")
    void boundedByRuns(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = openLarge(disk.getPath("/store"), encrypted, null)) {
            var log = log(ledger);
            for (var round = 0; round < 4; round++) {
                // Commits queued without waiting, so the flush takes many of them at a time
                for (var i = 0; i < 3_000; i++) {
                    var key = "r" + round + "-" + i;
                    var value = "v".repeat(100 + (i * 37) % 900);
                    ledger.commit(ledger.newBatch().put(NAMES, key, value));
                    model.put(key, value);
                }
                ledger.awaitDurable();
                assertTrue(log.mostGroupGrowthsForTests() <= 1, "the output grew " + log.mostGroupGrowthsForTests() + " times for one group");
            }
            assertTrue(log.outputAllocationsForTests() > 0, "no group grew the output");
        }
        survivesPowerLossLarge(disk, encrypted, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("gives the output back after a group written in several calls, takes it back for the next one, and not for a medium one")
    void givesBack(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = openLarge(disk.getPath("/store"), encrypted, null)) {
            var log = log(ledger);
            commit(ledger, model, "first", 500);
            // 300 values of 4 KB: a group of 1.2 MB, more than one write takes
            commitBatch(ledger, model, "a", 300, 4_000);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength(), "the output was not given back");
            var spare = log.spareOutputForTests();
            assertNotNull(spare);
            assertEquals(LogWriter.OUTPUT, spare.length);
            var allocations = log.outputAllocationsForTests();
            commit(ledger, model, "small", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
            // A medium group grows an output of its own, leaving the spare to the collector
            commitBatch(ledger, model, "m", 25, 4_000);
            var medium = log.outputLength();
            assertTrue(medium > 100_000 && medium < 110_000, "a medium group made an output of " + medium + " bytes");
            assertEquals(allocations + 1, log.outputAllocationsForTests());
            commitBatch(ledger, model, "b", 300, 4_000);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
            assertSame(spare, log.spareOutputForTests(), "the next large group did not take the spare back");
            assertEquals(allocations + 1, log.outputAllocationsForTests(), "the next large group allocated an output");
            // Once the collector took the spare, the next large group allocates one output of the largest size
            log.dropSpareOutputForTests();
            assertNull(log.spareOutputForTests());
            commitBatch(ledger, model, "c", 300, 4_000);
            assertEquals(allocations + 2, log.outputAllocationsForTests(), "a large group grew its output more than once");
            var replaced = log.spareOutputForTests();
            assertNotNull(replaced);
            assertNotSame(spare, replaced);
            assertEquals(LogWriter.OUTPUT, replaced.length);
            // Groups of every size after the spare was used again, their padding over the bytes of earlier groups
            for (var i = 0; i < 8; i++) {
                commit(ledger, model, "after" + i, 300 + 700 * i);
                commitBatch(ledger, model, "d" + i + "-", 270 + 10 * i, 4_000);
            }
            assertTrue(log.mostGroupGrowthsForTests() <= 1, "the output grew " + log.mostGroupGrowthsForTests() + " times for one group");
            survivesPowerLossLarge(disk, encrypted, model);
        }
        try (var reopened = openLarge(disk.getPath("/store"), encrypted, null)) {
            assertEquals(model, contents(reopened));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("keeps the output of a commit's own group of just under one write through a force an interrupt cut off, which writes it again")
    void interruptedOwnGroup(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var committer = Thread.currentThread();
        var armed = new AtomicBoolean();
        var interrupts = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")
                    && Thread.currentThread() == committer && armed.compareAndSet(true, false)) {
                var interrupter = Thread.ofPlatform().start(committer::interrupt);
                try {
                    interrupter.join();
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
                interrupts.incrementAndGet();
            }
        });
        var released = new CountDownLatch(1);
        try (var pool = IoPool.create(1); var ledger = openLarge(disk.getPath("/store"), encrypted, pool)) {
            var log = log(ledger);
            occupy(pool, released);
            for (var i = 0; i < 6; i++) {
                armed.set(true);
                // 250 values of 4 KB: about a megabyte, which one write still takes
                var batch = ledger.newBatch();
                for (var j = 0; j < 250; j++) {
                    var key = "k" + i + "-" + j;
                    var value = "w".repeat(4_000) + j;
                    batch.put(NAMES, key, value);
                    model.put(key, value);
                }
                var commit = ledger.commit(batch);
                try {
                    commit.awaitDurable();
                } catch (InterruptedException _) {
                    commit.awaitDurable();
                }
                assertTrue(commit.isDurable());
                Thread.interrupted();
                assertTrue(log.outputLength() > 1_000_000 && log.outputLength() <= LogWriter.OUTPUT,
                        "the output of a group one write takes holds " + log.outputLength() + " bytes");
                assertNull(log.spareOutputForTests(), "a group one write takes gave its output back");
            }
            armed.set(false);
            released.countDown();
            assertEquals(6, interrupts.get(), "a commit's force went by without the interrupt");
            survivesPowerLossLarge(disk, encrypted, model);
        }
        Thread.interrupted();
        try (var reopened = openLarge(disk.getPath("/store"), encrypted, null)) {
            assertEquals(model, contents(reopened));
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("keeps the output of a group written in several calls whose force failed, and stops the log")
    void failedForceKeepsOutput(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                throw new IOException("The device failed");
            }
        });
        var ledger = openLarge(disk.getPath("/store"), encrypted, null);
        CrashFileSystem survivor;
        try {
            var log = log(ledger);
            commitBatch(ledger, model, "a", 300, 4_000);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
            var spare = log.spareOutputForTests();
            assertNotNull(spare);
            commit(ledger, model, "small", 500);
            failing.set(true);
            var batch = ledger.newBatch();
            for (var i = 0; i < 300; i++) {
                batch.put(NAMES, "b" + i, "w".repeat(4_000) + i);
            }
            var commit = ledger.commit(batch);
            assertThrows(LedgerException.class, commit::awaitDurable);
            assertSame(spare, log.spareOutputForTests(), "a group whose force failed took another output");
            assertEquals(LogWriter.OUTPUT, log.outputLength(), "a group whose force failed gave its output back");
            survivor = disk.afterPowerLoss(new Random(3));
        } finally {
            failing.set(false);
            try {
                ledger.close();
            } catch (LedgerException _) {
                // The log stopped with a group it could not make durable
            }
        }
        try (var reopened = openLarge(survivor.getPath("/store"), encrypted, null)) {
            var recovered = contents(reopened);
            for (var entry : model.entrySet()) {
                assertEquals(entry.getValue(), recovered.get(entry.getKey()), entry.getKey());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}, through: {1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    @DisplayName("settles for groups of about 3 and 6 KB at the size they need, without changing it or the write through's buffer again")
    void settles(boolean encrypted, boolean through) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var allocations = new AtomicInteger();
        var way = new WriteThrough(through ? WriteThrough.Kind.DSYNC : null, null);
        way.allocatorForTests = size -> {
            allocations.incrementAndGet();
            return Arena.ofAuto().allocate(size, LogWriter.ALIGNMENT);
        };
        try (var ledger = open(disk.getPath("/store"), encrypted, way)) {
            var log = log(ledger);
            commit(ledger, model, "first", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength(), "a store of small commits keeps a buffer of one block");
            var commits = 0;
            // Segments enough for a checkpoint to leave spares, which groups are written through into
            while (log.currentSegment() < 8) {
                commit(ledger, model, "w" + commits++, 500);
            }
            ledger.checkpoint();
            var throughBefore = log.writtenThrough();
            var allocationsBefore = allocations.get();
            var changes = 0;
            var length = log.outputLength();
            for (var i = 0; i < 600; i++) {
                commit(ledger, model, "a" + i, i % 2 == 0 ? 3_000 : 6_000);
                if (log.outputLength() != length) {
                    changes++;
                    length = log.outputLength();
                }
            }
            assertTrue(changes <= 2, "the buffer changed size " + changes + " times");
            assertEquals(2 * LogWriter.ALIGNMENT, length, "groups of up to two blocks keep a buffer of two");
            if (through) {
                assertTrue(log.writtenThrough() > throughBefore, "no group was written through");
                assertTrue(allocations.get() - allocationsBefore <= 2,
                        "the write through's buffer was replaced " + (allocations.get() - allocationsBefore) + " times");
            }
        }
        survivesPowerLoss(disk, encrypted, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("goes back to one block after a large group once a run of small groups follows it, and not before")
    void shrinks(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), encrypted, new WriteThrough(null, null))) {
            var log = log(ledger);
            commit(ledger, model, "large", 40_000);
            var grown = log.outputLength();
            assertTrue(grown > 40_000, "the buffer holds " + grown + " bytes after a group of 40 KB");
            for (var i = 0; i < SHRINK_AFTER; i++) {
                commit(ledger, model, "s" + i, 500);
                assertEquals(grown, log.outputLength(), "the buffer shrank after " + (i + 1) + " small groups");
            }
            commit(ledger, model, "next", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
            // A group too large to count as small starts the run again
            commit(ledger, model, "again", 20_000);
            for (var i = 0; i < SHRINK_AFTER - 1; i++) {
                commit(ledger, model, "t" + i, 500);
            }
            commit(ledger, model, "medium", 30_000);
            grown = log.outputLength();
            for (var i = 0; i < SHRINK_AFTER; i++) {
                commit(ledger, model, "u" + i, 500);
            }
            assertEquals(grown, log.outputLength(), "a large group in the run did not start it again");
            commit(ledger, model, "last", 500);
            assertEquals(LogWriter.ALIGNMENT, log.outputLength());
        }
        survivesPowerLoss(disk, encrypted, model);
    }
}
