package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// What the lazy forces of the log and its writes through make of each other, on the simulated file system: a group no
/// commit waits for stays in the cache, and the write through of the next one a commit does wait for takes it along, in
/// one write from where the last force ended. A delay of a minute stands for "never", so a test decides every force.
class LazyWriteThroughTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final long SEGMENT = 64 << 10;
    private static final Duration NEVER = Duration.ofMinutes(1);
    private static final int DRAWS = 6;

    private static LedgerBuilder builder(Path directory) {
        return (LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES)
                .durabilityDelay(NEVER);
    }

    private static Ledger open(Path directory, WriteThrough.Mode mode) {
        return builder(directory).segmentSize(SEGMENT).writeThrough(mode).build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static String value(String key) {
        return "v".repeat(500) + key;
    }

    private static Commit put(Ledger ledger, Map<String, String> model, String key) {
        var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value(key)));
        model.put(key, value(key));
        return commit;
    }

    private static void awaitWritten(Ledger ledger, Commit commit) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (log(ledger).writtenSequence() < commit.sequence()) {
            assertTrue(System.nanoTime() < deadline, "the flush never wrote the commit");
            Thread.sleep(1);
        }
    }

    /// Fills segments, makes them spares, and commits durably until a group goes through well inside a segment, so the
    /// next ones can too.
    ///
    /// @return the commits made, whose keys are `w0` onwards
    private static int reachWriteThrough(Ledger ledger, Map<String, String> model) throws InterruptedException {
        var log = log(ledger);
        var commits = 0;
        while (log.currentSegment() < 12) {
            put(ledger, model, "w" + commits++).awaitDurable();
        }
        ledger.checkpoint();
        return reachWellInside(ledger, model, commits);
    }

    /// Commits durably until a group goes through well inside a segment, then once without waiting: the group after two
    /// that commits waited for goes to the device at once, and the next stays in the cache.
    ///
    /// @return the commits made so far, `commits` of them before this call
    private static int reachWellInside(Ledger ledger, Map<String, String> model, int commits) throws InterruptedException {
        var log = log(ledger);
        for (var attempt = 0; attempt < 400; attempt++) {
            var before = log.writtenThrough();
            put(ledger, model, "w" + commits++).awaitDurable();
            if (log.writtenThrough() > before && log.groupEnd() < SEGMENT / 2) {
                awaitWritten(ledger, put(ledger, model, "w" + commits++));
                return commits;
            }
        }
        throw new AssertionError("no group was written through well inside a segment");
    }

    private static void survivesPowerLoss(CrashFileSystem disk, Map<String, String> model) {
        for (var seed = 0; seed < DRAWS; seed++) {
            var survivor = disk.afterPowerLoss(new Random(seed));
            try (var ledger = open(survivor.getPath("/store"), WriteThrough.Mode.OFF)) {
                var recovered = new TreeMap<String, String>();
                ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
                    recovered.put(key, value);
                    return true;
                });
                for (var entry : model.entrySet()) {
                    assertEquals(Optional.of(entry.getValue()), Optional.ofNullable(recovered.get(entry.getKey())),
                            "seed " + seed + ", key " + entry.getKey());
                }
            }
        }
    }

    /// Keeps the only thread of `pool` from any other task until `released` opens.
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
    @DisplayName("a group no commit waits for goes neither through nor to a force, however writable the segment")
    void staysInCache() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            var log = log(ledger);
            reachWriteThrough(ledger, model);
            var through = log.writtenThrough();
            var forces = log.forces();
            var last = put(ledger, new TreeMap<>(), "a");
            awaitWritten(ledger, last);
            assertEquals(through, log.writtenThrough());
            assertEquals(forces, log.forces());
            assertFalse(last.isDurable());
        }
    }

    @Test
    @DisplayName("the write through of a group a commit waits for takes the groups in the cache before it, in one durable write and no force")
    void takesAlong() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var forced = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                forced.incrementAndGet();
            }
        });
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            var log = log(ledger);
            var commits = reachWriteThrough(ledger, model);
            for (var round = 0; round < 3; round++) {
                commits = reachWellInside(ledger, model, commits);
                var first = put(ledger, model, "a" + round);
                awaitWritten(ledger, first);
                var second = put(ledger, model, "b" + round);
                awaitWritten(ledger, second);
                assertFalse(first.isDurable());
                var through = log.writtenThrough();
                var forces = log.forces();
                var seen = forced.get();
                var waited = put(ledger, model, "c" + round);
                waited.awaitDurable();
                assertEquals(through + 1, log.writtenThrough(), "the group was not written through");
                assertEquals(forces + 1, log.forces(), "a group written through counts as one force");
                assertEquals(1, forced.get() - seen, "one durable write of the segment, as the file system counts it");
                assertTrue(first.isDurable(), "the groups before it were left out");
                survivesPowerLoss(disk, model);
            }
        }
    }

    @Test
    @DisplayName("a wait for a group already in the cache has it written through, with the commit's own thread free to lead")
    void waitAfterWritten() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            var log = log(ledger);
            reachWriteThrough(ledger, model);
            var commit = put(ledger, model, "a");
            awaitWritten(ledger, commit);
            assertFalse(commit.isDurable());
            var through = log.writtenThrough();
            commit.awaitDurable();
            assertEquals(through + 1, log.writtenThrough());
            survivesPowerLoss(disk, model);
        }
    }

    @Test
    @DisplayName("the first commits of a store just opened share the write through of its first block, the mark of the close overwritten")
    void firstBlock() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            put(ledger, model, "before").awaitDurable();
        }
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            var log = log(ledger);
            var forces = log.forces();
            var first = ledger.commit(ledger.newBatch().put(NAMES, "x", "1"));
            model.put("x", "1");
            awaitWritten(ledger, first);
            var waited = ledger.commit(ledger.newBatch().put(NAMES, "y", "2"));
            model.put("y", "2");
            waited.awaitDurable();
            assertTrue(first.isDurable());
            assertEquals(forces + 1, log.forces(), "more than one durable write for the two commits");
            assertEquals(1, log.writtenThrough(), "the two commits were not written through");
            survivesPowerLoss(disk, model);
        }
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            assertEquals(Optional.of("1"), ledger.get(NAMES, "x"));
            assertEquals(Optional.of("2"), ledger.get(NAMES, "y"));
        }
    }

    @Test
    @DisplayName("an interrupt of a commit whose write through takes earlier groups along has them all written again, and the commit durable")
    void interrupted() throws Exception {
        var disk = CrashFileSystem.create();
        var committer = Thread.currentThread();
        var armed = new AtomicBoolean();
        var interrupts = new AtomicInteger();
        var model = new TreeMap<String, String>();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")
                    && Thread.currentThread() == committer && armed.compareAndSet(true, false)) {
                var interrupter = Thread.ofPlatform().start(committer::interrupt);
                while (interrupter.isAlive()) {
                    Thread.onSpinWait();
                }
                interrupts.incrementAndGet();
            }
        });
        try (var pool = IoPool.create(1);
             var ledger = ((LedgerBuilder) builder(disk.getPath("/store")).ioPool(pool)).segmentSize(SEGMENT).writeThrough(WriteThrough.Mode.DSYNC).build()) {
            var log = log(ledger);
            var commits = reachWriteThrough(ledger, model);
            for (var round = 0; round < 4; round++) {
                commits = reachWellInside(ledger, model, commits);
                var earlier = put(ledger, model, "a" + round);
                awaitWritten(ledger, earlier);
                var through = log.writtenThrough();
                var before = interrupts.get();
                var gate = new CountDownLatch(1);
                occupy(pool, gate);
                armed.set(true);
                put(ledger, model, "b" + round).awaitDurable();
                gate.countDown();
                Thread.interrupted();
                assertEquals(before + 1, interrupts.get(), "the commit left its group to the pool");
                assertEquals(through + 1, log.writtenThrough(), "the interrupt left the group to a force");
                assertTrue(earlier.isDurable());
                survivesPowerLoss(disk, model);
            }
            armed.set(false);
        }
        Thread.interrupted();
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            for (var entry : model.entrySet()) {
                assertEquals(Optional.of(entry.getValue()), ledger.get(NAMES, entry.getKey()));
            }
        }
    }

    @Test
    @DisplayName("a write through refused after a group in the cache has them all written again and forced, and the store forces from then on")
    void refused() throws Exception {
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
            var earlier = put(ledger, model, "a");
            awaitWritten(ledger, earlier);
            var through = log.writtenThrough();
            armed.set(true);
            put(ledger, model, "b").awaitDurable();
            assertEquals(1, refused.get());
            assertEquals(through, log.writtenThrough());
            assertTrue(earlier.isDurable());
            survivesPowerLoss(disk, model);
            for (var i = 0; i < 40; i++) {
                put(ledger, model, "w" + commits++).awaitDurable();
            }
            assertEquals(through, log.writtenThrough(), "a group was written through after a refused write");
            survivesPowerLoss(disk, model);
        }
    }

    @Test
    @DisplayName("frames in the cache past what the output keeps are forced with the next group: a write through never leaves any behind")
    void pastTheOutput() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        // Segments that hold what the output does not
        var segment = 2L << 20;
        try (var ledger = builder(disk.getPath("/store")).segmentSize(segment).writeThrough(WriteThrough.Mode.DSYNC).build()) {
            var log = log(ledger);
            var commits = 0;
            while (log.currentSegment() < 5) {
                var key = "w" + commits++;
                var value = "v".repeat(128 << 10) + key;
                ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                model.put(key, value);
            }
            ledger.checkpoint();
            // Well inside a spare, with room for the frames below
            for (var attempt = 0; ; attempt++) {
                assertTrue(attempt < 2_000, "no group was written through at the start of a spare");
                var before = log.writtenThrough();
                put(ledger, model, "w" + commits++).awaitDurable();
                if (log.writtenThrough() > before && log.groupEnd() < segment / 8) {
                    awaitWritten(ledger, put(ledger, model, "w" + commits++));
                    break;
                }
            }
            // More than the output, grown to 256 KiB for frames of this size, and less than the bytes that force them
            Commit last = null;
            for (var i = 0; i < 5; i++) {
                var key = "big" + i;
                var value = "v".repeat(128 << 10) + key;
                last = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                model.put(key, value);
                awaitWritten(ledger, last);
            }
            var through = log.writtenThrough();
            put(ledger, model, "after").awaitDurable();
            assertTrue(last.isDurable());
            assertEquals(through, log.writtenThrough(), "a write through cannot hold what the output gave up");
            survivesPowerLoss(disk, model);
        }
    }
}
