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
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The opening of the first segment of a store just opened to write through, on a thread of [SegmentPreparers] while
/// the store opens: the first group takes the handle, a segment that rolls over or a store that closes first closes it,
/// and the group is durable whichever way it went. On the simulated file system a test makes a channel opened with
/// `DSYNC` open early, as Windows' unbuffered way does.
class EarlyThroughTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Fifteen groups of a block each to a segment
    private static final long SEGMENT = 64 << 10;
    private static final int DRAWS = 8;

    private static Ledger open(Path directory, WriteThrough way) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(way)
                .build();
    }

    private static Ledger open(Path directory, WriteThrough.Mode mode) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(mode)
                .build();
    }

    /// {@return a channel opened with `DSYNC` that opens the first segment of a store early}
    private static WriteThrough early() {
        var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
        way.opensEarlyForTests = true;
        return way;
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key, int length) throws InterruptedException {
        var value = "v".repeat(length) + key;
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

    /// Holds every force of a segment being prepared on `disk`, from the first write of a group, until `hold` counts
    /// down, so no preparation flushes what a test looks at.
    private static void holdPreparations(CrashFileSystem disk, CountDownLatch hold) {
        var written = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            var name = path.toString();
            if (kind == ChangeInterceptor.ChangeKind.WRITE && name.endsWith(".log")) {
                written.set(true);
            } else if (kind == ChangeInterceptor.ChangeKind.FORCE && written.get() && (name.endsWith(".prep") || name.endsWith(".spare"))) {
                try {
                    if (!hold.await(60, TimeUnit.SECONDS)) {
                        throw new IOException("Held for too long");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new IOException(exception);
                }
            }
        });
    }

    private static void awaitTrue(BooleanSupplier condition, String message) throws IOException, InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IOException(message);
            }
            Thread.sleep(1);
        }
    }

    /// {@return a store on `disk` that committed once and closed, keeping its next segment}
    private static TreeMap<String, String> closedStore(CrashFileSystem disk) throws InterruptedException {
        var model = new TreeMap<String, String>();
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.DSYNC)) {
            commit(ledger, model, "before", 500);
        }
        return model;
    }

    @Nested
    @DisplayName("on a file system that simulates failures")
    class Simulated {
        @Test
        @DisplayName("opens the kept segment while the store opens, and its first group writes through that handle and survives a loss of power")
        void firstGroupTakesHandle() throws Exception {
            var disk = CrashFileSystem.create();
            var model = closedStore(disk);
            var way = early();
            var hold = new CountDownLatch(1);
            try (var ledger = open(disk.getPath("/store"), way)) {
                awaitTrue(() -> way.opensForTests() == 1, "the segment was not opened before a commit");
                holdPreparations(disk, hold);
                try {
                    commit(ledger, model, "first", 500);
                    assertEquals(1, log(ledger).writtenThrough());
                    assertEquals(1, way.opensForTests(), "the group opened a handle of its own");
                    survivesPowerLoss(disk, model);
                } finally {
                    hold.countDown();
                }
            }
            assertEquals(way.opensForTests(), way.closesForTests());
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(ledger));
            }
        }

        @ParameterizedTest(name = "rolled over: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("closes the handle no group takes, when the store moves to the next segment or closes first, and every group is durable")
        void unusedHandleClosed(boolean rolledOver) throws Exception {
            var disk = CrashFileSystem.create();
            var model = closedStore(disk);
            var way = early();
            try (var ledger = open(disk.getPath("/store"), way)) {
                var log = log(ledger);
                awaitTrue(() -> way.opensForTests() == 1, "the segment was not opened before a commit");
                var segment = log.currentSegment();
                // Larger than the one block the kept segment has room for, so it is forced
                commit(ledger, model, "large", 6_000);
                assertEquals(0, log.writtenThrough());
                if (rolledOver) {
                    var commits = 0;
                    while (log.currentSegment() == segment) {
                        commit(ledger, model, "w" + commits++, 6_000);
                    }
                    assertEquals(1, way.closesForTests(), "the handle of the segment left was not closed");
                }
                survivesPowerLoss(disk, model);
            }
            assertEquals(way.opensForTests(), way.closesForTests());
            try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(ledger));
            }
        }

        @Test
        @DisplayName("leaves no handle open when stores close right after they open, whether or not the opening ran yet")
        void closeRightAfterOpen() throws Exception {
            var disk = CrashFileSystem.create();
            var model = closedStore(disk);
            var way = early();
            for (var round = 0; round < 50; round++) {
                try (var ledger = open(disk.getPath("/store"), way)) {
                    assertEquals(model, contents(ledger));
                }
                assertEquals(way.opensForTests(), way.closesForTests(), "handles left open after close " + round);
            }
        }

        /// {@return whether `closer` waits in the close for an opening it gave up, past marking it given up}
        private static boolean awaitsOpening(Thread closer) {
            return closer.getState() == Thread.State.WAITING && Arrays.stream(closer.getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals("discardAndAwait"));
        }

        @ParameterizedTest(name = "rolled over: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("waits in the close for an opening still naming the volume, which then opens nothing, so no handle outlives the store")
        void closeWaitsForOpening(boolean rolledOver) throws Exception {
            var disk = CrashFileSystem.create();
            var model = closedStore(disk);
            var asking = new CountDownLatch(1);
            var answer = new CountDownLatch(1);
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, () -> true, () -> {
                asking.countDown();
                try {
                    answer.await();
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
                return true;
            });
            way.opensEarlyForTests = true;
            var ledger = open(disk.getPath("/store"), way);
            try {
                assertTrue(asking.await(60, TimeUnit.SECONDS), "the opening did not start");
                if (rolledOver) {
                    var log = log(ledger);
                    var segment = log.currentSegment();
                    var commits = 0;
                    // Forced groups, larger than the one block of the kept segment, so none waits for the opening
                    while (log.currentSegment() == segment) {
                        commit(ledger, model, "w" + commits++, 6_000);
                    }
                }
            } catch (Throwable throwable) {
                answer.countDown();
                ledger.close();
                throw throwable;
            }
            var closer = Thread.ofPlatform().start(ledger::close);
            // The close gives the opening up only once it reaches it, which a loaded machine may take well over 100 ms
            // to do; an answer given before would let the opening open the segment
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!awaitsOpening(closer)) {
                assertTrue(closer.isAlive(), "the close returned while the opening ran");
                assertTrue(System.nanoTime() < deadline, "the close never waited for the opening");
                Thread.sleep(1);
            }
            answer.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(60));
            assertTrue(!closer.isAlive(), "the close did not return");
            assertEquals(0, way.opensForTests(), "an opening given up while it named the volume opened the segment");
            assertEquals(0, way.closesForTests());
            try (var reopened = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(reopened));
            }
        }

        @Test
        @DisplayName("is not started by a store whose first segment is new, nor by a way that does not open early")
        void notStarted() throws Exception {
            var disk = CrashFileSystem.create();
            var fresh = early();
            var model = new TreeMap<String, String>();
            try (var ledger = open(disk.getPath("/store"), fresh)) {
                assertEquals(0, fresh.opensForTests());
                commit(ledger, model, "first", 500);
            }
            var late = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            try (var ledger = open(disk.getPath("/store"), late)) {
                Thread.sleep(20);
                assertEquals(0, late.opensForTests());
                commit(ledger, model, "second", 500);
                assertEquals(1, late.opensForTests());
                assertEquals(1, log(ledger).writtenThrough());
            }
        }
    }

    @Nested
    @DisplayName("the opening on its own")
    class Opening {
        private static LogFiles.Prepared segment(CrashFileSystem disk) throws IOException {
            var path = disk.getPath("/segment");
            Files.write(path, new byte[(int) LogWriter.KEPT_LENGTH]);
            return new LogFiles.Prepared(1, path, new byte[16], LogWriter.KEPT_LENGTH, true);
        }

        @Test
        @DisplayName("opens nothing once discarded before it runs")
        void discardedBeforeRun() throws Exception {
            var disk = CrashFileSystem.create();
            var way = early();
            var early = new LogWriter.EarlyThrough(way, segment(disk));
            early.discard();
            early.run();
            assertNull(early.get());
            assertEquals(0, way.opensForTests());
        }

        @Test
        @DisplayName("opens nothing when discarded while it names the volume, and closes one it opened when discarded after")
        void discardedWhileRunning() throws Exception {
            var disk = CrashFileSystem.create();
            var asking = new CountDownLatch(1);
            var answer = new CountDownLatch(1);
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, () -> true, () -> {
                asking.countDown();
                try {
                    answer.await();
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
                return true;
            });
            var running = new LogWriter.EarlyThrough(way, segment(disk));
            var thread = Thread.ofPlatform().start(running);
            assertTrue(asking.await(60, TimeUnit.SECONDS));
            running.discard();
            answer.countDown();
            thread.join();
            assertNull(running.get());
            assertEquals(0, way.opensForTests());

            var after = new LogWriter.EarlyThrough(way, segment(disk));
            after.run();
            assertEquals(1, way.opensForTests());
            after.discard();
            after.discard();
            assertEquals(1, way.closesForTests());
        }

        @Test
        @DisplayName("a discard that awaits runs an opening not started yet, which opens nothing, and waits for one running")
        void discardAndAwait() throws Exception {
            var disk = CrashFileSystem.create();
            var unstarted = early();
            var queued = new LogWriter.EarlyThrough(unstarted, segment(disk));
            queued.discardAndAwait();
            assertTrue(queued.isDone());
            assertEquals(0, unstarted.opensForTests());

            var asking = new CountDownLatch(1);
            var answer = new CountDownLatch(1);
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, () -> true, () -> {
                asking.countDown();
                try {
                    answer.await();
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
                return true;
            });
            var running = new LogWriter.EarlyThrough(way, segment(disk));
            var thread = Thread.ofPlatform().start(running);
            assertTrue(asking.await(60, TimeUnit.SECONDS));
            var waiter = Thread.ofPlatform().start(running::discardAndAwait);
            // Parked in the wait, so the discard is set before the opening goes on, however late the thread started
            var parked = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (waiter.getState() != Thread.State.WAITING && System.nanoTime() < parked) {
                Thread.onSpinWait();
            }
            waiter.join(100);
            assertTrue(waiter.isAlive(), "the discard returned while the opening ran");
            answer.countDown();
            waiter.join(TimeUnit.SECONDS.toMillis(60));
            thread.join(TimeUnit.SECONDS.toMillis(60));
            assertTrue(!waiter.isAlive() && !thread.isAlive());
            assertEquals(0, way.opensForTests());

            var opened = new LogWriter.EarlyThrough(way, segment(disk));
            opened.run();
            assertEquals(1, way.opensForTests());
            opened.discardAndAwait();
            assertEquals(1, way.closesForTests());
        }

        @Test
        @DisplayName("asks no device: a proof the process cannot answer yet leaves the segment to the first group")
        void asksNoDevice() throws Exception {
            var disk = CrashFileSystem.create();
            var asked = new AtomicBoolean();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, () -> {
                asked.set(true);
                return true;
            }, () -> null);
            var early = new LogWriter.EarlyThrough(way, segment(disk));
            early.run();
            assertNull(early.get());
            assertEquals(0, way.opensForTests());
            assertTrue(!asked.get(), "the device was asked");
            assertTrue(way.enabled());
            assertTrue(asked.get());
        }

        @Test
        @DisplayName("takes a known answer that a write through does not hold as the device's, and opens nothing")
        void knownRefusal() throws Exception {
            var disk = CrashFileSystem.create();
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, () -> {
                throw new AssertionError("the device was asked");
            }, () -> false);
            var early = new LogWriter.EarlyThrough(way, segment(disk));
            early.run();
            assertNull(early.get());
            assertNull(way.kind());
            assertEquals(0, way.opensForTests());
        }
    }

    @Nested
    @DisplayName("on the default file system")
    class Real {
        @ParameterizedTest(name = "{0}")
        @EnumSource(value = WriteThrough.Mode.class, names = {"AUTO", "DIRECT"})
        @EnabledOnOs(OS.WINDOWS)
        @DisplayName("on Windows a store just opened has its kept segment opened unbuffered before its first commit, which writes through it where the device proves it holds")
        void windows(WriteThrough.Mode mode, @TempDir Path directory) throws Exception {
            Assumptions.assumeTrue(WriteThrough.class.getModule().isNativeAccessEnabled(), "the tests run without native access");
            var model = new TreeMap<String, String>();
            try (var ledger = open(directory, mode)) {
                commit(ledger, model, "before", 500);
            }
            if (mode == WriteThrough.Mode.AUTO) {
                // The opening takes only an answer the process keeps
                var cache = WriteThrough.WriteCache.of(directory);
                Assumptions.assumeTrue(cache != null && cache.lastsForProcess(), "the device under the temporary directory gives no lasting answer");
            }
            var proved = mode == WriteThrough.Mode.DIRECT || WriteThrough.WriteCache.proves(directory);
            var asks = WriteThrough.WriteCache.asksForTests();
            var way = WriteThrough.of(mode, directory);
            assertTrue(way.opensEarly());
            try (var ledger = open(directory, way)) {
                var log = log(ledger);
                if (proved) {
                    awaitTrue(() -> way.opensForTests() == 1, "the segment was not opened before a commit");
                } else {
                    awaitTrue(() -> way.kind() == null, "the known answer was not taken");
                }
                commit(ledger, model, "first", 500);
                assertEquals(proved ? 1 : 0, log.writtenThrough());
                assertEquals(proved ? 1 : 0, way.opensForTests());
            }
            assertEquals(asks, WriteThrough.WriteCache.asksForTests(), "a device was asked again");
            assertEquals(way.opensForTests(), way.closesForTests());
            try (var ledger = open(directory, WriteThrough.Mode.OFF)) {
                assertEquals(model, contents(ledger));
            }
        }
    }
}
