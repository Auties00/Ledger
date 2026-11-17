package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Commits, opens, reads and closes on platform threads while an application's virtual threads compute on every
/// carrier, which nothing preempts: none of them may wait for a virtual thread of the store's that never starts, so each
/// ends with no carrier free. A store's virtual threads start once the application's let go of the carriers, after
/// each case.
@Timeout(600)
class BusyCarriersTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.utf8());
    // Past which a value goes to the file of large values
    private static final int LARGE = 4_096;
    // Values of 1,400 bytes put two entries in a leaf, so the loader takes several runs
    private static final int SESSION_ENTRIES = 1_200;
    private static final int NOTE_ENTRIES = 40_000;

    /// {@return what `work` returns, run on a platform thread while virtual threads compute on every carrier}
    ///
    /// @throws Exception what `work` throws
    static <T> T callWithBusyCarriers(Callable<T> work) throws Exception {
        var carriers = Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors());
        var stop = new AtomicBoolean();
        var computing = new CountDownLatch(carriers);
        var threads = new ArrayList<Thread>();
        try {
            for (var i = 0; i < carriers; i++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    computing.countDown();
                    while (!stop.get()) {
                        Thread.onSpinWait();
                    }
                }));
            }
            assertTrue(computing.await(30, TimeUnit.SECONDS), "the virtual threads did not all get a carrier");
            var task = new FutureTask<>(work);
            Thread.ofPlatform().daemon().start(task);
            try {
                // Time for the work on a machine with every processor busy, far from the moment it would hang
                return task.get(120, TimeUnit.SECONDS);
            } catch (TimeoutException _) {
                return fail("the work waits for a carrier");
            } catch (ExecutionException exception) {
                switch (exception.getCause()) {
                    case Exception cause -> throw cause;
                    case Error cause -> throw cause;
                    case null, default -> throw exception;
                }
            }
        } finally {
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
        }
    }

    /// Runs `work` on a platform thread while virtual threads compute on every carrier, and checks that it ends.
    ///
    /// @throws Exception what `work` throws
    static void assertEndsWithBusyCarriers(Runnable work) throws Exception {
        callWithBusyCarriers(Executors.callable(work));
    }

    private static byte[] bytes(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static String session(int index) {
        return "s%05d".formatted(index);
    }

    private static String sessionValue(int index) {
        return "v" + index + "-" + "x".repeat(1_400);
    }

    private static String note(int index) {
        return "n%06d".formatted(index);
    }

    private static Ledger openFiles(Path directory, IoPool pool) {
        var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(FILES).largeValues(LARGE);
        if (pool != null) {
            options.ioPool(pool);
        }
        return options.build();
    }

    private static Ledger openSessions(Path directory, boolean atOpen) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(SESSIONS).keyspace(FILES)
                .largeValues(LARGE).loadResidentAtOpen(atOpen).build();
    }

    /// Writes every session and closes the store, whose final checkpoint holds them.
    ///
    /// @return the sessions
    private static TreeMap<String, String> populateSessions(Path directory) {
        var model = new TreeMap<String, String>();
        try (var ledger = openSessions(directory, false)) {
            for (var start = 0; start < SESSION_ENTRIES; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < start + 100; i++) {
                    batch.put(SESSIONS, session(i), sessionValue(i));
                    model.put(session(i), sessionValue(i));
                }
                ledger.commit(batch);
            }
        }
        return model;
    }

    private static TreeMap<String, String> contents(Ledger ledger, Keyspace<String, String> keyspace) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    @ParameterizedTest(name = "pool of one thread: {0}, the commit writing its own group: {1}")
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    @DisplayName("a durable commit of a large value ends, the file of large values forced on a platform thread")
    void largeValue(boolean pool, boolean ownGroup, @TempDir Path directory) throws Exception {
        try (var ioPool = pool ? IoPool.create(1) : null; var ledger = openFiles(directory, ioPool)) {
            ledger.commit(ledger.newBatch().put(FILES, "first", bytes(1, 2 * LARGE))).awaitDurable();
            // A commit that waits without a timeout writes its own group if no flush is writing; one with a timeout
            // leaves it to the flush threads
            assertTrue(callWithBusyCarriers(() -> {
                var commit = ledger.commit(ledger.newBatch().put(FILES, "large", bytes(2, 2 * LARGE)));
                if (!ownGroup) {
                    return commit.awaitDurable(Duration.ofSeconds(60));
                }
                commit.awaitDurable();
                return true;
            }));
            assertArrayEquals(bytes(2, 2 * LARGE), ledger.get(FILES, "large").orElseThrow());
        }
        try (var ledger = openFiles(directory, null)) {
            assertArrayEquals(bytes(1, 2 * LARGE), ledger.get(FILES, "first").orElseThrow());
            assertArrayEquals(bytes(2, 2 * LARGE), ledger.get(FILES, "large").orElseThrow());
        }
    }

    @Test
    @DisplayName("an interrupt of a commit that writes its own group with a large value leaves the file of large values to force")
    void largeValueInterrupts(@TempDir Path directory) throws Exception {
        var commits = 40;
        try (var ledger = openFiles(directory, null)) {
            for (var i = 0; i < commits; i++) {
                var index = i;
                var committer = Thread.ofPlatform().start(() -> {
                    try {
                        ledger.commit(ledger.newBatch().put(FILES, "v" + index, bytes(index, 2 * LARGE))).awaitDurable();
                    } catch (InterruptedException _) {
                        // The commit stays, durable with the next group
                    }
                });
                // At a different point of the commit each time, its force included
                var until = System.nanoTime() + ThreadLocalRandom.current().nextLong(2_000_000);
                while (System.nanoTime() < until) {
                    Thread.onSpinWait();
                }
                committer.interrupt();
                committer.join();
            }
            ledger.commit(ledger.newBatch().put(FILES, "last", bytes(commits, 2 * LARGE))).awaitDurable();
            ledger.awaitDurable();
        }
        try (var ledger = openFiles(directory, null)) {
            for (var i = 0; i <= commits; i++) {
                assertArrayEquals(bytes(i, 2 * LARGE), ledger.get(FILES, i == commits ? "last" : "v" + i).orElseThrow(), "value " + i);
            }
        }
    }

    @Test
    @DisplayName("a commit of a range removal whose writes miss the page cache ends, and so does the count it leaves")
    void rangeRemovalOfColdKeys(@TempDir Path directory) throws Exception {
        var model = new TreeMap<String, String>();
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NOTES).build()) {
            for (var start = 0; start < NOTE_ENTRIES; start += 500) {
                var batch = ledger.newBatch();
                for (var i = start; i < start + 500; i++) {
                    batch.put(NOTES, note(i), "note " + i + " " + "y".repeat(150));
                    model.put(note(i), "note " + i + " " + "y".repeat(150));
                }
                ledger.commit(batch);
            }
        }
        // Reopened, so the cache holds none of the leaves the writes look their keys up in
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NOTES).build()) {
            var size = callWithBusyCarriers(() -> {
                var batch = ledger.newBatch().deleteRange(NOTES, KeyRange.between(note(1_000), note(2_000)));
                for (var k = 0; k < 16; k++) {
                    batch.put(NOTES, note(3_000 + k * 2_311), "rewritten " + k);
                    batch.put(NOTES, note(3_000 + k * 2_311) + "a", "added " + k);
                }
                ledger.commit(batch).awaitDurable();
                return ledger.size(NOTES);
            });
            model.subMap(note(1_000), note(2_000)).clear();
            for (var k = 0; k < 16; k++) {
                model.put(note(3_000 + k * 2_311), "rewritten " + k);
                model.put(note(3_000 + k * 2_311) + "a", "added " + k);
            }
            assertEquals(model.size(), size);
            assertEquals(model, contents(ledger, NOTES));
        }
    }

    @Test
    @DisplayName("writes that wait for the load of a resident key space whose thread never starts load it themselves")
    void residentWrites(@TempDir Path directory) throws Exception {
        var model = populateSessions(directory);
        try (var ledger = openSessions(directory, false)) {
            var live = (LiveLedger) ledger;
            var size = callWithBusyCarriers(() -> {
                // The first use starts the loader
                assertEquals(Optional.of(sessionValue(0)), ledger.get(SESSIONS, session(0)));
                // Waits for the loader to reach its key
                ledger.commit(ledger.newBatch().put(SESSIONS, session(SESSION_ENTRIES / 2), "middle")).awaitDurable();
                assertEquals(Optional.of(sessionValue(SESSION_ENTRIES - 1)), ledger.get(SESSIONS, session(SESSION_ENTRIES - 1)));
                // Waits for every entry
                ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between(session(10), session(20)))).awaitDurable();
                assertTrue(live.residentLoadedForTests(SESSIONS));
                return ledger.size(SESSIONS);
            });
            model.put(session(SESSION_ENTRIES / 2), "middle");
            model.subMap(session(10), session(20)).clear();
            assertEquals(model.size(), size);
            assertEquals(model, contents(ledger, SESSIONS));
        }
        try (var ledger = openSessions(directory, true)) {
            assertEquals(model, contents(ledger, SESSIONS));
        }
    }

    @ParameterizedTest(name = "after a write that waited for the loader: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a close stops the load of a resident key space whose thread never started, and the next open loads it")
    void closeBeforeTheLoad(boolean written, @TempDir Path directory) throws Exception {
        var model = populateSessions(directory);
        var ledger = openSessions(directory, false);
        try {
            assertEndsWithBusyCarriers(() -> {
                assertEquals(Optional.of(sessionValue(1)), ledger.get(SESSIONS, session(1)));
                if (written) {
                    // Loads up to its key, and leaves the rest to a thread that does not start either
                    ledger.commit(ledger.newBatch().put(SESSIONS, session(300), "written"));
                    model.put(session(300), "written");
                }
                ledger.close();
            });
        } finally {
            ledger.close();
        }
        try (var reopened = openSessions(directory, false)) {
            assertEquals(model, contents(reopened, SESSIONS));
        }
    }

    @Test
    @DisplayName("an open that replays the log and loads a resident key space ends, and so do its first commits")
    void open(@TempDir Path directory) throws Exception {
        var model = populateSessions(directory);
        try (var ledger = openSessions(directory, false)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 50; i++) {
                batch.put(SESSIONS, session(SESSION_ENTRIES + i), sessionValue(SESSION_ENTRIES + i));
                model.put(session(SESSION_ENTRIES + i), sessionValue(SESSION_ENTRIES + i));
            }
            ledger.commit(batch.put(FILES, "large", bytes(7, 2 * LARGE))).awaitDurable();
            // Left for the next open to replay
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        var contents = callWithBusyCarriers(() -> {
            try (var ledger = openSessions(directory, true)) {
                assertArrayEquals(bytes(7, 2 * LARGE), ledger.get(FILES, "large").orElseThrow());
                ledger.commit(ledger.newBatch().put(FILES, "after", bytes(8, 2 * LARGE))).awaitDurable();
                assertTrue(ledger.commit(ledger.newBatch().put(SESSIONS, session(0), "after")).awaitDurable(Duration.ofSeconds(60)));
                return contents(ledger, SESSIONS);
            }
        });
        model.put(session(0), "after");
        assertEquals(model, contents);
    }
}
