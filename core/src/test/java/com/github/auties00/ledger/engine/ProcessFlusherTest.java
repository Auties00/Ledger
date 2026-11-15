package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The flusher the stores opened without a pool share: threads made as flushes need them and ended once idle, which no
/// store closes; its lanes, which run again after each task given and never two at once; and a flush no thread can be
/// made for, which fails the commits waiting for it rather than leaving them waiting.
///
/// The tests run their stores on a flusher of their own ([LedgerBuilder#flusher(ProcessFlusher)]), so the threads
/// other tests left waiting do not take their flushes.
class ProcessFlusherTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    private static Ledger open(Path directory, ProcessFlusher flusher) {
        var builder = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES);
        return ((LedgerBuilder) builder).flusher(flusher).build();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key) throws InterruptedException {
        var value = "v".repeat(500) + key;
        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
        model.put(key, value);
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    /// {@return a maker of daemon threads that counts them}
    private static ThreadFactory counting(AtomicInteger made) {
        var factory = Thread.ofPlatform().daemon().name("ledger-io-test-", 0).factory();
        return runnable -> {
            made.incrementAndGet();
            return factory.newThread(runnable);
        };
    }

    private static void awaitThreads(ProcessFlusher flusher, int count) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (flusher.threadsForTests() != count) {
            assertTrue(System.nanoTime() < deadline, "the flusher holds " + flusher.threadsForTests() + " threads");
            Thread.sleep(5);
        }
    }

    /// Waits until every thread of `flusher` waits for a lane.
    private static void awaitIdle(ProcessFlusher flusher) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (flusher.idleForTests() != flusher.threadsForTests()) {
            assertTrue(System.nanoTime() < deadline, "the flusher runs a lane");
            Thread.sleep(1);
        }
    }

    /// Makes every force of a log segment of `disk` spin for `nanos`, as a device takes its time.
    private static void spinForces(CrashFileSystem disk, long nanos) {
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                var end = System.nanoTime() + nanos;
                while (System.nanoTime() < end) {
                    Thread.onSpinWait();
                }
            }
        });
    }

    @Test
    @DisplayName("runs the flushes of stores used one after another on one thread, which outlives the stores and ends once idle")
    void shared() throws Exception {
        var made = new AtomicInteger();
        var flusher = new ProcessFlusher(counting(made));
        var disk = CrashFileSystem.create();
        spinForces(disk, 300_000);
        var models = new ArrayList<Map<String, String>>();
        var stores = new ArrayList<Ledger>();
        try {
            for (var store = 0; store < 8; store++) {
                var ledger = open(disk.getPath("/store" + store), flusher);
                stores.add(ledger);
                var model = new TreeMap<String, String>();
                models.add(model);
                for (var i = 0; i < 20; i++) {
                    commit(ledger, model, "k" + i);
                }
                // The flush a commit gave its lane may still run, writing nothing, after the commit wrote its own group
                awaitIdle(flusher);
            }
            // A flush given while its lane holds the thread runs on it, and one given once the lane gave the thread up
            // takes it back, as does the next store's
            assertEquals(1, made.get(), "8 stores used one after another made " + made.get() + " threads");
        } finally {
            for (var ledger : stores) {
                ledger.close();
            }
        }
        assertEquals(1, flusher.threadsForTests(), "a store's close ended the thread");
        flusher.keepAliveForTests(20, TimeUnit.MILLISECONDS);
        awaitThreads(flusher, 0);
        disk.intercept((path, kind) -> {
        });
        // A store opened after the threads ended makes one again
        try (var ledger = open(disk.getPath("/store0"), flusher)) {
            commit(ledger, models.getFirst(), "after");
            assertEquals(models.getFirst(), contents(ledger));
        }
        for (var store = 0; store < 8; store++) {
            try (var ledger = open(disk.afterPowerLoss(new Random(store)).getPath("/store" + store), flusher)) {
                assertEquals(models.get(store), contents(ledger), "store " + store);
            }
        }
    }

    @Test
    @DisplayName("makes a thread at once for a store's flush while every thread runs another store's, without waiting for one to come back")
    void busyThreads() throws Exception {
        var made = new AtomicInteger();
        var flusher = new ProcessFlusher(counting(made));
        var disk = CrashFileSystem.create();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var first = open(disk.getPath("/storeA"), flusher); var second = open(disk.getPath("/storeB"), flusher)) {
            try {
                flusher.keepAliveForTests(1, TimeUnit.MILLISECONDS);
                awaitThreads(flusher, 0);
                flusher.keepAliveForTests(ProcessFlusher.KEEP_ALIVE_SECONDS, TimeUnit.SECONDS);
                // The threads the opens made
                var opens = made.get();
                // The force of the first store's log holds the only thread until the second store's flush has a thread
                disk.intercept((path, kind) -> {
                    var name = path.toString();
                    if (kind == ChangeInterceptor.ChangeKind.FORCE && name.startsWith("/storeA") && name.endsWith(".log")) {
                        entered.countDown();
                        try {
                            release.await();
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
                // Neither commit waits for its durability, so neither writes its own group: the flusher's threads do
                var held = first.commit(first.newBatch().put(NAMES, "held", "v".repeat(500)));
                assertTrue(entered.await(30, TimeUnit.SECONDS), "the first store's flush never forced");
                assertEquals(1, flusher.threadsForTests());
                var value = "v".repeat(500) + "k";
                var other = second.commit(second.newBatch().put(NAMES, "k", value));
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (made.get() < opens + 2) {
                    assertTrue(System.nanoTime() < deadline, "the second store's flush got no thread of its own");
                    Thread.sleep(1);
                }
                assertTrue(other.awaitDurable(Duration.ofSeconds(30)), "the second store's commit never became durable");
                assertEquals(opens + 2, made.get());
                release.countDown();
                held.awaitDurable();
                assertEquals(Map.of("k", value), contents(second));
            } finally {
                release.countDown();
            }
        }
    }

    @Test
    @DisplayName("is the default of a store opened without a pool, one flusher for the process that no close stops")
    void processDefault() throws Exception {
        var flusher = ProcessFlusher.process();
        assertSame(flusher, ProcessFlusher.process());
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).build()) {
            commit(ledger, model, "k");
        }
        assertTrue(flusher.threadsForTests() > 0, "the store's close ended the process's flush threads");
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).build()) {
            commit(ledger, model, "again");
            assertEquals(model, contents(ledger));
        }
    }

    @ParameterizedTest(name = "refused by {0}")
    @ValueSource(strings = {"null", "error"})
    @DisplayName("fails the commits and the close of a store whose flush no thread can be made for, rather than leaving them waiting")
    void noThread(String failure) {
        var flusher = new ProcessFlusher(runnable -> {
            if (failure.equals("null")) {
                return null;
            }
            throw new OutOfMemoryError("unable to create native thread: possibly out of memory or process/resource limits reached");
        });
        var disk = CrashFileSystem.create();
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            Ledger ledger;
            try {
                ledger = open(disk.getPath("/store"), flusher);
            } catch (LedgerIoException _) {
                // The open's own durable write found no thread
                return;
            }
            try {
                assertThrows(LedgerIoException.class, () -> commit(ledger, new TreeMap<>(), "k"));
                assertThrows(LedgerIoException.class, () -> commit(ledger, new TreeMap<>(), "k2"));
            } finally {
                try {
                    ledger.close();
                } catch (LedgerIoException _) {
                    // The log stopped: the close reports it
                }
            }
        });
    }

    @Test
    @DisplayName("lets every commit complete or fail, none wait forever, when threads stop being made after the first ends")
    void threadsRunOut() throws Exception {
        var made = new AtomicInteger();
        var factory = Thread.ofPlatform().daemon().name("ledger-io-test-", 0).factory();
        var flusher = new ProcessFlusher(runnable -> made.getAndIncrement() == 0 ? factory.newThread(runnable) : null);
        flusher.keepAliveForTests(1, TimeUnit.MILLISECONDS);
        var disk = CrashFileSystem.create();
        var durable = new TreeMap<String, String>();
        assertTimeoutPreemptively(Duration.ofSeconds(60), () -> {
            var failed = false;
            Ledger ledger;
            try {
                ledger = open(disk.getPath("/store"), flusher);
            } catch (LedgerIoException _) {
                return;
            }
            try {
                for (var i = 0; i < 200; i++) {
                    try {
                        commit(ledger, durable, "k" + i);
                        assertFalse(failed, "a commit completed after one failed");
                    } catch (LedgerIoException _) {
                        failed = true;
                    }
                    if (i % 10 == 9) {
                        Thread.sleep(20);
                    }
                }
            } finally {
                try {
                    ledger.close();
                } catch (LedgerIoException _) {
                    // The log stopped: the close reports it
                }
            }
            assertTrue(failed, "every commit found a thread");
        });
        // What was acknowledged is durable
        try (var ledger = open(disk.afterPowerLoss(new Random(1)).getPath("/store"), ProcessFlusher.process())) {
            var recovered = contents(ledger);
            for (var entry : durable.entrySet()) {
                assertEquals(entry.getValue(), recovered.get(entry.getKey()), entry.getKey());
            }
        }
    }

    @Test
    @DisplayName("runs a lane's task once more after the tasks given while it runs, never two at once, on the lane's thread")
    void laneCoalesces() throws Exception {
        var made = new AtomicInteger();
        var flusher = new ProcessFlusher(counting(made));
        var lane = flusher.lane();
        var runs = new AtomicInteger();
        var running = new AtomicInteger();
        var overlapped = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Runnable task = () -> {
            if (running.incrementAndGet() > 1) {
                overlapped.incrementAndGet();
            }
            try {
                if (runs.incrementAndGet() == 1) {
                    entered.countDown();
                    release.await();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } finally {
                running.decrementAndGet();
            }
        };
        lane.execute(task);
        assertTrue(entered.await(30, TimeUnit.SECONDS), "the first task never ran");
        for (var i = 0; i < 5; i++) {
            lane.execute(task);
        }
        release.countDown();
        awaitIdle(flusher);
        assertEquals(2, runs.get(), "the tasks given while the first ran did not run once after it");
        assertEquals(0, overlapped.get());
        assertEquals(1, made.get());
        // Given once the lane gave its thread up: the thread, waiting, takes it
        lane.execute(task);
        awaitIdle(flusher);
        assertEquals(3, runs.get());
        assertEquals(1, made.get());
    }

    @Test
    @DisplayName("keeps a lane running after its task throws, and takes a thread again after one could not be made")
    void laneSurvivesFailures() throws Exception {
        var calls = new AtomicInteger();
        var factory = Thread.ofPlatform().daemon().name("ledger-io-test-", 0).factory();
        var flusher = new ProcessFlusher(runnable -> {
            if (calls.getAndIncrement() == 0) {
                return null;
            }
            var thread = factory.newThread(runnable);
            thread.setUncaughtExceptionHandler((_, _) -> {
            });
            return thread;
        });
        var lane = flusher.lane();
        var runs = new AtomicInteger();
        assertThrows(RejectedExecutionException.class, () -> lane.execute(runs::incrementAndGet));
        assertEquals(0, flusher.threadsForTests());
        lane.execute(() -> {
            runs.incrementAndGet();
            throw new IllegalStateException("thrown by a task");
        });
        awaitIdle(flusher);
        lane.execute(runs::incrementAndGet);
        awaitIdle(flusher);
        assertEquals(2, runs.get());
        assertEquals(1, flusher.threadsForTests());
    }
}
