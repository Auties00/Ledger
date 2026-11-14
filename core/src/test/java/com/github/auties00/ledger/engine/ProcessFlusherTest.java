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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The flusher the stores opened without a pool share: threads made as flushes need them and ended once idle, which no
/// store closes; and a flush no thread can be made for, which fails the commits waiting for it rather than leaving them
/// waiting.
///
/// The tests run their stores on a flusher of their own ([LedgerBuilder#flusher(PlatformIoPool)]), shaped as the
/// process's, so the threads other tests left waiting do not take their flushes.
class ProcessFlusherTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    private static Ledger open(Path directory, PlatformIoPool flusher) {
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

    private static void awaitThreads(PlatformIoPool pool, int count) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (pool.threadsForTests() != count) {
            assertTrue(System.nanoTime() < deadline, "the flusher holds " + pool.threadsForTests() + " threads");
            Thread.sleep(5);
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
    @DisplayName("runs the flushes of stores used one after another on a few threads, which outlive the stores and end once idle")
    void shared() throws Exception {
        var made = new AtomicInteger();
        var flusher = PlatformIoPool.sharedForTests(counting(made));
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
            }
            // One for the flushes, and a second for a commit that lands as the first ends a flush
            assertTrue(made.get() <= 3, "8 stores made " + made.get() + " threads");
        } finally {
            for (var ledger : stores) {
                ledger.close();
            }
        }
        assertFalse(((ThreadPoolExecutor) flusher.executor()).isShutdown(), "a store closed the flusher");
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
    @DisplayName("is the default of a store opened without a pool, one flusher for the process that no close stops")
    void processDefault() throws Exception {
        var flusher = PlatformIoPool.process();
        assertSame(flusher, PlatformIoPool.process());
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).build()) {
            commit(ledger, model, "k");
        }
        assertFalse(((ThreadPoolExecutor) flusher.executor()).isShutdown());
        flusher.close();
        assertFalse(((ThreadPoolExecutor) flusher.executor()).isShutdown(), "the process's flusher was closed");
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).build()) {
            commit(ledger, model, "again");
            assertEquals(model, contents(ledger));
        }
    }

    @ParameterizedTest(name = "refused by {0}")
    @ValueSource(strings = {"null", "error"})
    @DisplayName("fails the commits and the close of a store whose flush no thread can be made for, rather than leaving them waiting")
    void noThread(String failure) {
        var flusher = PlatformIoPool.sharedForTests(runnable -> {
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
        var flusher = PlatformIoPool.sharedForTests(runnable -> made.getAndIncrement() == 0 ? factory.newThread(runnable) : null);
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
        try (var ledger = open(disk.afterPowerLoss(new Random(1)).getPath("/store"), PlatformIoPool.process())) {
            var recovered = contents(ledger);
            for (var entry : durable.entrySet()) {
                assertEquals(entry.getValue(), recovered.get(entry.getKey()), entry.getKey());
            }
        }
    }
}
