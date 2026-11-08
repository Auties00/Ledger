package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The wait of the log's flush for the commits a force woke: concurrent committers come to share one force a round,
/// and the wait holds a commit back no longer than its bound, nor a close, a failed force or another store's flush.
///
/// Each force of a log segment sleeps a few milliseconds on the in-memory file system, so a round is long against the
/// time the committers take to come back, even on a loaded machine.
@Timeout(120)
class CohortWaitTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Long against a committer's way back, short enough for a few hundred rounds a test
    private static final long FORCE_MILLIS = 3;

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 7 + 1);
        }
    }

    /// {@return a file system whose forces of log segments take [#FORCE_MILLIS], and fail while `failing` is set}
    private static CrashFileSystem slowForces(AtomicBoolean failing) {
        var disk = CrashFileSystem.create();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                if (failing.get()) {
                    throw new IOException("The device failed");
                }
                try {
                    Thread.sleep(FORCE_MILLIS);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        return disk;
    }

    private static Ledger open(Path directory, IoPool pool) {
        var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES);
        if (pool != null) {
            options.ioPool(pool);
        }
        return options.build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    /// {@return the committers of a test, no more than the flush waits for on this machine}
    private static int committers() {
        return Math.min(8, 4 * Runtime.getRuntime().availableProcessors());
    }

    /// Runs `body` on `count` virtual threads, given each one's number, and waits for all of them.
    private static void run(int count, Committer body) throws Exception {
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var threads = new ArrayList<Thread>();
        for (var number = 0; number < count; number++) {
            var committer = number;
            threads.add(Thread.ofVirtual().start(() -> {
                try {
                    body.run(committer);
                } catch (Throwable throwable) {
                    failures.add(throwable);
                }
            }));
        }
        for (var thread : threads) {
            thread.join();
        }
        if (!failures.isEmpty()) {
            var failure = new AssertionError("a committer failed");
            failures.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    @FunctionalInterface
    private interface Committer {
        void run(int number) throws Exception;
    }

    @Test
    @DisplayName("concurrent committers come to share one force a round rather than taking turns in two halves")
    void shares() throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var committers = committers();
        var rounds = 40;
        try (var ledger = open(disk.getPath("/store"), null)) {
            run(committers, committer -> {
                for (var round = 0; round < rounds; round++) {
                    ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v")).awaitDurable();
                }
            });
            var commits = committers * rounds;
            assertEquals(commits, ledger.size(NAMES));
            var log = log(ledger);
            assertTrue(log.cohortsGathered() > 0, "the flush never waited for the commits a force woke");
            // Two halves taking turns make a force for every half of the committers
            var forces = log.forces();
            assertTrue(forces < commits * 3L / (2L * committers), () -> forces + " forces for " + commits + " commits of "
                    + committers + " committers, " + log.cohortsGathered() + " waits gathered, " + log.cohortsMissed() + " ran out");
        }
    }

    @ParameterizedTest(name = "with a timeout: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("two committers that start half a force apart come to share one force a round rather than taking turns")
    void staggered(boolean timed) throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var rounds = 100;
        try (var ledger = open(disk.getPath("/store"), null)) {
            ledger.commit(ledger.newBatch().put(NAMES, "first", "v")).awaitDurable();
            var log = log(ledger);
            var before = log.forces();
            run(2, committer -> {
                if (committer == 1) {
                    while (log.forces() == before) {
                        Thread.onSpinWait();
                    }
                    // Into the force the first committer's next commit waits for, so neither finds the other's group
                    Thread.sleep(FORCE_MILLIS / 2);
                }
                for (var round = 0; round < rounds; round++) {
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v"));
                    // A wait with a timeout leaves its group to the pool, whose next group always takes the other's
                    // commit before the woken one is back, unless the flush waits for it
                    if (timed) {
                        assertTrue(commit.awaitDurable(Duration.ofSeconds(30)), "a commit was not durable in 30 seconds");
                    } else {
                        commit.awaitDurable();
                    }
                }
            });
            assertEquals(1 + 2 * rounds, ledger.size(NAMES));
            // Taking turns makes a force for every commit
            var forces = log.forces() - before;
            assertTrue(forces < rounds * 3L / 2L, () -> forces + " forces for " + rounds + " commits of each of 2 committers, "
                    + log.cohortsGathered() + " waits gathered, " + log.cohortsMissed() + " ran out");
        }
    }

    @Test
    @DisplayName("every commit that comes back while the flush waits, to a queue it found empty, becomes durable, with or without a timeout")
    void rearrivals() throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var committers = Math.min(16, 4 * Runtime.getRuntime().availableProcessors());
        var rounds = 30;
        try (var ledger = open(disk.getPath("/store"), null)) {
            run(committers, committer -> {
                for (var round = 0; round < rounds; round++) {
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v"));
                    // A wait with a timeout leaves its group to the pool; one without may write it itself
                    if (committer % 2 == 0) {
                        assertTrue(commit.awaitDurable(Duration.ofSeconds(10)), "a commit was not durable in 10 seconds");
                    } else {
                        commit.awaitDurable();
                    }
                }
            });
            assertEquals(committers * rounds, ledger.size(NAMES));
            assertTrue(log(ledger).cohortsGathered() > 0, "the flush never waited for the commits a force woke");
        }
    }

    @Test
    @DisplayName("a wait for woken commits that do not come back releases the commit queued behind them within its bound")
    void stragglers() throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var committers = committers();
        var others = new CountDownLatch(committers - 1);
        var slowest = new AtomicLong();
        var alone = new AtomicLong();
        try (var ledger = open(disk.getPath("/store"), null)) {
            run(committers, committer -> {
                if (committer > 0) {
                    for (var round = 0; round < 20; round++) {
                        ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v")).awaitDurable();
                    }
                    others.countDown();
                    return;
                }
                for (var round = 0; others.getCount() > 0 || alone.get() < 30; round++) {
                    var stopped = others.getCount() == 0;
                    var start = System.nanoTime();
                    ledger.commit(ledger.newBatch().put(NAMES, "0-" + round, "v")).awaitDurable();
                    if (stopped) {
                        alone.incrementAndGet();
                        slowest.accumulateAndGet(System.nanoTime() - start, Math::max);
                    }
                }
            });
        }
        // Each wait lasts a millisecond at most, a force three: a second is noise, not a wait that went on
        assertTrue(slowest.get() < Duration.ofSeconds(1).toNanos(), () -> "a commit alone took " + slowest.get() / 1_000_000 + " ms");
    }

    @Test
    @DisplayName("a close while the flush waits for woken commits makes every acknowledged commit durable and returns")
    void closes() throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var committers = committers();
        var acknowledged = ConcurrentHashMap.<String>newKeySet();
        var unexpected = new ConcurrentLinkedQueue<Throwable>();
        var ledger = open(disk.getPath("/store"), null);
        var threads = new ArrayList<Thread>();
        for (var number = 0; number < committers; number++) {
            var committer = number;
            threads.add(Thread.ofVirtual().start(() -> {
                for (var round = 0; ; round++) {
                    var key = committer + "-" + round;
                    try {
                        ledger.commit(ledger.newBatch().put(NAMES, key, "v")).awaitDurable();
                        acknowledged.add(key);
                    } catch (IllegalStateException _) {
                        // The store closed
                        return;
                    } catch (Throwable throwable) {
                        unexpected.add(throwable);
                        return;
                    }
                }
            }));
        }
        var log = log(ledger);
        while (log.cohortsGathered() < 10) {
            Thread.sleep(1);
        }
        ledger.close();
        for (var thread : threads) {
            thread.join();
        }
        assertTrue(unexpected.isEmpty(), () -> "a committer failed: " + unexpected);
        assertFalse(acknowledged.isEmpty());
        try (var reopened = open(disk.getPath("/store"), null)) {
            for (var key : acknowledged) {
                assertEquals(Optional.of("v"), reopened.get(NAMES, key), key);
            }
        }
    }

    @Test
    @DisplayName("a failed force wakes every commit waiting for durability, those that came back during a wait among them")
    void failure() throws Exception {
        var failing = new AtomicBoolean();
        var disk = slowForces(failing);
        var committers = committers();
        var ended = new ConcurrentLinkedQueue<Throwable>();
        var ledger = open(disk.getPath("/store"), null);
        try {
            var threads = new ArrayList<Thread>();
            for (var number = 0; number < committers; number++) {
                var committer = number;
                threads.add(Thread.ofVirtual().start(() -> {
                    for (var round = 0; ; round++) {
                        try {
                            ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v")).awaitDurable();
                        } catch (Throwable throwable) {
                            ended.add(throwable);
                            return;
                        }
                    }
                }));
            }
            var log = log(ledger);
            while (log.cohortsGathered() < 10) {
                Thread.sleep(1);
            }
            failing.set(true);
            for (var thread : threads) {
                thread.join();
            }
            assertEquals(committers, ended.size());
            for (var throwable : ended) {
                assertTrue(throwable instanceof LedgerIoException, () -> "a committer ended with " + throwable);
            }
        } finally {
            try {
                ledger.close();
            } catch (LedgerException _) {
                // The log stopped with frames it could not make durable
            }
        }
    }

    @Test
    @DisplayName("a store whose queue never drains gives its one-thread pool to a store sharing it after each group")
    void sharedPool() throws Exception {
        var disk = slowForces(new AtomicBoolean());
        var committers = committers();
        var stop = new AtomicBoolean();
        var slowest = new AtomicLong();
        try (var pool = IoPool.create(1);
             var busy = open(disk.getPath("/busy"), pool);
             var quiet = open(disk.getPath("/quiet"), pool)) {
            var running = new CountDownLatch(committers);
            var threads = new ArrayList<Thread>();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            for (var number = 0; number < committers; number++) {
                var committer = number;
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                        for (var round = 0; !stop.get(); round++) {
                            busy.commit(busy.newBatch().put(NAMES, committer + "-" + round, "v")).awaitDurable();
                            if (round == 10) {
                                running.countDown();
                            }
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
            }
            running.await();
            try {
                for (var round = 0; round < 100; round++) {
                    // Anywhere in the busy store's round: arriving during a wait, it splits the group the flush takes next
                    Thread.sleep(Duration.ofNanos(ThreadLocalRandom.current().nextLong(FORCE_MILLIS * 1_000_000)));
                    var start = System.nanoTime();
                    // A wait with a timeout never writes its own group: the pool's thread writes it
                    var commit = quiet.commit(quiet.newBatch().put(NAMES, "q-" + round, "v"));
                    assertTrue(commit.awaitDurable(Duration.ofSeconds(30)), "the quiet store's commit never became durable");
                    slowest.accumulateAndGet(System.nanoTime() - start, Math::max);
                }
            } finally {
                stop.set(true);
                for (var thread : threads) {
                    thread.join();
                }
            }
            assertTrue(failures.isEmpty(), () -> "a committer failed: " + failures);
            assertEquals(100, quiet.size(NAMES));
        }
        // The busy store's flush gives up the pool after each group, a force of a few milliseconds
        assertTrue(slowest.get() < Duration.ofSeconds(1).toNanos(), () -> "a commit of the quiet store took " + slowest.get() / 1_000_000 + " ms");
    }

    @Test
    @DisplayName("one committer at a time never makes the flush wait, and writes its own group after any other")
    void solitary() throws Exception {
        var disk = CrashFileSystem.create();
        try (var pool = IoPool.create(1); var ledger = open(disk.getPath("/store"), pool)) {
            // Some of these groups are the pool's and some the committer's, however the two race
            for (var round = 0; round < 50; round++) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + round, "v")).awaitDurable();
            }
            var committer = Thread.currentThread();
            var forcedHere = new AtomicLong();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && Thread.currentThread() == committer) {
                    forcedHere.incrementAndGet();
                }
            });
            try (var _ = new HeldPool(pool)) {
                for (var round = 0; round < 50; round++) {
                    ledger.commit(ledger.newBatch().put(NAMES, "held" + round, "v")).awaitDurable();
                }
            }
            assertEquals(50, forcedHere.get());
            assertEquals(0, log(ledger).cohortsGathered() + log(ledger).cohortsMissed());
        }
    }
}
