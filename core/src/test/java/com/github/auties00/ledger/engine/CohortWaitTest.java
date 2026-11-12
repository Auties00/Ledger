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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The wait of the log's flush for the commits a force woke: concurrent committers come to share one force a round,
/// and the wait holds a commit back no longer than its bound, nor a close, a failed force or another store's flush; a
/// crash or interrupts while committers gather lose no acknowledged commit and hang none.
///
/// Each force of a log segment sleeps a few milliseconds on the in-memory file system, so a round is long against the
/// time the committers take to come back, even on a loaded machine.
@Timeout(120)
class CohortWaitTest {
    private static final byte[] KEY = new byte[32];
    private static final String VALUE = "v".repeat(200);
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Long against a committer's way back, short enough for a few hundred rounds a test
    private static final long FORCE_MILLIS = 3;
    // How long a test that needs the flush to wait for woken commits gives it, which a loaded machine may make miss
    // for a while, each miss turning the waits off for up to 64 groups
    private static final Duration WINDOWS = Duration.ofSeconds(60);

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

    /// {@return a store that writes its groups through as `mode` says, in log segments of 256 KiB, so the tests of a
    /// crash and of interrupts roll over}
    private static Ledger open(Path directory, WriteThrough.Mode mode, IoPool pool) {
        var options = (LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES);
        options.writeThrough(mode).segmentSize(256 << 10);
        if (pool != null) {
            options.ioPool(pool);
        }
        return options.build();
    }

    /// Makes every force of a log segment of `disk` spin for `nanos`, which a sleep, a millisecond at least on Windows,
    /// would outlast.
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

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    /// Waits until the flush of `log` waited `count` times for the commits a force woke, and all of them came back.
    private static void awaitGathered(LogWriter log, int count) throws InterruptedException {
        var deadline = System.nanoTime() + WINDOWS.toNanos();
        while (log.cohortsGathered() < count) {
            assertTrue(System.nanoTime() < deadline, () -> "the flush waited for woken commits " + log.cohortsGathered()
                    + " times in " + WINDOWS + ", " + log.cohortsMissed() + " ran out");
            Thread.sleep(1);
        }
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
            var log = log(ledger);
            var deadline = System.nanoTime() + WINDOWS.toNanos();
            var windows = 0;
            String last;
            // A window whose waits gathered no commit, as when early misses turned the waits off on a loaded machine,
            // says nothing about them: windows go on until one shares its forces, within a deadline no window reaches
            // while the committers take turns
            while (true) {
                var window = windows++;
                var forcesBefore = log.forces();
                var gatheredBefore = log.cohortsGathered();
                var missedBefore = log.cohortsMissed();
                run(committers, committer -> {
                    for (var round = 0; round < rounds; round++) {
                        ledger.commit(ledger.newBatch().put(NAMES, window + "-" + committer + "-" + round, "v")).awaitDurable();
                    }
                });
                var forces = log.forces() - forcesBefore;
                var gathered = log.cohortsGathered() - gatheredBefore;
                var missed = log.cohortsMissed() - missedBefore;
                last = forces + " forces for " + rounds + " rounds of " + committers + " committers, " + gathered
                        + " waits gathered, " + missed + " ran out";
                // Two halves taking turns make two forces a round, one shared force one
                if (gathered > 0 && forces < rounds * 3L / 2L) {
                    break;
                }
                assertTrue(System.nanoTime() < deadline, "no window of " + windows + " shared its forces; the last: " + last);
            }
            assertEquals((long) windows * committers * rounds, ledger.size(NAMES));
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
            var log = log(ledger);
            var deadline = System.nanoTime() + WINDOWS.toNanos();
            // Until the flush waited for woken commits that came back: the case this covers
            run(committers, committer -> {
                for (var round = 0; round < rounds || (log.cohortsGathered() == 0 && System.nanoTime() < deadline); round++) {
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, "v"));
                    // A wait with a timeout leaves its group to the pool; one without may write it itself
                    if (committer % 2 == 0) {
                        assertTrue(commit.awaitDurable(Duration.ofSeconds(10)), "a commit was not durable in 10 seconds");
                    } else {
                        commit.awaitDurable();
                    }
                }
            });
            assertTrue(log.cohortsGathered() > 0, "the flush never waited for the commits a force woke, in " + WINDOWS);
            assertTrue(ledger.size(NAMES) >= (long) committers * rounds);
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
        awaitGathered(log(ledger), 10);
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
            awaitGathered(log(ledger), 10);
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
            var log = log(ledger);
            // A woken committer that commits again before the flush looks at the waiters arms a wait for itself, which
            // its registration, counted already, ends at the first look: gathered, never missed
            assertEquals(0, log.cohortsMissed());
            try (var _ = new HeldPool(pool)) {
                var waits = log.cohortsGathered() + log.cohortsMissed();
                for (var round = 0; round < 50; round++) {
                    ledger.commit(ledger.newBatch().put(NAMES, "held" + round, "v")).awaitDurable();
                }
                // Counted before the pool's thread is let go: a flush of its own never waits
                assertEquals(waits, log.cohortsGathered() + log.cohortsMissed());
            }
            assertEquals(50, forcedHere.get());
            assertEquals(0, log.cohortsMissed());
        }
    }

    @Test
    @DisplayName("one stalled force moves the average the wait's budget comes from by no more than a force of four times it")
    void stalledForce() throws Exception {
        var stall = new AtomicBoolean();
        var disk = CrashFileSystem.create();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                try {
                    Thread.sleep(stall.getAndSet(false) ? 300 : 1);
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        try (var ledger = open(disk.getPath("/store"), null)) {
            var log = log(ledger);
            for (var round = 0; round < 30; round++) {
                ledger.commit(ledger.newBatch().put(NAMES, "k" + round, "v")).awaitDurable();
            }
            var average = forceNanos(log);
            assertTrue(average > 0);
            stall.set(true);
            var forces = log.forces();
            ledger.commit(ledger.newBatch().put(NAMES, "stalled", "v")).awaitDurable();
            assertEquals(forces + 1, log.forces());
            // The stall, far above four times the average, counts as four times it
            var stalled = forceNanos(log);
            assertEquals(average + (4 * average - average) / 8, stalled, () -> "the average went from " + average + " to " + stalled + " ns");
        }
    }

    private static long forceNanos(LogWriter log) throws ReflectiveOperationException {
        var field = LogWriter.class.getDeclaredField("forceNanos");
        field.setAccessible(true);
        return field.getLong(log);
    }

    static Stream<Arguments> crashes() {
        return IntStream.range(0, 6).boxed().flatMap(seed -> Stream.of(
                Arguments.of(seed, WriteThrough.Mode.OFF), Arguments.of(seed, WriteThrough.Mode.DSYNC)));
    }

    @ParameterizedTest(name = "seed {0}, writes through: {1}")
    @MethodSource("crashes")
    @DisplayName("a store stopped at a force while committers gather keeps every acknowledged commit, and each committer's commits as a prefix")
    void crashWhileGathering(int seed, WriteThrough.Mode mode) throws Exception {
        var random = new Random(seed);
        var disk = seed % 3 == 2 ? CrashFileSystem.createNtfs() : CrashFileSystem.create();
        spinForces(disk, 200_000);
        disk.stopAtForce(5 + random.nextInt(250));
        var committers = 8;
        var acknowledged = ConcurrentHashMap.<String>newKeySet();
        var rounds = new AtomicIntegerArray(committers);
        var ended = new ConcurrentLinkedQueue<Throwable>();
        var ledger = open(disk.getPath("/store"), mode, null);
        var threads = new ArrayList<Thread>();
        for (var number = 0; number < committers; number++) {
            var committer = number;
            var timed = committer % 3 == 0;
            threads.add(Thread.ofVirtual().start(() -> {
                for (var round = 0; ; round++) {
                    var key = committer + "-" + round;
                    rounds.set(committer, round + 1);
                    try {
                        var commit = ledger.commit(ledger.newBatch().put(NAMES, key, VALUE));
                        if (timed) {
                            assertTrue(commit.awaitDurable(Duration.ofSeconds(60)), "a commit was not durable in 60 seconds");
                        } else {
                            commit.awaitDurable();
                        }
                        acknowledged.add(key);
                    } catch (Throwable throwable) {
                        ended.add(throwable);
                        return;
                    }
                }
            }));
        }
        for (var thread : threads) {
            thread.join();
        }
        for (var throwable : ended) {
            assertTrue(throwable instanceof LedgerException || throwable instanceof IllegalStateException, () -> "a committer ended with " + throwable);
        }
        var survivor = seed % 2 == 0 ? disk.afterPowerLoss(new Random(seed * 31L)) : disk.afterProcessCrash();
        try {
            ledger.close();
        } catch (LedgerException _) {
            // The log stopped with frames it could not make durable
        }
        try (var reopened = open(survivor.getPath("/store"), mode, null)) {
            for (var key : acknowledged) {
                assertEquals(Optional.of(VALUE), reopened.get(NAMES, key), key + " was acknowledged and lost");
            }
            for (var committer = 0; committer < committers; committer++) {
                var gap = -1;
                for (var round = 0; round < rounds.get(committer); round++) {
                    if (reopened.get(NAMES, committer + "-" + round).isEmpty()) {
                        gap = gap < 0 ? round : gap;
                    } else {
                        assertTrue(gap < 0, "committer " + committer + " lost round " + gap + " and kept round " + round);
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "pool threads: {0}, writes through: {1}")
    @CsvSource({"0,OFF", "1,DSYNC", "2,OFF"})
    @DisplayName("interrupts of committers waiting beside the flush's waits hang none and lose no acknowledged commit")
    void interrupts(int poolThreads, WriteThrough.Mode mode) throws Exception {
        var disk = CrashFileSystem.create();
        spinForces(disk, 800_000);
        var acknowledged = ConcurrentHashMap.<String>newKeySet();
        var failures = new ConcurrentLinkedQueue<Throwable>();
        var pool = poolThreads == 0 ? null : IoPool.create(poolThreads);
        var first = open(disk.getPath("/first"), mode, pool);
        var second = pool == null ? null : open(disk.getPath("/second"), mode, pool);
        var stop = new AtomicBoolean();
        var threads = new ArrayList<Thread>();
        for (var number = 0; number < 12; number++) {
            var committer = number;
            var timed = committer % 3 == 0;
            Runnable body = () -> {
                var store = second != null && committer >= 9 ? second : first;
                for (var round = 0; !stop.get() && round < 4_000; round++) {
                    var key = committer + "-" + round;
                    try {
                        var commit = store.commit(store.newBatch().put(NAMES, key, VALUE));
                        if (timed) {
                            if (!commit.awaitDurable(Duration.ofSeconds(60))) {
                                failures.add(new AssertionError("a commit was not durable in 60 seconds: " + key));
                                return;
                            }
                        } else {
                            commit.awaitDurable();
                        }
                        if (store == first) {
                            acknowledged.add(key);
                        }
                    } catch (InterruptedException _) {
                        // The wait was interrupted, not the commit
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                        return;
                    }
                }
            };
            threads.add(committer % 2 == 0 ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
        }
        var random = new Random(1);
        var end = System.nanoTime() + Duration.ofSeconds(2).toNanos();
        while (System.nanoTime() < end && failures.isEmpty()) {
            threads.get(random.nextInt(threads.size())).interrupt();
            Thread.sleep(0, 200_000);
        }
        stop.set(true);
        for (var thread : threads) {
            thread.join(Duration.ofSeconds(30));
            assertFalse(thread.isAlive(), () -> "a committer hung:\n" + Arrays.stream(thread.getStackTrace())
                    .map(Object::toString).collect(Collectors.joining("\n")));
        }
        assertTrue(failures.isEmpty(), () -> "a committer failed: " + failures);
        first.close();
        if (second != null) {
            second.close();
        }
        if (pool != null) {
            pool.close();
        }
        try (var reopened = open(disk.afterPowerLoss(new Random(5)).getPath("/first"), WriteThrough.Mode.OFF, null)) {
            for (var key : acknowledged) {
                assertEquals(Optional.of(VALUE), reopened.get(NAMES, key), key + " was acknowledged and lost");
            }
        }
    }
}
