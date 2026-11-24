package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The spin of a commit on a platform thread waiting for its group to be durable: a commit whose spin ends without a
/// wake parks and is unparked, a wake between a commit's last read of durability and its park unparks it, a commit
/// durable by the time it joins the waiters does not spin, a stop or an interrupt ends a spin at once, a wake unparks
/// only the commits that park, virtual threads and waits with a timeout never spin, and a crash while commits spin loses
/// no acknowledged commit.
///
/// The tests that need a commit spinning at a given step hold the flush in the force of its group on the in-memory
/// file system, and set the spin's length through [LogWriter#spinBudgetForTests].
@Timeout(120)
class PlatformWaiterSpinTest {
    private static final byte[] KEY = new byte[32];
    private static final String VALUE = "v".repeat(200);
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 7 + 1);
        }
    }

    @AfterEach
    void resetSpinners() {
        ProcessFlusher.maxSpinnersForTests(-1);
    }

    private static Ledger open(Path directory, WriteThrough.Mode mode) {
        var options = (LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES);
        options.writeThrough(mode).segmentSize(256 << 10);
        return options.build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    /// {@return whether the caller runs inside the flush of a group, rather than a preparation of a segment}
    private static boolean inFlush() {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("flushGroups")));
    }

    /// A force of a group that blocks while [#held] is set, telling [#entered] when it starts, and fails once it is
    /// let go while [#failing] is set.
    private static final class HeldForces implements ChangeInterceptor {
        final AtomicBoolean held = new AtomicBoolean();
        final AtomicBoolean failing = new AtomicBoolean();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch released = new CountDownLatch(1);
        final long spinNanos;

        HeldForces(long spinNanos) {
            this.spinNanos = spinNanos;
        }

        @Override
        public void beforeChange(Path path, ChangeKind kind) throws IOException {
            if (kind != ChangeKind.FORCE || !path.toString().endsWith(".log") || !inFlush()) {
                return;
            }
            if (held.get()) {
                entered.countDown();
                try {
                    if (!released.await(60, TimeUnit.SECONDS)) {
                        throw new IOException("The force was held for a minute");
                    }
                } catch (InterruptedException _) {
                    Thread.currentThread().interrupt();
                }
                if (failing.get()) {
                    throw new IOException("The device failed");
                }
            }
            var end = System.nanoTime() + spinNanos;
            while (System.nanoTime() < end) {
                Thread.onSpinWait();
            }
        }

        void release() {
            held.set(false);
            released.countDown();
        }
    }

    /// Makes a few groups durable, each written by its own commit, so the writer has measured its forces, then holds
    /// the next force of a group and commits `key`, returning once the flush holds that commit's group in its force.
    private static Commit commitHeld(Ledger ledger, HeldForces forces, String key) throws Exception {
        for (var i = 0; i < 4; i++) {
            ledger.commit(ledger.newBatch().put(NAMES, "warm-" + i, VALUE)).awaitDurable();
        }
        forces.held.set(true);
        var commit = ledger.commit(ledger.newBatch().put(NAMES, key, VALUE));
        assertTrue(forces.entered.await(30, TimeUnit.SECONDS), "the flush never forced the held group");
        return commit;
    }

    /// Starts a platform thread that waits for `commit` with no timeout, recording how the wait ended.
    private static Thread awaitOnPlatform(Commit commit, AtomicReference<Throwable> ended) {
        return Thread.ofPlatform().start(() -> {
            try {
                commit.awaitDurable();
            } catch (Throwable throwable) {
                ended.set(throwable);
            }
        });
    }

    private static void awaitSpinners(int count) throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (ProcessFlusher.spinnersForTests() != count) {
            assertTrue(System.nanoTime() < deadline, "the spinners never reached " + count);
            Thread.sleep(1);
        }
    }

    private static void awaitParked(Thread thread) throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (thread.getState() != Thread.State.WAITING) {
            assertTrue(System.nanoTime() < deadline, "the commit never parked: " + thread.getState());
            Thread.sleep(1);
        }
    }

    /// Waits until no flush holds the log, which a commit that returned leaves only once the wake that woke it wrote
    /// its counters: the wake counts the commits it woke after it unparked them.
    private static void awaitWakes(LogWriter log) throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (log.flushRunningForTests()) {
            assertTrue(System.nanoTime() < deadline, "a flush never let go of the log");
            Thread.sleep(1);
        }
    }

    private static void assertEnds(Thread thread) throws InterruptedException {
        thread.join(Duration.ofSeconds(30));
        assertFalse(thread.isAlive(), () -> "the commit hung:\n" + Arrays.stream(thread.getStackTrace())
                .map(Object::toString).collect(Collectors.joining("\n")));
    }

    @Test
    @DisplayName("a commit whose spin ends before its group is durable parks, and the group's wake unparks it")
    void missedSpinParks() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            var spinEnded = new CountDownLatch(1);
            var durableAtEnd = new AtomicBoolean(true);
            log.spinEndHookForTests = () -> {
                durableAtEnd.set(commit.isDurable());
                spinEnded.countDown();
            };
            log.spinBudgetForTests = 1_000;
            var parkedBefore = log.spinsParkedForTests();
            var unparkedBefore = log.unparkedWaitersForTests();
            var ended = new AtomicReference<Throwable>();
            var waiter = awaitOnPlatform(commit, ended);
            assertTrue(spinEnded.await(30, TimeUnit.SECONDS), "the commit never spun");
            assertFalse(durableAtEnd.get(), "the spin ended with the group durable");
            awaitParked(waiter);
            forces.release();
            assertEnds(waiter);
            awaitWakes(log);
            assertEquals(null, ended.get());
            assertTrue(commit.isDurable());
            assertEquals(parkedBefore + 1, log.spinsParkedForTests());
            assertEquals(unparkedBefore + 1, log.unparkedWaitersForTests(), "the wake did not unpark the parked commit");
            awaitSpinners(0);
        }
    }

    @Test
    @DisplayName("a commit whose group is published between the end of its spin and its announced park returns without a wake")
    void wakeBeforeParkAnnounced() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            var wokenBefore = log.wokenWaitersForTests();
            var unparkedBefore = log.unparkedWaitersForTests();
            var hookFailure = new AtomicReference<Throwable>();
            // The spin ends at once; the group is published and its wake runs before the commit announces its park,
            // so the wake finds a commit that does not park and skips it
            log.spinEndHookForTests = () -> {
                try {
                    forces.release();
                    var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (log.wokenWaitersForTests() == wokenBefore) {
                        assertTrue(System.nanoTime() < deadline, "the group's wake never ran");
                        Thread.onSpinWait();
                    }
                } catch (Throwable throwable) {
                    hookFailure.set(throwable);
                }
            };
            log.spinBudgetForTests = 1;
            var ended = new AtomicReference<Throwable>();
            var waiter = awaitOnPlatform(commit, ended);
            assertEnds(waiter);
            awaitWakes(log);
            assertEquals(null, hookFailure.get());
            assertEquals(null, ended.get());
            assertTrue(commit.isDurable());
            assertEquals(wokenBefore + 1, log.wokenWaitersForTests());
            assertEquals(unparkedBefore, log.unparkedWaitersForTests(), "the wake unparked a commit that had not announced a park");
        }
    }

    @Test
    @DisplayName("a commit whose group is published and woken before it joins the waiters returns without spinning")
    void wakeBeforeRegistration() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            var spunWokenBefore = log.spinsWokenForTests();
            var spunParkedBefore = log.spinsParkedForTests();
            var hookFailure = new AtomicReference<Throwable>();
            var ran = new AtomicBoolean();
            // The group is published, and its wake finds no waiter, after the commit found itself not durable and
            // before it joins the waiters: a commit that spun then would look for a wake that already ran
            log.registerHookForTests = () -> {
                if (!ran.compareAndSet(false, true)) {
                    return;
                }
                try {
                    forces.release();
                    var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (!commit.isDurable()) {
                        assertTrue(System.nanoTime() < deadline, "the held group never became durable");
                        Thread.onSpinWait();
                    }
                } catch (Throwable throwable) {
                    hookFailure.set(throwable);
                }
            };
            var budget = Duration.ofSeconds(20);
            log.spinBudgetForTests = budget.toNanos();
            var ended = new AtomicReference<Throwable>();
            var start = System.nanoTime();
            var waiter = awaitOnPlatform(commit, ended);
            assertEnds(waiter);
            var waited = Duration.ofNanos(System.nanoTime() - start);
            assertTrue(ran.get(), "the commit never reached the waiters");
            assertEquals(null, hookFailure.get());
            assertEquals(null, ended.get());
            assertEquals(spunWokenBefore, log.spinsWokenForTests(), "the durable commit spun");
            assertEquals(spunParkedBefore, log.spinsParkedForTests(), "the durable commit spun");
            assertTrue(waited.compareTo(budget.dividedBy(2)) < 0, () -> "the durable commit waited " + waited);
            awaitSpinners(0);
        }
    }

    @Test
    @DisplayName("a commit whose group is woken after its last read of durable and before it parks is unparked")
    void wakeBeforePark() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            var wokenBefore = log.wokenWaitersForTests();
            var unparkedBefore = log.unparkedWaitersForTests();
            var spunParkedBefore = log.spinsParkedForTests();
            var hookFailure = new AtomicReference<Throwable>();
            var ran = new AtomicBoolean();
            // The spin ends at once, and the group's wake runs after the commit read durable for the last time: it
            // unparks the commit only if the commit announced its park before that read
            log.parkHookForTests = () -> {
                if (!ran.compareAndSet(false, true)) {
                    return;
                }
                try {
                    forces.release();
                    var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (log.wokenWaitersForTests() == wokenBefore) {
                        assertTrue(System.nanoTime() < deadline, "the group's wake never ran");
                        Thread.onSpinWait();
                    }
                } catch (Throwable throwable) {
                    hookFailure.set(throwable);
                }
            };
            log.spinBudgetForTests = 1;
            var ended = new AtomicReference<Throwable>();
            var waiter = awaitOnPlatform(commit, ended);
            assertEnds(waiter);
            awaitWakes(log);
            assertTrue(ran.get(), "the commit never reached its park");
            assertEquals(null, hookFailure.get());
            assertEquals(null, ended.get());
            assertTrue(commit.isDurable());
            assertEquals(spunParkedBefore + 1, log.spinsParkedForTests());
            assertEquals(wokenBefore + 1, log.wokenWaitersForTests());
            assertEquals(unparkedBefore + 1, log.unparkedWaitersForTests(), "the wake did not unpark the commit about to park");
        }
    }

    @Test
    @DisplayName("a stop while a commit spins ends its wait with the failure at once")
    void stopWhileSpinning() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF);
        try {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            log.spinBudgetForTests = Duration.ofMinutes(5).toNanos();
            var ended = new AtomicReference<Throwable>();
            var waiter = awaitOnPlatform(commit, ended);
            awaitSpinners(1);
            forces.failing.set(true);
            forces.release();
            assertEnds(waiter);
            assertInstanceOf(LedgerIoException.class, ended.get());
            awaitSpinners(0);
        } finally {
            try {
                ledger.close();
            } catch (LedgerException _) {
                // The log stopped with a group it could not make durable
            }
        }
    }

    @Test
    @DisplayName("an interrupt of a commit that spins ends its wait with an InterruptedException at once")
    void interruptWhileSpinning() throws Exception {
        ProcessFlusher.maxSpinnersForTests(1);
        var forces = new HeldForces(0);
        var disk = CrashFileSystem.create();
        disk.intercept(forces);
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var commit = commitHeld(ledger, forces, "held");
            log.spinBudgetForTests = Duration.ofMinutes(5).toNanos();
            var ended = new AtomicReference<Throwable>();
            var waiter = awaitOnPlatform(commit, ended);
            awaitSpinners(1);
            waiter.interrupt();
            assertEnds(waiter);
            assertInstanceOf(InterruptedException.class, ended.get());
            awaitSpinners(0);
            forces.release();
            commit.awaitDurable();
        }
    }

    @Test
    @DisplayName("concurrent platform committers spin, and the wakes unpark only the commits that announced a park")
    void unparksOnlyParking() throws Exception {
        ProcessFlusher.maxSpinnersForTests(2);
        var disk = CrashFileSystem.create();
        disk.intercept(new HeldForces(50_000));
        var committers = 8;
        var rounds = 300;
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var threads = new ArrayList<Thread>();
            for (var number = 0; number < committers; number++) {
                var committer = number;
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        for (var round = 0; round < rounds; round++) {
                            ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, VALUE)).awaitDurable();
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                }));
            }
            for (var thread : threads) {
                assertEnds(thread);
            }
            assertTrue(failures.isEmpty(), () -> "a committer failed: " + failures);
            assertEquals((long) committers * rounds, ledger.size(NAMES));
            awaitWakes(log);
            var woken = log.wokenWaitersForTests();
            var unparked = log.unparkedWaitersForTests();
            var spunWoken = log.spinsWokenForTests();
            var spunParked = log.spinsParkedForTests();
            var counts = woken + " woken, " + unparked + " unparked, " + spunWoken + " spun to their wake, " + spunParked
                    + " parked after a spin";
            assertTrue(spunWoken > 0, () -> "no commit spun to its wake: " + counts);
            // A commit that spun to its wake never announced a park; one that parked after its spin was unparked
            // unless its wake ran before it announced the park
            assertTrue(unparked <= woken - spunWoken, () -> "a wake unparked a commit that spun to it: " + counts);
            assertTrue(unparked >= woken - spunWoken - spunParked, () -> "a wake skipped a parked commit: " + counts);
            assertTrue(woken <= (long) committers * rounds, counts);
            awaitSpinners(0);
        }
    }

    @Test
    @DisplayName("commits on virtual threads and waits with a timeout never spin, and every one of them is unparked")
    void virtualAndTimedNeverSpin() throws Exception {
        ProcessFlusher.maxSpinnersForTests(2);
        var disk = CrashFileSystem.create();
        disk.intercept(new HeldForces(50_000));
        var committers = 8;
        var rounds = 200;
        try (var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF)) {
            var log = log(ledger);
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var threads = new ArrayList<Thread>();
            for (var number = 0; number < committers; number++) {
                var committer = number;
                var virtual = committer % 2 == 0;
                Runnable body = () -> {
                    try {
                        for (var round = 0; round < rounds; round++) {
                            var commit = ledger.commit(ledger.newBatch().put(NAMES, committer + "-" + round, VALUE));
                            if (virtual) {
                                commit.awaitDurable();
                            } else {
                                assertTrue(commit.awaitDurable(Duration.ofSeconds(60)), "a commit was not durable in 60 seconds");
                            }
                        }
                    } catch (Throwable throwable) {
                        failures.add(throwable);
                    }
                };
                threads.add(virtual ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
            }
            for (var thread : threads) {
                assertEnds(thread);
            }
            assertTrue(failures.isEmpty(), () -> "a committer failed: " + failures);
            awaitWakes(log);
            assertEquals(0, log.spinsWokenForTests() + log.spinsParkedForTests());
            assertEquals(log.wokenWaitersForTests(), log.unparkedWaitersForTests());
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {0, 1, 2, 3, 4, 5})
    @DisplayName("a store stopped at a force while platform committers spin keeps every acknowledged commit, and each committer's commits as a prefix")
    void crashWhileSpinning(int seed) throws Exception {
        ProcessFlusher.maxSpinnersForTests(2);
        var random = new Random(seed);
        var disk = seed % 3 == 2 ? CrashFileSystem.createNtfs() : CrashFileSystem.create();
        disk.intercept(new HeldForces(100_000));
        disk.stopAtForce(5 + random.nextInt(250));
        var committers = 8;
        var acknowledged = ConcurrentHashMap.<String>newKeySet();
        var rounds = new AtomicIntegerArray(committers);
        var ended = new ConcurrentLinkedQueue<Throwable>();
        var ledger = open(disk.getPath("/store"), WriteThrough.Mode.OFF);
        var threads = new ArrayList<Thread>();
        for (var number = 0; number < committers; number++) {
            var committer = number;
            threads.add(Thread.ofPlatform().start(() -> {
                for (var round = 0; ; round++) {
                    var key = committer + "-" + round;
                    rounds.set(committer, round + 1);
                    try {
                        ledger.commit(ledger.newBatch().put(NAMES, key, VALUE)).awaitDurable();
                        acknowledged.add(key);
                    } catch (Throwable throwable) {
                        ended.add(throwable);
                        return;
                    }
                }
            }));
        }
        for (var thread : threads) {
            assertEnds(thread);
        }
        for (var throwable : ended) {
            assertTrue(throwable instanceof LedgerException || throwable instanceof IllegalStateException, () -> "a committer ended with " + throwable);
        }
        awaitSpinners(0);
        var survivor = seed % 2 == 0 ? disk.afterPowerLoss(new Random(seed * 31L)) : disk.afterProcessCrash();
        try {
            ledger.close();
        } catch (LedgerException _) {
            // The log stopped with frames it could not make durable
        }
        try (var reopened = open(survivor.getPath("/store"), WriteThrough.Mode.OFF)) {
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
}
