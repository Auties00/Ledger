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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The wait of the flush of a store's lane for a commit expected to write its own group: it gives way to that commit
/// when it waits for its durability at once, and holds back neither a commit that is never waited for, nor one waited
/// for with a timeout or by another thread, nor a close; a run of waits that ran out turns the waits off for a while;
/// a store on a pool never waits.
///
/// Each test holds the lane's flushes at their start while a commit waits for its durability, so that commit writes its
/// own group and the next one on its thread is expected to as well, whatever the machine's timing.
@Timeout(120)
class LeadWaitTest {
    private static final byte[] KEY = new byte[32];
    private static final String LANE_THREADS = "ledger-io-lead-";
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());

    @AfterEach
    void restoreSpinners() {
        ProcessFlusher.maxSpinnersForTests(-1);
    }

    /// Opens a store and makes one commit durable, so the log has a segment open and a commit can write its own group.
    private static Ledger open(Path directory, ProcessFlusher flusher) throws InterruptedException {
        var builder = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES);
        var ledger = ((LedgerBuilder) builder).flusher(flusher).build();
        ledger.commit(ledger.newBatch().put(NAMES, "open", "v")).awaitDurable();
        return ledger;
    }

    private static ProcessFlusher flusher() {
        return new ProcessFlusher(Thread.ofPlatform().daemon().name(LANE_THREADS, 0).factory());
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).logForTests();
    }

    /// Holds every flush of the lane of `log` at its start while `held` is set.
    private static void gate(LogWriter log, AtomicBoolean held) {
        log.probe().flushHook = () -> {
            var deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            while (held.get() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        };
    }

    private static Commit put(Ledger ledger, Map<String, String> model, String key) {
        var value = "v".repeat(300) + key;
        model.put(key, value);
        return ledger.commit(ledger.newBatch().put(NAMES, key, value));
    }

    /// Commits `key` and waits for it while the lane's flushes are held, so the calling thread writes its own group.
    private static void lead(Ledger ledger, Map<String, String> model, AtomicBoolean held, String key) throws InterruptedException {
        held.set(true);
        try {
            put(ledger, model, key).awaitDurable();
        } finally {
            held.set(false);
        }
    }

    private static void awaitDurable(Commit commit) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!commit.isDurable()) {
            assertTrue(System.nanoTime() < deadline, "the commit never became durable");
            Thread.sleep(1);
        }
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        // The commit every open makes
        entries.remove("open");
        return entries;
    }

    @Test
    @DisplayName("expects the next commit of the thread that wrote the last group alone to lead, and none of another thread")
    void hintFollowsTheCommitter() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            lead(ledger, model, held, "a");
            held.set(true);
            var own = put(ledger, model, "b");
            assertEquals(own.sequence(), log.leadHintForTests(), "the thread that led is expected to lead again");
            own.awaitDurable();
            // The thread that led commits nothing; another commits, and the thread that led waits for it
            var other = new AtomicReference<Commit>();
            var committer = Thread.ofPlatform().start(() -> other.set(put(ledger, new TreeMap<>(), "c")));
            committer.join();
            model.put("c", "v".repeat(300) + "c");
            assertEquals(0, log.leadHintForTests(), "a commit of another thread is not expected to lead");
            other.get().awaitDurable();
            // Now the waiting thread wrote the last group alone, and the committer commits again
            committer = Thread.ofPlatform().start(() -> other.set(put(ledger, new TreeMap<>(), "d")));
            committer.join();
            model.put("d", "v".repeat(300) + "d");
            assertEquals(0, log.leadHintForTests(), "a commit awaited by another thread is not expected to lead");
            held.set(false);
            // Awaited a millisecond later by another thread: the flush takes the group at once
            var late = new AtomicReference<Throwable>();
            var awaiter = Thread.ofPlatform().start(() -> {
                try {
                    Thread.sleep(1);
                    assertTrue(other.get().awaitDurable(Duration.ofSeconds(30)));
                } catch (Throwable throwable) {
                    late.set(throwable);
                }
            });
            awaiter.join();
            if (late.get() != null) {
                throw new AssertionError(late.get());
            }
            assertEquals(0, log.leadsMissedForTests());
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("gives the group to the commit that leads, which writes every group of a thread that commits and waits in turn")
    void givesWayToTheLeader() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        var committer = Thread.currentThread();
        var ownForces = new AtomicInteger();
        var laneForces = new AtomicInteger();
        var counting = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (counting.get() && kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                if (Thread.currentThread() == committer) {
                    ownForces.incrementAndGet();
                } else if (Thread.currentThread().getName().startsWith(LANE_THREADS)) {
                    laneForces.incrementAndGet();
                }
            }
        });
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            // The flush waits for the commit however long it takes to lead, whatever thread the lane found
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(1);
            log.probe().leadWaitAlways = true;
            lead(ledger, model, held, "first");
            counting.set(true);
            // Enough that a lane's flush taking running in the moment the commit tries, before it sees the frame it
            // then gives back, happens many times over
            for (var i = 0; i < 2_000; i++) {
                put(ledger, model, "k" + i).awaitDurable();
            }
            counting.set(false);
            assertTrue(ownForces.get() >= 2_000, "the committing thread forced " + ownForces.get() + " times, the lane "
                    + laneForces.get());
            assertEquals(0, laneForces.get(), "the lane took a group from the commit that led");
            assertEquals(0, log.leadsMissedForTests());
            assertTrue(log.leadsTakenForTests() > 0, "the lane's flushes never waited");
            assertEquals(model, contents(ledger));
        }
        try (var ledger = open(disk.afterPowerLoss(new Random(1)).getPath("/store"), ProcessFlusher.process())) {
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("lets a commit that found the lane's flush holding the writer write its group once the flush gives it back")
    void retakesTheGroupGivenBack() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        var committer = Thread.currentThread();
        var laneForces = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")
                    && Thread.currentThread() != committer && Thread.currentThread().getName().startsWith(LANE_THREADS)) {
                laneForces.incrementAndGet();
            }
        });
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(1);
            log.probe().leadWaitAlways = true;
            for (var i = 0; i < 20; i++) {
                var forces = laneForces.get();
                var retaken = log.leadsRetakenForTests();
                var waits = log.retakeWaitsForTests();
                holdingRunning(ledger, model, held, log, "led" + i, "k" + i, () -> log.retakeWaitsForTests() > waits);
                assertEquals(forces, laneForces.get(), "the lane wrote the group it gave back");
                assertEquals(retaken + 1, log.leadsRetakenForTests(), "the commit did not write the group given back");
            }
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("wakes a commit expected to lead whose group the lane's flush took, once the group is durable")
    void lostLeadIsWoken() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        var laneForces = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")
                    && Thread.currentThread().getName().startsWith(LANE_THREADS)) {
                laneForces.incrementAndGet();
            }
        });
        // No flush gives a group back nor waits for a commit, and no commit tries again
        ProcessFlusher.maxSpinnersForTests(0);
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            var committer = Thread.currentThread();
            for (var i = 0; i < 20; i++) {
                var forces = laneForces.get();
                holdingRunning(ledger, model, held, log, "led" + i, "k" + i,
                        () -> committer.getState() == Thread.State.WAITING);
                assertTrue(laneForces.get() > forces, "the lane did not write the group it took");
            }
            assertEquals(0, log.retakeWaitsForTests() + log.leadsTakenForTests() + log.leadsMissedForTests());
            assertEquals(model, contents(ledger));
        }
    }

    /// Once the lane is idle, commits `led` and waits for it while the lane's flush is held at its start, so the calling
    /// thread writes its own group; then lets that flush take the writer and hold it, before it looks at the queue,
    /// until `next` is committed on the same thread and `tried` holds, once that commit found the writer taken; then
    /// waits for `next`.
    private static void holdingRunning(Ledger ledger, Map<String, String> model, AtomicBoolean held, LogWriter log,
                                       String led, String next, BooleanSupplier tried) throws InterruptedException {
        // A flush still running from before would take the writer past the gate
        var idle = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!log.laneIdleForTests()) {
            assertTrue(System.nanoTime() < idle, "the lane never went idle");
            Thread.onSpinWait();
        }
        var entered = new AtomicBoolean();
        var after = new AtomicLong(Long.MAX_VALUE);
        log.probe().takeHook = () -> {
            if (!Thread.currentThread().getName().startsWith(LANE_THREADS) || !entered.compareAndSet(false, true)) {
                return;
            }
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (log.leadHintForTests() <= after.get() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            while (!tried.getAsBoolean() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
        };
        try {
            held.set(true);
            var first = put(ledger, model, led);
            first.awaitDurable();
            after.set(first.sequence());
            held.set(false);
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (!entered.get()) {
                assertTrue(System.nanoTime() < deadline, "the lane's flush never took the writer");
                Thread.onSpinWait();
            }
            var commit = put(ledger, model, next);
            assertEquals(commit.sequence(), log.leadHintForTests(), "the commit is not expected to lead");
            commit.awaitDurable();
        } finally {
            held.set(false);
            log.probe().takeHook = null;
        }
    }

    @Test
    @DisplayName("stops waiting at once for a commit waited for with a timeout, which never leads")
    void timedWait() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(2);
            log.probe().leadWaitAlways = true;
            lead(ledger, model, held, "first");
            for (var i = 0; i < 20; i++) {
                var commit = put(ledger, model, "k" + i);
                assertNotEquals(0, log.leadHintForTests());
                // Shorter than the flush would wait: the commit's wait ends it
                assertTrue(commit.awaitDurable(Duration.ofSeconds(30)), "the flush kept waiting for a timed wait");
                lead(ledger, model, held, "led" + i);
            }
            assertEquals(0, log.leadsMissedForTests());
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("makes a commit never waited for durable once the wait runs out, and turns the waits off for twice as many groups after each in a row")
    void unawaited() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWaitAlways = true;
            lead(ledger, model, held, "first");
            for (var round = 1; round <= 3; round++) {
                var commit = put(ledger, model, "unawaited" + round);
                assertNotEquals(0, log.leadHintForTests(), "the hints stayed off past the groups of round " + (round - 1));
                awaitDurable(commit);
                assertEquals(round, log.leadsMissedForTests());
                offGroups(ledger, model, held, log, 1 << round, "r" + round);
            }
            // A commit that leads while the flush waits for it resets the count
            var taken = log.leadsTakenForTests();
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(1);
            // The flush sees the lead only if its thread reaches the queue before the commit's group is durable, which
            // a loaded machine can delay past it: commit again until it does
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            for (var led = 0; log.leadsTakenForTests() <= taken; led++) {
                assertTrue(System.nanoTime() < deadline, "the lane's flush never saw a commit lead");
                put(ledger, model, "led" + led).awaitDurable();
                awaitLeadsTaken(log, taken + 1, TimeUnit.MILLISECONDS.toNanos(200));
            }
            log.probe().leadWait = 0;
            var commit = put(ledger, model, "unawaited");
            assertNotEquals(0, log.leadHintForTests());
            awaitDurable(commit);
            assertEquals(4, log.leadsMissedForTests());
            offGroups(ledger, model, held, log, 2, "reset");
            put(ledger, model, "last");
            assertNotEquals(0, log.leadHintForTests(), "a count not reset turned the hints off for more than 2 groups");
            held.set(true);
            ledger.awaitDurable();
            held.set(false);
            assertEquals(model, contents(ledger));
        }
    }

    /// Commits `groups` groups, each written by the calling thread, none of whose frames the flush is expected to wait
    /// for.
    private static void offGroups(Ledger ledger, Map<String, String> model, AtomicBoolean held, LogWriter log, int groups,
                                  String prefix) throws InterruptedException {
        for (var group = 0; group < groups; group++) {
            held.set(true);
            var next = put(ledger, model, prefix + "g" + group);
            assertEquals(0, log.leadHintForTests(), prefix + " group " + group);
            next.awaitDurable();
            held.set(false);
        }
    }

    /// Waits until no place among the process's spinners is taken, failing if one stays taken: a flusher thread that
    /// waits for its next lane spins holding a place for a bounded time, so one may be taken just after the test gave
    /// its own back.
    private static void awaitNoSpinners() throws InterruptedException {
        var deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (ProcessFlusher.spinnersForTests() != 0) {
            assertTrue(System.nanoTime() < deadline, "a place was left taken");
            Thread.sleep(1);
        }
    }

    /// Waits until the lane's flush saw at least `taken` commits lead, for at most `nanos`.
    private static void awaitLeadsTaken(LogWriter log, long taken, long nanos) throws InterruptedException {
        var deadline = System.nanoTime() + nanos;
        while (log.leadsTakenForTests() < taken && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
    }

    @Test
    @DisplayName("makes an interrupted commit durable, which then leads or waits as any other")
    void interrupted() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWaitAlways = true;
            for (var i = 0; i < 20; i++) {
                lead(ledger, model, held, "led" + i);
                var commit = put(ledger, model, "k" + i);
                Thread.currentThread().interrupt();
                try {
                    commit.awaitDurable();
                } catch (InterruptedException _) {
                    commit.awaitDurable();
                }
                assertTrue(commit.isDurable());
            }
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("lets a close end the wait and make the commit it waited for durable")
    void closeEndsTheWait() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        var ledger = open(disk.getPath("/store"), flusher());
        var closed = false;
        try {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(2);
            log.probe().leadWaitAlways = true;
            lead(ledger, model, held, "first");
            var commit = put(ledger, model, "unawaited");
            assertNotEquals(0, log.leadHintForTests());
            // The lane's flush is waiting for the commit by now, or starts after the close began
            Thread.sleep(50);
            var start = System.nanoTime();
            ledger.close();
            closed = true;
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(60), "the close waited out the flush's wait");
            assertTrue(commit.isDurable());
        } finally {
            if (!closed) {
                ledger.close();
            }
        }
        try (var reopened = open(disk.afterPowerLoss(new Random(2)).getPath("/store"), ProcessFlusher.process())) {
            assertEquals(model, contents(reopened));
        }
    }

    @Test
    @DisplayName("never waits for a commit on a pool another store shares, whose thread the wait would hold")
    void pool() throws Exception {
        var disk = CrashFileSystem.create();
        var models = new TreeMap<String, String>();
        try (var pool = IoPool.create(1);
             var first = Ledger.builder().directory(disk.getPath("/a")).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool).build();
             var second = Ledger.builder().directory(disk.getPath("/b")).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool).build()) {
            for (var i = 0; i < 100; i++) {
                for (var ledger : new Ledger[]{first, second}) {
                    put(ledger, models, "k" + i).awaitDurable();
                    assertEquals(0, log(ledger).leadHintForTests());
                }
            }
            assertEquals(0, log(first).leadsTakenForTests() + log(second).leadsTakenForTests());
            assertEquals(0, log(first).leadsMissedForTests() + log(second).leadsMissedForTests());
        }
    }

    @Test
    @DisplayName("waits only with a place among the process's spinners, none of which are left taken")
    void spinnerPlace() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        ProcessFlusher.maxSpinnersForTests(0);
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            lead(ledger, model, held, "first");
            for (var i = 0; i < 200; i++) {
                put(ledger, model, "k" + i).awaitDurable();
            }
            assertEquals(0, log.leadsTakenForTests() + log.leadsMissedForTests(), "a flush waited with no place");
            assertEquals(0, ProcessFlusher.spinnersForTests());
            assertEquals(model, contents(ledger));
        }
    }

    @Test
    @DisplayName("retakes a group given back only with a place among the process's spinners, and parks when none is left")
    void retakeTakesAPlace() throws Exception {
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var held = new AtomicBoolean();
        ProcessFlusher.maxSpinnersForTests(1);
        try (var ledger = open(disk.getPath("/store"), flusher())) {
            var log = log(ledger);
            gate(log, held);
            log.probe().leadWait = TimeUnit.MINUTES.toNanos(1);
            var committer = Thread.currentThread();
            // The only place, as another store's spinning flush thread holds it
            assertTrue(ProcessFlusher.takeSpinner());
            try {
                for (var i = 0; i < 10; i++) {
                    var waits = log.retakeWaitsForTests();
                    holdingRunning(ledger, model, held, log, "led" + i, "k" + i,
                            () -> committer.getState() == Thread.State.WAITING);
                    assertEquals(waits, log.retakeWaitsForTests(), "a commit retook a group with no place left");
                }
            } finally {
                ProcessFlusher.releaseSpinner();
            }
            awaitNoSpinners();
            assertEquals(model, contents(ledger));
        }
    }
}
