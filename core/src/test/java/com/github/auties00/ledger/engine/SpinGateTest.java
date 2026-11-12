package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// The gate that lets only a few waiting platform threads of the commit pipeline spin once the committing threads
/// outnumber the processors: its slots and flags, the waits that consult it, which must give back every slot they take
/// on every way out and still help while gated, and stores whose committers outnumber the processors, in memory and on a
/// disk that stops mid-workload.
class SpinGateTest {
    private static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> VALUES = Keyspace.paged("values", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());

    @Test
    @DisplayName("no more threads hold a slot at once than the limit, and every slot comes back")
    void slotsNeverExceedLimit() throws Exception {
        var slots = new Padded.Slots();
        var holders = new AtomicInteger();
        var most = new AtomicInteger();
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 16; t++) {
            threads.add(Thread.ofPlatform().start(() -> {
                for (var i = 0; i < 100_000; i++) {
                    if (slots.tryTake(2)) {
                        most.accumulateAndGet(holders.incrementAndGet(), Math::max);
                        holders.decrementAndGet();
                        slots.giveBack();
                    }
                }
            }));
        }
        for (var thread : threads) {
            thread.join();
        }
        assertTrue(most.get() <= 2, () -> most.get() + " holders at once");
        assertEquals(0, slots.taken());
        assertFalse(new Padded.Slots().tryTake(0));
    }

    @Test
    @DisplayName("a platform thread's flag is raised while it commits and lowered while it parks; a virtual thread has none")
    void flags() throws Exception {
        var gate = new SpinGate();
        var flag = gate.enter();
        assertTrue(flag >= 0);
        assertEquals(1, gate.raised());
        gate.parks();
        assertEquals(0, gate.raised());
        gate.unparks();
        assertEquals(1, gate.raised());
        assertEquals(SpinGate.FREE, gate.admit());
        gate.exit(flag);
        assertEquals(0, gate.raised());
        var virtual = new int[1];
        Thread.ofVirtual().start(() -> {
            virtual[0] = gate.enter();
            gate.parks();
            gate.unparks();
        }).join();
        assertEquals(-1, virtual[0]);
        assertEquals(0, gate.raised());
        gate.exit(-1);
        assertEquals(0, gate.raised());
    }

    @Test
    @DisplayName("once the committing threads outnumber the processors, only the slots' worth of waits spin on")
    void closesWhenOversubscribed() throws Exception {
        var gate = new SpinGate();
        try (var committers = Committers.closing(gate)) {
            for (var i = 0; i < SpinGate.SLOTS; i++) {
                assertEquals(SpinGate.HOLDING, gate.admit());
            }
            assertEquals(SpinGate.GATED, gate.admit());
            assertFalse(gate.reclaim());
            gate.release();
            if (SpinGate.SLOTS > 0) {
                assertTrue(gate.reclaim());
                gate.release();
            }
            for (var i = 1; i < SpinGate.SLOTS; i++) {
                gate.release();
            }
            assertEquals(0, gate.spinning());
        }
        assertEquals(0, gate.raised());
        assertEquals(SpinGate.FREE, gate.admit());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Gate.class)
    @DisplayName("a wait for an earlier commit to apply spins its whole budget unless the closed gate has no slot left, and then yields from the gate on")
    void appliedWaitYieldsWhenGated(Gate state) throws Exception {
        var gate = new SpinGate();
        try (var _ = state.prepare(gate)) {
            var commit = new PendingCommit(null, null, null, null, null, null);
            var waiter = Thread.ofPlatform().start(() -> commit.awaitApplied(gate));
            awaitYielding(gate, waiter);
            commit.advanceApplied();
            waiter.join(Duration.ofSeconds(30));
            assertFalse(waiter.isAlive(), "the waiter hangs");
            assertEquals(state.spins, gate.yieldedAfter(waiter));
            assertEquals(state.taken, gate.spinning());
            state.releaseTaken(gate);
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Gate.class)
    @DisplayName("a wait for a commit to publish spins its whole budget unless the closed gate has no slot left, and then yields from the gate on")
    void publishWaitYieldsWhenGated(Gate state) throws Exception {
        var gate = new SpinGate();
        try (var _ = state.prepare(gate)) {
            // Made on the waiter's thread, the owner that a move to DONE wakes
            var commit = new PendingCommit[1];
            var made = new CountDownLatch(1);
            // The role held, so the wait never helps
            var role = new Padded.Role();
            assertTrue(role.tryAcquire());
            var waiter = Thread.ofPlatform().start(() -> {
                commit[0] = new PendingCommit(null, null, null, null, null, null);
                made.countDown();
                commit[0].awaitHelping(PendingCommit.DONE, role, new Padded.Role(), () -> {
                }, gate);
            });
            made.await();
            awaitYielding(gate, waiter);
            commit[0].advance(PendingCommit.DONE);
            waiter.join(Duration.ofSeconds(30));
            assertFalse(waiter.isAlive(), "the waiter hangs");
            assertEquals(state.spins, gate.yieldedAfter(waiter));
            assertEquals(state.taken, gate.spinning());
            state.releaseTaken(gate);
        }
    }

    @Test
    @DisplayName("a wait the closed gate keeps from spinning helps at once, before its first yield, and ends when its help publishes it")
    void gatedWaitHelps() throws Exception {
        var gate = new SpinGate();
        try (var _ = Gate.SLOTS_TAKEN.prepare(gate)) {
            var commit = new PendingCommit(null, null, null, null, null, null);
            var helps = new AtomicInteger();
            var yieldedAtHelp = new AtomicInteger(-1);
            commit.awaitHelping(PendingCommit.DONE, new Padded.Role(), new Padded.Role(), () -> {
                // The help points at spins 63, 127, 191 and 255 come before the gate, the fifth after it
                if (helps.incrementAndGet() == 5) {
                    yieldedAtHelp.set(gate.yieldedAfter(Thread.currentThread()));
                    commit.advance(PendingCommit.DONE);
                }
            }, gate);
            assertEquals(5, helps.get());
            assertEquals(PendingCommit.FREE_SPINS, yieldedAtHelp.get(), "the fifth help came while the wait still spun");
            assertEquals(Gate.SLOTS_TAKEN.taken, gate.spinning());
            Gate.SLOTS_TAKEN.releaseTaken(gate);
        }
    }

    @Test
    @DisplayName("a wait holding a slot gives it back before it helps, so a help that throws leaks no slot")
    void helpThatThrows() throws Exception {
        var gate = new SpinGate();
        try (var committers = Committers.closing(gate)) {
            var commit = new PendingCommit(null, null, null, null, null, null);
            var helps = new AtomicInteger();
            var spinningInHelp = new AtomicInteger(-1);
            var failure = assertThrows(IllegalStateException.class, () -> commit.awaitHelping(PendingCommit.DONE,
                    new Padded.Role(), new Padded.Role(), () -> {
                        // The help points at spins 63, 127, 191 and 255 come before the gate
                        if (helps.incrementAndGet() == 5) {
                            spinningInHelp.set(gate.spinning());
                            throw new IllegalStateException("help");
                        }
                    }, gate));
            assertEquals("help", failure.getMessage());
            assertEquals(0, spinningInHelp.get());
            assertEquals(0, gate.spinning());
        }
    }

    @Test
    @DisplayName("waits past the gate give their slots back however they end: done while spinning, yielding or parked")
    void slotsComeBack() throws Exception {
        var gate = new SpinGate();
        try (var committers = Committers.closing(gate)) {
            for (var delay : new long[]{0, 1, 20}) {
                // Made on the waiter's thread, the owner that a move to DONE wakes
                var commits = new PendingCommit[2];
                var made = new CountDownLatch(1);
                // The role held, so the publishing wait helps only after a park that ended early
                var role = new Padded.Role();
                assertTrue(role.tryAcquire());
                var waiter = Thread.ofPlatform().start(() -> {
                    var flag = gate.enter();
                    try {
                        commits[0] = new PendingCommit(null, null, null, null, null, null);
                        commits[1] = new PendingCommit(null, null, null, null, null, null);
                        made.countDown();
                        commits[0].awaitApplied(gate);
                        commits[1].awaitHelping(PendingCommit.DONE, role, new Padded.Role(), () -> {
                        }, gate);
                    } finally {
                        gate.exit(flag);
                    }
                });
                var mover = Thread.ofPlatform().start(() -> {
                    try {
                        made.await();
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                    sleep(delay);
                    commits[0].advanceApplied();
                    sleep(delay);
                    commits[1].advance(PendingCommit.DONE);
                });
                mover.join();
                waiter.join(Duration.ofSeconds(30));
                assertFalse(waiter.isAlive(), "the waiter hangs");
                assertEquals(0, gate.spinning());
            }
        }
        assertEquals(0, gate.raised());
    }

    @Test
    @DisplayName("more platform committers than processors on hot keys, with range removals and snapshots, all finish with exact counts and leave the gate clear")
    void oversubscribedStress() throws Exception {
        try (var ledger = (LiveLedger) Ledger.builder().memory().keyspace(VALUES).build()) {
            // Flags raised beside the store's own, so that the gate is closed whatever the committers' hashes
            var closing = Committers.closing(ledger.spinGate());
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<String>();
            var commits = new AtomicLong();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 2 * PROCESSORS + 4; t++) {
                var id = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(id);
                    try {
                        while (!stop.get()) {
                            var choice = random.nextInt(100);
                            if (choice < 1) {
                                var from = random.nextInt(32);
                                ledger.commit(ledger.newBatch().deleteRange(VALUES, KeyRange.between("k" + from, "k" + (from + 3))));
                            } else if (choice < 3) {
                                ledger.update(transaction -> transaction.put(VALUES, "k" + random.nextInt(32), "t"));
                            } else if (choice < 5) {
                                try (var snapshot = ledger.snapshot()) {
                                    var seen = snapshot.range(VALUES, KeyRange.all(), 100_000).size();
                                    if (seen != snapshot.size(VALUES)) {
                                        failures.add("snapshot read " + seen + " entries where it counts " + snapshot.size(VALUES));
                                    }
                                }
                            } else {
                                var batch = ledger.newBatch();
                                for (var i = 0; i < 1 + random.nextInt(3); i++) {
                                    // Hot keys, so later commits wait for earlier ones to apply
                                    var key = random.nextBoolean() ? "k" + random.nextInt(32) : "own" + id + "-" + random.nextInt(256);
                                    if (random.nextInt(5) == 0) {
                                        batch.delete(VALUES, key);
                                    } else {
                                        batch.put(VALUES, key, "v" + id);
                                    }
                                }
                                ledger.commit(batch);
                            }
                            commits.incrementAndGet();
                        }
                    } catch (RuntimeException | Error failure) {
                        if (failures.isEmpty()) {
                            failure.printStackTrace();
                        }
                        failures.add(failure.toString());
                    }
                }));
            }
            Thread.sleep(2_500);
            stop.set(true);
            for (var thread : threads) {
                thread.join(Duration.ofSeconds(60));
                assertFalse(thread.isAlive(), "a committer hangs");
            }
            closing.close();
            assertTrue(failures.isEmpty(), () -> failures.size() + " failures, first: " + failures.peek());
            assertTrue(commits.get() > 1_000, () -> commits + " commits");
            assertTrue(ledger.spinGate().closedAnswers() > 0, "no wait asked the closed gate");
            assertEquals(ledger.range(VALUES, KeyRange.all(), 100_000).size(), ledger.size(VALUES));
            assertEquals(0, ledger.spinGate().spinning());
            assertEquals(0, ledger.spinGate().raised());
        }
    }

    @ParameterizedTest(name = "stop at {0} fifths of the workload's changes")
    @ValueSource(ints = {1, 2, 3, 4})
    @DisplayName("more platform committers than processors on a disk that stops mid-workload all end, leave the gate clear, and recover a prefix of each one's commits that holds every durable one")
    void oversubscribedCrash(int fifths) throws Exception {
        var threadCount = 2 * PROCESSORS + 4;
        var dry = CrashFileSystem.create();
        CrashRun run = crashWorkload(dry, threadCount);
        var disk = dry;
        var changes = 0L;
        // The changes of a run vary with how the commits group, so a run that ends before its stop counts again
        for (var attempt = 0; !disk.stopped(); attempt++) {
            var ended = run;
            assertTrue(ended.failures().isEmpty(), () -> "a commit failed on a disk that did not stop: " + ended.failures().peek());
            assertTrue(attempt < 8, "the disk never stopped");
            try {
                ((LiveLedger) run.ledger()).close();
            } catch (LedgerIoException _) {
                // The disk stopped in the close's checkpoint, after the workload; the next attempt stops earlier
            }
            changes = Math.max(1, disk.changes() * fifths / 5);
            disk = CrashFileSystem.create();
            disk.stopAfter(changes);
            run = crashWorkload(disk, threadCount);
        }
        var live = (LiveLedger) run.ledger();
        assertEquals(0, live.spinGate().spinning());
        assertEquals(0, live.spinGate().raised());
        try {
            live.closeWithoutCheckpoint();
        } catch (RuntimeException _) {
            // The stopped disk may fail the close
        }
        var survivor = disk.afterPowerLoss(new Random(changes));
        try (var recovered = open(survivor.getPath("/store"))) {
            for (var id = 0; id < threadCount; id++) {
                var count = 0;
                while (recovered.get(VALUES, key(id, count)).isPresent()) {
                    count++;
                }
                var present = recovered.range(VALUES, KeyRange.between(key(id, 0), key(id, 999_999)), 100_000).size();
                assertEquals(count, present, "thread " + id + " recovered commits that are not a prefix");
                var durable = run.acknowledged()[id];
                assertTrue(count > durable, "thread " + id + " lost durable commit " + durable);
            }
        }
    }

    /// Runs `threadCount` platform threads committing to a store on `disk` until each has made its commits or one
    /// fails, and checks that every one of them ends.
    private static CrashRun crashWorkload(CrashFileSystem disk, int threadCount) throws InterruptedException {
        var acknowledged = new int[threadCount];
        var ledger = open(disk.getPath("/store"));
        // Flags raised beside the store's own, so that the gate is closed whatever the committers' hashes
        var closing = Committers.closing(((LiveLedger) ledger).spinGate());
        var failures = new ConcurrentLinkedQueue<RuntimeException>();
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < threadCount; t++) {
            var id = t;
            acknowledged[id] = -1;
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    for (var n = 0; n < 1_000; n++) {
                        var commit = ledger.commit(ledger.newBatch()
                                .put(VALUES, key(id, n), "v" + n)
                                // A key every thread writes, so commits wait for earlier ones to apply
                                .put(CHATS, "chat" + (n % 4), "t" + id));
                        if (n % 16 == 15 && commit.awaitDurable(Duration.ofSeconds(30))) {
                            acknowledged[id] = n;
                        }
                    }
                } catch (RuntimeException failure) {
                    // The disk stopped
                    failures.add(failure);
                } catch (InterruptedException exception) {
                    throw new AssertionError(exception);
                }
            }));
        }
        for (var thread : threads) {
            thread.join(Duration.ofSeconds(120));
            assertFalse(thread.isAlive(), "a committer hangs");
        }
        closing.close();
        return new CrashRun(ledger, acknowledged, failures);
    }

    /// A store a crash workload ran on, the last commit of each thread it acknowledged durable, or -1, and the
    /// failures of the threads.
    private record CrashRun(Ledger ledger, int[] acknowledged, ConcurrentLinkedQueue<RuntimeException> failures) {
    }

    private static Ledger open(Path directory) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(VALUES).keyspace(CHATS))
                .segmentSize(16_384)
                .build();
    }

    private static String key(int thread, int n) {
        return "t%03d-%06d".formatted(thread, n);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            throw new AssertionError(exception);
        }
    }

    /// Waits, spinning, until the wait of `waiter` turned to yielding.
    private static void awaitYielding(SpinGate gate, Thread waiter) {
        while (gate.yieldedAfter(waiter) == 0) {
            assertTrue(waiter.isAlive(), "the waiter ended before it yielded");
            Thread.onSpinWait();
        }
    }

    /// A state of the gate a wait asks once it outlasts its first stretch, with the spins after which the wait turns
    /// to yielding.
    private enum Gate {
        OPEN(PendingCommit.PLATFORM_SPINS),
        SLOT_FREE(PendingCommit.PLATFORM_SPINS),
        SLOTS_TAKEN(PendingCommit.FREE_SPINS);

        /// The spins after which a wait turns to yielding.
        final int spins;
        /// The slots the test's thread holds while the state lasts.
        final int taken;

        Gate(int spins) {
            this.spins = spins;
            this.taken = spins == PendingCommit.FREE_SPINS ? SpinGate.SLOTS : 0;
        }

        /// Puts `gate` in this state: closed, unless open, by flags raised beside it, with every slot held by the
        /// current thread if taken.
        ///
        /// @return the threads holding the flags, which the close stops
        Committers prepare(SpinGate gate) throws InterruptedException {
            if (this == OPEN) {
                return new Committers(gate, new CountDownLatch(0), List.of());
            }
            assumeTrue(this != SLOT_FREE || SpinGate.SLOTS > 0, "no slot to take");
            var committers = Committers.closing(gate);
            for (var i = 0; i < taken; i++) {
                assertEquals(SpinGate.HOLDING, gate.admit());
            }
            if (this == SLOTS_TAKEN) {
                assertEquals(SpinGate.GATED, gate.admit());
            }
            return committers;
        }

        /// Gives back the slots [#prepare(SpinGate)] took.
        void releaseTaken(SpinGate gate) {
            for (var i = 0; i < taken; i++) {
                gate.release();
            }
            assertEquals(0, gate.spinning());
        }
    }

    /// Platform threads that each raise their flag of a gate, started until more flags are raised than there are
    /// processors, and stopped by the close.
    private record Committers(SpinGate gate, CountDownLatch done, List<Thread> threads) implements AutoCloseable {
        static Committers closing(SpinGate gate) throws InterruptedException {
            var done = new CountDownLatch(1);
            var threads = new ArrayList<Thread>();
            while (gate.raised() <= PROCESSORS) {
                assertTrue(threads.size() < 4_096, "the flags never outnumber the processors");
                var raised = new CountDownLatch(1);
                threads.add(Thread.ofPlatform().daemon().start(() -> {
                    var flag = gate.enter();
                    raised.countDown();
                    try {
                        done.await();
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    } finally {
                        gate.exit(flag);
                    }
                }));
                raised.await();
            }
            return new Committers(gate, done, threads);
        }

        @Override
        public void close() throws InterruptedException {
            done.countDown();
            for (var thread : threads) {
                thread.join();
            }
        }
    }
}
