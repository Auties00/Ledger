package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The mechanisms of the durable commit path together, under committers on platform and virtual threads that wait
/// for their commits, wait with a timeout or not at all: the linger of an open's first group with the handle opened
/// early, the commits expected to lead and their retakes, the platform waiters that spin, segments rolling over onto
/// spares across reopens; every acknowledged commit survives the losses of power drawn while they run and the stops of
/// the file system at random points, and no committer nor close hangs after a stop.
@Timeout(600)
class CommitPathStressTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final long SEGMENT = 64 << 10;
    private static final int COMMITTERS = 6;

    @AfterEach
    void restoreSpinners() {
        ProcessFlusher.maxSpinnersForTests(-1);
    }

    private static Ledger open(Path directory, WriteThrough way) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(way)
                .checkpointEvery(1L << 20)
                .build();
    }

    private static Ledger openForcing(Path directory) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static Map<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(NAMES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    /// {@return the first key of `acked` that `ledger` does not hold with its value, or `null` if it holds them all}
    private static String lost(Ledger ledger, Map<String, String> acked) {
        var recovered = contents(ledger);
        for (var entry : acked.entrySet()) {
            if (!entry.getValue().equals(recovered.get(entry.getKey()))) {
                return entry.getKey();
            }
        }
        return null;
    }

    /// A state a loss of power left, and the commits acknowledged before it was drawn.
    private record Survivor(CrashFileSystem disk, Map<String, String> acked, String label) {
    }

    /// {@return a committer of `ops` commits to `ledger` named after `cycle` and `id`, each of which it waits for,
    /// waits for with a timeout, or leaves, recording in `acked` every commit it saw durable}
    private static Runnable committer(Ledger ledger, int cycle, int id, int ops, long seed, AtomicLong counter,
                                      Map<String, String> acked, List<Throwable> failures) {
        return () -> {
            var random = new Random(seed);
            var pending = new ArrayList<Map.Entry<String, Commit>>();
            var values = new HashMap<String, String>();
            try {
                for (var op = 0; op < ops; op++) {
                    var key = "c" + cycle + "-t" + id + "-" + counter.incrementAndGet();
                    // Mostly one block, now and then a group of a few
                    var value = "x".repeat(random.nextInt(random.nextInt(8) == 0 ? 3_000 : 120)) + key;
                    var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                    values.put(key, value);
                    switch (random.nextInt(5)) {
                        case 0, 1 -> {
                            commit.awaitDurable();
                            acked.put(key, value);
                        }
                        case 2 -> {
                            if (commit.awaitDurable(Duration.ofNanos(random.nextInt(400_000)))) {
                                acked.put(key, value);
                            } else {
                                pending.add(Map.entry(key, commit));
                            }
                        }
                        default -> pending.add(Map.entry(key, commit));
                    }
                    if (random.nextInt(10) == 0) {
                        Thread.sleep(0, random.nextInt(300_000));
                    }
                    pending.removeIf(entry -> {
                        if (!entry.getValue().isDurable()) {
                            return false;
                        }
                        acked.put(entry.getKey(), values.get(entry.getKey()));
                        return true;
                    });
                }
                for (var entry : pending) {
                    entry.getValue().awaitDurable();
                    acked.put(entry.getKey(), values.get(entry.getKey()));
                }
            } catch (Throwable throwable) {
                failures.add(throwable);
            }
        };
    }

    @ParameterizedTest(name = "lead always {0}, spin budget {1}, spinners {2}")
    @CsvSource({"false,-1,2", "true,200000,2", "true,200000,1", "false,300000,1"})
    @DisplayName("every acknowledged commit survives the losses of power drawn while committers of every kind run, across reopens")
    void powerLoss(boolean leadAlways, long spinBudget, int spinners) throws Exception {
        ProcessFlusher.maxSpinnersForTests(spinners);
        var disk = CrashFileSystem.create();
        var path = disk.getPath("/store");
        var acked = new ConcurrentHashMap<String, String>();
        var survivors = new ArrayList<Survivor>();
        var failures = new CopyOnWriteArrayList<Throwable>();
        var counter = new AtomicLong();
        var random = new Random(42);
        for (var cycle = 0; cycle < 8; cycle++) {
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            way.opensEarlyForTests = true;
            try (var ledger = open(path, way)) {
                var log = ((LiveLedger) ledger).log();
                log.leadWaitAlwaysForTests = leadAlways;
                log.spinBudgetForTests = spinBudget;
                // The first group of the open as a cleared chat makes it: a commit no one waits for, then a durable one,
                // which the flush lingers for in two cycles of three
                var shape = cycle % 3;
                if (shape != 0) {
                    log.lingerNanos = 50_000_000;
                }
                var unawaited = "c" + cycle + "-unawaited";
                var first = ledger.commit(ledger.newBatch().put(NAMES, unawaited, "u" + cycle));
                if (shape == 1) {
                    var deadline = System.nanoTime() + 5_000_000_000L;
                    while (!log.lingeringForTests() && System.nanoTime() < deadline) {
                        Thread.onSpinWait();
                    }
                }
                if (shape != 2) {
                    var durable = "c" + cycle + "-first";
                    ledger.commit(ledger.newBatch().put(NAMES, durable, "f" + cycle)).awaitDurable();
                    acked.put(durable, "f" + cycle);
                    acked.put(unawaited, "u" + cycle);
                } else {
                    var value = "u" + cycle;
                    Thread.ofVirtual().start(() -> {
                        try {
                            first.awaitDurable();
                            acked.put(unawaited, value);
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    });
                }
                var threads = new ArrayList<Thread>();
                for (var id = 0; id < COMMITTERS; id++) {
                    var body = committer(ledger, cycle, id, 200, random.nextLong(), counter, acked, failures);
                    threads.add(id % 2 == 1 ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
                }
                for (var draw = 0; draw < 2; draw++) {
                    Thread.sleep(random.nextInt(30));
                    var copy = Map.copyOf(acked);
                    survivors.add(new Survivor(disk.afterPowerLoss(new Random(random.nextLong())), copy, "cycle " + cycle + ", draw " + draw));
                }
                for (var thread : threads) {
                    thread.join();
                }
                var copy = Map.copyOf(acked);
                survivors.add(new Survivor(disk.afterPowerLoss(new Random(random.nextLong())), copy, "cycle " + cycle + ", end"));
            }
            assertEquals(List.of(), failures);
            // A close makes everything durable
            survivors.add(new Survivor(disk.afterPowerLoss(new Random(random.nextLong())), Map.copyOf(acked), "cycle " + cycle + ", closed"));
        }
        var problems = new ArrayList<String>();
        for (var survivor : survivors) {
            try (var ledger = openForcing(survivor.disk().getPath("/store"))) {
                var key = lost(ledger, survivor.acked());
                if (key != null) {
                    problems.add(survivor.label() + ": lost " + key);
                }
            } catch (Throwable throwable) {
                problems.add(survivor.label() + ": " + throwable);
            }
        }
        assertEquals(List.of(), problems);
    }

    @ParameterizedTest(name = "lead always {0}, spin budget {1}")
    @CsvSource({"false,-1", "true,200000"})
    @DisplayName("no committer nor close hangs when the file system stops at a random point, and every acknowledged commit survives")
    void stops(boolean leadAlways, long spinBudget) throws Exception {
        ProcessFlusher.maxSpinnersForTests(2);
        var disk = CrashFileSystem.create();
        var acked = new ConcurrentHashMap<String, String>();
        var counter = new AtomicLong();
        var random = new Random(7);
        var unexpected = new CopyOnWriteArrayList<Throwable>();
        for (var cycle = 0; cycle < 16; cycle++) {
            var way = new WriteThrough(WriteThrough.Kind.DSYNC, null);
            way.opensEarlyForTests = true;
            var ledger = open(disk.getPath("/store"), way);
            var log = ((LiveLedger) ledger).log();
            log.leadWaitAlwaysForTests = leadAlways;
            log.spinBudgetForTests = spinBudget;
            log.lingerNanos = 20_000_000;
            var threads = new ArrayList<Thread>();
            for (var id = 0; id < COMMITTERS; id++) {
                var seed = random.nextLong();
                var name = id;
                var finalCycle = cycle;
                Runnable body = () -> {
                    var own = new Random(seed);
                    try {
                        for (var op = 0; op < 300; op++) {
                            var key = "c" + finalCycle + "-t" + name + "-" + counter.incrementAndGet();
                            var value = "x".repeat(own.nextInt(own.nextInt(8) == 0 ? 3_000 : 120)) + key;
                            var commit = ledger.commit(ledger.newBatch().put(NAMES, key, value));
                            if (own.nextInt(4) != 0) {
                                commit.awaitDurable();
                                acked.put(key, value);
                            }
                        }
                    } catch (LedgerException | IllegalStateException _) {
                        // The store stopped
                    } catch (Throwable throwable) {
                        unexpected.add(throwable);
                    }
                };
                threads.add(id % 2 == 1 ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
            }
            Thread.sleep(random.nextInt(20));
            disk.stopAfter(random.nextInt(cycle % 2 == 0 ? 5 : 200));
            for (var thread : threads) {
                thread.join(120_000);
                if (thread.isAlive()) {
                    unexpected.add(new AssertionError("cycle " + cycle + ": a committer hung after the stop: "
                            + Arrays.toString(thread.getStackTrace())));
                }
            }
            var closer = Thread.ofPlatform().start(() -> {
                try {
                    ledger.close();
                } catch (Throwable _) {
                    // A stopped store may fail its close
                }
            });
            closer.join(120_000);
            if (closer.isAlive()) {
                unexpected.add(new AssertionError("cycle " + cycle + ": the close hung: " + Arrays.toString(closer.getStackTrace())));
            }
            assertEquals(List.of(), unexpected);
            var copy = Map.copyOf(acked);
            disk = disk.afterPowerLoss(new Random(random.nextLong()));
            try (var recovered = openForcing(disk.getPath("/store"))) {
                assertNull(lost(recovered, copy), "cycle " + cycle);
            }
        }
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    @DisplayName("on Linux the descriptor writes of virtual leads beside the channel writes of the lane keep every acknowledged commit, and the log writing through")
    void linuxDescriptorsBesideChannels(@TempDir Path directory) throws Exception {
        Assumptions.assumeTrue(WriteThrough.Posix.ELIGIBLE, "the process cannot write through descriptors of the C library");
        assertTrue(WriteThrough.Posix.link(), "the calls of the C library did not link");
        var acked = new ConcurrentHashMap<String, String>();
        var failures = new CopyOnWriteArrayList<Throwable>();
        var counter = new AtomicLong();
        var random = new Random(11);
        var path = directory.resolve("store");
        var through = 0L;
        for (var cycle = 0; cycle < 6; cycle++) {
            var ledger = open(path, WriteThrough.of(WriteThrough.Mode.DIRECT, path));
            var log = ((LiveLedger) ledger).log();
            try {
                var threads = new ArrayList<Thread>();
                for (var id = 0; id < COMMITTERS; id++) {
                    var body = committer(ledger, cycle, id, 300, random.nextLong(), counter, acked, failures);
                    threads.add(id % 2 == 1 ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertEquals(List.of(), failures);
                assertNotNull(log.writeThroughKind(), "cycle " + cycle + ": the log moved to forces");
                through += log.writtenThrough();
            } finally {
                // As a crash of the process leaves the store: only what was made durable
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            try (var reopened = openForcing(path)) {
                assertNull(lost(reopened, Map.copyOf(acked)), "cycle " + cycle);
            }
        }
        assertTrue(through > 0, "no group was written through");
    }
}
