package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The threads that prepare the log's segments: a thread that finished a preparation takes the next, from any store
/// of the process, and a preparation runs once whichever of its thread, a rollover and a close gets to it first, so a
/// store whose preparations no thread starts, or starts late, still rolls over, keeps its segment at a close and loses
/// nothing to a loss of power.
class SegmentPreparersTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    // Fifteen groups of a block each to a segment
    private static final long SEGMENT = 64 << 10;
    private static final int DRAWS = 8;

    private static Ledger open(Path directory) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                .segmentSize(SEGMENT)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static void commit(Ledger ledger, Map<String, String> model, String key) throws InterruptedException {
        var value = "v".repeat(500) + key;
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

    private static void survivesPowerLoss(CrashFileSystem disk, Map<String, String> model) {
        for (var seed = 0; seed < DRAWS; seed++) {
            try (var ledger = open(disk.afterPowerLoss(new Random(seed)).getPath("/store"))) {
                var recovered = contents(ledger);
                for (var entry : model.entrySet()) {
                    assertEquals(Optional.of(entry.getValue()), Optional.ofNullable(recovered.get(entry.getKey())),
                            "seed " + seed + ", key " + entry.getKey());
                }
            }
        }
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

    @Nested
    @DisplayName("the threads")
    class Threads {
        @Test
        @DisplayName("run a preparation on a daemon platform thread named ledger-prepare, apart from the thread that starts it")
        void platformThread() throws Exception {
            var ran = new FutureTask<>(Thread::currentThread);
            SegmentPreparers.start(ran);
            var thread = ran.get(60, TimeUnit.SECONDS);
            assertFalse(thread.isVirtual());
            assertTrue(thread.isDaemon());
            assertEquals("ledger-prepare", thread.getName());
            assertFalse(thread == Thread.currentThread());
        }

        @Test
        @DisplayName("hand a preparation to a thread that finished one, rather than starting a thread for it")
        void reuse() throws Exception {
            var seen = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
            var first = new FutureTask<>(Thread::currentThread);
            SegmentPreparers.start(first);
            seen.add(first.get(60, TimeUnit.SECONDS));
            var reused = false;
            var started = SegmentPreparers.startedForTests();
            for (var attempt = 0; attempt < 100 && !reused; attempt++) {
                awaitTrue(() -> SegmentPreparers.waitingForTests() > 0, "no thread waits for a preparation");
                var next = new FutureTask<>(Thread::currentThread);
                SegmentPreparers.start(next);
                reused = !seen.add(next.get(60, TimeUnit.SECONDS));
            }
            assertTrue(reused, "every preparation had a thread of its own");
            assertTrue(SegmentPreparers.startedForTests() - started < 100, "a thread was started for every preparation");
        }

        @Test
        @DisplayName("run each of preparations started together, each on a thread of its own when none waits")
        void concurrent() throws Exception {
            var release = new CountDownLatch(1);
            var running = new CountDownLatch(4);
            var threads = Collections.newSetFromMap(new ConcurrentHashMap<Thread, Boolean>());
            var preparations = new ArrayList<FutureTask<Void>>();
            for (var i = 0; i < 4; i++) {
                var preparation = new FutureTask<Void>(() -> {
                    threads.add(Thread.currentThread());
                    running.countDown();
                    release.await();
                    return null;
                });
                preparations.add(preparation);
                SegmentPreparers.start(preparation);
            }
            assertTrue(running.await(60, TimeUnit.SECONDS), "the preparations did not all run at once");
            release.countDown();
            for (var preparation : preparations) {
                preparation.get(60, TimeUnit.SECONDS);
            }
            assertEquals(4, threads.size());
        }

        @Test
        @DisplayName("run a preparation once, however many of its thread, a rollover and a close run it beside each other")
        void runsOnce() throws Exception {
            for (var round = 0; round < 200; round++) {
                var runs = new AtomicInteger();
                var preparation = new FutureTask<>(runs::incrementAndGet);
                var gate = new CountDownLatch(1);
                var others = new ArrayList<Thread>();
                for (var i = 0; i < 3; i++) {
                    others.add(Thread.ofPlatform().start(() -> {
                        try {
                            gate.await();
                        } catch (InterruptedException _) {
                            return;
                        }
                        preparation.run();
                    }));
                }
                gate.countDown();
                SegmentPreparers.start(preparation);
                preparation.run();
                for (var other : others) {
                    other.join();
                }
                assertEquals(1, preparation.get(60, TimeUnit.SECONDS));
                assertEquals(1, runs.get());
            }
        }
    }

    @Nested
    @DisplayName("a store")
    class Stores {
        @Test
        @DisplayName("settles the preparation its creation starts, with no commit of its own")
        void openSettles() throws Exception {
            var disk = CrashFileSystem.create();
            // A new store has no segment to write into, so its writer starts preparing the one after its first
            try (var ledger = open(disk.getPath("/store"))) {
                awaitTrue(log(ledger)::nextSettled, "the preparation of the open never settled");
            }
            try (var ledger = open(disk.getPath("/store"))) {
                assertTrue(contents(ledger).isEmpty());
            }
        }

        @Test
        @DisplayName("whose preparations no thread starts rolls over by running each itself, and keeps the last at a close")
        void unstarted() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var held = Collections.synchronizedList(new ArrayList<FutureTask<?>>());
            long keptNumber;
            try (var ledger = open(disk.getPath("/store"))) {
                var log = log(ledger);
                awaitTrue(log::nextSettled, "the preparation of the open never settled");
                log.starterForTests = held::add;
                var commits = 0;
                while (log.currentSegment() < 6) {
                    commit(ledger, model, "w" + commits++);
                }
                assertFalse(held.isEmpty(), "no preparation was held");
                for (var preparation : held.subList(0, held.size() - 1)) {
                    assertTrue(preparation.isDone(), "a rollover passed a preparation it did not run");
                }
                keptNumber = log.currentSegment() + 1;
                ledger.close();
                assertEquals(keptNumber, log.nextOpenSegment());
                assertTrue(held.getLast().isDone(), "the close did not run the last preparation");
            }
            var kept = disk.getPath("/store", "log", LogWriter.segmentName(keptNumber));
            assertEquals(LogWriter.KEPT_LENGTH, Files.size(kept));
            try (var ledger = open(disk.getPath("/store"))) {
                assertEquals(keptNumber, log(ledger).currentSegment());
                assertEquals(model, contents(ledger));
                commit(ledger, model, "after");
            }
            survivesPowerLoss(disk, model);
        }

        @Test
        @DisplayName("closes once the preparation it holds is run by the close and by its late start beside it, keeping the segment")
        void lateStart() throws Exception {
            for (var round = 0; round < 8; round++) {
                var disk = CrashFileSystem.create();
                var model = new TreeMap<String, String>();
                var held = Collections.synchronizedList(new ArrayList<FutureTask<?>>());
                long keptNumber;
                try (var ledger = open(disk.getPath("/store"))) {
                    var log = log(ledger);
                    awaitTrue(log::nextSettled, "the preparation of the open never settled");
                    log.starterForTests = held::add;
                    var commits = 0;
                    var segment = log.currentSegment();
                    while (log.currentSegment() == segment) {
                        commit(ledger, model, "w" + commits++);
                    }
                    keptNumber = log.currentSegment() + 1;
                    var last = held.getLast();
                    var start = new CountDownLatch(1);
                    // The thread the start would have reached, running it as the close does
                    var late = Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                        } catch (InterruptedException _) {
                            return;
                        }
                        last.run();
                    });
                    start.countDown();
                    ledger.close();
                    late.join();
                    assertTrue(last.isDone());
                    assertEquals(keptNumber, log.nextOpenSegment());
                }
                var kept = disk.getPath("/store", "log", LogWriter.segmentName(keptNumber));
                assertEquals(LogWriter.KEPT_LENGTH, Files.size(kept));
                try (var ledger = open(disk.getPath("/store"))) {
                    assertEquals(keptNumber, log(ledger).currentSegment());
                    assertEquals(model, contents(ledger));
                }
                survivesPowerLoss(disk, model);
            }
        }

        @Test
        @DisplayName("of many opened one after another in a process create few preparation threads")
        void reopens() throws Exception {
            var disk = CrashFileSystem.create();
            var model = new TreeMap<String, String>();
            var started = SegmentPreparers.startedForTests();
            for (var open = 0; open < 20; open++) {
                try (var ledger = open(disk.getPath("/store"))) {
                    commit(ledger, model, "o" + open);
                    awaitTrue(log(ledger)::nextSettled, "a preparation never settled");
                }
            }
            assertTrue(SegmentPreparers.startedForTests() - started < 20, "each open started a preparation thread");
            try (var ledger = open(disk.getPath("/store"))) {
                assertEquals(model, contents(ledger));
            }
        }
    }
}
