package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.ChangeInterceptor.ChangeKind;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Checkpoints that build while the one before them is made durable ([Checkpointer.Overlap#ALWAYS]), on the file
/// system in memory, whose forces of the data file the tests slow, hold or fail on the checkpoints' own threads so the
/// two stages overlap: what reads see, what reopening and recovery find, the pages and units the checkpoints use, and
/// what a failure of either stage leaves.
class OverlappedCheckpointTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, CHATS);
    private static final ValueCodec<Long> LONGS = ValueCodec.ofTrusted(_ -> Long.BYTES,
            (value, target) -> target.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value),
            source -> source.get(ValueLayout.JAVA_LONG_UNALIGNED, 0));
    private static final long INTERVAL = 1 << 20;
    private static final long TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);

    private static LedgerBuilder options(Path directory, Checkpointer.Overlap overlap, long largeValues,
                                         Consumer<? super LedgerException> failures, Keyspace<?, ?>... keyspaces) {
        var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).checkpointEvery(INTERVAL)
                .largeValues(largeValues).onBackgroundFailure(failures);
        for (var keyspace : keyspaces.length == 0 ? KEYSPACES.toArray(Keyspace<?, ?>[]::new) : keyspaces) {
            options.keyspace(keyspace);
        }
        return ((LedgerBuilder) options).checkpointOverlap(overlap).segmentSize(256 << 10);
    }

    private static LiveLedger open(Path directory, Checkpointer.Overlap overlap, Consumer<? super LedgerException> failures) {
        return (LiveLedger) options(directory, overlap, 4_096, failures).build();
    }

    private static boolean dataFile(Path path) {
        return path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE);
    }

    /// {@return whether the caller is a thread a checkpoint the store started runs on}
    private static boolean checkpointThread() {
        return Thread.currentThread().getName().equals("ledger-checkpoint");
    }

    /// {@return an interceptor that makes every force of the data file on a checkpoint's own thread take `millis`}
    private static ChangeInterceptor slowForces(long millis) {
        return (path, kind) -> {
            if (kind == ChangeKind.FORCE && dataFile(path) && checkpointThread()) {
                sleep(millis);
            }
        };
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    /// Waits for `latch` on a thread of the store, failing the change it intercepts if it waits too long.
    private static void await(CountDownLatch latch) throws IOException {
        try {
            if (!latch.await(TIMEOUT_NANOS, TimeUnit.NANOSECONDS)) {
                throw new IOException("The test's choreography timed out");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
        }
    }

    /// Waits on the test's thread until `condition` holds.
    private static void await(BooleanSupplier condition, String what) {
        var deadline = System.nanoTime() + TIMEOUT_NANOS;
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("Timed out waiting for " + what);
            }
            sleep(1);
        }
    }

    private static Map<String, TreeMap<String, String>> emptyModel() {
        var model = new TreeMap<String, TreeMap<String, String>>();
        for (var keyspace : KEYSPACES) {
            model.put(keyspace.name(), new TreeMap<>());
        }
        return model;
    }

    private static Map<String, TreeMap<String, String>> contents(View view) {
        var contents = new TreeMap<String, TreeMap<String, String>>();
        for (var keyspace : KEYSPACES) {
            var entries = new TreeMap<String, String>();
            view.forEach(keyspace, KeyRange.all(), (key, value) -> {
                entries.put(key, value);
                return true;
            });
            contents.put(keyspace.name(), entries);
        }
        return contents;
    }

    private static void verify(View view, Map<String, TreeMap<String, String>> model) {
        var contents = contents(view);
        for (var keyspace : KEYSPACES) {
            var expected = model.get(keyspace.name());
            var actual = contents.get(keyspace.name());
            if (!expected.equals(actual)) {
                fail(keyspace.name() + " holds " + actual.size() + " entries where the model holds " + expected.size()
                        + "; first difference at " + firstDifference(expected, actual));
            }
            assertEquals(expected.size(), view.size(keyspace), keyspace.name() + " count");
        }
    }

    private static String firstDifference(TreeMap<String, String> expected, TreeMap<String, String> actual) {
        var keys = new TreeMap<String, Boolean>();
        expected.keySet().forEach(key -> keys.put(key, true));
        actual.keySet().forEach(key -> keys.put(key, true));
        for (var key : keys.keySet()) {
            var left = expected.get(key);
            var right = actual.get(key);
            if (left == null ? right != null : !left.equals(right)) {
                return key + " (expected " + (left == null ? "none" : left.length() + " chars") + ", found "
                        + (right == null ? "none" : right.length() + " chars") + ")";
            }
        }
        return "none";
    }

    private static void put(Ledger ledger, Map<String, TreeMap<String, String>> model, Keyspace<String, String> keyspace,
                            String key, String value) {
        ledger.commit(ledger.newBatch().put(keyspace, key, value));
        model.get(keyspace.name()).put(key, value);
    }

    /// Commits messages under `prefix` until the log has grown by `bytes`.
    private static void fill(LiveLedger ledger, Map<String, TreeMap<String, String>> model, String prefix, long bytes) {
        var log = ledger.log();
        var start = log.appendedBytes();
        var value = "f".repeat(2_000);
        for (var i = 0; log.appendedBytes() - start < bytes; i++) {
            put(ledger, model, MESSAGES, prefix + "%06d".formatted(i), value);
        }
    }

    /// Commits a batch of one to four random writes to `model` and `ledger`, over `keys` keys of each key space.
    private static void randomBatch(Ledger ledger, Map<String, TreeMap<String, String>> model, Random random, int keys,
                                    int smallest, int largest, int large) {
        var batch = ledger.newBatch();
        var writes = 1 + random.nextInt(4);
        for (var i = 0; i < writes; i++) {
            var keyspace = random.nextInt(4) == 0 ? CHATS : MESSAGES;
            var entries = model.get(keyspace.name());
            var key = "k%05d".formatted(random.nextInt(keys));
            var kind = random.nextInt(60);
            if (kind == 0) {
                var to = "k%05d".formatted(random.nextInt(keys));
                batch.deleteRange(keyspace, KeyRange.between(key, to));
                if (key.compareTo(to) < 0) {
                    entries.subMap(key, to).clear();
                }
            } else if (kind < 9) {
                batch.delete(keyspace, key);
                entries.remove(key);
            } else {
                var length = random.nextInt(8) == 0 ? large + random.nextInt(large) : smallest + random.nextInt(largest - smallest);
                var value = Character.toString('a' + random.nextInt(26)).repeat(length);
                batch.put(keyspace, key, value);
                entries.put(key, value);
            }
        }
        ledger.commit(batch);
    }

    @Nested
    @DisplayName("reads")
    class Reads {
        @ParameterizedTest(name = "seed {0}")
        @ValueSource(longs = {1, 2, 3})
        @DisplayName("match a model while checkpoints build beside the forces of the ones before, and after reopening")
        void model(long seed) throws Exception {
            var disk = CrashFileSystem.create();
            disk.intercept(slowForces(10));
            var directory = disk.getPath("/store");
            var random = new Random(seed);
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            try (var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add)) {
                for (var step = 0; step < 4_000; step++) {
                    randomBatch(ledger, model, random, 2_000, 200, 2_500, 5_000);
                    if (step % 500 == 499) {
                        try (var snapshot = ledger.snapshot()) {
                            verify(snapshot, model);
                        }
                    }
                }
                assertTrue(ledger.checkpointer().overlappedForTests() > 0, "no checkpoint built beside the force of another");
                verify(ledger, model);
            }
            assertTrue(failures.isEmpty(), () -> "reported: " + failures.peek());
            try (var ledger = open(directory, Checkpointer.Overlap.NEVER, failures::add)) {
                verify(ledger, model);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("see consistent totals and exact counts while writers race and checkpoints build beside the forces of the ones before")
        void racingWriters() throws Exception {
            var accounts = Keyspace.paged("accounts", KeyCodec.int64(), LONGS);
            var disk = CrashFileSystem.create();
            disk.intercept(slowForces(15));
            var directory = disk.getPath("/store");
            var count = 256;
            var expected = count * 1_000L;
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            try (var ledger = (LiveLedger) options(directory, Checkpointer.Overlap.ALWAYS, 4_096, failures::add, accounts, MESSAGES).build()) {
                var initial = ledger.newBatch();
                for (var account = 0; account < count; account++) {
                    initial.put(accounts, (long) account, 1_000L);
                }
                ledger.commit(initial);
                var stop = new AtomicBoolean();
                var inconsistent = new ConcurrentLinkedQueue<String>();
                var transfers = new AtomicLong();
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < 3; writer++) {
                        executor.execute(() -> {
                            var random = ThreadLocalRandom.current();
                            while (!stop.get()) {
                                var from = (long) random.nextInt(count);
                                var to = (long) random.nextInt(count);
                                var amount = (long) random.nextInt(1, 10);
                                ledger.update(transaction -> {
                                    transaction.put(accounts, from, transaction.get(accounts, from).orElseThrow() - amount);
                                    transaction.put(accounts, to, transaction.get(accounts, to).orElseThrow() + amount);
                                });
                                transfers.incrementAndGet();
                            }
                        });
                    }
                    // New keys, written blind, and range removals of the oldest, so the counts depend on the trees
                    executor.execute(() -> {
                        var random = ThreadLocalRandom.current();
                        var value = "m".repeat(1_500);
                        for (var next = 0L; !stop.get(); next++) {
                            var batch = ledger.newBatch();
                            for (var i = 0; i < 4; i++) {
                                batch.put(MESSAGES, "m%09d".formatted(random.nextLong(next + 1) * 4 + i), value);
                            }
                            if (next % 200 == 199) {
                                batch.deleteRange(MESSAGES, KeyRange.between("m%09d".formatted(random.nextLong(next) * 4),
                                        "m%09d".formatted(random.nextLong(next) * 4 + 40)));
                            }
                            ledger.commit(batch);
                        }
                    });
                    for (var reader = 0; reader < 2; reader++) {
                        executor.execute(() -> {
                            while (!stop.get()) {
                                try (var snapshot = ledger.snapshot()) {
                                    var total = new long[1];
                                    snapshot.forEach(accounts, KeyRange.all(), (_, value) -> {
                                        total[0] += value;
                                        return true;
                                    });
                                    if (total[0] != expected) {
                                        inconsistent.add("total " + total[0]);
                                    }
                                    var messages = new long[1];
                                    snapshot.forEach(MESSAGES, KeyRange.all(), (_, _) -> {
                                        messages[0]++;
                                        return true;
                                    });
                                    var counted = snapshot.size(MESSAGES);
                                    if (counted != messages[0]) {
                                        inconsistent.add("count " + counted + " of " + messages[0] + " messages");
                                    }
                                }
                            }
                        });
                    }
                    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4);
                    while (System.nanoTime() < deadline && inconsistent.isEmpty()) {
                        sleep(20);
                    }
                    stop.set(true);
                }
                assertTrue(inconsistent.isEmpty(), () -> inconsistent.size() + " inconsistent reads, first: " + inconsistent.peek());
                assertTrue(transfers.get() > 100, () -> transfers + " transfers");
                assertTrue(ledger.checkpointer().overlappedForTests() > 0, "no checkpoint built beside the force of another");
            }
            assertTrue(failures.isEmpty(), () -> "reported: " + failures.peek());
            try (var ledger = options(directory, Checkpointer.Overlap.NEVER, 4_096, failures::add, accounts, MESSAGES).build()) {
                var total = new long[1];
                ledger.forEach(accounts, KeyRange.all(), (_, value) -> {
                    total[0] += value;
                    return true;
                });
                assertEquals(expected, total[0]);
                var messages = new long[1];
                ledger.forEach(MESSAGES, KeyRange.all(), (_, _) -> {
                    messages[0]++;
                    return true;
                });
                assertEquals(messages[0], ledger.size(MESSAGES));
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("large values")
    class LargeValues {
        /// Holds the forces of the data file on the checkpoints' own threads while it is held, each until a permit lets
        /// it go: a checkpoint forces the data file twice, before its superblock and with it.
        private static final class ForceGate implements ChangeInterceptor {
            private final Semaphore permits = new Semaphore(0);
            private final AtomicInteger waiting = new AtomicInteger();
            private volatile boolean holding;

            @Override
            public void beforeChange(Path path, ChangeKind kind) {
                if (holding && kind == ChangeKind.FORCE && dataFile(path) && checkpointThread()) {
                    waiting.incrementAndGet();
                    try {
                        permits.acquireUninterruptibly();
                    } finally {
                        waiting.decrementAndGet();
                    }
                }
            }
        }

        @Test
        @DisplayName("a checkpoint built beside the one before frees none of the units that one's publication frees or takes, which commits reuse after")
        void unitsOfTheCheckpointBefore() throws Exception {
            var disk = CrashFileSystem.create();
            var gate = new ForceGate();
            disk.intercept(gate);
            var directory = disk.getPath("/store");
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            try (var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add)) {
                var checkpointer = ledger.checkpointer();
                // The first unit, superseded before the first checkpoint, and the second, which that one takes
                put(ledger, model, MESSAGES, "x", "a".repeat(6_000));
                put(ledger, model, MESSAGES, "x", "b".repeat(6_000));
                gate.holding = true;
                fill(ledger, model, "f", INTERVAL);
                await(() -> gate.waiting.get() == 1, "the first checkpoint to force");
                // Replaces the unit the first checkpoint takes, before the second one's cut
                put(ledger, model, MESSAGES, "x", "c".repeat(6_000));
                fill(ledger, model, "g", INTERVAL + (64 << 10));
                await(() -> checkpointer.queuedForTests() == 2, "the second checkpoint to build beside the first");
                gate.permits.release(2);
                await(() -> ledger.tree() != null && ledger.tree().number == 1 && gate.waiting.get() == 1,
                        "the first checkpoint to publish while the second forces");
                // Takes the pages of the first unit, which the first checkpoint's publication freed
                put(ledger, model, MESSAGES, "z", "z".repeat(6_000));
                gate.permits.release(2);
                await(() -> ledger.tree().number == 2 && checkpointer.queuedForTests() == 0, "the second checkpoint to publish");
                gate.holding = false;
                gate.permits.release(16);
                for (var i = 0; i < 8; i++) {
                    put(ledger, model, MESSAGES, "w" + i, Character.toString('0' + i).repeat(6_000));
                    fill(ledger, model, "h" + i, 128 << 10);
                }
                verify(ledger, model);
                ledger.checkpoint();
                verify(ledger, model);
            }
            assertTrue(failures.isEmpty(), () -> "reported: " + failures.peek());
            try (var ledger = open(directory, Checkpointer.Overlap.NEVER, failures::add)) {
                verify(ledger, model);
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("failures")
    class Failures {
        /// Commits messages until `done` holds, at most 64 MiB of them.
        private static void commitUntil(LiveLedger ledger, Map<String, TreeMap<String, String>> model, BooleanSupplier done) {
            var deadline = System.nanoTime() + TIMEOUT_NANOS;
            for (var round = 0; !done.getAsBoolean() && round < 1_024; round++) {
                if (System.nanoTime() > deadline) {
                    fail("Timed out committing");
                }
                fill(ledger, model, "m%04d-".formatted(round), 64 << 10);
            }
            assertTrue(done.getAsBoolean(), "the choreography never happened");
        }

        /// Checks that the store still takes commits and checkpoints after a failure, and that it reopens whole.
        private static void goesOn(LiveLedger ledger, Path directory, Map<String, TreeMap<String, String>> model) throws IOException {
            fill(ledger, model, "after", 3 * INTERVAL);
            ledger.checkpoint();
            verify(ledger, model);
            ledger.close();
            try (var reopened = open(directory, Checkpointer.Overlap.NEVER, _ -> {
            })) {
                verify(reopened, model);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("a checkpoint whose force fails gives up the one building on its trees, and keeps its own pages until that one stops")
        void forceFailsUnderBuild() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var forcing = new AtomicReference<Thread>();
            var written = new CountDownLatch(1);
            var failed = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (!dataFile(path) || !checkpointThread()) {
                    return;
                }
                if (kind == ChangeKind.FORCE && armed.compareAndSet(true, false)) {
                    forcing.set(Thread.currentThread());
                    // The next checkpoint builds on this one's trees meanwhile, until it writes its units
                    await(written);
                    failed.countDown();
                    throw new IOException("injected failure of a checkpoint's force");
                }
                if (kind == ChangeKind.WRITE && forcing.get() != null && forcing.get() != Thread.currentThread()
                        && written.getCount() > 0) {
                    written.countDown();
                    await(failed);
                    // The failed checkpoint waits for this build before its pages go back
                    sleep(100);
                }
            });
            var directory = disk.getPath("/store");
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add);
            armed.set(true);
            commitUntil(ledger, model, () -> !failures.isEmpty());
            assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures.peek());
            assertTrue(ledger.checkpointer().overlappedForTests() > 0, "no checkpoint built beside the failed one");
            goesOn(ledger, directory, model);
            // The build given up with the failed checkpoint is no failure of its own
            assertEquals(1, failures.size(), () -> "reported: " + failures);
        }

        @Test
        @DisplayName("a checkpoint whose force fails gives up the one queued behind it, built on its trees")
        void forceFailsBeforeQueued() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var store = new AtomicReference<LiveLedger>();
            disk.intercept((path, kind) -> {
                if (kind == ChangeKind.FORCE && dataFile(path) && checkpointThread() && armed.compareAndSet(true, false)) {
                    var deadline = System.nanoTime() + TIMEOUT_NANOS;
                    while (store.get().checkpointer().queuedForTests() < 2 && System.nanoTime() < deadline) {
                        sleep(1);
                    }
                    throw new IOException("injected failure of a checkpoint's force");
                }
            });
            var directory = disk.getPath("/store");
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add);
            store.set(ledger);
            armed.set(true);
            commitUntil(ledger, model, () -> !failures.isEmpty());
            assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures.peek());
            assertTrue(ledger.checkpointer().overlappedForTests() > 0, "no checkpoint built beside the failed one");
            goesOn(ledger, directory, model);
            assertEquals(1, failures.size(), () -> "reported: " + failures);
        }

        @Test
        @DisplayName("a build that fails beside the force of the checkpoint before gives its own pages back, and that one publishes")
        void buildFailsBesideForce() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var forcing = new AtomicReference<Thread>();
            var writeFailed = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (!dataFile(path) || !checkpointThread()) {
                    return;
                }
                if (kind == ChangeKind.FORCE && armed.compareAndSet(true, false)) {
                    forcing.set(Thread.currentThread());
                    await(writeFailed);
                    return;
                }
                if (kind == ChangeKind.WRITE && forcing.get() != null && forcing.get() != Thread.currentThread()
                        && writeFailed.getCount() > 0) {
                    writeFailed.countDown();
                    throw new IOException("injected failure of a build's write");
                }
            });
            var directory = disk.getPath("/store");
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add);
            armed.set(true);
            commitUntil(ledger, model, () -> !failures.isEmpty() && ledger.tree() != null);
            assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures.peek());
            assertTrue(ledger.tree().number >= 1, "the checkpoint the failed build updated did not publish");
            goesOn(ledger, directory, model);
            assertEquals(1, failures.size(), () -> "reported: " + failures);
        }

        @Test
        @DisplayName("a checkpoint whose superblock fails stops the checkpoints, the one building on it included, and the store reopens whole")
        void superblockFailsUnderBuild() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var forcing = new AtomicReference<Thread>();
            var forced = new AtomicBoolean();
            var written = new CountDownLatch(1);
            var failed = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (!dataFile(path) || !checkpointThread()) {
                    return;
                }
                var current = Thread.currentThread();
                if (kind == ChangeKind.FORCE && armed.compareAndSet(true, false)) {
                    forcing.set(current);
                    await(written);
                    forced.set(true);
                    return;
                }
                if (kind == ChangeKind.WRITE && current == forcing.get() && forced.compareAndSet(true, false)) {
                    failed.countDown();
                    throw new IOException("injected failure of a superblock");
                }
                if (kind == ChangeKind.WRITE && forcing.get() != null && current != forcing.get() && written.getCount() > 0) {
                    written.countDown();
                    await(failed);
                    sleep(100);
                }
            });
            var directory = disk.getPath("/store");
            var model = emptyModel();
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var ledger = open(directory, Checkpointer.Overlap.ALWAYS, failures::add);
            armed.set(true);
            commitUntil(ledger, model, () -> !failures.isEmpty());
            await(() -> ledger.checkpointer().queuedForTests() == 0, "the failure to settle");
            assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures.peek());
            assertTrue(ledger.checkpointer().overlappedForTests() > 0, "no checkpoint built beside the failed one");
            assertThrows(LedgerIoException.class, ledger::checkpoint);
            fill(ledger, model, "after", INTERVAL);
            ledger.awaitDurable();
            assertThrows(LedgerIoException.class, ledger::close);
            try (var reopened = open(directory, Checkpointer.Overlap.NEVER, _ -> {
            })) {
                verify(reopened, model);
                reopened.checkpoint();
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("crashes")
    class Crashes {
        static Stream<Arguments> cases() {
            return IntStream.range(0, 40).mapToObj(seed -> Arguments.of(seed, seed % 3 == 0 ? "process crash" : "power loss"));
        }

        @ParameterizedTest(name = "seed {0}, {1}")
        @MethodSource("cases")
        @DisplayName("recover a state between the last durable commit and the last visible one, wherever the process stops among checkpoints that overlap")
        void recovers(int seed, String failure) throws Exception {
            var dry = CrashFileSystem.create();
            dry.intercept(slowForces(2));
            new Workload(seed).run(dry.getPath("/store"));
            var disk = CrashFileSystem.create();
            disk.intercept(slowForces(2));
            var random = new Random(seed * 7919L + 3);
            if (random.nextBoolean()) {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            } else {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            }
            var workload = new Workload(seed);
            workload.run(disk.getPath("/store"));
            var survivor = failure.equals("process crash") ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            var directory = survivor.getPath("/store");
            try (var ledger = open(directory, Checkpointer.Overlap.ALWAYS, _ -> {
            })) {
                var recovered = contents(ledger);
                var match = -1;
                for (var commit = workload.durable; commit <= workload.visible; commit++) {
                    if (workload.history.get(commit).equals(recovered)) {
                        match = commit;
                        break;
                    }
                }
                if (match < 0) {
                    fail("The store recovered a state no commit from " + workload.durable + " to " + workload.visible
                            + " left (of " + (workload.history.size() - 1) + ")");
                }
                for (var keyspace : KEYSPACES) {
                    assertEquals(recovered.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " count");
                }
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery").put(MESSAGES, "large", "v".repeat(10_000)))
                        .awaitDurable();
                ledger.checkpoint();
            }
            try (var ledger = open(directory, Checkpointer.Overlap.NEVER, _ -> {
            })) {
                assertEquals("recovery", ledger.get(MESSAGES, "after").orElseThrow());
                assertEquals(10_000, ledger.get(MESSAGES, "large").orElseThrow().length());
            }
            CheckpointTest.verifyPages(directory, true);
        }

        /// A workload drawn from a seed, whose checkpoints the store starts on its own every interval of log, and what
        /// it learned while it ran: the state after each commit, the last commit it made visible and the last one it
        /// knows durable.
        private static final class Workload {
            private final Random random;
            final List<Map<String, TreeMap<String, String>>> history;
            final Map<String, TreeMap<String, String>> state;
            int visible;
            int durable;

            Workload(int seed) {
                this.random = new Random(seed);
                this.history = new ArrayList<>();
                this.state = emptyModel();
                history.add(copy(state));
            }

            /// Runs until it ends or the store fails, which a stopped file system makes it do.
            void run(Path directory) {
                Ledger ledger = null;
                try {
                    ledger = options(directory, Checkpointer.Overlap.ALWAYS, 16_384, _ -> {
                    }).build();
                    for (var step = 0; step < 220; step++) {
                        var next = copy(state);
                        randomBatch(ledger, next, random, 150, 6_000, 16_000, 16_384);
                        commit(next);
                        if (random.nextInt(4) == 0) {
                            ledger.awaitDurable();
                            durable = visible;
                        }
                    }
                    ledger.close();
                    durable = visible;
                } catch (RuntimeException | InterruptedException failure) {
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (RuntimeException _) {
                            // The file system stopped: closing only lets go of the threads
                        }
                    }
                }
            }

            private void commit(Map<String, TreeMap<String, String>> next) {
                for (var entry : next.entrySet()) {
                    state.put(entry.getKey(), entry.getValue());
                }
                visible++;
                history.add(copy(state));
            }

            private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
                var copy = new TreeMap<String, TreeMap<String, String>>();
                state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
                return copy;
            }
        }
    }
}
