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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The writer of a checkpoint's units under threads that queue at once: reads that find every unit queued or written,
/// flushes that fail or race a discard, the nonce counter at its end, a parallel tree update with a subtree that fails,
/// and stores that crash or fail in the middle of a checkpoint that flushes while it rewrites its trees.
///
/// The writers below flush from 64 KiB, so a few hundred units make many flushes and reach twice the limit; the stores
/// write checkpoints past the 8 MiB the writer of a store flushes from.
class PageFileWriterTest {
    private static final byte[] KEY = new byte[32];
    private static final int LIMIT = 64 << 10;
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? new SecretKeySpec(new byte[32], "AES") : null);
    }

    private static boolean dataFile(Path path) {
        return path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE);
    }

    /// A unit a test queued and the content it holds.
    private record Queued(PageRef ref, byte[] content) {
    }

    /// Queues a value unit of random content in pages taken from `space`.
    private static Queued queue(PageFile.Writer writer, FreeSpace space, Random random, boolean cached) throws IOException {
        var unitPages = random.nextInt(10) == 0 ? 2 + random.nextInt(3) : 1;
        var page = space.allocate(unitPages);
        var unit = new byte[unitPages * PageFormat.SIZE];
        var length = 1 + random.nextInt(PageFormat.capacity(unitPages));
        var content = new byte[length];
        random.nextBytes(content);
        System.arraycopy(content, 0, unit, PageFormat.HEADER, length);
        return new Queued(writer.write(PageFormat.VALUE, 0, page, unitPages, unit, length, cached), content);
    }

    private static void assertHolds(Queued queued, byte[] unit) {
        assertEquals(queued.content().length, Bytes.i32(unit, PageFormat.LENGTH), "the length of the unit at page " + queued.ref().page());
        assertArrayEquals(queued.content(), Arrays.copyOfRange(unit, PageFormat.HEADER, PageFormat.HEADER + queued.content().length),
                "the content of the unit at page " + queued.ref().page());
    }

    /// {@return the unit `ref` names, read as a checkpoint reads what it wrote: queued, cached or in the file}
    private static byte[] readBack(PageFile pages, PageFile.Writer writer, PageRef ref) {
        var unit = writer.staged(ref);
        return unit != null ? unit : pages.read(ref, PageFormat.VALUE, false);
    }

    /// Waits until every one of `threads` waits, on the flush lock or in the file system.
    private static void awaitWaiting(Thread[] threads) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (Arrays.stream(threads).anyMatch(thread -> thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING)) {
            if (System.nanoTime() > deadline) {
                fail("the threads that queue never all waited");
            }
            Thread.sleep(5);
        }
    }

    @Nested
    @DisplayName("queueing")
    class Queueing {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("threads and fork-join tasks that queue at once read back every unit, queued or written, and the file holds them all")
        void concurrent(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var directory = Files.createDirectories(disk.getPath("/store"));
            try (var pages = PageFile.open(directory, cipher(encrypted), 1 << 20, new HeapPool(4 << 20, false))) {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var writer = pages.writerForTests(42, LIMIT, 0);
                var all = new ConcurrentLinkedQueue<Queued>();
                var failures = new ConcurrentLinkedQueue<Throwable>();
                var start = new CountDownLatch(1);
                var producers = 8;
                Runnable[] work = new Runnable[producers];
                for (var p = 0; p < producers; p++) {
                    var seed = p;
                    work[p] = () -> {
                        try {
                            start.await();
                            var random = new Random(seed);
                            var mine = new ArrayList<Queued>();
                            for (var i = 0; i < 1_500; i++) {
                                mine.add(queue(writer, space, random, random.nextBoolean()));
                                // A checkpoint reads back the nodes it wrote, while other tasks flush them
                                var probe = mine.get(random.nextInt(mine.size()));
                                assertHolds(probe, readBack(pages, writer, probe.ref()));
                            }
                            all.addAll(mine);
                        } catch (Throwable throwable) {
                            failures.add(throwable);
                        }
                    };
                }
                // Half on threads of their own, half in the common pool, whose workers also seal the flushes
                var threads = new ArrayList<Thread>();
                var tasks = new ArrayList<ForkJoinTask<?>>();
                for (var p = 0; p < producers; p++) {
                    if (p % 2 == 0) {
                        threads.add(Thread.ofPlatform().start(work[p]));
                    } else {
                        tasks.add(ForkJoinPool.commonPool().submit(work[p]));
                    }
                }
                start.countDown();
                for (var thread : threads) {
                    thread.join(TimeUnit.MINUTES.toMillis(2));
                    assertFalse(thread.isAlive(), "a thread that queues never ended");
                }
                for (var task : tasks) {
                    task.get(2, TimeUnit.MINUTES);
                }
                assertTrue(failures.isEmpty(), () -> "failed: " + failures.peek());
                writer.flush();
                pages.cache().clearForTests();
                assertEquals(producers * 1_500, all.size());
                for (var queued : all) {
                    assertNull(writer.staged(queued.ref()), "a unit stayed queued after the flush");
                    assertHolds(queued, pages.read(queued.ref(), PageFormat.VALUE, false));
                }
            }
        }

        @Test
        @DisplayName("each run of consecutive pages reaches the file with one write")
        void oneWritePerRun() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = Files.createDirectories(disk.getPath("/store"));
            try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(4 << 20, false))) {
                var writes = new AtomicInteger();
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                        writes.incrementAndGet();
                    }
                });
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var writer = pages.writer(9);
                var random = new Random(3);
                var queued = new ArrayList<Queued>();
                // 128 consecutive pages, then a hole, then 10 more: two runs of up to 64 pages and one of 10
                for (var i = 0; i < 128; i++) {
                    queued.add(queue1(writer, space.allocate(1), random));
                }
                space.allocate(1);
                for (var i = 0; i < 10; i++) {
                    queued.add(queue1(writer, space.allocate(1), random));
                }
                writer.flush();
                assertEquals(3, writes.get());
                pages.cache().clearForTests();
                for (var unit : queued) {
                    assertHolds(unit, pages.read(unit.ref(), PageFormat.VALUE, false));
                }
            }
        }

        private static Queued queue1(PageFile.Writer writer, int page, Random random) throws IOException {
            var unit = new byte[PageFormat.SIZE];
            var content = new byte[100];
            random.nextBytes(content);
            System.arraycopy(content, 0, unit, PageFormat.HEADER, content.length);
            return new Queued(writer.write(PageFormat.VALUE, 0, page, 1, unit, content.length, false), content);
        }

        @Test
        @DisplayName("the nonces of threads that take them at once are unique, and the counter stops before it wraps")
        void nonces() throws Exception {
            try (var pages = PageFile.open(Files.createDirectories(CrashFileSystem.create().getPath("/store")), cipher(true),
                    1 << 20, new HeapPool(1 << 20, false))) {
                var writer = pages.writerForTests(5, LIMIT, -20_000);
                var taken = ConcurrentHashMap.<Integer>newKeySet();
                var duplicates = new AtomicInteger();
                var threads = new Thread[8];
                for (var t = 0; t < threads.length; t++) {
                    threads[t] = Thread.ofPlatform().start(() -> {
                        while (true) {
                            int nonce;
                            try {
                                nonce = writer.nextNonce();
                            } catch (IllegalStateException _) {
                                return;
                            }
                            if (!taken.add(nonce)) {
                                duplicates.incrementAndGet();
                            }
                        }
                    });
                }
                for (var thread : threads) {
                    thread.join();
                }
                assertEquals(0, duplicates.get());
                assertEquals(19_999, taken.size());
                assertFalse(taken.contains(-1), "the last nonce of the counter was given out");
                assertFalse(taken.contains(0), "the counter wrapped");
                assertTrue(taken.contains(-20_000) && taken.contains(-2));
                assertThrows(IllegalStateException.class, writer::nextNonce);
            }
        }
    }

    @Nested
    @DisplayName("failures")
    class Failures {
        @Test
        @DisplayName("a flush that fails ends every thread that queues, those waiting at twice the limit too, and nothing is written after it")
        void failedFlush() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = Files.createDirectories(disk.getPath("/store"));
            try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(4 << 20, false))) {
                var attempts = new AtomicInteger();
                var release = new CountDownLatch(1);
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                        attempts.incrementAndGet();
                        // The first write holds the flush while the other threads queue up to twice the limit
                        try {
                            release.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                        throw new IOException("No space left on device");
                    }
                });
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var writer = pages.writerForTests(11, LIMIT, 0);
                var outcomes = new ConcurrentLinkedQueue<Throwable>();
                var threads = new Thread[6];
                for (var t = 0; t < threads.length; t++) {
                    var seed = t;
                    threads[t] = Thread.ofPlatform().start(() -> {
                        var random = new Random(seed);
                        try {
                            while (true) {
                                queue(writer, space, random, true);
                            }
                        } catch (Throwable throwable) {
                            outcomes.add(throwable);
                        }
                    });
                }
                // One thread waits in the failing write, the others for its flush
                awaitWaiting(threads);
                release.countDown();
                for (var thread : threads) {
                    thread.join(TimeUnit.SECONDS.toMillis(30));
                    assertFalse(thread.isAlive(), "a thread that queued past twice the limit never ended");
                }
                assertEquals(threads.length, outcomes.size());
                for (var outcome : outcomes) {
                    assertInstanceOf(IOException.class, outcome);
                }
                assertEquals(1, attempts.get(), "the writer wrote on after a flush failed");
                assertThrows(IOException.class, writer::flush);
                assertThrows(IOException.class, writer::awaitForce);
                assertThrows(IOException.class, () -> queue(writer, space, new Random(), true));
                writer.discard();
                assertThrows(IllegalStateException.class, () -> queue(writer, space, new Random(), true));
                assertEquals(1, attempts.get());
            }
        }

        @Test
        @DisplayName("a discard waits for the flush that runs, and nothing queued reaches the file after it")
        void discardDuringFlush() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = Files.createDirectories(disk.getPath("/store"));
            try (var pages = PageFile.open(directory, cipher(true), 1 << 20, new HeapPool(4 << 20, false))) {
                var writes = new AtomicInteger();
                var flushing = new CountDownLatch(1);
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                        writes.incrementAndGet();
                        flushing.countDown();
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var writer = pages.writerForTests(13, LIMIT, 0);
                var outcomes = new ConcurrentLinkedQueue<Throwable>();
                var queued = new ConcurrentLinkedQueue<Queued>();
                var threads = new Thread[4];
                for (var t = 0; t < threads.length; t++) {
                    var seed = t;
                    threads[t] = Thread.ofPlatform().start(() -> {
                        var random = new Random(seed);
                        try {
                            while (true) {
                                queued.add(queue(writer, space, random, true));
                            }
                        } catch (Throwable throwable) {
                            outcomes.add(throwable);
                        }
                    });
                }
                assertTrue(flushing.await(30, TimeUnit.SECONDS));
                writer.discard();
                var afterDiscard = writes.get();
                for (var thread : threads) {
                    thread.join(TimeUnit.SECONDS.toMillis(30));
                    assertFalse(thread.isAlive(), "a thread that queued never ended after the discard");
                }
                Thread.sleep(100);
                assertEquals(afterDiscard, writes.get(), "a unit was written after the discard");
                for (var outcome : outcomes) {
                    assertInstanceOf(IllegalStateException.class, outcome);
                }
                assertThrows(IllegalStateException.class, writer::flush);
                assertFalse(queued.isEmpty());
            }
        }

        @Test
        @DisplayName("a parallel update whose first subtree fails returns once the others ended, and every page they took goes back")
        void failedSubtree() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = Files.createDirectories(disk.getPath("/store"));
            var random = new Random(17);
            try (var pages = PageFile.open(directory, cipher(true), 8 << 20, new HeapPool(4 << 20, false))) {
                var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
                var model = new TreeMap<String, byte[]>();
                var ops = new ArrayList<TreeUpdate.Op>();
                for (var i = 0; i < 60_000; i++) {
                    var key = "k%06d".formatted(i);
                    var value = new byte[150];
                    random.nextBytes(value);
                    model.put(key, value);
                    ops.add(new TreeUpdate.Op(segment(key.getBytes()), segment(value)));
                }
                var writer = pages.writerForTests(1, LIMIT, 0);
                var build = new TreeUpdate(pages, writer, space);
                var root = build.apply(null, ops, List.of());
                var deltas = build.finishDeltas(DeltaTable.EMPTY);
                writer.flush();
                assertTrue(TreeNodes.level(pages.read(root.ref(), PageFile.NODE, true)) >= 2, "the root has no subtrees to update in parallel");

                // Slow writes, so the subtrees that do not fail run long after the first one did
                var writes = new AtomicInteger();
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                        writes.incrementAndGet();
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
                var overwrites = new TreeMap<String, byte[]>();
                for (var i = 0; i < 60_000; i += 2) {
                    var value = new byte[150];
                    random.nextBytes(value);
                    overwrites.put("k%06d".formatted(i), value);
                }
                // The first key's value lives in memory freed before the update: the first subtree fails reading it
                var arena = Arena.ofShared();
                var poisoned = arena.allocate(150);
                arena.close();
                var failing = new ArrayList<TreeUpdate.Op>();
                for (var entry : overwrites.entrySet()) {
                    failing.add(new TreeUpdate.Op(segment(entry.getKey().getBytes()),
                            entry.getKey().equals("k000000") ? poisoned : segment(entry.getValue())));
                }
                var failedWriter = pages.writerForTests(2, LIMIT, 0);
                var failed = new TreeUpdate(pages, failedWriter, space);
                assertThrows(IllegalStateException.class, () -> failed.apply(root, failing, List.of()));
                var afterFailure = writes.get();
                var pageCount = space.pageCount();
                Thread.sleep(200);
                assertEquals(afterFailure, writes.get(), "a subtree wrote on after the update failed");
                assertEquals(pageCount, space.pageCount(), "a subtree took pages after the update failed");
                failedWriter.discard();
                for (var unit : failed.allocated()) {
                    space.release(unit.page(), unit.pages());
                }
                verifyPages(pages, space, root, deltas);

                // The same writes without the failing value
                disk.intercept((_, _) -> {
                });
                var retryOps = new ArrayList<TreeUpdate.Op>();
                for (var entry : overwrites.entrySet()) {
                    retryOps.add(new TreeUpdate.Op(segment(entry.getKey().getBytes()), segment(entry.getValue())));
                }
                var retryWriter = pages.writerForTests(3, LIMIT, 0);
                var retry = new TreeUpdate(pages, retryWriter, space);
                var next = retry.apply(root, retryOps, List.of());
                deltas = retry.finishDeltas(deltas);
                retryWriter.flush();
                for (var unit : retry.freed()) {
                    space.release(unit.page(), unit.pages());
                }
                model.putAll(overwrites);
                pages.cache().clearForTests();
                verifyPages(pages, space, next, deltas);
                var cursor = new TreeCursor(pages, next, null, null, false);
                for (var entry : model.entrySet()) {
                    assertTrue(cursor.next(), "the tree ended early");
                    assertEquals(entry.getKey(), new String(cursor.key().toArray(ValueLayout.JAVA_BYTE)));
                    assertArrayEquals(entry.getValue(), cursor.value().toArray(ValueLayout.JAVA_BYTE));
                }
                assertFalse(cursor.next());
            }
        }

        private static MemorySegment segment(byte[] bytes) {
            return MemorySegment.ofArray(bytes.clone()).asReadOnly();
        }

        /// Checks that every page of `space` is used by the tree under `root` or free, not both.
        private static void verifyPages(PageFile pages, FreeSpace space, TreeRoot root, DeltaTable deltas) {
            var used = new BitSet();
            var records = new TreeMap<Integer, Integer>();
            CheckpointTest.walk(pages, root.ref(), used, new BitSet(), records);
            CheckpointTest.verifyDeltas(used, records, deltas);
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < space.pageCount(); page++) {
                var free = space.isUnused(page);
                assertTrue(free != used.get(page), "page " + page + (free ? " is free and in use" : " is neither free nor in use"));
            }
        }
    }

    @Nested
    @DisplayName("stores")
    class Stores {
        private static final int KEYS = 40_000;
        private static final String VALUE = "v".repeat(290);

        private static Ledger open(Path directory, Encryption encryption) {
            return Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).checkpointEvery(1L << 40).build();
        }

        /// Writes a tree of [#KEYS] keys, then overwrites half of them, durably, and closes the store before its
        /// checkpoint takes them: the next open rewrites most leaves with one checkpoint, which flushes as it goes.
        private static Map<String, String> prepare(Path directory, Encryption encryption) throws InterruptedException {
            var model = new TreeMap<String, String>();
            try (var ledger = open(directory, encryption)) {
                for (var batch = 0; batch < KEYS / 1_000; batch++) {
                    var writes = ledger.newBatch();
                    for (var i = batch * 1_000; i < (batch + 1) * 1_000; i++) {
                        var key = "m%06d".formatted(i);
                        writes.put(MESSAGES, key, VALUE + i);
                        model.put(key, VALUE + i);
                    }
                    ledger.commit(writes);
                }
                ledger.checkpoint();
                for (var batch = 0; batch < KEYS / 2_000; batch++) {
                    var writes = ledger.newBatch();
                    for (var i = batch * 2_000; i < (batch + 1) * 2_000; i += 2) {
                        var key = "m%06d".formatted(i);
                        writes.put(MESSAGES, key, VALUE + "x" + i);
                        model.put(key, VALUE + "x" + i);
                    }
                    ledger.commit(writes);
                }
                ledger.awaitDurable();
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            return model;
        }

        private static Map<String, String> contents(Ledger ledger) {
            var contents = new TreeMap<String, String>();
            ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                contents.put(key, value);
                return true;
            });
            return contents;
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a checkpoint whose flush fails while it rewrites its trees fails alone, and the next one holds every commit")
        void failingFlush(boolean encrypted) throws Exception {
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var model = prepare(directory, encryption);
            var writes = new AtomicInteger();
            var failing = new AtomicBoolean(true);
            disk.intercept((path, kind) -> {
                // Past the first flush, while the trees are still being rewritten
                if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path) && failing.get() && writes.incrementAndGet() > 16) {
                    throw new IOException("No space left on device");
                }
            });
            try (var ledger = open(directory, encryption)) {
                assertThrows(LedgerIoException.class, ledger::checkpoint);
                failing.set(false);
                assertEquals(model, contents(ledger));
                ledger.checkpoint();
                assertEquals(model, contents(ledger));
            }
            try (var ledger = open(directory, encryption)) {
                assertEquals(model, contents(ledger));
            }
            CheckpointTest.verifyPages(directory, encrypted);
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5})
        @DisplayName("a crash at any point of a checkpoint that flushes while it rewrites its trees leaves a store that recovers every commit")
        void crashDuringCheckpoint(int seed) throws Exception {
            var encrypted = seed % 2 == 0;
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var setup = CrashFileSystem.create();
            var model = prepare(setup.getPath("/store"), encryption);
            // How many changes the checkpoint makes, about: its runs depend on what each flush finds queued
            var dry = setup.afterProcessCrash();
            long changes;
            try (var ledger = open(dry.getPath("/store"), encryption)) {
                var before = dry.changes();
                ledger.checkpoint();
                changes = dry.changes() - before;
            }
            var random = new Random(seed);
            var disk = setup.afterProcessCrash();
            try (var ledger = open(disk.getPath("/store"), encryption)) {
                disk.stopAfter(random.nextLong(changes + 1));
                ledger.checkpoint();
            } catch (RuntimeException _) {
                // The process stopped where it was told to
            }
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            try (var ledger = open(survivor.getPath("/store"), encryption)) {
                assertEquals(model, contents(ledger));
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery")).awaitDurable();
                ledger.checkpoint();
            }
            try (var ledger = open(survivor.getPath("/store"), encryption)) {
                assertEquals("recovery", ledger.get(MESSAGES, "after").orElseThrow());
                assertEquals(KEYS + 1, ledger.size(MESSAGES));
            }
            CheckpointTest.verifyPages(survivor.getPath("/store"), encrypted);
        }
    }
}
