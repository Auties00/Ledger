package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The writer of a checkpoint's units: batches sealed and written outside its lock while other threads queue and read
/// units, one batch in flight at a time, a failed batch that stays readable and fails every later write, and nonces
/// given without its lock.
class PageWriterTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");
    // The bytes that fill a batch of the writer
    private static final int BATCH = 4 << 20;

    /// A unit queued by a test and the plaintext it must read back as.
    private record Written(PageRef ref, byte[] content) {
    }

    private static Written write(PageFile.Writer writer, FreeSpace space, int unitPages, int length, Random random) throws IOException {
        var content = new byte[length];
        random.nextBytes(content);
        var unit = new byte[PageFormat.HEADER + length];
        System.arraycopy(content, 0, unit, PageFormat.HEADER, length);
        var ref = writer.write(PageFormat.VALUE, 0, space.allocate(unitPages), unitPages, unit, length, false);
        return new Written(ref, content);
    }

    /// A write of the data file the interceptor holds, the first for which `when` holds, until `release` opens.
    private record Gate(BooleanSupplier when, CountDownLatch entered, CountDownLatch release) {
    }

    /// {@return the field `name` of `owner`, read through reflection}
    private static Object field(Object owner, String name) {
        try {
            var field = owner.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(owner);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    /// Checks that `written` reads back, queued or from the file.
    private static void assertReads(PageFile pages, PageFile.Writer writer, Written written) {
        var staged = writer.staged(written.ref());
        var unit = staged != null ? staged : pages.read(written.ref(), PageFormat.VALUE, false);
        assertArrayEquals(written.content(), Arrays.copyOfRange(unit, PageFormat.HEADER, PageFormat.HEADER + written.content().length),
                "page " + written.ref().page());
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(120)
    @DisplayName("units queued by many threads at once, of one page, of a few, past a run and past a window, read back while they are queued, written and after, under unique nonces")
    void concurrentWrites(boolean encrypted, @TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(encrypted ? KEY : null), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(7);
            var threads = 8;
            var all = new ConcurrentLinkedQueue<Written>();
            var failure = new AtomicReference<Throwable>();
            var start = new CountDownLatch(1);
            var workers = new ArrayList<Thread>();
            for (var t = 0; t < threads; t++) {
                var seed = t;
                workers.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    var mine = new ArrayList<Written>();
                    try {
                        start.await();
                        // About 5 MiB of pages a thread: ten batches in all
                        for (var i = 0; i < 800; i++) {
                            var unitPages = i % 200 == 0 ? 70
                                    : seed == 0 && i % 400 == 50 ? 300
                                    : i % 7 == 0 ? 3 : 1;
                            var length = 1 + random.nextInt(PageFormat.capacity(unitPages));
                            var written = write(writer, space, unitPages, length, random);
                            mine.add(written);
                            assertReads(pages, writer, written);
                            if (i % 10 == 0) {
                                assertReads(pages, writer, mine.get(random.nextInt(mine.size())));
                            }
                        }
                        all.addAll(mine);
                    } catch (Throwable throwable) {
                        failure.compareAndSet(null, throwable);
                    }
                }));
            }
            start.countDown();
            for (var worker : workers) {
                worker.join();
            }
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            writer.flush();
            writer.awaitForce();
            var nonces = new HashSet<Integer>();
            for (var written : all) {
                assertTrue(nonces.add(written.ref().nonceLow()), "nonce " + written.ref().nonceLow() + " given twice");
                assertNull(writer.staged(written.ref()), "a flushed unit stayed queued");
                assertReads(pages, writer, written);
            }
            assertEquals(threads * 800, nonces.size());
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("while a batch is written, other threads queue and read its units and the next batch's, and one that queues a second batch behind it waits for it")
    void writesBesideBatchInFlight() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var armed = new AtomicBoolean();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && armed.compareAndSet(true, false)) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException exception) {
                    throw new IOException(exception);
                }
            }
        });
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(3);
            var random = new Random(5);
            var first = new ArrayList<Written>();
            // One page short of a batch
            for (var i = 0; i < BATCH / PageFormat.SIZE - 1; i++) {
                first.add(write(writer, space, 1, 100, random));
            }
            armed.set(true);
            var failure = new AtomicReference<Throwable>();
            var filling = new AtomicReference<Written>();
            var flusher = Thread.ofPlatform().start(() -> {
                try {
                    filling.set(write(writer, space, 1, 100, new Random(6)));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            assertTrue(entered.await(30, TimeUnit.SECONDS), "the batch was never written");
            // The batch is in flight and its write blocked: its units read back, and a second batch queues beside it
            for (var written : first) {
                assertNotNull(writer.staged(written.ref()), "a unit of the batch in flight is not readable");
            }
            var second = new ArrayList<Written>();
            for (var i = 0; i < BATCH / PageFormat.SIZE; i++) {
                second.add(write(writer, space, 1, 100, random));
            }
            for (var written : second) {
                assertReads(pages, writer, written);
            }
            // A second batch is queued behind the one in flight: the next write waits for it
            var waited = new CountDownLatch(1);
            var third = new AtomicReference<Written>();
            var blocked = Thread.ofPlatform().start(() -> {
                try {
                    var random3 = new Random(7);
                    for (var i = 0; i < BATCH / PageFormat.SIZE + 1; i++) {
                        third.set(write(writer, space, 1, 100, random3));
                    }
                    waited.countDown();
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            assertFalse(waited.await(500, TimeUnit.MILLISECONDS), "a write past two batches went on while one was written");
            assertTrue(flusher.isAlive());
            release.countDown();
            flusher.join();
            blocked.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertEquals(0, waited.getCount());
            writer.flush();
            first.add(filling.get());
            for (var written : first) {
                assertNull(writer.staged(written.ref()));
                assertReads(pages, writer, written);
            }
            for (var written : second) {
                assertNull(writer.staged(written.ref()));
                assertReads(pages, writer, written);
            }
            assertReads(pages, writer, third.get());
            assertEquals(3L * BATCH + PageFormat.SIZE, writer.written());
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a batch in flight holds one batch's worth and the unit that passes it, however many queued behind it while the one before was written")
    void batchInFlightIsBounded() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var gate = new AtomicReference<Gate>();
        disk.intercept((path, kind) -> {
            var armed = gate.get();
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && armed != null
                    && armed.when().getAsBoolean() && gate.compareAndSet(armed, null)) {
                armed.entered().countDown();
                try {
                    armed.release().await();
                } catch (InterruptedException exception) {
                    throw new IOException(exception);
                }
            }
        });
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(4);
            var random = new Random(8);
            var all = new ConcurrentLinkedQueue<Written>();
            for (var i = 0; i < BATCH / PageFormat.SIZE - 1; i++) {
                all.add(write(writer, space, 1, 100, random));
            }
            var first = new Gate(() -> true, new CountDownLatch(1), new CountDownLatch(1));
            gate.set(first);
            var failure = new AtomicReference<Throwable>();
            var flusher = Thread.ofPlatform().start(() -> {
                try {
                    all.add(write(writer, space, 1, 100, new Random(9)));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            assertTrue(first.entered().await(30, TimeUnit.SECONDS), "the first batch was never written");
            var firstBatch = field(writer, "inFlight");
            // Two batches' worth queued behind the first, the last write of them waiting for it
            for (var i = 0; i < 2 * BATCH / PageFormat.SIZE - 1; i++) {
                all.add(write(writer, space, 1, 100, random));
            }
            var large = 70;
            var waiter = Thread.ofPlatform().start(() -> {
                try {
                    all.add(write(writer, space, large, PageFormat.capacity(large), new Random(10)));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (waiter.getState() != Thread.State.WAITING) {
                assertTrue(waiter.isAlive() && System.nanoTime() < deadline, "the write past two batches did not wait");
                Thread.onSpinWait();
            }
            var second = new Gate(() -> field(writer, "inFlight") != firstBatch, new CountDownLatch(1), new CountDownLatch(1));
            gate.set(second);
            first.release().countDown();
            assertTrue(second.entered().await(30, TimeUnit.SECONDS), "the second batch was never written");
            var inFlight = (long) field(writer, "inFlightBytes");
            assertTrue(inFlight >= BATCH && inFlight <= BATCH + (long) large * PageFormat.SIZE,
                    "a batch of " + inFlight + " bytes is in flight");
            // What the second batch left queued is under two batches' worth: the waiting write goes on
            waiter.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(waiter.isAlive(), "the write past two batches still waits");
            second.release().countDown();
            flusher.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            writer.flush();
            for (var written : all) {
                assertNull(writer.staged(written.ref()));
                assertReads(pages, writer, written);
            }
            assertEquals(3L * BATCH + (large - 1L) * PageFormat.SIZE, writer.written());
        }
    }

    @ParameterizedTest(name = "then flushed: {0}")
    @ValueSource(booleans = {true, false})
    @Timeout(120)
    @DisplayName("a write whose cache update fails leaves the batch it fills queued, so a flush writes it and a discard returns")
    void cacheFailureLeavesNoBatchInFlight(boolean flushed, @TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(6);
            var random = new Random(12);
            var written = new ArrayList<Written>();
            for (var i = 0; i < BATCH / PageFormat.SIZE - 1; i++) {
                written.add(write(writer, space, 1, 100, random));
            }
            // A cache with no table fails every update, as a rebuild of its table that runs out of memory would
            var cache = field(pages, "cache");
            var table = PageCache.class.getDeclaredField("table");
            table.setAccessible(true);
            var held = table.get(cache);
            table.set(cache, null);
            try {
                assertThrows(NullPointerException.class, () -> write(writer, space, 1, 100, random));
            } finally {
                table.set(cache, held);
            }
            var failure = new AtomicReference<Throwable>();
            var ending = Thread.ofPlatform().start(() -> {
                try {
                    if (flushed) {
                        writer.flush();
                    } else {
                        writer.discard();
                    }
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            ending.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(ending.isAlive(), flushed ? "the flush waits for a batch no one writes" : "the discard waits for a batch no one writes");
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            if (flushed) {
                for (var unit : written) {
                    assertNull(writer.staged(unit.ref()));
                    assertReads(pages, writer, unit);
                }
                assertEquals(BATCH, writer.written());
            }
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a batch whose write fails stays readable, fails every later write and flush with its cause, and goes with a discard")
    void failedBatchStaysReadable() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var failing = new AtomicBoolean();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && failing.get()) {
                throw new IOException("No space left on device");
            }
        });
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(9);
            var random = new Random(11);
            var written = new ArrayList<Written>();
            // A first batch that is written, then one that fails
            for (var i = 0; i < BATCH / PageFormat.SIZE; i++) {
                written.add(write(writer, space, 1, 200, random));
            }
            failing.set(true);
            for (var i = 0; i < BATCH / PageFormat.SIZE - 1; i++) {
                written.add(write(writer, space, 1, 200, random));
            }
            var cause = assertThrows(IOException.class, () -> write(writer, space, 1, 200, random));
            assertEquals("No space left on device", cause.getMessage());
            failing.set(false);
            for (var unit : written) {
                assertReads(pages, writer, unit);
            }
            var later = assertThrows(IOException.class, () -> write(writer, space, 1, 200, random));
            assertSame(cause, later.getCause());
            var flushed = assertThrows(IOException.class, writer::flush);
            assertSame(cause, flushed.getCause());
            writer.discard();
            for (var unit : written.subList(BATCH / PageFormat.SIZE, written.size())) {
                assertNull(writer.staged(unit.ref()), "a discarded unit is still queued");
            }
        }
    }

    @Test
    @DisplayName("a writer refuses units once its checkpoint discarded it")
    void discardedWriterRefuses(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1);
            var random = new Random(1);
            var queued = write(writer, space, 1, 10, random);
            writer.discard();
            assertNull(writer.staged(queued.ref()));
            assertThrows(IllegalStateException.class, () -> write(writer, space, 1, 10, random));
        }
    }

    @Test
    @DisplayName("a page written twice fails, whether the first unit is queued or in flight")
    void pageWrittenTwice(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var writer = pages.writer(1);
            var unit = new byte[PageFormat.HEADER + 10];
            writer.write(PageFormat.VALUE, 0, 10, 1, unit, 10, false);
            assertThrows(IllegalStateException.class, () -> writer.write(PageFormat.VALUE, 0, 10, 1, new byte[PageFormat.HEADER + 10], 10, false));
            writer.flush();
            // Written already: the writer keeps no record of the page
            writer.write(PageFormat.VALUE, 0, 10, 1, new byte[PageFormat.HEADER + 10], 10, false);
        }
    }

    @Test
    @DisplayName("a unit of another checkpoint is never queued, and a reference that differs only in its nonce reads nothing")
    void stagedNamesItsCheckpoint(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var writer = pages.writer(42);
            var unit = new byte[PageFormat.HEADER + 10];
            var ref = writer.write(PageFormat.VALUE, 0, 10, 1, unit, 10, false);
            assertSame(unit, writer.staged(ref));
            assertNull(writer.staged(new PageRef(10, 1, 41, ref.nonceLow())));
            assertNull(writer.staged(new PageRef(10, 1, 42, ref.nonceLow() + 1)));
        }
    }

    @Test
    @Timeout(60)
    @DisplayName("nonces given on many threads at once are unique, and the last one is followed by a failure every time")
    void nonces(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var writer = pages.writer(1);
            var given = new ConcurrentLinkedQueue<Integer>();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 8; t++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    for (var i = 0; i < 50_000; i++) {
                        given.add(writer.nextNonce());
                    }
                }));
            }
            for (var thread : threads) {
                thread.join();
            }
            assertEquals(400_000, new HashSet<>(given).size());
            assertEquals(400_000, writer.nextNonce());
            // Three nonces before the last of the 2^32
            var field = PageFile.Writer.class.getDeclaredField("nonceLow");
            field.setAccessible(true);
            ((AtomicInteger) field.get(writer)).set(-3);
            assertEquals(-3, writer.nextNonce());
            assertEquals(-2, writer.nextNonce());
            assertThrows(IllegalStateException.class, writer::nextNonce);
            assertThrows(IllegalStateException.class, writer::nextNonce);
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a force beside the checkpoint that fails fails the checkpoint's writer, however the batches end")
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = "the writer forces beside a checkpoint only off Windows")
    void failedForceBeside() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var failing = new AtomicBoolean();
        var forces = new AtomicInteger();
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.FORCE && path.endsWith(PageFile.FILE) && failing.get()) {
                forces.incrementAndGet();
                throw new IOException("The device failed");
            }
        });
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(16 << 20))) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var writer = pages.writer(1, () -> true);
            var random = new Random(3);
            failing.set(true);
            List<Throwable> failures = new ArrayList<>();
            // Past the 32 MiB from which a checkpoint that lags forces beside its work, and well past its end
            for (var i = 0; i < 16_000; i++) {
                try {
                    write(writer, space, 1, 10, random);
                } catch (IOException exception) {
                    failures.add(exception);
                }
            }
            try {
                writer.flush();
            } catch (IOException exception) {
                failures.add(exception);
            }
            try {
                writer.awaitForce();
            } catch (IOException exception) {
                failures.add(exception);
            }
            assertTrue(forces.get() > 0, "no force ran beside the checkpoint");
            assertFalse(failures.isEmpty(), "the failed force was dropped");
            writer.discard();
        }
    }
}
