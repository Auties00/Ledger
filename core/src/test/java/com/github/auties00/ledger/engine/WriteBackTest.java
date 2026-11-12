package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/// The early writeback of the file of large values. Most tests replace the kernel's calls with a recorder of the
/// ranges passed, so they run on every platform; one checks that the real calls are taken on Linux, where CI runs.
class WriteBackTest {
    private static final Keyspace<String, byte[]> FILES = Keyspace.paged("files", KeyCodec.utf8(), ValueCodec.bytes());
    private static final long FIRST = PageFormat.offset(PageFormat.FIRST_UNIT_PAGE);

    @AfterEach
    void restore() {
        WriteBack.callsForTests = null;
    }

    private static Ledger open(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(new byte[32]) : Encryption.none())
                .keyspace(FILES).largeValues(4_096).build();
    }

    private static byte[] value(int seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out");
            Thread.sleep(1);
        }
    }

    /// Calls that record the ranges passed and can fail or hold them.
    static final class Recorder implements WriteBack.Calls {
        private static final int FD = 42;
        final long pageSize;
        final List<long[]> ranges = new ArrayList<>();
        volatile int openResult = FD;
        volatile int syncResult = 0;
        volatile CountDownLatch hold;
        // The time each call spins for, as a call the kernel makes wait would
        volatile long spinNanos;
        final CountDownLatch entered = new CountDownLatch(1);
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final AtomicInteger afterClose = new AtomicInteger();
        volatile boolean closed;

        Recorder(long pageSize) {
            this.pageSize = pageSize;
        }

        @Override
        public int open(Path path) {
            opens.incrementAndGet();
            return openResult;
        }

        @Override
        public long pageSize() {
            return pageSize;
        }

        @Override
        public int syncRange(int fd, long offset, long length) throws InterruptedException {
            assertEquals(FD, fd);
            if (closed) {
                afterClose.incrementAndGet();
            }
            entered.countDown();
            var spin = spinNanos;
            if (spin > 0) {
                var until = System.nanoTime() + spin;
                while (System.nanoTime() < until) {
                    Thread.onSpinWait();
                }
            }
            var held = hold;
            if (held != null) {
                held.await();
            }
            synchronized (ranges) {
                ranges.add(new long[]{offset, offset + length});
            }
            return syncResult;
        }

        @Override
        public void close(int fd) {
            assertEquals(FD, fd);
            closed = true;
            closes.incrementAndGet();
        }

        /// Waits until a range passed ends at `end`; the ranges of one unit are passed in order.
        void awaitEnd(long end) throws InterruptedException {
            await(() -> {
                var passed = ranges();
                return !passed.isEmpty() && passed.getLast()[1] == end;
            });
        }

        List<long[]> ranges() {
            synchronized (ranges) {
                return List.copyOf(ranges);
            }
        }

        int count() {
            synchronized (ranges) {
                return ranges.size();
            }
        }
    }

    private static Recorder record(long pageSize) {
        var recorder = new Recorder(pageSize);
        WriteBack.callsForTests = _ -> recorder;
        return recorder;
    }

    private static WriteBack withCalls(Recorder recorder) throws Exception {
        WriteBack.callsForTests = _ -> recorder;
        var file = Files.createTempFile("ledger-writeback", ".blobs");
        Files.delete(file);
        var writeBack = WriteBack.of(file);
        assertTrue(writeBack.active());
        return writeBack;
    }

    /// Asserts that `ranges` are aligned to `pageSize` and follow one another from `from` to `to`.
    private static void assertCovers(List<long[]> ranges, long pageSize, long from, long to) {
        var sorted = new ArrayList<>(ranges);
        sorted.sort(Comparator.comparingLong(range -> range[0]));
        var at = from;
        for (var range : sorted) {
            assertEquals(0, range[0] % pageSize, "a range starts inside a page");
            assertEquals(0, range[1] % pageSize, "a range ends inside a page");
            assertEquals(at, range[0], "the ranges leave a gap or overlap");
            assertTrue(range[1] > range[0], "an empty range");
            at = range[1];
        }
        assertEquals(to, at, "the ranges do not end at the unit's end");
    }

    @Nested
    @DisplayName("ranges")
    class Ranges {
        @Test
        @DisplayName("hand over whole pages of a unit, a part at a time, never past the page the last part ends in")
        void parts() throws Exception {
            var recorder = new Recorder(PageFormat.SIZE);
            try (var writeBack = withCalls(recorder)) {
                var start = 3L * PageFormat.SIZE;
                var end = start + 1_000L * PageFormat.SIZE;
                var unit = writeBack.unit(start, end);
                // The parts of an encrypted unit: the header and four slices, then four slices each
                var at = start + PageFormat.HEADER + WriteBack.PART;
                unit.written(at);
                await(() -> recorder.count() == 1);
                assertArrayEquals(new long[]{start, start + WriteBack.PART}, recorder.ranges().get(0));
                at += WriteBack.PART;
                unit.written(at);
                await(() -> recorder.count() == 2);
                assertArrayEquals(new long[]{start + WriteBack.PART, start + 2L * WriteBack.PART}, recorder.ranges().get(1));
                unit.written(end);
                await(() -> recorder.count() == 3);
                assertArrayEquals(new long[]{start + 2L * WriteBack.PART, end}, recorder.ranges().get(2));
            }
            assertEquals(1, recorder.opens.get());
            assertEquals(1, recorder.closes.get());
        }

        @Test
        @DisplayName("leave a unit written in one part to the force")
        void onePart() throws Exception {
            var recorder = new Recorder(PageFormat.SIZE);
            try (var writeBack = withCalls(recorder)) {
                writeBack.unit(0, 32L * PageFormat.SIZE).written(32L * PageFormat.SIZE);
            }
            assertEquals(0, recorder.count());
            assertEquals(0, recorder.opens.get(), "the file was opened for nothing");
        }

        @Test
        @DisplayName("align to the kernel's pages, leaving out those the unit shares with its neighbours")
        void largerPages() throws Exception {
            var pageSize = 16_384L;
            var recorder = new Recorder(pageSize);
            var start = FIRST;
            var end = start + 200L * PageFormat.SIZE;
            try (var writeBack = withCalls(recorder)) {
                var unit = writeBack.unit(start, end);
                unit.written(start + PageFormat.HEADER + WriteBack.PART);
                unit.written(start + PageFormat.HEADER + 2L * WriteBack.PART);
                unit.written(end);
                recorder.awaitEnd((end / pageSize) * pageSize);
            }
            assertCovers(recorder.ranges(), pageSize, 16_384, (end / pageSize) * pageSize);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("cover a large value's unit exactly, which reads back after a reopen")
        void ledger(boolean encrypted, @TempDir Path directory) throws Exception {
            var recorder = record(PageFormat.SIZE);
            var large = value(1, 1 << 20);
            var small = value(2, 100_000);
            try (var ledger = open(directory, encrypted)) {
                assertTrue(((LiveLedger) ledger).blobsForTests().writeBackForTests().active());
                ledger.commit(ledger.newBatch().put(FILES, "large", large)).awaitDurable();
                ledger.commit(ledger.newBatch().put(FILES, "small", small)).awaitDurable();
                // The close drops the ranges still queued
                recorder.awaitEnd(FIRST + (long) PageFormat.pagesFor(large.length) * PageFormat.SIZE);
            }
            var pages = PageFormat.pagesFor(large.length);
            assertCovers(recorder.ranges(), PageFormat.SIZE, FIRST, FIRST + (long) pages * PageFormat.SIZE);
            WriteBack.callsForTests = null;
            try (var ledger = open(directory, encrypted)) {
                assertArrayEquals(large, ledger.get(FILES, "large").orElseThrow());
                assertArrayEquals(small, ledger.get(FILES, "small").orElseThrow());
            }
        }
    }

    @Nested
    @DisplayName("fallback")
    class Fallback {
        @Test
        @DisplayName("is off on a file system other than the default one, and with the property off")
        void off(@TempDir Path directory) throws Exception {
            try (var disk = CrashFileSystem.create()) {
                assertFalse(WriteBack.of(disk.getPath("/store/" + BlobFile.FILE)).active());
            }
            var previous = System.getProperty("ledger.blobs.writeBack");
            System.setProperty("ledger.blobs.writeBack", "off");
            try {
                assertFalse(WriteBack.of(directory.resolve(BlobFile.FILE)).active());
            } finally {
                if (previous == null) {
                    System.clearProperty("ledger.blobs.writeBack");
                } else {
                    System.setProperty("ledger.blobs.writeBack", previous);
                }
            }
            WriteBack.callsForTests = _ -> null;
            var writeBack = WriteBack.of(directory.resolve(BlobFile.FILE));
            assertFalse(writeBack.active());
            assertSame(null, writeBack.unit(0, 1 << 20));
            writeBack.close();
        }

        @Test
        @DisplayName("turns off when the file cannot be opened, and the values still read back")
        void openFails(@TempDir Path directory) throws Exception {
            var recorder = record(PageFormat.SIZE);
            recorder.openResult = -1;
            var values = new ArrayList<byte[]>();
            try (var ledger = open(directory, true)) {
                var writeBack = ((LiveLedger) ledger).blobsForTests().writeBackForTests();
                for (var i = 0; i < 3; i++) {
                    values.add(value(i, 1 << 20));
                    ledger.commit(ledger.newBatch().put(FILES, "v" + i, values.get(i))).awaitDurable();
                    if (i == 0) {
                        await(() -> !writeBack.active());
                    }
                }
            }
            assertEquals(1, recorder.opens.get());
            assertEquals(0, recorder.count());
            assertEquals(0, recorder.closes.get(), "a descriptor that never opened was closed");
            try (var ledger = open(directory, true)) {
                for (var i = 0; i < values.size(); i++) {
                    assertArrayEquals(values.get(i), ledger.get(FILES, "v" + i).orElseThrow());
                }
            }
        }

        @Test
        @DisplayName("turns off at the first call that fails, closing the file")
        void callFails(@TempDir Path directory) throws Exception {
            var recorder = record(PageFormat.SIZE);
            recorder.syncResult = -1;
            var values = new ArrayList<byte[]>();
            try (var ledger = open(directory, false)) {
                var writeBack = ((LiveLedger) ledger).blobsForTests().writeBackForTests();
                values.add(value(0, 4 << 20));
                ledger.commit(ledger.newBatch().put(FILES, "v0", values.get(0))).awaitDurable();
                await(() -> !writeBack.active());
                values.add(value(1, 4 << 20));
                ledger.commit(ledger.newBatch().put(FILES, "v1", values.get(1))).awaitDurable();
            }
            assertEquals(1, recorder.count());
            assertEquals(1, recorder.closes.get());
            try (var ledger = open(directory, false)) {
                assertArrayEquals(values.get(0), ledger.get(FILES, "v0").orElseThrow());
                assertArrayEquals(values.get(1), ledger.get(FILES, "v1").orElseThrow());
            }
        }
    }

    @Nested
    @DisplayName("close")
    class Close {
        @Test
        @DisplayName("waits for the call in progress, drops the ranges queued, and makes no call once the file is closed")
        void duringCall(@TempDir Path directory) throws Exception {
            var recorder = record(PageFormat.SIZE);
            var held = new CountDownLatch(1);
            recorder.hold = held;
            var ledger = open(directory, true);
            var first = value(1, 2 << 20);
            var second = value(2, 2 << 20);
            ledger.commit(ledger.newBatch().put(FILES, "first", first)).awaitDurable();
            assertTrue(recorder.entered.await(30, TimeUnit.SECONDS));
            // Queued behind the held call, and dropped by the close
            ledger.commit(ledger.newBatch().put(FILES, "second", second)).awaitDurable();
            var closer = Thread.ofPlatform().start(ledger::close);
            closer.join(200);
            assertTrue(closer.isAlive(), "the close did not wait for the call in progress");
            assertFalse(recorder.closed);
            held.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(closer.isAlive());
            assertTrue(recorder.closed);
            assertEquals(1, recorder.closes.get());
            assertEquals(0, recorder.afterClose.get());
            assertEquals(1, recorder.count(), "a range queued before the close was passed");
            WriteBack.callsForTests = null;
            try (var reopened = open(directory, true)) {
                assertArrayEquals(first, reopened.get(FILES, "first").orElseThrow());
                assertArrayEquals(second, reopened.get(FILES, "second").orElseThrow());
            }
        }

        @Test
        @DisplayName("lets concurrent writers of large values race a close without a call after it")
        void racingWriters(@TempDir Path directory) throws Exception {
            for (var round = 0; round < 5; round++) {
                var recorder = record(PageFormat.SIZE);
                var store = directory.resolve("r" + round);
                var ledger = open(store, round % 2 == 0);
                var started = new CountDownLatch(4);
                var writers = new ArrayList<Thread>();
                for (var w = 0; w < 4; w++) {
                    var writer = w;
                    writers.add(Thread.ofPlatform().start(() -> {
                        started.countDown();
                        try {
                            for (var i = 0; ; i++) {
                                ledger.commit(ledger.newBatch().put(FILES, writer + "-" + i, value(i, 600_000)));
                            }
                        } catch (RuntimeException _) {
                            // The store closed under the writer
                        }
                    }));
                }
                started.await();
                await(() -> recorder.count() > 4);
                ledger.close();
                for (var writer : writers) {
                    writer.join(TimeUnit.SECONDS.toMillis(30));
                    assertFalse(writer.isAlive());
                }
                assertTrue(recorder.closed);
                assertEquals(1, recorder.closes.get());
                assertEquals(0, recorder.afterClose.get());
            }
        }

        @Test
        @DisplayName("returns while ranges that follow one another are merged, and stops the thread of the calls")
        void duringMerge() throws Exception {
            var pageSize = PageFormat.SIZE;
            for (var round = 0; round < 300; round++) {
                var recorder = new Recorder(pageSize);
                recorder.spinNanos = 2_000;
                var writeBack = withCalls(recorder);
                // Hands over one page at a time, so the thread of the calls finds the next range queued as it merges
                var unit = writeBack.unit(0, Long.MAX_VALUE & -pageSize);
                var writer = Thread.ofPlatform().start(() -> {
                    for (var at = 2L * pageSize; writeBack.active(); at += pageSize) {
                        unit.written(at);
                    }
                });
                var calls = 1 + round % 4;
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (recorder.count() < calls) {
                    assertTrue(System.nanoTime() < deadline, "timed out");
                    Thread.onSpinWait();
                }
                var closer = Thread.ofPlatform().start(writeBack::close);
                closer.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(closer.isAlive(), "the close hung at round " + round);
                writer.join(TimeUnit.SECONDS.toMillis(30));
                assertFalse(writer.isAlive());
                assertTrue(recorder.closed);
                assertEquals(1, recorder.closes.get());
                assertEquals(0, recorder.afterClose.get());
            }
        }
    }

    @Nested
    @DisplayName("Linux")
    class Linux {
        @Test
        @DisplayName("takes the kernel's calls on Linux, and the values written back read back")
        void live(@TempDir Path directory) throws Exception {
            assumeTrue(System.getProperty("os.name", "").equals("Linux")
                    && Set.of("amd64", "x86_64", "aarch64", "riscv64").contains(System.getProperty("os.arch", "")));
            var values = new ArrayList<byte[]>();
            try (var ledger = open(directory, true)) {
                var writeBack = ((LiveLedger) ledger).blobsForTests().writeBackForTests();
                assertTrue(writeBack.active(), "the writeback is off on Linux");
                for (var length : new int[]{1 << 20, 16 << 20, 300_000}) {
                    var value = value(length, length);
                    values.add(value);
                    ledger.commit(ledger.newBatch().put(FILES, "v" + values.size(), value)).awaitDurable();
                }
                await(() -> writeBack.passedForTests() > 0);
                assertTrue(writeBack.active(), "a call failed");
            }
            try (var ledger = open(directory, true)) {
                for (var i = 0; i < values.size(); i++) {
                    assertArrayEquals(values.get(i), ledger.get(FILES, "v" + (i + 1)).orElseThrow());
                }
            }
        }
    }
}
