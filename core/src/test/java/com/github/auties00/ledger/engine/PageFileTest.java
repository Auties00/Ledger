package com.github.auties00.ledger.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.spec.SecretKeySpec;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The batched reads of the data file on their own: from many threads at once beside the file's other reads, with idle
/// ticks closing the channels no read takes, by an interrupted caller, and once the file closed.
class PageFileTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");

    @Test
    @DisplayName("batch reads from many threads at once, beside single reads and idle ticks, read every unit as it was written")
    void concurrentBatches(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var units = write(pages, 20_000);
            var refs = new ArrayList<>(units.keySet());
            pages.cache().clearForTests();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            var start = new CountDownLatch(1);
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 8; t++) {
                var random = new Random(t);
                threads.add(Thread.ofPlatform().start(() -> run(start, failures, () -> {
                    for (var round = 0; round < 30; round++) {
                        var batch = new ArrayList<>(refs);
                        Collections.shuffle(batch, random);
                        batch.subList(50 + random.nextInt(150), batch.size()).clear();
                        var kinds = new byte[batch.size()];
                        java.util.Arrays.fill(kinds, PageFile.NODE);
                        var read = pages.readBatch(batch, kinds);
                        for (var i = 0; i < batch.size(); i++) {
                            assertArrayEquals(java.util.Arrays.copyOf(units.get(batch.get(i)), read[i].length), read[i], "a batch read another unit");
                        }
                    }
                })));
            }
            for (var t = 0; t < 2; t++) {
                var random = new Random(100 + t);
                threads.add(Thread.ofPlatform().start(() -> run(start, failures, () -> {
                    for (var i = 0; i < 2_000; i++) {
                        var ref = refs.get(random.nextInt(refs.size()));
                        assertArrayEquals(units.get(ref), pages.read(ref, PageFile.NODE, false), "a read found another unit");
                    }
                })));
            }
            threads.add(Thread.ofPlatform().start(() -> run(start, failures, () -> {
                for (var i = 0; i < 200; i++) {
                    // Ticks close the idle batch channels whenever no read took one since the tick before
                    IdleTicker.tickAll();
                    Thread.sleep(2);
                }
            })));
            start.countDown();
            for (var thread : threads) {
                thread.join();
            }
            if (!failures.isEmpty()) {
                var failure = new AssertionError("a reader failed");
                failures.forEach(failure::addSuppressed);
                throw failure;
            }
            // Channels the ticks closed are opened again
            IdleTicker.tickAll();
            IdleTicker.tickAll();
            var kinds = new byte[refs.size()];
            java.util.Arrays.fill(kinds, PageFile.NODE);
            var read = pages.readBatch(refs, kinds);
            for (var i = 0; i < refs.size(); i++) {
                assertArrayEquals(java.util.Arrays.copyOf(units.get(refs.get(i)), read[i].length), read[i]);
            }
        }
    }

    @Test
    @DisplayName("a batch read by an interrupted thread reads every unit and keeps the interrupt")
    void interruptedBatch(@TempDir Path directory) throws Exception {
        try (var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20))) {
            var units = write(pages, 5_000);
            var refs = new ArrayList<>(units.keySet());
            var kinds = new byte[refs.size()];
            java.util.Arrays.fill(kinds, PageFile.NODE);
            Thread.currentThread().interrupt();
            byte[][] read;
            try {
                read = pages.readBatch(refs, kinds);
            } finally {
                assertTrue(Thread.interrupted(), "the batch lost the caller's interrupt");
            }
            for (var i = 0; i < refs.size(); i++) {
                assertArrayEquals(java.util.Arrays.copyOf(units.get(refs.get(i)), read[i].length), read[i]);
            }
            // The channels the interrupt closed were replaced
            read = pages.readBatch(refs, kinds);
            for (var i = 0; i < refs.size(); i++) {
                assertArrayEquals(java.util.Arrays.copyOf(units.get(refs.get(i)), read[i].length), read[i]);
            }
        }
    }

    @Test
    @DisplayName("a batch refuses a delta record, and throws once the file closed")
    void refusals(@TempDir Path directory) throws Exception {
        var pages = PageFile.open(directory, new PageCipher(KEY), 1 << 20, new HeapPool(1 << 20));
        var units = write(pages, 2_000);
        var refs = new ArrayList<>(units.keySet());
        var record = new PageRef(refs.getFirst().page(), 1, refs.getFirst().nonceHigh(), refs.getFirst().nonceLow(), 0);
        assertThrows(IllegalArgumentException.class, () -> pages.readBatch(List.of(record), new byte[]{PageFile.NODE}));
        var kinds = new byte[refs.size()];
        java.util.Arrays.fill(kinds, PageFile.NODE);
        assertEquals(refs.size(), pages.readBatch(refs, kinds).length);
        pages.close();
        assertThrows(IllegalStateException.class, () -> pages.readBatch(refs, kinds));
    }

    /// Writes a tree of `keys` keys to `pages` and {@return the plaintext of each of its units, read back}
    private static Map<PageRef, byte[]> write(PageFile pages, int keys) throws Exception {
        var random = new Random(keys);
        var ops = new ArrayList<TreeUpdate.Op>();
        for (var i = 0; i < keys; i++) {
            var value = new byte[100];
            random.nextBytes(value);
            ops.add(new TreeUpdate.Op(MemorySegment.ofArray("k%07d".formatted(i).getBytes()), MemorySegment.ofArray(value)));
        }
        var writer = pages.writer(random.nextLong());
        var root = new TreeUpdate(pages, writer, new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet())).apply(null, ops, List.of());
        writer.flush();
        pages.cache().clearForTests();
        var units = new HashMap<PageRef, byte[]>();
        collect(pages, root.ref(), units);
        return units;
    }

    private static void collect(PageFile pages, PageRef ref, Map<PageRef, byte[]> units) {
        var unit = pages.read(ref, PageFile.NODE, false);
        units.put(ref, unit);
        if (!TreeNodes.isLeaf(unit)) {
            for (var i = 0; i < TreeNodes.count(unit); i++) {
                collect(pages, TreeNodes.childRef(unit, i), units);
            }
        }
    }

    private static void run(CountDownLatch start, ConcurrentLinkedQueue<Throwable> failures, Work work) {
        try {
            start.await();
            work.run();
        } catch (Throwable throwable) {
            failures.add(throwable);
        }
    }

    @FunctionalInterface
    private interface Work {
        void run() throws Exception;
    }
}
