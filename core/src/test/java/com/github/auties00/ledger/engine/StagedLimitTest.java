package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Covers the queue of a checkpoint's writer: the flush its limit of 2 MiB starts, which writes windows of 1 MiB and
/// keeps a short last one queued, and the explicit flush, which writes everything.
class StagedLimitTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");
    // The writer's limit and seal window, which the expected flushes below are worked out from
    private static final long LIMIT = 2 << 20;
    private static final long WINDOW = 1 << 20;
    // Units of 3 pages: a window holds 85 of them, and the 171st reaches the limit
    private static final int UNIT_PAGES = 3;

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? KEY : null);
    }

    private static byte[] content(Random random, int pages) {
        var content = new byte[PageFormat.capacity(pages) - random.nextInt(64)];
        random.nextBytes(content);
        return content;
    }

    private static PageRef write(PageFile.Writer writer, int page, byte[] content) throws IOException {
        var pages = PageFormat.pagesFor(content.length);
        var unit = new byte[PageFormat.HEADER + content.length];
        System.arraycopy(content, 0, unit, PageFormat.HEADER, content.length);
        return writer.write(PageFormat.VALUE, 0, page, pages, unit, content.length, false);
    }

    private static void assertReads(PageFile pages, List<PageRef> refs, List<byte[]> contents) {
        for (var i = 0; i < refs.size(); i++) {
            var read = pages.read(refs.get(i), PageFormat.VALUE, false);
            assertArrayEquals(contents.get(i), Arrays.copyOfRange(read, PageFormat.HEADER, PageFormat.HEADER + contents.get(i).length),
                    "unit " + i);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("the flush the limit starts writes two whole windows and keeps the short last one queued, and the explicit flush writes the rest")
    void keepsShortWindow(boolean encrypted, @TempDir Path directory) throws Exception {
        var unitBytes = (long) UNIT_PAGES * PageFormat.SIZE;
        var perWindow = (int) (WINDOW / unitBytes);
        var atLimit = (int) ((LIMIT + unitBytes - 1) / unitBytes);
        assertEquals(85, perWindow);
        assertEquals(171, atLimit);
        var random = new Random(11);
        var refs = new ArrayList<PageRef>();
        var contents = new ArrayList<byte[]>();
        try (var pages = PageFile.open(directory, cipher(encrypted), 16 << 20, new HeapPool(1 << 20))) {
            var writer = pages.writer(5);
            var page = PageFormat.FIRST_UNIT_PAGE;
            for (var i = 0; i < atLimit; i++, page += UNIT_PAGES) {
                contents.add(content(random, UNIT_PAGES));
                refs.add(write(writer, page, contents.getLast()));
                if (i < atLimit - 1) {
                    assertNotNull(writer.staged(refs.getFirst()), "unit 0 after " + (i + 1) + " units");
                }
            }
            for (var i = 0; i < 2 * perWindow; i++) {
                assertNull(writer.staged(refs.get(i)), "unit " + i + " is still queued");
            }
            for (var i = 2 * perWindow; i < atLimit; i++) {
                assertArrayEquals(contents.get(i), Arrays.copyOfRange(writer.staged(refs.get(i)), PageFormat.HEADER,
                        PageFormat.HEADER + contents.get(i).length), "unit " + i);
            }
            assertEquals(atLimit * unitBytes, writer.written());
            // The written windows are in the file already, and read back from it
            assertReads(pages, refs.subList(0, 2 * perWindow), contents.subList(0, 2 * perWindow));

            writer.flush();
            for (var ref : refs) {
                assertNull(writer.staged(ref));
            }
            assertEquals(atLimit * unitBytes, writer.written());
            pages.force();
            assertReads(pages, refs, contents);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("units of any length queued in any order of their pages never queue the limit, and every one is written once flushed")
    void boundsQueue(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 17 : 19);
        var count = 3_000;
        var slots = new ArrayList<int[]>();
        var page = PageFormat.FIRST_UNIT_PAGE;
        for (var i = 0; i < count; i++) {
            var pages = 1 + random.nextInt(4);
            slots.add(new int[]{page, pages});
            // A gap now and then, so a window holds several runs
            page += pages + (random.nextInt(8) == 0 ? 1 : 0);
        }
        Collections.shuffle(slots, random);
        var refs = new ArrayList<PageRef>();
        var contents = new ArrayList<byte[]>();
        var queued = new ArrayList<PageRef>();
        var total = 0L;
        try (var pages = PageFile.open(directory, cipher(encrypted), 16 << 20, new HeapPool(1 << 20))) {
            var writer = pages.writer(6);
            var flushes = 0;
            for (var slot : slots) {
                contents.add(content(random, slot[1]));
                var ref = write(writer, slot[0], contents.getLast());
                refs.add(ref);
                queued.add(ref);
                total += (long) slot[1] * PageFormat.SIZE;
                var flushed = queued.stream().filter(candidate -> writer.staged(candidate) == null).toList();
                if (!flushed.isEmpty()) {
                    flushes++;
                    queued.removeAll(flushed);
                    // A flush writes the units of the lowest pages and keeps those of the highest
                    var highestFlushed = flushed.stream().mapToInt(PageRef::page).max().orElseThrow();
                    var lowestQueued = queued.stream().mapToInt(PageRef::page).min().orElse(Integer.MAX_VALUE);
                    assertTrue(highestFlushed < lowestQueued, "page " + highestFlushed + " written, page " + lowestQueued + " kept");
                }
                var queuedBytes = queued.stream().mapToLong(candidate -> (long) candidate.pages() * PageFormat.SIZE).sum();
                assertTrue(queuedBytes < LIMIT, queuedBytes + " bytes queued");
                assertEquals(total, writer.written());
            }
            assertTrue(flushes > 5, flushes + " flushes");
            writer.flush();
            for (var ref : refs) {
                assertNull(writer.staged(ref));
            }
            pages.force();
            assertReads(pages, refs, contents);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a write that fails in the flush the limit starts fails the unit that started it, and after the discard a retry writes the same pages")
    void failedLimitFlush(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var pool = new HeapPool(64 << 20, false);
        // The overflow kept through collections, as the free arrays are looked for in it
        pool.holdOverflowForTests(true);
        var writes = new AtomicInteger(-1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && writes.get() >= 0
                    && writes.incrementAndGet() == 2) {
                throw new IOException("No space left on device");
            }
        });
        var random = new Random(23);
        var contents = new ArrayList<byte[]>();
        for (var i = 0; i < 400; i++) {
            contents.add(content(random, UNIT_PAGES));
        }
        try (var pages = PageFile.open(directory, cipher(encrypted), 16 << 20, pool)) {
            var writer = pages.writer(7);
            writes.set(0);
            var failedAt = -1;
            for (var i = 0; i < contents.size(); i++) {
                try {
                    write(writer, PageFormat.FIRST_UNIT_PAGE + i * UNIT_PAGES, contents.get(i));
                } catch (IOException _) {
                    failedAt = i;
                    break;
                }
            }
            // In the first flush, which the 171st unit starts
            assertEquals(170, failedAt);
            writer.discard();
            writes.set(-1);
            ValueUnitsTest.assertNoArrayTwice(pool);

            var retry = pages.writer(8);
            var refs = new ArrayList<PageRef>();
            for (var i = 0; i < contents.size(); i++) {
                refs.add(write(retry, PageFormat.FIRST_UNIT_PAGE + i * UNIT_PAGES, contents.get(i)));
            }
            retry.flush();
            pages.force();
            assertReads(pages, refs, contents);
            ValueUnitsTest.assertNoArrayTwice(pool);
        }
    }
}
