package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor.ChangeKind;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The window a checkpoint's writer seals its units into side by side: each unit on the file is the one sealed in an
/// array of its own, whatever the window held before; a flush of a single window, before the writer has a window, seals
/// into the pool's arrays, which go back to its overflow; a writer takes one window, from the pool's spare windows when
/// one is left, and gives it back to them after its last flush, never after a failed one or a discard, and never to the
/// pool's arrays; a unit longer than the window seals into an array of the pool, which goes back to the overflow only
/// once the unit is written; and a store whose checkpoints write every kind of unit, in windows with gaps between their
/// pages, reads every value back from the file.
class CheckpointWindowTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    // A value whose unit is longer than the window, and one whose unit fills it alone
    private static final int LONGER_THAN_WINDOW = 1_000_000;
    private static final int FILLS_THE_WINDOW = PageFormat.capacity(PageFile.WINDOW_BYTES / PageFormat.SIZE);

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? new SecretKeySpec(KEY, "AES") : null);
    }

    /// The data file of one test, its pool watched for the arrays it takes back.
    private static final class Pages implements AutoCloseable {
        final HeapPool pool;
        final PageFile file;
        final Path path;
        final Set<byte[]> released;

        Pages(Path directory, boolean encrypted) throws IOException {
            Files.createDirectories(directory);
            this.pool = new HeapPool(64L << 20, false);
            var cache = PageCache.isolated(64L << 20);
            IdleTicker.unwatch(cache);
            this.file = PageFile.open(directory, cipher(encrypted), cache, pool);
            this.path = directory.resolve(PageFile.FILE);
            this.released = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
            pool.watchForTests(new HeapPool.Watcher() {
                @Override
                public void acquired(byte[] array) {
                }

                @Override
                public void released(byte[] array) {
                    released.add(array);
                }
            });
        }

        PageFile.Writer writer(long nonceHigh) {
            return file.writer(nonceHigh, () -> false, () -> false);
        }

        @Override
        public void close() {
            file.close();
        }
    }

    /// Queues a free-list unit of `length` random bytes of content at `page`, and records its plaintext in `expected`.
    private static void queueNode(PageFile.Writer writer, TreeMap<Integer, byte[]> expected, Random random, int page, int length) throws IOException {
        var unit = new byte[PageFormat.HEADER + length];
        random.nextBytes(unit);
        writer.write(PageFormat.FREE_LIST, 0, page, 1, unit, length, false);
        expected.put(page, unit.clone());
    }

    /// Queues a value unit of `length` random bytes at `page`, and records its plaintext in `expected`.
    ///
    /// @return its pages
    private static int queueValue(PageFile.Writer writer, TreeMap<Integer, byte[]> expected, Random random, int page, int length) throws IOException {
        var value = new byte[length];
        random.nextBytes(value);
        var pages = PageFormat.pagesFor(length);
        var ref = writer.writeValue(page, pages, MemorySegment.ofArray(value));
        var plain = new byte[PageFormat.HEADER + length];
        PageFile.header(plain, PageFormat.VALUE, 0, page, pages, length, ref.nonceHigh(), ref.nonceLow());
        System.arraycopy(value, 0, plain, PageFormat.HEADER, length);
        expected.put(page, plain);
        return pages;
    }

    /// Checks that each unit of `expected` is on the file as the unit sealed alone from its plaintext, and opens to it.
    private static void assertSealedAlone(Path file, TreeMap<Integer, byte[]> expected, boolean encrypted) throws IOException {
        var reference = cipher(encrypted);
        var opener = cipher(encrypted);
        try (var channel = FileChannel.open(file, StandardOpenOption.READ)) {
            for (var entry : expected.entrySet()) {
                var plain = entry.getValue();
                var pages = Bytes.i32(plain, PageFormat.PAGES);
                var alone = new byte[pages * PageFormat.SIZE];
                reference.seal(plain, pages, alone, 0);
                var onFile = ByteBuffer.allocate(alone.length);
                var position = PageFormat.offset(entry.getKey());
                while (onFile.hasRemaining() && channel.read(onFile, position + onFile.position()) > 0) {
                    // Reads until the unit is whole
                }
                assertArrayEquals(alone, onFile.array(), "the unit at page " + entry.getKey());
                var opened = new byte[alone.length];
                assertTrue(opener.open(onFile.array(), 0, pages, opened), "the unit at page " + entry.getKey());
                assertArrayEquals(plain, Arrays.copyOf(opened, plain.length), "the unit at page " + entry.getKey());
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a flush of many windows seals node and value units side by side, across gaps between their pages, into one window, each unit the one sealed alone")
    void sealsAsAlone(boolean encrypted, @TempDir Path directory) throws IOException {
        try (var pages = new Pages(directory, encrypted)) {
            var writer = pages.writer(0x1234_5678_9ABCL);
            var random = new Random(encrypted ? 7 : 8);
            var expected = new TreeMap<Integer, byte[]>();
            var page = PageFormat.FIRST_UNIT_PAGE;
            var longUnits = 0;
            var queued = 0L;
            while (queued < 5L << 20) {
                var choice = random.nextInt(20);
                int unitPages;
                if (choice < 12) {
                    queueNode(writer, expected, random, page, random.nextInt(PageFormat.capacity(1) + 1));
                    unitPages = 1;
                } else if (choice < 18) {
                    unitPages = queueValue(writer, expected, random, page, PageFormat.capacity(1) + 1 + random.nextInt(30_000));
                } else {
                    // A gap of pages no unit of this checkpoint takes
                    page += 1 + random.nextInt(3);
                    continue;
                }
                page += unitPages;
                queued += (long) unitPages * PageFormat.SIZE;
                if (queued > 2L << 20 && longUnits == 0) {
                    // A unit longer than the window between two of its windows, and one the window holds alone
                    page += queueValue(writer, expected, random, page, LONGER_THAN_WINDOW);
                    page += queueValue(writer, expected, random, page, FILLS_THE_WINDOW);
                    longUnits++;
                }
            }
            var acquires = pages.pool.acquiresForTests();
            writer.flush();
            var window = writer.windowForTests();
            assertNotNull(window);
            assertEquals(PageFile.WINDOW_BYTES, window.length);
            assertEquals(1, writer.windowsTakenForTests(), "the windows the writer allocated");
            assertEquals(acquires + 1, pages.pool.acquiresForTests(), "the arrays of the pool the flush took: the long unit's");
            assertEquals(1, pages.released.size(), "the arrays the flush gave back: the long unit's");
            assertFalse(pages.released.contains(window), "the window went to the pool");
            assertSealedAlone(pages.path, expected, encrypted);

            // The window keeps the last units' bytes: the next flush seals over them
            var more = new TreeMap<Integer, byte[]>();
            for (var i = 0; i < 40; i++) {
                if (i % 3 == 0) {
                    page += queueValue(writer, more, random, page, 100 + random.nextInt(9_000));
                } else {
                    queueNode(writer, more, random, page++, random.nextInt(PageFormat.capacity(1) + 1));
                }
            }
            writer.finish();
            assertNull(writer.windowForTests(), "the window after the last flush");
            assertEquals(1, writer.windowsTakenForTests());
            assertFalse(pages.released.contains(window), "the window went to the pool's arrays");
            assertEquals(window.length, pages.pool.windowSpareBytesForTests(), "the window the pool keeps for the next checkpoint");
            assertSame(window, pages.pool.acquireWindow(PageFormat.SIZE), "the window the next checkpoint takes");
            assertSealedAlone(pages.path, more, encrypted);
        }
    }

    @Test
    @DisplayName("a flush of one window seals into the pool's arrays and gives them back to its overflow; a longer one takes the whole window, which later flushes keep and the last gives back to the pool's spares")
    void pooledUntilAWindow(@TempDir Path directory) throws IOException {
        try (var pages = new Pages(directory, true)) {
            // No collection frees the overflow the flush gives its arrays back to before the test counts them
            pages.pool.holdOverflowForTests(true);
            var writer = pages.writer(1L);
            var random = new Random(3);
            var expected = new TreeMap<Integer, byte[]>();
            var page = PageFormat.FIRST_UNIT_PAGE;
            for (var i = 0; i < 3; i++) {
                queueNode(writer, expected, random, page++, 100);
            }
            page += queueValue(writer, expected, random, page, 20_000);
            var acquires = pages.pool.acquiresForTests();
            writer.flush();
            assertNull(writer.windowForTests(), "the window of a flush of one window");
            assertEquals(acquires + 4, pages.pool.acquiresForTests(), "the arrays of the pool the flush took");
            assertEquals(4, pages.released.size(), "the arrays the flush gave back");
            assertEquals(0, pages.pool.reservedBytesForTests(), "the bytes the flush gave back to the reserve");
            assertTrue(pages.pool.overflowBytesForTests() > 0, "the bytes the flush gave back to the overflow");
            assertSealedAlone(pages.path, expected, true);

            for (var i = 0; i < 600; i++) {
                queueNode(writer, expected, random, page++, 100);
            }
            writer.flush();
            var window = writer.windowForTests();
            assertNotNull(window, "the window of a flush of more than one window");
            assertEquals(PageFile.WINDOW_BYTES, window.length);
            for (var i = 0; i < 3; i++) {
                queueNode(writer, expected, random, page++, 100);
            }
            pages.released.clear();
            writer.flush();
            assertSame(window, writer.windowForTests(), "the window of a later flush of one window");
            assertTrue(pages.released.isEmpty(), "arrays went to the pool");
            assertEquals(1, writer.windowsTakenForTests());
            assertEquals(0, pages.pool.windowSpareBytesForTests(), "the spare windows while the writer holds its own");
            writer.finish();
            assertNull(writer.windowForTests());
            assertFalse(pages.released.contains(window), "the window went to the pool's arrays");
            assertEquals(window.length, pages.pool.windowSpareBytesForTests(), "the spare windows after the last flush");
            assertSealedAlone(pages.path, expected, true);

            // The next checkpoint that needs a window takes the spare; a discard drops it
            var discarded = pages.writer(2L);
            for (var i = 0; i < 600; i++) {
                queueNode(discarded, new TreeMap<>(), random, page++, 100);
            }
            discarded.flush();
            assertSame(window, discarded.windowForTests(), "the window of the next checkpoint");
            assertEquals(1, discarded.windowsTakenForTests());
            discarded.discard();
            assertNull(discarded.windowForTests(), "the window after a discard");
            assertEquals(0, pages.pool.windowSpareBytesForTests(), "the spare windows after a discard");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("neither a window nor the pool's arrays a flush failed to write go back to the pool")
    void failedFlush(boolean encrypted) throws IOException {
        var disk = CrashFileSystem.create();
        try (var pages = new Pages(disk.getPath("/failed"), encrypted)) {
            var random = new Random(4);
            var page = PageFormat.FIRST_UNIT_PAGE;
            var windowed = pages.writer(9L);
            for (var i = 0; i < 300; i++) {
                queueNode(windowed, new TreeMap<>(), random, page++, 100);
            }
            windowed.flush();
            assertNotNull(windowed.windowForTests());
            var pooled = pages.writer(10L);
            for (var i = 0; i < 3; i++) {
                page += queueValue(windowed, new TreeMap<>(), random, page, 2_000);
                page += queueValue(pooled, new TreeMap<>(), random, page, 2_000);
            }
            pages.released.clear();
            disk.intercept((path, kind) -> {
                if (kind == ChangeKind.WRITE && path.endsWith(PageFile.FILE)) {
                    throw new IOException("The device failed");
                }
            });
            assertThrows(IOException.class, windowed::finish);
            assertNull(windowed.windowForTests(), "the window after a failed flush");
            assertThrows(IOException.class, pooled::finish);
            assertNull(pooled.windowForTests(), "the window of a failed flush of one window");
            assertEquals(0, pages.pool.windowSpareBytesForTests(), "the spare windows after failed flushes");
            assertTrue(pages.released.isEmpty(), "an array holding the plaintext of a value went to the pool");
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a unit longer than the window seals into an array of the pool, which goes back once the unit is written and never if the write fails")
    void longUnit(boolean encrypted) throws IOException {
        var disk = CrashFileSystem.create();
        try (var pages = new Pages(disk.getPath("/written"), encrypted)) {
            var writer = pages.writer(5L);
            var expected = new TreeMap<Integer, byte[]>();
            queueValue(writer, expected, new Random(1), PageFormat.FIRST_UNIT_PAGE, LONGER_THAN_WINDOW);
            writer.finish();
            assertEquals(0, writer.windowsTakenForTests(), "a window for a unit longer than it");
            assertEquals(1, pages.released.size(), "the arrays the unit gave back");
            assertTrue(pages.released.iterator().next().length >= PageFormat.pagesFor(LONGER_THAN_WINDOW) * PageFormat.SIZE);
            assertEquals(0, pages.pool.reservedBytesForTests(), "the bytes the unit's array keeps in the reserve");
            assertSealedAlone(pages.path, expected, encrypted);
        }

        var failing = CrashFileSystem.create();
        try (var pages = new Pages(failing.getPath("/failed"), encrypted)) {
            var writer = pages.writer(6L);
            queueValue(writer, new TreeMap<>(), new Random(2), PageFormat.FIRST_UNIT_PAGE, LONGER_THAN_WINDOW);
            failing.intercept((path, kind) -> {
                if (kind == ChangeKind.WRITE && path.endsWith(PageFile.FILE)) {
                    throw new IOException("The device failed");
                }
            });
            assertThrows(IOException.class, writer::finish);
            assertTrue(pages.released.isEmpty(), "an array holding the plaintext of a value went to the pool");
            assertEquals(0, pages.pool.overflowBytesForTests() + pages.pool.reservedBytesForTests());
        }
    }

    /// Opens a store that keeps every value in the data file, whatever its length, and moves the values of its durable
    /// commits to the log past a budget of 256 KiB, so its checkpoints write value units both from memory and from the
    /// log.
    private static LiveLedger open(Path directory, boolean encrypted) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).largeValues(Long.MAX_VALUE))
                .segmentSize(16L << 20)
                .spillBudget(256 << 10)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.state(MESSAGES).overwrites.forceSpillsForTests();
        return ledger;
    }

    private static byte[] key(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).array();
    }

    private static byte[] value(int i, int round) {
        var length = i == 7 ? LONGER_THAN_WINDOW : i == 11 ? FILLS_THE_WINDOW : i % 4 == 0 ? 5_000 + i % 13 * 700 : 200 + i % 7;
        var value = new byte[length];
        for (var j = 0; j < length; j += 97) {
            value[j] = (byte) (i * 31 + round * 7 + j);
        }
        value[length - 1] = (byte) (i + round);
        return value;
    }

    /// Writes `keys` from `from`, with a stride of `step`, as `round` in one commit, removing those `remove` selects,
    /// and records them in `expected`; then moves the durable values past the budget to the log.
    private static void write(LiveLedger ledger, TreeMap<Integer, byte[]> expected, int from, int to, int step, int round, boolean remove) throws Exception {
        var batch = ledger.newBatch();
        for (var i = from; i < to; i += step) {
            if (remove && i % 3 == 0) {
                batch.delete(MESSAGES, key(i));
                expected.remove(i);
            } else {
                var value = value(i, round);
                batch.put(MESSAGES, key(i), value);
                expected.put(i, value);
            }
        }
        ledger.commit(batch).awaitDurable();
        var spills = ledger.spills();
        spills.drain(ledger.log()::durableSequence);
        spills.awaitIdleForTests();
    }

    private static void verify(Ledger ledger, TreeMap<Integer, byte[]> expected, int keys) {
        for (var i = 0; i < keys; i++) {
            var value = ledger.get(MESSAGES, key(i));
            if (expected.containsKey(i)) {
                assertArrayEquals(expected.get(i), value.orElseThrow(), "key " + i);
            } else {
                assertTrue(value.isEmpty(), "key " + i);
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store whose checkpoints write leaves, value units from memory and from the log, units longer than the window and units into freed pages reads every value back from the file")
    void readsEveryUnitBack(boolean encrypted, @TempDir Path directory) throws Exception {
        var keys = 4_000;
        var expected = new TreeMap<Integer, byte[]>();
        try (var ledger = open(directory, encrypted)) {
            write(ledger, expected, 0, keys, 1, 0, false);
            assertTrue(ledger.spills().loggedValuesForTests() > 0, "no value moved to the log");
            ledger.checkpoint();
            // Replaced and removed values free their units' pages, which the next checkpoints take again
            write(ledger, expected, 0, keys, 2, 1, true);
            ledger.checkpoint();
            write(ledger, expected, 1, keys, 2, 2, true);
            write(ledger, expected, 0, keys, 3, 3, false);
            ledger.checkpoint();
            ledger.clearPageCacheForTests();
            verify(ledger, expected, keys);
        }
        try (var ledger = open(directory, encrypted)) {
            verify(ledger, expected, keys);
            write(ledger, expected, 0, keys, 5, 4, false);
        }
        try (var ledger = open(directory, encrypted)) {
            verify(ledger, expected, keys);
        }
    }
}
