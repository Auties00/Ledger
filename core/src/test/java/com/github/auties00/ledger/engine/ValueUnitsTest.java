package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The value units a checkpoint builds from the versions' values in the arrays it seals them into, uncached: they are
/// the bytes a unit built in an array of its own and sealed from it makes, whatever the pool's arrays held before, the
/// queued ones still read back, and a flush that fails part way leaves the pool and the store sound.
class ValueUnitsTest {
    private static final SecretKeySpec KEY = new SecretKeySpec(new byte[32], "AES");
    private static final byte DIRT = 0x5A;
    private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());

    private static PageCipher cipher(boolean encrypted) {
        return new PageCipher(encrypted ? KEY : null);
    }

    static Stream<Arguments> lengths() {
        var slice = 64 << 10;
        var lengths = new int[]{0, 1, 15, 16, 17, 2_000, 4_000, PageFormat.capacity(1), PageFormat.capacity(1) + 1,
                PageFormat.capacity(3), 16 << 10, slice - 1, slice, slice + 1, 2 * slice + 13, (1 << 20) - 100};
        return Arrays.stream(lengths).boxed().flatMap(length -> Stream.of(true, false).map(encrypted -> Arguments.of(length, encrypted)));
    }

    @ParameterizedTest(name = "{0} bytes, encrypted: {1}")
    @MethodSource("lengths")
    @DisplayName("a unit sealed over itself in a longer, used array is the unit sealed from an array of its own, and the bytes past it stay")
    void sealsInPlace(int length, boolean encrypted) {
        var pages = PageFormat.pagesFor(length);
        var unitLength = pages * PageFormat.SIZE;
        var tag = unitLength - PageFormat.TAG;
        var content = new byte[length];
        new Random(length).nextBytes(content);
        var plain = new byte[PageFormat.HEADER + length];
        PageFile.header(plain, PageFormat.VALUE, 0, 9, pages, length, 41L, length + 3);
        System.arraycopy(content, 0, plain, PageFormat.HEADER, length);
        var whole = new byte[unitLength];
        cipher(encrypted).seal(plain, pages, whole, 0);

        // As a pool lends it: rounded up past the unit, with the bytes of its last use
        var unit = new byte[Integer.highestOneBit(unitLength) << 1];
        Arrays.fill(unit, DIRT);
        System.arraycopy(plain, 0, unit, 0, plain.length);
        Arrays.fill(unit, plain.length, tag, (byte) 0);
        cipher(encrypted).seal(unit, pages);
        assertArrayEquals(whole, Arrays.copyOf(unit, unitLength));
        for (var i = unitLength; i < unit.length; i++) {
            assertEquals(DIRT, unit[i], "byte " + i + " past the unit");
        }
        var opened = new byte[unitLength];
        assertTrue(cipher(encrypted).open(unit, 0, pages, opened));
        assertArrayEquals(plain, Arrays.copyOf(opened, plain.length));

        assertThrows(IllegalArgumentException.class, () -> cipher(encrypted).seal(new byte[unitLength - 1], pages));
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("value units queued from segments write the bytes of units queued whole, in several seal windows, and are read back while queued but never cached")
    void writesTheSameUnits(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(31);
        var sizes = new ArrayList<Integer>(List.of(2_000, 2_100, 4_000, PageFormat.capacity(1), PageFormat.capacity(1) + 1,
                PageFormat.capacity(3), 16 << 10, 70_000, (1 << 20) - 100));
        // Past the seal window of a flush and the units sealed on every processor
        for (var i = 0; i < 150; i++) {
            sizes.add(2_000 + random.nextInt(14_000));
        }
        var values = new byte[sizes.size()][];
        for (var i = 0; i < values.length; i++) {
            values[i] = new byte[sizes.get(i)];
            random.nextBytes(values[i]);
        }
        var fromSegments = directory.resolve("segments");
        var whole = directory.resolve("whole");
        Files.createDirectories(fromSegments);
        Files.createDirectories(whole);
        var segmentRefs = new PageRef[values.length];
        var wholeRefs = new PageRef[values.length];
        try (var segmentPages = PageFile.open(fromSegments, cipher(encrypted), 16 << 20, dirtyPool());
             var wholePages = PageFile.open(whole, cipher(encrypted), 16 << 20, dirtyPool())) {
            var segmentSpace = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var wholeSpace = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var segmentWriter = segmentPages.writer(5);
            var wholeWriter = wholePages.writer(5);
            for (var i = 0; i < values.length; i++) {
                var pages = PageFormat.pagesFor(values[i].length);
                // A read-only slice of a longer array, as a version's value is
                var home = new byte[values[i].length + 11];
                System.arraycopy(values[i], 0, home, 7, values[i].length);
                var value = MemorySegment.ofArray(home).asReadOnly().asSlice(7, values[i].length);
                segmentRefs[i] = segmentWriter.writeValue(segmentSpace.allocate(pages), pages, value);
                var unit = new byte[PageFormat.HEADER + values[i].length];
                System.arraycopy(values[i], 0, unit, PageFormat.HEADER, values[i].length);
                wholeRefs[i] = wholeWriter.write(PageFormat.VALUE, 0, wholeSpace.allocate(pages), pages, unit, values[i].length, false);
                assertEquals(wholeRefs[i], segmentRefs[i]);
                // Read back before it is sealed, as the unit queued whole is
                var staged = segmentWriter.staged(segmentRefs[i]);
                if (staged != null) {
                    assertArrayEquals(wholeWriter.staged(wholeRefs[i]), staged);
                }
            }
            assertEquals(0, segmentPages.cache().size(), "the checkpoint cached a value unit");
            segmentWriter.flush();
            wholeWriter.flush();
            segmentPages.force();
            wholePages.force();
            var segmentFile = Files.readAllBytes(fromSegments.resolve(PageFile.FILE));
            var wholeFile = Files.readAllBytes(whole.resolve(PageFile.FILE));
            for (var i = 0; i < values.length; i++) {
                var ref = segmentRefs[i];
                var start = (int) PageFormat.offset(ref.page());
                var end = start + ref.pages() * PageFormat.SIZE;
                assertArrayEquals(Arrays.copyOfRange(wholeFile, start, end), Arrays.copyOfRange(segmentFile, start, end),
                        "the unit of " + values[i].length + " bytes");
                var read = segmentPages.read(ref, PageFormat.VALUE, false);
                assertArrayEquals(values[i], Arrays.copyOfRange(read, PageFormat.HEADER, PageFormat.HEADER + values[i].length));
            }
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a flush whose second window fails, then the discard of the checkpoint, put no array in the pool twice, and the pages are written again")
    void failedFlush(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        Files.createDirectories(directory);
        var pool = new HeapPool(64 << 20, false);
        // The overflow kept through collections, as the free arrays are looked for in it
        pool.holdOverflowForTests(true);
        var writes = new AtomicInteger(-1);
        disk.intercept((path, kind) -> {
            if (kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE) && writes.get() >= 0
                    && writes.incrementAndGet() >= 2) {
                throw new IOException("No space left on device");
            }
        });
        var random = new Random(37);
        var values = new byte[400][];
        for (var i = 0; i < values.length; i++) {
            values[i] = new byte[2_500 + random.nextInt(9_500)];
            random.nextBytes(values[i]);
        }
        try (var pages = PageFile.open(directory, cipher(encrypted), 16 << 20, pool)) {
            var space = new FreeSpace(PageFormat.FIRST_UNIT_PAGE, new BitSet());
            var slots = new int[values.length];
            for (var i = 0; i < values.length; i++) {
                slots[i] = space.allocate(PageFormat.pagesFor(values[i].length));
            }
            var writer = pages.writer(7);
            for (var i = 0; i < values.length; i++) {
                writer.writeValue(slots[i], PageFormat.pagesFor(values[i].length), MemorySegment.ofArray(values[i]).asReadOnly());
            }
            writes.set(0);
            assertThrows(IOException.class, writer::flush);
            writer.discard();
            writes.set(-1);
            assertNoArrayTwice(pool);

            // The checkpoint that follows writes the same pages under nonces of its own
            var retry = pages.writer(8);
            var refs = new PageRef[values.length];
            for (var i = 0; i < values.length; i++) {
                refs[i] = retry.writeValue(slots[i], PageFormat.pagesFor(values[i].length), MemorySegment.ofArray(values[i]).asReadOnly());
            }
            retry.flush();
            pages.force();
            for (var i = 0; i < values.length; i++) {
                var read = pages.read(refs[i], PageFormat.VALUE, true);
                assertArrayEquals(values[i], Arrays.copyOfRange(read, PageFormat.HEADER, PageFormat.HEADER + values[i].length));
            }
            assertNoArrayTwice(pool);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store whose checkpoint of value units fails on the data file keeps every value, through the next checkpoint and a reopen")
    void failedCheckpoint(boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var failing = new AtomicBoolean(true);
        disk.intercept((path, kind) -> {
            if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && path.endsWith(PageFile.FILE)) {
                throw new IOException("No space left on device");
            }
        });
        var directory = disk.getPath("/store");
        var encryption = encrypted ? Encryption.key(new byte[32]) : Encryption.none();
        var random = new Random(41);
        var values = new byte[300][];
        var failures = new ConcurrentLinkedQueue<LedgerException>();
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES)
                .onBackgroundFailure(failures::add).build()) {
            for (var i = 0; i < values.length; i++) {
                values[i] = new byte[2_500 + random.nextInt(9_500)];
                random.nextBytes(values[i]);
                ledger.commit(ledger.newBatch().put(MESSAGES, key(i), values[i]));
            }
            ledger.awaitDurable();
            assertThrows(LedgerIoException.class, ledger::checkpoint);
            failing.set(false);
            ledger.checkpoint();
            for (var i = 0; i < values.length; i++) {
                assertArrayEquals(values[i], ledger.get(MESSAGES, key(i)).orElseThrow(), key(i));
            }
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).build()) {
            assertEquals(values.length, ledger.size(MESSAGES));
            for (var i = 0; i < values.length; i++) {
                assertArrayEquals(values[i], ledger.get(MESSAGES, key(i)).orElseThrow(), key(i));
            }
            assertEquals(Optional.empty(), ledger.get(MESSAGES, key(values.length)));
        }
    }

    private static String key(int index) {
        return "m%05d".formatted(index);
    }

    /// {@return a pool whose free arrays, of every size a value unit seals into, hold bytes of an earlier use}
    private static HeapPool dirtyPool() {
        var pool = new HeapPool(64 << 20, false);
        // The overflow kept through collections, as the free arrays are looked for in it
        pool.holdOverflowForTests(true);
        var arrays = new ArrayList<byte[]>();
        for (var shift = 12; shift <= 20; shift++) {
            for (var i = 0; i < 8; i++) {
                var array = pool.acquire(1 << shift);
                Arrays.fill(array, DIRT);
                arrays.add(array);
            }
        }
        arrays.forEach(pool::release);
        return pool;
    }

    /// Takes every free array out of `pool`, failing if one is there twice.
    private static void assertNoArrayTwice(HeapPool pool) {
        Set<byte[]> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var shift = 12; shift <= 20; shift++) {
            while (true) {
                var before = pool.pooledBytesForTests();
                var array = pool.acquire(1 << shift);
                if (pool.pooledBytesForTests() == before) {
                    break;
                }
                assertNotNull(array);
                assertTrue(seen.add(array), "an array of " + array.length + " bytes was in the pool twice");
            }
        }
        assertEquals(0, pool.pooledBytesForTests());
    }
}
