package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The values too large for a leaf that a batch of many puts in a paged key space, which go to the file of large
/// values together, once, where a batch of a few keeps them in the log: their routing, their space, their recovery,
/// the shares their threads write and how a failed share fails the commit. A store without encryption lets the tests
/// find where a value's bytes went.
class BulkValuesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.bytes());
    // Values of a batch that takes the four threads of a write: 64 of 40,000 bytes, 11 pages each
    private static final int SHARED_VALUES = 64;
    private static final int SHARED_LENGTH = 40_000;

    private static Ledger.Options options(Path directory, boolean encrypted) {
        return Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS);
    }

    private static LiveLedger open(Path directory, boolean encrypted) {
        return (LiveLedger) options(directory, encrypted).build();
    }

    private static byte[] value(long seed, int length) {
        var value = new byte[length];
        new Random(seed).nextBytes(value);
        return value;
    }

    /// {@return a batch putting `count` values of `length` bytes in `keyspace`, under `prefix` and a number, the
    /// value of number `i` being `value(seed + i, length)`}
    private static WriteBatch batch(Ledger ledger, Keyspace<String, byte[]> keyspace, String prefix, long seed, int count, int length) {
        var batch = ledger.newBatch();
        for (var i = 0; i < count; i++) {
            batch.put(keyspace, key(prefix, i), value(seed + i, length));
        }
        return batch;
    }

    private static String key(String prefix, int i) {
        return prefix + "%04d".formatted(i);
    }

    private static void assertValues(Ledger ledger, Keyspace<String, byte[]> keyspace, String prefix, long seed, int count, int length) {
        for (var i = 0; i < count; i++) {
            assertArrayEquals(value(seed + i, length), ledger.get(keyspace, key(prefix, i)).orElseThrow(), key(prefix, i));
        }
    }

    /// {@return whether a file under `directory` whose name holds `name` holds the first 64 bytes of `value`}
    private static boolean holds(Path directory, String name, byte[] value) throws IOException {
        var pattern = Arrays.copyOf(value, 64);
        try (var files = Files.walk(directory)) {
            for (var file : files.filter(Files::isRegularFile).filter(path -> path.toString().contains(name)).toList()) {
                var bytes = Files.readAllBytes(file);
                for (var i = 0; i + pattern.length <= bytes.length; i++) {
                    if (Arrays.equals(bytes, i, i + pattern.length, pattern, 0, pattern.length)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /// Overwrites the last page of the file of large values with zeros: the end of the last unit written.
    private static void damageLastPage(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory.resolve(BlobFile.FILE), StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.allocate(PageFormat.SIZE), channel.size() - PageFormat.SIZE);
            channel.force(true);
        }
    }

    private static boolean blobFile(Path path) {
        return path.getFileName() != null && path.getFileName().toString().equals(BlobFile.FILE);
    }

    /// Commits a value, then `batches` batches of many that take several threads each, the odd ones awaited durable,
    /// until the store stops, which a stopped file system makes it do.
    ///
    /// @return how many of the batches it knows durable, and how many it made visible
    private static int[] runBatches(Path directory, int batches) {
        LiveLedger ledger = null;
        var durable = 0;
        var visible = 0;
        try {
            ledger = open(directory, true);
            ledger.commit(ledger.newBatch().put(MESSAGES, "first", value(1, 100))).awaitDurable();
            for (var batch = 0; batch < batches; batch++) {
                var commit = ledger.commit(batch(ledger, MESSAGES, "b" + batch + "-", 1_000L * (batch + 1), SHARED_VALUES, SHARED_LENGTH));
                visible++;
                if (batch % 2 == 1) {
                    commit.awaitDurable();
                    durable = visible;
                }
            }
            ledger.close();
            durable = visible;
        } catch (RuntimeException | InterruptedException _) {
            if (ledger != null) {
                try {
                    ledger.close();
                } catch (RuntimeException _) {
                    // The file system stopped: closing only lets go of the threads
                }
            }
        }
        return new int[]{durable, visible};
    }

    @Nested
    @DisplayName("routing")
    class Routing {
        @Test
        @DisplayName("write the values of a batch of many once, to the file of large values: neither the log nor a checkpoint copies them")
        void once(@TempDir Path directory) throws Exception {
            try (var ledger = (LiveLedger) options(directory, false).largeValues(64 << 10).build()) {
                ledger.commit(batch(ledger, MESSAGES, "m", 1, 40, 5_000)).awaitDurable();
                ledger.checkpoint();
                assertEquals(0, ledger.blobsForTests().unadoptedForTests(), "units in flight after the checkpoint");
                assertEquals(0, ledger.inFlightForTests(), "versions in flight after the checkpoint");
            }
            for (var i = 0; i < 40; i += 13) {
                var value = value(1 + i, 5_000);
                assertTrue(holds(directory, BlobFile.FILE, value), "the file of large values does not hold value " + i);
                assertFalse(holds(directory, ".log", value), "the log holds value " + i);
                assertFalse(holds(directory, PageFile.FILE, value), "the data file holds value " + i);
            }
            try (var ledger = open(directory, false)) {
                assertValues(ledger, MESSAGES, "m", 1, 40, 5_000);
            }
            CheckpointTest.verifyPages(directory, false);
        }

        @Test
        @DisplayName("keep in the log the values of a batch of a few, of one below the threshold together, of a resident key space and those that fit a leaf")
        void kept(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory, false).largeValues(128 << 10).build()) {
                // 15 values, in 15 units of 12,288 bytes
                ledger.commit(batch(ledger, MESSAGES, "few", 10, 15, 8_200)).awaitDurable();
                // 16 values, in 65,536 bytes of units
                ledger.commit(batch(ledger, MESSAGES, "small", 30, 16, 3_000)).awaitDurable();
                ledger.commit(batch(ledger, CHATS, "resident", 50, 40, 5_000)).awaitDurable();
                // 200 values that fit a leaf, of 300,000 bytes together
                ledger.commit(batch(ledger, MESSAGES, "inline", 100, 200, 1_500)).awaitDurable();
            }
            assertFalse(holds(directory, BlobFile.FILE, value(10, 8_200)));
            assertFalse(holds(directory, BlobFile.FILE, value(30, 3_000)));
            assertFalse(holds(directory, BlobFile.FILE, value(50, 5_000)));
            assertFalse(holds(directory, BlobFile.FILE, value(100, 1_500)));
            try (var ledger = open(directory, false)) {
                assertValues(ledger, MESSAGES, "few", 10, 15, 8_200);
                assertValues(ledger, MESSAGES, "small", 30, 16, 3_000);
                assertValues(ledger, CHATS, "resident", 50, 40, 5_000);
                assertValues(ledger, MESSAGES, "inline", 100, 200, 1_500);
            }
        }

        @Test
        @DisplayName("hold a batch against the threshold by the whole pages of its units, which the force writes, not by the bytes of its values")
        void unitBytes(@TempDir Path directory) throws Exception {
            // 16 values of 48,000 bytes together, in 65,536 bytes of units
            var at = directory.resolve("at");
            try (var ledger = options(at, false).largeValues(64 << 10).build()) {
                ledger.commit(batch(ledger, MESSAGES, "m", 1, 16, 3_000)).awaitDurable();
            }
            assertTrue(holds(at, BlobFile.FILE, value(1, 3_000)));
            var above = directory.resolve("above");
            try (var ledger = options(above, false).largeValues((64 << 10) + 1).build()) {
                ledger.commit(batch(ledger, MESSAGES, "m", 1, 16, 3_000)).awaitDurable();
            }
            assertFalse(holds(above, BlobFile.FILE, value(1, 3_000)));
        }

        @Test
        @DisplayName("keep in the log the values of a key space whose threshold writes every value there")
        void unlimited(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory, false).largeValues(64 << 10).largeValues(MESSAGES, Long.MAX_VALUE).build()) {
                ledger.commit(batch(ledger, MESSAGES, "m", 1, 40, 5_000)).awaitDurable();
            }
            assertFalse(holds(directory, BlobFile.FILE, value(1, 5_000)));
        }

        @Test
        @DisplayName("write the values of a batch of many with its large values, one unit each")
        void mixed(@TempDir Path directory) throws Exception {
            try (var ledger = options(directory, true).largeValues(64 << 10).build()) {
                var batch = batch(ledger, MESSAGES, "m", 1, 40, 5_000);
                batch.put(MESSAGES, "large", value(99, 100_000));
                batch.put(MESSAGES, "inline", value(98, 100));
                batch.put(CHATS, "chat", value(97, 5_000));
                ledger.commit(batch).awaitDurable();
                ledger.checkpoint();
                ((LiveLedger) ledger).clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "m", 1, 40, 5_000);
                assertArrayEquals(value(99, 100_000), ledger.get(MESSAGES, "large").orElseThrow());
            }
            try (var ledger = open(directory, true)) {
                assertValues(ledger, MESSAGES, "m", 1, 40, 5_000);
                assertArrayEquals(value(99, 100_000), ledger.get(MESSAGES, "large").orElseThrow());
                assertArrayEquals(value(98, 100), ledger.get(MESSAGES, "inline").orElseThrow());
                assertArrayEquals(value(97, 5_000), ledger.get(CHATS, "chat").orElseThrow());
            }
            CheckpointTest.verifyPages(directory, true);
        }
    }

    @Nested
    @DisplayName("space")
    class Space {
        @Test
        @DisplayName("free at the checkpoint the unit of a key a batch puts twice")
        void sameKeyTwice(@TempDir Path directory) throws Exception {
            var last = value(77, 6_000);
            try (var ledger = open(directory, true)) {
                var batch = batch(ledger, MESSAGES, "m", 1, SHARED_VALUES, SHARED_LENGTH);
                batch.put(MESSAGES, "dup", value(76, 9_000));
                batch.put(MESSAGES, "dup", last);
                ledger.commit(batch).awaitDurable();
                assertArrayEquals(last, ledger.get(MESSAGES, "dup").orElseThrow());
                ledger.checkpoint();
                assertEquals(0, ledger.blobsForTests().unadoptedForTests(), "units in flight after the checkpoint");
                assertEquals(0, ledger.inFlightForTests(), "versions in flight after the checkpoint");
            }
            // Every page of the file of large values is a tree's or free: the first unit of the key went back
            CheckpointTest.verifyPages(directory, true);
            try (var ledger = open(directory, true)) {
                assertArrayEquals(last, ledger.get(MESSAGES, "dup").orElseThrow());
            }
        }

        @Test
        @DisplayName("take the pages removals freed among the units of an earlier batch, too short for a whole batch, before the file grows")
        void freedRuns(@TempDir Path directory) throws Exception {
            int pages;
            try (var ledger = open(directory, true)) {
                ledger.commit(batch(ledger, MESSAGES, "a", 1, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                ledger.checkpoint();
                pages = ledger.blobsForTests().pageCount();
                var removal = ledger.newBatch();
                for (var i = 0; i < SHARED_VALUES; i += 2) {
                    removal.delete(MESSAGES, key("a", i));
                }
                ledger.commit(removal).awaitDurable();
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "tick", value(2, 100))).awaitDurable();
                ledger.commit(batch(ledger, MESSAGES, "b", 500, SHARED_VALUES / 2, SHARED_LENGTH)).awaitDurable();
                assertEquals(pages, ledger.blobsForTests().pageCount(), "the file grew beside the runs freed");
                ledger.clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "b", 500, SHARED_VALUES / 2, SHARED_LENGTH);
                ledger.checkpoint();
            }
            CheckpointTest.verifyPages(directory, true);
            try (var ledger = open(directory, true)) {
                assertValues(ledger, MESSAGES, "b", 500, SHARED_VALUES / 2, SHARED_LENGTH);
                for (var i = 1; i < SHARED_VALUES; i += 2) {
                    assertArrayEquals(value(1 + i, SHARED_LENGTH), ledger.get(MESSAGES, key("a", i)).orElseThrow(), key("a", i));
                }
            }
        }

        @Test
        @DisplayName("give every unit of a batch free pages or pages past the file, which a later batch of one size does not take while runs that hold it are left")
        void allocation() {
            var random = new Random(23);
            for (var round = 0; round < 2_000; round++) {
                var pageCount = PageFormat.FIRST_UNIT_PAGE + random.nextInt(200);
                var free = new BitSet();
                var density = 1 + random.nextInt(9);
                for (var page = PageFormat.FIRST_UNIT_PAGE; page < pageCount; page++) {
                    if (random.nextInt(10) < density) {
                        free.set(page);
                    }
                }
                var before = (BitSet) free.clone();
                var space = new FreeSpace(pageCount, free);
                var sameSize = random.nextBoolean();
                var size = 1 + random.nextInt(6);
                var sizes = new int[1 + random.nextInt(30)];
                var total = 0;
                for (var i = 0; i < sizes.length; i++) {
                    sizes[i] = sameSize ? size : 1 + random.nextInt(6);
                    total += sizes[i];
                }
                var firsts = space.allocateAll(sizes, total);
                var taken = new BitSet();
                var pastEnd = false;
                for (var i = 0; i < sizes.length; i++) {
                    for (var page = firsts[i]; page < firsts[i] + sizes[i]; page++) {
                        assertTrue(page >= pageCount || before.get(page), "round " + round + ": page " + page + " was not free");
                        assertFalse(taken.get(page), "round " + round + ": page " + page + " taken twice");
                        assertFalse(space.isUnused(page), "round " + round + ": page " + page + " still free");
                        taken.set(page);
                    }
                    pastEnd |= firsts[i] >= pageCount;
                }
                if (pastEnd && sameSize) {
                    // Every run of the file that held one more unit went to one
                    var start = before.nextSetBit(0);
                    var grown = space.pageCount();
                    for (var page = PageFormat.FIRST_UNIT_PAGE; page < pageCount; page++) {
                        if (taken.get(page)) {
                            before.clear(page);
                        }
                    }
                    start = before.nextSetBit(0);
                    while (start >= 0 && start < pageCount) {
                        var end = before.nextClearBit(start);
                        if (end < pageCount) {
                            assertTrue(end - start < size, "round " + round + ": the file grew to " + grown + " beside a free run of " + (end - start));
                        }
                        start = before.nextSetBit(end);
                    }
                }
            }
        }

        @Test
        @DisplayName("free at the checkpoint the units of the values range removals took before it, in flight between batches of many")
        void rangeRemovals(@TempDir Path directory) throws Exception {
            for (var encrypted : new boolean[]{false, true}) {
                var store = directory.resolve(encrypted ? "encrypted" : "plain");
                try (var ledger = open(store, encrypted)) {
                    ledger.commit(batch(ledger, MESSAGES, "a", 1, SHARED_VALUES, SHARED_LENGTH));
                    ledger.commit(batch(ledger, MESSAGES, "b", 100, SHARED_VALUES, SHARED_LENGTH));
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key("a", 10), key("a", 30))));
                    // Added after a lookup of a range, and merged with what it ordered at the next one
                    ledger.commit(batch(ledger, MESSAGES, "a", 1_000, 30, SHARED_LENGTH));
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key("a", 5), key("a", 15))));
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key("b", 60), "c")));
                    // Values of a key both a removal and a later put replaced
                    ledger.commit(batch(ledger, MESSAGES, "b", 2_000, 30, SHARED_LENGTH));
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between(key("b", 0), key("b", 10))));
                    assertEquals(SHARED_VALUES * 2 + 30 + 30, ledger.inFlightForTests());
                    ledger.checkpoint();
                    assertEquals(0, ledger.blobsForTests().unadoptedForTests(), "units in flight after the checkpoint");
                    assertEquals(0, ledger.inFlightForTests(), "versions in flight after the checkpoint");
                }
                CheckpointTest.verifyPages(store, encrypted);
                try (var ledger = open(store, encrypted)) {
                    for (var i = 0; i < SHARED_VALUES; i++) {
                        var value = ledger.get(MESSAGES, key("a", i));
                        if (i < 5) {
                            assertArrayEquals(value(1_000 + i, SHARED_LENGTH), value.orElseThrow(), key("a", i));
                        } else if (i < 15) {
                            assertEquals(Optional.empty(), value, key("a", i));
                        } else if (i < 30) {
                            assertArrayEquals(value(1_000 + i, SHARED_LENGTH), value.orElseThrow(), key("a", i));
                        } else {
                            assertArrayEquals(value(1 + i, SHARED_LENGTH), value.orElseThrow(), key("a", i));
                        }
                    }
                    for (var i = 0; i < SHARED_VALUES; i++) {
                        var value = ledger.get(MESSAGES, key("b", i));
                        if (i < 10) {
                            assertEquals(Optional.empty(), value, key("b", i));
                        } else if (i < 30) {
                            assertArrayEquals(value(2_000 + i, SHARED_LENGTH), value.orElseThrow(), key("b", i));
                        } else if (i < 60) {
                            assertArrayEquals(value(100 + i, SHARED_LENGTH), value.orElseThrow(), key("b", i));
                        } else {
                            assertEquals(Optional.empty(), value, key("b", i));
                        }
                    }
                    assertEquals(5 + 15 + 34 + 20 + 30, ledger.size(MESSAGES));
                }
            }
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        @Test
        @DisplayName("reads back the values of the batches it replays, and takes their pages again")
        void replays(@TempDir Path directory) throws Exception {
            var ledger = open(directory, true);
            ledger.commit(batch(ledger, MESSAGES, "c", 1, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
            ledger.checkpoint();
            ledger.commit(batch(ledger, MESSAGES, "r", 100, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
            ledger.commit(batch(ledger, MESSAGES, "s", 200, 40, 5_000)).awaitDurable();
            ledger.closeWithoutCheckpoint();
            try (var reopened = open(directory, true)) {
                assertValues(reopened, MESSAGES, "r", 100, SHARED_VALUES, SHARED_LENGTH);
                assertValues(reopened, MESSAGES, "s", 200, 40, 5_000);
                // The claimed pages are not handed to new values
                reopened.commit(batch(reopened, MESSAGES, "n", 300, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                reopened.clearPageCacheForTests();
                assertValues(reopened, MESSAGES, "c", 1, SHARED_VALUES, SHARED_LENGTH);
                assertValues(reopened, MESSAGES, "r", 100, SHARED_VALUES, SHARED_LENGTH);
                assertValues(reopened, MESSAGES, "n", 300, SHARED_VALUES, SHARED_LENGTH);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("ends the log at a last batch one of whose units the file does not hold whole, as at a torn frame")
        void tornUnit(@TempDir Path directory) throws Exception {
            var ledger = open(directory, false);
            ledger.commit(ledger.newBatch().put(MESSAGES, "kept", "before".getBytes())).awaitDurable();
            ledger.commit(batch(ledger, MESSAGES, "t", 1, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
            ledger.closeWithoutCheckpoint();
            damageLastPage(directory);
            try (var reopened = open(directory, false)) {
                assertArrayEquals("before".getBytes(), reopened.get(MESSAGES, "kept").orElseThrow());
                for (var i = 0; i < SHARED_VALUES; i++) {
                    assertEquals(Optional.empty(), reopened.get(MESSAGES, key("t", i)));
                }
                assertEquals(1, reopened.size(MESSAGES));
                reopened.commit(batch(reopened, MESSAGES, "u", 500, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
            }
            try (var reopened = open(directory, false)) {
                assertValues(reopened, MESSAGES, "u", 500, SHARED_VALUES, SHARED_LENGTH);
            }
            CheckpointTest.verifyPages(directory, false);
        }

        @Test
        @DisplayName("refuses a log whose batch lacks a unit a later group follows: that batch was acknowledged")
        void damagedUnit(@TempDir Path directory) throws Exception {
            var ledger = open(directory, false);
            ledger.commit(batch(ledger, MESSAGES, "d", 1, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
            ledger.commit(ledger.newBatch().put(MESSAGES, "later", "after".getBytes())).awaitDurable();
            ledger.closeWithoutCheckpoint();
            damageLastPage(directory);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, false));
        }
    }

    @Nested
    @DisplayName("threads")
    class Threads {
        @Test
        @DisplayName("a failed share fails the batch once every share is done, frees every unit, and the store goes on")
        void failedShare() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var failed = new AtomicBoolean();
            var delayed = new AtomicReference<Thread>();
            var delayDone = new AtomicBoolean();
            disk.intercept((path, kind) -> {
                if (!armed.get() || kind != ChangeInterceptor.ChangeKind.WRITE || !blobFile(path)
                        || !Thread.currentThread().getName().equals(BlobFile.WRITER_NAME)) {
                    return;
                }
                if (failed.compareAndSet(false, true)) {
                    throw new IOException("No space left on device");
                }
                if (delayed.compareAndSet(null, Thread.currentThread())) {
                    // A share still writing when another failed: its bytes land after the failure
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException exception) {
                        throw new IOException(exception);
                    }
                    delayDone.set(true);
                }
            });
            var directory = disk.getPath("/store");
            try (var ledger = open(directory, true)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "first", value(1, 100))).awaitDurable();
                armed.set(true);
                assertThrows(LedgerIoException.class, () -> ledger.commit(batch(ledger, MESSAGES, "f", 10, SHARED_VALUES, SHARED_LENGTH)));
                armed.set(false);
                assertTrue(failed.get(), "no share failed");
                assertTrue(delayed.get() != null, "no other share wrote");
                assertTrue(delayDone.get(), "the commit failed before every share was done");
                assertEquals(0, ledger.blobsForTests().unadoptedForTests(), "units of the failed batch in flight");
                assertEquals(0, ledger.inFlightForTests());
                assertEquals(1, ledger.size(MESSAGES));
                assertEquals(Optional.empty(), ledger.get(MESSAGES, key("f", 0)));
                // The next batch takes the pages the failed one gave back, which a late write of it would break
                ledger.commit(batch(ledger, MESSAGES, "g", 100, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                ledger.clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "g", 100, SHARED_VALUES, SHARED_LENGTH);
                ledger.checkpoint();
                ledger.clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "g", 100, SHARED_VALUES, SHARED_LENGTH);
            }
            try (var ledger = open(directory, true)) {
                assertValues(ledger, MESSAGES, "g", 100, SHARED_VALUES, SHARED_LENGTH);
                assertEquals(SHARED_VALUES + 1, ledger.size(MESSAGES));
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("an interrupted committer writes its share of a batch of many, and gets its interrupt back")
        void interruptedCommitter(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, true)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "first", value(1, 100))).awaitDurable();
                // An interrupted thread's file operations close their channel at once
                Thread.currentThread().interrupt();
                var commit = ledger.commit(batch(ledger, MESSAGES, "i", 10, SHARED_VALUES, SHARED_LENGTH));
                assertTrue(Thread.interrupted(), "the committer lost its interrupt");
                commit.awaitDurable();
                ledger.clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "i", 10, SHARED_VALUES, SHARED_LENGTH);
            }
            try (var ledger = open(directory, true)) {
                assertValues(ledger, MESSAGES, "i", 10, SHARED_VALUES, SHARED_LENGTH);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("batches of many written by several threads at once, beside readers and checkpoints, keep every value")
        void concurrentBatches(@TempDir Path directory) throws Exception {
            var writers = 4;
            var rounds = 3;
            var committed = new ConcurrentLinkedQueue<String>();
            var failures = new ConcurrentLinkedQueue<Throwable>();
            try (var ledger = open(directory, true)) {
                var start = new CountDownLatch(1);
                var done = new AtomicBoolean();
                var threads = new ArrayList<Thread>();
                for (var writer = 0; writer < writers; writer++) {
                    var id = writer;
                    threads.add(Thread.ofPlatform().start(() -> {
                        try {
                            start.await();
                            for (var round = 0; round < rounds; round++) {
                                var prefix = "w" + id + "r" + round + "-";
                                ledger.commit(batch(ledger, MESSAGES, prefix, seed(id, round), SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                                committed.add(prefix);
                            }
                        } catch (Throwable failure) {
                            failures.add(failure);
                        }
                    }));
                }
                var reader = Thread.ofPlatform().start(() -> {
                    var random = new Random(5);
                    try {
                        start.await();
                        while (!done.get()) {
                            var prefixes = new ArrayList<>(committed);
                            if (prefixes.isEmpty()) {
                                Thread.onSpinWait();
                                continue;
                            }
                            var prefix = prefixes.get(random.nextInt(prefixes.size()));
                            var i = random.nextInt(SHARED_VALUES);
                            var parts = prefix.substring(1, prefix.length() - 1).split("r");
                            var expected = value(seed(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])) + i, SHARED_LENGTH);
                            assertArrayEquals(expected, ledger.get(MESSAGES, key(prefix, i)).orElseThrow(), key(prefix, i));
                            if (random.nextInt(8) == 0) {
                                ledger.clearPageCacheForTests();
                            }
                        }
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                });
                var checkpoints = Thread.ofPlatform().start(() -> {
                    try {
                        start.await();
                        while (!done.get()) {
                            ledger.checkpoint();
                            Thread.sleep(20);
                        }
                    } catch (Throwable failure) {
                        failures.add(failure);
                    }
                });
                start.countDown();
                for (var thread : threads) {
                    thread.join();
                }
                done.set(true);
                reader.join();
                checkpoints.join();
                assertTrue(failures.isEmpty(), () -> "failed: " + failures.peek());
                assertEquals(writers * rounds * SHARED_VALUES, ledger.size(MESSAGES));
                ledger.checkpoint();
                assertEquals(0, ledger.blobsForTests().unadoptedForTests());
            }
            try (var ledger = open(directory, true)) {
                for (var writer = 0; writer < writers; writer++) {
                    for (var round = 0; round < rounds; round++) {
                        assertValues(ledger, MESSAGES, "w" + writer + "r" + round + "-", seed(writer, round), SHARED_VALUES, SHARED_LENGTH);
                    }
                }
            }
            CheckpointTest.verifyPages(directory, true);
        }

        private static long seed(int writer, int round) {
            return 10_000L * (writer * 10 + round + 1);
        }

        @ParameterizedTest(name = "seed {0}")
        @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23})
        @DisplayName("a stop anywhere among the writes of the threads, then a power loss, leaves whole batches, every one acknowledged durable among them")
        void stopped(int seed) throws Exception {
            var batches = 4;
            var dry = CrashFileSystem.create();
            runBatches(dry.getPath("/store"), batches);
            var random = new Random(seed * 131L + 7);
            var disk = CrashFileSystem.create();
            if (random.nextBoolean()) {
                disk.stopAfter(random.nextLong(dry.changes() + 1));
            } else {
                disk.stopAtForce(random.nextLong(dry.forces() + 1));
            }
            var counts = runBatches(disk.getPath("/store"), batches);
            var survivor = seed % 3 == 0 ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
            var directory = survivor.getPath("/store");
            try (var ledger = open(directory, true)) {
                var whole = 0;
                for (var batch = 0; batch < batches; batch++) {
                    var prefix = "b" + batch + "-";
                    var present = 0;
                    for (var i = 0; i < SHARED_VALUES; i++) {
                        var value = ledger.get(MESSAGES, key(prefix, i));
                        if (value.isPresent()) {
                            assertArrayEquals(value(1_000L * (batch + 1) + i, SHARED_LENGTH), value.get(), key(prefix, i));
                            present++;
                        }
                    }
                    assertTrue(present == 0 || present == SHARED_VALUES, "batch " + batch + " holds " + present + " values");
                    if (present == SHARED_VALUES) {
                        assertEquals(batch, whole, "batch " + batch + " recovered after one that was not");
                        whole++;
                    }
                }
                assertTrue(whole >= counts[0], "only " + whole + " batches recovered of the " + counts[0] + " acknowledged durable");
                // The store takes batches of many in the pages recovery left free
                ledger.commit(batch(ledger, MESSAGES, "after", 90_000, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                ledger.checkpoint();
                ledger.clearPageCacheForTests();
                assertValues(ledger, MESSAGES, "after", 90_000, SHARED_VALUES, SHARED_LENGTH);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("a close waits for a batch of many one of whose threads is writing, which applies, and a store opened after it loses no value of either")
        void closeWaits() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && blobFile(path)
                        && Thread.currentThread().getName().equals(BlobFile.WRITER_NAME) && armed.compareAndSet(true, false)) {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException exception) {
                        throw new IOException(exception);
                    }
                }
            });
            var directory = disk.getPath("/store");
            var first = open(directory, true);
            first.commit(first.newBatch().put(MESSAGES, "first", value(1, 100))).awaitDurable();
            armed.set(true);
            var failure = new AtomicReference<Throwable>();
            var committer = Thread.ofPlatform().start(() -> {
                try {
                    first.commit(batch(first, MESSAGES, "x", 1_000, SHARED_VALUES, SHARED_LENGTH));
                } catch (Throwable throwable) {
                    failure.set(throwable);
                }
            });
            entered.await();
            var closed = new CountDownLatch(1);
            var closer = Thread.ofPlatform().start(() -> {
                first.close();
                closed.countDown();
            });
            try {
                assertFalse(closed.await(500, TimeUnit.MILLISECONDS), "the close did not wait for the commit");
            } finally {
                release.countDown();
                committer.join();
                closer.join();
            }
            assertNull(failure.get(), "the commit a close waited for failed");
            assertThrows(IllegalStateException.class, first.blobsForTests()::enterCommit);
            try (var second = open(directory, true)) {
                assertValues(second, MESSAGES, "x", 1_000, SHARED_VALUES, SHARED_LENGTH);
                second.commit(batch(second, MESSAGES, "y", 5_000, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                second.clearPageCacheForTests();
                second.checkpoint();
                second.clearPageCacheForTests();
                assertValues(second, MESSAGES, "x", 1_000, SHARED_VALUES, SHARED_LENGTH);
                assertValues(second, MESSAGES, "y", 5_000, SHARED_VALUES, SHARED_LENGTH);
            }
            CheckpointTest.verifyPages(directory, true);
        }

        @Test
        @DisplayName("a read of a unit past a close is refused, as one of the data file is, and opens no channel nobody would close")
        void readPastClose(@TempDir Path directory) throws Exception {
            var ledger = open(directory, true);
            var blobs = ledger.blobsForTests();
            var unit = blobs.write(MemorySegment.ofArray(value(1, 5_000)));
            assertTrue(blobs.readUnit(unit) != null);
            ledger.close();
            assertThrows(IllegalStateException.class, () -> blobs.readUnit(unit));
        }

        @Test
        @DisplayName("a read of a unit goes on while every thread of a batch of many waits for the device")
        void readBesideBatch() throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (armed.get() && kind == ChangeInterceptor.ChangeKind.WRITE && blobFile(path)) {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException exception) {
                        throw new IOException(exception);
                    }
                }
            });
            try (var ledger = open(disk.getPath("/store"), true)) {
                ledger.commit(batch(ledger, MESSAGES, "a", 1, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                ledger.clearPageCacheForTests();
                armed.set(true);
                var failure = new AtomicReference<Throwable>();
                var writer = Thread.ofPlatform().start(() -> {
                    try {
                        ledger.commit(batch(ledger, MESSAGES, "b", 500, SHARED_VALUES, SHARED_LENGTH)).awaitDurable();
                    } catch (Throwable throwable) {
                        failure.set(throwable);
                    }
                });
                try {
                    entered.await();
                    assertTimeoutPreemptively(Duration.ofSeconds(60), () -> assertValues(ledger, MESSAGES, "a", 1, SHARED_VALUES, SHARED_LENGTH));
                } finally {
                    armed.set(false);
                    release.countDown();
                    writer.join();
                }
                assertEquals(null, failure.get());
                assertValues(ledger, MESSAGES, "b", 500, SHARED_VALUES, SHARED_LENGTH);
            }
        }
    }

    @Nested
    @DisplayName("the log's queue")
    class Queue {
        // 40 values of 16,000 bytes: 640 KiB of units, so two batches pass the limit and frames of keys never do
        private static final int QUEUED_VALUES = 40;
        private static final int QUEUED_LENGTH = 16_000;
        private static final long LIMIT = 1 << 20;

        private static LiveLedger limited(Path directory) {
            return (LiveLedger) ((LedgerBuilder) options(directory, true).largeValues(64 << 10)).queueLimit(LIMIT).build();
        }

        /// {@return a batch of [#QUEUED_VALUES] values, which a range removal makes commit alone, through the log's
        /// other way in}
        private static WriteBatch queued(Ledger ledger, boolean alone, String prefix, long seed) {
            var batch = batch(ledger, MESSAGES, prefix, seed, QUEUED_VALUES, QUEUED_LENGTH);
            if (alone) {
                batch.deleteRange(MESSAGES, KeyRange.between("zz0", "zz1"));
            }
            return batch;
        }

        @ParameterizedTest(name = "alone {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("takes off a durable group the bytes of its units, so batches that wrote many times the limit in all never wait for good")
        void drains(boolean alone, @TempDir Path directory) throws Exception {
            // Not closed on a failure: a close waits for the commit that would be stuck
            var ledger = limited(directory);
            assertTimeoutPreemptively(Duration.ofSeconds(120), () -> {
                var last = ledger.commit(queued(ledger, alone, "a0-", 1));
                for (var i = 1; i < 8; i++) {
                    last = ledger.commit(queued(ledger, alone, "a" + i + "-", 1 + i * 1_000L));
                }
                last.awaitDurable();
            });
            for (var i = 0; i < 8; i++) {
                assertValues(ledger, MESSAGES, "a" + i + "-", 1 + i * 1_000L, QUEUED_VALUES, QUEUED_LENGTH);
            }
            ledger.close();
        }

        @ParameterizedTest(name = "alone {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("holds back a batch whose units pass the limit with those of a group the device has not made durable")
        void waits(boolean alone) throws Exception {
            var disk = CrashFileSystem.create();
            var armed = new AtomicBoolean();
            var entered = new CountDownLatch(1);
            var release = new CountDownLatch(1);
            disk.intercept((_, kind) -> {
                if (armed.get() && kind == ChangeInterceptor.ChangeKind.FORCE) {
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException exception) {
                        throw new IOException(exception);
                    }
                }
            });
            // A queue that never drains fails here, rather than hanging the run
            assertTimeoutPreemptively(Duration.ofSeconds(120), () -> {
                try (var ledger = limited(disk.getPath("/store"))) {
                    // Its unit makes the epoch durable, which needs the forces the test holds below
                    ledger.commit(queued(ledger, alone, "a", 1)).awaitDurable();
                    armed.set(true);
                    ledger.commit(queued(ledger, alone, "b", 1_000));
                    var failure = new AtomicReference<Throwable>();
                    var done = new CountDownLatch(1);
                    Thread second = null;
                    try {
                        assertTrue(entered.await(60, TimeUnit.SECONDS), "the group was not forced");
                        second = Thread.ofPlatform().start(() -> {
                            try {
                                ledger.commit(queued(ledger, alone, "c", 2_000));
                            } catch (Throwable throwable) {
                                failure.set(throwable);
                            }
                            done.countDown();
                        });
                        assertFalse(done.await(500, TimeUnit.MILLISECONDS), "the batch did not wait for the queue");
                    } finally {
                        armed.set(false);
                        release.countDown();
                        if (second != null) {
                            second.join();
                        }
                    }
                    assertNull(failure.get());
                    assertValues(ledger, MESSAGES, "c", 2_000, QUEUED_VALUES, QUEUED_LENGTH);
                }
            });
        }
    }

    @Nested
    @DisplayName("units sealed in place")
    class Sealing {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("are the units a copy of the header and content seals, and open")
        void sameUnits(boolean encrypted) {
            // Two, as a cipher refuses the nonce of its last seal
            var cipher = new PageCipher(encrypted ? new SecretKeySpec(value(1, 32), "AES") : null);
            var reference = new PageCipher(encrypted ? new SecretKeySpec(value(1, 32), "AES") : null);
            var random = new Random(3);
            var out = new byte[64 * PageFormat.SIZE];
            for (var round = 0; round < 200; round++) {
                var pages = 1 + random.nextInt(4);
                var length = random.nextInt(PageFormat.capacity(pages) + 1);
                var content = value(round, length);
                var header = new byte[PageFormat.HEADER];
                var page = 2 + random.nextInt(1_000);
                PageFile.header(header, PageFormat.EXTERNAL, 0, page, pages, length, random.nextLong(), random.nextInt());
                var plain = new byte[pages * PageFormat.SIZE];
                System.arraycopy(header, 0, plain, 0, PageFormat.HEADER);
                System.arraycopy(content, 0, plain, PageFormat.HEADER, length);
                var expected = new byte[pages * PageFormat.SIZE];
                reference.seal(plain, pages, expected, 0);
                // Over bytes a unit sealed before left there
                var offset = random.nextInt(out.length - pages * PageFormat.SIZE + 1);
                random.nextBytes(out);
                cipher.seal(header, MemorySegment.ofArray(content), pages, out, offset);
                assertArrayEquals(expected, Arrays.copyOfRange(out, offset, offset + pages * PageFormat.SIZE), "round " + round);
                var opened = new byte[pages * PageFormat.SIZE];
                assertTrue(cipher.open(out, offset, pages, opened), "round " + round);
                assertArrayEquals(content, Arrays.copyOfRange(opened, PageFormat.HEADER, PageFormat.HEADER + length));
            }
        }

        @Test
        @DisplayName("refuse content larger than the unit")
        void tooLarge() {
            var cipher = new PageCipher(null);
            var header = new byte[PageFormat.HEADER];
            assertThrows(IllegalArgumentException.class, () -> cipher.seal(header, MemorySegment.ofArray(new byte[PageFormat.capacity(1) + 1]), 1,
                    new byte[2 * PageFormat.SIZE], 0));
        }
    }

    @Nested
    @DisplayName("versions in flight")
    class InFlight {
        @Test
        @DisplayName("give a range removal every version of a key in its range, whatever additions and removals came between")
        void ranges() {
            var random = new Random(11);
            var units = new InFlightUnits();
            var all = new ArrayList<Object[]>();
            var stamp = 0L;
            for (var step = 0; step < 4_000; step++) {
                var choice = random.nextInt(10);
                if (choice < 6) {
                    var key = randomKey(random);
                    var chain = new Chain(MemorySegment.ofArray(key).asReadOnly(), Segments.hash(MemorySegment.ofArray(key)));
                    var version = new Version.External(++stamp, MemorySegment.ofArray(new byte[0]), 0, 0, null, null,
                            new PageRef((int) stamp, 1, stamp, 0));
                    units.add(chain, version);
                    all.add(new Object[]{key, version});
                } else if (choice < 9) {
                    var from = random.nextInt(5) == 0 ? null : randomKey(random);
                    var to = random.nextInt(5) == 0 ? null : randomKey(random);
                    var seen = new ArrayList<Version.External>();
                    units.forEachIn(from == null ? null : MemorySegment.ofArray(from), to == null ? null : MemorySegment.ofArray(to), seen::add);
                    var expected = Collections.newSetFromMap(new IdentityHashMap<Version.External, Boolean>());
                    for (var entry : all) {
                        var key = (byte[]) entry[0];
                        if ((from == null || Arrays.compareUnsigned(key, from) >= 0) && (to == null || Arrays.compareUnsigned(key, to) < 0)) {
                            expected.add((Version.External) entry[1]);
                        }
                    }
                    var visited = Collections.newSetFromMap(new IdentityHashMap<Version.External, Boolean>());
                    visited.addAll(seen);
                    assertEquals(seen.size(), visited.size(), "a version was given twice");
                    assertEquals(expected, visited, "step " + step);
                } else {
                    var below = random.nextLong(stamp + 1);
                    units.removeIf(version -> version.stamp < below && version.stamp % 3 != 0);
                    all.removeIf(entry -> ((Version.External) entry[1]).stamp < below && ((Version.External) entry[1]).stamp % 3 != 0);
                }
                assertEquals(all.size(), units.size());
                var every = new HashSet<Long>();
                units.forEach(version -> every.add(version.stamp));
                assertEquals(all.size(), every.size());
            }
        }

        private static byte[] randomKey(Random random) {
            var key = new byte[random.nextInt(4)];
            for (var i = 0; i < key.length; i++) {
                key[i] = (byte) (random.nextInt(4) * 85);
            }
            return key;
        }
    }
}
