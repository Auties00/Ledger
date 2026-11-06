package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.IoPool;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerKeyException;
import com.github.auties00.ledger.LedgerLockedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// A store in a directory: what survives a close, a crash, a torn log and a wrong key.
class DurableLedgerTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> OTHER = Keyspace.resident("other", KeyCodec.utf8(), ValueCodec.utf8());

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 7 + 1);
        }
    }

    private static Ledger open(Path directory, Keyspace<?, ?>... keyspaces) {
        return open(directory, Encryption.key(KEY), keyspaces);
    }

    private static Ledger open(Path directory, Encryption encryption, Keyspace<?, ?>... keyspaces) {
        var options = Ledger.builder().directory(directory).encryption(encryption);
        for (var keyspace : keyspaces) {
            options.keyspace(keyspace);
        }
        return options.build();
    }

    /// {@return the segments of the store in `directory`, by number}
    private static List<Path> segments(Path directory) throws IOException {
        try (var files = Files.list(directory.resolve("log"))) {
            return files.filter(file -> LogFiles.segmentNumber(file.getFileName().toString()) >= 0)
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        }
    }

    /// {@return every file of the log of the store in `directory`, segments, spares and preparations alike}
    private static List<Path> logFiles(Path directory) throws IOException {
        try (var files = Files.list(directory.resolve("log"))) {
            return files.toList();
        }
    }

    /// {@return the segment `ledger` writes now}
    private static Path writing(Path directory, Ledger ledger) {
        return directory.resolve("log").resolve(LogWriter.segmentName(((LiveLedger) ledger).log().currentSegment()));
    }

    @Nested
    @DisplayName("reopening")
    class Reopening {
        @Test
        @DisplayName("every commit survives a close, ranges and counts included, and sequences continue")
        void survivesClose(@TempDir Path directory) {
            long sequence;
            try (var ledger = open(directory, NAMES)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 100; i++) {
                    batch.put(NAMES, "k" + (100 + i), "v" + i);
                }
                ledger.commit(batch);
                ledger.commit(ledger.newBatch().deleteRange(NAMES, KeyRange.between("k120", "k150")).delete(NAMES, "k100"));
                sequence = ledger.commit(ledger.newBatch().put(NAMES, "k125", "back")).sequence();
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(70, ledger.size(NAMES));
                assertEquals(Optional.of("back"), ledger.get(NAMES, "k125"));
                assertEquals(Optional.empty(), ledger.get(NAMES, "k130"));
                assertEquals(Optional.empty(), ledger.get(NAMES, "k100"));
                assertEquals(Optional.of("v1"), ledger.get(NAMES, "k101"));
                assertEquals(sequence + 1, ledger.commit(ledger.newBatch().put(NAMES, "x", "y")).sequence());
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(71, ledger.size(NAMES));
            }
        }

        @Test
        @DisplayName("key spaces join on a later open, and those left out are kept for the next one")
        void keyspacesComeAndGo(@TempDir Path directory) {
            try (var ledger = open(directory, NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
            }
            try (var ledger = open(directory, NAMES, OTHER)) {
                assertEquals(0, ledger.size(OTHER));
                ledger.commit(ledger.newBatch().put(OTHER, "b", "2"));
            }
            try (var ledger = open(directory, OTHER)) {
                assertEquals(Optional.of("2"), ledger.get(OTHER, "b"));
                ledger.commit(ledger.newBatch().put(OTHER, "c", "3"));
            }
            try (var ledger = open(directory, NAMES, OTHER)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
                assertEquals(2, ledger.size(OTHER));
            }
        }

        @Test
        @DisplayName("a passphrase and no encryption reopen too")
        void otherEncryptions(@TempDir Path base) throws IOException {
            var passphrase = base.resolve("passphrase");
            try (var ledger = open(passphrase, Encryption.passphrase("correct horse".toCharArray()), NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
            }
            try (var ledger = open(passphrase, Encryption.passphrase("correct horse".toCharArray()), NAMES)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
            }
            var plain = base.resolve("plain");
            try (var ledger = open(plain, Encryption.none(), NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "PLAINTEXT-MARKER"));
            }
            try (var ledger = open(plain, Encryption.none(), NAMES)) {
                assertEquals(Optional.of("PLAINTEXT-MARKER"), ledger.get(NAMES, "a"));
            }
            assertTrue(contains(plain, "PLAINTEXT-MARKER"));
        }
    }

    @Nested
    @DisplayName("the log")
    class TheLog {
        @Test
        @DisplayName("commits roll over into new segments, a frame larger than a segment included, and replay across them")
        void rollover(@TempDir Path directory) throws Exception {
            var value = "v".repeat(300);
            var large = "L".repeat(40_000);
            var writing = small(directory);
            try {
                for (var i = 0; i < 100; i++) {
                    writing.commit(writing.newBatch().put(NAMES, "k%03d".formatted(i), value)).awaitDurable();
                }
                writing.commit(writing.newBatch().put(NAMES, "large", large)).awaitDurable();
                writing.commit(writing.newBatch().deleteRange(NAMES, KeyRange.between("k000", "k050"))).awaitDurable();
            } finally {
                crash(writing);
            }
            var count = segments(directory).size();
            assertTrue(count > 10, () -> count + " segments");
            try (var ledger = small(directory)) {
                assertEquals(51, ledger.size(NAMES));
                assertEquals(Optional.of(large), ledger.get(NAMES, "large"));
                assertEquals(Optional.of(value), ledger.get(NAMES, "k099"));
                assertEquals(Optional.empty(), ledger.get(NAMES, "k049"));
            }
        }

        @Test
        @DisplayName("a group that ends just short of a block boundary is followed by the next one")
        void shortPadding(@TempDir Path base) throws Exception {
            // The length of a group holding one commit, which grows by one byte per byte of value; groups start on
            // a block boundary, so the length alone places the group's end
            var probe = base.resolve("probe");
            var probeLength = loneCommit(probe, 300, false);
            var length = 300 + (int) (LogWriter.ALIGNMENT - 10 - probeLength % LogWriter.ALIGNMENT);
            var directory = base.resolve("store");
            var groupLength = loneCommit(directory, length, true);
            var tail = groupLength % LogWriter.ALIGNMENT;
            assertTrue(tail > LogWriter.ALIGNMENT - LogWriter.FRAME_HEADER, () -> "the group ends " + tail + " bytes into a block");
            try (var ledger = open(directory, NAMES)) {
                assertEquals(Optional.of("x".repeat(length)), ledger.get(NAMES, "lone"));
                assertEquals(Optional.of("next"), ledger.get(NAMES, "after"));
            }
        }

        @Test
        @DisplayName("a transaction is journaled when it returns and not at all when it throws")
        void transactions(@TempDir Path directory) {
            try (var ledger = open(directory, NAMES)) {
                ledger.update(transaction -> {
                    transaction.put(NAMES, "a", "1");
                    transaction.put(NAMES, "b", "2");
                });
                assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                    transaction.put(NAMES, "c", "3");
                    throw new IllegalStateException("abandoned");
                }));
                ledger.update(transaction -> transaction.delete(NAMES, "a"));
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(List.of("b"), ledger.range(NAMES, KeyRange.all(), 10).stream().map(com.github.auties00.ledger.Entry::key).toList());
                // The abandoned transaction took no sequence
                assertEquals(3, ledger.commit(ledger.newBatch().put(NAMES, "d", "4")).sequence());
            }
        }

        private static Ledger small(Path directory) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                    .segmentSize(16_384)
                    .build();
        }

        /// Writes one commit with a value of `length` bytes in a group of its own and returns the length of its frames,
        /// then, if `next`, commits once more into the same segment.
        private static long loneCommit(Path directory, int length, boolean next) throws Exception {
            open(directory, NAMES).close();
            var ledger = open(directory, NAMES);
            try {
                // Flushes the catalog frame the open queued, which would otherwise join the group or not by timing
                ledger.commit(ledger.newBatch().put(NAMES, "warm", "x")).awaitDurable();
                ledger.commit(ledger.newBatch().put(NAMES, "lone", "x".repeat(length))).awaitDurable();
                var log = ((LiveLedger) ledger).log();
                var size = log.groupEnd() - log.groupStart();
                if (next) {
                    ledger.commit(ledger.newBatch().put(NAMES, "after", "next")).awaitDurable();
                }
                return size;
            } finally {
                crash(ledger);
            }
        }
    }

    @Nested
    @DisplayName("protection")
    class Protection {
        @Test
        @DisplayName("an encrypted store writes neither keys nor values in plaintext")
        void sealed(@TempDir Path directory) throws IOException {
            try (var ledger = open(directory, NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "KEY-MARKER", "VALUE-MARKER"));
            }
            assertTrue(!contains(directory, "KEY-MARKER") && !contains(directory, "VALUE-MARKER"));
        }

        @Test
        @DisplayName("a wrong key, a passphrase for a keyed store and no key for an encrypted one are refused")
        void wrongEncryption(@TempDir Path directory) {
            try (var ledger = open(directory, NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
            }
            var wrong = KEY.clone();
            wrong[0] ^= 1;
            assertThrows(LedgerKeyException.class, () -> open(directory, Encryption.key(wrong), NAMES));
            assertThrows(LedgerKeyException.class, () -> open(directory, Encryption.passphrase("x".toCharArray()), NAMES));
            assertThrows(LedgerKeyException.class, () -> open(directory, Encryption.none(), NAMES));
            try (var ledger = open(directory, NAMES)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
            }
        }

        @Test
        @DisplayName("one store at a time holds a directory")
        void locked(@TempDir Path directory) {
            try (var _ = open(directory, NAMES)) {
                assertThrows(LedgerLockedException.class, () -> open(directory, NAMES));
            }
            open(directory, NAMES).close();
        }
    }

    @Nested
    @DisplayName("durability")
    class DurabilityOfCommits {
        @Test
        @DisplayName("a durable commit is in the files before the store closes")
        void durableBeforeClose(@TempDir Path base) throws Exception {
            var directory = base.resolve("store");
            var copy = base.resolve("copy");
            var ledger = open(directory, NAMES);
            try {
                var commit = ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
                commit.awaitDurable();
                assertTrue(commit.isDurable());
                ledger.commit(ledger.newBatch().put(NAMES, "b", "2"));
                ledger.awaitDurable();
                // What a crash would leave: the files as they are, without the lock
                copyWithoutLock(directory, copy);
            } finally {
                ledger.close();
            }
            try (var recovered = open(copy, NAMES)) {
                assertEquals(Optional.of("1"), recovered.get(NAMES, "a"));
                assertEquals(Optional.of("2"), recovered.get(NAMES, "b"));
            }
        }

        @Test
        @DisplayName("a wait for durability times out, takes an interrupt, and returns once the force ends")
        void waits() throws Exception {
            var disk = CrashFileSystem.create();
            var slow = new java.util.concurrent.atomic.AtomicBoolean();
            var released = new java.util.concurrent.CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (slow.get() && kind == ChangeInterceptor.ChangeKind.FORCE) {
                    try {
                        released.await();
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            try (var ledger = open(disk.getPath("/store"), NAMES)) {
                slow.set(true);
                var commit = ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
                assertFalse(commit.awaitDurable(java.time.Duration.ofMillis(50)));
                var waiter = Thread.ofVirtual().start(() -> {
                    try {
                        commit.awaitDurable();
                    } catch (InterruptedException expected) {
                        return;
                    }
                    throw new AssertionError("the wait ended without an interrupt");
                });
                waiter.interrupt();
                waiter.join(5_000);
                assertFalse(waiter.isAlive());
                var durable = Thread.ofVirtual().start(() -> {
                    try {
                        commit.awaitDurable();
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                });
                slow.set(false);
                released.countDown();
                durable.join(5_000);
                assertFalse(durable.isAlive());
                assertTrue(commit.isDurable());
                assertTrue(commit.awaitDurable(java.time.Duration.ZERO));
            }
        }

        @Test
        @DisplayName("concurrent committers share forces")
        void groupCommit(@TempDir Path directory) throws Exception {
            try (var pool = IoPool.create(2);
                 var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES).ioPool(pool).build()) {
                var commits = 400;
                var results = new ArrayList<Future<?>>();
                try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (var i = 0; i < commits; i++) {
                        var key = "k" + i;
                        results.add(executor.submit(() -> {
                            ledger.commit(ledger.newBatch().put(NAMES, key, "v")).awaitDurable();
                            return null;
                        }));
                    }
                }
                for (var result : results) {
                    result.get();
                }
                var forces = ((LiveLedger) ledger).log().forces();
                assertEquals(commits, ledger.size(NAMES));
                assertTrue(forces < commits, () -> forces + " forces for " + commits + " commits");
            }
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        @Test
        @DisplayName("a torn tail is dropped and the store goes on after it")
        void tornTail(@TempDir Path directory) throws Exception {
            var crashed = open(directory, NAMES);
            Path segment;
            long end;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2")).awaitDurable();
                segment = writing(directory, crashed);
                end = ((LiveLedger) crashed).log().groupEnd();
            } finally {
                crash(crashed);
            }
            // A crash in the middle of the last frame: its last bytes, and what follows, are garbage
            try (var channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap("garbage after the tear".getBytes(StandardCharsets.US_ASCII)), end - 5);
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
                assertEquals(Optional.empty(), ledger.get(NAMES, "b"));
                ledger.commit(ledger.newBatch().put(NAMES, "c", "3"));
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(List.of("a", "c"), ledger.range(NAMES, KeyRange.all(), 10).stream().map(com.github.auties00.ledger.Entry::key).toList());
            }
        }

        @Test
        @DisplayName("damage before data written after a later force is refused, not dropped")
        void damageBeforeLaterGroup(@TempDir Path directory) throws Exception {
            var crashed = open(directory, NAMES);
            Path segment;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2")).awaitDurable();
                segment = writing(directory, crashed);
            } finally {
                crash(crashed);
            }
            var bytes = Files.readAllBytes(segment);
            // The body of the first frame after the segment header
            bytes[LogWriter.SEGMENT_HEADER + LogWriter.FRAME_HEADER + 2] ^= 0x40;
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, NAMES));
        }

        @Test
        @DisplayName("damage to the first frame header before data written after a later force is refused, not dropped")
        void damagedFirstFrameHeader(@TempDir Path directory) throws Exception {
            var crashed = open(directory, NAMES);
            Path segment;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2")).awaitDurable();
                segment = writing(directory, crashed);
            } finally {
                crash(crashed);
            }
            var bytes = Files.readAllBytes(segment);
            // The first frame's header holds the tag of the use that wrote the segment
            bytes[LogWriter.SEGMENT_HEADER + LogWriter.USE_TAG_OFFSET + 1] ^= 0x40;
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, NAMES));
        }

        @Test
        @DisplayName("a first frame block zeroed before data written after a later force is refused, not taken for unwritten")
        void zeroedFirstBlock(@TempDir Path directory) throws Exception {
            var crashed = open(directory, NAMES);
            Path segment;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2")).awaitDurable();
                segment = writing(directory, crashed);
            } finally {
                crash(crashed);
            }
            var bytes = Files.readAllBytes(segment);
            Arrays.fill(bytes, LogWriter.SEGMENT_HEADER, LogWriter.SEGMENT_HEADER + LogWriter.ALIGNMENT, (byte) 0);
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, NAMES));
        }

        @Test
        @DisplayName("a segment of another store is refused")
        void foreignSegment(@TempDir Path base) throws IOException {
            var first = base.resolve("first");
            var second = base.resolve("second");
            var crashed = open(first, NAMES);
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1"));
            } finally {
                crash(crashed);
            }
            var other = open(second, NAMES);
            try {
                other.commit(other.newBatch().put(NAMES, "b", "2"));
            } finally {
                crash(other);
            }
            // The second store's segment, renamed to the one the first store's next open would write
            Files.copy(segments(second).getFirst(), first.resolve("log").resolve(LogWriter.segmentName(2)), StandardCopyOption.REPLACE_EXISTING);
            assertThrows(LedgerCorruptedException.class, () -> open(first, NAMES));
        }

        @Test
        @DisplayName("a segment cut short in its header is a crash while it was created, and is replaced")
        void tornCreation(@TempDir Path directory) throws IOException {
            var crashed = open(directory, NAMES);
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1"));
            } finally {
                crash(crashed);
            }
            var torn = directory.resolve("log").resolve(LogWriter.segmentName(2));
            Files.write(torn, Arrays.copyOf(Files.readAllBytes(segments(directory).getFirst()), LogWriter.SEGMENT_HEADER / 2));
            try (var ledger = open(directory, NAMES)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
                ledger.commit(ledger.newBatch().put(NAMES, "b", "2"));
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(2, ledger.size(NAMES));
            }
        }

        @Test
        @DisplayName("a damaged header before written frames is refused, not deleted")
        void damagedHeader(@TempDir Path directory) throws IOException {
            var crashed = open(directory, NAMES);
            Path segment;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1"));
                segment = writing(directory, crashed);
            } finally {
                crash(crashed);
            }
            var bytes = Files.readAllBytes(segment);
            bytes[3] ^= 1;
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, NAMES));
            assertTrue(Files.exists(segment));
        }
    }

    @Nested
    @DisplayName("reusing the log")
    class Reuse {
        @Test
        @DisplayName("a clean close leaves one empty segment of two blocks, which the next open writes into")
        void cleanClose(@TempDir Path directory) throws Exception {
            var kept = -1L;
            for (var round = 0; round < 3; round++) {
                try (var ledger = open(directory, NAMES)) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + round, "v" + round)).awaitDurable();
                    assertEquals(round + 1, ledger.size(NAMES));
                    if (kept >= 0) {
                        assertEquals(kept, ((LiveLedger) ledger).log().currentSegment());
                    }
                }
                var files = logFiles(directory);
                assertEquals(1, files.size(), () -> "files left in the log: " + files);
                kept = LogFiles.segmentNumber(files.getFirst().getFileName().toString());
                assertTrue(kept >= 0, () -> files.getFirst() + " is not a segment");
                assertEquals(LogWriter.KEPT_LENGTH, Files.size(files.getFirst()));
            }
        }

        @Test
        @DisplayName("a store opened and closed without a commit keeps the segment it was given")
        void idleOpen(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, NAMES)) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
            }
            var before = logFiles(directory);
            open(directory, NAMES).close();
            assertEquals(before, logFiles(directory));
            try (var ledger = open(directory, NAMES)) {
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
                ledger.commit(ledger.newBatch().put(NAMES, "b", "2")).awaitDurable();
                assertEquals(LogFiles.segmentNumber(before.getFirst().getFileName().toString()), ((LiveLedger) ledger).log().currentSegment());
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(2, ledger.size(NAMES));
            }
        }

        @Test
        @DisplayName("a reused segment keeps the length of its first use, so writing into it never grows it")
        void reusedLength(@TempDir Path directory) throws Exception {
            var segmentSize = 64 << 10;
            try (var ledger = sized(directory, segmentSize)) {
                var log = ((LiveLedger) ledger).log();
                var commits = 0;
                while (log.currentSegment() < 4) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + commits++, "v".repeat(1_000))).awaitDurable();
                }
                // Makes the full segments obsolete: one stays as a spare
                ledger.checkpoint();
                for (var attempt = 0; attempt < 200; attempt++) {
                    ledger.commit(ledger.newBatch().put(NAMES, "k" + commits++, "v".repeat(1_000))).awaitDurable();
                    var segment = writing(directory, ledger);
                    if (Files.size(segment) >= segmentSize && log.groupEnd() < segmentSize / 4) {
                        var size = Files.size(segment);
                        for (var i = 0; i < 3; i++) {
                            ledger.commit(ledger.newBatch().put(NAMES, "k" + commits++, "v".repeat(1_000))).awaitDurable();
                        }
                        assertEquals(segment, writing(directory, ledger));
                        assertEquals(size, Files.size(segment));
                        return;
                    }
                }
                throw new AssertionError("no spare was reused");
            }
        }

        @Test
        @DisplayName("obsolete segments are reused, and recovery ignores the frames of their earlier use")
        void recycled(@TempDir Path directory) throws Exception {
            var model = new TreeMap<String, String>();
            var random = new Random(9);
            var ledger = sized(directory, 16_384);
            try {
                for (var round = 0; round < 30; round++) {
                    for (var i = 0; i < 12; i++) {
                        var key = "k" + random.nextInt(200);
                        var value = "v".repeat(random.nextInt(2_000));
                        ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                        model.put(key, value);
                    }
                    ledger.checkpoint();
                    var files = logFiles(directory).size();
                    assertTrue(files <= 8, () -> files + " files in the log");
                }
                // Write into reused files, then stop without a checkpoint, so recovery reads them
                for (var i = 0; i < 20; i++) {
                    var key = "k" + random.nextInt(200);
                    var value = "after " + i;
                    ledger.commit(ledger.newBatch().put(NAMES, key, value)).awaitDurable();
                    model.put(key, value);
                }
            } finally {
                crash(ledger);
            }
            try (var reopened = sized(directory, 16_384)) {
                var entries = new TreeMap<String, String>();
                reopened.forEach(NAMES, KeyRange.all(), (key, value) -> {
                    entries.put(key, value);
                    return true;
                });
                assertEquals(model, entries);
            }
        }

        @Test
        @DisplayName("frames a reused file kept from its earlier use are never replayed, even when they authenticate")
        void staleFrames(@TempDir Path base) throws Exception {
            // Without encryption a stale frame passes its CRC, and these carry sequences the new commits reach
            var old = base.resolve("old");
            var previous = open(old, Encryption.none(), NAMES);
            Path used;
            try {
                for (var i = 1; i <= 10; i++) {
                    previous.commit(previous.newBatch().put(NAMES, "stale" + i, "x")).awaitDurable();
                }
                used = writing(old, previous);
            } finally {
                crash(previous);
            }
            var directory = base.resolve("store");
            open(directory, Encryption.none(), NAMES).close();
            Files.copy(used, directory.resolve("log").resolve("0000000000000099.spare"));
            var crashed = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(NAMES))
                    .segmentSize(16_384)
                    .build();
            try {
                // The first group goes to the segment the close kept, and the next segment is prepared from the spare:
                // the commits roll over into it
                crashed.commit(crashed.newBatch().put(NAMES, "own", "1")).awaitDurable();
                var log = ((LiveLedger) crashed).log();
                var first = log.currentSegment();
                var commits = 0;
                while (log.currentSegment() == first) {
                    crashed.commit(crashed.newBatch().put(NAMES, "own" + commits++, "x".repeat(1_000))).awaitDurable();
                }
                crashed.commit(crashed.newBatch().put(NAMES, "last", "1")).awaitDurable();
            } finally {
                crash(crashed);
            }
            try (var ledger = open(directory, Encryption.none(), NAMES)) {
                assertTrue(ledger.range(NAMES, KeyRange.all(), 1_000).stream().noneMatch(entry -> entry.key().startsWith("stale")));
                assertEquals(Optional.of("1"), ledger.get(NAMES, "last"));
            }
        }

        @Test
        @DisplayName("a segment a crash left prepared and unwritten is written under its number")
        void unwrittenSegment(@TempDir Path directory) throws Exception {
            var crashed = open(directory, NAMES);
            long number;
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                number = ((LiveLedger) crashed).log().currentSegment();
            } finally {
                crash(crashed);
            }
            // What a crash right after preparing the next segment leaves: the segment at its full length, not the one
            // a close cuts to two blocks
            Files.delete(directory.resolve("log").resolve(LogWriter.segmentName(number + 1)));
            var opened = StoreHeader.openOrCreate(directory, EncryptionSpec.key(KEY));
            new LogFiles(directory.resolve("log"), opened.keys(), opened.header().storeId()).prepare(number + 1, 16 << 20, true);
            try (var ledger = open(directory, NAMES)) {
                assertEquals(number + 1, ((LiveLedger) ledger).log().currentSegment());
                assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
                ledger.commit(ledger.newBatch().put(NAMES, "b", "2"));
            }
            try (var ledger = open(directory, NAMES)) {
                assertEquals(2, ledger.size(NAMES));
            }
        }

        private static Ledger sized(Path directory, long segmentSize) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(NAMES))
                    .segmentSize(segmentSize)
                    .build();
        }
    }

    /// Stops `ledger` as a process would once its commits are durable, leaving the log for the next open to replay.
    private static void crash(Ledger ledger) {
        ((LiveLedger) ledger).closeWithoutCheckpoint();
    }

    private static boolean contains(Path directory, String marker) throws IOException {
        var needle = marker.getBytes(StandardCharsets.US_ASCII);
        try (var files = Files.walk(directory)) {
            for (var file : files.filter(Files::isRegularFile).filter(file -> !file.getFileName().toString().equals(DirectoryLock.FILE)).toList()) {
                var bytes = Files.readAllBytes(file);
                outer:
                for (var i = 0; i + needle.length <= bytes.length; i++) {
                    for (var j = 0; j < needle.length; j++) {
                        if (bytes[i + j] != needle[j]) {
                            continue outer;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    private static void copyWithoutLock(Path source, Path target) throws IOException {
        try (var files = Files.walk(source)) {
            for (var file : files.toList()) {
                var destination = target.resolve(source.relativize(file));
                if (Files.isDirectory(file)) {
                    Files.createDirectories(destination);
                } else if (!file.getFileName().toString().equals(DirectoryLock.FILE)) {
                    try {
                        Files.copy(file, destination);
                    } catch (java.nio.file.NoSuchFileException _) {
                        // A segment being prepared beside the store took its name meanwhile: a crash would not see it
                    }
                }
            }
        }
    }
}
