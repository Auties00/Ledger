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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private static List<Path> segments(Path directory) throws IOException {
        try (var files = Files.list(directory.resolve("log"))) {
            return files.sorted(Comparator.comparing(Path::toString)).toList();
        }
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
            // The size of a segment holding one commit, which grows by one byte per byte of value
            var probe = base.resolve("probe");
            var probeSize = loneCommit(probe, 300, false);
            var boundary = (probeSize / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
            var length = 300 + (int) (boundary - 10 - probeSize);
            var directory = base.resolve("store");
            var size = loneCommit(directory, length, true);
            assertTrue(size > boundary - LogWriter.FRAME_HEADER && size < boundary, () -> "the group ends at " + size);
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

        /// Writes one commit with a value of `length` bytes into a segment of its own and returns the segment's size,
        /// then, if `next`, commits once more into the same segment.
        private static long loneCommit(Path directory, int length, boolean next) throws Exception {
            open(directory, NAMES).close();
            var ledger = open(directory, NAMES);
            try {
                ledger.commit(ledger.newBatch().put(NAMES, "lone", "x".repeat(length))).awaitDurable();
                var size = Files.size(segments(directory).getLast());
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
        void tornTail(@TempDir Path directory) throws IOException {
            var crashed = open(directory, NAMES);
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1"));
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2"));
            } finally {
                crash(crashed);
            }
            var segment = segments(directory).getLast();
            // A crash in the middle of the last frame: cut it short, then leave garbage after it
            var size = Files.size(segment);
            try (var channel = Files.newByteChannel(segment, StandardOpenOption.WRITE)) {
                channel.truncate(size - 5);
            }
            Files.write(segment, "garbage after the tear".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.APPEND);
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
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1")).awaitDurable();
                crashed.commit(crashed.newBatch().put(NAMES, "b", "2")).awaitDurable();
            } finally {
                crash(crashed);
            }
            var segment = segments(directory).getLast();
            var bytes = Files.readAllBytes(segment);
            // The body of the first frame after the segment header
            bytes[LogWriter.SEGMENT_HEADER + LogWriter.FRAME_HEADER + 2] ^= 0x40;
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
            Files.copy(segments(second).getFirst(), first.resolve("log").resolve(LogWriter.segmentName(2)));
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
            try {
                crashed.commit(crashed.newBatch().put(NAMES, "a", "1"));
            } finally {
                crash(crashed);
            }
            var segment = segments(directory).getLast();
            var bytes = Files.readAllBytes(segment);
            bytes[3] ^= 1;
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(directory, NAMES));
            assertTrue(Files.exists(segment));
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
                    Files.copy(file, destination);
                }
            }
        }
    }
}
