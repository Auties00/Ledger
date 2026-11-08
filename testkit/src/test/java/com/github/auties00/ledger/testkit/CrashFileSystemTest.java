package com.github.auties00.ledger.testkit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The crash file system on its own: the files API a store uses, and what each kind of failure keeps.
class CrashFileSystemTest {
    private static final int DRAWS = 200;

    private static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void writeForced(Path file, byte[] bytes) throws IOException {
        try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes), 0);
            channel.force(true);
        }
    }

    @Nested
    @DisplayName("the files API")
    class FilesApi {
        @Test
        @DisplayName("creates, lists, copies, moves, sizes and deletes like a file system")
        void operations() throws IOException {
            var fs = CrashFileSystem.create();
            var directory = fs.getPath("/store/log");
            Files.createDirectories(directory);
            assertTrue(Files.isDirectory(directory));
            Files.write(directory.resolve("a"), "hello".getBytes(StandardCharsets.US_ASCII));
            Files.write(directory.resolve("a"), " world".getBytes(StandardCharsets.US_ASCII), StandardOpenOption.APPEND);
            assertEquals("hello world", Files.readString(directory.resolve("a")));
            assertEquals(11, Files.size(directory.resolve("a")));
            Files.copy(directory.resolve("a"), directory.resolve("b"));
            Files.move(directory.resolve("b"), directory.resolve("c"), StandardCopyOption.ATOMIC_MOVE);
            try (var entries = Files.list(directory)) {
                assertEquals(List.of("a", "c"), entries.map(path -> path.getFileName().toString()).sorted().toList());
            }
            try (var all = Files.walk(fs.getPath("/store"))) {
                assertEquals(4, all.count());
            }
            Files.delete(directory.resolve("c"));
            assertFalse(Files.exists(directory.resolve("c")));
            assertThrows(NoSuchFileException.class, () -> Files.delete(directory.resolve("c")));
            assertEquals("/store/log/a", directory.resolve("a").toString());
            assertEquals(fs.getPath("/store"), directory.getParent());
            assertEquals(fs.getPath("log/a"), fs.getPath("/store").relativize(directory.resolve("a")));
        }

        @Test
        @DisplayName("a file lock is exclusive until released")
        void locks() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/lock");
            try (var first = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                 var second = FileChannel.open(file, StandardOpenOption.WRITE)) {
                var lock = first.tryLock();
                assertThrows(OverlappingFileLockException.class, second::tryLock);
                lock.release();
                second.tryLock().release();
            }
        }
    }

    @Nested
    @DisplayName("a loss of power")
    class PowerLoss {
        @Test
        @DisplayName("keeps every forced write, and only sometimes, or in part, the others")
        void writes() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            var forced = new byte[2_048];
            new Random(1).nextBytes(forced);
            writeForced(file, forced);
            forceDirectory(fs.getPath("/"));
            var unforced = new byte[2_048];
            new Random(2).nextBytes(unforced);
            try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(unforced), 1_024);
            }
            var outcomes = new HashSet<String>();
            for (var seed = 0; seed < DRAWS; seed++) {
                var after = fs.afterPowerLoss(new Random(seed));
                var bytes = Files.readAllBytes(after.getPath("/file"));
                // The first kilobyte only ever held forced bytes
                for (var i = 0; i < 1_024; i++) {
                    assertEquals(forced[i], bytes[i]);
                }
                var keptAll = bytes.length == 3_072 && java.util.Arrays.equals(bytes, 1_024, 3_072, unforced, 0, 2_048);
                var keptNone = bytes.length == 2_048 && java.util.Arrays.equals(bytes, 1_024, 2_048, forced, 1_024, 2_048);
                outcomes.add(keptAll ? "all" : keptNone ? "none" : "torn or zeros");
            }
            assertEquals(3, outcomes.size(), () -> "outcomes drawn: " + outcomes);
        }

        @Test
        @DisplayName("keeps an entry only once its directory was forced")
        void entries() throws IOException {
            var fs = CrashFileSystem.create();
            writeForced(fs.getPath("/unlisted"), new byte[]{1});
            var seen = new HashSet<Boolean>();
            for (var seed = 0; seed < DRAWS; seed++) {
                seen.add(Files.exists(fs.afterPowerLoss(new Random(seed)).getPath("/unlisted")));
            }
            assertEquals(2, seen.size(), "an entry not forced survives some losses and not others");
            forceDirectory(fs.getPath("/"));
            for (var seed = 0; seed < DRAWS; seed++) {
                assertArrayEquals(new byte[]{1}, Files.readAllBytes(fs.afterPowerLoss(new Random(seed)).getPath("/unlisted")));
            }
        }

        @Test
        @DisplayName("keeps a prefix of the namespace changes not forced, in the order they ran")
        void prefix() throws IOException {
            var fs = CrashFileSystem.create();
            Files.createDirectory(fs.getPath("/a"));
            Files.createDirectory(fs.getPath("/b"));
            writeForced(fs.getPath("/a/one"), new byte[]{1});
            writeForced(fs.getPath("/b/two"), new byte[]{2});
            for (var seed = 0; seed < DRAWS; seed++) {
                var after = fs.afterPowerLoss(new Random(seed));
                // Created second, so it never survives without the first
                if (Files.exists(after.getPath("/b/two"))) {
                    assertTrue(Files.exists(after.getPath("/a/one")));
                }
            }
        }

        @Test
        @DisplayName("a crash of the process alone keeps everything written")
        void processCrash() throws IOException {
            var fs = CrashFileSystem.create();
            Files.createDirectories(fs.getPath("/x"));
            Files.write(fs.getPath("/x/file"), new byte[]{7, 8});
            var after = fs.afterProcessCrash();
            assertArrayEquals(new byte[]{7, 8}, Files.readAllBytes(after.getPath("/x/file")));
        }
    }

    @Nested
    @DisplayName("forces")
    class Forces {
        @Test
        @DisplayName("count those that asked for metadata, per path")
        void metadata() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            var other = fs.getPath("/other");
            try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3}), 0);
                channel.force(false);
                assertEquals(0, fs.metadataForces(file));
                channel.force(true);
                assertEquals(1, fs.metadataForces(file));
            }
            assertEquals(0, fs.metadataForces(other));
        }

        @Test
        @DisplayName("without metadata make a write past the end durable")
        void growth() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            var bytes = new byte[]{4, 5, 6};
            try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(bytes), 0);
                channel.force(false);
            }
            forceDirectory(fs.getPath("/"));
            for (var draw = 0L; draw < DRAWS; draw++) {
                assertArrayEquals(bytes, Files.readAllBytes(fs.afterPowerLoss(new Random(draw)).getPath("/file")));
            }
        }
    }

    @Nested
    @DisplayName("stopping the process")
    class Stopping {
        @Test
        @DisplayName("the change it names fails, and so does everything after it")
        void stops() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            fs.stopAfter(2);
            try (var channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(new byte[]{1}), 0);
                assertThrows(IOException.class, () -> channel.force(true));
                assertThrows(IOException.class, () -> channel.write(ByteBuffer.wrap(new byte[]{2}), 1));
            }
            assertTrue(fs.stopped());
            assertThrows(IOException.class, () -> Files.readAllBytes(file));
            assertEquals(2, fs.changes());
            assertArrayEquals(new byte[]{1}, Files.readAllBytes(fs.afterProcessCrash().getPath("/file")));
        }
    }
}
