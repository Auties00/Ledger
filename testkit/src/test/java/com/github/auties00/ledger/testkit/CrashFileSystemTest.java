package com.github.auties00.ledger.testkit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
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

        @Test
        @DisplayName("a crash of the process leaves what no force made durable at risk from a later loss of power")
        void processCrashKeepsCacheAtRisk() throws IOException {
            var fs = CrashFileSystem.create();
            var forced = fs.getPath("/forced");
            writeForced(forced, new byte[]{1});
            forceDirectory(fs.getPath("/"));
            Files.write(fs.getPath("/cached"), new byte[]{2});
            try (var channel = FileChannel.open(forced, StandardOpenOption.WRITE)) {
                channel.write(ByteBuffer.wrap(new byte[]{3}), 0);
            }
            var crashed = fs.afterProcessCrash();
            assertArrayEquals(new byte[]{3}, Files.readAllBytes(crashed.getPath("/forced")));
            assertArrayEquals(new byte[]{2}, Files.readAllBytes(crashed.getPath("/cached")));
            var lostName = false;
            var lostWrite = false;
            for (var seed = 0; seed < DRAWS; seed++) {
                var after = crashed.afterPowerLoss(new Random(seed));
                lostName |= !Files.exists(after.getPath("/cached"));
                var contents = Files.readAllBytes(after.getPath("/forced"));
                lostWrite |= contents[0] == 1;
                assertTrue(contents[0] == 1 || contents[0] == 3);
            }
            assertTrue(lostName, "the entry no force made durable always survived");
            assertTrue(lostWrite, "the write no force made durable always survived");
        }

        @Test
        @DisplayName("a force after a crash of the process makes durable what the process left in the cache")
        void forceAfterProcessCrash() throws IOException {
            var fs = CrashFileSystem.create();
            Files.write(fs.getPath("/file"), new byte[]{4});
            var crashed = fs.afterProcessCrash();
            forceDirectory(crashed.getPath("/"));
            try (var channel = FileChannel.open(crashed.getPath("/file"), StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            for (var seed = 0; seed < DRAWS; seed++) {
                assertArrayEquals(new byte[]{4}, Files.readAllBytes(crashed.afterPowerLoss(new Random(seed)).getPath("/file")));
            }
        }

        @Test
        @DisplayName("a rename, a deletion and a file renamed since it was forced survive a crash of the process as pending changes")
        void processCrashKeepsPendingNames() throws IOException {
            var fs = CrashFileSystem.create();
            writeForced(fs.getPath("/gone"), new byte[]{1});
            writeForced(fs.getPath("/old"), new byte[]{2});
            forceDirectory(fs.getPath("/"));
            Files.delete(fs.getPath("/gone"));
            Files.move(fs.getPath("/old"), fs.getPath("/new"), StandardCopyOption.ATOMIC_MOVE);
            var crashed = fs.afterProcessCrash();
            assertFalse(Files.exists(crashed.getPath("/gone")));
            assertTrue(Files.exists(crashed.getPath("/new")));
            var keptOld = false;
            var keptGone = false;
            for (var seed = 0; seed < DRAWS; seed++) {
                var after = crashed.afterPowerLoss(new Random(seed));
                keptOld |= Files.exists(after.getPath("/old"));
                keptGone |= Files.exists(after.getPath("/gone"));
                assertTrue(Files.exists(after.getPath("/old")) ^ Files.exists(after.getPath("/new")));
            }
            assertTrue(keptOld);
            assertTrue(keptGone);
        }
    }

    @Nested
    @DisplayName("a file system that journals names as NTFS does")
    class Ntfs {
        @Test
        @DisplayName("refuses to open a directory")
        void noDirectoryChannel() throws IOException {
            var fs = CrashFileSystem.createNtfs();
            Files.createDirectory(fs.getPath("/x"));
            assertThrows(AccessDeniedException.class, () -> forceDirectory(fs.getPath("/x")));
        }

        @Test
        @DisplayName("makes every name durable with a force of any file")
        void forceFlushesNames() throws IOException {
            var fs = CrashFileSystem.createNtfs();
            Files.write(fs.getPath("/named"), new byte[]{1});
            writeForced(fs.getPath("/other"), new byte[]{2});
            for (var seed = 0; seed < DRAWS; seed++) {
                assertTrue(Files.exists(fs.afterPowerLoss(new Random(seed)).getPath("/named")));
            }
        }

        @Test
        @DisplayName("makes no name durable with a write of a channel opened with DSYNC")
        void dsyncLeavesNames() throws IOException {
            var fs = CrashFileSystem.createNtfs();
            writeForced(fs.getPath("/forced"), new byte[4_096]);
            Files.move(fs.getPath("/forced"), fs.getPath("/renamed"), StandardCopyOption.ATOMIC_MOVE);
            try (var channel = FileChannel.open(fs.getPath("/renamed"), StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                channel.write(ByteBuffer.wrap(new byte[4_096]), 0);
            }
            var lost = false;
            for (var seed = 0; seed < DRAWS; seed++) {
                lost |= !Files.exists(fs.afterPowerLoss(new Random(seed)).getPath("/renamed"));
            }
            assertTrue(lost, "the write through made the rename durable");
        }

        @Test
        @DisplayName("journals as NTFS does after a loss of power and after a crash of the process")
        void keepsMode() throws IOException {
            var fs = CrashFileSystem.createNtfs();
            Files.createDirectory(fs.getPath("/x"));
            writeForced(fs.getPath("/x/file"), new byte[]{1});
            assertThrows(AccessDeniedException.class, () -> forceDirectory(fs.afterProcessCrash().getPath("/x")));
            assertThrows(AccessDeniedException.class, () -> forceDirectory(fs.afterPowerLoss(new Random(1)).getPath("/x")));
        }
    }

    @Nested
    @DisplayName("a channel opened with DSYNC")
    class Dsync {
        @Test
        @DisplayName("makes each write durable once it returns, and leaves the unforced writes of other ranges at risk")
        void durableAtReturn() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            writeForced(file, new byte[4_096]);
            forceDirectory(fs.getPath("/"));
            var unforced = filled(1_024, (byte) 1);
            var through = filled(1_024, (byte) 2);
            try (var plain = FileChannel.open(file, StandardOpenOption.WRITE);
                 var sync = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                plain.write(ByteBuffer.wrap(unforced), 0);
                sync.write(ByteBuffer.wrap(through), 2_048);
            }
            var kept = new HashSet<Boolean>();
            for (var seed = 0; seed < DRAWS; seed++) {
                var bytes = Files.readAllBytes(fs.afterPowerLoss(new Random(seed)).getPath("/file"));
                assertArrayEquals(through, java.util.Arrays.copyOfRange(bytes, 2_048, 3_072));
                kept.add(bytes[0] == 1);
            }
            assertEquals(2, kept.size(), "the unforced write of another range survives some losses and not others");
        }

        @Test
        @DisplayName("keeps its bytes over an unforced write it overlaps, which keeps its own bytes at risk")
        void overlapping() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            writeForced(file, new byte[4_096]);
            forceDirectory(fs.getPath("/"));
            var through = filled(1_024, (byte) 2);
            try (var plain = FileChannel.open(file, StandardOpenOption.WRITE);
                 var sync = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                plain.write(ByteBuffer.wrap(filled(2_048, (byte) 1)), 512);
                sync.write(ByteBuffer.wrap(through), 1_024);
            }
            var edges = new HashSet<String>();
            for (var seed = 0; seed < DRAWS; seed++) {
                var bytes = Files.readAllBytes(fs.afterPowerLoss(new Random(seed)).getPath("/file"));
                assertEquals(4_096, bytes.length);
                assertArrayEquals(through, java.util.Arrays.copyOfRange(bytes, 1_024, 2_048));
                edges.add(bytes[512] + "," + bytes[2_048]);
            }
            assertTrue(edges.size() > 1, () -> "the parts of the unforced write outside it: " + edges);
        }

        @Test
        @DisplayName("grows the file durably when it writes past its end")
        void grows() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            writeForced(file, new byte[512]);
            forceDirectory(fs.getPath("/"));
            try (var sync = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                sync.write(ByteBuffer.wrap(filled(512, (byte) 3)), 1_024);
            }
            for (var seed = 0; seed < DRAWS; seed++) {
                var bytes = Files.readAllBytes(fs.afterPowerLoss(new Random(seed)).getPath("/file"));
                assertEquals(1_536, bytes.length);
                assertEquals(3, bytes[1_024]);
                assertEquals(3, bytes[1_535]);
            }
        }

        @Test
        @DisplayName("counts as a force for a stop at a force, which leaves the write to a loss of power")
        void stopAtForce() throws IOException {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            writeForced(file, new byte[1_024]);
            forceDirectory(fs.getPath("/"));
            var before = fs.forces();
            try (var sync = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                sync.write(ByteBuffer.wrap(filled(512, (byte) 4)), 0);
                assertEquals(before + 1, fs.forces());
                fs.stopAtForce(0);
                assertThrows(IOException.class, () -> sync.write(ByteBuffer.wrap(filled(512, (byte) 5)), 512));
            }
            var kept = new HashSet<Boolean>();
            for (var seed = 0; seed < DRAWS; seed++) {
                var bytes = Files.readAllBytes(fs.afterPowerLoss(new Random(seed)).getPath("/file"));
                assertEquals(4, bytes[0]);
                kept.add(bytes[512] == 5);
            }
            assertEquals(2, kept.size(), "the write the stop cut off survives some losses and not others");
        }

        @Test
        @DisplayName("shows the interceptor a write and then a force, and an interrupt between them closes the channel")
        void interrupted() throws Exception {
            var fs = CrashFileSystem.create();
            var file = fs.getPath("/file");
            writeForced(file, new byte[1_024]);
            var kinds = new java.util.ArrayList<ChangeInterceptor.ChangeKind>();
            var writer = Thread.currentThread();
            fs.intercept((_, kind) -> {
                kinds.add(kind);
                if (kind == ChangeInterceptor.ChangeKind.FORCE) {
                    // From another thread, as only that closes a channel in the middle of an operation; not waited
                    // for with a join, which the interrupt would end before it reached the channel
                    var interrupter = Thread.ofPlatform().start(writer::interrupt);
                    while (interrupter.isAlive()) {
                        Thread.onSpinWait();
                    }
                }
            });
            try (var sync = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.DSYNC)) {
                assertThrows(java.nio.channels.ClosedByInterruptException.class, () -> sync.write(ByteBuffer.wrap(filled(512, (byte) 6)), 0));
                assertFalse(sync.isOpen());
            }
            assertTrue(Thread.interrupted());
            assertEquals(List.of(ChangeInterceptor.ChangeKind.WRITE, ChangeInterceptor.ChangeKind.FORCE), kinds);
        }

        private static byte[] filled(int length, byte value) {
            var bytes = new byte[length];
            java.util.Arrays.fill(bytes, value);
            return bytes;
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
