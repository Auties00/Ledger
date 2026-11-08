package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The log's lazy forces: frames no commit waits for are written at once and forced later, with the commit that needs
/// them, at a rollover, at a close or after the delay. A delay of a minute, longer than any test, stands for "never" so
/// a test decides every force itself; a wait a lost wake-up would stretch to that delay fails on its timeout.
class LazyForceTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Duration NEVER = Duration.ofMinutes(1);
    private static final Duration SOON = Duration.ofSeconds(20);

    private static Ledger open(Path directory, Encryption encryption, Duration delay) {
        return Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).durabilityDelay(delay).build();
    }

    private static Ledger open(Path directory, Duration delay) {
        return open(directory, Encryption.key(KEY), delay);
    }

    private static LogWriter log(Ledger ledger) {
        return ((LiveLedger) ledger).log();
    }

    private static Commit put(Ledger ledger, String key, String value) {
        return ledger.commit(ledger.newBatch().put(NAMES, key, value));
    }

    /// Waits for the flush of `commit` to write it, whether it forces it or not.
    private static void awaitWritten(Ledger ledger, Commit commit) throws InterruptedException {
        var deadline = System.nanoTime() + SOON.toNanos();
        while (log(ledger).writtenSequence() < commit.sequence()) {
            assertTrue(System.nanoTime() < deadline, "the flush never wrote the commit");
            Thread.sleep(1);
        }
    }

    /// Commits and waits for a force, so a test starts past the first force of the store, with the next segment prepared.
    private static void warm(Ledger ledger) throws InterruptedException {
        put(ledger, "warm", "x").awaitDurable();
    }

    private static Ledger openSmall(Path directory, Encryption encryption) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).durabilityDelay(NEVER))
                .segmentSize(16_384)
                .build();
    }

    private static Path writing(Path directory, Ledger ledger) {
        return directory.resolve("log").resolve(LogWriter.segmentName(log(ledger).currentSegment()));
    }

    private static void assertSoon(java.util.function.BooleanSupplier condition, String message) throws InterruptedException {
        var deadline = System.nanoTime() + SOON.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail(message);
            }
            Thread.sleep(1);
        }
    }

    /// A frame of a segment of a store with no encryption.
    private record Frame(long offset, long end, boolean groupStart, long sequence) {
        static Frame at(byte[] segment, long offset) {
            var header = ByteBuffer.wrap(segment, (int) offset, LogWriter.FRAME_HEADER);
            assertEquals(LogWriter.MAGIC, header.getInt(), "no frame at " + offset);
            var sealed = header.getInt();
            var sequence = header.getLong();
            var flags = segment[(int) offset + 17];
            return new Frame(offset, offset + LogWriter.FRAME_HEADER + sealed, (flags & LogWriter.GROUP_START) != 0, sequence);
        }
    }

    private static Frame frameOf(byte[] segment, long sequence) {
        var offset = (long) LogWriter.SEGMENT_HEADER;
        while (offset + LogWriter.FRAME_HEADER <= segment.length) {
            if (segment[(int) offset] == 0) {
                offset = (offset / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
                continue;
            }
            var frame = Frame.at(segment, offset);
            if (frame.sequence() == sequence) {
                return frame;
            }
            offset = frame.end();
        }
        throw new AssertionError("no frame of commit " + sequence);
    }

    @Nested
    @DisplayName("a group no commit waits for")
    class Unawaited {
        @Test
        @DisplayName("is written, so a crash of the process keeps it, and is not forced")
        void writtenNotForced() throws Exception {
            var disk = CrashFileSystem.create();
            var ledger = open(disk.getPath("/store"), NEVER);
            warm(ledger);
            var forces = log(ledger).forces();
            var commit = put(ledger, "a", "1");
            awaitWritten(ledger, commit);
            assertEquals(forces, log(ledger).forces());
            assertFalse(commit.isDurable());
            var crashed = disk.afterProcessCrash();
            try (var recovered = open(crashed.getPath("/store"), NEVER)) {
                assertEquals(Optional.of("1"), recovered.get(NAMES, "a"));
            }
            ledger.close();
        }

        @Test
        @DisplayName("shares the force of the commit after it, which the commit makes itself")
        void sharesForce() throws Exception {
            var disk = CrashFileSystem.create();
            var committer = Thread.currentThread();
            var forcedByCommitter = new AtomicInteger();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && Thread.currentThread() == committer) {
                    forcedByCommitter.incrementAndGet();
                }
            });
            var ledger = open(disk.getPath("/store"), NEVER);
            warm(ledger);
            var before = forcedByCommitter.get();
            // A thread of the pool may reach the second commit's frame first, so the commit forces it itself only mostly
            for (var i = 0; i < 200; i++) {
                var forces = log(ledger).forces();
                var first = put(ledger, "a" + i, "1");
                awaitWritten(ledger, first);
                assertEquals(forces, log(ledger).forces());
                var second = put(ledger, "b" + i, "2");
                second.awaitDurable();
                assertEquals(forces + 1, log(ledger).forces());
                assertTrue(first.isDurable());
            }
            assertTrue(forcedByCommitter.get() > before, "every commit left its force to the pool");
            for (var seed = 0; seed < 20; seed++) {
                var survivor = disk.afterPowerLoss(new Random(seed));
                try (var recovered = open(survivor.getPath("/store"), NEVER)) {
                    for (var i = 0; i < 200; i++) {
                        assertEquals(Optional.of("1"), recovered.get(NAMES, "a" + i));
                        assertEquals(Optional.of("2"), recovered.get(NAMES, "b" + i));
                    }
                }
            }
            ledger.close();
        }

        @Test
        @DisplayName("is forced at once with a delay of zero")
        void zeroDelay() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), Duration.ZERO)) {
                warm(ledger);
                var commit = put(ledger, "a", "1");
                assertSoon(commit::isDurable, "the group was not forced");
            }
        }

        @Test
        @DisplayName("is forced after the delay")
        void delayed() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), Duration.ofMillis(5))) {
                warm(ledger);
                for (var i = 0; i < 20; i++) {
                    var commit = put(ledger, "k" + i, "v");
                    assertSoon(commit::isDurable, "the delay passed with commit " + i + " not forced");
                }
            }
        }

        @Test
        @DisplayName("is forced at the first rollover")
        void rollover() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES).durabilityDelay(NEVER))
                    .segmentSize(16_384).build()) {
                warm(ledger);
                var first = put(ledger, "a", "1");
                awaitWritten(ledger, first);
                assertFalse(first.isDurable());
                var segment = log(ledger).currentSegment();
                for (var i = 0; log(ledger).currentSegment() == segment; i++) {
                    awaitWritten(ledger, put(ledger, "k" + i, "v".repeat(1_000)));
                }
                assertTrue(first.isDurable());
            }
        }

        @Test
        @DisplayName("is forced once a segment's worth of them is not")
        void byteBound() throws Exception {
            var disk = CrashFileSystem.create();
            var value = "v".repeat(1 << 20);
            try (var ledger = ((LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.none()).keyspace(NAMES)
                    .durabilityDelay(NEVER).largeValues(Long.MAX_VALUE)).segmentSize(64L << 20).build()) {
                warm(ledger);
                var first = put(ledger, "a", "1");
                awaitWritten(ledger, first);
                assertFalse(first.isDurable());
                for (var i = 0; i < 18; i++) {
                    awaitWritten(ledger, put(ledger, "k" + i, value));
                }
                assertSoon(first::isDurable, "18 MiB stayed not forced");
            }
        }

        @Test
        @DisplayName("carrying units of the file of large values is forced, and the open's epoch does not wait the delay")
        void units() {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(SOON, () -> {
                try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY)).keyspace(NAMES)
                        .durabilityDelay(NEVER).largeValues(4_096).build()) {
                    warm(ledger);
                    var commit = put(ledger, "large", "v".repeat(5_000));
                    assertSoon(commit::isDurable, "the group with a unit was not forced");
                }
            });
        }

        @Test
        @DisplayName("is durable after a close, which does not wait the delay")
        void close() {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(SOON, () -> {
                var ledger = open(disk.getPath("/store"), NEVER);
                warm(ledger);
                awaitWritten(ledger, put(ledger, "a", "1"));
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            });
            for (var seed = 0; seed < 20; seed++) {
                var survivor = disk.afterPowerLoss(new Random(seed));
                try (var recovered = open(survivor.getPath("/store"), NEVER)) {
                    assertEquals(Optional.of("1"), recovered.get(NAMES, "a"));
                }
            }
        }
    }

    @Nested
    @DisplayName("the first frames of a use of a segment")
    class FirstFrames {
        @Test
        @DisplayName("of a reopened store share the force of the first commit a thread waits for")
        void sharedAfterOpen() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            for (var i = 0; i < 10; i++) {
                var ledger = open(directory, NEVER);
                var first = put(ledger, "a" + i, "1");
                awaitWritten(ledger, first);
                assertEquals(0, log(ledger).forces());
                assertFalse(first.isDurable());
                var second = put(ledger, "b" + i, "2");
                second.awaitDurable();
                assertEquals(1, log(ledger).forces());
                assertTrue(first.isDurable());
                var survivor = disk.afterPowerLoss(new Random(i));
                ledger.close();
                try (var recovered = open(survivor.getPath("/store"), NEVER)) {
                    assertEquals(Optional.of("1"), recovered.get(NAMES, "a" + i));
                    assertEquals(Optional.of("2"), recovered.get(NAMES, "b" + i));
                }
            }
        }

        @Test
        @DisplayName("are not forced under a load no thread waits for, which forces at its rollovers only")
        void forcesAtRollovers() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = openSmall(disk.getPath("/store"), Encryption.key(KEY))) {
                var start = log(ledger).currentSegment();
                Commit last = null;
                for (var i = 0; i < 400; i++) {
                    last = put(ledger, "k" + i, "v".repeat(1_000));
                }
                awaitWritten(ledger, last);
                var rollovers = log(ledger).currentSegment() - start;
                assertTrue(rollovers >= 20, rollovers + " rollovers");
                assertEquals(rollovers, log(ledger).forces());
            }
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = {"zeroed", "torn"})
        @DisplayName("lost to a loss of power, with later frames surviving, recover to nothing of the use, over any number of opens")
        void lost(String damage) throws Exception {
            for (var encryption : new Encryption[]{Encryption.key(KEY), Encryption.none()}) {
                var disk = CrashFileSystem.create();
                var directory = disk.getPath("/store");
                open(directory, encryption, NEVER).close();
                var ledger = open(directory, encryption, NEVER);
                // Crossing block boundaries, each its own pass of the flush
                for (var i = 0; i < 6; i++) {
                    awaitWritten(ledger, put(ledger, "k" + i, "v".repeat(3_000)));
                }
                assertEquals(0, log(ledger).forces());
                var segment = writing(directory, ledger);
                var number = log(ledger).currentSegment();
                var crashed = disk.afterProcessCrash();
                ledger.close();
                var copy = crashed.getPath(segment.toString());
                var bytes = Files.readAllBytes(copy);
                if (damage.equals("zeroed")) {
                    Arrays.fill(bytes, LogWriter.SEGMENT_HEADER, LogWriter.SEGMENT_HEADER + LogWriter.ALIGNMENT, (byte) 0);
                } else {
                    bytes[LogWriter.SEGMENT_HEADER + 9] ^= 0x40;
                }
                Files.write(copy, bytes);
                // What the loss of power left is on the disk, for the later one to find
                try (var channel = FileChannel.open(copy, StandardOpenOption.WRITE)) {
                    channel.force(false);
                }
                var recovered = open(crashed.getPath("/store"), encryption, NEVER);
                // A use of its own over what is left, or a segment of its own
                assertEquals(damage.equals("zeroed") ? number : number + 1, log(recovered).currentSegment());
                for (var i = 0; i < 6; i++) {
                    assertEquals(Optional.empty(), recovered.get(NAMES, "k" + i), "k" + i);
                }
                // Past its first group, so the frames of the use before lie behind a group of this one
                put(recovered, "after", "1").awaitDurable();
                put(recovered, "again", "2").awaitDurable();
                var survivor = crashed.afterPowerLoss(new Random(7));
                recovered.close();
                for (var round = 0; round < 2; round++) {
                    try (var reopened = open(survivor.getPath("/store"), encryption, NEVER)) {
                        assertEquals(Optional.of("1"), reopened.get(NAMES, "after"));
                        assertEquals(Optional.of("2"), reopened.get(NAMES, "again"));
                        for (var i = 0; i < 6; i++) {
                            assertEquals(Optional.empty(), reopened.get(NAMES, "k" + i), "k" + i);
                        }
                        put(reopened, "round" + round, "3").awaitDurable();
                    }
                }
            }
        }

        @Test
        @DisplayName("lost with the next segment prepared leave the commit to that one, and the full segment before them whole")
        void lostWithNextPrepared() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var ledger = openSmall(directory, Encryption.none());
            var start = log(ledger).currentSegment();
            var value = "v".repeat(1_000);
            var rolled = 0;
            while (log(ledger).currentSegment() == start) {
                awaitWritten(ledger, put(ledger, "k" + rolled++, value));
            }
            // The last one is the first of the new segment; these pass the point where the next one starts to be prepared
            var total = rolled + 6;
            for (var i = rolled; i < total; i++) {
                awaitWritten(ledger, put(ledger, "k" + i, value));
            }
            var writing = log(ledger).currentSegment();
            assertEquals(start + 1, writing);
            var prepared = directory.resolve("log").resolve(LogWriter.segmentName(writing + 1));
            assertSoon(() -> Files.exists(prepared), "the next segment was not prepared with no force");
            assertEquals(1, log(ledger).forces());
            var segment = writing(directory, ledger);
            var crashed = disk.afterProcessCrash();
            ledger.close();
            var copy = crashed.getPath(segment.toString());
            var bytes = Files.readAllBytes(copy);
            Arrays.fill(bytes, LogWriter.SEGMENT_HEADER, LogWriter.SEGMENT_HEADER + LogWriter.ALIGNMENT, (byte) 0);
            Files.write(copy, bytes);
            for (var round = 0; round < 2; round++) {
                try (var recovered = openSmall(crashed.getPath("/store"), Encryption.none())) {
                    if (round == 0) {
                        assertEquals(writing + 1, log(recovered).currentSegment());
                    }
                    for (var i = 0; i < rolled - 1; i++) {
                        assertEquals(Optional.of(value), recovered.get(NAMES, "k" + i), "k" + i);
                    }
                    for (var i = rolled - 1; i < total; i++) {
                        assertEquals(Optional.empty(), recovered.get(NAMES, "k" + i), "k" + i);
                    }
                    put(recovered, "after" + round, "1").awaitDurable();
                }
            }
        }
    }

    @Nested
    @DisplayName("the layout of the log")
    class Layout {
        @Test
        @DisplayName("frames after one not forced follow it at once with no flag; the first after a force takes it, on a block, after zeros")
        void continuation() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            try (var ledger = open(directory, Encryption.none(), NEVER)) {
                warm(ledger);
                var first = put(ledger, "a", "1");
                awaitWritten(ledger, first);
                var second = put(ledger, "b", "2");
                awaitWritten(ledger, second);
                var bytes = Files.readAllBytes(writing(directory, ledger));
                var firstFrame = frameOf(bytes, first.sequence());
                var secondFrame = frameOf(bytes, second.sequence());
                assertTrue(firstFrame.groupStart(), "the first frame after a force takes the flag");
                assertEquals(0, firstFrame.offset() % LogWriter.ALIGNMENT);
                assertEquals(firstFrame.end(), secondFrame.offset(), "no padding between frames not forced");
                assertFalse(secondFrame.groupStart());
                assertFalse(first.isDurable());

                second.awaitDurable();
                var third = put(ledger, "c", "3");
                third.awaitDurable();
                bytes = Files.readAllBytes(writing(directory, ledger));
                var thirdFrame = frameOf(bytes, third.sequence());
                assertTrue(thirdFrame.groupStart());
                assertEquals(0, thirdFrame.offset() % LogWriter.ALIGNMENT);
                assertTrue(thirdFrame.offset() > secondFrame.end());
                for (var at = secondFrame.end(); at < thirdFrame.offset(); at++) {
                    assertEquals(0, bytes[(int) at], "padding at " + at);
                }
            }
        }

        @Test
        @DisplayName("a force the timer makes pads too, so a frame after it starts a block")
        void timerPads() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            try (var ledger = open(directory, Encryption.none(), Duration.ofMillis(2))) {
                warm(ledger);
                var first = put(ledger, "a", "1");
                assertSoon(first::isDurable, "the delay passed with the commit not forced");
                var second = put(ledger, "b", "2");
                assertSoon(second::isDurable, "the delay passed with the commit not forced");
                var bytes = Files.readAllBytes(writing(directory, ledger));
                var frame = frameOf(bytes, second.sequence());
                assertTrue(frame.groupStart());
                assertEquals(0, frame.offset() % LogWriter.ALIGNMENT);
            }
        }

        @Test
        @DisplayName("damage before a group written after a lazy force is refused")
        void damageBeforeLaterGroup() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            Path segment;
            long damaged;
            var ledger = open(directory, Encryption.none(), NEVER);
            warm(ledger);
            var first = put(ledger, "a", "1");
            awaitWritten(ledger, first);
            var second = put(ledger, "b", "2");
            awaitWritten(ledger, second);
            second.awaitDurable();
            put(ledger, "c", "3").awaitDurable();
            segment = writing(directory, ledger);
            damaged = frameOf(Files.readAllBytes(segment), second.sequence()).offset();
            var crashed = disk.afterProcessCrash();
            ledger.close();
            var copy = crashed.getPath(segment.toString());
            var bytes = Files.readAllBytes(copy);
            bytes[(int) damaged + LogWriter.FRAME_HEADER + 1] ^= 0x40;
            Files.write(copy, bytes);
            assertThrows(LedgerCorruptedException.class, () -> open(crashed.getPath("/store"), Encryption.none(), NEVER));
        }

        @ParameterizedTest(name = "{0}")
        @ValueSource(strings = {"its header", "its body", "the sectors to the block's end"})
        @DisplayName("damage among frames no force covered ends the log there, with later frames surviving")
        void tornContinuation(String where) throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var ledger = open(directory, Encryption.none(), NEVER);
            warm(ledger);
            // Crossing block boundaries, each its own pass of the flush
            var commits = new ArrayList<Commit>();
            for (var i = 0; i < 6; i++) {
                var commit = put(ledger, "k" + i, "v".repeat(3_000));
                awaitWritten(ledger, commit);
                commits.add(commit);
            }
            var segment = writing(directory, ledger);
            var crashed = disk.afterProcessCrash();
            ledger.close();
            var copy = crashed.getPath(segment.toString());
            var bytes = Files.readAllBytes(copy);
            var frame = frameOf(bytes, commits.get(2).sequence());
            assertFalse(frame.groupStart());
            switch (where) {
                case "its header" -> bytes[(int) frame.offset() + 9] ^= 0x40;
                case "its body" -> bytes[(int) frame.offset() + LogWriter.FRAME_HEADER + 100] ^= 0x40;
                default -> Arrays.fill(bytes, (int) frame.offset(),
                        (int) ((frame.offset() / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT), (byte) 0);
            }
            Files.write(copy, bytes);
            try (var recovered = open(crashed.getPath("/store"), Encryption.none(), NEVER)) {
                assertEquals(Optional.of("x"), recovered.get(NAMES, "warm"));
                for (var i = 0; i < 2; i++) {
                    assertTrue(recovered.get(NAMES, "k" + i).isPresent(), "k" + i);
                }
                for (var i = 2; i < 6; i++) {
                    assertEquals(Optional.empty(), recovered.get(NAMES, "k" + i), "k" + i);
                }
                put(recovered, "after", "1").awaitDurable();
            }
            try (var recovered = open(crashed.getPath("/store"), Encryption.none(), NEVER)) {
                assertEquals(Optional.of("1"), recovered.get(NAMES, "after"));
                assertEquals(Optional.empty(), recovered.get(NAMES, "k2"));
            }
        }

        @ParameterizedTest(name = "delay {0} ms")
        @ValueSource(ints = {0, 1, 60_000})
        @DisplayName("a loss of power keeps a prefix of the commits holding every commit known durable, over reused segments")
        void powerLoss(int delay) throws Exception {
            for (var seed = 0; seed < 40; seed++) {
                var random = new Random(seed);
                var disk = CrashFileSystem.create();
                var directory = disk.getPath("/store");
                var encryption = seed % 2 == 0 ? Encryption.key(KEY) : Encryption.none();
                var ledger = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES)
                        .durabilityDelay(Duration.ofMillis(delay))).segmentSize(16_384).build();
                var durable = -1;
                var stop = 10 + random.nextInt(150);
                for (var i = 0; i < stop; i++) {
                    var commit = put(ledger, "k%04d".formatted(i), "v".repeat(random.nextInt(2_000)));
                    switch (random.nextInt(12)) {
                        case 0 -> {
                            commit.awaitDurable();
                            durable = i;
                        }
                        case 1 -> awaitWritten(ledger, commit);
                        case 2 -> {
                            // Obsolete segments become spares, whose frames of an earlier use lie past the frames written
                            ledger.checkpoint();
                            durable = i;
                        }
                        default -> {
                        }
                    }
                }
                var survivor = disk.afterPowerLoss(new Random(seed + 1_000L));
                ledger.close();
                try (var recovered = Ledger.builder().directory(survivor.getPath("/store")).encryption(encryption).keyspace(NAMES).build()) {
                    var last = -1;
                    for (var i = 0; i < stop && recovered.get(NAMES, "k%04d".formatted(i)).isPresent(); i++) {
                        last = i;
                    }
                    assertTrue(last >= durable, "seed " + seed + ": commit " + durable + " was durable, the store recovered up to " + last);
                    for (var i = last + 1; i < stop; i++) {
                        assertEquals(Optional.empty(), recovered.get(NAMES, "k%04d".formatted(i)), "seed " + seed + ": a hole before commit " + i);
                    }
                    put(recovered, "after", "1").awaitDurable();
                }
            }
        }
    }

    @Nested
    @DisplayName("a wait for a commit written and not forced")
    class Waits {
        @Test
        @DisplayName("with a timeout, on its own thread or not, ends with a force and not with the delay")
        void timed() {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(SOON, () -> {
                try (var ledger = open(disk.getPath("/store"), NEVER)) {
                    warm(ledger);
                    var commit = put(ledger, "a", "1");
                    awaitWritten(ledger, commit);
                    assertTrue(commit.awaitDurable(Duration.ofSeconds(15)));
                }
            });
        }

        @Test
        @DisplayName("for every commit of the store forces them")
        void ledgerWait() {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(SOON, () -> {
                try (var ledger = open(disk.getPath("/store"), NEVER)) {
                    warm(ledger);
                    var commit = put(ledger, "a", "1");
                    awaitWritten(ledger, commit);
                    ledger.awaitDurable();
                    assertTrue(commit.isDurable());
                }
            });
        }

        @Test
        @DisplayName("for one of several, with later commits written, forces them")
        void earlierOfSeveral() {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(SOON, () -> {
                try (var ledger = open(disk.getPath("/store"), NEVER)) {
                    warm(ledger);
                    var first = put(ledger, "a", "1");
                    var second = put(ledger, "b", "2");
                    awaitWritten(ledger, second);
                    first.awaitDurable();
                    assertTrue(second.isDurable());
                }
            });
        }

        @Test
        @DisplayName("never waits out the delay, however it races the flush")
        void noLostWakeUp() throws Exception {
            var disk = CrashFileSystem.create();
            try (var ledger = open(disk.getPath("/store"), NEVER);
                 var threads = Executors.newFixedThreadPool(4)) {
                warm(ledger);
                var results = new ArrayList<java.util.concurrent.Future<?>>();
                for (var t = 0; t < 4; t++) {
                    var thread = t;
                    results.add(threads.submit(() -> {
                        var random = ThreadLocalRandom.current();
                        for (var i = 0; i < 1_500; i++) {
                            var commit = put(ledger, "k" + thread + "-" + i, "v");
                            // Lands the wait before, inside and after the write of the group
                            for (var spin = random.nextInt(thread * 40 + 1); spin > 0; spin--) {
                                Thread.onSpinWait();
                            }
                            if (i % 3 == 0) {
                                assertTrue(commit.awaitDurable(Duration.ofSeconds(50)));
                            } else if (i % 3 == 1) {
                                commit.awaitDurable();
                            }
                        }
                        return null;
                    }));
                }
                for (var result : results) {
                    result.get(50, TimeUnit.SECONDS);
                }
            }
        }

        @Test
        @DisplayName("alone, one after the other, never waits out the delay")
        void noLostWakeUpAlone() throws Exception {
            var disk = CrashFileSystem.create();
            assertTimeoutPreemptively(Duration.ofSeconds(50), () -> {
                try (var ledger = open(disk.getPath("/store"), NEVER)) {
                    warm(ledger);
                    for (var i = 0; i < 5_000; i++) {
                        put(ledger, "d" + i, "v");
                        var commit = put(ledger, "k" + i, "v");
                        for (var spin = ThreadLocalRandom.current().nextInt(2_000); spin > 0; spin--) {
                            Thread.onSpinWait();
                        }
                        commit.awaitDurable();
                    }
                }
            });
        }
    }

    @Nested
    @DisplayName("an interrupt")
    class Interrupts {
        @Test
        @DisplayName("of a commit forcing its own group after frames the pool wrote writes them again, and they are durable")
        void forceRewritesUnforced() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var committer = Thread.currentThread();
            var armed = new AtomicBoolean();
            var interrupts = new AtomicInteger();
            var writes = new AtomicInteger();
            disk.intercept((path, kind) -> {
                if (!path.toString().endsWith(".log") || Thread.currentThread() != committer) {
                    return;
                }
                if (kind == ChangeInterceptor.ChangeKind.WRITE) {
                    writes.incrementAndGet();
                } else if (kind == ChangeInterceptor.ChangeKind.FORCE && armed.compareAndSet(true, false)) {
                    var interrupter = Thread.ofPlatform().start(committer::interrupt);
                    try {
                        interrupter.join();
                    } catch (InterruptedException _) {
                        Thread.currentThread().interrupt();
                    }
                    interrupts.incrementAndGet();
                }
            });
            var ledger = open(directory, NEVER);
            warm(ledger);
            for (var i = 0; i < 200; i++) {
                var first = put(ledger, "a" + i, "1");
                awaitWritten(ledger, first);
                writes.set(0);
                armed.set(true);
                var before = interrupts.get();
                var second = put(ledger, "b" + i, "2");
                second.awaitDurable();
                armed.set(false);
                assertTrue(first.isDurable());
                if (interrupts.get() > before) {
                    assertTrue(Thread.interrupted(), "the wait gave the interrupt back");
                    // Everything since the last force written again, after the commit's own group if the flush left that
                    // to it
                    assertTrue(writes.get() >= 1, "the interrupted force wrote nothing again");
                }
                log(ledger).checkHealthy();
            }
            assertTrue(interrupts.get() > 0, "no commit forced its own group");
            var survivor = disk.afterPowerLoss(new Random(5));
            ledger.close();
            try (var recovered = open(survivor.getPath("/store"), NEVER)) {
                for (var i = 0; i < 200; i++) {
                    assertEquals(Optional.of("1"), recovered.get(NAMES, "a" + i));
                    assertEquals(Optional.of("2"), recovered.get(NAMES, "b" + i));
                }
            }
        }

        @Test
        @DisplayName("of a thread waiting for frames the output no longer holds leaves the force to the pool, which goes on")
        void poolForcesWhatOutputLost() throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/store");
            var committer = Thread.currentThread();
            var committerForces = new AtomicInteger();
            var slow = new AtomicBoolean();
            var started = new java.util.concurrent.CountDownLatch(1);
            var released = new java.util.concurrent.CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log")) {
                    if (Thread.currentThread() == committer) {
                        committerForces.incrementAndGet();
                    } else if (slow.get()) {
                        started.countDown();
                        try {
                            released.await();
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
            try (var ledger = open(directory, NEVER)) {
                warm(ledger);
                var forcesBefore = committerForces.get();
                // More than the output starts with, in passes of the flush that leave the frames not forced
                Commit last = null;
                for (var i = 0; i < 10; i++) {
                    last = put(ledger, "k" + i, "v".repeat(3_000));
                    awaitWritten(ledger, last);
                }
                slow.set(true);
                var waited = last;
                var waiter = Thread.ofPlatform().start(() -> {
                    try {
                        waited.awaitDurable();
                        throw new AssertionError("the wait ended without an interrupt");
                    } catch (InterruptedException _) {
                        // Expected
                    }
                });
                assertTrue(started.await(SOON.toSeconds(), TimeUnit.SECONDS), "no force on the pool");
                waiter.interrupt();
                waiter.join(SOON.toMillis());
                assertFalse(waiter.isAlive());
                slow.set(false);
                released.countDown();
                assertTrue(waited.awaitDurable(SOON));
                assertEquals(forcesBefore, committerForces.get());
                log(ledger).checkHealthy();
                put(ledger, "after", "1").awaitDurable();
            }
        }
    }

    @Nested
    @DisplayName("the delayed force")
    class Timer {
        @Test
        @DisplayName("is not held back by a flush that commits keep busy on the store's one thread")
        void busyFlush() throws Exception {
            var disk = CrashFileSystem.create();
            var slow = new AtomicBoolean();
            var writes = new AtomicInteger();
            // Each write outlasts the delay, so time counts in writes, whatever else the machine is doing
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && slow.get() && path.toString().endsWith(".log")) {
                    writes.incrementAndGet();
                    try {
                        Thread.sleep(25);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            try (var ledger = open(disk.getPath("/store"), Duration.ofMillis(20))) {
                warm(ledger);
                slow.set(true);
                var stop = new AtomicBoolean();
                var committer = Thread.ofPlatform().start(() -> {
                    for (var i = 0; !stop.get(); i++) {
                        put(ledger, "steady" + i, "v");
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException _) {
                            return;
                        }
                    }
                });
                try {
                    for (var round = 0; round < 5; round++) {
                        var commit = put(ledger, "probe" + round, "v");
                        var start = writes.get();
                        while (!commit.isDurable()) {
                            // The pass under way, the commit's own, the padding of the force, and as many again
                            assertTrue(writes.get() - start <= 8, "the delay passed " + (writes.get() - start) + " times over with commit " + round + " not forced");
                            Thread.sleep(1);
                        }
                    }
                } finally {
                    stop.set(true);
                    slow.set(false);
                    committer.join();
                }
            }
        }

        @Test
        @DisplayName("finds nothing to do after a close")
        void afterClose() throws Exception {
            var disk = CrashFileSystem.create();
            var ledger = open(disk.getPath("/store"), Duration.ofMillis(30));
            warm(ledger);
            var commit = put(ledger, "a", "1");
            awaitWritten(ledger, commit);
            ledger.close();
            var changes = disk.changes();
            Thread.sleep(150);
            assertEquals(changes, disk.changes());
            assertTrue(commit.isDurable());
            log(ledger).checkHealthy();
        }

        @Test
        @DisplayName("writes and forces nothing after a failed force, and every wait reports it")
        void afterFailure() throws Exception {
            var disk = CrashFileSystem.create();
            var fail = new AtomicBoolean();
            var attempts = new AtomicInteger();
            disk.intercept((path, kind) -> {
                if (fail.get() && path.toString().endsWith(".log")) {
                    if (kind == ChangeInterceptor.ChangeKind.FORCE) {
                        attempts.incrementAndGet();
                        throw new IOException("The device failed");
                    }
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && attempts.get() > 0) {
                        attempts.incrementAndGet();
                    }
                }
            });
            var ledger = open(disk.getPath("/store"), Duration.ofMillis(5));
            try {
                warm(ledger);
                fail.set(true);
                var commit = put(ledger, "a", "1");
                assertSoon(() -> {
                    try {
                        log(ledger).checkHealthy();
                        return false;
                    } catch (LedgerIoException _) {
                        return true;
                    }
                }, "the failed force did not stop the log");
                var seen = attempts.get();
                assertEquals(1, seen);
                Thread.sleep(50);
                assertEquals(seen, attempts.get(), "the log went on after the failed force");
                assertThrows(LedgerIoException.class, commit::isDurable);
                assertThrows(LedgerIoException.class, commit::awaitDurable);
                assertThrows(LedgerIoException.class, () -> put(ledger, "b", "2"));
            } finally {
                fail.set(false);
                try {
                    ledger.close();
                } catch (LedgerIoException _) {
                    // The close reports the failed force
                }
            }
        }
    }

    @Nested
    @DisplayName("the option")
    class Option {
        @Test
        @DisplayName("takes zero to a minute")
        void range() {
            var options = Ledger.builder().memory();
            assertThrows(IllegalArgumentException.class, () -> options.durabilityDelay(Duration.ofMillis(-1)));
            assertThrows(IllegalArgumentException.class, () -> options.durabilityDelay(Duration.ofSeconds(61)));
            options.durabilityDelay(Duration.ZERO);
            options.durabilityDelay(Duration.ofMinutes(1));
        }
    }
}
