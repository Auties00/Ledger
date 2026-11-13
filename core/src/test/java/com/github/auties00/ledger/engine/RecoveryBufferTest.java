package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Recovery reading the log through one buffer that passes from segment to segment and opening each frame where the
/// buffer holds it: frames across a refill of the buffer, frames longer than it, segments shorter and longer than it,
/// and the torn or damaged frames among them, which end their segment as before. No frame is opened ahead
/// ([FrameReadAhead], which FrameReadAheadTest covers), so recovery opens every encrypted frame through the buffer too.
class RecoveryBufferTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int BUFFER = 1 << 20;
    private static final long LARGE_SEGMENT = 8L << 20;
    private static final long SMALL_SEGMENT = 256L << 10;

    @BeforeEach
    void noReadAhead() {
        FrameReadAhead.framesForTests(0);
    }

    @AfterEach
    void defaultReadAhead() {
        FrameReadAhead.framesForTests(-1);
    }

    private static Ledger open(Path directory, boolean encrypted, long segmentSize) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).checkpointEvery(1L << 40))
                .checkpointHeap(1L << 40)
                .segmentSize(segmentSize)
                .build();
    }

    private static TreeMap<String, String> contents(Ledger ledger) {
        var entries = new TreeMap<String, String>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            entries.put(key, value);
            return true;
        });
        return entries;
    }

    private static String text(Random random, int length) {
        var chars = new char[length];
        for (var i = 0; i < length; i++) {
            chars[i] = (char) ('a' + random.nextInt(26));
        }
        return new String(chars);
    }

    /// The state after each commit of a workload, from the empty store at index 0.
    private static final class History {
        private final List<TreeMap<String, String>> states = new ArrayList<>(List.of(new TreeMap<>()));

        TreeMap<String, String> last() {
            return states.getLast();
        }

        TreeMap<String, String> after(long commits) {
            return states.get((int) commits);
        }

        /// Commits about `bytes` of values of a few KiB each, durably one at a time so each is a frame of its own, and
        /// in the middle one batch of about 1.6 MiB, a frame longer than the buffer.
        void commit(Ledger ledger, Random random, String prefix, long bytes) throws InterruptedException {
            var written = 0L;
            var large = false;
            for (var index = 0; written < bytes; index++) {
                var next = new TreeMap<>(last());
                var batch = ledger.newBatch();
                if (!large && written > bytes / 2) {
                    large = true;
                    for (var i = 0; i < 16; i++) {
                        var key = prefix + "-large-" + i;
                        var value = text(random, 100_000);
                        batch.put(MESSAGES, key, value);
                        next.put(key, value);
                    }
                    written += 1_600_000;
                } else {
                    var key = prefix + "-" + index;
                    var value = text(random, 500 + random.nextInt(12_000));
                    batch.put(MESSAGES, key, value);
                    next.put(key, value);
                    written += value.length();
                }
                ledger.commit(batch).awaitDurable();
                states.add(next);
            }
        }
    }

    /// A frame of a segment as the writer laid it out.
    ///
    /// @param position where its header starts
    /// @param sealedLength the length of its sealed body
    /// @param sequence the sequence its header carries
    /// @param type its type
    private record Frame(long position, int sealedLength, long sequence, byte type) {
        long bodyStart() {
            return position + LogWriter.FRAME_HEADER;
        }

        long end() {
            return bodyStart() + sealedLength;
        }

        boolean holdsCommits() {
            return type == FrameBody.COMMIT || type == FrameBody.COMMITS;
        }
    }

    /// {@return the frames of the segment at `path`, up to the first that is not whole, skipping the padding after
    /// each group}
    private static List<Frame> frames(Path path) throws IOException {
        var data = Files.readAllBytes(path);
        var frames = new ArrayList<Frame>();
        var position = (long) LogWriter.SEGMENT_HEADER;
        while (position + LogWriter.FRAME_HEADER <= data.length) {
            if (position % LogWriter.ALIGNMENT != 0 && data[(int) position] == 0) {
                position = (position + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT;
                continue;
            }
            var header = ByteBuffer.wrap(data, (int) position, LogWriter.FRAME_HEADER).slice();
            if (header.getInt(0) != LogWriter.MAGIC) {
                break;
            }
            var frame = new Frame(position, header.getInt(4), header.getLong(8), header.get(16));
            if (frame.end() > data.length) {
                break;
            }
            frames.add(frame);
            position = frame.end();
        }
        return frames;
    }

    /// {@return the segments of the store at `store`, in the order of their numbers}
    private static List<Path> segments(Path store) throws IOException {
        try (var listing = Files.list(store.resolve("log"))) {
            return listing.filter(path -> LogFiles.segmentNumber(path.getFileName().toString()) >= 0)
                    .sorted()
                    .toList();
        }
    }

    /// {@return the segment of `segments` holding the most frames}
    private static Path fullest(List<Path> segments) throws IOException {
        Path fullest = null;
        var most = -1;
        for (var segment : segments) {
            var count = frames(segment).size();
            if (count > most) {
                most = count;
                fullest = segment;
            }
        }
        return fullest;
    }

    private static void truncate(Path path, long size) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.truncate(size);
        }
    }

    private static void flip(Path path, long position) throws IOException {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var one = ByteBuffer.allocate(1);
            channel.read(one, position);
            one.put(0, (byte) (one.get(0) ^ 0x5A));
            one.rewind();
            channel.write(one, position);
        }
    }

    /// What a process crash left of a workload: the disk, the state after each commit, and the segment holding the
    /// workload's frames.
    private record Crashed(CrashFileSystem survivor, History history, Path segment) {
    }

    /// {@return whether the body of `frame` crosses the end of the first buffer, which a reader fills from the start of
    /// the segment, so only a refill reads all of it}
    private static boolean crossesBuffer(Frame frame) {
        return frame.holdsCommits() && frame.sealedLength() <= BUFFER && frame.bodyStart() < BUFFER && frame.end() > BUFFER;
    }

    /// Runs the workload of a single large segment on a new disk and returns what a process crash leaves of it, drawing
    /// values until the body of a frame crosses the end of the first buffer.
    private static Crashed crashedLargeSegment(boolean encrypted) throws Exception {
        for (var seed = 0; ; seed++) {
            var history = new History();
            var disk = CrashFileSystem.create();
            CrashFileSystem survivor;
            try (var ledger = open(disk.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                history.commit(ledger, new Random(seed * 2L + (encrypted ? 1 : 0)), "m", 4L << 20);
                survivor = disk.afterProcessCrash();
            }
            var segment = fullest(segments(survivor.getPath("/store")));
            if (frames(segment).stream().anyMatch(RecoveryBufferTest::crossesBuffer)) {
                return new Crashed(survivor, history, segment);
            }
            assertTrue(seed < 32, "no workload wrote a frame across the end of the first buffer");
        }
    }

    @Nested
    @DisplayName("a store that stopped without closing")
    class Replay {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("replays every commit, whose frames cross refills of the buffer and one is longer than it")
        void crossesRefills(boolean encrypted) throws Exception {
            var crashed = crashedLargeSegment(encrypted);
            var frames = frames(crashed.segment());
            assertTrue(frames.stream().anyMatch(frame -> frame.sealedLength() > BUFFER), "no frame is longer than the buffer");
            var survivor = crashed.survivor();
            var history = crashed.history();
            try (var ledger = open(survivor.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                assertEquals(history.last(), contents(ledger));
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("replays segments shorter than the buffer, then one longer, which needs a buffer of full length")
        void shortThenLong(boolean encrypted) throws Exception {
            var history = new History();
            var random = new Random(encrypted ? 23 : 29);
            var disk = CrashFileSystem.create();
            var ledger = open(disk.getPath("/store"), encrypted, SMALL_SEGMENT);
            CrashFileSystem first;
            try {
                // Smaller values, so none of the large batch is needed to fill several short segments
                for (var i = 0; i < 120; i++) {
                    var next = new TreeMap<>(history.last());
                    var key = "s-" + i;
                    var value = text(random, 1_000 + random.nextInt(4_000));
                    ledger.commit(ledger.newBatch().put(MESSAGES, key, value)).awaitDurable();
                    next.put(key, value);
                    history.states.add(next);
                }
                first = disk.afterProcessCrash();
            } finally {
                ledger.close();
            }
            CrashFileSystem second;
            ledger = open(first.getPath("/store"), encrypted, LARGE_SEGMENT);
            try {
                history.commit(ledger, random, "l", 3L << 20);
                second = first.afterProcessCrash();
            } finally {
                ledger.close();
            }
            var segments = segments(second.getPath("/store"));
            var sizes = new ArrayList<Long>();
            for (var segment : segments) {
                if (!frames(segment).isEmpty()) {
                    sizes.add(Files.size(segment));
                }
            }
            assertTrue(sizes.size() >= 3, "too few segments hold frames: " + sizes);
            assertTrue(sizes.getFirst() < BUFFER, "the first segment is not shorter than the buffer: " + sizes);
            assertTrue(sizes.getLast() > BUFFER, "the last segment is not longer than the buffer: " + sizes);
            try (var reopened = open(second.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                assertEquals(history.last(), contents(reopened));
            }
        }
    }

    @Nested
    @DisplayName("a frame that ends the log")
    class LastFrame {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("damaged past the end of the first buffer, so only a refill reads the damage, ends its segment")
        void damagedAcrossRefill(boolean encrypted) throws Exception {
            var crashed = crashedLargeSegment(encrypted);
            var survivor = crashed.survivor();
            var history = crashed.history();
            var segment = crashed.segment();
            var frame = frames(segment).stream()
                    .filter(RecoveryBufferTest::crossesBuffer)
                    .findFirst()
                    .orElseThrow();
            flip(segment, BUFFER);
            truncate(segment, frame.end());
            try (var ledger = open(survivor.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                assertEquals(history.after(frame.sequence() - 1), contents(ledger));
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("cut short across the end of the first buffer ends its segment")
        void tornAcrossRefill(boolean encrypted) throws Exception {
            var crashed = crashedLargeSegment(encrypted);
            var survivor = crashed.survivor();
            var history = crashed.history();
            var segment = crashed.segment();
            var frame = frames(segment).stream()
                    .filter(RecoveryBufferTest::crossesBuffer)
                    .findFirst()
                    .orElseThrow();
            truncate(segment, frame.end() - 1);
            try (var ledger = open(survivor.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                assertEquals(history.after(frame.sequence() - 1), contents(ledger));
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("longer than the buffer and damaged ends its segment")
        void damagedLongerThanBuffer(boolean encrypted) throws Exception {
            var crashed = crashedLargeSegment(encrypted);
            var survivor = crashed.survivor();
            var history = crashed.history();
            var segment = crashed.segment();
            var frame = frames(segment).stream()
                    .filter(candidate -> candidate.holdsCommits() && candidate.sealedLength() > BUFFER)
                    .findFirst()
                    .orElseThrow();
            flip(segment, frame.bodyStart() + frame.sealedLength() / 2);
            truncate(segment, frame.end());
            try (var ledger = open(survivor.getPath("/store"), encrypted, LARGE_SEGMENT)) {
                assertEquals(history.after(frame.sequence() - 1), contents(ledger));
            }
        }
    }

    @Nested
    @DisplayName("the reader of a segment")
    class Reader {
        /// Writes `length` bytes counting up from `seed` to a new file of `disk` and returns them.
        private static byte[] write(Path path, int length, int seed) throws IOException {
            var data = new byte[length];
            for (var i = 0; i < length; i++) {
                data[i] = (byte) (seed + i * 31);
            }
            Files.write(path, data);
            return data;
        }

        @Test
        @DisplayName("views bytes inside the buffer, across its end and of its full length as the file holds them")
        void views() throws IOException {
            var disk = CrashFileSystem.create();
            var path = disk.getPath("/segment");
            var data = write(path, 3 * BUFFER, 7);
            try (var reader = new LogRecovery.SegmentReader(path, null)) {
                assertEquals(BUFFER, reader.buffer().length);
                var cases = new long[][]{{0, 100}, {500, 1_000}, {BUFFER - 10, 100}, {BUFFER + 5, BUFFER}, {2 * BUFFER + 4, BUFFER - 4},
                        {BUFFER, 1}};
                for (var span : cases) {
                    var position = span[0];
                    var count = (int) span[1];
                    var at = reader.view(position, count);
                    assertTrue(at >= 0, "position " + position);
                    assertArrayEquals(Arrays.copyOfRange(data, (int) position, (int) position + count),
                            Arrays.copyOfRange(reader.buffer(), at, at + count), "position " + position);
                }
                assertEquals(-1, reader.view(3L * BUFFER - 10, 11));
                assertEquals(-1, reader.view(-1, 1));
            }
        }

        @Test
        @DisplayName("ends a view or a read at a file shorter than when it opened, rather than give the bytes of an earlier fill")
        void shortRefill() throws IOException {
            var disk = CrashFileSystem.create();
            var path = disk.getPath("/segment");
            write(path, 2 * BUFFER, 3);
            try (var reader = new LogRecovery.SegmentReader(path, null)) {
                assertTrue(reader.view(0, 16) >= 0);
                truncate(path, BUFFER + BUFFER / 2);
                assertEquals(-1, reader.view(BUFFER + BUFFER / 4, BUFFER / 2));
                assertFalse(reader.read(BUFFER + BUFFER / 4, new byte[BUFFER / 2], 0, BUFFER / 2));
                assertFalse(reader.read(BUFFER + BUFFER / 2 - 8, new byte[16], 0, 16));
                var whole = new byte[16];
                assertTrue(reader.read(BUFFER + BUFFER / 2 - 16, whole, 0, 16));
            }
        }

        @Test
        @DisplayName("takes the buffer of an earlier reader when it is long enough, and reads none of what that reader left in it")
        void sharedBuffer() throws IOException {
            var disk = CrashFileSystem.create();
            var full = disk.getPath("/full");
            var small = disk.getPath("/small");
            var larger = disk.getPath("/larger");
            write(full, 2 * BUFFER, 9);
            var smallData = new byte[3];
            Files.write(small, smallData);
            var largerData = write(larger, BUFFER + 100, 13);
            byte[] buffer;
            try (var reader = new LogRecovery.SegmentReader(full, null)) {
                assertTrue(reader.view(10, BUFFER - 20) >= 0);
                buffer = reader.buffer();
            }
            try (var reader = new LogRecovery.SegmentReader(small, buffer)) {
                assertSame(buffer, reader.buffer());
                var read = new byte[3];
                assertTrue(reader.read(0, read, 0, 3));
                assertArrayEquals(smallData, read);
                assertTrue(reader.isZero(0, 4096), "bytes past the end count as zero, whatever the buffer held");
                assertFalse(reader.read(1, new byte[3], 0, 3));
                assertEquals(-1, reader.view(0, 4));
            }
            byte[] shorter;
            try (var reader = new LogRecovery.SegmentReader(small, null)) {
                shorter = reader.buffer();
                assertEquals(3, shorter.length);
            }
            try (var reader = new LogRecovery.SegmentReader(larger, shorter)) {
                assertNotSame(shorter, reader.buffer());
                assertEquals(BUFFER, reader.buffer().length);
                var at = reader.view(BUFFER - 50, 150);
                assertTrue(at >= 0);
                assertArrayEquals(Arrays.copyOfRange(largerData, BUFFER - 50, BUFFER + 100),
                        Arrays.copyOfRange(reader.buffer(), at, at + 150));
                var direct = new byte[BUFFER + 100];
                assertTrue(reader.read(0, direct, 0, direct.length), "a read longer than the buffer goes to the file");
                assertArrayEquals(largerData, direct);
            }
        }
    }
}
