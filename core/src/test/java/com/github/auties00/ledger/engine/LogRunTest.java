package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// The runs of commits the log seals as one frame: what the writer puts in a segment, what recovery replays of it
/// whole, torn, damaged, malformed or cut short by a missing unit, and what a store recovers of groups of runs after a
/// crash, with and without encryption.
///
/// The writer of the log tests runs its flush on the thread that appends, so each batch it is given is one group,
/// written and forced before the append returns.
class LogRunTest {
    private static final byte[] KEY = new byte[32];
    private static final byte[] STORE_ID = new byte[16];
    private static final byte[] SALT = new byte[16];
    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(NAMES, CHATS);

    static {
        for (var i = 0; i < KEY.length; i++) {
            KEY[i] = (byte) (i * 11 + 3);
        }
        for (var i = 0; i < STORE_ID.length; i++) {
            STORE_ID[i] = (byte) (i * 5 + 7);
            SALT[i] = (byte) (i * 13 + 1);
        }
    }

    /// A frame header found in a segment.
    private record Frame(int offset, byte type, long sequence, int sealedLength, boolean groupStart) {
        int bodyAt() {
            return offset + LogWriter.FRAME_HEADER;
        }

        int end() {
            return bodyAt() + sealedLength;
        }
    }

    /// {@return the frames the use of `segment` that wrote its first frame wrote, in order, found as recovery finds them}
    private static List<Frame> frames(Path segment) throws IOException {
        var bytes = Files.readAllBytes(segment);
        var frames = new ArrayList<Frame>();
        var position = LogWriter.SEGMENT_HEADER;
        var crc = new CRC32C();
        var useTag = 0L;
        while (position + LogWriter.FRAME_HEADER <= bytes.length) {
            if (position % LogWriter.ALIGNMENT != 0) {
                var boundary = Math.min((position + LogWriter.ALIGNMENT - 1) / LogWriter.ALIGNMENT * LogWriter.ALIGNMENT, bytes.length);
                if (isZero(bytes, position, boundary)) {
                    position = boundary;
                    continue;
                }
            }
            crc.reset();
            crc.update(bytes, position, LogWriter.FRAME_HEADER - Integer.BYTES);
            if (Bytes.i32(bytes, position) != LogWriter.MAGIC
                    || Bytes.i32(bytes, position + LogWriter.FRAME_HEADER - Integer.BYTES) != (int) crc.getValue()) {
                break;
            }
            var tag = Bytes.i64(bytes, position + LogWriter.USE_TAG_OFFSET);
            if (frames.isEmpty()) {
                useTag = tag;
            } else if (tag != useTag) {
                break;
            }
            var frame = new Frame(position, bytes[position + 16], Bytes.i64(bytes, position + 8), Bytes.i32(bytes, position + 4),
                    (bytes[position + 17] & LogWriter.GROUP_START) != 0);
            frames.add(frame);
            position = frame.end();
        }
        return frames;
    }

    /// {@return the frames of the log in `directory`, of every segment, by segment then offset}
    private static List<Frame> logFrames(Path directory) throws IOException {
        var frames = new ArrayList<Frame>();
        try (var files = Files.list(directory)) {
            for (var file : files.sorted().toList()) {
                if (LogFiles.segmentNumber(file.getFileName().toString()) >= 0) {
                    frames.addAll(frames(file));
                }
            }
        }
        return frames;
    }

    private static boolean isZero(byte[] bytes, int from, int to) {
        for (var i = from; i < to; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return true;
    }

    /// {@return the bytes of the varint of `value`}
    private static byte[] varInt(long value) {
        var out = new ByteArrayOutputStream();
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
        return out.toByteArray();
    }

    /// {@return the commit bodies of the run body `run`}
    private static List<byte[]> entries(byte[] run) {
        var entries = new ArrayList<byte[]>();
        var position = 0;
        while (position < run.length) {
            var length = 0;
            for (var shift = 0; ; shift += 7) {
                var next = run[position++];
                length |= (next & 0x7F) << shift;
                if ((next & 0x80) == 0) {
                    break;
                }
            }
            entries.add(Arrays.copyOfRange(run, position, position + length));
            position += length;
        }
        return entries;
    }

    /// A log in a directory, written group by group by a writer that runs its flush on the appending thread, and the
    /// commit bodies it was given, by sequence from 1.
    private static final class Log {
        final Path directory;
        final StoreKeys keys;
        final LogFiles files;
        final LogWriter writer;
        final List<byte[]> bodies;

        Log(Path directory, boolean encrypted, long segmentSize) throws IOException {
            this(directory, encrypted, segmentSize, WriteThrough.Mode.OFF);
        }

        /// Starts a log that writes through in `mode`, into a first segment written with zeros unless that is
        /// [WriteThrough.Mode#OFF].
        Log(Path directory, boolean encrypted, long segmentSize, WriteThrough.Mode mode) throws IOException {
            Files.createDirectories(directory);
            this.directory = directory;
            this.keys = StoreKeys.derive(encrypted ? EncryptionSpec.key(KEY) : EncryptionSpec.NONE, SALT, 0);
            this.files = new LogFiles(directory, keys, STORE_ID);
            var empty = mode == WriteThrough.Mode.OFF ? null : files.prepare(1, segmentSize, true, true, () -> false);
            this.writer = new LogWriter(files, keys, STORE_ID, 1, 0, segmentSize, Runnable::run, null, Duration.ZERO, empty,
                    WriteThrough.of(mode, directory));
            this.bodies = new ArrayList<>();
        }

        /// Writes one group of commits whose bodies take each of `sizes` bytes.
        void group(int... sizes) {
            var batch = new LogWriter.Batch();
            for (var size : sizes) {
                add(batch, size);
            }
            writer.appendAll(batch);
        }

        /// Writes one group of `count` commits whose bodies take `size` bytes each.
        void group(int count, int size) {
            var sizes = new int[count];
            Arrays.fill(sizes, size);
            group(sizes);
        }

        /// Adds to `batch` the next commit, with a body of `size` bytes; one of 0 bytes is the empty body of a commit
        /// whose writes were taken back.
        void add(LogWriter.Batch batch, int size) {
            var body = new FrameBody(size);
            if (size > 0) {
                var sequence = bodies.size() + 1;
                var key = ("key" + sequence).getBytes(StandardCharsets.US_ASCII);
                var value = new byte[Math.max(1, size - key.length - 6)];
                Arrays.fill(value, (byte) sequence);
                body.put(1, MemorySegment.ofArray(key), MemorySegment.ofArray(value));
            }
            bodies.add(Arrays.copyOf(body.array(), body.size()));
            batch.add(FrameBody.COMMIT, bodies.size(), body, 0, size > 0 ? 1 : 0, false);
        }

        /// Adds `body` to `batch` as the next commit, recording the bytes it holds.
        void add(LogWriter.Batch batch, FrameBody body) {
            bodies.add(body.toBytes());
            batch.add(FrameBody.COMMIT, bodies.size(), body, 0, body.size() > 0 ? 1 : 0, false);
        }

        /// {@return the sequence of the last commit given}
        long last() {
            return bodies.size();
        }

        /// {@return the bytes sealing adds to a body}
        int overhead() {
            return keys.encrypted() ? LogWriter.TAG_BYTES : Integer.BYTES;
        }

        /// {@return the frames of every segment, by segment number}
        TreeMap<Long, List<Frame>> frames() throws IOException {
            var frames = new TreeMap<Long, List<Frame>>();
            for (var number : files.segments()) {
                frames.put(number, LogRunTest.frames(files.segment(number)));
            }
            return frames;
        }

        /// {@return the key the frame at `offset` of segment `number`, whose bytes are `bytes`, is sealed under}
        SecretKeySpec key(long number, byte[] bytes, int offset) {
            var salt = Arrays.copyOfRange(bytes, LogWriter.SALT_OFFSET, LogWriter.SALT_OFFSET + LogWriter.SALT_BYTES);
            return keys.segmentKey(STORE_ID, number, salt, Bytes.i64(bytes, offset + LogWriter.USE_TAG_OFFSET));
        }

        /// {@return the body of `frame` of segment `number`, opened out of place}
        byte[] open(long number, Frame frame) throws IOException, GeneralSecurityException {
            var bytes = Files.readAllBytes(files.segment(number));
            if (keys.encrypted()) {
                var cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.DECRYPT_MODE, key(number, bytes, frame.offset()), new GCMParameterSpec(128, LogWriter.nonce(frame.offset())));
                cipher.updateAAD(bytes, frame.offset(), LogWriter.FRAME_HEADER);
                return cipher.doFinal(bytes, frame.bodyAt(), frame.sealedLength());
            }
            var length = frame.sealedLength() - Integer.BYTES;
            var crc = new CRC32C();
            crc.update(bytes, frame.offset(), LogWriter.FRAME_HEADER + length);
            assertEquals((int) crc.getValue(), Bytes.i32(bytes, frame.bodyAt() + length), "the CRC of the frame at " + frame.offset());
            return Arrays.copyOfRange(bytes, frame.bodyAt(), frame.bodyAt() + length);
        }

        /// {@return `body` sealed out of place, as the frame header `header` at `offset` of segment `number`, whose
        /// bytes are `bytes`, has it sealed}
        byte[] seal(long number, byte[] bytes, int offset, byte[] header, byte[] body) throws GeneralSecurityException {
            if (keys.encrypted()) {
                var cipher = Cipher.getInstance("AES/GCM/NoPadding");
                cipher.init(Cipher.ENCRYPT_MODE, key(number, bytes, offset), new GCMParameterSpec(128, LogWriter.nonce(offset)));
                cipher.updateAAD(header);
                return cipher.doFinal(body);
            }
            var crc = new CRC32C();
            crc.update(header);
            crc.update(body);
            var sealed = Arrays.copyOf(body, body.length + Integer.BYTES);
            Bytes.i32(sealed, body.length, (int) crc.getValue());
            return sealed;
        }

        /// Replaces the frame at `frame` of segment `number`, and everything after it, with a frame of type `type` and
        /// the sequence of `frame`, holding `body` sealed under the key of its segment.
        void rewrite(long number, Frame frame, byte type, byte[] body) throws IOException, GeneralSecurityException {
            var path = files.segment(number);
            var bytes = Files.readAllBytes(path);
            var header = Arrays.copyOfRange(bytes, frame.offset(), frame.bodyAt());
            Bytes.i32(header, 4, body.length + overhead());
            header[16] = type;
            var crc = new CRC32C();
            crc.update(header, 0, LogWriter.FRAME_HEADER - Integer.BYTES);
            Bytes.i32(header, LogWriter.FRAME_HEADER - Integer.BYTES, (int) crc.getValue());
            var sealed = seal(number, bytes, frame.offset(), header, body);
            var rewritten = Arrays.copyOf(bytes, frame.bodyAt() + sealed.length);
            System.arraycopy(header, 0, rewritten, frame.offset(), header.length);
            System.arraycopy(sealed, 0, rewritten, frame.bodyAt(), sealed.length);
            Files.write(path, rewritten);
        }

        /// {@return what recovery replays of the log, skipping the commits up to `checkpoint`}
        Replayed replay(long checkpoint) throws IOException {
            return replay(checkpoint, -1);
        }

        /// {@return what recovery replays of the log, skipping the commits up to `checkpoint`, with the file of large
        /// values lacking a unit of the commit `missing`}
        Replayed replay(long checkpoint, long missing) throws IOException {
            return replay(files, checkpoint, missing);
        }

        /// {@return what recovery replays of the log in `files`, skipping the commits up to `checkpoint`, with the file
        /// of large values lacking a unit of the commit `missing`}
        Replayed replay(LogFiles files, long checkpoint, long missing) throws IOException {
            var replayed = new Replayed(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new long[1]);
            var result = LogRecovery.replay(files, keys, STORE_ID, 0, checkpoint, new LogRecovery.Sink() {
                @Override
                public void catalog(int keyspace, String name) {
                    replayed.catalog().add(name);
                }

                @Override
                public boolean commit(long sequence, byte[] body) {
                    if (sequence == missing) {
                        return false;
                    }
                    replayed.sequences().add(sequence);
                    replayed.bodies().add(body);
                    return true;
                }

                @Override
                public void epoch(int epoch) {
                }
            });
            replayed.last()[0] = result.lastSequence();
            return replayed;
        }

        /// Checks that `replayed` holds the commits from `first` to `last`, with the bodies they were given, and ends at
        /// `last`.
        void assertReplayed(Replayed replayed, long first, long last) {
            var expected = new ArrayList<Long>();
            for (var sequence = first; sequence <= last; sequence++) {
                expected.add(sequence);
            }
            assertEquals(expected, replayed.sequences());
            for (var i = 0; i < replayed.bodies().size(); i++) {
                assertArrayEquals(bodies.get((int) (first + i - 1)), replayed.bodies().get(i), "the body of commit " + (first + i));
            }
            assertEquals(Math.max(first - 1, last), replayed.last()[0]);
        }

        void close() {
            writer.close();
        }
    }

    /// What a replay passed to its sink, and the last sequence it ended at.
    private record Replayed(List<Long> sequences, List<byte[]> bodies, List<String> catalog, long[] last) {
    }

    @Nested
    @DisplayName("the writer")
    class Writer {
        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("seals the small commits of a group that follow on as runs, ended by other frames and large bodies, in place as one seal out of place would")
        void runs(boolean encrypted, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            var catalog = new FrameBody();
            catalog.catalog(1, "names");
            // 1; 2 to 301; 302 to 312 with 307 large, 309 empty and a catalog after 310; 313 to 2,312
            log.group(120);
            log.group(300, 120);
            var batch = new LogWriter.Batch();
            for (var size : new int[]{100, 100, 100, 100, 100, 20_000, 100, 0, 100}) {
                log.add(batch, size);
            }
            batch.add(FrameBody.CATALOG, log.last(), catalog, 0, 0, false);
            log.add(batch, 100);
            log.add(batch, 100);
            log.writer.appendAll(batch);
            log.group(2_000, 100);
            log.close();

            var segments = log.frames();
            var number = segments.firstKey();
            var frames = segments.get(number);
            var shapes = frames.stream().limit(8).map(frame -> frame.type() + "@" + frame.sequence() + (frame.groupStart() ? "*" : "")).toList();
            assertEquals(List.of("1@1*", "4@2*", "4@302*", "1@307", "4@308", "2@310", "4@311", "4@313*"), shapes);
            var bytes = Files.readAllBytes(log.files.segment(number));
            var next = 1L;
            for (var frame : frames) {
                var body = log.open(number, frame);
                byte[] expected;
                if (frame.type() == FrameBody.COMMITS) {
                    var entries = entries(body);
                    assertTrue(entries.size() > 1, () -> "a run of " + entries.size() + " at " + frame.sequence());
                    assertTrue(body.length <= LogWriter.RUN_BYTES, () -> body.length + " bytes in the run at " + frame.sequence());
                    assertEquals(next, frame.sequence());
                    var plain = new ByteArrayOutputStream();
                    for (var i = 0; i < entries.size(); i++) {
                        var commit = log.bodies.get((int) (next + i - 1));
                        assertTrue(commit.length <= LogWriter.RUN_MAX_BODY);
                        plain.writeBytes(varInt(commit.length));
                        plain.writeBytes(commit);
                    }
                    expected = plain.toByteArray();
                    next += entries.size();
                } else if (frame.type() == FrameBody.COMMIT) {
                    assertEquals(next, frame.sequence());
                    expected = log.bodies.get((int) next - 1);
                    next++;
                } else {
                    expected = Arrays.copyOf(catalog.array(), catalog.size());
                }
                assertArrayEquals(expected, body, "the body of the frame at " + frame.offset());
                var header = Arrays.copyOfRange(bytes, frame.offset(), frame.bodyAt());
                assertArrayEquals(log.seal(number, bytes, frame.offset(), header, expected), Arrays.copyOfRange(bytes, frame.bodyAt(), frame.end()),
                        "the frame at " + frame.offset() + " sealed in place");
            }
            assertEquals(log.last() + 1, next);
            assertTrue(frames.size() < 15, () -> frames.size() + " frames for " + log.last() + " commits");

            var replayed = log.replay(0);
            log.assertReplayed(replayed, 1, log.last());
            assertEquals(List.of("names"), replayed.catalog());
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("writes groups of runs through to stable storage with no force, whole after a loss of power")
        void writtenThrough(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var directory = disk.getPath("/log");
            var log = new Log(directory, encrypted, 1L << 20, WriteThrough.Mode.DSYNC);
            try {
                log.group(300, 120);
                log.group(1_000, 100);
                assertEquals(2, log.writer.writtenThrough(), "the groups written through");
                var shapes = log.frames().get(1L).stream().map(Frame::type).distinct().toList();
                assertEquals(List.of(FrameBody.COMMITS), shapes);
                var survivor = disk.afterPowerLoss(new Random(encrypted ? 1 : 2));
                var files = new LogFiles(survivor.getPath("/log"), log.keys, STORE_ID);
                log.assertReplayed(log.replay(files, 0, -1), 1, log.last());
            } finally {
                log.close();
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("ends a run where the segment ends, and starts the next segment with the rest of the group")
        void rollover(boolean encrypted, @TempDir Path directory) throws Exception {
            var segmentSize = 16_384;
            var log = new Log(directory, encrypted, segmentSize);
            log.group(400, 100);
            log.group(50, 100);
            // A frame larger than a segment, then commits after it in the same group
            var batch = new LogWriter.Batch();
            log.add(batch, 20_000);
            for (var i = 0; i < 10; i++) {
                log.add(batch, 100);
            }
            log.writer.appendAll(batch);
            log.close();

            var written = log.frames().entrySet().stream().filter(entry -> !entry.getValue().isEmpty()).toList();
            assertTrue(written.size() >= 4, () -> written.size() + " segments written");
            var withRuns = 0;
            for (var index = 0; index < written.size(); index++) {
                var frames = written.get(index).getValue();
                assertTrue(frames.getFirst().groupStart(), "a segment starts with the first frame of a group");
                var large = frames.stream().anyMatch(frame -> frame.sealedLength() > LogWriter.RUN_MAX_BODY + log.overhead());
                if (!large) {
                    for (var frame : frames) {
                        assertTrue(frame.end() <= segmentSize, () -> "a frame ends at " + frame.end());
                    }
                    var end = frames.getLast().end();
                    var lastSequence = index < written.size() - 1 ? written.get(index + 1).getValue().getFirst().sequence() - 1 : log.last();
                    if (lastSequence != 400 && lastSequence != 450 && lastSequence != log.last()) {
                        // A group that rolled over filled the segment up to less than the frame of a commit alone
                        assertTrue(segmentSize - end < LogWriter.FRAME_HEADER + 100 + log.overhead(), () -> "a segment filled to " + end);
                    }
                }
                if (frames.stream().anyMatch(frame -> frame.type() == FrameBody.COMMITS)) {
                    withRuns++;
                }
            }
            var segmentsWithRuns = withRuns;
            assertTrue(segmentsWithRuns >= 3, () -> segmentsWithRuns + " segments with runs");
            log.assertReplayed(log.replay(0), 1, log.last());
        }
    }

    @Nested
    @DisplayName("recovery")
    class Recovery {
        @ParameterizedTest(name = "encrypted: {0}, checkpoint at {1}")
        @CsvSource({"true, 0", "true, 4", "false, 4", "true, 10", "false, 11", "true, 12"})
        @DisplayName("skips the commits of a run up to the checkpoint and replays the rest")
        void checkpoint(boolean encrypted, long checkpoint, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(10, 80);
            log.group(2, 80);
            log.close();
            log.assertReplayed(log.replay(checkpoint), checkpoint + 1, 12);
        }

        @ParameterizedTest(name = "encrypted: {0}, {1}")
        @CsvSource({"true, cut", "false, cut", "true, flipped", "false, flipped"})
        @DisplayName("drops a torn run of the last group whole, and keeps the groups before it")
        void tornRun(boolean encrypted, String damage, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(20, 80);
            log.group(40, 80);
            var start = log.writer.groupStart();
            var segment = log.files.segment(log.writer.currentSegment());
            log.close();
            if (damage.equals("cut")) {
                try (var channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
                    channel.truncate(start + LogWriter.FRAME_HEADER + 100);
                }
            } else {
                var bytes = Files.readAllBytes(segment);
                bytes[(int) start + LogWriter.FRAME_HEADER + 100] ^= 1;
                Files.write(segment, bytes);
            }
            log.assertReplayed(log.replay(0), 1, 20);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("refuses a damaged run before a group written after a later force")
        void damagedBeforeLaterGroup(boolean encrypted, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(20, 80);
            log.group(10, 80);
            var segment = log.files.segment(log.writer.currentSegment());
            log.close();
            var bytes = Files.readAllBytes(segment);
            bytes[LogWriter.SEGMENT_HEADER + LogWriter.FRAME_HEADER + 50] ^= 0x40;
            Files.write(segment, bytes);
            assertThrows(LedgerCorruptedException.class, () -> log.replay(0));
        }

        @ParameterizedTest(name = "encrypted: {0}, later group: {1}")
        @CsvSource({"true, false", "false, false", "true, true", "false, true"})
        @DisplayName("stops in a run at a commit whose units are missing, and refuses it before a later group")
        void missingUnit(boolean encrypted, boolean laterGroup, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(20, 80);
            if (laterGroup) {
                log.group(10, 80);
            }
            log.close();
            if (laterGroup) {
                assertThrows(LedgerCorruptedException.class, () -> log.replay(0, 8));
            } else {
                log.assertReplayed(log.replay(0, 8), 1, 7);
            }
        }

        @ParameterizedTest(name = "encrypted: {0}, {1}")
        @CsvSource({"true, overrun", "false, overrun", "true, trailing", "false, trailing", "true, empty", "false, empty"})
        @DisplayName("refuses a run that authenticates but holds a malformed entry, or none")
        void malformed(boolean encrypted, String kind, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(5, 80);
            log.close();
            var number = log.files.segments().getFirst();
            var frame = log.frames().get(number).getFirst();
            assertEquals(FrameBody.COMMITS, frame.type());
            var body = log.open(number, frame);
            var malformed = switch (kind) {
                // The first entry claims more bytes than the run holds
                case "overrun" -> {
                    var rest = Arrays.copyOfRange(body, varInt(log.bodies.getFirst().length).length, body.length);
                    var out = new ByteArrayOutputStream();
                    out.writeBytes(varInt(body.length));
                    out.writeBytes(rest);
                    yield out.toByteArray();
                }
                // A length cut off after the last entry
                case "trailing" -> {
                    var longer = Arrays.copyOf(body, body.length + 1);
                    longer[body.length] = (byte) 0x80;
                    yield longer;
                }
                default -> new byte[0];
            };
            log.rewrite(number, frame, FrameBody.COMMITS, malformed);
            assertThrows(LedgerCorruptedException.class, () -> log.replay(0));
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("replays a run of one commit, which the writer never writes, and refuses commits that do not follow on")
        void wellFormedButUnusual(boolean encrypted, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            log.group(80);
            log.close();
            var number = log.files.segments().getFirst();
            var frame = log.frames().get(number).getFirst();
            var body = log.open(number, frame);
            var run = new ByteArrayOutputStream();
            run.writeBytes(varInt(body.length));
            run.writeBytes(body);
            log.rewrite(number, frame, FrameBody.COMMITS, run.toByteArray());
            log.assertReplayed(log.replay(0), 1, 1);
            // A run carrying the sequence after the one its first commit has
            var path = log.files.segment(number);
            var bytes = Files.readAllBytes(path);
            Bytes.i64(bytes, frame.offset() + 8, 2);
            Files.write(path, bytes);
            log.rewrite(number, frame, FrameBody.COMMITS, run.toByteArray());
            assertThrows(LedgerCorruptedException.class, () -> log.replay(0));
        }
    }

    @Nested
    @DisplayName("a store")
    class Store {
        private static Ledger open(Path directory, Encryption encryption, Duration commitWindow) {
            return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(NAMES).keyspace(CHATS)
                    .commitWindow(commitWindow))
                    .segmentSize(16_384)
                    .build();
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("seals the commits of concurrent committers that queued behind a force as runs, which a power loss keeps once durable")
        void queuedBehindForce(boolean encrypted) throws Exception {
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var disk = CrashFileSystem.create();
            var blocked = new AtomicBoolean();
            var forcing = new CountDownLatch(1);
            var released = new CountDownLatch(1);
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.FORCE && path.toString().endsWith(".log") && blocked.get()) {
                    forcing.countDown();
                    try {
                        released.await();
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            var threads = 8;
            var commits = 150;
            var ledger = open(disk.getPath("/store"), encryption, Duration.ZERO);
            try {
                ledger.commit(ledger.newBatch().put(NAMES, "warm", "x")).awaitDurable();
                blocked.set(true);
                ledger.commit(ledger.newBatch().put(NAMES, "first", "blocked"));
                assertTrue(forcing.await(10, TimeUnit.SECONDS), "the first group never reached its force");
                var committers = new ArrayList<Thread>();
                for (var t = 0; t < threads; t++) {
                    var thread = t;
                    committers.add(Thread.ofVirtual().start(() -> {
                        for (var i = 0; i < commits; i++) {
                            var batch = ledger.newBatch().put(NAMES, "n" + thread + "-" + i, "v".repeat(i % 90) + thread);
                            if (i % 3 == 0) {
                                batch.put(CHATS, "c" + thread + "-" + i, "chat" + i);
                            }
                            ledger.commit(batch);
                        }
                    }));
                }
                for (var committer : committers) {
                    committer.join();
                }
                blocked.set(false);
                released.countDown();
                ledger.awaitDurable();
                var runs = logFrames(disk.getPath("/store/log")).stream().filter(frame -> frame.type() == FrameBody.COMMITS).count();
                assertTrue(runs > 0, "no run was written");
            } finally {
                released.countDown();
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            var survivor = disk.afterPowerLoss(new Random(encrypted ? 1 : 2));
            try (var reopened = open(survivor.getPath("/store"), encryption, Duration.ZERO)) {
                assertEquals(2 + threads * commits, reopened.size(NAMES));
                assertEquals(threads * ((commits + 2) / 3), reopened.size(CHATS));
                for (var t = 0; t < threads; t++) {
                    for (var i = 0; i < commits; i++) {
                        assertEquals("v".repeat(i % 90) + t, reopened.get(NAMES, "n" + t + "-" + i).orElseThrow());
                        if (i % 3 == 0) {
                            assertEquals("chat" + i, reopened.get(CHATS, "c" + t + "-" + i).orElseThrow());
                        }
                    }
                }
                assertEquals("blocked", reopened.get(NAMES, "first").orElseThrow());
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("recovers a state between the last durable commit and the last visible one whichever force of a workload of runs fails")
        void everyForce(boolean encrypted) throws Exception {
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var dry = CrashFileSystem.create();
            var probe = new Workload(encryption, 3);
            probe.run(dry.getPath("/store"));
            assertTrue(probe.runs, "the workload wrote no run");
            for (var force = 0; force <= dry.forces(); force++) {
                var disk = CrashFileSystem.create();
                disk.stopAtForce(force);
                var workload = new Workload(encryption, 3);
                workload.run(disk.getPath("/store"));
                var random = new Random(force * 31L + (encrypted ? 1 : 0));
                var survivor = force % 2 == 0 ? disk.afterPowerLoss(random) : disk.afterProcessCrash();
                try (var ledger = open(survivor.getPath("/store"), encryption, Duration.ZERO)) {
                    var recovered = contents(ledger);
                    var match = -1;
                    for (var commit = workload.durable; commit <= workload.visible; commit++) {
                        if (workload.history.get(commit).equals(recovered)) {
                            match = commit;
                            break;
                        }
                    }
                    if (match < 0) {
                        fail("At force " + force + ", the store recovered a state no commit from " + workload.durable + " to "
                                + workload.visible + " left: " + recovered);
                    }
                    for (var keyspace : KEYSPACES) {
                        assertEquals(recovered.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " count");
                    }
                    ledger.commit(ledger.newBatch().put(NAMES, "after", "recovery")).awaitDurable();
                }
                try (var ledger = open(survivor.getPath("/store"), encryption, Duration.ZERO)) {
                    assertEquals("recovery", ledger.get(NAMES, "after").orElseThrow());
                }
            }
        }

        private static Map<String, TreeMap<String, String>> contents(Ledger ledger) {
            var contents = new TreeMap<String, TreeMap<String, String>>();
            for (var keyspace : KEYSPACES) {
                var entries = new TreeMap<String, String>();
                ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                    entries.put(key, value);
                    return true;
                });
                contents.put(keyspace.name(), entries);
            }
            return contents;
        }

        /// Rounds of commits that do not wait for durability followed by one that does, on a store whose flush waits a
        /// millisecond for commits to join its group, and the state after each commit.
        private static final class Workload {
            private final Encryption encryption;
            private final Random random;
            final List<Map<String, TreeMap<String, String>>> history;
            final Map<String, TreeMap<String, String>> state;
            int visible;
            int durable;
            // Whether the log held a run before the store closed
            boolean runs;

            Workload(Encryption encryption, int seed) {
                this.encryption = encryption;
                this.random = new Random(seed);
                this.history = new ArrayList<>();
                this.state = new TreeMap<>();
                for (var keyspace : KEYSPACES) {
                    state.put(keyspace.name(), new TreeMap<>());
                }
                history.add(copy(state));
            }

            /// Runs until it ends or the store fails, which a stopped file system makes it do.
            void run(Path directory) {
                Ledger ledger = null;
                try {
                    ledger = open(directory, encryption, Duration.ofMillis(1));
                    for (var round = 0; round < 5; round++) {
                        for (var i = 0; i < 30; i++) {
                            commit(ledger);
                        }
                        commit(ledger).awaitDurable();
                        durable = visible;
                    }
                    try {
                        runs = logFrames(directory.resolve("log")).stream().anyMatch(frame -> frame.type() == FrameBody.COMMITS);
                    } catch (IOException exception) {
                        throw new java.io.UncheckedIOException(exception);
                    }
                    ledger.close();
                    durable = visible;
                } catch (RuntimeException | InterruptedException failure) {
                    if (ledger != null) {
                        try {
                            ledger.close();
                        } catch (RuntimeException _) {
                            // The file system stopped: closing only lets go of the threads
                        }
                    }
                }
            }

            private com.github.auties00.ledger.Commit commit(Ledger ledger) {
                var next = copy(state);
                var batch = ledger.newBatch();
                for (var i = random.nextInt(3); i >= 0; i--) {
                    var keyspace = KEYSPACES.get(random.nextInt(KEYSPACES.size()));
                    var key = "k%02d".formatted(random.nextInt(60));
                    if (random.nextInt(6) == 0) {
                        batch.delete(keyspace, key);
                        next.get(keyspace.name()).remove(key);
                    } else {
                        var value = "v".repeat(random.nextInt(300)) + random.nextInt(1_000);
                        batch.put(keyspace, key, value);
                        next.get(keyspace.name()).put(key, value);
                    }
                }
                var commit = ledger.commit(batch);
                state.clear();
                state.putAll(next);
                visible++;
                history.add(copy(state));
                return commit;
            }

            private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
                var copy = new TreeMap<String, TreeMap<String, String>>();
                state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
                return copy;
            }
        }
    }

    @Nested
    @DisplayName("frames that refer to values")
    class References {
        // Both sides of the shortest values a body refers to, of the shortest piece the writer hands to the cipher as
        // it is, of an AES block and of a slice of the writer
        private static final int[] LENGTHS = {0, 1, 15, 16, 17, 255, 256, 257, 2_047, 2_048, 2_049, 4_095, 65_535, 65_536,
                65_537, 70_001};

        /// {@return a commit body of `writes` writes, built twice: `copied` holds every value itself, the returned
        /// one refers to those of at least `referable` bytes and keeps its own bytes past a chunk on chunks of `pool`,
        /// if not `null`; values of `longest` bytes at most}
        static FrameBody body(Random random, int writes, int referable, int longest, FrameBody copied, HeapPool pool) {
            var referring = new FrameBody(64, referable, pool);
            for (var write = 0; write < writes; write++) {
                var key = MemorySegment.ofArray(("key" + write + ":" + random.nextInt(1_000)).getBytes(StandardCharsets.US_ASCII));
                if (random.nextInt(8) == 0) {
                    copied.delete(1, key);
                    referring.delete(1, key);
                    continue;
                }
                var length = random.nextBoolean() ? LENGTHS[random.nextInt(LENGTHS.length)] : random.nextInt(3_000);
                length = Math.min(length, longest);
                var offset = random.nextInt(40);
                var array = new byte[offset + length + random.nextInt(40)];
                random.nextBytes(array);
                copied.put(1, key, MemorySegment.ofArray(array).asSlice(offset, length));
                referring.put(1, key, array, offset, length);
            }
            return referring;
        }

        /// Checks every frame of `log` against its bodies sealed out of place, and the bodies recovery replays.
        static void assertSealedAsCopied(Log log) throws Exception {
            var next = 1L;
            for (var segment : log.frames().entrySet()) {
                var number = segment.getKey();
                var bytes = Files.readAllBytes(log.files.segment(number));
                for (var frame : segment.getValue()) {
                    var body = log.open(number, frame);
                    byte[] expected;
                    if (frame.type() == FrameBody.COMMITS) {
                        var entries = entries(body);
                        var plain = new ByteArrayOutputStream();
                        for (var i = 0; i < entries.size(); i++) {
                            var commit = log.bodies.get((int) (next + i - 1));
                            plain.writeBytes(varInt(commit.length));
                            plain.writeBytes(commit);
                        }
                        expected = plain.toByteArray();
                        next += entries.size();
                    } else {
                        assertEquals(FrameBody.COMMIT, frame.type());
                        expected = log.bodies.get((int) next - 1);
                        next++;
                    }
                    assertArrayEquals(expected, body, "the body of the frame at " + frame.offset() + " of segment " + number);
                    var header = Arrays.copyOfRange(bytes, frame.offset(), frame.bodyAt());
                    assertArrayEquals(log.seal(number, bytes, frame.offset(), header, expected),
                            Arrays.copyOfRange(bytes, frame.bodyAt(), frame.end()), "the frame at " + frame.offset() + " of segment " + number);
                }
            }
            assertEquals(log.last() + 1, next, "the commits in frames");
            log.assertReplayed(log.replay(0), 1, log.last());
        }

        @ParameterizedTest(name = "seed {0}, encrypted: {1}")
        @CsvSource({"1, true", "2, true", "3, true", "4, true", "1, false", "2, false", "3, false", "4, false"})
        @DisplayName("seals small and large bodies that refer to values as it seals the same bodies holding them, across segments")
        void sealedAsCopied(int seed, boolean encrypted, @TempDir Path directory) throws Exception {
            var random = new Random(seed);
            var log = new Log(directory, encrypted, 256 << 10);
            try {
                for (var group = 0; group < 40; group++) {
                    var batch = new LogWriter.Batch();
                    for (var commit = random.nextInt(12); commit >= 0; commit--) {
                        var copied = new FrameBody();
                        var shape = random.nextInt(5);
                        // Bodies a run takes, sealed whole in the output; bodies past RUN_MAX_BODY whose pieces are
                        // long, short or a single byte each; and bodies that refer to nothing
                        var referring = switch (shape) {
                            case 0 -> body(random, 1 + random.nextInt(3), FrameBody.RUN_REFERABLE, 4_000, copied, null);
                            case 1 -> body(random, 4 + random.nextInt(30), FrameBody.RUN_REFERABLE, 70_001, copied, null);
                            case 2 -> body(random, 4 + random.nextInt(30), FrameBody.SEALED_REFERABLE, 70_001, copied, null);
                            case 3 -> body(random, 40 + random.nextInt(200), 1, 600, copied, null);
                            default -> body(random, 1 + random.nextInt(8), FrameBody.NOT_REFERABLE, 3_000, copied, null);
                        };
                        assertEquals(copied.size(), referring.size());
                        assertArrayEquals(copied.toBytes(), referring.toBytes());
                        log.add(batch, referring);
                    }
                    log.writer.appendAll(batch);
                }
            } finally {
                log.close();
            }
            assertTrue(log.frames().size() > 3, "the frames took " + log.frames().size() + " segments");
            assertSealedAsCopied(log);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("seals a body whose pieces fall on every offset of an AES block, gathered or handed to the cipher")
        void blockOffsets(boolean encrypted, @TempDir Path directory) throws Exception {
            var random = new Random(5);
            var log = new Log(directory, encrypted, 8 << 20);
            try {
                for (var shift = 0; shift < 32; shift++) {
                    var batch = new LogWriter.Batch();
                    var referring = new FrameBody(64, FrameBody.RUN_REFERABLE);
                    var key = MemorySegment.ofArray(new byte[1 + shift]);
                    // A first piece of its own bytes that ends on each offset of a block, then values on both sides of
                    // the direct pieces, pieces that run past a slice, and short ones between them
                    referring.put(1, key, MemorySegment.ofArray(new byte[7 * shift + 1]));
                    for (var length : new int[]{2_048 + shift, 300, 17, 65_536 + 3 * shift, 2_047, 4_096, 256 + shift, 70_001 - shift}) {
                        var array = new byte[length + shift];
                        random.nextBytes(array);
                        referring.put(1, key, array, shift, length);
                        if (random.nextBoolean()) {
                            referring.delete(1, key);
                        }
                    }
                    log.add(batch, referring);
                    log.writer.appendAll(batch);
                }
            } finally {
                log.close();
            }
            assertSealedAsCopied(log);
        }
    }

    @Nested
    @DisplayName("bodies on chunks of a pool")
    class Chunked {
        /// Adds to `batch` the next commit, a body of at least `size` bytes on chunks of `pool`, of writes of random sizes
        /// so its chunk boundaries fall inside them.
        ///
        /// @return the arrays of the body, by identity
        private static List<byte[]> add(Log log, LogWriter.Batch batch, HeapPool pool, int size, Random random) {
            return add(log, batch, chunked(pool, size, random));
        }

        /// {@return a body of at least `size` bytes on chunks of `pool`, of writes of random sizes so its chunk
        /// boundaries fall inside them}
        private static FrameBody chunked(HeapPool pool, int size, Random random) {
            var body = new FrameBody(random.nextBoolean() ? 256 : size, FrameBody.NOT_REFERABLE, pool);
            while (body.size() < size) {
                var key = new byte[1 + random.nextInt(60)];
                random.nextBytes(key);
                switch (random.nextInt(6)) {
                    case 0 -> body.putExternal(1, MemorySegment.ofArray(key), 100_000 + random.nextInt(1_000_000),
                            new PageRef(random.nextInt(), 1 + random.nextInt(9), random.nextLong(), random.nextInt()));
                    case 1 -> body.delete(2, MemorySegment.ofArray(key));
                    default -> {
                        var value = new byte[random.nextInt(4_000)];
                        random.nextBytes(value);
                        body.put(1, MemorySegment.ofArray(key), MemorySegment.ofArray(value));
                    }
                }
            }
            return body;
        }

        /// Adds `body` to `batch` as the next commit.
        ///
        /// @return the arrays of the body, by identity
        private static List<byte[]> add(Log log, LogWriter.Batch batch, FrameBody body) {
            log.bodies.add(body.toBytes());
            var arrays = new ArrayList<byte[]>();
            for (var i = 0; i < body.chunkCount(); i++) {
                arrays.add(body.chunk(i));
            }
            batch.add(FrameBody.COMMIT, log.last(), body, 0, 1, false);
            return arrays;
        }

        /// {@return a pool of 1 GiB that keeps its overflow through collections, as the chunks released are looked for in it}
        private static HeapPool pool() {
            var pool = new HeapPool(1L << 30, false);
            pool.holdOverflowForTests(true);
            return pool;
        }

        /// Checks that `pool` holds no chunk free twice, and holds every one of `released`.
        private static void assertReleased(HeapPool pool, List<byte[]> released) {
            var free = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
            for (var array : pool.freeForTests(FrameBody.CHUNK_BYTES)) {
                assertTrue(free.add(array), "a chunk is free twice");
            }
            for (var array : released) {
                if (array.length == FrameBody.CHUNK_BYTES) {
                    assertTrue(free.contains(array), "a chunk of a sealed body did not go back");
                }
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("seals bodies of 300 KiB, 600 KiB and 1.5 MiB on chunks as one array would, beside runs, and gives each chunk back once")
        void sealed(boolean encrypted, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 64L << 20);
            var pool = pool();
            var random = new Random(encrypted ? 7 : 8);
            for (var round = 0; round < 3; round++) {
                // The chunks the last round gave back go to this one's bodies
                var batch = new LogWriter.Batch();
                var released = new ArrayList<byte[]>();
                released.addAll(add(log, batch, pool, 300 << 10, random));
                for (var i = 0; i < 20; i++) {
                    log.add(batch, 100);
                }
                released.addAll(add(log, batch, pool, 600 << 10, random));
                released.addAll(add(log, batch, pool, 1_536 << 10, random));
                // A body started on a chunk that ends small, which a run takes
                var small = new FrameBody(FrameBody.CHUNK_BYTES, FrameBody.NOT_REFERABLE, pool);
                small.put(1, MemorySegment.ofArray(new byte[]{1, 2, 3}), MemorySegment.ofArray(new byte[1_000]));
                log.bodies.add(small.toBytes());
                released.add(small.array());
                batch.add(FrameBody.COMMIT, log.last(), small, 0, 1, false);
                for (var i = 0; i < 20; i++) {
                    log.add(batch, 100);
                }
                log.writer.appendAll(batch);
                assertReleased(pool, released);
            }
            log.close();

            var segments = log.frames();
            var number = segments.firstKey();
            var bytes = Files.readAllBytes(log.files.segment(number));
            var large = 0;
            for (var frame : segments.get(number)) {
                if (frame.type() != FrameBody.COMMIT) {
                    continue;
                }
                var expected = log.bodies.get((int) frame.sequence() - 1);
                assertArrayEquals(expected, log.open(number, frame), "the body of commit " + frame.sequence());
                var header = Arrays.copyOfRange(bytes, frame.offset(), frame.bodyAt());
                assertArrayEquals(log.seal(number, bytes, frame.offset(), header, expected), Arrays.copyOfRange(bytes, frame.bodyAt(), frame.end()),
                        "commit " + frame.sequence() + " sealed as one call would");
                if (expected.length > FrameBody.CHUNK_BYTES) {
                    large++;
                }
            }
            assertEquals(9, large, "the frames of bodies on more than one chunk");
            log.assertReplayed(log.replay(0), 1, log.last());
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("refuses a group once stopped and leaves its bodies on chunks to their writers, whose discard gives each chunk back once")
        void refused(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var log = new Log(disk.getPath("/log"), encrypted, 64L << 20);
            var pool = pool();
            var random = new Random(encrypted ? 11 : 12);
            try {
                log.group(5, 100);
                // The next group's force fails, which stops the writer
                disk.stopAtForce(0);
                try {
                    log.group(5, 100);
                } catch (LedgerIoException _) {
                    // The flush that failed may report it here or leave it to the next append
                }
                assertThrows(LedgerIoException.class, log.writer::checkHealthy);
                // A publisher pass as the store's: bodies on chunks among small ones, refused by the stopped writer
                var batch = new LogWriter.Batch();
                var bodies = new ArrayList<FrameBody>();
                var chunks = new ArrayList<byte[]>();
                for (var size : new int[]{300 << 10, 600 << 10, 1_536 << 10}) {
                    log.add(batch, 100);
                    var body = chunked(pool, size, random);
                    bodies.add(body);
                    for (var array : add(log, batch, body)) {
                        if (array.length == FrameBody.CHUNK_BYTES) {
                            chunks.add(array);
                        }
                    }
                }
                assertTrue(chunks.size() >= 9, "the chunks of the bodies");
                assertThrows(LedgerIoException.class, () -> log.writer.appendAll(batch));
                assertEquals(List.of(), pool.freeForTests(FrameBody.CHUNK_BYTES), "the chunks given back before the discards");
                // The rollback of each commit discards its body, as the store's does when the log refuses its pass
                for (var body : bodies) {
                    body.discard();
                    body.discard();
                }
                assertReleased(pool, chunks);
                assertEquals(chunks.size(), pool.freeForTests(FrameBody.CHUNK_BYTES).size(), "the chunks given back");
            } finally {
                // The close of a stopped writer reports its failure
                assertThrows(LedgerIoException.class, log::close);
            }
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("a writer that stops with bodies on chunks queued and deferred lets go of their holds on the pool and keeps their chunks")
        void abandoned(boolean encrypted) throws Exception {
            var disk = CrashFileSystem.create();
            var log = new Log(disk.getPath("/log"), encrypted, 64L << 20);
            var pool = pool();
            var random = new Random(encrypted ? 13 : 14);
            var queued = chunked(pool, 600 << 10, random);
            var deferred = chunked(pool, 300 << 10, random);
            assertTrue(queued.holdsPoolForTests() && deferred.holdsPoolForTests(), "the holds the bodies took with their chunks");
            // The first group starts preparing the next segment on a thread of its own: held here and run once the
            // interceptor is armed, its writes, to another file, must leave the failing write to the current segment
            var held = Collections.synchronizedList(new ArrayList<FutureTask<?>>());
            log.writer.starterForTests = held::add;
            try {
                log.group(5, 100);
                // The next group's first write queues one body and defers the other behind it, then fails
                var armed = new AtomicBoolean(true);
                var segment = LogWriter.segmentName(1);
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && path.getFileName().toString().equals(segment)
                            && armed.compareAndSet(true, false)) {
                        log.writer.append(FrameBody.COMMIT, log.last() + 1, queued);
                        log.writer.defer(FrameBody.COMMIT, log.last() + 2, deferred);
                        throw new IOException("No space left on device");
                    }
                });
                assertEquals(1, held.size(), "the preparation of the next segment");
                held.getFirst().run();
                assertTrue(armed.get(), "the preparation of the next segment took the failing write");
                try {
                    log.group(5, 100);
                } catch (LedgerIoException _) {
                    // The flush that failed may report it here or leave it to the next append
                }
                assertFalse(armed.get(), "the write that fails");
                assertThrows(LedgerIoException.class, log.writer::checkHealthy);
                assertFalse(queued.holdsPoolForTests(), "the queued body still holds the pool");
                assertFalse(deferred.holdsPoolForTests(), "the deferred body still holds the pool");
                assertEquals(List.of(), pool.freeForTests(FrameBody.CHUNK_BYTES), "the chunks of the bodies a seal may still read");
            } catch (Throwable failure) {
                try {
                    log.close();
                } catch (RuntimeException closing) {
                    failure.addSuppressed(closing);
                }
                throw failure;
            }
            // The close of a stopped writer reports its failure
            assertThrows(LedgerIoException.class, log::close);
        }

        @ParameterizedTest(name = "encrypted: {0}")
        @ValueSource(booleans = {true, false})
        @DisplayName("seals bodies on chunks across segment rollovers, one larger than a segment, which recovery replays")
        void rollover(boolean encrypted, @TempDir Path directory) throws Exception {
            var log = new Log(directory, encrypted, 1L << 20);
            var pool = pool();
            var random = new Random(encrypted ? 9 : 10);
            for (var size : new int[]{600 << 10, 700 << 10, 1_536 << 10, 300 << 10}) {
                var batch = new LogWriter.Batch();
                var released = new ArrayList<byte[]>();
                for (var i = 0; i < 5; i++) {
                    log.add(batch, 100);
                }
                released.addAll(add(log, batch, pool, size, random));
                released.addAll(add(log, batch, pool, 400 << 10, random));
                log.add(batch, 100);
                log.writer.appendAll(batch);
                assertReleased(pool, released);
            }
            log.close();
            assertTrue(log.frames().values().stream().filter(frames -> !frames.isEmpty()).count() >= 4, "the segments written");
            log.assertReplayed(log.replay(0), 1, log.last());
        }

        @ParameterizedTest(name = "seed {0}, encrypted: {1}")
        @CsvSource({"1, true", "2, true", "1, false", "2, false"})
        @DisplayName("seals bodies on chunks that refer to values, as a transaction's are, as it seals the same bodies holding them, and gives each chunk back once")
        void referring(int seed, boolean encrypted, @TempDir Path directory) throws Exception {
            var random = new Random(seed);
            var log = new Log(directory, encrypted, 8L << 20);
            var pool = pool();
            try {
                for (var group = 0; group < 4; group++) {
                    var batch = new LogWriter.Batch();
                    var released = new ArrayList<byte[]>();
                    for (var commit = 1 + random.nextInt(2); commit > 0; commit--) {
                        var copied = new FrameBody();
                        var referring = References.body(random, 1_000 + random.nextInt(1_500), FrameBody.SEALED_REFERABLE, 4_096, copied, pool);
                        assertTrue(referring.references() > 0 && referring.chunkCount() > 1, "a body on chunks that refers to values");
                        assertEquals(copied.size(), referring.size());
                        assertArrayEquals(copied.toBytes(), referring.toBytes());
                        for (var i = 0; i < referring.chunkCount(); i++) {
                            released.add(referring.chunk(i));
                        }
                        log.add(batch, referring);
                        log.add(batch, 100);
                    }
                    log.writer.appendAll(batch);
                    assertReleased(pool, released);
                }
            } finally {
                log.close();
            }
            References.assertSealedAsCopied(log);
        }
    }
}
