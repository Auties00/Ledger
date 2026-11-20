package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.zip.CRC32C;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Stores whose log a build of an older segment version wrote and left uncheckpointed, with a large value of each key
/// space in a unit and an empty prepared segment of that version after the last written one, open with the current
/// build: recovery replays the older segments, writes its own commits, split ones among them, to a segment of the
/// current version rather than into the prepared one, and the store survives a loss of power at sampled forces of its
/// later writes.
///
/// The fixtures `segments-v<version>-encrypted.zip` and `segments-v<version>-plain.zip` hold such stores, written by
/// the commits of [#writeFixture(Writer)] through a build of that segment version, opened with
/// [#open(Path, boolean)]'s options and segments of 64 KiB, its process halted once the last commit was durable: the
/// version 3 ones by f33bbc5, whose units of large values are of the first format and whose third segment is the
/// prepared one, the version 4 ones by e0538fc, whose units of an encrypted store are of format
/// [PageFormat#CLOSE_TAG] and whose fourth segment is the prepared one.
class OlderSegmentsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.bytes());
    /// The writes of a split commit: values of 300 to 500 bytes, about 1.2 MB of them, which take several value frames.
    private static final int SPLIT_WRITES = 3_000;

    /// {@return the prepared, empty segment of the fixture of segment version `version`}
    private static long preparedSegment(int version) {
        return switch (version) {
            case 3 -> 3;
            case 4 -> 4;
            default -> throw new IllegalArgumentException("no fixture of segment version " + version);
        };
    }

    private static LiveLedger open(Path directory, boolean encrypted) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS)
                .commitWindow(Duration.ZERO)
                .checkpointEvery(1L << 40)
                .largeValues(16_384))
                .segmentSize(64 << 10)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static byte[] value(String key, int generation, int length) {
        var bytes = new byte[length];
        new Random(key.hashCode() * 31L + generation).nextBytes(bytes);
        return bytes;
    }

    /// The writes of a commit, applied to the store or to the expected state.
    private interface Writer {
        void put(Keyspace<String, byte[]> keyspace, String key, byte[] value);

        void delete(Keyspace<String, byte[]> keyspace, String key);

        void deleteRange(Keyspace<String, byte[]> keyspace, String from, String to);

        void commit();
    }

    // The commits the fixtures hold, as the older build wrote them
    private static void writeFixture(Writer writer) {
        for (var c = 0; c < 30; c++) {
            for (var w = 0; w < 8; w++) {
                var key = "m-%04d".formatted(c * 8 + w);
                writer.put(MESSAGES, key, value(key, 0, (c * 8 + w) * 37 % 400));
            }
            var chat = "c-%02d".formatted(c % 6);
            writer.put(CHATS, chat, value(chat, c, 20 + c * 3));
            writer.commit();
        }
        writer.put(MESSAGES, "m-large", value("m-large", 0, 20_000));
        writer.put(CHATS, "c-large", value("c-large", 0, 17_000));
        writer.commit();
        for (var i = 0; i < 2_100; i++) {
            var key = "b-%05d".formatted(i);
            writer.put(MESSAGES, key, value(key, 0, 1 + i % 16));
        }
        writer.commit();
        for (var i = 0; i < 240; i += 9) {
            writer.delete(MESSAGES, "m-%04d".formatted(i));
        }
        writer.deleteRange(MESSAGES, "b-01000", "b-01100");
        writer.put(MESSAGES, "m-0001", value("m-0001", 1, 300));
        writer.commit();
    }

    private static Map<String, TreeMap<String, String>> fixtureState() {
        var state = emptyState();
        writeFixture(model(state, () -> {
        }));
        return state;
    }

    private static Map<String, TreeMap<String, String>> emptyState() {
        var state = new TreeMap<String, TreeMap<String, String>>();
        state.put(MESSAGES.name(), new TreeMap<>());
        state.put(CHATS.name(), new TreeMap<>());
        return state;
    }

    private static Writer model(Map<String, TreeMap<String, String>> state, Runnable commit) {
        return new Writer() {
            @Override
            public void put(Keyspace<String, byte[]> keyspace, String key, byte[] value) {
                state.get(keyspace.name()).put(key, HexFormat.of().formatHex(value));
            }

            @Override
            public void delete(Keyspace<String, byte[]> keyspace, String key) {
                state.get(keyspace.name()).remove(key);
            }

            @Override
            public void deleteRange(Keyspace<String, byte[]> keyspace, String from, String to) {
                state.get(keyspace.name()).subMap(from, to).clear();
            }

            @Override
            public void commit() {
                commit.run();
            }
        };
    }

    private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
        var copy = new TreeMap<String, TreeMap<String, String>>();
        state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
        return copy;
    }

    private static Map<String, TreeMap<String, String>> contents(Ledger ledger) {
        var contents = emptyState();
        for (var keyspace : List.of(MESSAGES, CHATS)) {
            var entries = contents.get(keyspace.name());
            ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                entries.put(key, HexFormat.of().formatHex(value));
                return true;
            });
            assertEquals(entries.size(), ledger.size(keyspace), keyspace.name() + " count");
            for (var entry : entries.entrySet()) {
                assertEquals(entry.getValue(), HexFormat.of().formatHex(ledger.get(keyspace, entry.getKey()).orElseThrow()),
                        keyspace.name() + " " + entry.getKey());
            }
        }
        return contents;
    }

    // Unpacks the fixture of segment version `version` into `directory`, forcing every file and directory so a loss of
    // power keeps all of it
    private static void unpack(int version, boolean encrypted, Path directory) throws IOException {
        var name = "segments-v%d-%s.zip".formatted(version, encrypted ? "encrypted" : "plain");
        var directories = new LinkedHashSet<Path>();
        Files.createDirectories(directory);
        directories.add(directory);
        try (var input = OlderSegmentsTest.class.getResourceAsStream(name)) {
            assertTrue(input != null, "missing fixture " + name);
            try (var zip = new ZipInputStream(input)) {
                for (var entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry()) {
                    var path = directory.resolve(entry.getName());
                    if (entry.isDirectory()) {
                        Files.createDirectories(path);
                        directories.add(path);
                        continue;
                    }
                    var bytes = zip.readAllBytes();
                    try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                        var buffer = ByteBuffer.wrap(bytes);
                        while (buffer.hasRemaining()) {
                            channel.write(buffer);
                        }
                        channel.force(true);
                    }
                }
            }
        }
        if (!directory.getFileSystem().equals(FileSystems.getDefault())) {
            for (var path : directories.reversed()) {
                try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
                    channel.force(true);
                }
            }
        }
    }

    private static int segmentVersion(Path segment) throws IOException {
        try (var channel = FileChannel.open(segment, StandardOpenOption.READ)) {
            var version = ByteBuffer.allocate(Integer.BYTES);
            channel.read(version, 8);
            return version.flip().getInt();
        }
    }

    private static boolean framesWritten(Path segment) throws IOException {
        try (var channel = FileChannel.open(segment, StandardOpenOption.READ)) {
            var frame = ByteBuffer.allocate(LogWriter.FRAME_HEADER);
            channel.read(frame, LogWriter.SEGMENT_HEADER);
            for (var i = 0; i < frame.position(); i++) {
                if (frame.get(i) != 0) {
                    return true;
                }
            }
            return false;
        }
    }

    /// {@return the split frames of the segment at `segment` whose headers are whole}
    ///
    /// The header of a frame is never encrypted, so this reads the segments of an encrypted store too.
    private static int splitFrames(Path segment) throws IOException {
        var bytes = Files.readAllBytes(segment);
        var buffer = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        var splits = 0;
        for (var at = LogWriter.SEGMENT_HEADER; at + LogWriter.FRAME_HEADER <= bytes.length; at++) {
            if (buffer.getInt(at) != LogWriter.MAGIC || bytes[at + 16] != FrameBody.COMMIT_SPLIT) {
                continue;
            }
            crc.reset();
            crc.update(bytes, at, LogWriter.FRAME_HEADER - Integer.BYTES);
            if ((int) crc.getValue() == buffer.getInt(at + LogWriter.FRAME_HEADER - Integer.BYTES)) {
                splits++;
            }
        }
        return splits;
    }

    /// Gives `put` the [#SPLIT_WRITES] values of a split commit of generation `generation`, every `step`th of them.
    private static void putSplit(BiConsumer<String, byte[]> put, int generation, int step) {
        for (var i = 0; i < SPLIT_WRITES; i += step) {
            var key = "s-%05d".formatted(i);
            put.accept(key, value(key, generation, 300 + (i + generation * 50) % 200));
        }
    }

    @ParameterizedTest(name = "version {0}, encrypted {1}")
    @CsvSource({"3,true", "3,false", "4,true", "4,false"})
    @DisplayName("replays an uncheckpointed log of an older version, writes its own commits, a split one among them, to a segment of the current version past the older prepared one, and keeps them through a crash, a checkpoint and reopens")
    void opensOlderLog(int version, boolean encrypted, @TempDir Path directory) throws Exception {
        unpack(version, encrypted, directory);
        var log = directory.resolve("log");
        var preparedSegment = preparedSegment(version);
        var prepared = log.resolve("%016x.log".formatted(preparedSegment));
        assertEquals(version, segmentVersion(prepared));
        assertFalse(framesWritten(prepared), "the fixture's last segment is prepared and empty");
        var expected = fixtureState();
        try (var ledger = open(directory, encrypted)) {
            assertEquals(expected, contents(ledger));
            var batch = ledger.newBatch();
            batch.put(MESSAGES, "m-large", value("m-large", 1, 30_000));
            batch.put(MESSAGES, "m-0002", value("m-0002", 1, 200));
            ledger.commit(batch).awaitDurable();
            var split = ledger.newBatch();
            putSplit((key, value) -> {
                split.put(MESSAGES, key, value);
                expected.get(MESSAGES.name()).put(key, HexFormat.of().formatHex(value));
            }, 0, 1);
            ledger.commit(split).awaitDurable();
            expected.get(MESSAGES.name()).put("m-large", HexFormat.of().formatHex(value("m-large", 1, 30_000)));
            expected.get(MESSAGES.name()).put("m-0002", HexFormat.of().formatHex(value("m-0002", 1, 200)));
            ledger.closeWithoutCheckpoint();
        }
        var later = 0;
        var splits = 0;
        try (var segments = Files.list(log)) {
            for (var segment : segments.filter(path -> path.getFileName().toString().endsWith(".log")).toList()) {
                var number = Long.parseLong(segment.getFileName().toString().substring(0, 16), 16);
                if (number > preparedSegment) {
                    assertEquals(LogWriter.SEGMENT_VERSION, segmentVersion(segment), segment.toString());
                    splits += splitFrames(segment);
                    later++;
                } else {
                    assertEquals(version, segmentVersion(segment), segment.toString());
                }
            }
        }
        assertTrue(later > 0, "no segment of the current version");
        assertTrue(splits > 0, "no split frame after the older log");
        if (Files.exists(prepared)) {
            assertFalse(framesWritten(prepared), "a commit written into the older prepared segment");
        }
        try (var ledger = open(directory, encrypted)) {
            assertEquals(expected, contents(ledger));
            ledger.checkpoint();
            assertEquals(expected, contents(ledger));
        }
        try (var ledger = open(directory, encrypted)) {
            assertEquals(expected, contents(ledger));
        }
    }

    /// The commits the current build adds to a fixture store, and the states each leaves visible.
    private static final class Continuation {
        final boolean encrypted;
        final List<Map<String, TreeMap<String, String>>> history = new ArrayList<>();
        final Map<String, TreeMap<String, String>> state = fixtureState();
        int visible;
        int durable;

        Continuation(boolean encrypted) {
            this.encrypted = encrypted;
            history.add(copy(state));
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted);
                write(ledger, writer -> {
                    writer.put(MESSAGES, "m-large", value("m-large", 1, 30_000));
                    writer.put(MESSAGES, "m-0002", value("m-0002", 1, 200));
                    writer.put(CHATS, "c-00", value("c-00", 100, 64));
                });
                write(ledger, writer -> {
                    for (var i = 0; i < LiveLedger.BULK_RUN_WRITES + 52; i++) {
                        var key = "n-%05d".formatted(i);
                        writer.put(MESSAGES, key, value(key, 0, 1 + i % 40));
                    }
                });
                var last = write(ledger, writer -> {
                    writer.deleteRange(MESSAGES, "b-00000", "b-00050");
                    writer.put(MESSAGES, "b-00500", value("b-00500", 1, 500));
                    writer.delete(MESSAGES, "m-0001");
                });
                last.awaitDurable();
                durable = visible;
                // Two split commits, the second rewriting half of the first's values
                write(ledger, writer -> putSplit((key, value) -> writer.put(MESSAGES, key, value), 0, 1));
                write(ledger, writer -> {
                    putSplit((key, value) -> writer.put(MESSAGES, key, value), 1, 2);
                    writer.put(CHATS, "c-02", value("c-02", 100, 64));
                });
                ledger.checkpoint();
                write(ledger, writer -> writer.put(CHATS, "c-large", value("c-large", 1, 18_000)));
                write(ledger, writer -> {
                    for (var i = 0; i < LiveLedger.BULK_RUN_WRITES + 10; i++) {
                        var key = "o-%05d".formatted(i);
                        writer.put(MESSAGES, key, value(key, 0, 1 + i % 24));
                    }
                });
                last = write(ledger, writer -> writer.delete(CHATS, "c-01"));
                last.awaitDurable();
                durable = visible;
                ledger.close();
                durable = visible;
            } catch (RuntimeException | InterruptedException failure) {
                if (ledger != null) {
                    try {
                        ledger.close();
                    } catch (RuntimeException _) {
                        // Stopped
                    }
                }
            }
        }

        private Commit write(LiveLedger ledger, Consumer<Writer> writes) {
            var batch = new WriteBatch[]{ledger.newBatch()};
            var next = copy(state);
            var expected = model(next, () -> {
            });
            writes.accept(new Writer() {
                @Override
                public void put(Keyspace<String, byte[]> keyspace, String key, byte[] value) {
                    batch[0].put(keyspace, key, value);
                    expected.put(keyspace, key, value);
                }

                @Override
                public void delete(Keyspace<String, byte[]> keyspace, String key) {
                    batch[0].delete(keyspace, key);
                    expected.delete(keyspace, key);
                }

                @Override
                public void deleteRange(Keyspace<String, byte[]> keyspace, String from, String to) {
                    batch[0].deleteRange(keyspace, KeyRange.between(from, to));
                    expected.deleteRange(keyspace, from, to);
                }

                @Override
                public void commit() {
                }
            });
            var commit = ledger.commit(batch[0]);
            state.clear();
            state.putAll(next);
            visible++;
            history.add(copy(state));
            return commit;
        }
    }

    @ParameterizedTest(name = "version {0}, encrypted {1}, part {2}")
    @CsvSource({"3,true,0", "3,true,1", "3,false,0", "4,true,0", "4,true,1", "4,false,0"})
    @DisplayName("recovers a state between the last durable and the last visible commit at sampled forces of the writes that follow an older log, split commits among them, and keeps it through a checkpoint and a reopen")
    void powerLossAfterOlderLog(int version, boolean encrypted, int part) throws Exception {
        var dry = CrashFileSystem.create();
        unpack(version, encrypted, dry.getPath("/store"));
        var unpacked = dry.forces();
        var dryRun = new Continuation(encrypted);
        dryRun.run(dry.getPath("/store"));
        assertEquals(dryRun.visible, dryRun.durable, "the dry run failed");
        var forces = dry.forces() - unpacked;
        var random = new Random(forces * 31 + part);
        var points = new ArrayList<Long>();
        for (var i = 0; i < 20; i++) {
            points.add(part == 0 ? forces * i / 20 : random.nextLong(forces + 1));
        }
        points.add(forces);
        for (var force : points) {
            var disk = CrashFileSystem.create();
            unpack(version, encrypted, disk.getPath("/store"));
            disk.stopAtForce(force);
            var continuation = new Continuation(encrypted);
            continuation.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            Map<String, TreeMap<String, String>> recovered;
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                recovered = contents(ledger);
                var match = -1;
                for (var commit = continuation.durable; commit <= continuation.visible; commit++) {
                    if (continuation.history.get(commit).equals(recovered)) {
                        match = commit;
                        break;
                    }
                }
                if (match < 0) {
                    fail("force " + force + " of " + forces + ": recovered no state from " + continuation.durable + " to "
                            + continuation.visible);
                }
                ledger.checkpoint();
                assertEquals(recovered, contents(ledger), "after a checkpoint");
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                assertEquals(recovered, contents(ledger), "after a reopen");
            }
        }
    }
}
