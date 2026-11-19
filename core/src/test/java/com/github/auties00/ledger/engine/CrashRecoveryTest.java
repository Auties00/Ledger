package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// What a store recovers after its process stops at a random point of a workload, then loses power or only crashes:
/// the state after some commit between the last one it acknowledged durable and the last one it made visible, never
/// part of a batch, and a store that keeps working, even when a second failure strikes during recovery.
///
/// A dry run of each workload counts its file system changes and forces, so the stop lands anywhere in it, half the
/// time at a force: in a commit, a checkpoint, a rollover, a close or an open. Half the workloads write their values of
/// more than 4 KiB to the file of large values, whose force runs beside the log's; the others write some values of
/// 128 KiB and more, on arrays of their own, which move to the log as soon as their commits are durable. Half the
/// workloads write the groups one write holds through, each write of which is a force of its own. The workloads of
/// bulk commits add batches of hundreds of writes, whose frames leave the values on their batch's arrays to a section
/// ([FrameBody#COMMIT_SPLIT]), so a stop tears such a frame anywhere.
class CrashRecoveryTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, CHATS);

    static Stream<Arguments> cases() {
        // Half the workloads write the groups one write holds through, as a store writes them where it can
        return IntStream.range(0, 120).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0, seed % 3 == 0 ? "process crash" : "power loss",
                seed / 4 % 2 == 0 ? WriteThrough.Mode.DSYNC : WriteThrough.Mode.OFF));
    }

    @ParameterizedTest(name = "seed {0}, encrypted: {1}, {2}, write through: {3}")
    @MethodSource("cases")
    @DisplayName("recovers a state between the last durable commit and the last visible one")
    void recovers(int seed, boolean encrypted, String failure, WriteThrough.Mode writeThrough) throws Exception {
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        recovers(seed, encryption, failure, writeThrough, false);
    }

    /// Runs the workload of `seed` once to count its changes, then again on a disk that stops at one of them, and
    /// checks what the store recovers.
    private static void recovers(int seed, Encryption encryption, String failure, WriteThrough.Mode writeThrough, boolean bulk)
            throws Exception {
        var dry = CrashFileSystem.create();
        new Workload(seed, encryption, writeThrough, bulk).run(dry.getPath("/store"));
        var disk = CrashFileSystem.create();
        var random = new Random(seed * 7919L + 1);
        if (random.nextBoolean()) {
            disk.stopAfter(random.nextLong(dry.changes() + 1));
        } else {
            // At a force, where the writes it would have made durable are at risk together
            disk.stopAtForce(random.nextLong(dry.forces() + 1));
        }
        check(seed, encryption, writeThrough, bulk, disk, failure.equals("process crash"), random);
    }

    static Stream<Arguments> bulkCases() {
        return IntStream.range(0, 40).mapToObj(seed -> Arguments.of(seed, seed % 2 == 0, seed % 3 == 0 ? "process crash" : "power loss",
                seed / 4 % 2 == 0 ? WriteThrough.Mode.DSYNC : WriteThrough.Mode.OFF));
    }

    @ParameterizedTest(name = "seed {0}, encrypted: {1}, {2}, write through: {3}")
    @MethodSource("bulkCases")
    @DisplayName("recovers a state between the last durable commit and the last visible one from a log of split frames, torn anywhere")
    void recoversSplitFrames(int seed, boolean encrypted, String failure, WriteThrough.Mode writeThrough) throws Exception {
        recovers(seed, encrypted ? Encryption.key(KEY) : Encryption.none(), failure, writeThrough, true);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(ints = {2_000, 2_001})
    @DisplayName("recovers whichever force of a workload of split frames the power fails at")
    void everyForceSplitFrames(int seed) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.create();
        var workload = new Workload(seed, encryption, WriteThrough.Mode.OFF, true);
        workload.run(dry.getPath("/store"));
        assertTrue(workload.bulkCommits > 0, "the workload made no bulk commit");
        for (var force = 0; force <= dry.forces(); force++) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            check(seed, encryption, WriteThrough.Mode.OFF, true, disk, false, new Random(seed * 31L + force));
        }
    }

    /// Opens a store without encryption in `directory`, with segments of 4 MiB.
    private static LiveLedger openPlain(Path directory) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS))
                .segmentSize(4L << 20)
                .build();
    }

    /// Commits, durably, a batch of 300 messages of about a kilobyte whose frame is split, from key `first`, and returns
    /// what it wrote.
    private static TreeMap<String, String> commitSplit(Ledger ledger, int first) throws InterruptedException {
        var written = new TreeMap<String, String>();
        var batch = ledger.newBatch();
        for (var i = first; i < first + 300; i++) {
            var value = ((char) ('a' + i % 26) + "").repeat(1_000 + i % 300);
            batch.put(MESSAGES, "k%04d".formatted(i), value);
            written.put("k%04d".formatted(i), value);
        }
        ledger.commit(batch).awaitDurable();
        return written;
    }

    /// {@return what `ledger` holds of `MESSAGES`}
    private static TreeMap<String, String> messages(Ledger ledger) {
        var read = new TreeMap<String, String>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            read.put(key, value);
            return true;
        });
        return read;
    }

    /// A frame of a segment whose header is whole: where it starts, its type, the length of its sealed body and the
    /// sequence it carries.
    private record Frame(int at, byte type, int sealedLength, long sequence) {
    }

    /// {@return the frames of `bytes`, the bytes of a segment without encryption, whose headers are whole, in order}
    private static List<Frame> frames(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        var frames = new ArrayList<Frame>();
        for (var at = LogWriter.SEGMENT_HEADER; at + LogWriter.FRAME_HEADER <= bytes.length; at++) {
            if (buffer.getInt(at) != LogWriter.MAGIC) {
                continue;
            }
            crc.reset();
            crc.update(bytes, at, LogWriter.FRAME_HEADER - Integer.BYTES);
            if ((int) crc.getValue() != buffer.getInt(at + LogWriter.FRAME_HEADER - Integer.BYTES)) {
                continue;
            }
            var sealedLength = buffer.getInt(at + 4);
            frames.add(new Frame(at, bytes[at + 16], sealedLength, buffer.getLong(at + 8)));
            at += LogWriter.FRAME_HEADER + sealedLength - 1;
        }
        return frames;
    }

    /// Writes anew the header checksum of the frame at `at` of `bytes`, a segment without encryption, and the checksum
    /// of its header and body, so recovery takes the frame for whole and authentic.
    private static void reseal(byte[] bytes, int at) {
        var buffer = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        crc.update(bytes, at, LogWriter.FRAME_HEADER - Integer.BYTES);
        buffer.putInt(at + LogWriter.FRAME_HEADER - Integer.BYTES, (int) crc.getValue());
        var body = buffer.getInt(at + 4) - Integer.BYTES;
        crc.reset();
        crc.update(bytes, at, LogWriter.FRAME_HEADER + body);
        buffer.putInt(at + LogWriter.FRAME_HEADER + body, (int) crc.getValue());
    }

    /// {@return the only segment of `directory` that holds a split frame, and its frames}
    private static Path splitSegment(Path directory) throws Exception {
        try (var files = Files.walk(directory)) {
            for (var segment : files.filter(path -> path.toString().endsWith(".log")).toList()) {
                if (frames(Files.readAllBytes(segment)).stream().anyMatch(frame -> frame.type() == FrameBody.COMMIT_SPLIT)) {
                    return segment;
                }
            }
        }
        throw new AssertionError("no split frame in the log");
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"writes length past the head", "writes length below its four bytes",
            "writes a byte too long", "table one value frame short", "table naming a value frame a byte shorter"})
    @DisplayName("refuses to open a store whose log holds a split frame with a valid checksum and a malformed head")
    void malformedSection(String damage) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        try (var ledger = openPlain(directory)) {
            commitSplit(ledger, 0);
            ledger.closeWithoutCheckpoint();
        }
        var segment = splitSegment(directory);
        var bytes = Files.readAllBytes(segment);
        var head = frames(bytes).stream().filter(frame -> frame.type() == FrameBody.COMMIT_SPLIT).findFirst().orElseThrow();
        var body = head.at() + LogWriter.FRAME_HEADER;
        var length = head.sealedLength() - Integer.BYTES;
        var writes = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt(body);
        var table = FrameBody.splitTable(bytes, body, length);
        assertTrue(table.length > 1, "a split frame of " + table.length + " value frames");
        switch (damage) {
            case "writes length past the head" -> putInt(bytes, body, length + 1);
            case "writes length below its four bytes" -> putInt(bytes, body, 2);
            // The table starts a byte late, inside its own count
            case "writes a byte too long" -> putInt(bytes, body, writes + 1);
            // The count of the table, one value frame fewer: the table then ends before the head does
            case "table one value frame short" -> bytes[body + writes] = (byte) (table.length - 1);
            // The last value frame then holds a byte more than the table says
            default -> {
                var shorter = table.clone();
                shorter[shorter.length - 1]--;
                var rewritten = new java.io.ByteArrayOutputStream();
                writeVarInt(rewritten, shorter.length);
                for (var frameLength : shorter) {
                    writeVarInt(rewritten, frameLength);
                }
                assertEquals(length - writes, rewritten.size(), "a table of another length");
                System.arraycopy(rewritten.toByteArray(), 0, bytes, body + writes, rewritten.size());
            }
        }
        reseal(bytes, head.at());
        Files.write(segment, bytes);
        assertThrows(LedgerCorruptedException.class, () -> Ledger.builder().directory(directory).encryption(Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS).build().close());
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"a sequence of another commit", "the type of a commit", "the flag of a group's first frame"})
    @DisplayName("refuses to open a store whose log holds a value frame with a valid checksum that its split frame does not announce")
    void wrongValueFrame(String damage) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        try (var ledger = openPlain(directory)) {
            commitSplit(ledger, 0);
            ledger.closeWithoutCheckpoint();
        }
        var segment = splitSegment(directory);
        var bytes = Files.readAllBytes(segment);
        var values = frames(bytes).stream().filter(frame -> frame.type() == FrameBody.VALUES).toList();
        assertTrue(values.size() > 1, values.size() + " value frames");
        var frame = values.get(1);
        switch (damage) {
            case "a sequence of another commit" -> ByteBuffer.wrap(bytes).putLong(frame.at() + 8, frame.sequence() + 1);
            case "the type of a commit" -> bytes[frame.at() + 16] = FrameBody.COMMIT;
            default -> bytes[frame.at() + 17] = LogWriter.GROUP_START;
        }
        reseal(bytes, frame.at());
        Files.write(segment, bytes);
        assertThrows(LedgerCorruptedException.class, () -> Ledger.builder().directory(directory).encryption(Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS).build().close());
    }

    @ParameterizedTest(name = "value frame {0}, a later group: {1}")
    @CsvSource({"0, false", "1, false", "-1, false", "0, true", "-1, true"})
    @DisplayName("a value frame torn or failing authentication drops its split commit whole as a torn frame of the last group, and refuses the log if a later group follows")
    void tornValueFrame(int index, boolean laterGroup) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        TreeMap<String, String> before;
        try (var ledger = openPlain(directory)) {
            ledger.commit(ledger.newBatch().put(MESSAGES, "before", "first")).awaitDurable();
            before = messages(ledger);
            commitSplit(ledger, 0);
            if (laterGroup) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "later")).awaitDurable();
            }
            ledger.closeWithoutCheckpoint();
        }
        var segment = splitSegment(directory);
        var bytes = Files.readAllBytes(segment);
        var values = frames(bytes).stream().filter(frame -> frame.type() == FrameBody.VALUES).toList();
        var frame = values.get(index < 0 ? values.size() - 1 : index);
        // A byte of its body, which its checksum no longer matches
        bytes[frame.at() + LogWriter.FRAME_HEADER + 7] ^= 1;
        Files.write(segment, bytes);
        if (laterGroup) {
            assertThrows(LedgerCorruptedException.class, () -> Ledger.builder().directory(directory).encryption(Encryption.none())
                    .keyspace(MESSAGES).keyspace(CHATS).build().close());
            return;
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(MESSAGES).keyspace(CHATS).build()) {
            assertEquals(before, messages(ledger));
            // The store goes on writing after the torn commit, which no later open replays
            commitSplit(ledger, 1_000);
            ledger.commit(ledger.newBatch().put(MESSAGES, "after", "later")).awaitDurable();
        }
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.none()).keyspace(MESSAGES).keyspace(CHATS).build()) {
            var expected = new TreeMap<>(before);
            for (var i = 1_000; i < 1_300; i++) {
                expected.put("k%04d".formatted(i), ((char) ('a' + i % 26) + "").repeat(1_000 + i % 300));
            }
            expected.put("after", "later");
            assertEquals(expected, messages(ledger));
        }
    }

    @ParameterizedTest(name = "cut {0} bytes into the value frames, encrypted: {1}")
    @CsvSource({"0, false", "1, false", "40, false", "300000, false", "0, true", "100, true", "300000, true"})
    @DisplayName("a crash between a split frame and its value frames, or inside one, recovers the state before the split commit")
    void crashBetweenValueFrames(int cut, boolean encrypted) throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
        TreeMap<String, String> before;
        try (var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption)
                .keyspace(MESSAGES).keyspace(CHATS))
                .segmentSize(4L << 20)
                .build()) {
            ledger.commit(ledger.newBatch().put(MESSAGES, "before", "first")).awaitDurable();
            before = messages(ledger);
            commitSplit(ledger, 0);
            ledger.closeWithoutCheckpoint();
        }
        // The segment as a crash leaves it, the bytes from `cut` into the first value frame on never written
        Path segment = null;
        int end = -1;
        try (var files = Files.walk(directory)) {
            for (var path : files.filter(file -> file.toString().endsWith(".log")).toList()) {
                var bytes = Files.readAllBytes(path);
                var at = splitHeadEnd(bytes);
                if (at >= 0) {
                    segment = path;
                    end = at;
                }
            }
        }
        assertTrue(segment != null, "no split frame in the log");
        var bytes = Files.readAllBytes(segment);
        Arrays.fill(bytes, Math.min(bytes.length, end + cut), bytes.length, (byte) 0);
        Files.write(segment, bytes);
        try (var ledger = Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).keyspace(CHATS).build()) {
            assertEquals(before, messages(ledger));
        }
    }

    /// {@return where the split frame of `bytes`, a segment, ends, as its header says, or -1 if it holds none}
    ///
    /// The header of a frame is never encrypted, so this reads the segments of an encrypted store too.
    private static int splitHeadEnd(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var crc = new CRC32C();
        for (var at = LogWriter.SEGMENT_HEADER; at + LogWriter.FRAME_HEADER <= bytes.length; at++) {
            if (buffer.getInt(at) != LogWriter.MAGIC || bytes[at + 16] != FrameBody.COMMIT_SPLIT) {
                continue;
            }
            crc.reset();
            crc.update(bytes, at, LogWriter.FRAME_HEADER - Integer.BYTES);
            if ((int) crc.getValue() == buffer.getInt(at + LogWriter.FRAME_HEADER - Integer.BYTES)) {
                return at + LogWriter.FRAME_HEADER + buffer.getInt(at + 4);
            }
        }
        return -1;
    }

    private static void writeVarInt(java.io.ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static void putInt(byte[] bytes, int at, int value) {
        bytes[at] = (byte) value;
        bytes[at + 1] = (byte) (value >>> 8);
        bytes[at + 2] = (byte) (value >>> 16);
        bytes[at + 3] = (byte) (value >>> 24);
    }

    @Test
    @DisplayName("reads a split frame back from a segment of the current version, and refuses one in a segment of an older version, naming it")
    void splitFrameInOlderSegment() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        TreeMap<String, String> expected;
        try (var ledger = openPlain(directory)) {
            expected = commitSplit(ledger, 0);
            ledger.closeWithoutCheckpoint();
        }
        List<Path> segments;
        try (var files = Files.walk(directory)) {
            segments = files.filter(path -> path.toString().endsWith(".log")).toList();
        }
        splitSegment(directory);
        var copy = disk.afterProcessCrash();
        try (var ledger = Ledger.builder().directory(copy.getPath("/store")).encryption(Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS).build()) {
            assertEquals(expected, messages(ledger));
        }
        for (var segment : segments) {
            setVersion(segment, LogWriter.SPLIT_SEGMENT_VERSION - 1);
        }
        var failure = assertThrows(LedgerCorruptedException.class, () -> Ledger.builder().directory(directory)
                .encryption(Encryption.none()).keyspace(MESSAGES).keyspace(CHATS).build().close());
        assertTrue(failure.getMessage().contains("version " + (LogWriter.SPLIT_SEGMENT_VERSION - 1)), failure::getMessage);
    }

    /// Gives the segment at `path` the version `version`, its header intact.
    private static void setVersion(Path path, int version) throws Exception {
        var bytes = Files.readAllBytes(path);
        var buffer = ByteBuffer.wrap(bytes);
        buffer.putInt(8, version);
        var crc = new CRC32C();
        crc.update(bytes, 0, LogWriter.HEADER_CRC_OFFSET);
        buffer.putInt(LogWriter.HEADER_CRC_OFFSET, (int) crc.getValue());
        Files.write(path, bytes);
    }
    @ParameterizedTest(name = "seed {0}, write through: {1}")
    @MethodSource("workloads")
    @DisplayName("recovers whichever force of a workload the power fails at, a write through included")
    void everyForce(int seed, WriteThrough.Mode writeThrough) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.create();
        new Workload(seed, encryption, writeThrough).run(dry.getPath("/store"));
        for (var force = 0; force <= dry.forces(); force++) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            check(seed, encryption, writeThrough, false, disk, false, new Random(seed * 31L + force));
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("ntfsWorkloads")
    @DisplayName("recovers whichever force of a workload the power fails at, on a file system that journals names as NTFS does")
    void everyForceOnNtfs(int seed) throws Exception {
        var encryption = Encryption.key(KEY);
        var dry = CrashFileSystem.createNtfs();
        new Workload(seed, encryption, WriteThrough.Mode.DSYNC).run(dry.getPath("/store"));
        for (var force = 0; force <= dry.forces(); force++) {
            var disk = CrashFileSystem.createNtfs();
            disk.stopAtForce(force);
            check(seed, encryption, WriteThrough.Mode.DSYNC, false, disk, false, new Random(seed * 31L + force));
        }
    }

    static IntStream ntfsWorkloads() {
        return IntStream.range(1_000, 1_002);
    }

    @Test
    @DisplayName("the workloads that write through do write groups through, so a stop can land on one")
    void reachesWriteThrough() {
        var reached = IntStream.range(0, 120).filter(seed -> {
            var workload = new Workload(seed, Encryption.none(), WriteThrough.Mode.DSYNC);
            workload.run(CrashFileSystem.create().getPath("/store"));
            return workload.writtenThrough > 0;
        }).count();
        // Not a share of them: 2 of the 120 write through when every commit that does not wait joins the group of the
        // next that does, and the log fills too fast for a segment written with zeros, as on a fast machine
        assertTrue(reached > 0, "no workload wrote a group through");
    }

    static Stream<Arguments> workloads() {
        return IntStream.range(1_000, 1_004).boxed()
                .flatMap(seed -> Stream.of(Arguments.of(seed, WriteThrough.Mode.DSYNC), Arguments.of(seed, WriteThrough.Mode.OFF)));
    }

    /// Runs the workload of `seed` on `disk` until it stops, fails it, and checks what the store recovers.
    private static void check(int seed, Encryption encryption, WriteThrough.Mode writeThrough, boolean bulk,
                              CrashFileSystem disk, boolean processCrash, Random random) throws Exception {
        var workload = new Workload(seed, encryption, writeThrough, bulk);
        workload.run(disk.getPath("/store"));
        var largeValues = largeValues(seed);
        var survivor = processCrash ? disk.afterProcessCrash() : disk.afterPowerLoss(new Random(random.nextLong()));
        // A second failure, during the recovery itself
        var recoveryStop = random.nextInt(4) == 0 ? random.nextLong(40) : -1;
        if (recoveryStop >= 0) {
            survivor.stopAfter(recoveryStop);
            try (var ledger = open(survivor.getPath("/store"), encryption, largeValues, writeThrough)) {
                ledger.checkpoint();
            } catch (RuntimeException _) {
                // The recovery stopped where it was told to
            }
            survivor = survivor.afterPowerLoss(new Random(random.nextLong()));
        }
        try (var ledger = open(survivor.getPath("/store"), encryption, largeValues, writeThrough)) {
            var recovered = contents(ledger);
            var match = -1;
            for (var commit = workload.durable; commit <= workload.visible; commit++) {
                if (workload.history.get(commit).equals(recovered)) {
                    match = commit;
                    break;
                }
            }
            if (match < 0) {
                fail("The store recovered a state no commit from " + workload.durable + " to " + workload.visible
                        + " left (of " + (workload.history.size() - 1) + "): " + recovered);
            }
            for (var keyspace : KEYSPACES) {
                assertEquals(recovered.get(keyspace.name()).size(), ledger.size(keyspace), keyspace.name() + " count");
                // A descending scan walks the chains the recovery replayed along the links its first walk sets
                var descending = new ArrayList<String>();
                ledger.forEach(keyspace, KeyRange.<String>all().reversed(), (key, _) -> descending.add(key));
                assertEquals(new ArrayList<>(recovered.get(keyspace.name()).descendingKeySet()), descending, keyspace.name() + " descending");
            }
            // The recovered store takes commits and checkpoints, a large value included
            var large = "v".repeat(10_000);
            ledger.commit(ledger.newBatch().put(MESSAGES, "after", "recovery").put(CHATS, "after", "recovery")
                    .put(MESSAGES, "large", large)).awaitDurable();
            ledger.checkpoint();
        }
        try (var ledger = open(survivor.getPath("/store"), encryption, largeValues, writeThrough)) {
            assertEquals("recovery", ledger.get(MESSAGES, "after").orElseThrow());
            assertEquals(10_000, ledger.get(MESSAGES, "large").orElseThrow().length());
        }
        CheckpointTest.verifyPages(survivor.getPath("/store"), encryption != Encryption.none());
    }

    private static Ledger open(Path directory, Encryption encryption, long largeValues, WriteThrough.Mode writeThrough) {
        var builder = ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encryption).keyspace(MESSAGES).keyspace(CHATS)
                .largeValues(largeValues))
                .segmentSize(16_384)
                .writeThrough(writeThrough);
        if (largeValues == Long.MAX_VALUE) {
            // Every value on an array of its own leaves the heap once its commit is durable
            builder.spillBudget(0);
        }
        return builder.build();
    }

    /// {@return the size from which the workload of `seed` writes values to the file of large values}
    private static long largeValues(int seed) {
        return seed % 4 < 2 ? 4_096 : Long.MAX_VALUE;
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

    /// A workload drawn from a seed, and what it learned while it ran: the state after each commit, the last commit it
    /// made visible and the last one it knows durable.
    private static final class Workload {
        private final Random random;
        private final Encryption encryption;
        private final long largeValues;
        private final WriteThrough.Mode writeThrough;
        // Whether it makes bulk commits too
        private final boolean bulk;
        // The bulk commits it made
        int bulkCommits;
        // The state after each commit, from the empty store
        // The groups the store wrote through, in the opens this workload closed
        long writtenThrough;
        final List<Map<String, TreeMap<String, String>>> history;
        final Map<String, TreeMap<String, String>> state;
        int visible;
        int durable;

        Workload(int seed, Encryption encryption, WriteThrough.Mode writeThrough) {
            this(seed, encryption, writeThrough, false);
        }

        Workload(int seed, Encryption encryption, WriteThrough.Mode writeThrough, boolean bulk) {
            this.bulk = bulk;
            this.random = new Random(seed);
            this.encryption = encryption;
            this.largeValues = largeValues(seed);
            this.writeThrough = writeThrough;
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
                ledger = open(directory, encryption, largeValues, writeThrough);
                for (var step = 0; step < 70; step++) {
                    var choice = random.nextInt(25);
                    if (choice == 0) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (choice == 1) {
                        close(ledger);
                        durable = visible;
                        ledger = open(directory, encryption, largeValues, writeThrough);
                    } else if (choice < 6) {
                        transaction(ledger);
                    } else if (bulk && choice >= 22) {
                        bulk(ledger);
                    } else {
                        batch(ledger, choice < 9);
                    }
                }
                close(ledger);
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

        private void close(Ledger ledger) {
            writtenThrough += ((LiveLedger) ledger).log().writtenThrough();
            ledger.close();
        }

        private void batch(Ledger ledger, boolean awaitDurable) throws InterruptedException {
            var next = copy(state);
            var batch = ledger.newBatch();
            var writes = 1 + random.nextInt(12);
            for (var i = 0; i < writes; i++) {
                var keyspace = KEYSPACES.get(random.nextInt(KEYSPACES.size()));
                var entries = next.get(keyspace.name());
                var key = "k%03d".formatted(random.nextInt(120));
                var kind = random.nextInt(20);
                if (kind == 0) {
                    var to = "k%03d".formatted(random.nextInt(120));
                    batch.deleteRange(keyspace, KeyRange.between(key, to));
                    if (key.compareTo(to) < 0) {
                        entries.subMap(key, to).clear();
                    }
                } else if (kind < 5) {
                    batch.delete(keyspace, key);
                    entries.remove(key);
                } else {
                    var value = value();
                    batch.put(keyspace, key, value);
                    entries.put(key, value);
                }
            }
            var commit = ledger.commit(batch);
            commit(next);
            if (awaitDurable) {
                commit.awaitDurable();
                durable = visible;
            }
        }

        /// Commits a batch of hundreds of writes, past a chunk of a batch, whose frame is split: values of a kilobyte or
        /// two on its rope, a few long enough for the file of large values of a workload that has one, and one in 60 on
        /// an array of its own in a workload that has none.
        private void bulk(Ledger ledger) throws InterruptedException {
            var next = copy(state);
            var batch = ledger.newBatch();
            var writes = 200 + random.nextInt(200);
            for (var i = 0; i < writes; i++) {
                var keyspace = random.nextInt(50) == 0 ? CHATS : MESSAGES;
                var key = random.nextBoolean() ? "k%03d".formatted(random.nextInt(120)) : "b%04d".formatted(random.nextInt(2_000));
                if (random.nextInt(40) == 0) {
                    batch.delete(keyspace, key);
                    next.get(keyspace.name()).remove(key);
                    continue;
                }
                var length = 600 + random.nextInt(1_500);
                if (random.nextInt(30) == 0) {
                    length = 4_500 + random.nextInt(4_000);
                } else if (largeValues == Long.MAX_VALUE && random.nextInt(60) == 0) {
                    length = 130_000 + random.nextInt(60_000);
                }
                var value = letters(length);
                batch.put(keyspace, key, value);
                next.get(keyspace.name()).put(key, value);
            }
            var commit = ledger.commit(batch);
            commit(next);
            bulkCommits++;
            if (random.nextBoolean()) {
                commit.awaitDurable();
                durable = visible;
            }
        }

        private void transaction(Ledger ledger) {
            var from = "k%03d".formatted(random.nextInt(120));
            var to = "k%03d".formatted(random.nextInt(120));
            var next = copy(state);
            var moved = next.get(MESSAGES.name()).remove(from);
            if (moved == null) {
                moved = "new";
            }
            next.get(MESSAGES.name()).put(to, moved);
            next.get(CHATS.name()).put(to, "moved");
            ledger.update(transaction -> {
                var value = transaction.get(MESSAGES, from).orElse("new");
                transaction.delete(MESSAGES, from);
                transaction.put(MESSAGES, to, value);
                transaction.put(CHATS, to, "moved");
            });
            commit(next);
        }

        private void commit(Map<String, TreeMap<String, String>> next) {
            for (var entry : next.entrySet()) {
                state.put(entry.getKey(), entry.getValue());
            }
            visible++;
            history.add(copy(state));
            assertTrue(history.size() == visible + 1);
        }

        private String value() {
            var length = random.nextInt(10) == 0 ? 2_500 + random.nextInt(6_000) : random.nextInt(80);
            if (largeValues == Long.MAX_VALUE && random.nextInt(40) == 0) {
                // On an array of its own in its batch
                length = 130_000 + random.nextInt(140_000);
            }
            return letters(length);
        }

        private String letters(int length) {
            var builder = new StringBuilder(length);
            for (var i = 0; i < length; i++) {
                builder.append((char) ('a' + random.nextInt(26)));
            }
            return builder.toString();
        }

        private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
            var copy = new TreeMap<String, TreeMap<String, String>>();
            state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
            return copy;
        }
    }
}
