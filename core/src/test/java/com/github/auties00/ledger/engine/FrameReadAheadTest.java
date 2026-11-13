package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Recovery with the frames of an encrypted log opened ahead ([FrameReadAhead]): it takes most frames as read ahead,
/// and replays, refuses or stops at exactly what it does when it opens every frame itself, over logs of groups of one
/// and of several frames, across segments, with a frame damaged, its header damaged or the log cut anywhere.
///
/// Each case writes one log, crashes without a checkpoint, and opens copies of the directory with no frame read ahead
/// and with one and three; the outcomes are compared, not predicted.
class FrameReadAheadTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final List<Keyspace<String, String>> KEYSPACES = List.of(MESSAGES, CHATS);
    // Small enough for the log to roll over a few times
    private static final long SEGMENT_SIZE = 256 << 10;

    @AfterEach
    void defaultFrames() {
        FrameReadAhead.framesForTests(-1);
    }

    private static LiveLedger open(Path directory) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(MESSAGES).keyspace(CHATS)).segmentSize(SEGMENT_SIZE).build();
    }

    /// Writes commits of random sizes, alone and from several threads at once so that groups hold one frame and
    /// several, then closes without a checkpoint, so the next open replays every one.
    private static void writeLog(Path directory, long seed) throws InterruptedException {
        try (var ledger = open(directory)) {
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 3; t++) {
                var id = t;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed * 31 + id);
                    for (var commit = 0; commit < 120; commit++) {
                        var batch = ledger.newBatch();
                        var writes = 1 + random.nextInt(12);
                        for (var i = 0; i < writes; i++) {
                            var key = "m%d-%05d-%02d".formatted(id, commit, i);
                            batch.put(MESSAGES, key, "v".repeat(16 + random.nextInt(random.nextInt(8) == 0 ? 6_000 : 400)));
                        }
                        batch.put(CHATS, "chat" + random.nextInt(20), "c" + commit);
                        if (random.nextInt(10) == 0) {
                            batch.delete(MESSAGES, "m%d-%05d-%02d".formatted(id, random.nextInt(commit + 1), 0));
                        }
                        try {
                            ledger.commit(batch).awaitDurable();
                        } catch (InterruptedException interrupted) {
                            throw new IllegalStateException(interrupted);
                        }
                    }
                }));
            }
            for (var thread : threads) {
                thread.join();
            }
            ledger.closeWithoutCheckpoint();
        }
    }

    /// {@return what an open of a copy of `directory` finds, with `frames` frames read ahead: the contents of every
    /// key space, or the failure that refused it}
    private static String outcome(Path directory, Path copy, int frames) throws IOException {
        copyTree(directory, copy);
        FrameReadAhead.framesForTests(frames);
        try (var ledger = open(copy)) {
            var contents = new TreeMap<String, TreeMap<String, String>>();
            for (var keyspace : KEYSPACES) {
                var entries = new TreeMap<String, String>();
                ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                    entries.put(key, value);
                    return true;
                });
                contents.put(keyspace.name(), entries);
            }
            return contents.toString();
        } catch (LedgerCorruptedException refused) {
            return "refused";
        }
    }

    private static void copyTree(Path from, Path to) throws IOException {
        try (var paths = Files.walk(from)) {
            for (var path : paths.toList()) {
                var target = to.resolve(from.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(path, target);
                }
            }
        }
    }

    /// {@return the segments of the log of the store in `directory`, in the order of their numbers}
    private static List<Path> segments(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".log")).sorted().toList();
        }
    }

    /// {@return the offsets of the frames of the segment `bytes`, following the lengths and the padding after groups}
    private static List<Integer> frames(byte[] bytes) {
        var offsets = new ArrayList<Integer>();
        var position = LogWriter.SEGMENT_HEADER;
        while (position + LogWriter.FRAME_HEADER <= bytes.length) {
            if (bytes[position] == 0) {
                var boundary = (position / LogWriter.ALIGNMENT + 1) * LogWriter.ALIGNMENT;
                if (position % LogWriter.ALIGNMENT == 0 || boundary + LogWriter.FRAME_HEADER > bytes.length || bytes[boundary] == 0) {
                    break;
                }
                position = boundary;
            }
            offsets.add(position);
            position += LogWriter.FRAME_HEADER + ByteBuffer.wrap(bytes).getInt(position + 4);
        }
        return offsets;
    }

    @Test
    @DisplayName("takes most frames of an encrypted log as read ahead, and replays what it replays opening each itself")
    void takesFramesAhead(@TempDir Path directory) throws Exception {
        var store = directory.resolve("store");
        writeLog(store, 1);
        var frameCount = 0;
        for (var segment : segments(store)) {
            frameCount += frames(Files.readAllBytes(segment)).size();
        }
        assertTrue(segments(store).size() > 1, "the log did not roll over");
        var alone = outcome(store, directory.resolve("alone"), 0);
        var before = FrameReadAhead.takenForTests();
        var ahead = outcome(store, directory.resolve("ahead"), 3);
        var taken = FrameReadAhead.takenForTests() - before;
        assertEquals(alone, ahead);
        assertTrue(taken >= frameCount * 3L / 4, "taken ahead: " + taken + " of " + frameCount);
    }

    static Stream<Integer> seeds() {
        return IntStream.range(0, 16).boxed();
    }

    @ParameterizedTest(name = "seed {0}")
    @MethodSource("seeds")
    @DisplayName("a damaged frame, a damaged header or a cut log ends or refuses recovery as it does with no frame read ahead")
    void damageAsWithout(int seed, @TempDir Path directory) throws Exception {
        var store = directory.resolve("store");
        writeLog(store, seed);
        var random = new Random(seed);
        // A segment that holds frames: the last one may be empty, prepared for the next writer
        var segments = new ArrayList<Path>();
        for (var path : segments(store)) {
            if (!frames(Files.readAllBytes(path)).isEmpty()) {
                segments.add(path);
            }
        }
        var segment = segments.get(random.nextInt(segments.size()));
        var bytes = Files.readAllBytes(segment);
        var offsets = frames(bytes);
        var frame = offsets.get(random.nextInt(offsets.size()));
        switch (seed % 4) {
            // A byte of a sealed body: the frame fails authentication
            case 0 -> bytes[frame + LogWriter.FRAME_HEADER + random.nextInt(ByteBuffer.wrap(bytes).getInt(frame + 4))] ^= 1;
            // A byte of a frame header: its checksum fails
            case 1 -> bytes[frame + 1 + random.nextInt(LogWriter.FRAME_HEADER - 1)] ^= 0x10;
            // The log cut inside a frame, as a torn write leaves it
            case 2 -> bytes = java.util.Arrays.copyOf(bytes, frame + 1 + random.nextInt(LogWriter.FRAME_HEADER + 64));
            // A frame's first byte zeroed, so the frame reads as the padding after a group
            default -> bytes[frame] = 0;
        }
        Files.write(segment, bytes);
        var alone = outcome(store, directory.resolve("alone"), 0);
        assertEquals(alone, outcome(store, directory.resolve("one"), 1), "one frame read ahead");
        assertEquals(alone, outcome(store, directory.resolve("three"), 3), "three frames read ahead");
    }
}
