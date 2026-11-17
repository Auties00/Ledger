package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Recovery that opens frames into the arrays of the frames it replayed ([FrameReadAhead.Spares]): a store reopened
/// after commits whose frames grow and shrink, across opens that add a catalog and an epoch frame, finds every value of
/// key spaces resident and paged, of byte arrays, of strings, and of codecs of the user's that keep their input or copy
/// it, with values in the file of large values among them, in a log read ahead or not, encrypted or plain, its values
/// kept in the heap or moved to the log as the open replays them.
class ReplayRecycledFramesTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    // Its decoder keeps the memory it is given, which the codec, not trusted, copies first
    private static final Keyspace<String, MemorySegment> KEEPING = Keyspace.resident("keeping", KeyCodec.utf8(),
            ValueCodec.of(MemorySegment::byteSize, (value, target) -> target.copyFrom(value), source -> source));
    // Its decoder reads the store's memory in place and copies what it keeps, as a trusted one must
    private static final Keyspace<String, String> COPYING = Keyspace.resident("copying", KeyCodec.utf8(),
            ValueCodec.ofTrusted(String::length,
                    (value, target) -> target.copyFrom(MemorySegment.ofArray(value.getBytes(StandardCharsets.US_ASCII))),
                    source -> new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII)));
    // Added by the second open, which writes a catalog frame for it
    private static final Keyspace<String, String> LATER = Keyspace.paged("later", KeyCodec.utf8(), ValueCodec.utf8());
    private static final long LARGE_VALUES = 100_000;
    // The bytes of the values of the commits, in the order the commits take them: frames that grow and shrink, some
    // longer than the recovery buffer and than half a region, one longer than the read-ahead reads
    private static final int[] SIZES = {300, 40_000, 2_000, 600_000, 900, 1_300_000, 70_000, 50, 250_000, 9_000_000,
            1_000, 480_000, 120};

    @AfterEach
    void defaultReadAhead() {
        FrameReadAhead.framesForTests(-1);
    }

    private static Ledger open(Path directory, boolean encrypted, boolean spill, boolean later) {
        var builder = Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(SESSIONS).keyspace(CHATS).keyspace(MESSAGES).keyspace(KEEPING).keyspace(COPYING)
                .checkpointEvery(1L << 40).largeValues(LARGE_VALUES);
        if (later) {
            builder.keyspace(LATER);
        }
        var ledger = ((LedgerBuilder) builder).checkpointHeap(1L << 40).segmentSize(16L << 20);
        if (spill) {
            ledger.spillBudget(0);
        }
        return ledger.build();
    }

    /// The contents a workload expects of each key space, its values as text.
    private static final class Model {
        private final Map<String, TreeMap<String, String>> contents = new LinkedHashMap<>();

        TreeMap<String, String> of(Keyspace<?, ?> keyspace) {
            return contents.computeIfAbsent(keyspace.name(), _ -> new TreeMap<>());
        }

        /// Checks that `ledger` holds exactly the contents, and counts them.
        void check(Ledger ledger, boolean later) {
            check(ledger, SESSIONS, value -> new String(value, StandardCharsets.US_ASCII));
            check(ledger, CHATS, value -> value);
            check(ledger, MESSAGES, value -> value);
            check(ledger, KEEPING, value -> new String(value.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII));
            check(ledger, COPYING, value -> value);
            if (later) {
                check(ledger, LATER, value -> value);
            }
        }

        private <V> void check(Ledger ledger, Keyspace<String, V> keyspace, java.util.function.Function<V, String> text) {
            var found = new TreeMap<String, String>();
            ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                found.put(key, text.apply(value));
                return true;
            });
            var expected = of(keyspace);
            assertEquals(expected.keySet(), found.keySet(), keyspace.name());
            for (var entry : expected.entrySet()) {
                assertEquals(entry.getValue(), found.get(entry.getKey()), keyspace.name() + " " + entry.getKey());
            }
            assertEquals(expected.size(), ledger.size(keyspace), keyspace.name());
            for (var entry : expected.entrySet()) {
                assertEquals(entry.getValue(), ledger.get(keyspace, entry.getKey()).map(text).orElse(null),
                        keyspace.name() + " " + entry.getKey());
            }
        }
    }

    private static String text(Random random, int length) {
        var chars = new byte[length];
        for (var i = 0; i < length; i++) {
            chars[i] = (byte) ('a' + random.nextInt(26));
        }
        return new String(chars, StandardCharsets.US_ASCII);
    }

    /// Commits `commits` commits, each durable alone, so each is a frame of its own whose length follows [#SIZES]: the
    /// sessions and the other resident key spaces overwritten with values of the same and of other lengths, messages
    /// put, removed and removed by range.
    private static void commit(Ledger ledger, Model model, Random random, int first, int commits, boolean later)
            throws InterruptedException {
        for (var index = first; index < first + commits; index++) {
            var batch = ledger.newBatch();
            var bytes = SIZES[index % SIZES.length];
            // Below the threshold of large values, so the frame holds them
            var messages = 1 + bytes / 50_000;
            for (var i = 0; i < messages; i++) {
                var key = "m%05d-%d".formatted(index, i);
                var value = text(random, Math.max(1, bytes / messages));
                batch.put(MESSAGES, key, value);
                model.of(MESSAGES).put(key, value);
            }
            if (random.nextInt(8) == 0) {
                var key = "m%05d-large".formatted(index);
                var value = text(random, (int) LARGE_VALUES + random.nextInt(50_000));
                batch.put(MESSAGES, key, value);
                model.of(MESSAGES).put(key, value);
            }
            for (var i = 0; i < 3; i++) {
                var key = "s" + random.nextInt(6);
                var previous = model.of(SESSIONS).get(key);
                // Half of the overwrites keep the length of the value they replace
                var length = previous != null && random.nextBoolean() ? previous.length()
                        : random.nextInt(10) == 0 ? (int) LARGE_VALUES + random.nextInt(50_000) : 1 + random.nextInt(3_000);
                var value = text(random, length);
                batch.put(SESSIONS, key, value.getBytes(StandardCharsets.US_ASCII));
                model.of(SESSIONS).put(key, value);
            }
            var chat = "c" + random.nextInt(8);
            var chatValue = text(random, 1 + random.nextInt(400));
            batch.put(CHATS, chat, chatValue);
            model.of(CHATS).put(chat, chatValue);
            var kept = "k" + random.nextInt(8);
            var keptValue = text(random, 1 + random.nextInt(2_000));
            batch.put(KEEPING, kept, MemorySegment.ofArray(keptValue.getBytes(StandardCharsets.US_ASCII)));
            model.of(KEEPING).put(kept, keptValue);
            var copied = "p" + random.nextInt(8);
            var copiedValue = text(random, 1 + random.nextInt(2_000));
            batch.put(COPYING, copied, copiedValue);
            model.of(COPYING).put(copied, copiedValue);
            if (later) {
                var key = "l%05d".formatted(index);
                var value = text(random, 1 + random.nextInt(5_000));
                batch.put(LATER, key, value);
                model.of(LATER).put(key, value);
            }
            if (index > 0 && random.nextInt(4) == 0) {
                var key = "m%05d-0".formatted(random.nextInt(index));
                batch.delete(MESSAGES, key);
                model.of(MESSAGES).remove(key);
            }
            if (index > 0 && random.nextInt(9) == 0) {
                var from = "c" + random.nextInt(4);
                var to = "c" + (4 + random.nextInt(4));
                batch.deleteRange(CHATS, KeyRange.between(from, to));
                model.of(CHATS).subMap(from, to).clear();
            }
            ledger.commit(batch).awaitDurable();
        }
    }

    @ParameterizedTest(name = "encrypted: {0}, frames read ahead: {1}, values moved: {2}")
    @CsvSource({"true, 3, false", "true, 0, false", "false, 0, false", "true, 3, true", "false, 0, true"})
    @DisplayName("an open replays frames that grow and shrink, a catalog and an epoch, opening them into the arrays of the frames before them, and finds every value")
    void growingAndShrinking(boolean encrypted, int ahead, boolean spill, @TempDir Path directory) throws Exception {
        FrameReadAhead.framesForTests(ahead);
        var random = new Random(ahead * 7L + (encrypted ? 1 : 0) + (spill ? 2 : 0));
        var model = new Model();
        try (var ledger = open(directory, encrypted, spill, false)) {
            commit(ledger, model, random, 0, 2 * SIZES.length, false);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        var before = FrameReadAhead.Spares.reusedForTests();
        // The second open replays the first one's log, binds the new key space in a catalog frame, and writes its
        // epoch with its first commit
        try (var ledger = open(directory, encrypted, spill, true)) {
            model.check(ledger, false);
            commit(ledger, model, random, 2 * SIZES.length, SIZES.length, true);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        assertTrue(FrameReadAhead.Spares.reusedForTests() > before, "no frame was opened into an array of another");
        try (var ledger = open(directory, encrypted, spill, true)) {
            model.check(ledger, true);
            ledger.checkpoint();
            model.check(ledger, true);
        }
        try (var ledger = open(directory, encrypted, spill, true)) {
            model.check(ledger, true);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @CsvSource({"true", "false"})
    @DisplayName("a decoded value that keeps the memory of an untrusted codec, or a copy a trusted one made, survives the later frames opened into its frame's array")
    void decodedValuesSurvive(boolean encrypted, @TempDir Path directory) throws Exception {
        FrameReadAhead.framesForTests(encrypted ? 3 : 0);
        var random = new Random(5);
        var model = new Model();
        try (var ledger = open(directory, encrypted, false, false)) {
            commit(ledger, model, random, 0, 3 * SIZES.length, false);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, false, false)) {
            var kept = new ArrayList<MemorySegment>();
            var copied = new ArrayList<String>();
            for (var key : model.of(KEEPING).keySet()) {
                kept.add(ledger.get(KEEPING, key).orElseThrow());
            }
            for (var key : model.of(COPYING).keySet()) {
                copied.add(ledger.get(COPYING, key).orElseThrow());
            }
            model.check(ledger, false);
            assertEquals(List.copyOf(model.of(KEEPING).values()),
                    kept.stream().map(value -> new String(value.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII)).toList());
            assertEquals(List.copyOf(model.of(COPYING).values()), copied);
        }
    }
}
