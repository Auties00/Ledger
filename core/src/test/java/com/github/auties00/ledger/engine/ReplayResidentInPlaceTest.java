package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The open's replay overwriting in place the arrays of the resident versions of byte arrays the open made, by its
/// resident loader or by the replay itself: a key overwritten many times with values of the same and of other lengths
/// reads its last value after the open, after a checkpoint and after a reopen; the first replayed put of a loaded key
/// with the loaded value's length writes the loader's array; an array a read returned after the open is never written
/// again; a store whose replay fails part way reads the data file's values at the next open; and the overwrite leaves
/// alone the versions a unit of the file of large values holds and the arrays of a key space with a codec of its own.
/// Range removals among the replayed writes, and removals of loaded keys put again, keep the counts. A replayed put over
/// a version an earlier replayed commit made, with no older version, restamps that version rather than making one,
/// exactly as often as the tests count, and the store reads the same values, scans and counts after the open, a
/// checkpoint and a reopen, with the first checkpoint due at the same heap as without the restamps.
class ReplayResidentInPlaceTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    // Byte arrays under a codec of the user's, whose decoded value the store never takes for its encoding
    private static final Keyspace<String, byte[]> CODED = Keyspace.resident("coded", KeyCodec.utf8(), ValueCodec.of(
            value -> value.length,
            (value, target) -> MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length),
            Segments::toBytes));

    private static Ledger open(Path directory, boolean encrypted, long largeValues, boolean atOpen) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(CODED).checkpointEvery(1L << 40).largeValues(largeValues)
                .loadResidentAtOpen(atOpen))
                .checkpointHeap(1L << 40)
                .build();
    }

    private static Ledger open(Path directory, boolean encrypted) {
        return open(directory, encrypted, Long.MAX_VALUE, false);
    }

    private static byte[] value(Random random, int length) {
        var value = new byte[length];
        random.nextBytes(value);
        return value;
    }

    /// Checks that `ledger` holds exactly `model` in [#SESSIONS].
    private static void check(Ledger ledger, TreeMap<String, byte[]> model) {
        check(ledger, SESSIONS, model);
    }

    /// Checks that `ledger` holds exactly `model` in `keyspace`.
    private static void check(View ledger, Keyspace<String, byte[]> keyspace, TreeMap<String, byte[]> model) {
        for (var entry : model.entrySet()) {
            assertArrayEquals(entry.getValue(), ledger.get(keyspace, entry.getKey()).orElse(null), entry.getKey());
        }
        var found = new TreeMap<String, byte[]>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            found.put(key, value);
            return true;
        });
        assertEquals(model.keySet(), found.keySet());
        for (var entry : model.entrySet()) {
            assertArrayEquals(entry.getValue(), found.get(entry.getKey()), entry.getKey());
        }
        assertEquals(model.size(), ledger.size(keyspace));
    }

    /// Writes [#LOADED] keys of `keyspace` with values of 300 bytes and checkpoints them, then overwrites in durable
    /// commits of a few writes the first 40 keys with values of 300 bytes, keys 40 to 49 with values of 301 bytes first
    /// and of any length after, puts new keys, removes keys 50 and 51 and puts key 50 again; and crashes.
    ///
    /// @return the values the store holds, by key
    private static TreeMap<String, byte[]> loadedThenOverwritten(Path directory, Keyspace<String, byte[]> keyspace,
                                                                  boolean encrypted, boolean atOpen, Random random)
            throws InterruptedException {
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < LOADED; i++) {
                var value = value(random, 300);
                batch.put(keyspace, loadedKey(i), value);
                model.put(loadedKey(i), value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            for (var commit = 0; commit < 120; commit++) {
                var overwrites = ledger.newBatch();
                if (commit == 0) {
                    // Another length first, so the first replayed put of each key takes an array of its own
                    for (var index = 40; index < 50; index++) {
                        var value = value(random, 301);
                        overwrites.put(keyspace, loadedKey(index), value);
                        model.put(loadedKey(index), value);
                    }
                }
                for (var i = 0; i < 3; i++) {
                    var index = random.nextInt(50);
                    var length = index < 40 || random.nextBoolean() ? 300 : 1 + random.nextInt(600);
                    var value = value(random, length);
                    overwrites.put(keyspace, loadedKey(index), value);
                    model.put(loadedKey(index), value);
                }
                if (commit % 10 == 0) {
                    var value = value(random, 300);
                    overwrites.put(keyspace, "new" + commit, value);
                    model.put("new" + commit, value);
                }
                if (commit == 30) {
                    overwrites.delete(keyspace, loadedKey(50)).delete(keyspace, loadedKey(51));
                    model.remove(loadedKey(50));
                    model.remove(loadedKey(51));
                }
                if (commit == 60) {
                    var value = value(random, 300);
                    overwrites.put(keyspace, loadedKey(50), value);
                    model.put(loadedKey(50), value);
                }
                overwrites.put(MESSAGES, "m" + commit, "message " + commit);
                ledger.commit(overwrites).awaitDurable();
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        return model;
    }

    // The keys the checkpoint holds before the replayed overwrites
    private static final int LOADED = 2_000;

    private static String loadedKey(int index) {
        return "k%05d".formatted(index);
    }

    /// Overwrites a few keys `commits` times, each commit durable alone, with values whose lengths stay the same for
    /// some keys and change for others, two puts of one key in some commits.
    private static void overwrite(Ledger ledger, TreeMap<String, byte[]> model, Random random, int commits)
            throws InterruptedException {
        for (var commit = 0; commit < commits; commit++) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 3; i++) {
                var index = random.nextInt(8);
                var key = "s" + index;
                // Keys below 4 keep one length, the others change it now and then
                var length = index < 4 ? 200 + index * 300 : random.nextInt(3) == 0 ? 1 + random.nextInt(2_000) : 1_000;
                var value = value(random, length);
                batch.put(SESSIONS, key, value);
                model.put(key, value);
            }
            batch.put(MESSAGES, "m" + commit, "message " + commit);
            ledger.commit(batch).awaitDurable();
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a key overwritten with values of the same and of other lengths reads its last value after the open, a checkpoint and a reopen")
    void lastValueWins(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(encrypted ? 1 : 2);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted)) {
            overwrite(ledger, model, random, 300);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
            overwrite(ledger, model, random, 50);
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
            ledger.checkpoint();
            check(ledger, model);
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
        }
    }

    @Test
    @DisplayName("an array a read returned after the open, or a snapshot reads, keeps its bytes when a later commit puts a value of its length")
    void readArraysStay(@TempDir Path directory) throws Exception {
        var random = new Random(3);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, true)) {
            overwrite(ledger, model, random, 100);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true)) {
            var read = ledger.get(SESSIONS, "s1").orElseThrow();
            var bytes = read.clone();
            assertArrayEquals(model.get("s1"), bytes);
            try (var snapshot = ledger.snapshot()) {
                for (var i = 0; i < 20; i++) {
                    var batch = ledger.newBatch();
                    var next = value(random, read.length);
                    batch.put(SESSIONS, "s1", next);
                    model.put("s1", next);
                    ledger.commit(batch).awaitDurable();
                }
                assertArrayEquals(bytes, read);
                assertArrayEquals(bytes, snapshot.get(SESSIONS, "s1").orElseThrow());
            }
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true)) {
            check(ledger, model);
        }
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a replayed put of a value in the file of large values over a replayed version of its length, and one over it, keep their own bytes")
    void externalOverOwn(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(4);
        var model = new TreeMap<String, byte[]>();
        // The first open writes values of 6,000 bytes in its frames, the second, with a lower threshold, to the file
        try (var ledger = open(directory, encrypted)) {
            for (var i = 0; i < 10; i++) {
                var value = value(random, 6_000);
                ledger.commit(ledger.newBatch().put(SESSIONS, "s", value)).awaitDurable();
                model.put("s", value);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 4_096, false)) {
            check(ledger, model);
            for (var i = 0; i < 4; i++) {
                var value = value(random, 6_000);
                ledger.commit(ledger.newBatch().put(SESSIONS, "s", value)).awaitDurable();
                model.put("s", value);
            }
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        // Values below the threshold again, over the last external version
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
            for (var i = 0; i < 4; i++) {
                var value = value(random, 6_000);
                ledger.commit(ledger.newBatch().put(SESSIONS, "s", value)).awaitDurable();
                model.put("s", value);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, 4_096, false)) {
            check(ledger, model);
            ledger.checkpoint();
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
        }
    }

    @ParameterizedTest(name = "removal between puts: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a range removal between two replayed puts of a key, of one length, leaves the second put's value, or the key removed")
    void rangeRemovalBetween(boolean putAfter, @TempDir Path directory) throws Exception {
        var random = new Random(5);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, true)) {
            for (var round = 0; round < 6; round++) {
                var first = value(random, 500);
                ledger.commit(ledger.newBatch().put(SESSIONS, "a1", first).put(SESSIONS, "b1", value(random, 500))).awaitDurable();
                ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between("a", "b"))).awaitDurable();
                model.remove("a1");
                if (putAfter || round < 5) {
                    var second = value(random, 500);
                    ledger.commit(ledger.newBatch().put(SESSIONS, "a1", second)).awaitDurable();
                    model.put("a1", second);
                }
                var b = value(random, 500);
                ledger.commit(ledger.newBatch().put(SESSIONS, "b1", b)).awaitDurable();
                model.put("b1", b);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true)) {
            check(ledger, model);
            ledger.checkpoint();
            check(ledger, model);
        }
        try (var ledger = open(directory, true)) {
            check(ledger, model);
        }
    }

    @ParameterizedTest(name = "loaded at open: {0}, encrypted: {1}")
    @CsvSource({"true, true", "false, true", "true, false"})
    @DisplayName("overwrites replayed over versions the checkpoint holds, loaded in the background while the replay runs or at first use, read the last value")
    void overCheckpoint(boolean atOpen, boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(6);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 5_000; i++) {
                var value = value(random, 300 + i % 3);
                batch.put(SESSIONS, "k%05d".formatted(i), value);
                model.put("k%05d".formatted(i), value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            for (var commit = 0; commit < 200; commit++) {
                var overwrites = ledger.newBatch();
                for (var i = 0; i < 4; i++) {
                    var key = "k%05d".formatted(random.nextInt(50) * 100);
                    var value = value(random, model.get(key).length);
                    overwrites.put(SESSIONS, key, value);
                    model.put(key, value);
                }
                ledger.commit(overwrites).awaitDurable();
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, !atOpen)) {
            ledger.checkpoint();
            check(ledger, model);
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
            assertTrue(Arrays.equals(model.get("k00000"), ledger.get(SESSIONS, "k00000").orElseThrow()));
        }
    }

    @ParameterizedTest(name = "loaded at open: {0}, encrypted: {1}")
    @CsvSource({"true, true", "false, true", "true, false", "false, false"})
    @DisplayName("loaded keys overwritten with values of their length and of others, removed and put again, read their last values and keep the count after the open, a checkpoint and a reopen")
    void loadedKeysOverwritten(boolean atOpen, boolean encrypted, @TempDir Path directory) throws Exception {
        var model = loadedThenOverwritten(directory, SESSIONS, encrypted, atOpen, new Random(atOpen ? 7 : 8));
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, !atOpen)) {
            check(ledger, model);
            ledger.checkpoint();
            check(ledger, model);
        }
        try (var ledger = open(directory, encrypted)) {
            check(ledger, model);
        }
    }

    @Test
    @DisplayName("a key space with a codec of its own reads the last values of loaded keys overwritten with values of their length")
    void codecKeysOverwritten(@TempDir Path directory) throws Exception {
        var model = loadedThenOverwritten(directory, CODED, true, false, new Random(9));
        try (var ledger = open(directory, true)) {
            check(ledger, CODED, model);
            ledger.checkpoint();
        }
        try (var ledger = open(directory, true)) {
            check(ledger, CODED, model);
        }
    }

    @ParameterizedTest(name = "{0}, loaded at open: {1}, longer: {2}")
    @CsvSource({"sessions, true, false", "sessions, false, false", "sessions, true, true", "sessions, false, true",
            "coded, true, false", "coded, false, false"})
    @DisplayName("the first replayed put of a loaded key of byte arrays with the loaded value's length writes the loader's array, and one of a key space with a codec of its own never does")
    void loaderArraysReused(String name, boolean atOpen, boolean longer, @TempDir Path directory) throws Exception {
        var keyspace = name.equals("sessions") ? SESSIONS : CODED;
        var random = new Random(12);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, true, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < KEYS; i++) {
                var value = value(random, LENGTH);
                batch.put(keyspace, loadedKey(i), value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            // Each loaded value overwritten once, with a value of its length or one byte longer
            for (var i = 0; i < KEYS; i++) {
                var value = value(random, longer ? LENGTH + 1 : LENGTH);
                ledger.commit(ledger.newBatch().put(keyspace, loadedKey(i), value)).awaitDurable();
                model.put(loadedKey(i), value);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true, Long.MAX_VALUE, atOpen)) {
            // Each key is replayed once, so every array a replayed put wrote is one the loader made
            var inPlace = ((LiveLedger) ledger).replayedInPlaceForTests();
            assertEquals(keyspace == SESSIONS && !longer ? KEYS : 0, inPlace);
            check(ledger, keyspace, model);
            ledger.checkpoint();
            check(ledger, keyspace, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true, Long.MAX_VALUE, !atOpen)) {
            check(ledger, keyspace, model);
        }
    }

    // The loaded keys of loaderArraysReused and the length of their values
    private static final int KEYS = 40;
    private static final int LENGTH = 32 << 10;

    @ParameterizedTest(name = "loaded at open: {0}, encrypted: {1}")
    @CsvSource({"true, true", "false, true", "true, false"})
    @DisplayName("a replayed range removal before puts of loaded keys with the loaded length keeps the values, the scan and the count, after a checkpoint and a reopen")
    void rangeRemovalBeforeLoadedOverwrite(boolean atOpen, boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(10);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var prefix : new String[]{"a", "b", "c"}) {
                for (var i = 0; i < 10; i++) {
                    var value = value(random, 300);
                    batch.put(SESSIONS, prefix + i, value);
                    model.put(prefix + i, value);
                }
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            // Loaded keys outside the removal first, inside it after
            for (var round = 0; round < 3; round++) {
                var value = value(random, 300);
                ledger.commit(ledger.newBatch().put(SESSIONS, "c" + round, value)).awaitDurable();
                model.put("c" + round, value);
            }
            ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between("a", "b"))).awaitDurable();
            model.keySet().removeIf(key -> key.startsWith("a"));
            for (var round = 0; round < 6; round++) {
                for (var key : new String[]{"a" + round, "b" + round, "c" + (round + 3)}) {
                    var value = value(random, 300);
                    ledger.commit(ledger.newBatch().put(SESSIONS, key, value)).awaitDurable();
                    model.put(key, value);
                }
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            check(ledger, model);
            ledger.checkpoint();
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, !atOpen)) {
            check(ledger, model);
        }
    }

    @ParameterizedTest(name = "loaded at open: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an open whose replay fails at a damaged frame after overwriting loaded arrays leaves the data file's values to the next open, with the frame restored or the log cut there")
    void failedReplayKeepsTreeValues(boolean atOpen, @TempDir Path base) throws Exception {
        var directory = base.resolve("store");
        var random = new Random(11);
        // The values after each commit, by the commit's sequence, and the checkpoint's
        var states = new TreeMap<Long, TreeMap<String, byte[]>>();
        var model = new TreeMap<String, byte[]>();
        Path segment;
        try (var ledger = open(directory, true, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                var value = value(random, 300);
                batch.put(SESSIONS, loadedKey(i), value);
                model.put(loadedKey(i), value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            for (var commit = 0; commit < 40; commit++) {
                var key = loadedKey(random.nextInt(30));
                var value = value(random, 300);
                var done = ledger.commit(ledger.newBatch().put(SESSIONS, key, value));
                done.awaitDurable();
                model.put(key, value);
                states.put(done.sequence(), new TreeMap<>(model));
            }
            segment = directory.resolve("log").resolve(LogWriter.segmentName(((LiveLedger) ledger).log().currentSegment()));
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        // A copy of the crashed store, whose log is cut at the damaged frame below
        var copy = base.resolve("cut");
        copyTree(directory, copy);
        var original = Files.readAllBytes(segment);
        var frames = frames(original);
        // A frame of a commit two thirds in, after many replayed overwrites, with forced frames after it
        var sequences = new ArrayList<>(states.keySet());
        var cut = sequences.get(sequences.size() * 2 / 3);
        var at = frames.get(cut);
        assertNotNull(at, "no frame of commit " + cut);
        var damaged = original.clone();
        damaged[at + LogWriter.FRAME_HEADER + 2] ^= 0x40;
        Files.write(segment, damaged);
        assertThrows(LedgerCorruptedException.class, () -> open(directory, true, Long.MAX_VALUE, atOpen));
        assertThrows(LedgerCorruptedException.class, () -> open(directory, true, Long.MAX_VALUE, !atOpen));
        Files.write(segment, original);
        try (var ledger = open(directory, true, Long.MAX_VALUE, atOpen)) {
            check(ledger, model);
        }
        // The log cut at the damaged frame: the commits before it, over the checkpoint's values
        try (var channel = FileChannel.open(copy.resolve(directory.relativize(segment)), StandardOpenOption.WRITE)) {
            channel.truncate(at);
        }
        var before = states.get(sequences.get(sequences.indexOf(cut) - 1));
        try (var ledger = open(copy, true, Long.MAX_VALUE, atOpen)) {
            check(ledger, before);
        }
    }

    /// Copies the files under `from` to the same paths under `to`.
    private static void copyTree(Path from, Path to) throws IOException {
        try (var files = Files.walk(from)) {
            for (var file : files.toList()) {
                var target = to.resolve(from.relativize(file).toString());
                if (Files.isDirectory(file)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(file, target);
                }
            }
        }
    }

    /// {@return where each commit frame of the log segment `bytes` starts, by the sequence its header names}
    private static Map<Long, Integer> frames(byte[] bytes) {
        var frames = new HashMap<Long, Integer>();
        var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        for (var at = LogWriter.SEGMENT_HEADER; at + LogWriter.FRAME_HEADER <= bytes.length; at++) {
            if (bytes[at] == 'L' && bytes[at + 1] == 'G' && bytes[at + 2] == 'F' && bytes[at + 3] == '1') {
                frames.putIfAbsent(buffer.getLong(at + 8), at);
            }
        }
        return frames;
    }

    @Test
    @DisplayName("a commit that puts one key twice replays both puts and reads the second")
    void twoPutsInOneCommit(@TempDir Path directory) throws Exception {
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, true)) {
            for (var i = 0; i < 10; i++) {
                var first = ("first " + i).repeat(20).getBytes(StandardCharsets.US_ASCII);
                var second = ("other " + i).repeat(20).getBytes(StandardCharsets.US_ASCII);
                ledger.commit(ledger.newBatch().put(SESSIONS, "s", first).put(SESSIONS, "s", second)).awaitDurable();
                model.put("s", second);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, true)) {
            check(ledger, model);
        }
    }

    /// {@return the puts of the open of `ledger` that restamped the head of their chain}
    private static long restamped(Ledger ledger) {
        return ((LiveLedger) ledger).restampedForTests();
    }

    /// Opens `directory`, checks `model` in [#SESSIONS] with a snapshot taken at once and with the store, and closes it
    /// without a checkpoint.
    ///
    /// @return the puts the open restamped
    private static long reopenAndCheck(Path directory, boolean encrypted, boolean atOpen, TreeMap<String, byte[]> model) {
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            try (var snapshot = ledger.snapshot()) {
                check(snapshot, SESSIONS, model);
            }
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
            return restamped(ledger);
        }
    }

    /// Checks `model` after a checkpoint of the store at `directory`, and after a reopen whose log the checkpoint
    /// retired, so the data file alone holds the values the restamped versions had.
    private static void checkpointAndCheck(Path directory, boolean encrypted, boolean atOpen,
                                           TreeMap<String, byte[]> model) {
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            ledger.checkpoint();
            check(ledger, model);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, !atOpen)) {
            assertEquals(0, restamped(ledger), "a reopen after the checkpoint replays nothing");
            check(ledger, model);
        }
    }

    @ParameterizedTest(name = "loaded at open: {0}, encrypted: {1}")
    @CsvSource({"true, true", "false, true", "true, false", "false, false"})
    @DisplayName("a loaded key and a new key overwritten in separate commits with one length restamp every put after their first replayed one")
    void restampsOverwrites(boolean atOpen, boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(atOpen ? 13 : 14);
        var model = new TreeMap<String, byte[]>();
        var commits = 25;
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                var value = value(random, 300);
                batch.put(SESSIONS, loadedKey(i), value);
                model.put(loadedKey(i), value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            for (var commit = 0; commit < commits; commit++) {
                var loaded = value(random, 300);
                var fresh = value(random, 700);
                ledger.commit(ledger.newBatch().put(SESSIONS, loadedKey(7), loaded)).awaitDurable();
                ledger.commit(ledger.newBatch().put(SESSIONS, "fresh", fresh)).awaitDurable();
                model.put(loadedKey(7), loaded);
                model.put("fresh", fresh);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        // The first replayed put of each key makes a version, over the loader's or over none; each later one restamps
        // it, as the commit that made it pruned the version it replaced
        assertEquals(2L * (commits - 1), reopenAndCheck(directory, encrypted, atOpen, model));
        // The same log replayed again by an open that loads the other way
        assertEquals(2L * (commits - 1), reopenAndCheck(directory, encrypted, !atOpen, model));
        checkpointAndCheck(directory, encrypted, atOpen, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("put, delete, put and put of one key restamps only the last put and keeps the count")
    void restampAfterDelete(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(15);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted)) {
            var other = value(random, 64);
            ledger.commit(ledger.newBatch().put(SESSIONS, "other", other)).awaitDurable();
            model.put("other", other);
            ledger.commit(ledger.newBatch().put(SESSIONS, "s", value(random, 400))).awaitDurable();
            ledger.commit(ledger.newBatch().delete(SESSIONS, "s")).awaitDurable();
            ledger.commit(ledger.newBatch().put(SESSIONS, "s", value(random, 400))).awaitDurable();
            var last = value(random, 400);
            ledger.commit(ledger.newBatch().put(SESSIONS, "s", last)).awaitDurable();
            model.put("s", last);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        assertEquals(1, reopenAndCheck(directory, encrypted, false, model));
        checkpointAndCheck(directory, encrypted, false, model);
    }

    @ParameterizedTest(name = "loaded at open: {0}, encrypted: {1}")
    @CsvSource({"true, true", "false, true", "true, false", "false, false"})
    @DisplayName("a replayed range removal removes the keys restamped before it, and no put of its key space restamps after it")
    void rangeRemovalAfterRestamps(boolean atOpen, boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(16);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted, Long.MAX_VALUE, atOpen)) {
            var batch = ledger.newBatch();
            for (var key : new String[]{"a0", "b0", "c0"}) {
                var value = value(random, 200);
                batch.put(SESSIONS, key, value);
                model.put(key, value);
            }
            ledger.commit(batch).awaitDurable();
            ledger.checkpoint();
            // Two restamps of a1 and one of b1
            for (var key : new String[]{"a1", "a1", "a1", "b1", "b1"}) {
                var value = value(random, 200);
                ledger.commit(ledger.newBatch().put(SESSIONS, key, value)).awaitDurable();
                model.put(key, value);
            }
            ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between("a", "b"))).awaitDurable();
            model.keySet().removeIf(key -> key.startsWith("a"));
            // Overwrites of one length outside the removal and inside it, none of which restamps
            for (var key : new String[]{"b1", "b1", "b1", "a2", "a2", "a2", "c0", "c0"}) {
                var value = value(random, 200);
                ledger.commit(ledger.newBatch().put(SESSIONS, key, value)).awaitDurable();
                model.put(key, value);
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        assertEquals(3, reopenAndCheck(directory, encrypted, atOpen, model));
        checkpointAndCheck(directory, encrypted, atOpen, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("two puts of a new key in one commit and a third in a later one restamp the first put's version twice")
    void restampWithinCommit(boolean encrypted, @TempDir Path directory) throws Exception {
        var random = new Random(17);
        var model = new TreeMap<String, byte[]>();
        try (var ledger = open(directory, encrypted)) {
            ledger.commit(ledger.newBatch().put(SESSIONS, "n", value(random, 90)).put(SESSIONS, "n", value(random, 90)))
                    .awaitDurable();
            var last = value(random, 90);
            ledger.commit(ledger.newBatch().put(SESSIONS, "n", last)).awaitDurable();
            model.put("n", last);
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        assertEquals(2, reopenAndCheck(directory, encrypted, false, model));
        checkpointAndCheck(directory, encrypted, false, model);
    }

    @ParameterizedTest(name = "encrypted: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("a store whose replay restamps estimates the same replayed heap as one whose key space never does")
    void replayedHeapUnchanged(boolean encrypted, @TempDir Path base) throws Exception {
        var heaps = new long[2];
        var restamps = new long[2];
        var keyspaces = List.of(SESSIONS, CODED);
        for (var i = 0; i < 2; i++) {
            var directory = base.resolve("store" + i);
            var keyspace = keyspaces.get(i);
            var random = new Random(18);
            try (var ledger = open(directory, encrypted)) {
                for (var commit = 0; commit < 60; commit++) {
                    var batch = ledger.newBatch();
                    for (var write = 0; write < 3; write++) {
                        batch.put(keyspace, "s" + random.nextInt(6), value(random, 250));
                    }
                    ledger.commit(batch).awaitDurable();
                }
                ((LiveLedger) ledger).closeWithoutCheckpoint();
            }
            try (var ledger = open(directory, encrypted)) {
                heaps[i] = ((LiveLedger) ledger).replayedHeap();
                restamps[i] = restamped(ledger);
            }
        }
        assertTrue(restamps[0] > 100, "restamps " + restamps[0]);
        assertEquals(0, restamps[1]);
        assertEquals(heaps[1], heaps[0]);
    }
}
