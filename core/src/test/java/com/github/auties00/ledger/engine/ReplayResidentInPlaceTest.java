package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The open's replay overwriting in place the arrays of the resident versions of byte arrays it made itself: a key
/// overwritten many times with values of the same and of other lengths reads its last value after the open, after a
/// checkpoint and after a reopen; an array a read returned after the open is never written again; and the overwrite
/// leaves alone the versions a unit of the file of large values holds, the versions the checkpoint loaded, those under
/// a range removal, and those the background loader loads while the replay runs.
class ReplayResidentInPlaceTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

    private static Ledger open(Path directory, boolean encrypted, long largeValues, boolean atOpen) {
        return ((LedgerBuilder) Ledger.builder().directory(directory).encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(SESSIONS).keyspace(MESSAGES).checkpointEvery(1L << 40).largeValues(largeValues)
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
        for (var entry : model.entrySet()) {
            assertArrayEquals(entry.getValue(), ledger.get(SESSIONS, entry.getKey()).orElse(null), entry.getKey());
        }
        var found = new TreeMap<String, byte[]>();
        ledger.forEach(SESSIONS, KeyRange.all(), (key, value) -> {
            found.put(key, value);
            return true;
        });
        assertEquals(model.keySet(), found.keySet());
        for (var entry : model.entrySet()) {
            assertArrayEquals(entry.getValue(), found.get(entry.getKey()), entry.getKey());
        }
        assertEquals(model.size(), ledger.size(SESSIONS));
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
}
