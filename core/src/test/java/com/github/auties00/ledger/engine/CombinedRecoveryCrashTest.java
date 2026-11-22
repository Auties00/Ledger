package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/// Power loss at sampled forces of one store that combines a bulk commit's counted run over an empty key space, resident
/// overwrites the replay restamps in place, large values sealed as units of records, idle commits appending to an
/// uncounted run, overwrites, removals and a range removal, checkpoints and a reopen: the recovered store holds a state
/// between the last durable and the last visible commit, and keeps it through the checkpoint after the recovery, which
/// writes the replay's run as a tree straight from its arrays when the store lost power before its first checkpoint,
/// and a reopen.
class CombinedRecoveryCrashTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, byte[]> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.bytes());
    private static final int BULK = LiveLedger.BULK_RUN_WRITES + 52;
    private static final int[] LENGTHS = {40, 100, 300, 1_500, 0, 260, 96, 2_000};
    private static LiveLedger open(Path directory, boolean encrypted) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(SESSIONS).commitWindow(Duration.ZERO)
                .largeValues(8_192))
                .segmentSize(1 << 20)
                .spillBudget(0)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
    }

    private static String value(String key, int generation, int length) {
        var builder = new StringBuilder(length);
        var seed = key.hashCode() * 31 + generation;
        for (var i = 0; i < length; i++) {
            builder.append((char) ('a' + Math.floorMod(seed + i * 7, 26)));
        }
        return builder.toString();
    }

    private static final class Workload {
        final boolean encrypted;
        final boolean largeEarly;
        final List<Map<String, TreeMap<String, String>>> history = new ArrayList<>();
        final Map<String, TreeMap<String, String>> state = new TreeMap<>();
        Map<String, TreeMap<String, String>> next;
        int visible;
        int durable;

        Workload(boolean encrypted, boolean largeEarly) {
            this.encrypted = encrypted;
            this.largeEarly = largeEarly;
            state.put(MESSAGES.name(), new TreeMap<>());
            state.put(SESSIONS.name(), new TreeMap<>());
            history.add(copy(state));
        }

        void put(com.github.auties00.ledger.WriteBatch batch, String key, String value) {
            batch.put(MESSAGES, key, value);
            next.get(MESSAGES.name()).put(key, value);
        }

        void session(com.github.auties00.ledger.WriteBatch batch, String key, String value) {
            batch.put(SESSIONS, key, value.getBytes(StandardCharsets.ISO_8859_1));
            next.get(SESSIONS.name()).put(key, value);
        }

        com.github.auties00.ledger.WriteBatch begin(LiveLedger ledger) {
            next = copy(state);
            return ledger.newBatch();
        }

        void end(LiveLedger ledger, com.github.auties00.ledger.WriteBatch batch, boolean await) throws InterruptedException {
            var commit = ledger.commit(batch);
            state.clear();
            state.putAll(next);
            visible++;
            history.add(copy(state));
            if (await) {
                commit.awaitDurable();
                durable = visible;
            }
        }

        void drain(LiveLedger ledger) throws InterruptedException {
            ledger.spills().drain(ledger.log()::durableSequence);
            ledger.spills().awaitIdleForTests();
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted);
                ledger.state(MESSAGES).overwrites.forceSpillsForTests();
                // Phase A: no checkpoint; a counted bulk run, new keys, replayed resident overwrites
                var batch = begin(ledger);
                for (var i = 0; i < BULK; i++) {
                    var key = "a-%05d".formatted(i);
                    put(batch, key, value(key, 0, LENGTHS[i % LENGTHS.length]));
                }
                for (var s = 0; s < 4; s++) {
                    session(batch, "s" + s, value("s" + s, 0, 60));
                }
                end(ledger, batch, true);
                drain(ledger);
                for (var c = 1; c <= 6; c++) {
                    batch = begin(ledger);
                    for (var s = 0; s < 4; s++) {
                        session(batch, "s" + s, value("s" + s, c, 60));
                    }
                    for (var n = 0; n < 7; n++) {
                        var key = "n%d-%02d".formatted(c, n);
                        put(batch, key, value(key, c, LENGTHS[(n + c) % LENGTHS.length]));
                    }
                    if (largeEarly && c == 3) {
                        put(batch, "L1", value("L1", c, 700_000));
                    }
                    end(ledger, batch, c % 2 == 0);
                }
                drain(ledger);
                ledger.checkpoint();
                durable = visible;
                // Phase B: medium runs over a populated tree, overwrites, deletes, large values, a range removal
                for (var step = 0; step < 6; step++) {
                    batch = begin(ledger);
                    for (var i = 0; i < 100; i++) {
                        var key = "m%d-%05d".formatted(step, i);
                        put(batch, key, value(key, step, LENGTHS[(i + step) % LENGTHS.length]));
                    }
                    end(ledger, batch, true);
                    drain(ledger);
                    batch = begin(ledger);
                    for (var w = 0; w < 6; w++) {
                        var key = "a-%05d".formatted((w * 397 + step * 131) % BULK);
                        if (w == 5) {
                            batch.delete(MESSAGES, key);
                            next.get(MESSAGES.name()).remove(key);
                        } else {
                            put(batch, key, value(key, 100 + step, w == 0 ? 9_000 : 120));
                        }
                    }
                    for (var s = 0; s < 4; s++) {
                        session(batch, "s" + s, value("s" + s, 50 + step, s == 3 && step == 2 ? 61 : 60));
                    }
                    if (step == 1 || step == 4) {
                        put(batch, "L" + (2 + step), value("L" + step, step, 524_241 + step * 150_001));
                    }
                    if (step == 3) {
                        var from = "a-00100";
                        var to = "a-00300";
                        batch.deleteRange(MESSAGES, KeyRange.between(from, to));
                        next.get(MESSAGES.name()).subMap(from, to).clear();
                    }
                    end(ledger, batch, step % 2 == 1);
                    if (step == 2) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (step == 4) {
                        ledger.close();
                        durable = visible;
                        ledger = open(directory, encrypted);
                        ledger.state(MESSAGES).overwrites.forceSpillsForTests();
                    }
                }
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
    }

    private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
        var copy = new TreeMap<String, TreeMap<String, String>>();
        state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
        return copy;
    }

    private static Map<String, TreeMap<String, String>> contents(Ledger ledger) {
        var contents = new TreeMap<String, TreeMap<String, String>>();
        var messages = new TreeMap<String, String>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            messages.put(key, value);
            return true;
        });
        contents.put(MESSAGES.name(), messages);
        assertEquals(messages.size(), ledger.size(MESSAGES), "messages count");
        var sessions = new TreeMap<String, String>();
        ledger.forEach(SESSIONS, KeyRange.all(), (key, value) -> {
            sessions.put(key, new String(value, StandardCharsets.ISO_8859_1));
            return true;
        });
        contents.put(SESSIONS.name(), sessions);
        assertEquals(sessions.size(), ledger.size(SESSIONS), "sessions count");
        for (var entry : sessions.entrySet()) {
            assertEquals(entry.getValue(), new String(ledger.get(SESSIONS, entry.getKey()).orElseThrow(), StandardCharsets.ISO_8859_1));
        }
        return contents;
    }

    @ParameterizedTest(name = "encrypted {0}, large early {1}, part {2}")
    @CsvSource({"true,true,1", "false,false,1"})
    @DisplayName("recovers a state between the last durable and the last visible commit at sampled forces of bulk, medium and resident commits with large values of records, range removals, checkpoints and a reopen, and keeps it through a checkpoint and a reopen")
    void sampledForces(boolean encrypted, boolean largeEarly, int part) throws Exception {
        var dry = CrashFileSystem.create();
        var dryRun = new Workload(encrypted, largeEarly);
        dryRun.run(dry.getPath("/store"));
        assertEquals(dryRun.visible, dryRun.durable, "dry run failed");
        try (var ledger = open(dry.getPath("/store"), encrypted)) {
            assertEquals(dryRun.history.get(dryRun.visible), contents(ledger), "dry run reopened");
        }
        var forces = dry.forces();
        var random = new Random(forces * 31 + part);
        var points = new ArrayList<Long>();
        for (var i = 0; i < 30; i++) {
            points.add(part == 0 ? forces * i / 30 : random.nextLong(forces + 1));
        }
        points.add(forces);
        for (var force : points) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            var workload = new Workload(encrypted, largeEarly);
            workload.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            Map<String, TreeMap<String, String>> recovered;
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                recovered = contents(ledger);
                var match = -1;
                for (var commit = workload.durable; commit <= workload.visible; commit++) {
                    if (workload.history.get(commit).equals(recovered)) {
                        match = commit;
                        break;
                    }
                }
                if (match < 0) {
                    fail("force " + force + " of " + forces + ": recovered no state from " + workload.durable + " to "
                            + workload.visible + "; messages " + recovered.get(MESSAGES.name()).size());
                }
                ledger.checkpoint();
                assertEquals(recovered, contents(ledger), "after a checkpoint, force " + force);
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                assertEquals(recovered, contents(ledger), "after a reopen, force " + force);
            }
        }
    }
}
