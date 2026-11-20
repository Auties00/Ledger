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

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Power loss at sampled forces of a workload past [LiveLedger#BULK_RUN_WRITES]: bulk commits appending to one run,
/// their values moved to the log by the spill, beside large values, small overwrites and removals of the run's keys,
/// a checkpoint and a reopen. The store recovers a state between its last durable and its last visible commit, keeps
/// it through a checkpoint, and opens to it again.
class BulkCommitCrashTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int BULK = LiveLedger.BULK_RUN_WRITES + 100;
    private static final int[] LENGTHS = {40, 100, 300, 1_500, 0, 260};

    private static LiveLedger open(Path directory, boolean encrypted) {
        return (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(CHATS).commitWindow(Duration.ZERO)
                .largeValues(8_192))
                .segmentSize(256 << 10)
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
        final List<Map<String, TreeMap<String, String>>> history = new ArrayList<>();
        final Map<String, TreeMap<String, String>> state = new TreeMap<>();
        int visible;
        int durable;
        int appended;

        Workload(boolean encrypted) {
            this.encrypted = encrypted;
            state.put(MESSAGES.name(), new TreeMap<>());
            state.put(CHATS.name(), new TreeMap<>());
            history.add(copy(state));
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            try {
                ledger = open(directory, encrypted);
                ledger.state(MESSAGES).overwrites.forceSpillsForTests();
                for (var step = 0; step < 9; step++) {
                    // A bulk commit of new keys: the first makes the run, the next ones append to it until a cut
                    var batch = ledger.newBatch();
                    var next = copy(state);
                    for (var i = 0; i < BULK; i++) {
                        var key = "b%02d-%05d".formatted(step, i);
                        var value = value(key, step, LENGTHS[(i + step) % LENGTHS.length]);
                        batch.put(MESSAGES, key, value);
                        next.get(MESSAGES.name()).put(key, value);
                    }
                    var commit = ledger.commit(batch);
                    commit(next);
                    var run = ledger.state(MESSAGES).run;
                    if (run != null && run.committed() && run.size() > BULK) {
                        appended++;
                    }
                    // Small commits: overwrites and removals of the run's keys, large values, a resident write
                    for (var c = 0; c < 3; c++) {
                        batch = ledger.newBatch();
                        next = copy(state);
                        for (var w = 0; w < 5; w++) {
                            var key = "b%02d-%05d".formatted(Math.max(0, step - c), (w * 397 + c * 131 + step) % BULK);
                            if (w == 4) {
                                batch.delete(MESSAGES, key);
                                next.get(MESSAGES.name()).remove(key);
                            } else {
                                var value = value(key, 100 + step * 10 + c, w == 0 ? 9_000 + step * 100 : 120);
                                batch.put(MESSAGES, key, value);
                                next.get(MESSAGES.name()).put(key, value);
                            }
                        }
                        var chatKey = "c%02d".formatted(c);
                        var chatValue = value(chatKey, step, 30 + (step % 3) * 20);
                        batch.put(CHATS, chatKey, chatValue);
                        next.get(CHATS.name()).put(chatKey, chatValue);
                        commit = ledger.commit(batch);
                        commit(next);
                    }
                    commit.awaitDurable();
                    durable = visible;
                    // The values of the commits leave the heap for the log now
                    ledger.spills().drain(ledger.log()::durableSequence);
                    ledger.spills().awaitIdleForTests();
                    if (step == 4) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (step == 6) {
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

        private void commit(Map<String, TreeMap<String, String>> next) {
            state.putAll(next);
            visible++;
            history.add(copy(state));
        }
    }

    private static Map<String, TreeMap<String, String>> copy(Map<String, TreeMap<String, String>> state) {
        var copy = new TreeMap<String, TreeMap<String, String>>();
        state.forEach((name, entries) -> copy.put(name, new TreeMap<>(entries)));
        return copy;
    }

    private static Map<String, TreeMap<String, String>> contents(Ledger ledger) {
        var contents = new TreeMap<String, TreeMap<String, String>>();
        for (var keyspace : List.of(MESSAGES, CHATS)) {
            var entries = new TreeMap<String, String>();
            ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
                entries.put(key, value);
                return true;
            });
            contents.put(keyspace.name(), entries);
            assertEquals(entries.size(), ledger.size(keyspace), keyspace.name() + " count");
        }
        return contents;
    }

    @ParameterizedTest(name = "encrypted {0}, part {1}")
    @CsvSource({"true,0", "true,1", "false,0"})
    @DisplayName("recovers a state between the last durable and the last visible commit at sampled forces of a workload of appending bulk commits, spills and large values")
    void sampledForces(boolean encrypted, int part) throws Exception {
        var dry = CrashFileSystem.create();
        var dryRun = new Workload(encrypted);
        dryRun.run(dry.getPath("/store"));
        assertTrue(dryRun.appended > 0, "no bulk commit appended");
        var forces = dry.forces();
        var random = new Random(forces * 31 + part);
        var points = new ArrayList<Long>();
        for (var i = 0; i < 25; i++) {
            points.add(part == 0 ? forces * i / 25 : random.nextLong(forces + 1));
        }
        points.add(forces);
        for (var force : points) {
            var disk = CrashFileSystem.create();
            disk.stopAtForce(force);
            var workload = new Workload(encrypted);
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
                assertEquals(recovered, contents(ledger), "after a checkpoint");
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted)) {
                assertEquals(recovered, contents(ledger), "after a reopen");
            }
        }
    }
}
