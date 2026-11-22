package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Power loss at sampled forces of a store whose medium commits append to uncounted runs beside range removals, a
/// checkpoint and a reopen: the recovered store holds a state between the last durable and the last visible commit,
/// and takes medium commits, a checkpoint and a reopen after it.
class MediumCommitRunCrashTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MESSAGES = Keyspace.paged("messages", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> TEXTS = Keyspace.paged("texts", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int POPULATED = 3_000;
    private static final int[] LENGTHS = {0, 40, SpillQueue.LOG_VALUE_MIN, 200, 700, 1_500};

    private static LiveLedger open(Path directory, boolean encrypted, long budget) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MESSAGES).keyspace(TEXTS).keyspace(CHATS).commitWindow(Duration.ZERO))
                .segmentSize(1L << 20)
                .spillBudget(budget)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        return ledger;
    }

    private static byte[] key(int message) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(message).array();
    }

    private static byte[] value(int message, int generation) {
        var length = LENGTHS[Math.floorMod(message * 7 + generation, LENGTHS.length)];
        var value = new byte[length];
        for (var i = 0; i < length; i++) {
            value[i] = (byte) (message * 31 + generation * 17 + i);
        }
        return value;
    }

    private static void put(WriteBatch batch, Keyspace<byte[], byte[]> keyspace, TreeMap<Integer, byte[]> expected,
                            int message, int generation) {
        var value = value(message, generation);
        batch.put(keyspace, key(message), value);
        expected.put(message, value);
    }

    // Idle commits of 100 puts appending to an uncounted run, half new keys and half keys of the tree
    private static void medium(LiveLedger ledger, TreeMap<Integer, byte[]> expected, int commit, Random random) {
        var batch = ledger.newBatch();
        for (var i = 0; i < 60; i++) {
            put(batch, MESSAGES, expected, POPULATED + commit * 100 + i, commit);
        }
        for (var i = 0; i < 40; i++) {
            put(batch, MESSAGES, expected, random.nextInt(POPULATED), commit + 1);
        }
        var removed = random.nextInt(POPULATED + commit * 100 + 60);
        batch.delete(MESSAGES, key(removed));
        expected.remove(removed);
        ledger.commit(batch);
    }

    private static final class Workload {
        final boolean encrypted;
        final List<TreeMap<Integer, byte[]>> history = new ArrayList<>();
        TreeMap<Integer, byte[]> state = new TreeMap<>();
        int visible;
        int durable;

        Workload(boolean encrypted) {
            this.encrypted = encrypted;
            history.add(new TreeMap<>(state));
        }

        private void committed(TreeMap<Integer, byte[]> next) {
            state = next;
            visible++;
            history.add(new TreeMap<>(state));
        }

        void run(Path directory) {
            LiveLedger ledger = null;
            var random = new Random(21);
            try {
                ledger = open(directory, encrypted, 0);
                ledger.state(MESSAGES).overwrites.forceSpillsForTests();
                var next = new TreeMap<>(state);
                var batch = ledger.newBatch();
                for (var message = 0; message < 1_500; message++) {
                    put(batch, MESSAGES, next, message, 0);
                }
                var first = ledger.commit(batch);
                committed(next);
                first.awaitDurable();
                durable = visible;
                ledger.checkpoint();
                for (var commit = 0; commit < 24; commit++) {
                    next = new TreeMap<>(state);
                    batch = ledger.newBatch();
                    for (var i = 0; i < 70; i++) {
                        put(batch, MESSAGES, next, 1_500 + commit * 70 + i, commit);
                    }
                    for (var i = 0; i < 20; i++) {
                        put(batch, MESSAGES, next, random.nextInt(1_500), commit + 1);
                    }
                    var removed = random.nextInt(1_500 + commit * 70);
                    batch.delete(MESSAGES, key(removed));
                    next.remove(removed);
                    var commitTicket = ledger.commit(batch);
                    committed(next);
                    if (commit % 6 == 5) {
                        next = new TreeMap<>(state);
                        var range = ledger.newBatch();
                        var from = random.nextInt(1_500 + commit * 70);
                        range.deleteRange(MESSAGES, KeyRange.between(key(from), key(from + 90)));
                        next.subMap(from, from + 90).clear();
                        commitTicket = ledger.commit(range);
                        committed(next);
                    }
                    if (commit % 4 == 3) {
                        commitTicket.awaitDurable();
                        durable = visible;
                        ledger.spills().drain(ledger.log()::durableSequence);
                        ledger.spills().awaitIdleForTests();
                    }
                    if (commit == 11) {
                        ledger.checkpoint();
                        durable = visible;
                    } else if (commit == 17) {
                        ledger.close();
                        durable = visible;
                        ledger = open(directory, encrypted, 0);
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

    private static TreeMap<Integer, byte[]> contents(Ledger ledger) {
        var entries = new TreeMap<Integer, byte[]>();
        ledger.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
            entries.put(ByteBuffer.wrap(key).getInt(), value);
            return true;
        });
        assertEquals(entries.size(), ledger.size(MESSAGES), "messages count");
        return entries;
    }

    private static boolean same(TreeMap<Integer, byte[]> a, TreeMap<Integer, byte[]> b) {
        if (!a.keySet().equals(b.keySet())) {
            return false;
        }
        for (var entry : a.entrySet()) {
            if (!java.util.Arrays.equals(entry.getValue(), b.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    @ParameterizedTest(name = "encrypted {0}, part {1}")
    @CsvSource({"true,0", "false,1"})
    @DisplayName("recovers a state between the last durable and the last visible commit at sampled forces of medium commits appending to uncounted runs beside range removals, checkpoints and a reopen")
    void sampledForces(boolean encrypted, int part) throws Exception {
        var dry = CrashFileSystem.create();
        var dryRun = new Workload(encrypted);
        dryRun.run(dry.getPath("/store"));
        assertEquals(dryRun.visible, dryRun.durable);
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
            var workload = new Workload(encrypted);
            workload.run(disk.getPath("/store"));
            var survivor = disk.afterPowerLoss(new Random(random.nextLong()));
            TreeMap<Integer, byte[]> recovered;
            try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
                recovered = contents(ledger);
                var match = -1;
                for (var commit = workload.durable; commit <= workload.visible; commit++) {
                    if (same(workload.history.get(commit), recovered)) {
                        match = commit;
                        break;
                    }
                }
                if (match < 0) {
                    fail("force " + force + " of " + forces + ": recovered no state from " + workload.durable + " to "
                            + workload.visible + "; messages " + recovered.size());
                }
                // Medium commits over the recovered store, then a checkpoint
                var expected = new TreeMap<>(recovered);
                for (var commit = 0; commit < 4; commit++) {
                    medium(ledger, expected, 40 + commit, new Random(commit));
                }
                assertTrue(same(expected, contents(ledger)), "after medium commits over the recovered store");
                ledger.checkpoint();
                assertTrue(same(expected, contents(ledger)), "after a checkpoint");
                recovered = expected;
            }
            try (var ledger = open(survivor.getPath("/store"), encrypted, 0)) {
                assertTrue(same(recovered, contents(ledger)), "after a reopen");
            }
        }
    }
}
