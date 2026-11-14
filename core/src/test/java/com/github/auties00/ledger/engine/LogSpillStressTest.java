package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// A stress of the values that leave the heap for the log ([SpillQueue]) beside everything that reads or replaces
/// them at once: writers that overwrite, delete and remove ranges of groups of keys, transactions, commits of 1,280
/// writes that forget their values once published, snapshots scanned both ways, gets, ranges and encoded reads,
/// checkpoints every 15 ms, collections every 20 ms that empty the weak pools, and reopens that alternate a crash-like
/// close without a checkpoint and a clean one. The store runs with a budget of 0, key spaces that move values from their
/// first commit and a cold slab, so every value of 256 bytes or more of a durable commit moves to the log. Each value
/// names its key, generation and length and fills its bytes from them, and the 8 keys of a group always change
/// together at one generation, so a value read from a wrong place, a released array or a torn snapshot shows as a
/// corrupt value, a group of mixed generations or a group partly present.
class LogSpillStressTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<byte[], byte[]> MSG = Keyspace.paged("msg", KeyCodec.bytes(), ValueCodec.bytes());
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int GROUPS = 160;
    private static final int PER = 8;
    private static final int[] LENGTHS = {24, 200, 256, 300, 700, 1_500, 5_000, 20_000};

    private static LiveLedger open(Path dir, boolean encrypted) {
        var ledger = (LiveLedger) ((LedgerBuilder) Ledger.builder().directory(dir)
                .encryption(encrypted ? Encryption.key(KEY) : Encryption.none())
                .keyspace(MSG).keyspace(RES).commitWindow(Duration.ZERO))
                .segmentSize(256 << 10)
                .spillBudget(0)
                .writeThrough(WriteThrough.Mode.OFF)
                .build();
        ledger.valueSlab().forceColdForTests();
        ledger.state(MSG).overwrites.forceSpillsForTests();
        return ledger;
    }

    private static byte[] key(int group, int i) {
        return ByteBuffer.allocate(4).putInt(group * PER + i).array();
    }

    private static byte[] value(int k, long gen, int length) {
        var v = new byte[Math.max(length, 16)];
        var b = ByteBuffer.wrap(v);
        b.putLong(gen).putInt(k).putInt(v.length);
        for (var i = 16; i < v.length; i++) {
            v[i] = (byte) (gen * 131 + k * 31 + i * 7);
        }
        return v;
    }

    /// {@return the generation of `v`, after checking that it is a whole value of `key`}
    private static long check(byte[] key, byte[] v) {
        var k = ByteBuffer.wrap(key).getInt();
        assertTrue(v.length >= 16, "short value for " + k);
        var b = ByteBuffer.wrap(v);
        var gen = b.getLong();
        assertEquals(k, b.getInt(), "value of another key under " + k);
        assertEquals(v.length, b.getInt(), "length of value under " + k);
        for (var i = 16; i < v.length; i++) {
            if (v[i] != (byte) (gen * 131 + k * 31 + i * 7)) {
                fail("corrupt byte " + i + " of " + v.length + " under key " + k + " gen " + gen);
            }
        }
        return gen;
    }

    /// {@return the generation of each key of `entries`, after checking each value and that every group is absent or
    /// whole at one generation}
    private static TreeMap<Integer, Long> checkAtomic(List<? extends Map.Entry<byte[], byte[]>> entries, String what) {
        var gens = new TreeMap<Integer, Long>();
        for (var e : entries) {
            gens.put(ByteBuffer.wrap(e.getKey()).getInt(), check(e.getKey(), e.getValue()));
        }
        for (var g = 0; g < GROUPS; g++) {
            Long seen = null;
            var count = 0;
            for (var i = 0; i < PER; i++) {
                var gen = gens.get(g * PER + i);
                if (gen != null) {
                    count++;
                    if (seen == null) {
                        seen = gen;
                    } else {
                        assertEquals(seen, gen, what + ": group " + g + " mixes generations " + gens.subMap(g * PER, g * PER + PER));
                    }
                }
            }
            assertTrue(count == 0 || count == PER, what + ": group " + g + " partly present " + gens.subMap(g * PER, g * PER + PER));
        }
        return gens;
    }

    private static List<Map.Entry<byte[], byte[]>> scan(View view, boolean reversed) {
        var out = new ArrayList<Map.Entry<byte[], byte[]>>();
        view.forEach(MSG, reversed ? KeyRange.<byte[]>all().reversed() : KeyRange.all(), (k, v) -> {
            out.add(Map.entry(k, v));
            return true;
        });
        if (reversed) {
            return out.reversed();
        }
        return out;
    }

    private static void sameEntries(List<? extends Map.Entry<byte[], byte[]>> a, List<? extends Map.Entry<byte[], byte[]>> b, String what) {
        assertEquals(a.size(), b.size(), what + ": sizes");
        for (var i = 0; i < a.size(); i++) {
            assertArrayEquals(a.get(i).getKey(), b.get(i).getKey(), what + ": key " + i);
            assertArrayEquals(a.get(i).getValue(), b.get(i).getValue(), what + ": value " + i);
        }
    }

    @ParameterizedTest(name = "encrypted={0} cold={1}")
    @CsvSource({"true,false", "true,true", "false,true"})
    @Timeout(300)
    @DisplayName("snapshots stay isolated and values intact while values move to the log, pools empty, large commits forget their values and the store reopens")
    void stress(boolean encrypted, boolean cold, @TempDir Path dir) throws Exception {
        PageFile.forceColdNewForTests(cold);
        try {
            var gens = new AtomicLong(1);
            List<Map.Entry<byte[], byte[]>> carried = null;
            for (var phase = 0; phase < 4; phase++) {
                var ledger = open(dir, encrypted);
                if (carried != null) {
                    sameEntries(carried, scan(ledger, false), "after reopen " + phase);
                    checkAtomic(scan(ledger, true), "reversed after reopen " + phase);
                }
                var stop = new AtomicBoolean();
                var failure = new AtomicReference<Throwable>();
                var threads = new ArrayList<Thread>();
                for (var w = 0; w < 3; w++) {
                    var id = w + phase * 10;
                    threads.add(Thread.ofPlatform().start(() -> {
                        var random = new Random(id);
                        try {
                            while (!stop.get() && failure.get() == null) {
                                var batch = ledger.newBatch();
                                var op = random.nextInt(100);
                                if (op < 3) {
                                    // A commit of 1,280 writes, which forgets itself once published
                                    var gen = gens.getAndIncrement();
                                    for (var g = 0; g < GROUPS; g++) {
                                        for (var i = 0; i < PER; i++) {
                                            var k = g * PER + i;
                                            batch.put(MSG, key(g, i), value(k, gen, 300 + random.nextInt(400)));
                                        }
                                    }
                                } else if (op < 13) {
                                    var g = random.nextInt(GROUPS);
                                    for (var i = 0; i < PER; i++) {
                                        batch.delete(MSG, key(g, i));
                                    }
                                } else if (op < 16) {
                                    var g = random.nextInt(GROUPS);
                                    var gen = gens.getAndIncrement();
                                    ledger.update(tx -> {
                                        for (var i = 0; i < PER; i++) {
                                            tx.put(MSG, key(g, i), value(g * PER + i, gen, LENGTHS[random.nextInt(LENGTHS.length)]));
                                        }
                                    });
                                    continue;
                                } else if (op < 18) {
                                    var g = random.nextInt(GROUPS - 3);
                                    var n = 1 + random.nextInt(3);
                                    batch.deleteRange(MSG, KeyRange.between(key(g, 0), key(g + n, 0)));
                                } else {
                                    var groups = 1 + random.nextInt(6);
                                    for (var j = 0; j < groups; j++) {
                                        var g = random.nextInt(GROUPS);
                                        var gen = gens.getAndIncrement();
                                        for (var i = 0; i < PER; i++) {
                                            var k = g * PER + i;
                                            batch.put(MSG, key(g, i), value(k, gen, LENGTHS[random.nextInt(LENGTHS.length)]));
                                        }
                                    }
                                    batch.put(RES, "r" + random.nextInt(50), "x" + id);
                                }
                                ledger.commit(batch);
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }));
                }
                for (var r = 0; r < 3; r++) {
                    var id = r;
                    threads.add(Thread.ofPlatform().start(() -> {
                        var random = new Random(100 + id);
                        try {
                            while (!stop.get() && failure.get() == null) {
                                switch (random.nextInt(4)) {
                                    case 0 -> {
                                        try (var snapshot = ledger.snapshot()) {
                                            var first = scan(snapshot, false);
                                            var model = checkAtomic(first, "snapshot forward");
                                            Thread.sleep(random.nextInt(5));
                                            var second = scan(snapshot, true);
                                            sameEntries(first, second, "snapshot reversed");
                                            for (var n = 0; n < 20; n++) {
                                                var g = random.nextInt(GROUPS);
                                                var i = random.nextInt(PER);
                                                var got = snapshot.get(MSG, key(g, i));
                                                var expected = model.get(g * PER + i);
                                                if (expected == null) {
                                                    assertTrue(got.isEmpty(), "snapshot get found a key its scan did not");
                                                } else {
                                                    assertEquals(expected, check(key(g, i), got.orElseThrow()), "snapshot get");
                                                }
                                            }
                                        }
                                    }
                                    case 1 -> {
                                        var g = random.nextInt(GROUPS - 4);
                                        var entries = ledger.range(MSG, KeyRange.between(key(g, 0), key(g + 4, 0)), Integer.MAX_VALUE);
                                        var list = new ArrayList<Map.Entry<byte[], byte[]>>();
                                        for (var e : entries) {
                                            list.add(Map.entry(e.key(), e.value()));
                                        }
                                        checkAtomic(list, "range");
                                    }
                                    case 2 -> {
                                        var g = random.nextInt(GROUPS);
                                        var i = random.nextInt(PER);
                                        ledger.get(MSG, key(g, i)).ifPresent(v -> check(key(g, i), v));
                                        ledger.readEncoded(MSG, key(g, i), s -> s.toArray(ValueLayout.JAVA_BYTE))
                                                .ifPresent(v -> check(key(g, i), v));
                                    }
                                    default -> {
                                        var g = random.nextInt(GROUPS);
                                        var entries = ledger.range(MSG, KeyRange.<byte[]>atLeast(key(g, 0)).reversed(), 40);
                                        for (var e : entries) {
                                            check(e.key(), e.value());
                                        }
                                    }
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }));
                }
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get() && failure.get() == null) {
                            Thread.sleep(15);
                            ledger.checkpoint();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
                threads.add(Thread.ofPlatform().start(() -> {
                    try {
                        while (!stop.get() && failure.get() == null) {
                            Thread.sleep(20);
                            System.gc();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                }));
                var deadline = System.nanoTime() + Duration.ofSeconds(4).toNanos();
                while (System.nanoTime() < deadline && failure.get() == null) {
                    Thread.sleep(50);
                }
                stop.set(true);
                for (var t : threads) {
                    t.join();
                }
                if (failure.get() != null) {
                    ledger.close();
                    throw new AssertionError("phase " + phase, failure.get());
                }
                ledger.awaitDurable();
                carried = scan(ledger, false);
                checkAtomic(carried, "end of phase " + phase);
                assertTrue(ledger.spills().loggedValuesForTests() > 0, "values moved to the log in phase " + phase);
                if (phase % 2 == 0) {
                    ledger.closeWithoutCheckpoint();
                } else {
                    ledger.close();
                }
            }
            try (var ledger = open(dir, encrypted)) {
                sameEntries(carried, scan(ledger, false), "final reopen");
            }
        } finally {
            PageFile.forceColdNewForTests(false);
        }
    }
}
