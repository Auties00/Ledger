package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// A differential test of the rule that a transaction's scan is one read, made when it is called, against a model:
/// random stores in memory and in a directory, checkpointed, reopened beside a slowed loader and with the page cache
/// cleared, whose transactions write before their scans and run bounded, limited, nested, forward and reverse scans,
/// decoded and encoded, with visitors that put, delete, put again and remove ranges anywhere, some of them aborted.
/// The number of seeds is the `ledger.scanFuzzSeeds` system property, 12 by default.
class TransactionScanFuzzTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAG = Keyspace.paged("pag", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int SPACE = 900;

    static Stream<Arguments> seeds() {
        var count = Integer.getInteger("ledger.scanFuzzSeeds", 12);
        var list = new ArrayList<Arguments>();
        for (var seed = 1L; seed <= count; seed++) {
            if (seed % 4 == 0) {
                list.add(Arguments.of(seed, false));
            }
            list.add(Arguments.of(seed, true));
        }
        return list.stream();
    }

    private static final class Abort extends RuntimeException {
    }

    private static Ledger open(Path dir, boolean atOpen) {
        if (dir == null) {
            return Ledger.builder().memory().keyspace(RES).keyspace(PAG).build();
        }
        return Ledger.builder().directory(dir).encryption(Encryption.key(KEY)).keyspace(RES).keyspace(PAG)
                .loadResidentAtOpen(atOpen).build();
    }

    private static String key(int i) {
        return "k%04d".formatted(i);
    }

    private static String anyKey(Random random) {
        var base = key(random.nextInt(SPACE));
        return switch (random.nextInt(8)) {
            case 0 -> base + "z";
            case 1 -> "a" + base;
            default -> base;
        };
    }

    private static String value(Random random, String tag) {
        return tag + ":" + "x".repeat(random.nextInt(8) == 0 ? 900 + random.nextInt(900) : random.nextInt(120));
    }

    private static KeyRange<String> range(Random random) {
        KeyRange<String> range;
        switch (random.nextInt(5)) {
            case 0 -> range = KeyRange.all();
            case 1 -> range = KeyRange.atLeast(anyKey(random));
            case 2 -> range = KeyRange.below(anyKey(random));
            default -> {
                var a = anyKey(random);
                var b = anyKey(random);
                if (a.compareTo(b) > 0) {
                    var t = a;
                    a = b;
                    b = t;
                }
                range = KeyRange.between(a, b);
            }
        }
        return random.nextBoolean() ? range.reversed() : range;
    }

    private static List<String> expected(NavigableMap<String, String> model, KeyRange<String> range) {
        NavigableMap<String, String> sub = model;
        var from = range.from().orElse(null);
        var to = range.to().orElse(null);
        if (from != null && to != null) {
            if (from.compareTo(to) >= 0) {
                return List.of();
            }
            sub = model.subMap(from, true, to, false);
        } else if (from != null) {
            sub = model.tailMap(from, true);
        } else if (to != null) {
            sub = model.headMap(to, false);
        }
        var list = new ArrayList<String>();
        (range.descending() ? sub.descendingMap() : sub).forEach((k, v) -> list.add(k + "=" + v));
        return list;
    }

    private static String utf8(MemorySegment segment) {
        return new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /// One random write of the transaction, applied to `live` too.
    private static void write(Random random, Transaction tx, Keyspace<String, String> ks, TreeMap<String, String> live, String tag) {
        switch (random.nextInt(10)) {
            case 0, 1, 2, 3 -> {
                var k = anyKey(random);
                var v = value(random, tag);
                tx.put(ks, k, v);
                live.put(k, v);
            }
            case 4, 5, 6 -> {
                // Prefer a key that exists
                var k = anyKey(random);
                var ceiling = live.ceilingKey(k);
                if (ceiling != null && random.nextBoolean()) {
                    k = ceiling;
                }
                tx.delete(ks, k);
                live.remove(k);
            }
            case 7 -> {
                var a = random.nextInt(SPACE);
                var b = Math.min(SPACE, a + 1 + random.nextInt(40));
                tx.deleteRange(ks, KeyRange.between(key(a), key(b)));
                live.subMap(key(a), true, key(b), false).clear();
            }
            case 8 -> {
                // Overwrite the same key several times
                var k = anyKey(random);
                for (var i = 0; i < 3; i++) {
                    var v = value(random, tag + "#" + i);
                    tx.put(ks, k, v);
                    live.put(k, v);
                }
            }
            default -> {
                // Delete then put back, and the reverse
                var k = anyKey(random);
                tx.delete(ks, k);
                var v = value(random, tag + "!");
                tx.put(ks, k, v);
                live.put(k, v);
                if (random.nextBoolean()) {
                    tx.delete(ks, k);
                    live.remove(k);
                }
            }
        }
    }

    /// Runs one scan of `tx` whose visitor writes, and checks it against the state at its call.
    private static void scan(Random random, Transaction tx, Keyspace<String, String> ks, TreeMap<String, String> live, int depth,
                             String tag) {
        var range = range(random);
        var all = expected(live, range);
        var limit = random.nextInt(3) == 0 ? random.nextInt(60) + 1 : Integer.MAX_VALUE;
        var writeRate = random.nextInt(4) == 0 ? 0 : 1 + random.nextInt(12);
        var seen = new ArrayList<String>();
        var encoded = random.nextBoolean();
        var step = new int[1];
        BiPredicate<String, String> body = (k, v) -> {
            seen.add(k + "=" + v);
            var n = step[0]++;
            if (writeRate != 0 && n % writeRate == 0) {
                var writes = 1 + random.nextInt(3);
                for (var i = 0; i < writes; i++) {
                    if (random.nextInt(6) == 0) {
                        // The current key
                        if (random.nextBoolean()) {
                            var nv = value(random, tag + "cur");
                            tx.put(ks, k, nv);
                            live.put(k, nv);
                        } else {
                            tx.delete(ks, k);
                            live.remove(k);
                        }
                    } else {
                        write(random, tx, ks, live, tag + "v" + n);
                    }
                }
                var probe = random.nextBoolean() ? k : anyKey(random);
                assertEquals(live.get(probe), tx.get(ks, probe).orElse(null), "a point read inside the visitor reads the writes");
                if (random.nextInt(8) == 0) {
                    assertEquals(live.size(), tx.size(ks), "size inside the visitor counts the writes");
                }
                if (random.nextInt(8) == 0) {
                    var r = range(random);
                    var want = expected(live, r);
                    var got = tx.range(ks, r, 25).stream().map(e -> e.key() + "=" + e.value()).toList();
                    assertEquals(want.subList(0, Math.min(25, want.size())), got, "range inside the visitor reads the writes");
                }
                if (depth < 2 && random.nextInt(10) == 0) {
                    scan(random, tx, ks, live, depth + 1, tag + "n");
                }
            }
            return seen.size() < limit;
        };
        long visited;
        if (encoded) {
            visited = tx.forEachEncoded(ks, range, (k, v) -> {
                // Decoded after the visitor's writes: the segments stay valid for the call
                var keyBefore = utf8(k);
                var valueBefore = utf8(v);
                var go = body.test(keyBefore, valueBefore);
                assertEquals(keyBefore, utf8(k), "the key segment holds through the visitor's writes");
                assertEquals(valueBefore, utf8(v), "the value segment holds through the visitor's writes");
                return go;
            });
        } else {
            visited = tx.forEach(ks, range, body);
        }
        var want = all.subList(0, (int) Math.min(all.size(), limit));
        assertEquals(want, seen, () -> "scan " + range + " depth " + depth + " limit " + limit + " encoded " + encoded);
        assertEquals(want.size(), visited);
    }

    private static void check(View view, Keyspace<String, String> ks, NavigableMap<String, String> model, String what) {
        var want = expected(model, KeyRange.all());
        var got = new ArrayList<String>();
        view.forEach(ks, KeyRange.all(), (k, v) -> got.add(k + "=" + v));
        assertEquals(want, got, what);
        assertEquals(model.size(), view.size(ks), what + " size");
    }

    @ParameterizedTest(name = "seed {0} directory={1}")
    @MethodSource("seeds")
    @DisplayName("random transactions scan the state as of each scan's call")
    void fuzz(long seed, boolean directory, @TempDir Path temp) {
        var random = new Random(seed * 7919 + (directory ? 1 : 0));
        var dir = directory ? temp : null;
        var models = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
        models.put(RES, new TreeMap<>());
        models.put(PAG, new TreeMap<>());
        var ledger = open(dir, false);
        record Held(Snapshot snapshot, Map<Keyspace<String, String>, TreeMap<String, String>> state) {
        }
        var held = new ArrayList<Held>();
        try {
            var seedBatch = ledger.newBatch();
            for (var ks : List.of(RES, PAG)) {
                for (var i = 0; i < SPACE; i += 2) {
                    var v = value(random, "seed" + i);
                    seedBatch.put(ks, key(i), v);
                    models.get(ks).put(key(i), v);
                }
            }
            ledger.commit(seedBatch);
            for (var round = 0; round < 70; round++) {
                var ks = random.nextBoolean() ? RES : PAG;
                var model = models.get(ks);
                var action = random.nextInt(20);
                if (action < 3) {
                    var batch = ledger.newBatch();
                    var n = 1 + random.nextInt(60);
                    for (var i = 0; i < n; i++) {
                        var k = anyKey(random);
                        if (random.nextInt(3) == 0) {
                            batch.delete(ks, k);
                            model.remove(k);
                        } else {
                            var v = value(random, "b" + round);
                            batch.put(ks, k, v);
                            model.put(k, v);
                        }
                    }
                    ledger.commit(batch);
                } else if (action < 5) {
                    if (directory) {
                        ledger.checkpoint();
                    }
                } else if (action < 7) {
                    if (directory) {
                        for (var h : held) {
                            h.snapshot().close();
                        }
                        held.clear();
                        ledger.close();
                        ledger = open(dir, random.nextInt(3) == 0);
                        if (random.nextBoolean()) {
                            // Slow the loaders, so the transactions that follow run beside them
                            ((LiveLedger) ledger).hookResidentLoadsForTests(_ -> {
                                try {
                                    Thread.sleep(2);
                                } catch (InterruptedException exception) {
                                    Thread.currentThread().interrupt();
                                }
                            });
                        }
                    }
                } else if (action < 9) {
                    if (held.size() < 3) {
                        var copy = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
                        models.forEach((k, v) -> copy.put(k, new TreeMap<>(v)));
                        held.add(new Held(ledger.snapshot(), copy));
                    } else {
                        var h = held.remove(random.nextInt(held.size()));
                        check(h.snapshot(), RES, h.state().get(RES), "a snapshot taken before");
                        check(h.snapshot(), PAG, h.state().get(PAG), "a snapshot taken before");
                        h.snapshot().close();
                    }
                } else if (action < 10 && directory) {
                    ((LiveLedger) ledger).clearPageCacheForTests();
                } else {
                    var live = new TreeMap<>(model);
                    var abort = random.nextInt(5) == 0;
                    var tag = "t" + round;
                    try {
                        ledger.update(tx -> {
                            var pre = random.nextInt(3) == 0 ? 0 : random.nextInt(30);
                            for (var i = 0; i < pre; i++) {
                                write(random, tx, ks, live, tag + "pre");
                            }
                            var scans = 1 + random.nextInt(3);
                            for (var i = 0; i < scans; i++) {
                                scan(random, tx, ks, live, 0, tag + "s" + i);
                            }
                            check(tx, ks, live, "a scan after the visits");
                            if (abort) {
                                throw new Abort();
                            }
                        });
                        assertFalse(abort);
                        models.put(ks, live);
                    } catch (Abort expected) {
                        assertTrue(abort);
                    }
                }
                if (random.nextInt(4) == 0) {
                    check(ledger, ks, models.get(ks), "the store after round " + round);
                }
            }
            check(ledger, RES, models.get(RES), "the store at the end");
            check(ledger, PAG, models.get(PAG), "the store at the end");
            for (var h : held) {
                check(h.snapshot(), RES, h.state().get(RES), "a snapshot held to the end");
                check(h.snapshot(), PAG, h.state().get(PAG), "a snapshot held to the end");
                h.snapshot().close();
            }
            held.clear();
            if (directory) {
                ledger.close();
                ledger = open(dir, true);
                check(ledger, RES, models.get(RES), "the store reopened");
                check(ledger, PAG, models.get(PAG), "the store reopened");
            }
        } finally {
            for (var h : held) {
                h.snapshot().close();
            }
            ledger.close();
        }
    }
}
