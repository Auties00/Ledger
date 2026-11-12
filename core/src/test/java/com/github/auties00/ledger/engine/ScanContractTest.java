package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/// What a transaction's scan passes when its visitor writes the scanned key space, in every layout a key space can be
/// in: the entries and values the transaction saw when it called the scan, whatever the visitor writes, while point
/// reads and size inside the visitor, and later scans, see the writes.
class ScanContractTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> RES = Keyspace.resident("res", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAG = Keyspace.paged("pag", KeyCodec.utf8(), ValueCodec.utf8());
    private static final int N = 1200;

    enum Layout {MEM_RES, MEM_PAG, DIR_RES_LOADED, DIR_RES_PAUSED, DIR_PAG_TREE, DIR_PAG_MEM, DIR_PAG_MIXED}

    enum Kind {OVERWRITE_AHEAD, DELETE_AHEAD, INSERT_AHEAD, INSERT_BEHIND, RANGE_AHEAD, OVERWRITE_CURRENT, DELETE_CURRENT, REKEY_AHEAD}

    static Stream<Arguments> cases() {
        var list = new ArrayList<Arguments>();
        for (var layout : Layout.values()) {
            for (var kind : Kind.values()) {
                for (var descending : new boolean[]{false, true}) {
                    list.add(Arguments.of(layout, kind, descending));
                }
            }
        }
        return list.stream();
    }

    private static Ledger openDir(Path dir) {
        return Ledger.builder().directory(dir).encryption(Encryption.key(KEY)).keyspace(RES).keyspace(PAG)
                .loadResidentAtOpen(false).build();
    }

    private static String key(int i) {
        return "k%05d".formatted(i);
    }

    private static String value(int i) {
        return "v" + i + "-" + "x".repeat(300);
    }

    @ParameterizedTest(name = "{0} {1} descending={2}")
    @MethodSource("cases")
    @DisplayName("a transaction's scan reads the state as of its call, whatever its visitor writes")
    void snapshotAtCall(Layout layout, Kind kind, boolean descending, @TempDir Path dir) throws Exception {
        var ks = layout.name().contains("RES") ? RES : PAG;
        var model = new TreeMap<String, String>();
        for (var i = 0; i < N; i += 2) {
            model.put(key(i), value(i));
        }
        Ledger ledger;
        var gate = new CountDownLatch(1);
        Thread opener = null;
        if (layout == Layout.MEM_RES || layout == Layout.MEM_PAG) {
            ledger = Ledger.builder().memory().keyspace(RES).keyspace(PAG).build();
            var batch = ledger.newBatch();
            model.forEach((k, v) -> batch.put(ks, k, v));
            ledger.commit(batch);
        } else {
            try (var seed = openDir(dir)) {
                var batch = seed.newBatch();
                model.forEach((k, v) -> batch.put(ks, k, v));
                seed.commit(batch);
                seed.checkpoint();
            }
            ledger = openDir(dir);
            var live = (LiveLedger) ledger;
            switch (layout) {
                case DIR_RES_LOADED -> {
                    ledger.range(ks, KeyRange.all(), Integer.MAX_VALUE);
                    while (!live.residentLoadedForTests(ks)) {
                        Thread.sleep(5);
                    }
                }
                case DIR_RES_PAUSED -> {
                    live.hookResidentLoadsForTests(step -> {
                        if (step == 1) {
                            try {
                                gate.await();
                            } catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    });
                    ledger.get(ks, key(0));
                    Thread.sleep(100);
                    opener = Thread.ofPlatform().start(() -> {
                        try {
                            Thread.sleep(300);
                        } catch (InterruptedException e) {
                            return;
                        }
                        gate.countDown();
                    });
                }
                case DIR_PAG_MEM -> {
                    // Every key also in memory, as written since the checkpoint
                    var batch = ledger.newBatch();
                    model.forEach((k, v) -> batch.put(ks, k, v));
                    ledger.commit(batch);
                }
                case DIR_PAG_MIXED -> {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < N; i += 6) {
                        batch.put(ks, key(i), "m" + i);
                        model.put(key(i), "m" + i);
                    }
                    ledger.commit(batch);
                }
                default -> {
                }
            }
        }
        try {
            var before = new ArrayList<>(descending ? model.descendingMap().entrySet() : model.entrySet())
                    .stream().map(e -> e.getKey() + "=" + e.getValue()).toList();
            var after = new TreeMap<>(model);
            var seen = new ArrayList<String>();
            var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
            var step = descending ? -1 : 1;
            var pointReadsOk = new boolean[]{true};
            ledger.update(tx -> {
                var n = new int[1];
                tx.forEach(ks, range, (k, v) -> {
                    seen.add(k + "=" + v);
                    if (n[0]++ % 7 != 0 || seen.size() > N) {
                        return seen.size() <= 2 * N;
                    }
                    var p = Integer.parseInt(k.replaceAll("[^0-9]", ""));
                    switch (kind) {
                        case OVERWRITE_AHEAD -> {
                            var t = p + 20 * step;
                            if (t >= 0 && t < N) {
                                tx.put(ks, key(t), "new" + p);
                                after.put(key(t), "new" + p);
                            }
                        }
                        case DELETE_AHEAD -> {
                            var t = p + 20 * step;
                            if (t >= 0 && t < N) {
                                tx.delete(ks, key(t));
                                after.remove(key(t));
                            }
                        }
                        case INSERT_AHEAD -> {
                            var t = p + 21 * step;
                            if (t >= 0 && t < N) {
                                tx.put(ks, key(t), "ins" + p);
                                after.put(key(t), "ins" + p);
                            }
                        }
                        case INSERT_BEHIND -> {
                            var t = p - 21 * step;
                            if (t >= 0 && t < N) {
                                tx.put(ks, key(t), "beh" + p);
                                after.put(key(t), "beh" + p);
                            }
                        }
                        case RANGE_AHEAD -> {
                            var a = p + 10 * step;
                            var b = p + 30 * step;
                            var lo = Math.max(0, Math.min(a, b));
                            var hi = Math.min(N, Math.max(a, b));
                            if (lo < hi) {
                                tx.deleteRange(ks, KeyRange.between(key(lo), key(hi)));
                                after.subMap(key(lo), key(hi)).clear();
                            }
                        }
                        case OVERWRITE_CURRENT -> {
                            tx.put(ks, k, "cur" + p);
                            after.put(k, "cur" + p);
                        }
                        case DELETE_CURRENT -> {
                            tx.delete(ks, k);
                            after.remove(k);
                        }
                        case REKEY_AHEAD -> {
                            // A migration that renames each key to one that sorts ahead of it in the scan's order
                            if (!k.endsWith("z")) {
                                tx.delete(ks, k);
                                after.remove(k);
                                var renamed = descending ? "a" + k : k + "z";
                                tx.put(ks, renamed, v);
                                after.put(renamed, v);
                            }
                        }
                    }
                    // Point reads inside the visitor see the transaction's writes
                    for (var probe : List.of(key(Math.max(0, Math.min(N - 1, p + 20 * step))), k)) {
                        var got = tx.get(ks, probe).orElse(null);
                        if (!Objects.equals(got, after.get(probe))) {
                            pointReadsOk[0] = false;
                        }
                    }
                    return true;
                });
                var postScan = tx.range(ks, KeyRange.all(), Integer.MAX_VALUE).stream()
                        .map(e -> e.key() + "=" + e.value()).toList();
                assertEquals(after.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList(), postScan,
                        "a scan after the visit reads the writes");
            });
            assertTrue(pointReadsOk[0], "point reads inside the visitor see the transaction's writes");
            var diff = diff(before, seen);
            assertTrue(diff.isEmpty(), () -> layout + " " + kind + " descending=" + descending + ": scan diverged from the state at its call: " + diff);
            var committed = ledger.range(ks, KeyRange.all(), Integer.MAX_VALUE).stream()
                    .map(e -> e.key() + "=" + e.value()).toList();
            assertEquals(after.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList(), committed);
            if (opener != null) {
                opener.join();
            }
        } finally {
            gate.countDown();
            ledger.close();
        }
    }

    @ParameterizedTest(name = "directory={0} {1}")
    @CsvSource({"false,res", "false,pag", "true,res", "true,pag"})
    @DisplayName("the count a scan returns is of the entries it passed, while size inside the visitor counts the visitor's writes")
    void countAndSize(boolean directory, String which, @TempDir Path dir) {
        var ks = which.equals("res") ? RES : PAG;
        try (var ledger = directory ? openDir(dir) : Ledger.builder().memory().keyspace(RES).keyspace(PAG).build()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 20; i += 2) {
                batch.put(ks, key(i), value(i));
            }
            ledger.commit(batch);
            if (directory) {
                ledger.checkpoint();
            }
            ledger.update(tx -> {
                // Before the scan, so the scan passes this entry and counts it
                tx.put(ks, key(1), "before");
                var sizes = new ArrayList<Long>();
                var count = tx.forEach(ks, KeyRange.all(), (k, v) -> {
                    if (k.equals(key(0))) {
                        tx.delete(ks, key(2));
                        tx.delete(ks, key(4));
                        tx.put(ks, key(3), "ins");
                    }
                    sizes.add(tx.size(ks));
                    return true;
                });
                assertEquals(11, count);
                assertEquals(Collections.nCopies(11, 10L), sizes);
                assertEquals(10, tx.size(ks));
                assertEquals(10, tx.forEach(ks, KeyRange.all(), (k, v) -> true));
            });
            assertEquals(10, ledger.size(ks));
        }
    }

    @ParameterizedTest(name = "commits before the transaction: {0}")
    @CsvSource({"0", "1"})
    @DisplayName("a scan after the last write a commit can hold reads the transaction's writes")
    void lastStamp(int commitsBefore) {
        try (var ledger = Ledger.builder().memory().keyspace(RES).keyspace(PAG).build()) {
            // The commits before set the parity of the transaction's sequence, which the stamp after the last write
            // carried into
            for (var i = 0; i < commitsBefore; i++) {
                ledger.commit(ledger.newBatch().put(PAG, "other" + i, "x"));
            }
            var seen = new ArrayList<String>();
            var sequence = ledger.update(tx -> {
                tx.put(PAG, "mine", "written");
                try {
                    var applierField = LiveTransaction.class.getDeclaredField("applier");
                    applierField.setAccessible(true);
                    var applier = applierField.get(tx);
                    var op = applier.getClass().getDeclaredField("op");
                    op.setAccessible(true);
                    // As after the last of the writes a commit holds
                    op.setInt(applier, Reads.MAX_OPS + 1);
                } catch (ReflectiveOperationException exception) {
                    throw new IllegalStateException(exception);
                }
                assertEquals(Optional.of("written"), tx.get(PAG, "mine"));
                tx.forEach(PAG, KeyRange.all(), (k, v) -> seen.add(k + "=" + v));
            }).sequence();
            assertTrue(seen.contains("mine=written"), "sequence " + sequence + " saw " + seen);
        }
    }

    private static String diff(List<String> expected, List<String> actual) {
        var e = new LinkedHashSet<>(expected);
        var a = new LinkedHashSet<>(actual);
        var missing = new ArrayList<String>();
        var extra = new ArrayList<String>();
        for (var x : e) {
            if (!a.contains(x)) {
                missing.add(x.length() > 40 ? x.substring(0, 40) : x);
            }
        }
        for (var x : a) {
            if (!e.contains(x)) {
                extra.add(x.length() > 40 ? x.substring(0, 40) : x);
            }
        }
        if (missing.isEmpty() && extra.isEmpty() && !expected.equals(actual)) {
            return "same entries, different order or duplicates (" + actual.size() + " vs " + expected.size() + ")";
        }
        if (missing.isEmpty() && extra.isEmpty()) {
            return "";
        }
        return "missing " + missing.size() + " (first " + missing.stream().limit(2).toList() + "), extra " + extra.size()
                + " (first " + extra.stream().limit(2).toList() + ")";
    }
}
