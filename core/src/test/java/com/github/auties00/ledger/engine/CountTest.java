package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/// [View#count] against a model and against a scan of the same range: random puts, removals, blind writes, range
/// removals, checkpoints, reopens and open snapshots, in resident and paged key spaces of stores in memory and in a
/// directory.
class CountTest {
    private static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.utf8());

    static Stream<Arguments> cases() {
        var cases = new ArrayList<Arguments>();
        for (var seed = 0; seed < 6; seed++) {
            for (var memory : List.of(true, false)) {
                for (var keyspace : List.of(PAGED, RESIDENT)) {
                    cases.add(Arguments.of(seed, memory, keyspace));
                }
            }
        }
        return cases.stream();
    }

    private static Ledger open(boolean memory, Path directory) {
        var options = memory ? Ledger.builder().memory() : ((LedgerBuilder) Ledger.builder().directory(directory)
                .encryption(Encryption.none())).segmentSize(16_384).checkpointEvery(1 << 20);
        return options.keyspace(PAGED).keyspace(RESIDENT).build();
    }

    private static String key(Random random) {
        return "k%03d".formatted(random.nextInt(300));
    }

    private static KeyRange<String> range(Random random) {
        return switch (random.nextInt(6)) {
            case 0 -> KeyRange.all();
            case 1 -> KeyRange.atLeast(key(random));
            case 2 -> KeyRange.below(key(random));
            case 3 -> KeyRange.<String>all().reversed();
            default -> random.nextBoolean() ? KeyRange.between(key(random), key(random)) : KeyRange.between(key(random), key(random)).reversed();
        };
    }

    private static long expected(TreeMap<String, String> model, KeyRange<String> range) {
        var from = range.from().orElse(null);
        var to = range.to().orElse(null);
        if (from != null && to != null && from.compareTo(to) >= 0) {
            return 0;
        }
        var view = from == null ? model : model.tailMap(from, true);
        if (to != null) {
            view = view.headMap(to, false);
        }
        return view.size();
    }

    private static void check(View view, Keyspace<String, String> keyspace, TreeMap<String, String> model, Random random) {
        for (var i = 0; i < 8; i++) {
            var range = range(random);
            var counted = view.count(keyspace, range);
            assertEquals(expected(model, range), counted, "count of " + range);
            assertEquals(view.forEach(keyspace, range, (_, _) -> true), counted, "scan of " + range);
        }
    }

    @ParameterizedTest(name = "seed {0}, memory: {1}, {2}")
    @MethodSource("cases")
    @DisplayName("counts what a scan of the range finds and the model holds, at the latest state and in snapshots")
    void model(int seed, boolean memory, Keyspace<String, String> keyspace, @TempDir Path directory) {
        var random = new Random(seed);
        var model = new TreeMap<String, String>();
        var ledger = open(memory, directory);
        try {
            for (var step = 0; step < 400; step++) {
                var batch = ledger.newBatch();
                var writes = 1 + random.nextInt(20);
                for (var i = 0; i < writes; i++) {
                    var choice = random.nextInt(30);
                    var key = key(random);
                    if (choice == 0) {
                        var to = key(random);
                        batch.deleteRange(keyspace, KeyRange.between(key, to));
                        if (key.compareTo(to) < 0) {
                            model.subMap(key, to).clear();
                        }
                    } else if (choice < 8) {
                        batch.delete(keyspace, key);
                        model.remove(key);
                    } else {
                        batch.put(keyspace, key, "v" + step);
                        model.put(key, "v" + step);
                    }
                }
                ledger.commit(batch);
                if (step % 37 == 0) {
                    try (var snapshot = ledger.snapshot()) {
                        var frozen = new TreeMap<>(model);
                        ledger.commit(ledger.newBatch().put(keyspace, key(random), "later").delete(keyspace, key(random)));
                        check(snapshot, keyspace, frozen, new Random(step));
                        // The latest state moved on
                        model.clear();
                        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> model.put(key, value) == null);
                    }
                }
                if (step % 10 == 0) {
                    check(ledger, keyspace, model, random);
                }
                if (!memory && step % 97 == 96) {
                    if (random.nextBoolean()) {
                        ledger.checkpoint();
                    } else {
                        ledger.close();
                        ledger = open(memory, directory);
                    }
                }
            }
            check(ledger, keyspace, model, random);
        } finally {
            ledger.close();
        }
    }

    @Test
    @DisplayName("a transaction counts its own writes, and an empty or inverted range counts nothing")
    void transactions() {
        try (var ledger = open(true, null)) {
            ledger.commit(ledger.newBatch().put(PAGED, "a", "1").put(PAGED, "b", "2").put(RESIDENT, "a", "1"));
            ledger.update(transaction -> {
                transaction.put(PAGED, "c", "3");
                transaction.delete(PAGED, "a");
                transaction.put(RESIDENT, "b", "2");
                assertEquals(2, transaction.count(PAGED, KeyRange.all()));
                assertEquals(1, transaction.count(PAGED, KeyRange.atLeast("c")));
                assertEquals(2, transaction.count(RESIDENT, KeyRange.all()));
            });
            assertEquals(0, ledger.count(PAGED, KeyRange.between("c", "a")));
            assertEquals(0, ledger.count(PAGED, KeyRange.between("b", "b")));
        }
    }

    @Test
    @DisplayName("a resident key space reopened counts its entries once they are loaded, and a paged one after a crash replay")
    void reopened(@TempDir Path directory) {
        try (var ledger = open(false, directory)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 2_000; i++) {
                batch.put(RESIDENT, "k%05d".formatted(i), "v").put(PAGED, "k%05d".formatted(i), "v");
            }
            ledger.commit(batch);
            ledger.checkpoint();
            ledger.commit(ledger.newBatch().delete(PAGED, "k00010").delete(RESIDENT, "k00010").put(PAGED, "z", "v"));
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(false, directory)) {
            assertEquals(1_999, ledger.count(RESIDENT, KeyRange.all()));
            assertEquals(2_000, ledger.count(PAGED, KeyRange.all()));
            assertEquals(99, ledger.count(PAGED, KeyRange.between("k00000", "k00100")));
            assertEquals(1, ledger.count(PAGED, KeyRange.atLeast("y")));
        }
    }
}
