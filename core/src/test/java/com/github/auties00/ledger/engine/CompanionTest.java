package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Companion;
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
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Companions of computed writes: zero-length entries under the computed keys while their values are members, written
/// in step with the computed value on its first run and on its run in order, resident and paged.
class CompanionTest {
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, byte[]> STARRED = Keyspace.resident("messages.starred", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<String, byte[]> PINNED = Keyspace.paged("messages.pinned", KeyCodec.utf8(), ValueCodec.bytes());
    private static final List<Companion<String, ? super String>> INDEXES = List.of(
            new Companion<>(STARRED, (String value) -> value.contains("*")),
            new Companion<>(PINNED, (String value) -> value.contains("^")));

    enum Store {
        MEMORY, DIRECTORY
    }

    private static Ledger open(Store store, Path directory) {
        var options = store == Store.MEMORY ? Ledger.builder().memory()
                : Ledger.builder().directory(directory).encryption(Encryption.none());
        return options.keyspace(MESSAGES).keyspace(STARRED).keyspace(PINNED).build();
    }

    private static void compute(Ledger ledger, String key, String value) {
        var batch = ledger.newBatch();
        batch.compute(MESSAGES, key, _ -> value, INDEXES);
        ledger.commit(batch);
    }

    private static List<String> keys(Ledger ledger, Keyspace<String, byte[]> keyspace) {
        var keys = new ArrayList<String>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> {
            assertArrayEquals(new byte[0], value);
            return keys.add(key);
        });
        return keys;
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("an entry appears when the value becomes a member and goes when it stops being one or is removed")
    void membership(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            compute(ledger, "a", "plain");
            compute(ledger, "b", "star*");
            compute(ledger, "c", "both*^");
            assertEquals(List.of("b", "c"), keys(ledger, STARRED));
            assertEquals(List.of("c"), keys(ledger, PINNED));
            compute(ledger, "a", "now*");
            compute(ledger, "c", "pin^");
            assertEquals(List.of("a", "b"), keys(ledger, STARRED));
            assertEquals(List.of("c"), keys(ledger, PINNED));
            compute(ledger, "b", null);
            assertEquals(List.of("a"), keys(ledger, STARRED));
            assertEquals(1, ledger.size(STARRED));
            assertEquals(1, ledger.count(PINNED, KeyRange.all()));
            // Unchanged: nothing written
            var batch = ledger.newBatch();
            var outcome = batch.compute(MESSAGES, "a", value -> value, INDEXES);
            ledger.commit(batch);
            assertEquals(false, outcome.changed());
            assertEquals(List.of("a"), keys(ledger, STARRED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a run in order derives the companions from its own result, not the first run's")
    void rerun(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            compute(ledger, "a", "plain");
            var batch = ledger.newBatch();
            var runs = new AtomicInteger();
            batch.compute(MESSAGES, "a", value -> runs.incrementAndGet() == 1 ? "first*" : value + "^", INDEXES);
            ledger.commit(ledger.newBatch().put(MESSAGES, "a", "moved"));
            ledger.commit(batch);
            assertEquals(2, runs.get());
            assertEquals(List.of(), keys(ledger, STARRED));
            assertEquals(List.of("a"), keys(ledger, PINNED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a compute after a blind put of a member counts the companions it finds, not the ones membership implies")
    void afterBlindPut(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            // The blind put names no companion, so neither holds an entry for the member it writes
            ledger.commit(ledger.newBatch().put(MESSAGES, "a", "x*^"));
            compute(ledger, "a", "plain");
            assertEquals(List.of(), keys(ledger, STARRED));
            assertEquals(List.of(), keys(ledger, PINNED));
            assertEquals(0, ledger.size(STARRED));
            assertEquals(0, ledger.size(PINNED));
            assertEquals(0, ledger.count(PINNED, KeyRange.all()));
            if (store == Store.DIRECTORY) {
                ledger.checkpoint();
                assertEquals(0, ledger.size(PINNED));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a compute after a range removal of the primary alone counts the companions it finds")
    void afterPrimaryRangeRemoval(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            compute(ledger, "a", "x*^");
            if (store == Store.DIRECTORY) {
                ledger.checkpoint();
            }
            // The companions keep their entries: the removal names only the primary
            ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.all()));
            compute(ledger, "a", "y*^");
            assertEquals(List.of("a"), keys(ledger, STARRED));
            assertEquals(List.of("a"), keys(ledger, PINNED));
            assertEquals(1, ledger.size(STARRED));
            assertEquals(1, ledger.size(PINNED));
            if (store == Store.DIRECTORY) {
                ledger.checkpoint();
                assertEquals(1, ledger.size(PINNED));
            }
        }
    }

    @Test
    @DisplayName("a companion must be another key space, opened, and given once")
    void validation() {
        try (var ledger = open(Store.MEMORY, null)) {
            var batch = ledger.newBatch();
            assertThrows(IllegalArgumentException.class, () -> batch.compute(MESSAGES, "a", v -> v,
                    List.of(new Companion<>(MESSAGES, (String _) -> true))));
            assertThrows(IllegalArgumentException.class, () -> batch.compute(MESSAGES, "a", v -> v,
                    List.of(new Companion<>(STARRED, (String _) -> true), new Companion<>(STARRED, (String _) -> false))));
            var unknown = Keyspace.resident("unknown", KeyCodec.utf8(), ValueCodec.bytes());
            assertThrows(IllegalArgumentException.class, () -> batch.compute(MESSAGES, "a", v -> v,
                    List.of(new Companion<>(unknown, (String _) -> true))));
            assertTrue(batch.isEmpty());
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("random computes keep the companions equal to the membership of the values, across checkpoints and reopens")
    void model(Store store, @TempDir Path directory) {
        var random = new Random(7);
        var model = new TreeMap<String, String>();
        var ledger = open(store, directory);
        try {
            for (var step = 0; step < 2_000; step++) {
                var key = "k" + random.nextInt(60);
                var choice = random.nextInt(10);
                String value = choice == 0 ? null : "v" + random.nextInt(4) + (random.nextBoolean() ? "*" : "") + (random.nextInt(3) == 0 ? "^" : "");
                if (choice == 1) {
                    // A blind write between a compute's read and its turn
                    var batch = ledger.newBatch();
                    batch.compute(MESSAGES, key, old -> old == null ? "n*" : old + "^", INDEXES);
                    var blind = "b" + random.nextInt(3) + (random.nextBoolean() ? "*" : "");
                    // The blind put is not a computed write: it writes its companions itself
                    var blindBatch = ledger.newBatch();
                    blindBatch.compute(MESSAGES, key, _ -> blind, INDEXES);
                    ledger.commit(blindBatch);
                    model.put(key, blind);
                    ledger.commit(batch);
                    model.put(key, blind + "^");
                } else {
                    compute(ledger, key, value);
                    if (value == null) {
                        model.remove(key);
                    } else {
                        model.put(key, value);
                    }
                }
                if (store == Store.DIRECTORY && step % 500 == 499) {
                    if (random.nextBoolean()) {
                        ledger.checkpoint();
                    } else {
                        ledger.close();
                        ledger = open(store, directory);
                    }
                }
            }
            var starred = model.entrySet().stream().filter(e -> e.getValue().contains("*")).map(java.util.Map.Entry::getKey).toList();
            var pinned = model.entrySet().stream().filter(e -> e.getValue().contains("^")).map(java.util.Map.Entry::getKey).toList();
            assertEquals(starred, keys(ledger, STARRED));
            assertEquals(pinned, keys(ledger, PINNED));
            assertEquals(starred.size(), ledger.size(STARRED));
            assertEquals(pinned.size(), ledger.size(PINNED));
            assertEquals(model.size(), ledger.size(MESSAGES));
        } finally {
            ledger.close();
        }
    }
}
