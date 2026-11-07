package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.LedgerIoException;
import com.github.auties00.ledger.testkit.ChangeInterceptor;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Checkpoints: the trees they write, what reads see across them, what reopening finds, and the pages they use.
class CheckpointTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> NOTES = Keyspace.paged("notes", KeyCodec.utf8(), ValueCodec.utf8());
    private static final ValueCodec<Long> LONGS = ValueCodec.ofTrusted(_ -> Long.BYTES,
            (value, target) -> target.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, value),
            source -> source.get(ValueLayout.JAVA_LONG_UNALIGNED, 0));

    private static Ledger open(Path directory, Encryption encryption, Keyspace<?, ?>... keyspaces) {
        var options = Ledger.builder().directory(directory).encryption(encryption);
        for (var keyspace : keyspaces) {
            options.keyspace(keyspace);
        }
        return options.build();
    }

    private static Ledger open(Path directory, Keyspace<?, ?>... keyspaces) {
        return open(directory, Encryption.key(KEY), keyspaces);
    }

    private static void crash(Ledger ledger) {
        ((LiveLedger) ledger).closeWithoutCheckpoint();
    }

    @Nested
    @DisplayName("delta records")
    class DeltaRecords {
        @Test
        @DisplayName("hold the scattered writes of each checkpoint, read back in snapshots, after a reopen and with pages accounted")
        void scatteredWrites(@TempDir Path directory) throws IOException {
            var random = new Random(5);
            var model = new TreeMap<String, String>();
            try (var ledger = open(directory, MESSAGES)) {
                var seed = ledger.newBatch();
                for (var i = 0; i < 6_000; i++) {
                    var key = "m%05d".formatted(i * 2);
                    var value = value(random);
                    seed.put(MESSAGES, key, value);
                    model.put(key, value);
                }
                ledger.commit(seed);
                ledger.checkpoint();
                for (var round = 0; round < 6; round++) {
                    try (var before = ledger.snapshot()) {
                        var expected = new TreeMap<>(model);
                        scatter(ledger, model, random);
                        ledger.checkpoint();
                        assertMatches(before, expected, random);
                    }
                    assertMatches(ledger, model, random);
                }
            }
            assertTrue(verifyPages(directory, true) > 0, "no leaf was written as a delta record");
            try (var ledger = open(directory, MESSAGES)) {
                assertMatches(ledger, model, random);
                for (var round = 0; round < 4; round++) {
                    scatter(ledger, model, random);
                    ledger.checkpoint();
                    assertMatches(ledger, model, random);
                }
                // Removing most keys writes the leaves whole again, or drops them
                ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between("m02000", "m10000")));
                model.subMap("m02000", "m10000").clear();
                ledger.checkpoint();
                assertMatches(ledger, model, random);
            }
            verifyPages(directory, true);
            try (var ledger = open(directory, MESSAGES)) {
                assertMatches(ledger, model, random);
            }
        }

        private static String value(Random random) {
            return "v".repeat(150 + random.nextInt(100)) + random.nextInt();
        }

        /// Overwrites, removes and adds a few hundred keys spread over the whole key space.
        private static void scatter(Ledger ledger, TreeMap<String, String> model, Random random) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 300; i++) {
                var key = "m%05d".formatted(random.nextInt(12_000));
                if (random.nextInt(8) == 0) {
                    batch.delete(MESSAGES, key);
                    model.remove(key);
                } else {
                    var value = value(random);
                    batch.put(MESSAGES, key, value);
                    model.put(key, value);
                }
            }
            ledger.commit(batch);
        }

        private static void assertMatches(View view, TreeMap<String, String> model, Random random) {
            assertEquals(model.size(), view.size(MESSAGES));
            var all = view.range(MESSAGES, KeyRange.all(), 20_000);
            assertEquals(new ArrayList<>(model.keySet()), all.stream().map(Entry::key).toList());
            assertEquals(new ArrayList<>(model.values()), all.stream().map(Entry::value).toList());
            for (var i = 0; i < 200; i++) {
                var key = "m%05d".formatted(random.nextInt(12_000));
                assertEquals(Optional.ofNullable(model.get(key)), view.get(MESSAGES, key));
            }
        }
    }

    @Nested
    @DisplayName("lazily ordered chains")
    class LazilyOrdered {
        @Test
        @DisplayName("reads of a range see the keys written since the last checkpoint, which only they order")
        void rangesSeeUnorderedKeys(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                var live = (LiveLedger) ledger;
                var seed = ledger.newBatch();
                for (var i = 0; i < 1_000; i += 2) {
                    seed.put(MESSAGES, "m%04d".formatted(i), "even");
                }
                ledger.commit(seed);
                ledger.checkpoint();
                for (var i = 999; i > 0; i -= 2) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%04d".formatted(i), "odd"));
                }
                assertEquals(0, live.state(MESSAGES).entries.size());
                var page = ledger.range(MESSAGES, KeyRange.between("m0100", "m0110"), 100);
                assertEquals(List.of("m0100", "m0101", "m0102", "m0103", "m0104", "m0105", "m0106", "m0107", "m0108", "m0109"),
                        page.stream().map(Entry::key).toList());
                assertEquals(1_000, ledger.size(MESSAGES));
                try (var snapshot = ledger.snapshot()) {
                    // The removal counts the unordered keys of its range too
                    ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between("m0500", "m0600"))
                            .put(MESSAGES, "z", "new"));
                    assertEquals(901, ledger.size(MESSAGES));
                    ledger.checkpoint();
                    assertEquals(1_000, snapshot.size(MESSAGES));
                    assertEquals(100, snapshot.range(MESSAGES, KeyRange.between("m0500", "m0600"), 1_000).size());
                }
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "x"));
                assertEquals(902, ledger.range(MESSAGES, KeyRange.all(), 2_000).size());
            }
        }
    }

    @Nested
    @DisplayName("blind writes")
    class BlindWrites {
        @Test
        @DisplayName("keep counts exact before the checkpoint that settles them, in snapshots, after it and after a reopen")
        void exactCounts(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                var live = (LiveLedger) ledger;
                var seed = ledger.newBatch();
                for (var i = 0; i < 100; i++) {
                    seed.put(MESSAGES, "k" + i, "v");
                }
                ledger.commit(seed);
                ledger.checkpoint();
                assertEquals(100, ledger.size(MESSAGES));
                try (var before = ledger.snapshot()) {
                    var writes = ledger.newBatch();
                    for (var i = 0; i < 50; i++) {
                        writes.put(MESSAGES, "k" + i, "w");
                    }
                    for (var i = 100; i < 130; i++) {
                        writes.put(MESSAGES, "k" + i, "w");
                    }
                    for (var i = 50; i < 70; i++) {
                        writes.delete(MESSAGES, "k" + i);
                    }
                    for (var i = 200; i < 210; i++) {
                        writes.delete(MESSAGES, "k" + i);
                    }
                    ledger.commit(writes);
                    // Every write found its key only in the tree, or nowhere
                    assertEquals(110, live.state(MESSAGES).blind.listed());
                    assertEquals(100, before.size(MESSAGES));
                    try (var after = ledger.snapshot()) {
                        // Once a write settles, the next write of its key knows: the overwrite of a new key adds nothing
                        ledger.commit(ledger.newBatch().put(MESSAGES, "k100", "x").delete(MESSAGES, "k0"));
                        assertEquals(110, after.size(MESSAGES));
                        assertEquals(109, ledger.size(MESSAGES));
                        ledger.checkpoint();
                        assertEquals(100, before.size(MESSAGES));
                        assertEquals(110, after.size(MESSAGES));
                    }
                }
                assertEquals(109, ledger.size(MESSAGES));
            }
            try (var reopened = open(directory, MESSAGES)) {
                assertEquals(109, reopened.size(MESSAGES));
                assertEquals(Optional.of("w"), reopened.get(MESSAGES, "k1"));
                assertEquals(Optional.empty(), reopened.get(MESSAGES, "k0"));
                assertEquals(Optional.empty(), reopened.get(MESSAGES, "k55"));
                assertEquals(Optional.of("x"), reopened.get(MESSAGES, "k100"));
            }
        }

        @Test
        @DisplayName("count exactly in a transaction and in a range removal after them, and settle at the checkpoint")
        void transactionsAndRanges(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                var live = (LiveLedger) ledger;
                var seed = ledger.newBatch();
                for (var i = 0; i < 10; i++) {
                    seed.put(MESSAGES, "a" + i, "v");
                }
                ledger.commit(seed);
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "a1", "w").put(MESSAGES, "b1", "w").delete(MESSAGES, "a2"));
                var counted = new long[1];
                ledger.update(transaction -> {
                    counted[0] = transaction.size(MESSAGES);
                    transaction.put(MESSAGES, "b2", "w");
                });
                assertEquals(10, counted[0]);
                assertEquals(11, ledger.size(MESSAGES));
                // The removal counts the keys it removes on its own, blind writes among them
                ledger.commit(ledger.newBatch().deleteRange(MESSAGES, KeyRange.between("a0", "a5")));
                assertEquals(11 - 4, ledger.size(MESSAGES));
                ledger.checkpoint();
                assertEquals(7, ledger.size(MESSAGES));
                var listed = live.state(MESSAGES).blind.listed();
                ledger.commit(ledger.newBatch().put(MESSAGES, "a9", "z"));
                assertEquals(listed + 1, live.state(MESSAGES).blind.listed());
                assertEquals(7, ledger.size(MESSAGES));
            }
        }
    }

    @Nested
    @DisplayName("random histories")
    class RandomHistories {
        @ParameterizedTest(name = "encrypted: {0}, seed {1}")
        @CsvSource({"true, 3", "false, 5", "true, 17", "false, 23", "true, 41"})
        @DisplayName("reads match a model through checkpoints, reopens and crashes")
        void model(boolean encrypted, long seed, @TempDir Path directory) throws Exception {
            var random = new Random(seed);
            var encryption = encrypted ? Encryption.key(KEY) : Encryption.none();
            var models = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
            for (var keyspace : List.of(MESSAGES, CHATS, NOTES)) {
                models.put(keyspace, new TreeMap<>());
            }
            var ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
            var snapshots = new ArrayList<PinnedSnapshot>();
            try {
                for (var step = 0; step < 300; step++) {
                    var choice = random.nextInt(20);
                    if (choice == 0) {
                        ledger.checkpoint();
                    } else if (choice == 1 && snapshots.isEmpty()) {
                        ledger.close();
                        ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
                    } else if (choice == 2 && snapshots.isEmpty()) {
                        ledger.awaitDurable();
                        crash(ledger);
                        ledger = open(directory, encryption, MESSAGES, CHATS, NOTES);
                    } else if (choice == 3 && snapshots.size() < 2) {
                        snapshots.add(new PinnedSnapshot(ledger.snapshot(), copy(models)));
                    } else if (choice == 4 && !snapshots.isEmpty()) {
                        snapshots.removeFirst().snapshot().close();
                    } else if (choice == 5) {
                        transfer(ledger, models, random);
                    } else {
                        batch(ledger, models, random, step < 10 ? 600 : 40);
                    }
                    if (step % 3 == 0) {
                        for (var keyspace : models.keySet()) {
                            verify(ledger, keyspace, models.get(keyspace), random);
                        }
                        for (var pinned : snapshots) {
                            for (var keyspace : models.keySet()) {
                                verify(pinned.snapshot(), keyspace, pinned.models().get(keyspace), random);
                            }
                        }
                    }
                }
            } finally {
                for (var pinned : snapshots) {
                    pinned.snapshot().close();
                }
                ledger.close();
            }
            try (var reopened = open(directory, encryption, MESSAGES, CHATS, NOTES)) {
                for (var keyspace : models.keySet()) {
                    verify(reopened, keyspace, models.get(keyspace), random);
                }
            }
            verifyPages(directory, encrypted);
        }
    }

    private record PinnedSnapshot(Snapshot snapshot, Map<Keyspace<String, String>, TreeMap<String, String>> models) {
    }

    private static Map<Keyspace<String, String>, TreeMap<String, String>> copy(Map<Keyspace<String, String>, TreeMap<String, String>> models) {
        var copy = new HashMap<Keyspace<String, String>, TreeMap<String, String>>();
        models.forEach((keyspace, model) -> copy.put(keyspace, new TreeMap<>(model)));
        return copy;
    }

    private static String key(Random random) {
        return "k%04d".formatted(random.nextInt(3_000));
    }

    private static String value(Random random) {
        var length = switch (random.nextInt(25)) {
            case 0 -> 3_000 + random.nextInt(25_000);
            case 1 -> 1_950 + random.nextInt(100);
            default -> random.nextInt(120);
        };
        var builder = new StringBuilder(length);
        for (var i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return builder.toString();
    }

    private static void batch(Ledger ledger, Map<Keyspace<String, String>, TreeMap<String, String>> models, Random random, int writes) {
        var batch = ledger.newBatch();
        var keyspaces = List.copyOf(models.keySet());
        for (var i = 0; i < writes; i++) {
            var keyspace = keyspaces.get(random.nextInt(keyspaces.size()));
            var model = models.get(keyspace);
            var kind = random.nextInt(30);
            if (kind == 0) {
                var a = key(random);
                var b = key(random);
                var from = a.compareTo(b) <= 0 ? a : b;
                var to = from.equals(a) ? b : a;
                var bounds = random.nextInt(8);
                var range = bounds == 0 ? KeyRange.<String>all() : bounds == 1 ? KeyRange.atLeast(from)
                        : bounds == 2 ? KeyRange.<String>below(to) : KeyRange.between(from, to);
                batch.deleteRange(keyspace, range);
                subMap(model, range.from().orElse(null), range.to().orElse(null)).clear();
            } else if (kind < 8) {
                var key = key(random);
                batch.delete(keyspace, key);
                model.remove(key);
            } else {
                var key = key(random);
                var value = value(random);
                batch.put(keyspace, key, value);
                model.put(key, value);
            }
        }
        ledger.commit(batch);
    }

    private static void transfer(Ledger ledger, Map<Keyspace<String, String>, TreeMap<String, String>> models, Random random) {
        var key = key(random);
        var other = key(random);
        ledger.update(transaction -> {
            var value = transaction.get(NOTES, key).orElse("");
            transaction.put(NOTES, other, value + "!");
            transaction.delete(MESSAGES, key);
        });
        var notes = models.get(NOTES);
        notes.put(other, notes.getOrDefault(key, "") + "!");
        models.get(MESSAGES).remove(key);
    }

    private static NavigableMap<String, String> subMap(TreeMap<String, String> model, String from, String to) {
        if (from != null && to != null) {
            return from.compareTo(to) >= 0 ? new TreeMap<>() : model.subMap(from, true, to, false);
        }
        return from != null ? model.tailMap(from, true) : to != null ? model.headMap(to, false) : model;
    }

    private static void verify(View view, Keyspace<String, String> keyspace, TreeMap<String, String> model, Random random) {
        assertEquals(model.size(), view.size(keyspace), keyspace.name() + " size");
        var all = new ArrayList<Entry<String, String>>();
        view.forEach(keyspace, KeyRange.all(), (key, value) -> all.add(new Entry<>(key, value)));
        assertEquals(entries(model), all, keyspace.name() + " contents");
        for (var i = 0; i < 4; i++) {
            var a = key(random);
            var b = key(random);
            var from = a.compareTo(b) <= 0 ? a : b;
            var to = from.equals(a) ? b : a;
            var limit = 1 + random.nextInt(40);
            var expected = subMap(model, from, to);
            assertEquals(entries(expected).stream().limit(limit).toList(), view.range(keyspace, KeyRange.between(from, to), limit));
            assertEquals(entries(expected.descendingMap()).stream().limit(limit).toList(),
                    view.range(keyspace, KeyRange.between(from, to).reversed(), limit));
            assertEquals(entries(subMap(model, from, null)).stream().limit(limit).toList(), view.range(keyspace, KeyRange.atLeast(from), limit));
        }
        for (var i = 0; i < 30; i++) {
            var key = key(random);
            assertEquals(Optional.ofNullable(model.get(key)), view.get(keyspace, key), keyspace.name() + " " + key);
            var encoded = view.readEncoded(keyspace, key, segment -> new String(segment.toArray(ValueLayout.JAVA_BYTE), java.nio.charset.StandardCharsets.UTF_8));
            assertEquals(Optional.ofNullable(model.get(key)), encoded);
        }
    }

    private static List<Entry<String, String>> entries(Map<String, String> map) {
        var entries = new ArrayList<Entry<String, String>>(map.size());
        map.forEach((key, value) -> entries.add(new Entry<>(key, value)));
        return entries;
    }

    @Nested
    @DisplayName("memory and space")
    class MemoryAndSpace {
        @Test
        @DisplayName("a checkpoint takes the paged entries out of memory and keeps the resident ones")
        void releasesMemory(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES, CHATS)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 5_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "message " + i);
                    batch.put(CHATS, "c%05d".formatted(i), "chat " + i);
                }
                ledger.commit(batch);
                var live = (LiveLedger) ledger;
                assertEquals(5_000, live.state(MESSAGES).chains());
                ledger.checkpoint();
                // The next commit's maintenance and the checkpoint's pruning drop what the tree holds
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "x"));
                assertTrue(live.state(MESSAGES).chains() <= 1, () -> live.state(MESSAGES).chains() + " chains in memory");
                // A paged key space orders its chains only for a read of a range, which none made
                assertTrue(live.state(MESSAGES).entries.size() <= 1, () -> live.state(MESSAGES).entries.size() + " ordered");
                assertEquals(5_000, live.state(CHATS).entries.size());
                assertEquals(Optional.of("message 4321"), ledger.get(MESSAGES, "m04321"));
                assertEquals(5_001, ledger.size(MESSAGES));
            }
        }

        @Test
        @DisplayName("overwriting the same keys reuses pages instead of growing the file")
        void reusesPages(@TempDir Path directory) throws IOException {
            try (var ledger = open(directory, MESSAGES)) {
                for (var round = 0; round < 60; round++) {
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 2_000; i++) {
                        batch.put(MESSAGES, "m%05d".formatted(i), "round " + round + " " + "x".repeat(i % 200));
                    }
                    if (round % 5 == 0) {
                        batch.put(MESSAGES, "large", "L".repeat(50_000 + round));
                    }
                    ledger.commit(batch);
                    ledger.checkpoint();
                }
            }
            // About 2,000 entries of up to 230 bytes and one 50 KB value: a few hundred KiB live
            var size = Files.size(directory.resolve(PageFile.FILE));
            assertTrue(size < 4L << 20, () -> "the data file grew to " + size + " bytes");
            verifyPages(directory, true);
        }

        @Test
        @DisplayName("a snapshot keeps the pages of its trees through later checkpoints")
        void snapshotPinsPages(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 3_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "first " + i);
                }
                ledger.commit(batch);
                ledger.checkpoint();
                try (var snapshot = ledger.snapshot()) {
                    for (var round = 0; round < 10; round++) {
                        var overwrite = ledger.newBatch();
                        for (var i = 0; i < 3_000; i++) {
                            overwrite.put(MESSAGES, "m%05d".formatted(i), "round " + round + " " + i);
                        }
                        ledger.commit(overwrite);
                        ledger.checkpoint();
                    }
                    assertEquals(Optional.of("first 1234"), snapshot.get(MESSAGES, "m01234"));
                    var seen = new AtomicLong();
                    snapshot.forEach(MESSAGES, KeyRange.all(), (key, value) -> {
                        assertTrue(value.startsWith("first "), value);
                        seen.incrementAndGet();
                        return true;
                    });
                    assertEquals(3_000, seen.get());
                }
                assertEquals(Optional.of("round 9 1234"), ledger.get(MESSAGES, "m01234"));
            }
        }
    }

    @Nested
    @DisplayName("automatic checkpoints")
    class Automatic {
        @Test
        @DisplayName("the store checkpoints on its own as the log grows, and reopens from its checkpoints")
        void triggered(@TempDir Path directory) throws Exception {
            var value = "v".repeat(1_000);
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                    .checkpointEvery(1 << 20).build()) {
                for (var i = 0; i < 4_000; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value + i));
                }
                var live = (LiveLedger) ledger;
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (live.tree() == null && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                    Thread.sleep(10);
                }
                assertTrue(live.tree() != null, "no checkpoint started on its own");
                ledger.awaitDurable();
                crash(ledger);
            }
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(4_000, ledger.size(MESSAGES));
                assertEquals(Optional.of(value + 3999), ledger.get(MESSAGES, "m03999"));
            }
        }

        @Test
        @DisplayName("a store that commits nothing for a tick of the idle ticker checkpoints, once its writes since the last hold a megabyte of heap")
        void idle(@TempDir Path directory) throws Exception {
            var value = "v".repeat(64 << 10);
            Checkpointer checkpointer;
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                    .checkpointEvery(1L << 30).build()) {
                var live = (LiveLedger) ledger;
                checkpointer = live.checkpointer();
                // Ticked by the test alone: a tick of the process's ticker between two of these would move them
                IdleTicker.unwatch(checkpointer);
                ledger.commit(ledger.newBatch().put(MESSAGES, "small", "x")).awaitDurable();
                checkpointer.tick();
                checkpointer.tick();
                Thread.sleep(200);
                assertNull(live.tree(), "an idle store checkpointed a few bytes");
                for (var i = 0; i < 40; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value));
                }
                ledger.awaitDurable();
                // The commits since the last tick make this one busy
                checkpointer.tick();
                Thread.sleep(200);
                assertNull(live.tree(), "a store checkpointed at a tick that saw commits");
                checkpointer.tick();
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (live.tree() == null && System.nanoTime() < deadline) {
                    Thread.sleep(10);
                }
                assertTrue(live.tree() != null, "an idle store with megabytes the trees lack did not checkpoint");
                assertEquals(41, ledger.size(MESSAGES));
            }
            // A closed store's tick starts nothing
            checkpointer.tick();
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(Optional.of(value), ledger.get(MESSAGES, "m00039"));
            }
        }
    }

    @Nested
    @DisplayName("work on the store's own threads")
    class BackgroundWork {
        private static boolean dataFile(Path path) {
            return path.getFileName() != null && path.getFileName().toString().equals(PageFile.FILE);
        }

        @ParameterizedTest(name = "final checkpoint {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a checkpoint an idle tick started as the store closes writes nothing after its files close, and the store loses nothing")
        void tickThenClose(boolean checkpoint, @TempDir Path root) throws Exception {
            var value = "v".repeat(64 << 10);
            var reports = new ConcurrentLinkedQueue<LedgerException>();
            for (var attempt = 0; attempt < 3; attempt++) {
                var directory = Files.createDirectory(root.resolve("store" + attempt));
                var ledger = (LiveLedger) Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                        .keyspace(MESSAGES).checkpointEvery(1L << 30).onBackgroundFailure(reports::add).build();
                for (var i = 0; i < 200; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value));
                }
                ledger.awaitDurable();
                // The first tick sees the commits, the second an idle store with megabytes the trees lack
                ledger.checkpointer().tick();
                ledger.checkpointer().tick();
                if (checkpoint) {
                    ledger.close();
                } else {
                    ledger.closeWithoutCheckpoint();
                }
                Thread.sleep(300);
                try (var reopened = open(directory, MESSAGES)) {
                    assertEquals(200, reopened.size(MESSAGES));
                    assertEquals(Optional.of(value), reopened.get(MESSAGES, "m00199"));
                }
            }
            assertTrue(reports.isEmpty(), () -> "reported: " + reports.peek() + ", caused by " + reports.peek().getCause());
        }

        @Test
        @DisplayName("a checkpoint that fails on its own thread is reported, and the store goes on")
        void reportsFailures() throws Exception {
            var disk = CrashFileSystem.create();
            var failing = new AtomicBoolean(true);
            disk.intercept((path, kind) -> {
                if (failing.get() && kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                    throw new IOException("No space left on device");
                }
            });
            var failures = new ConcurrentLinkedQueue<LedgerException>();
            var directory = disk.getPath("/store");
            var value = "v".repeat(10_000);
            try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(MESSAGES)
                    .checkpointEvery(1 << 20).onBackgroundFailure(failures::add).build()) {
                var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                var written = 0;
                while (failures.isEmpty() && System.nanoTime() < deadline) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(written++), value));
                    if (written > 300) {
                        Thread.sleep(10);
                    }
                }
                assertTrue(failures.peek() instanceof LedgerIoException, () -> "reported: " + failures);
                ledger.commit(ledger.newBatch().put(MESSAGES, "after", "failure")).awaitDurable();
                failing.set(false);
                ledger.checkpoint();
                assertEquals(written + 1, ledger.size(MESSAGES));
            }
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(Optional.of("failure"), ledger.get(MESSAGES, "after"));
                assertEquals(Optional.of(value), ledger.get(MESSAGES, "m00000"));
            }
        }

        @Test
        @DisplayName("the heap budget is the process's: a store waits at two intervals while another store's writes fill the budget")
        void processBudget(@TempDir Path directory) throws Exception {
            var value = "v".repeat(64 << 10);
            // A store that buffers about 6 MB, checkpointing only at close
            try (var other = Ledger.builder().directory(directory.resolve("other")).encryption(Encryption.key(KEY))
                    .keyspace(MESSAGES).checkpointEvery(1L << 30).build()) {
                for (var i = 0; i < 100; i++) {
                    other.commit(other.newBatch().put(MESSAGES, "o%05d".formatted(i), value));
                }
                other.awaitDurable();
                var disk = CrashFileSystem.create();
                disk.intercept((path, kind) -> {
                    if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
                var every = 1 << 20;
                // A budget the other store's writes alone pass
                var options = (LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                        .keyspace(MESSAGES).checkpointEvery(every);
                try (var ledger = options.checkpointHeap(4L << 20).build()) {
                    var checkpointer = ((LiveLedger) ledger).checkpointer();
                    var largest = 0L;
                    for (var i = 0; i < 200; i++) {
                        ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value));
                        largest = Math.max(largest, checkpointer.backlog());
                    }
                    var bound = 2L * every + 2L * value.length();
                    var observed = largest;
                    assertTrue(observed <= bound, () -> observed + " bytes of log the trees lacked, past " + bound);
                }
            }
        }

        @ParameterizedTest(name = "heap budget {0} bytes")
        @ValueSource(longs = {Long.MAX_VALUE, 1})
        @DisplayName("commits wait for checkpoints slower than they are, so the log the trees lack stays within six intervals, or two once its writes pass the heap budget")
        void backpressure(long heapBudget) throws Exception {
            var disk = CrashFileSystem.create();
            disk.intercept((path, kind) -> {
                if (kind == ChangeInterceptor.ChangeKind.WRITE && dataFile(path)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            var every = 1 << 20;
            var value = "v".repeat(64 << 10);
            var options = (LedgerBuilder) Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(KEY))
                    .keyspace(MESSAGES).checkpointEvery(every);
            try (var ledger = options.checkpointHeap(heapBudget).build()) {
                var checkpointer = ((LiveLedger) ledger).checkpointer();
                var largest = 0L;
                for (var i = 0; i < 200; i++) {
                    ledger.commit(ledger.newBatch().put(MESSAGES, "m%05d".formatted(i), value));
                    largest = Math.max(largest, checkpointer.backlog());
                }
                // The intervals the budget allows, and the commit that found the log just under them
                var bound = (heapBudget == 1 ? 2L : 6L) * every + 2L * value.length();
                var observed = largest;
                assertTrue(observed <= bound, () -> observed + " bytes of log the trees lacked, past " + bound);
                assertEquals(200, ledger.size(MESSAGES));
            }
        }
    }

    @Nested
    @DisplayName("key spaces left out")
    class LeftOut {
        @Test
        @DisplayName("survive the checkpoints of opens without them, from the trees and from the log")
        void hidden(@TempDir Path directory) throws Exception {
            try (var ledger = open(directory, MESSAGES, NOTES)) {
                ledger.commit(ledger.newBatch().put(NOTES, "checkpointed", "1"));
            }
            var crashed = open(directory, MESSAGES, NOTES);
            crashed.commit(crashed.newBatch().put(NOTES, "logged", "2")).awaitDurable();
            crash(crashed);
            try (var ledger = open(directory, MESSAGES)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "m", "x"));
                ledger.checkpoint();
            }
            try (var ledger = open(directory, MESSAGES, NOTES)) {
                assertEquals(Optional.of("1"), ledger.get(NOTES, "checkpointed"));
                assertEquals(Optional.of("2"), ledger.get(NOTES, "logged"));
                assertEquals(2, ledger.size(NOTES));
                assertEquals(Optional.of("x"), ledger.get(MESSAGES, "m"));
            }
        }
    }

    @Nested
    @DisplayName("transactions that roll back")
    class RolledBack {
        @Test
        @DisplayName("leave the trees with the keys they wrote, and the checkpoints after them consistent")
        void treeKeys(@TempDir Path directory) {
            try (var ledger = open(directory, MESSAGES)) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "kept", "1"));
                // The checkpoint leaves the key in the tree only
                ledger.checkpoint();
                assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                    transaction.put(MESSAGES, "kept", "2").put(MESSAGES, "new", "3");
                    throw new IllegalStateException("abort");
                }));
                ledger.commit(ledger.newBatch().put(MESSAGES, "other", "4"));
                ledger.checkpoint();
                assertEquals(Optional.of("1"), ledger.get(MESSAGES, "kept"));
                assertEquals(Optional.empty(), ledger.get(MESSAGES, "new"));
                assertEquals(2, ledger.size(MESSAGES));
            }
            try (var ledger = open(directory, MESSAGES)) {
                assertEquals(Optional.of("1"), ledger.get(MESSAGES, "kept"));
                assertEquals(2, ledger.size(MESSAGES));
            }
        }
    }

    @Nested
    @DisplayName("commits over trees the cache does not hold")
    class ColdTrees {
        @Test
        @DisplayName("read the pages their lookups need before the writer lock and keep exact counts")
        void coldBatches(@TempDir Path directory) {
            var values = Keyspace.paged("values", KeyCodec.int64(), LONGS);
            var random = new Random(31);
            var model = new TreeMap<Long, Long>();
            var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(values).pageCache(1 << 20);
            try (var ledger = options.build()) {
                // Far more leaves than the 256 pages the cache holds
                for (var first = 0L; first < 60_000; first += 5_000) {
                    var batch = ledger.newBatch();
                    for (var key = first; key < first + 5_000; key++) {
                        batch.put(values, key * 2, key);
                        model.put(key * 2, key);
                    }
                    ledger.commit(batch);
                }
                ledger.checkpoint();
                for (var round = 0; round < 6; round++) {
                    ((LiveLedger) ledger).clearPageCacheForTests();
                    var batch = ledger.newBatch();
                    for (var i = 0; i < 400; i++) {
                        // Even keys exist, odd ones do not
                        var key = (long) random.nextInt(130_000);
                        if (random.nextInt(3) == 0) {
                            batch.delete(values, key);
                            model.remove(key);
                        } else {
                            batch.put(values, key, (long) round);
                            model.put(key, (long) round);
                        }
                    }
                    ledger.commit(batch);
                    assertEquals(model.size(), ledger.size(values), "round " + round);
                    if (round % 2 == 1) {
                        ledger.checkpoint();
                    }
                }
                for (var key = 0L; key < 130_000; key += 97) {
                    assertEquals(Optional.ofNullable(model.get(key)), ledger.get(values, key), "key " + key);
                }
            }
            try (var ledger = options.build()) {
                assertEquals(model.size(), ledger.size(values));
            }
        }
    }

    @Nested
    @DisplayName("damage")
    class Damage {
        @Test
        @DisplayName("a damaged page of a tree fails the read that reaches it")
        void damagedPage(@TempDir Path directory) throws IOException {
            try (var ledger = open(directory, MESSAGES)) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 2_000; i++) {
                    batch.put(MESSAGES, "m%05d".formatted(i), "value " + i);
                }
                ledger.commit(batch);
            }
            var file = directory.resolve(PageFile.FILE);
            var bytes = Files.readAllBytes(file);
            // A page in the middle of the units, past the superblocks
            var page = (bytes.length / PageFormat.SIZE) / 2;
            bytes[page * PageFormat.SIZE + PageFormat.HEADER + 100] ^= 1;
            Files.write(file, bytes);
            try (var ledger = open(directory, MESSAGES)) {
                assertThrows(LedgerCorruptedException.class, () -> ledger.forEach(MESSAGES, KeyRange.all(), (_, _) -> true));
            } catch (LedgerCorruptedException damagedOnOpen) {
                // The damaged unit was the catalog or the free list, which opening reads
            }
        }

        @Test
        @DisplayName("a torn superblock leaves the one before it, and the log replays the rest")
        void tornSuperblock(@TempDir Path directory) throws Exception {
            var ledger = open(directory, MESSAGES);
            try {
                ledger.commit(ledger.newBatch().put(MESSAGES, "a", "1"));
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "b", "2"));
                ledger.checkpoint();
                ledger.commit(ledger.newBatch().put(MESSAGES, "c", "3")).awaitDurable();
            } finally {
                crash(ledger);
            }
            // Tear the newest superblock, as a crash during its write would
            var file = directory.resolve(PageFile.FILE);
            var bytes = Files.readAllBytes(file);
            var newest = 0;
            bytes[newest * PageFormat.SIZE + PageFormat.HEADER + 3] ^= 1;
            Files.write(file, bytes);
            // A checkpoint deletes log segments only once its superblock is durable, so the log still covers the older one
            try (var reopened = open(directory, MESSAGES)) {
                assertEquals(List.of("a", "b", "c"), reopened.range(MESSAGES, KeyRange.all(), 10).stream().map(Entry::key).toList());
            }
        }
    }

    @Nested
    @DisplayName("concurrency")
    class Concurrency {
        @Test
        @DisplayName("writers racing on the same keys leave every snapshot's count equal to what it reads")
        void racingCounts(@TempDir Path directory) throws Exception {
            var values = Keyspace.paged("values", KeyCodec.int64(), LONGS);
            var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(values)
                    .checkpointEvery(1 << 20);
            long size;
            try (var ledger = options.build()) {
                var failures = new ConcurrentLinkedQueue<String>();
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < 8; writer++) {
                        var id = writer;
                        executor.execute(() -> {
                            var random = new Random(id);
                            for (var round = 0; round < 1_500; round++) {
                                var batch = ledger.newBatch();
                                // Few keys, so most commits wait on another's write of the same key
                                for (var i = 0; i < 1 + random.nextInt(4); i++) {
                                    var key = (long) random.nextInt(24);
                                    if (random.nextInt(3) == 0) {
                                        batch.delete(values, key);
                                    } else {
                                        batch.put(values, key, (long) round);
                                    }
                                }
                                ledger.commit(batch);
                                if (round % 50 == 0) {
                                    try (var snapshot = ledger.snapshot()) {
                                        var seen = snapshot.range(values, KeyRange.all(), 1_000).size();
                                        if (seen != snapshot.size(values)) {
                                            failures.add("writer " + id + " round " + round + " read " + seen + " entries where the count is "
                                                    + snapshot.size(values));
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
                assertTrue(failures.isEmpty(), () -> String.join(System.lineSeparator(), failures));
                size = ledger.size(values);
                assertEquals(size, ledger.range(values, KeyRange.all(), 1_000).size());
            }
            try (var ledger = options.build()) {
                assertEquals(size, ledger.size(values));
                assertEquals(size, ledger.range(values, KeyRange.all(), 1_000).size());
            }
        }

        @Test
        @DisplayName("writers that create keys at once see their commits in scans and counts, across checkpoints")
        void concurrentNewKeys(@TempDir Path directory) throws Exception {
            var values = Keyspace.paged("values", KeyCodec.int64(), LONGS);
            var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(values)
                    .checkpointEvery(1 << 20);
            var writers = 8;
            var rounds = 400;
            var keysPerRound = 16;
            long size;
            try (var ledger = options.build()) {
                var failures = new ConcurrentLinkedQueue<String>();
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < writers; writer++) {
                        var id = writer;
                        executor.execute(() -> {
                            var random = new Random(id);
                            for (var round = 0; round < rounds; round++) {
                                var batch = ledger.newBatch();
                                // Keys of their own above 2^32, so each writer's are new
                                var first = ((long) (id + 1) << 32) | ((long) round * keysPerRound);
                                for (var i = 0; i < keysPerRound; i++) {
                                    batch.put(values, first + i, (long) round);
                                }
                                // And a few hot keys every writer puts and deletes, so chains are dropped and opened again
                                var hot = (long) random.nextInt(64);
                                if (random.nextBoolean()) {
                                    batch.put(values, hot, (long) id);
                                } else {
                                    batch.delete(values, hot);
                                }
                                ledger.commit(batch);
                                var seen = ledger.range(values, KeyRange.between(first, first + keysPerRound), 100).size();
                                if (seen != keysPerRound) {
                                    failures.add("writer " + id + " round " + round + " saw " + seen + " of its keys");
                                }
                            }
                        });
                    }
                }
                assertTrue(failures.isEmpty(), () -> String.join(System.lineSeparator(), failures));
                var hot =ledger.range(values, KeyRange.between(0L, 64L), 1_000).size();
                size = (long) writers * rounds * keysPerRound + hot;
                assertEquals(size, ledger.size(values));
                assertEquals(size, ledger.range(values, KeyRange.all(), Integer.MAX_VALUE).size());
            }
            try (var ledger = options.build()) {
                assertEquals(size, ledger.size(values));
            }
        }

        @Test
        @DisplayName("readers see consistent totals while checkpoints run under the writers")
        void transfersWithCheckpoints(@TempDir Path directory) throws Exception {
            var accounts = Keyspace.paged("accounts", KeyCodec.int64(), LONGS);
            var count = 64;
            var expected = count * 1_000L;
            try (var ledger = open(directory, accounts)) {
                var initial = ledger.newBatch();
                for (var account = 0; account < count; account++) {
                    initial.put(accounts, (long) account, 1_000L);
                }
                ledger.commit(initial);
                var stop = new AtomicBoolean();
                var failures = new ConcurrentLinkedQueue<String>();
                var transfers = new AtomicLong();
                var checkpoints = new AtomicLong();
                try (var executor = Executors.newThreadPerTaskExecutor(Thread.ofPlatform().daemon().factory())) {
                    for (var writer = 0; writer < 4; writer++) {
                        executor.execute(() -> {
                            var random = ThreadLocalRandom.current();
                            while (!stop.get()) {
                                var from = (long) random.nextInt(count);
                                var to = (long) random.nextInt(count);
                                var amount = (long) random.nextInt(1, 10);
                                ledger.update(transaction -> {
                                    transaction.put(accounts, from, transaction.get(accounts, from).orElseThrow() - amount);
                                    transaction.put(accounts, to, transaction.get(accounts, to).orElseThrow() + amount);
                                });
                                transfers.incrementAndGet();
                            }
                        });
                    }
                    for (var reader = 0; reader < 4; reader++) {
                        var useSnapshot = reader % 2 == 0;
                        executor.execute(() -> {
                            while (!stop.get()) {
                                var total = new long[1];
                                if (useSnapshot) {
                                    try (var snapshot = ledger.snapshot()) {
                                        snapshot.forEach(accounts, KeyRange.all(), (_, value) -> {
                                            total[0] += value;
                                            return true;
                                        });
                                    }
                                } else {
                                    ledger.forEach(accounts, KeyRange.all(), (_, value) -> {
                                        total[0] += value;
                                        return true;
                                    });
                                }
                                if (total[0] != expected) {
                                    failures.add("total " + total[0]);
                                }
                            }
                        });
                    }
                    executor.execute(() -> {
                        while (!stop.get()) {
                            ledger.checkpoint();
                            checkpoints.incrementAndGet();
                        }
                    });
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                    stop.set(true);
                }
                assertTrue(failures.isEmpty(), () -> failures.size() + " inconsistent reads, first: " + failures.peek());
                assertTrue(transfers.get() > 100 && checkpoints.get() > 5, () -> transfers + " transfers, " + checkpoints + " checkpoints");
            }
            try (var ledger = open(directory, accounts)) {
                var total = new long[1];
                ledger.forEach(accounts, KeyRange.all(), (_, value) -> {
                    total[0] += value;
                    return true;
                });
                assertEquals(expected, total[0]);
                assertEquals(count, ledger.size(accounts));
            }
            verifyPages(directory, true);
        }
    }

    /// Walks the data file from its newest superblock and checks that every page is used once or free.
    ///
    /// @return the delta records the trees refer to
    static int verifyPages(Path directory, boolean encrypted) throws IOException {
        var header = StoreHeader.read(directory);
        var keys = header.open(encrypted ? EncryptionSpec.key(KEY) : EncryptionSpec.NONE);
        try (var pages = PageFile.open(directory, new PageCipher(keys.pageKey()), 1 << 20, new HeapPool(1 << 20))) {
            var superblock = pages.superblock();
            if (superblock == null) {
                return 0;
            }
            var used = new BitSet();
            var usedBlobs = new BitSet();
            mark(used, superblock.catalog());
            mark(used, superblock.freeList());
            if (superblock.blobFreeList() != null) {
                mark(used, superblock.blobFreeList());
            }
            var deltas = DeltaTable.EMPTY;
            if (superblock.deltaTable() != null) {
                mark(used, superblock.deltaTable());
                deltas = DeltaTable.decode(pages.read(superblock.deltaTable(), PageFormat.DELTA_TABLE, false));
            }
            var catalog = Catalog.decode(pages.read(superblock.catalog(), PageFormat.CATALOG, false));
            var records = new java.util.TreeMap<Integer, Integer>();
            for (var entry : catalog.entries()) {
                if (entry.root() != null) {
                    assertFalse(entry.root().ref().isRecord(), "a root is a delta record");
                    walk(pages, entry.root().ref(), used, usedBlobs, records);
                }
            }
            verifyDeltas(used, records, deltas);
            var free = Catalog.decodeFreeList(pages.read(superblock.freeList(), PageFormat.FREE_LIST, false), superblock.pageCount());
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < superblock.pageCount(); page++) {
                assertTrue(used.get(page) != free.get(page), "page " + page + (used.get(page) ? " is used and free" : " is neither used nor free"));
            }
            assertFalse(used.get(superblock.pageCount()), "a unit is past the end of the file");
            // A store closed cleanly has no unit in flight: every page of the file of large values is a tree's or free
            var blobFree = superblock.blobFreeList() == null ? new BitSet()
                    : Catalog.decodeFreeList(pages.read(superblock.blobFreeList(), PageFormat.FREE_LIST, false), superblock.blobPageCount());
            for (var page = PageFormat.FIRST_UNIT_PAGE; page < superblock.blobPageCount(); page++) {
                assertTrue(usedBlobs.get(page) != blobFree.get(page),
                        "page " + page + " of the large values" + (usedBlobs.get(page) ? " is used and free" : " is neither used nor free"));
            }
            assertTrue(usedBlobs.isEmpty() || usedBlobs.length() <= superblock.blobPageCount(), "a value is past the end of its file");
            return records.values().stream().mapToInt(Integer::intValue).sum();
        }
    }

    private static void mark(BitSet used, PageRef ref) {
        for (var page = ref.page(); page < ref.page() + ref.pages(); page++) {
            assertFalse(used.get(page), "page " + page + " is used twice");
            used.set(page);
        }
    }

    /// Marks the delta pages the trees refer to, each once, and checks their table counts the records walked.
    static void verifyDeltas(BitSet used, java.util.Map<Integer, Integer> records, DeltaTable deltas) {
        for (var entry : records.entrySet()) {
            mark(used, new PageRef(entry.getKey(), 1, 0, 0));
            assertEquals(entry.getValue(), deltas.records(entry.getKey()), "delta page " + entry.getKey() + " counts wrong");
        }
        assertEquals(records.size(), deltas.size(), "the table lists delta pages no tree refers to");
    }

    static void walk(PageFile pages, PageRef ref, BitSet used, BitSet usedBlobs, java.util.Map<Integer, Integer> records) {
        var unit = pages.read(ref, PageFile.NODE, false);
        if (ref.isRecord()) {
            records.merge(ref.page(), 1, Integer::sum);
            mark(used, TreeNodes.base(unit, ref));
        } else {
            mark(used, ref);
        }
        for (var i = 0; i < TreeNodes.count(unit); i++) {
            if (TreeNodes.isLeaf(unit)) {
                var entry = TreeNodes.entry(unit, i);
                if (TreeNodes.isBlob(unit, entry)) {
                    mark(used, TreeNodes.blobRef(unit, entry));
                } else if (TreeNodes.isExternal(unit, entry)) {
                    mark(usedBlobs, TreeNodes.blobRef(unit, entry));
                }
            } else {
                walk(pages, TreeNodes.childRef(unit, i), used, usedBlobs, records);
            }
        }
    }
}
