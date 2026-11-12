package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The load of a resident key space beside a store that opened without it: reads, counts, writes, snapshots,
/// checkpoints, closes, crashes and failures while the loader is paused between its runs ([Pause]), and all of them
/// at once while it runs. Values of 1,400 bytes put two entries in a leaf, so 1,200 entries take the loader seven runs.
class ResidentLoadTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());
    // The value whose decoding fails, or null
    private static final AtomicReference<String> REFUSED = new AtomicReference<>();
    // The decoding that waits for the test, or null
    private static final AtomicReference<Hold> HELD = new AtomicReference<>();
    private static final Keyspace<String, String> FRAGILE = Keyspace.resident("fragile", KeyCodec.utf8(), ValueCodec.of(
            String::length,
            (value, target) -> MemorySegment.copy(value.getBytes(StandardCharsets.US_ASCII), 0, target, ValueLayout.JAVA_BYTE, 0, value.length()),
            source -> {
                var value = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII);
                if (value.equals(REFUSED.get())) {
                    throw new IllegalStateException("refused " + value);
                }
                var held = HELD.get();
                if (held != null && value.equals(held.value())) {
                    held.hold();
                }
                return value;
            }));
    private static final Keyspace<String, byte[]> RAW = Keyspace.resident("raw", KeyCodec.utf8(), ValueCodec.bytes());
    private static final Keyspace<byte[], byte[]> RAW_KEYS = Keyspace.resident("raw_keys", KeyCodec.bytes(), ValueCodec.bytes());
    private static final int ENTRIES = 1_200;

    private static String key(int index) {
        return "s%05d".formatted(index);
    }

    private static String value(int index) {
        return "v" + index + "-" + "x".repeat(1_400);
    }

    private static Ledger open(Path directory, boolean atOpen) {
        return open(directory, atOpen, _ -> {
        });
    }

    private static Ledger open(Path directory, boolean atOpen, Consumer<? super LedgerException> failures) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY))
                .keyspace(SESSIONS).keyspace(MESSAGES).keyspace(FRAGILE)
                .loadResidentAtOpen(atOpen).onBackgroundFailure(failures).build();
    }

    /// Writes `entries` entries to `keyspace` and a few messages, and closes the store, whose final checkpoint holds
    /// them.
    ///
    /// @return the entries
    private static TreeMap<String, String> populate(Path directory, Keyspace<String, String> keyspace, int entries) {
        var model = new TreeMap<String, String>();
        try (var ledger = open(directory, false)) {
            for (var start = 0; start < entries; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < Math.min(entries, start + 100); i++) {
                    batch.put(keyspace, key(i), value(i));
                    model.put(key(i), value(i));
                }
                ledger.commit(batch);
            }
            ledger.commit(ledger.newBatch().put(MESSAGES, "m1", "first").put(MESSAGES, "m2", "second"));
        }
        return model;
    }

    private static void awaitLoaded(LiveLedger ledger, Keyspace<?, ?> keyspace) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!ledger.residentLoadedForTests(keyspace)) {
            assertTrue(System.nanoTime() < deadline, "the load did not end");
            Thread.sleep(1);
        }
    }

    /// {@return the key below which memory holds every entry, or `null` once it holds them all}
    private static String limit(LiveLedger ledger, Keyspace<?, ?> keyspace) {
        var loader = ledger.state(keyspace).loading;
        var limit = loader == null ? null : loader.limit();
        return limit == null ? null : new String(limit.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    /// Checks every read of `keyspace` that `view` makes against `model`: each key, keys it lacks, counts, and ranges
    /// in both directions, many across the loader's position while it is paused.
    private static void checkReads(View view, Keyspace<String, String> keyspace, TreeMap<String, String> model) {
        for (var entry : model.entrySet()) {
            assertEquals(Optional.of(entry.getValue()), view.get(keyspace, entry.getKey()), entry.getKey());
        }
        var probes = new ArrayList<>(model.keySet().stream().filter(key -> key.hashCode() % 7 == 0).toList());
        probes.addAll(List.of(key(0), key(ENTRIES - 1), "s", key(500) + "a", "zz"));
        for (var probe : probes) {
            assertEquals(Optional.ofNullable(model.get(probe)),
                    view.readEncoded(keyspace, probe, segment -> new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)), probe);
            assertEquals(Optional.ofNullable(model.get(probe)), view.get(keyspace, probe), probe);
        }
        assertEquals(Optional.empty(), view.get(keyspace, key(500) + "a"));
        assertEquals(model.size(), view.size(keyspace));
        assertEquals(entries(model), view.range(keyspace, KeyRange.all(), Integer.MAX_VALUE));
        var descending = entries(model);
        Collections.reverse(descending);
        assertEquals(descending, view.range(keyspace, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
        var bounds = List.of(key(0), key(10), key(31), key(32), key(33), key(95), key(96), key(97), key(200),
                key(201) + "a", key(700), key(ENTRIES - 1), "zz");
        for (var from : bounds) {
            for (var to : bounds) {
                if (from.compareTo(to) >= 0) {
                    continue;
                }
                var expected = entries(model.subMap(from, true, to, false));
                assertEquals(expected, view.range(keyspace, KeyRange.between(from, to), Integer.MAX_VALUE), from + ".." + to);
                var reversed = new ArrayList<>(expected);
                Collections.reverse(reversed);
                assertEquals(reversed, view.range(keyspace, KeyRange.between(from, to).reversed(), Integer.MAX_VALUE));
            }
            assertEquals(entries(model.tailMap(from, true)).stream().limit(5).toList(), view.range(keyspace, KeyRange.atLeast(from), 5));
            var below = entries(model.headMap(from, false));
            Collections.reverse(below);
            assertEquals(below.stream().limit(5).toList(), view.range(keyspace, KeyRange.below(from).reversed(), 5));
            var first = model.ceilingEntry(from);
            assertEquals(first == null ? Optional.empty() : Optional.of(new Entry<>(first.getKey(), first.getValue())),
                    view.first(keyspace, KeyRange.atLeast(from)));
        }
        var visited = new long[1];
        view.forEachEncoded(keyspace, KeyRange.all(), (key, value) -> {
            visited[0] += value.byteSize();
            return true;
        });
        assertEquals(model.values().stream().mapToLong(String::length).sum(), visited[0]);
    }

    private static List<Entry<String, String>> entries(Map<String, String> map) {
        var entries = new ArrayList<Entry<String, String>>(map.size());
        map.forEach((key, value) -> entries.add(new Entry<>(key, value)));
        return entries;
    }

    /// Holds the decoding of `value`, which the loader does in the middle of a run, until the test releases it.
    private record Hold(String value, CountDownLatch reached, CountDownLatch released) {
        Hold(String value) {
            this(value, new CountDownLatch(1), new CountDownLatch(1));
        }

        void hold() {
            reached.countDown();
            try {
                assertTrue(released.await(60, TimeUnit.SECONDS), "never released");
            } catch (InterruptedException exception) {
                throw new IllegalStateException(exception);
            }
        }
    }

    /// Pauses the loader at the steps given, until the test releases it.
    private static final class Pause implements IntConsumer {
        private final List<Integer> steps;
        private final Map<Integer, CountDownLatch> paused;
        private final Map<Integer, CountDownLatch> released;

        Pause(Integer... steps) {
            this.steps = List.of(steps);
            this.paused = new ConcurrentHashMap<>();
            this.released = new ConcurrentHashMap<>();
            for (var step : steps) {
                paused.put(step, new CountDownLatch(1));
                released.put(step, new CountDownLatch(1));
            }
        }

        @Override
        public void accept(int step) {
            if (!steps.contains(step) || paused.get(step).getCount() == 0) {
                return;
            }
            paused.get(step).countDown();
            try {
                assertTrue(released.get(step).await(60, TimeUnit.SECONDS), "never released");
            } catch (InterruptedException exception) {
                throw new IllegalStateException(exception);
            }
        }

        void awaitPaused(int step) throws InterruptedException {
            assertTrue(paused.get(step).await(60, TimeUnit.SECONDS), "never paused at " + step);
        }

        void release(int step) {
            released.get(step).countDown();
        }
    }

    @ParameterizedTest(name = "loader paused at run {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("every read before, during and after the load sees what the store holds")
    void reads(int step, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(step);
            live.hookResidentLoadsForTests(pause);
            assertFalse(live.residentLoadedForTests(SESSIONS));
            // The first use starts the loader
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(step);
            var limit = limit(live, SESSIONS);
            assertNotNull(limit);
            if (step == 0) {
                assertEquals("", limit);
            } else {
                assertTrue(limit.compareTo(key(1)) > 0 && limit.compareTo(key(ENTRIES / 2)) < 0, limit);
            }
            checkReads(ledger, SESSIONS, model);
            // A key the loader has not reached reads an instance of its own
            assertNotSame(ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow(), ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow());
            if (step > 0) {
                assertSame(ledger.get(SESSIONS, key(0)).orElseThrow(), ledger.get(SESSIONS, key(0)).orElseThrow());
            }
            pause.release(step);
            awaitLoaded(live, SESSIONS);
            checkReads(ledger, SESSIONS, model);
            assertSame(ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow(), ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow());
            assertEquals(ENTRIES, live.state(SESSIONS).chains());
        }
    }

    @Test
    @DisplayName("the store opens without loading a resident key space, and loads it at open when asked to")
    void startsAtFirstUse(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var started = new CountDownLatch(1);
            live.hookResidentLoadsForTests(step -> started.countDown());
            // Neither the reads and writes of a paged key space nor a snapshot start it
            ledger.commit(ledger.newBatch().put(MESSAGES, "m3", "third"));
            assertEquals(Optional.of("first"), ledger.get(MESSAGES, "m1"));
            ledger.snapshot().close();
            ledger.checkpoint();
            assertFalse(started.await(100, TimeUnit.MILLISECONDS));
            assertFalse(live.residentLoadedForTests(SESSIONS));
            // A count does, and is exact before it ends
            assertEquals(ENTRIES, ledger.size(SESSIONS));
            assertTrue(started.await(60, TimeUnit.SECONDS));
            awaitLoaded(live, SESSIONS);
            checkReads(ledger, SESSIONS, model);
        }
        try (var ledger = open(directory, true)) {
            var live = (LiveLedger) ledger;
            assertTrue(live.residentLoadedForTests(SESSIONS));
            assertEquals(ENTRIES, live.state(SESSIONS).chains());
            assertSame(ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow(), ledger.get(SESSIONS, key(ENTRIES - 1)).orElseThrow());
            checkReads(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("a write of a key the loader has not reached waits for it, and every count stays exact")
    void writesWait(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            ledger.get(SESSIONS, key(0));
            pause.awaitPaused(1);
            // Neither a key the loader passed nor a paged one waits
            ledger.commit(ledger.newBatch().put(SESSIONS, key(0), "loaded").put(SESSIONS, key(1) + "a", "between").delete(SESSIONS, key(2)));
            model.put(key(0), "loaded");
            model.put(key(1) + "a", "between");
            model.remove(key(2));
            ledger.commit(ledger.newBatch().put(MESSAGES, "m3", "third"));
            assertEquals(model.size(), ledger.size(SESSIONS));
            var put = executor.submit(() -> ledger.commit(ledger.newBatch().put(SESSIONS, key(1_000), "late")
                    .put(SESSIONS, key(1_000) + "a", "new").delete(SESSIONS, key(1_001)).delete(SESSIONS, key(1_001) + "a")));
            var removal = executor.submit(() -> ledger.commit(ledger.newBatch()
                    .deleteRange(SESSIONS, KeyRange.between(key(1_150), key(1_160)))));
            Thread.sleep(200);
            assertFalse(put.isDone());
            assertFalse(removal.isDone());
            // Reads go on meanwhile, and see none of it
            assertEquals(Optional.of(value(1_000)), ledger.get(SESSIONS, key(1_000)));
            assertEquals(Optional.of(value(1_155)), ledger.get(SESSIONS, key(1_155)));
            pause.release(1);
            put.get(60, TimeUnit.SECONDS);
            removal.get(60, TimeUnit.SECONDS);
            model.put(key(1_000), "late");
            model.put(key(1_000) + "a", "new");
            model.remove(key(1_001));
            model.subMap(key(1_150), key(1_160)).clear();
            // A transaction waits for its keys too, holding the roles: the loader needs none
            ledger.update(transaction -> transaction.put(SESSIONS, key(1_100), "transaction").delete(SESSIONS, key(1_101)));
            model.put(key(1_100), "transaction");
            model.remove(key(1_101));
            awaitLoaded(live, SESSIONS);
            checkReads(ledger, SESSIONS, model);
            ledger.checkpoint();
        }
        try (var ledger = open(directory, false)) {
            checkReads(ledger, SESSIONS, model);
        }
    }

    @ParameterizedTest(name = "snapshot taken {0}")
    @ValueSource(strings = {"before the first use", "while the loader is paused"})
    @DisplayName("a snapshot taken before the load ends keeps its state through it, and the writes and checkpoints after")
    void snapshots(String when, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            var early = when.startsWith("before");
            var snapshot = early ? ledger.snapshot() : null;
            // A checkpoint the snapshot's tree predates, so the loader reads a newer one
            ledger.commit(ledger.newBatch().put(MESSAGES, "m3", "third"));
            ledger.checkpoint();
            ledger.get(SESSIONS, key(0));
            pause.awaitPaused(1);
            if (!early) {
                snapshot = ledger.snapshot();
            }
            pause.release(1);
            awaitLoaded(live, SESSIONS);
            var changed = new TreeMap<>(model);
            ledger.commit(ledger.newBatch().put(SESSIONS, key(5), "new").delete(SESSIONS, key(6)).put(SESSIONS, "s99999", "added"));
            ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between(key(700), key(800))));
            changed.put(key(5), "new");
            changed.remove(key(6));
            changed.put("s99999", "added");
            changed.subMap(key(700), key(800)).clear();
            ledger.checkpoint();
            // Enough commits for the maintenance to prune whatever it may
            for (var i = 0; i < 100; i++) {
                ledger.commit(ledger.newBatch().put(MESSAGES, "n" + i, "x"));
            }
            checkReads(snapshot, SESSIONS, model);
            snapshot.close();
            checkReads(ledger, SESSIONS, changed);
        }
    }

    @Test
    @DisplayName("checkpoints while the loader is paused, before it took its tree and between its runs, hold every entry")
    void checkpoints(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(0, 2);
            live.hookResidentLoadsForTests(pause);
            ledger.get(SESSIONS, key(0));
            pause.awaitPaused(0);
            // The tree the loader registered for is replaced before it reads it
            ledger.commit(ledger.newBatch().put(MESSAGES, "m3", "third"));
            ledger.checkpoint();
            pause.release(0);
            pause.awaitPaused(2);
            ledger.commit(ledger.newBatch().put(SESSIONS, key(1), "changed").delete(SESSIONS, key(2)).put(SESSIONS, key(2) + "a", "new"));
            model.put(key(1), "changed");
            model.remove(key(2));
            model.put(key(2) + "a", "new");
            ledger.checkpoint();
            ledger.commit(ledger.newBatch().put(SESSIONS, key(3), "after"));
            model.put(key(3), "after");
            checkReads(ledger, SESSIONS, model);
            pause.release(2);
            awaitLoaded(live, SESSIONS);
            checkReads(ledger, SESSIONS, model);
            ledger.checkpoint();
        }
        try (var ledger = open(directory, false)) {
            checkReads(ledger, SESSIONS, model);
        }
        try (var ledger = open(directory, true)) {
            checkReads(ledger, SESSIONS, model);
        }
        CheckpointTest.verifyPages(directory, true);
    }

    @Test
    @DisplayName("a close stops the loader before its next run, and the next open loads what it did not")
    void closes(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        var ledger = open(directory, false);
        var live = (LiveLedger) ledger;
        var pause = new Pause(2);
        live.hookResidentLoadsForTests(pause);
        ledger.get(SESSIONS, key(0));
        pause.awaitPaused(2);
        ledger.commit(ledger.newBatch().put(SESSIONS, key(0), "durable")).awaitDurable();
        model.put(key(0), "durable");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var waiting = executor.submit(() -> ledger.commit(ledger.newBatch().put(SESSIONS, key(1_000), "never")));
            var closing = executor.submit(ledger::close);
            Thread.sleep(100);
            assertFalse(closing.isDone());
            pause.release(2);
            closing.get(60, TimeUnit.SECONDS);
            var failure = assertThrows(ExecutionException.class, () -> waiting.get(60, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, failure.getCause());
        }
        assertFalse(live.residentLoadedForTests(SESSIONS));
        try (var reopened = open(directory, false)) {
            checkReads(reopened, SESSIONS, model);
        }
    }

    @ParameterizedTest(name = "the log writes the resident key space: {0}")
    @ValueSource(booleans = {true, false})
    @DisplayName("an open that replays the log loads the resident key spaces its commits write, and only those")
    void replays(boolean resident, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        var ledger = open(directory, false);
        var live = (LiveLedger) ledger;
        var pause = new Pause(1);
        live.hookResidentLoadsForTests(pause);
        ledger.get(SESSIONS, key(0));
        pause.awaitPaused(1);
        ledger.commit(ledger.newBatch().put(MESSAGES, "m3", "third")).awaitDurable();
        if (resident) {
            ledger.commit(ledger.newBatch().put(SESSIONS, key(0), "replayed").delete(SESSIONS, key(1))).awaitDurable();
            model.put(key(0), "replayed");
            model.remove(key(1));
        }
        pause.release(1);
        live.closeWithoutCheckpoint();
        try (var reopened = open(directory, false)) {
            var reopenedLive = (LiveLedger) reopened;
            assertEquals(resident, reopenedLive.residentLoadedForTests(SESSIONS));
            assertEquals(Optional.of("third"), reopened.get(MESSAGES, "m3"));
            checkReads(reopened, SESSIONS, model);
            awaitLoaded(reopenedLive, SESSIONS);
            checkReads(reopened, SESSIONS, model);
        }
    }

    private static byte[] filled(int index, int length) {
        var value = new byte[length];
        Arrays.fill(value, (byte) index);
        return value;
    }

    private static Ledger openRaw(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(RAW).loadResidentAtOpen(false).build();
    }

    @Test
    @DisplayName("a byte array value the loader has not reached reads from the tree, and a batch on a rope with a value of its own waits for it")
    void byteValuesAndLargeBatches(@TempDir Path directory) throws Exception {
        // 1 MiB: past the size of an inline value, so the tree holds it apart from its leaf
        var large = filled(1, 1 << 20);
        try (var ledger = openRaw(directory)) {
            for (var start = 0; start < ENTRIES; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < start + 100; i++) {
                    batch.put(RAW, key(i), i == 1_150 ? large : filled(i, 1_400));
                }
                ledger.commit(batch);
            }
        }
        // 640 KiB of puts outgrow a batch's array, and 200,000 bytes take an array of their own at the put
        var ownValue = filled(2, 200_000);
        var written = filled(3, 16_384);
        try (var ledger = openRaw(directory); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            ledger.get(RAW, key(0));
            pause.awaitPaused(1);
            assertArrayEquals(filled(1_000, 1_400), ledger.get(RAW, key(1_000)).orElseThrow());
            assertNotSame(ledger.get(RAW, key(1_000)).orElseThrow(), ledger.get(RAW, key(1_000)).orElseThrow());
            assertArrayEquals(large, ledger.get(RAW, key(1_150)).orElseThrow());
            var batch = ledger.newBatch();
            for (var i = 1_000; i < 1_040; i++) {
                batch.put(RAW, key(i), written);
            }
            batch.put(RAW, key(1_100), ownValue);
            var commit = executor.submit(() -> ledger.commit(batch));
            Thread.sleep(200);
            assertFalse(commit.isDone());
            assertArrayEquals(filled(1_000, 1_400), ledger.get(RAW, key(1_000)).orElseThrow());
            pause.release(1);
            commit.get(60, TimeUnit.SECONDS);
            awaitLoaded(live, RAW);
            assertEquals(ENTRIES, ledger.size(RAW));
            assertSame(ledger.get(RAW, key(1_000)).orElseThrow(), ledger.get(RAW, key(1_000)).orElseThrow());
            assertArrayEquals(written, ledger.get(RAW, key(1_039)).orElseThrow());
            assertArrayEquals(ownValue, ledger.get(RAW, key(1_100)).orElseThrow());
            assertArrayEquals(large, ledger.get(RAW, key(1_150)).orElseThrow());
        }
        try (var ledger = openRaw(directory)) {
            assertEquals(ENTRIES, ledger.size(RAW));
            assertArrayEquals(written, ledger.get(RAW, key(1_000)).orElseThrow());
            assertArrayEquals(ownValue, ledger.get(RAW, key(1_100)).orElseThrow());
            assertArrayEquals(large, ledger.get(RAW, key(1_150)).orElseThrow());
            assertArrayEquals(filled(1_199, 1_400), ledger.get(RAW, key(1_199)).orElseThrow());
        }
    }

    private static Ledger openRawKeys(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(RAW_KEYS).loadResidentAtOpen(false).build();
    }

    private static void assertRawKey(Ledger ledger, byte[] key, byte[] expected) {
        var what = new String(key, StandardCharsets.US_ASCII);
        if (expected == null) {
            assertTrue(ledger.get(RAW_KEYS, key).isEmpty(), what);
            assertTrue(ledger.readEncoded(RAW_KEYS, key, segment -> segment.byteSize()).isEmpty(), what);
        } else {
            assertArrayEquals(expected, ledger.get(RAW_KEYS, key).orElseThrow(), what);
            assertArrayEquals(expected, ledger.readEncoded(RAW_KEYS, key, segment -> segment.toArray(ValueLayout.JAVA_BYTE)).orElseThrow(), what);
        }
    }

    @Test
    @DisplayName("a key looked up by its array reads the tree or memory as the loader's position says, at the position and around it, and a scan keeps its bounds")
    void arrayKeysAcrossThePosition(@TempDir Path directory) throws Exception {
        var model = new TreeMap<String, byte[]>();
        try (var ledger = openRawKeys(directory)) {
            for (var start = 0; start < ENTRIES; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < start + 100; i++) {
                    model.put(key(i), filled(i, 1_400));
                    batch.put(RAW_KEYS, key(i).getBytes(StandardCharsets.US_ASCII), model.get(key(i)));
                }
                ledger.commit(batch);
            }
        }
        try (var ledger = openRawKeys(directory)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            ledger.get(RAW_KEYS, key(0).getBytes(StandardCharsets.US_ASCII));
            pause.awaitPaused(1);
            var limit = limit(live, RAW_KEYS);
            assertNotNull(limit);
            var position = model.ceilingKey(limit);
            assertNotNull(position, "the load is at its end");
            assertFalse(position.equals(model.firstKey()), "the load has not started");
            for (var entry : model.entrySet()) {
                var encoded = entry.getKey().getBytes(StandardCharsets.US_ASCII);
                assertRawKey(ledger, encoded, entry.getValue());
                // An extension sorts right after its key and a prefix right before it, on either side of the position
                assertRawKey(ledger, Arrays.copyOf(encoded, encoded.length + 1), null);
                assertRawKey(ledger, Arrays.copyOf(encoded, encoded.length - 1), null);
            }
            assertRawKey(ledger, limit.getBytes(StandardCharsets.ISO_8859_1), model.get(limit));

            // Over the position: a visitor that changes the arrays of its range changes nothing of the scan
            var index = Integer.parseInt(position.substring(1));
            var low = key(Math.max(0, index - 10));
            var high = key(Math.min(ENTRIES - 1, index + 10));
            var from = low.getBytes(StandardCharsets.US_ASCII);
            var to = high.getBytes(StandardCharsets.US_ASCII);
            var expected = new ArrayList<>(model.subMap(low, high).keySet());
            var seen = new ArrayList<String>();
            ledger.forEach(RAW_KEYS, KeyRange.between(from, to), (key, _) -> {
                seen.add(new String(key, StandardCharsets.US_ASCII));
                Arrays.fill(from, (byte) 0xFF);
                Arrays.fill(to, (byte) 0x00);
                return true;
            });
            assertEquals(expected, seen);

            pause.release(1);
            awaitLoaded(live, RAW_KEYS);
            for (var entry : model.entrySet()) {
                var encoded = entry.getKey().getBytes(StandardCharsets.US_ASCII);
                assertRawKey(ledger, encoded, entry.getValue());
                assertRawKey(ledger, Arrays.copyOf(encoded, encoded.length + 1), null);
                assertRawKey(ledger, Arrays.copyOf(encoded, encoded.length - 1), null);
            }
        }
    }

    @Test
    @DisplayName("a loss of power while the loader runs recovers the durable commits")
    void powerLoss() throws Exception {
        var disk = CrashFileSystem.create();
        var directory = disk.getPath("/store");
        var model = populate(directory, SESSIONS, ENTRIES);
        var ledger = open(directory, false);
        var live = (LiveLedger) ledger;
        var pause = new Pause(1);
        live.hookResidentLoadsForTests(pause);
        ledger.get(SESSIONS, key(0));
        pause.awaitPaused(1);
        ledger.commit(ledger.newBatch().put(SESSIONS, key(0), "durable")).awaitDurable();
        model.put(key(0), "durable");
        var survivor = disk.afterPowerLoss(new Random(7));
        pause.release(1);
        ledger.close();
        try (var reopened = open(survivor.getPath("/store"), false)) {
            checkReads(reopened, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("a load that fails is reported, leaves the reads to the tree, and the next write retries it")
    void failures(@TempDir Path directory) throws Exception {
        var model = populate(directory, FRAGILE, ENTRIES);
        var reports = new LinkedBlockingQueue<LedgerException>();
        REFUSED.set(value(700));
        try (var ledger = open(directory, false, reports::add)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(FRAGILE, key(0)));
            var report = reports.poll(60, TimeUnit.SECONDS);
            assertNotNull(report);
            assertInstanceOf(IllegalStateException.class, report.getCause());
            assertFalse(live.residentLoadedForTests(FRAGILE));
            // Reads go on, from memory below where the loader stopped and from the tree after
            assertEquals(Optional.of(value(10)), ledger.get(FRAGILE, key(10)));
            assertEquals(Optional.of(value(1_100)), ledger.get(FRAGILE, key(1_100)));
            assertThrows(IllegalStateException.class, () -> ledger.get(FRAGILE, key(700)));
            assertEquals(ENTRIES, ledger.size(FRAGILE));
            // A long scan reads on from the tree: the loader it would wait for has stopped
            assertEquals(ENTRIES, ledger.forEachEncoded(FRAGILE, KeyRange.all(), (_, _) -> true));
            // A write of a key the loader did not reach retries the load, and throws an exception of its own thread
            var failed = assertThrows(LedgerCorruptedException.class, () -> ledger.commit(ledger.newBatch().put(FRAGILE, key(900), "x")));
            assertEquals("refused " + value(700), assertInstanceOf(IllegalStateException.class, failed.getCause()).getMessage());
            assertTrue(Arrays.stream(failed.getStackTrace()).anyMatch(frame -> frame.getMethodName().equals("commit")), "no frame of the commit");
            assertNotNull(reports.poll(60, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class,
                    assertThrows(LedgerCorruptedException.class, () -> ledger.update(transaction -> transaction.put(FRAGILE, key(900), "x"))).getCause());
            assertNotNull(reports.poll(60, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class,
                    assertThrows(LedgerCorruptedException.class, () -> ledger.update(transaction -> transaction.deleteRange(FRAGILE, KeyRange.all()))).getCause());
            assertNotNull(reports.poll(60, TimeUnit.SECONDS));
            assertEquals(Optional.of(value(900)), ledger.get(FRAGILE, key(900)));
            REFUSED.set(null);
            ledger.commit(ledger.newBatch().put(FRAGILE, key(900), "x"));
            model.put(key(900), "x");
            awaitLoaded(live, FRAGILE);
            assertEquals(ENTRIES, live.state(FRAGILE).chains());
            checkReads(ledger, FRAGILE, model);
        } finally {
            REFUSED.set(null);
        }
        try (var ledger = open(directory, false)) {
            checkReads(ledger, FRAGILE, model);
        }
    }

    @ParameterizedTest(name = "descending {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a scan ahead of the loader reads memory from where the loader has passed it, and the tree beyond")
    void scanFollowsLoader(boolean descending, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1, 2);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var before = limit(live, SESSIONS);
            var after = new AtomicReference<String>();
            var trigger = descending ? model.lastKey() : model.ceilingKey(before);
            var visited = new ArrayList<Entry<String, String>>();
            var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
            ledger.forEach(SESSIONS, range, (key, value) -> {
                if (key.equals(trigger)) {
                    pause.release(1);
                    awaitPausedQuietly(pause, 2);
                    after.set(limit(live, SESSIONS));
                }
                visited.add(new Entry<>(key, value));
                // Memory gives the instance the store holds, the tree an instance of the read's own
                var inMemory = after.get() == null ? key.compareTo(before) < 0 : !key.equals(trigger) && key.compareTo(after.get()) < 0;
                assertEquals(inMemory, value == ledger.get(SESSIONS, key).orElseThrow(), key);
                return true;
            });
            var expected = entries(model);
            if (descending) {
                Collections.reverse(expected);
            }
            assertEquals(expected, visited);
            pause.release(2);
            awaitLoaded(live, SESSIONS);
        }
    }

    @Test
    @DisplayName("a scan the load ends beside reads memory alone from there")
    void scanOutlivesLoader(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var first = new AtomicBoolean(true);
            var visited = new ArrayList<Entry<String, String>>();
            ledger.forEach(SESSIONS, KeyRange.between(key(600), key(900)), (key, value) -> {
                visited.add(new Entry<>(key, value));
                if (first.getAndSet(false)) {
                    pause.release(1);
                    try {
                        awaitLoaded(live, SESSIONS);
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException(exception);
                    }
                } else {
                    assertSame(ledger.get(SESSIONS, key).orElseThrow(), value, key);
                }
                return true;
            });
            assertEquals(entries(model.subMap(key(600), key(900))), visited);
        }
    }

    @Test
    @DisplayName("a long scan waits for the run the loader is reading rather than read its leaves too")
    void scanWaitsForRun(@TempDir Path directory) throws Exception {
        var model = populate(directory, FRAGILE, ENTRIES);
        // In the fourth run: 16, 32 and 64 leaves of two entries each come before it, and it takes 128
        var held = new Hold(value(400));
        HELD.set(held);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(FRAGILE, key(0)));
            assertTrue(held.reached().await(60, TimeUnit.SECONDS));
            var inMemory = model.headMap(limit(live, FRAGILE)).size();
            assertEquals(224, inMemory);
            var visited = new AtomicInteger();
            var scan = Thread.ofPlatform().start(() -> ledger.forEachEncoded(FRAGILE, KeyRange.all(), (_, _) -> {
                visited.incrementAndGet();
                return true;
            }));
            awaitParked(scan, visited);
            // The eight leaves it reads on its own at most, of two entries each
            assertTrue(visited.get() >= inMemory && visited.get() <= inMemory + 2 * 8, "visited " + visited);
            held.released().countDown();
            scan.join();
            assertEquals(ENTRIES, visited.get());
            awaitLoaded(live, FRAGILE);
        } finally {
            held.released().countDown();
            HELD.set(null);
        }
    }

    /// Waits until `thread` parks and `progress` stays put.
    private static void awaitParked(Thread thread, AtomicInteger progress) throws InterruptedException {
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        var last = -1;
        while (true) {
            assertTrue(thread.isAlive(), "ended without parking, at " + progress);
            assertTrue(System.nanoTime() < deadline, "never parked: " + thread.getState() + " at " + progress);
            var now = progress.get();
            if (thread.getState() == Thread.State.WAITING && now == last) {
                return;
            }
            last = now;
            Thread.sleep(100);
        }
    }

    private static void awaitPausedQuietly(Pause pause, int step) {
        try {
            pause.awaitPaused(step);
        } catch (InterruptedException exception) {
            throw new IllegalStateException(exception);
        }
    }

    @Test
    @DisplayName("a long scan beside the loader makes its next runs read as wide as an open, and in as few runs")
    void scanHurriesLoader(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            var runs = new AtomicInteger();
            live.hookResidentLoadsForTests(step -> {
                runs.accumulateAndGet(step, Math::max);
                pause.accept(step);
            });
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            // Past the leaves a scan reads alone; the loader reads none now, so the scan does not wait
            var seen = new AtomicInteger();
            ledger.forEachEncoded(SESSIONS, KeyRange.atLeast(key(100)), (_, _) -> seen.incrementAndGet() < 40);
            pause.release(1);
            awaitLoaded(live, SESSIONS);
            // Beside the store, without a scan, it takes seven: 16 leaves, then 512, then the 72 left of 600
            assertEquals(3, runs.get());
            assertEquals(ENTRIES, live.state(SESSIONS).chains());
            checkReads(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("a write that waits for the loader makes its next runs read as wide as an open, and in as few runs")
    void writeHurriesLoader(@TempDir Path directory) throws Exception {
        populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            var runs = new AtomicInteger();
            live.hookResidentLoadsForTests(step -> {
                runs.accumulateAndGet(step, Math::max);
                pause.accept(step);
            });
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var write = Thread.ofPlatform().start(() -> ledger.commit(ledger.newBatch().put(SESSIONS, key(ENTRIES - 1), "x")));
            awaitParked(write, new AtomicInteger());
            pause.release(1);
            write.join();
            awaitLoaded(live, SESSIONS);
            assertEquals(3, runs.get());
            assertEquals(Optional.of("x"), ledger.get(SESSIONS, key(ENTRIES - 1)));
        }
    }

    @Test
    @DisplayName("a scan of a transaction passes none of what its visitor writes ahead of it, past where the loader was, while point reads see it")
    void scanSkipsOwnWrites(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var trigger = model.ceilingKey(limit(live, SESSIONS));
            var visited = new ArrayList<Entry<String, String>>();
            var before = entries(model);
            ledger.update(transaction -> transaction.forEach(SESSIONS, KeyRange.all(), (key, value) -> {
                visited.add(new Entry<>(key, value));
                if (key.equals(trigger)) {
                    pause.release(1);
                    transaction.put(SESSIONS, key(300), "own");
                    transaction.delete(SESSIONS, key(301));
                    assertEquals(Optional.of("own"), transaction.get(SESSIONS, key(300)));
                    assertEquals(Optional.empty(), transaction.get(SESSIONS, key(301)));
                }
                return true;
            }));
            model.put(key(300), "own");
            model.remove(key(301));
            assertEquals(before, visited);
            assertEquals(entries(model), ledger.range(SESSIONS, KeyRange.all(), Integer.MAX_VALUE));
        }
    }

    @Test
    @DisplayName("a descending scan beside the loader walks its array, or once a commit created a chain orders the chains and links none, and the first after the load links them, and they stay linked through later writes")
    void linksAfterLoad(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var state = live.state(SESSIONS);
            assertEquals(descending(model), ledger.range(SESSIONS, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
            assertNotNull(state.loadedOrder());
            assertTrue(state.entries.isEmpty());
            // A new key the loader passed: the scans order the chains from here on
            ledger.commit(ledger.newBatch().put(SESSIONS, key(0) + "a", "entered"));
            model.put(key(0) + "a", "entered");
            assertNull(state.loadedOrder());
            assertEquals(descending(model), ledger.range(SESSIONS, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
            assertFalse(state.entries.isEmpty());
            for (var chain : state.entries.values()) {
                assertNull(chain.below());
            }
            pause.release(1);
            awaitLoaded(live, SESSIONS);
            assertEquals(descending(model), ledger.range(SESSIONS, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
            var below = Chain.BOTTOM;
            for (var chain : state.entries.values()) {
                assertSame(below, chain.below());
                below = chain;
            }
            assertEquals(ENTRIES + 1, state.entries.size());
            ledger.commit(ledger.newBatch().put(SESSIONS, key(500) + "a", "entered").delete(SESSIONS, key(501)));
            model.put(key(500) + "a", "entered");
            model.remove(key(501));
            assertEquals(descending(model), ledger.range(SESSIONS, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
        }
    }

    private static List<Entry<String, String>> descending(Map<String, String> model) {
        var descending = entries(model);
        Collections.reverse(descending);
        return descending;
    }

    @Test
    @DisplayName("scans in both directions beside a running loader visit every entry once and in order")
    void scansRaceLoader(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS, ENTRIES);
        var expected = entries(model);
        var reversed = new ArrayList<>(expected);
        Collections.reverse(reversed);
        for (var round = 0; round < 8; round++) {
            try (var ledger = open(directory, false)) {
                var live = (LiveLedger) ledger;
                // A slower loader in some rounds, so the scans meet it at different keys
                var delay = round % 4;
                live.hookResidentLoadsForTests(_ -> {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException exception) {
                        throw new IllegalStateException(exception);
                    }
                });
                var failures = new ConcurrentLinkedQueue<String>();
                var scans = new ArrayList<Thread>();
                for (var scan = 0; scan < 4; scan++) {
                    var descending = scan % 2 == 1;
                    scans.add(Thread.ofPlatform().start(() -> {
                        var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
                        try {
                            for (var i = 0; i < 3; i++) {
                                var visited = ledger.range(SESSIONS, range, Integer.MAX_VALUE);
                                if (!visited.equals(descending ? reversed : expected)) {
                                    failures.add((descending ? "descending" : "ascending") + " scan read " + visited.size() + " entries");
                                }
                            }
                        } catch (RuntimeException exception) {
                            failures.add("scan failed: " + exception);
                        }
                    }));
                }
                for (var scan : scans) {
                    scan.join();
                }
                assertTrue(failures.isEmpty(), failures.size() + " failures, first: " + failures.peek());
                awaitLoaded(live, SESSIONS);
            }
        }
    }

    @Test
    @DisplayName("readers, writers, range removals, snapshots and checkpoints at once while the loader runs lose nothing")
    void stress(@TempDir Path directory) throws Exception {
        // Keys u... no one writes, w<t>... writer t owns, z... the range remover owns
        var model = new TreeMap<String, String>();
        try (var ledger = open(directory, false)) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 3_000; i++) {
                var key = (i % 3 == 0 ? "u" : i % 3 == 1 ? "w" + (i % 4) : "z") + "%05d".formatted(i);
                var value = "v" + i + "-" + "x".repeat(600);
                batch.put(SESSIONS, key, value);
                model.put(key, value);
            }
            ledger.commit(batch);
        }
        for (var round = 0; round < 4; round++) {
            model = round(directory, model, round);
        }
        try (var ledger = open(directory, true)) {
            checkReads(ledger, SESSIONS, model);
        }
    }

    private static TreeMap<String, String> round(Path directory, TreeMap<String, String> initial, int round) throws Exception {
        var untouched = new TreeMap<>(initial.headMap("v"));
        var owned = new ArrayList<TreeMap<String, String>>();
        for (var writer = 0; writer < 4; writer++) {
            var prefix = "w" + writer;
            owned.add(new TreeMap<>(initial.subMap(prefix, prefix + "￿")));
        }
        var ranged = new TreeMap<>(initial.tailMap("z"));
        var failures = new ConcurrentLinkedQueue<String>();
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            // A slow loader, so the threads below run beside it
            live.hookResidentLoadsForTests(_ -> {
                try {
                    Thread.sleep(40);
                } catch (InterruptedException exception) {
                    throw new IllegalStateException(exception);
                }
            });
            var stop = new AtomicBoolean();
            var threads = new ArrayList<Thread>();
            for (var writer = 0; writer < 4; writer++) {
                var mine = owned.get(writer);
                var prefix = "w" + writer;
                var seed = round * 31 + writer;
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = new Random(seed);
                    for (var step = 0; step < 300; step++) {
                        var key = prefix + "%05d".formatted(random.nextInt(3_000));
                        try {
                            switch (random.nextInt(4)) {
                                case 0 -> {
                                    ledger.commit(ledger.newBatch().delete(SESSIONS, key));
                                    mine.remove(key);
                                }
                                case 1 -> {
                                    ledger.update(transaction -> transaction.put(SESSIONS, key, "t" + seed));
                                    mine.put(key, "t" + seed);
                                }
                                default -> {
                                    var value = "p" + step + "-" + seed;
                                    ledger.commit(ledger.newBatch().put(SESSIONS, key, value).put(MESSAGES, key, value));
                                    mine.put(key, value);
                                }
                            }
                            var read = ledger.get(SESSIONS, key);
                            if (!read.equals(Optional.ofNullable(mine.get(key)))) {
                                failures.add("writer read " + key + " as " + read);
                            }
                        } catch (RuntimeException exception) {
                            failures.add("writer failed: " + exception);
                        }
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                var random = new Random(round);
                for (var step = 0; step < 60; step++) {
                    var from = "z%05d".formatted(random.nextInt(3_000));
                    var to = "z%05d".formatted(random.nextInt(3_000));
                    try {
                        if (from.compareTo(to) < 0 && random.nextBoolean()) {
                            ledger.commit(ledger.newBatch().deleteRange(SESSIONS, KeyRange.between(from, to)));
                            ranged.subMap(from, to).clear();
                        } else {
                            ledger.commit(ledger.newBatch().put(SESSIONS, from, "r" + step));
                            ranged.put(from, "r" + step);
                        }
                        var range = ledger.range(SESSIONS, KeyRange.between("z", "z￿"), Integer.MAX_VALUE);
                        if (!range.equals(entries(ranged))) {
                            failures.add("range remover read " + range.size() + " entries where it holds " + ranged.size());
                        }
                    } catch (RuntimeException exception) {
                        failures.add("range remover failed: " + exception);
                    }
                }
            }));
            for (var reader = 0; reader < 2; reader++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = ThreadLocalRandom.current();
                    var keys = new ArrayList<>(untouched.keySet());
                    while (!stop.get()) {
                        try (var snapshot = ledger.snapshot()) {
                            var key = keys.get(random.nextInt(keys.size()));
                            if (!snapshot.get(SESSIONS, key).equals(Optional.of(untouched.get(key)))) {
                                failures.add("snapshot read " + key + " wrong");
                            }
                            var from = keys.get(random.nextInt(keys.size()));
                            var page = ledger.range(SESSIONS, KeyRange.between(from, "v"), 40);
                            var expected = entries(untouched.tailMap(from, true)).stream().limit(40).toList();
                            if (!page.equals(expected)) {
                                failures.add("range from " + from + " read " + page.size() + " entries");
                            }
                            var size = snapshot.size(SESSIONS);
                            var counted = snapshot.forEach(SESSIONS, KeyRange.all(), (_, _) -> true);
                            if (size != counted) {
                                failures.add("snapshot counted " + size + " but visited " + counted);
                            }
                            if (!snapshot.get(SESSIONS, key).equals(Optional.of(untouched.get(key)))) {
                                failures.add("snapshot read " + key + " wrong the second time");
                            }
                        } catch (RuntimeException exception) {
                            failures.add("reader failed: " + exception);
                        }
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    try {
                        ledger.checkpoint();
                        Thread.sleep(20);
                    } catch (RuntimeException exception) {
                        failures.add("checkpoint failed: " + exception);
                    } catch (InterruptedException exception) {
                        return;
                    }
                }
            }));
            // The writers and the range remover end on their own
            for (var i = 0; i < 5; i++) {
                threads.get(i).join();
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            assertTrue(failures.isEmpty(), failures.size() + " failures, first: " + failures.peek());
            awaitLoaded(live, SESSIONS);
            var expected = new TreeMap<>(untouched);
            for (var mine : owned) {
                expected.putAll(mine);
            }
            expected.putAll(ranged);
            checkReads(ledger, SESSIONS, expected);
            ledger.checkpoint();
            return expected;
        }
    }
}
