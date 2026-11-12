package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.View;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The array of the chains a resident key space's loader installed, which its scans walk while those are its only
/// chains: the staging, publication and retries of the loader on a bare key space, then reads, snapshots, transactions,
/// failed loads and commits of new keys racing scans on a store that opened without loading. Values of 1,400 bytes put
/// two entries in a leaf, so 1,200 entries take the loader several runs.
class LoadedOrderTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> SESSIONS = Keyspace.resident("sessions", KeyCodec.utf8(), ValueCodec.utf8());
    // The value whose decoding fails, or null
    private static final AtomicReference<String> REFUSED = new AtomicReference<>();
    private static final Keyspace<String, String> FRAGILE = Keyspace.resident("fragile", KeyCodec.utf8(), ValueCodec.of(
            String::length,
            (value, target) -> MemorySegment.copy(value.getBytes(StandardCharsets.US_ASCII), 0, target, ValueLayout.JAVA_BYTE, 0, value.length()),
            source -> {
                var value = new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII);
                if (value.equals(REFUSED.get())) {
                    throw new IllegalStateException("refused " + value);
                }
                return value;
            }));
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
                .keyspace(SESSIONS).keyspace(FRAGILE)
                .loadResidentAtOpen(atOpen).onBackgroundFailure(failures).build();
    }

    private static TreeMap<String, String> populate(Path directory, Keyspace<String, String> keyspace) {
        var model = new TreeMap<String, String>();
        try (var ledger = open(directory, false)) {
            for (var start = 0; start < ENTRIES; start += 100) {
                var batch = ledger.newBatch();
                for (var i = start; i < Math.min(ENTRIES, start + 100); i++) {
                    batch.put(keyspace, key(i), value(i));
                    model.put(key(i), value(i));
                }
                ledger.commit(batch);
            }
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

    private static List<Entry<String, String>> entries(Map<String, String> map) {
        var entries = new ArrayList<Entry<String, String>>(map.size());
        map.forEach((key, value) -> entries.add(new Entry<>(key, value)));
        return entries;
    }

    private static List<Entry<String, String>> reversed(List<Entry<String, String>> entries) {
        var reversed = new ArrayList<>(entries);
        Collections.reverse(reversed);
        return reversed;
    }

    /// Checks the scans of `keyspace` that `view` makes against `model`, whole and across bounds, in both directions.
    private static void checkScans(View view, Keyspace<String, String> keyspace, TreeMap<String, String> model) {
        assertEquals(entries(model), view.range(keyspace, KeyRange.all(), Integer.MAX_VALUE));
        assertEquals(reversed(entries(model)), view.range(keyspace, KeyRange.<String>all().reversed(), Integer.MAX_VALUE));
        var bounds = List.of("a", key(0), key(1), key(31), key(32), key(96), key(150) + "a", key(600), key(ENTRIES - 1), "zz");
        for (var from : bounds) {
            for (var to : bounds) {
                if (from.compareTo(to) >= 0) {
                    continue;
                }
                var expected = entries(model.subMap(from, true, to, false));
                assertEquals(expected, view.range(keyspace, KeyRange.between(from, to), Integer.MAX_VALUE), from + ".." + to);
                assertEquals(reversed(expected), view.range(keyspace, KeyRange.between(from, to).reversed(), Integer.MAX_VALUE),
                        to + ".." + from);
            }
            assertEquals(entries(model.tailMap(from, true)).stream().limit(3).toList(), view.range(keyspace, KeyRange.atLeast(from), 3));
            assertEquals(reversed(entries(model.headMap(from, false))).stream().limit(3).toList(),
                    view.range(keyspace, KeyRange.below(from).reversed(), 3));
        }
        var visited = new AtomicInteger();
        view.forEach(keyspace, KeyRange.all(), (_, _) -> visited.incrementAndGet() > 0);
        assertEquals(model.size(), visited.get());
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

    @Nested
    @DisplayName("on a bare key space")
    class Bare {
        private static final long STAMP = Reads.stamp(1, 0);

        private static MemorySegment chainKey(int index) {
            return Segments.encodeKey(KeyCodec.utf8(), "k%04d".formatted(index));
        }

        private static KeyspaceState loading() {
            var state = new KeyspaceState(SESSIONS);
            state.lazy = true;
            state.keepLoadedOrder();
            state.restageLoadedOrder();
            return state;
        }

        private static void install(KeyspaceState state, int from, int to) {
            for (var i = from; i < to; i++) {
                state.install(chainKey(i), new Version(STAMP, ("v" + i).getBytes(StandardCharsets.UTF_8), null, null));
            }
        }

        private static List<String> keys(KeyspaceState.Walk walk) {
            var keys = new ArrayList<String>();
            while (walk.next()) {
                keys.add(new String(walk.key().toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
            }
            return keys;
        }

        private static List<String> expected(int from, int to) {
            var keys = new ArrayList<String>();
            for (var i = from; i < to; i++) {
                keys.add("k%04d".formatted(i));
            }
            return keys;
        }

        @Test
        @DisplayName("keeps no array in a key space that is not resident, holds chains, or orders eagerly")
        void gated() {
            var paged = new KeyspaceState(Keyspace.paged("p", KeyCodec.utf8(), ValueCodec.utf8()));
            paged.lazy = true;
            paged.keepLoadedOrder();
            assertNull(paged.loadedOrder());
            var eager = new KeyspaceState(SESSIONS);
            eager.keepLoadedOrder();
            assertNull(eager.loadedOrder());
            var written = new KeyspaceState(SESSIONS);
            written.lazy = true;
            written.open(chainKey(0));
            written.keepLoadedOrder();
            assertNull(written.loadedOrder());
        }

        @Test
        @DisplayName("publishes the staged chains only when the loader does, and its walks never order the chains")
        void publication() {
            var state = loading();
            install(state, 0, 50);
            assertEquals(0, state.loadedOrder().count());
            assertEquals(List.of(), keys(state.walk(null, null, false)));
            state.publishLoadedOrder(false);
            assertEquals(50, state.loadedOrder().count());
            assertEquals(expected(0, 50), keys(state.walk(null, null, false)));
            assertEquals(expected(0, 50).reversed(), keys(state.walk(null, null, true)));
            assertEquals(expected(10, 20), keys(state.walk(chainKey(10), chainKey(20), false)));
            assertEquals(expected(10, 20).reversed(), keys(state.walk(chainKey(10), chainKey(20), true)));
            var between = Segments.encodeKey(KeyCodec.utf8(), "k0009a");
            assertEquals(expected(10, 50), keys(state.walk(between, null, false)));
            assertEquals(expected(0, 10).reversed(), keys(state.walk(null, between, true)));
            assertEquals(List.of(), keys(state.walk(chainKey(20), chainKey(10), false)));
            assertEquals(List.of(), keys(state.walk(chainKey(60), null, true)));
            assertEquals(0, state.entries.size());
            install(state, 50, 200);
            state.publishLoadedOrder(true);
            assertEquals(200, state.loadedOrder().count());
            assertEquals(expected(0, 200), keys(state.walk(null, null, false)));
            assertEquals(0, state.entries.size());
        }

        @Test
        @DisplayName("an attempt after one that failed stages again the chains it installed and never published, with no gap and no key twice")
        void retry() {
            var state = loading();
            install(state, 0, 50);
            state.publishLoadedOrder(false);
            // Staged past the array's 64 slots, on a copy, and never published
            install(state, 50, 90);
            assertEquals(50, state.loadedOrder().count());
            state.restageLoadedOrder();
            install(state, 50, 120);
            state.publishLoadedOrder(false);
            assertEquals(expected(0, 120), keys(state.walk(null, null, false)));
            state.restageLoadedOrder();
            install(state, 120, 130);
            state.publishLoadedOrder(true);
            assertEquals(130, state.loadedOrder().count());
            assertEquals(expected(0, 130), keys(state.walk(null, null, false)));
            assertEquals(expected(0, 130).reversed(), keys(state.walk(null, null, true)));
            assertEquals(130, state.chains());
        }

        @Test
        @DisplayName("a chain a commit creates drops the array for good, and the walks order every chain from then on")
        void creationDrops() {
            var state = loading();
            install(state, 0, 50);
            state.publishLoadedOrder(false);
            var held = state.walk(null, null, false);
            // A loaded key keeps the array
            state.open(chainKey(10));
            assertNotNull(state.loadedOrder());
            state.open(Segments.encodeKey(KeyCodec.utf8(), "k0010a"));
            assertNull(state.loadedOrder());
            // A walk that started before the commit keeps its array
            assertEquals(expected(0, 50), keys(held));
            var withNew = new ArrayList<>(expected(0, 50));
            withNew.add(11, "k0010a");
            assertEquals(withNew, keys(state.walk(null, null, false)));
            install(state, 50, 60);
            state.publishLoadedOrder(false);
            assertNull(state.loadedOrder());
            state.publishLoadedOrder(true);
            assertNull(state.loadedOrder());
            withNew.addAll(expected(50, 60));
            assertEquals(withNew, keys(state.walk(null, null, false)));
            assertEquals(withNew.reversed(), keys(state.walk(null, null, true)));
        }

        @ParameterizedTest(name = "descending: {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("a chain dropped during a walk of the array drops it, stays on the walk emptied, and leaves the walks after it")
        void dropDuringWalk(boolean descending) {
            var state = loading();
            install(state, 0, 50);
            state.publishLoadedOrder(true);
            var walk = state.walk(null, null, descending);
            assertTrue(walk.next());
            for (var i = 20; i < 30; i++) {
                var chain = state.chain(chainKey(i));
                assertTrue(state.drop(chain, chain.head, () -> 0L));
            }
            assertNull(state.loadedOrder());
            var seen = new ArrayList<String>();
            var emptied = 0;
            do {
                var version = KeyspaceState.resolve(walk.chain().head, walk.key(), Long.MAX_VALUE, RangeRemoval.NONE);
                if (version == null) {
                    emptied++;
                } else {
                    seen.add(new String(walk.key().toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
                }
            } while (walk.next());
            assertEquals(10, emptied);
            var kept = new ArrayList<>(expected(0, 20));
            kept.addAll(expected(30, 50));
            assertEquals(descending ? kept.reversed() : kept, seen);
            assertEquals(descending ? kept.reversed() : kept, keys(state.walk(null, null, descending)));
        }
    }

    @ParameterizedTest(name = "loader paused at run {0}")
    @ValueSource(ints = {0, 1, 2})
    @DisplayName("scans beside the loader and after it walk its array, and never order a chain")
    void scansBesideLoader(int step, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(step);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(step);
            var state = live.state(SESSIONS);
            checkScans(ledger, SESSIONS, model);
            assertNotNull(state.loadedOrder());
            pause.release(step);
            awaitLoaded(live, SESSIONS);
            checkScans(ledger, SESSIONS, model);
            assertEquals(ENTRIES, state.loadedOrder().count());
            assertEquals(0, state.entries.size());
            // An overwrite of loaded keys keeps it
            ledger.commit(ledger.newBatch().put(SESSIONS, key(10), "over").put(SESSIONS, key(ENTRIES - 1), "last"));
            model.put(key(10), "over");
            model.put(key(ENTRIES - 1), "last");
            assertNotNull(state.loadedOrder());
            checkScans(ledger, SESSIONS, model);
            assertEquals(0, state.entries.size());
            // A removal, whose chain a prune may drop, and the key written again
            ledger.commit(ledger.newBatch().delete(SESSIONS, key(11)));
            model.remove(key(11));
            checkScans(ledger, SESSIONS, model);
            ledger.commit(ledger.newBatch().put(SESSIONS, key(11), "back"));
            model.put(key(11), "back");
            checkScans(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("a store that loads while it opens scans the array from the start, and orders the chains once a commit adds a key")
    void loadedAtOpen(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, true)) {
            var live = (LiveLedger) ledger;
            var state = live.state(SESSIONS);
            assertEquals(ENTRIES, state.loadedOrder().count());
            checkScans(ledger, SESSIONS, model);
            assertEquals(0, state.entries.size());
            ledger.commit(ledger.newBatch().put(SESSIONS, key(600) + "a", "new").deleteRange(SESSIONS, KeyRange.between(key(700), key(710))));
            model.put(key(600) + "a", "new");
            model.subMap(key(700), key(710)).clear();
            assertNull(state.loadedOrder());
            checkScans(ledger, SESSIONS, model);
        }
    }

    @ParameterizedTest(name = "descending: {0}")
    @ValueSource(booleans = {false, true})
    @DisplayName("a snapshot taken before a commit of new keys scans what it saw, after the commit drops the array")
    void snapshotBeforeNewKey(boolean descending, @TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            awaitLoaded(live, SESSIONS);
            var state = live.state(SESSIONS);
            var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
            try (var snapshot = ledger.snapshot()) {
                var before = new TreeMap<>(model);
                ledger.commit(ledger.newBatch().delete(SESSIONS, key(5)));
                model.remove(key(5));
                // A snapshot after the removal, on the array still
                try (var removed = ledger.snapshot()) {
                    assertNotNull(state.loadedOrder());
                    ledger.commit(ledger.newBatch().put(SESSIONS, key(5) + "a", "new").put(SESSIONS, "a", "first").put(SESSIONS, key(7), "over"));
                    assertNull(state.loadedOrder());
                    var expected = descending ? reversed(entries(before)) : entries(before);
                    assertEquals(expected, snapshot.range(SESSIONS, range, Integer.MAX_VALUE));
                    checkScans(snapshot, SESSIONS, before);
                    checkScans(removed, SESSIONS, model);
                }
                model.put(key(5) + "a", "new");
                model.put("a", "first");
                model.put(key(7), "over");
                checkScans(ledger, SESSIONS, model);
            }
        }
    }

    @Test
    @DisplayName("a commit of a new key the loader passed drops the array, and the scans beside the loader and after it see the key")
    void newKeyBesideLoader(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            var pause = new Pause(1);
            live.hookResidentLoadsForTests(pause);
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            pause.awaitPaused(1);
            var state = live.state(SESSIONS);
            try (var snapshot = ledger.snapshot()) {
                var before = new TreeMap<>(model);
                ledger.commit(ledger.newBatch().put(SESSIONS, key(2) + "a", "new").put(SESSIONS, "a", "first"));
                model.put(key(2) + "a", "new");
                model.put("a", "first");
                assertNull(state.loadedOrder());
                checkScans(ledger, SESSIONS, model);
                checkScans(snapshot, SESSIONS, before);
                pause.release(1);
                awaitLoaded(live, SESSIONS);
                assertNull(state.loadedOrder());
                checkScans(ledger, SESSIONS, model);
                checkScans(snapshot, SESSIONS, before);
            }
        }
    }

    @Test
    @DisplayName("a load that fails mid-run and its retry leave an array with every key once, in order")
    void retryAfterFailure(@TempDir Path directory) throws Exception {
        var model = populate(directory, FRAGILE);
        var failures = new ConcurrentLinkedQueue<LedgerException>();
        REFUSED.set(value(150));
        try (var ledger = open(directory, false, failures::add)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(FRAGILE, key(0)));
            var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (failures.isEmpty()) {
                assertTrue(System.nanoTime() < deadline, "the load did not fail");
                Thread.sleep(1);
            }
            var state = live.state(FRAGILE);
            var published = state.loadedOrder().count();
            assertTrue(published > 0 && published < 150, published + " chains published");
            REFUSED.set(null);
            checkScans(ledger, FRAGILE, model);
            state.awaitLoaded(null);
            awaitLoaded(live, FRAGILE);
            assertEquals(ENTRIES, state.loadedOrder().count());
            var walk = state.walk(null, null, false);
            var keys = new ArrayList<String>();
            while (walk.next()) {
                keys.add(new String(walk.key().toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8));
            }
            assertEquals(new ArrayList<>(model.keySet()), keys);
            checkScans(ledger, FRAGILE, model);
            assertEquals(0, state.entries.size());
        } finally {
            REFUSED.set(null);
        }
    }

    @Test
    @DisplayName("a transaction's scan sees its earlier new key, and none of the keys its visitor adds or removes, after the load")
    void transactionScans(@TempDir Path directory) throws Exception {
        var model = populate(directory, SESSIONS);
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            awaitLoaded(live, SESSIONS);
            var state = live.state(SESSIONS);
            var before = entries(model);
            var visited = new ArrayList<Entry<String, String>>();
            ledger.update(transaction -> transaction.forEach(SESSIONS, KeyRange.all(), (key, value) -> {
                visited.add(new Entry<>(key, value));
                if (key.equals(key(100))) {
                    transaction.put(SESSIONS, key(600) + "a", "ahead");
                    transaction.delete(SESSIONS, key(700));
                    transaction.put(SESSIONS, key(800), "over");
                }
                return true;
            }));
            assertEquals(before, visited);
            assertNull(state.loadedOrder());
            model.put(key(600) + "a", "ahead");
            model.remove(key(700));
            model.put(key(800), "over");
            checkScans(ledger, SESSIONS, model);
        }
        try (var ledger = open(directory, false)) {
            var live = (LiveLedger) ledger;
            assertEquals(Optional.of(value(0)), ledger.get(SESSIONS, key(0)));
            awaitLoaded(live, SESSIONS);
            var state = live.state(SESSIONS);
            assertNotNull(state.loadedOrder());
            var visited = new ArrayList<Entry<String, String>>();
            ledger.update(transaction -> {
                transaction.put(SESSIONS, key(3) + "a", "own");
                transaction.forEach(SESSIONS, KeyRange.<String>all().reversed(), (key, value) -> visited.add(new Entry<>(key, value)));
            });
            model.put(key(3) + "a", "own");
            assertEquals(reversed(entries(model)), visited);
            checkScans(ledger, SESSIONS, model);
        }
    }

    @Test
    @DisplayName("commits of new keys racing scans beside the loader and after it: every scan passes each key it sees once, in order")
    void newKeysRacingScans(@TempDir Path directory) throws Exception {
        populate(directory, SESSIONS);
        for (var round = 0; round < 3; round++) {
            var prefix = "n" + round + "-";
            try (var ledger = open(directory, false); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                // Counted before each commit starts and after it returns: a commit a snapshot sees is between the two
                var started = new AtomicInteger();
                var committed = new AtomicInteger();
                var done = new AtomicBoolean();
                var futures = new ArrayList<Future<?>>();
                futures.add(executor.submit(() -> {
                    try {
                        for (var i = 0; i < 300; i++) {
                            // A new key among the loaded ones, and an overwrite of a loaded one
                            var batch = ledger.newBatch().put(SESSIONS, key(i * 4) + prefix + i, "new")
                                    .put(SESSIONS, key(i * 3), "over" + i);
                            started.incrementAndGet();
                            ledger.commit(batch);
                            committed.incrementAndGet();
                        }
                    } finally {
                        done.set(true);
                    }
                    return null;
                }));
                for (var scanner = 0; scanner < 3; scanner++) {
                    var descending = scanner % 2 == 1;
                    futures.add(executor.submit(() -> {
                        var range = descending ? KeyRange.<String>all().reversed() : KeyRange.<String>all();
                        do {
                            var low = committed.get();
                            try (var snapshot = ledger.snapshot()) {
                                var high = started.get();
                                var entries = snapshot.range(SESSIONS, range, Integer.MAX_VALUE);
                                var added = 0;
                                var loaded = 0;
                                for (var i = 0; i < entries.size(); i++) {
                                    var key = entries.get(i).key();
                                    if (i > 0) {
                                        var order = entries.get(i - 1).key().compareTo(key);
                                        assertTrue(descending ? order > 0 : order < 0, "out of order at " + key);
                                    }
                                    if (key.contains(prefix)) {
                                        added++;
                                    } else if (key.length() == key(0).length()) {
                                        loaded++;
                                    }
                                }
                                assertEquals(ENTRIES, loaded);
                                assertTrue(added >= low && added <= high, added + " new keys, " + low + ".." + high + " committed");
                                assertEquals(entries.size(), snapshot.size(SESSIONS));
                            }
                        } while (!done.get());
                        return null;
                    }));
                }
                for (var future : futures) {
                    future.get(120, TimeUnit.SECONDS);
                }
            }
        }
    }
}
