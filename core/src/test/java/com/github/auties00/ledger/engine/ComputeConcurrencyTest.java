package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Computed writes from many threads, platform and virtual: no update is ever lost, a function runs at most twice, and
/// every history is linearizable per key, the value each kept run received being what the write before it in the order
/// of the commits left, among blind puts and removals, range removals, transactions, open snapshots and checkpoints
/// that drop chains and swap trees as the threads run.
class ComputeConcurrencyTest {
    private static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.utf8());

    static Stream<Arguments> cases() {
        var cases = new ArrayList<Arguments>();
        for (var store : List.of("memory", "directory")) {
            for (var virtual : List.of(false, true)) {
                for (var keyspace : List.of(PAGED, RESIDENT)) {
                    cases.add(Arguments.of(store, virtual, keyspace));
                }
            }
        }
        return cases.stream();
    }

    private static Ledger open(String store, Path directory) {
        var options = store.equals("memory") ? Ledger.builder().memory()
                : ((LedgerBuilder) Ledger.builder().directory(directory).encryption(Encryption.none()))
                .segmentSize(1 << 18).checkpointEvery(1 << 20);
        return options.keyspace(PAGED).keyspace(RESIDENT).build();
    }

    private static ThreadFactory threads(boolean virtual) {
        return virtual ? Thread.ofVirtual().factory() : Thread.ofPlatform().factory();
    }

    /// Increments the counter in `value`, `count|writer`, or starts one.
    private static String increment(String value, int writer) {
        var count = value == null ? 0 : Long.parseLong(value.substring(0, value.indexOf('|')));
        return (count + 1) + "|" + writer;
    }

    @ParameterizedTest(name = "{0}, virtual: {1}, {2}")
    @MethodSource("cases")
    @DisplayName("increments of one key from many threads are never lost, each function running at most twice")
    void hotKey(String store, boolean virtual, Keyspace<String, String> keyspace, @TempDir Path directory) throws Exception {
        var writers = 16;
        var increments = 500;
        var maxRuns = new AtomicInteger();
        try (var ledger = open(store, directory);
             var executor = Executors.newThreadPerTaskExecutor(threads(virtual))) {
            var futures = new ArrayList<Future<?>>();
            for (var writer = 0; writer < writers; writer++) {
                var id = writer;
                futures.add(executor.submit(() -> {
                    for (var i = 0; i < increments; i++) {
                        var runs = new AtomicInteger();
                        if (i % 2 == 0) {
                            ledger.compute(keyspace, "hot", value -> {
                                runs.incrementAndGet();
                                return increment(value, id);
                            });
                        } else {
                            // In a batch beside a write of a key of its own
                            var batch = ledger.newBatch().put(keyspace, "own" + id, String.valueOf(i));
                            batch.compute(keyspace, "hot", value -> {
                                runs.incrementAndGet();
                                return increment(value, id);
                            });
                            ledger.commit(batch);
                        }
                        maxRuns.accumulateAndGet(runs.get(), Math::max);
                    }
                    return null;
                }));
            }
            for (var future : futures) {
                future.get();
            }
            var value = ledger.get(keyspace, "hot").orElseThrow();
            assertEquals(writers * increments, Long.parseLong(value.substring(0, value.indexOf('|'))));
            assertTrue(maxRuns.get() <= 2, "a function ran " + maxRuns.get() + " times");
        }
    }

    /// One write of a key, as the history check orders them.
    ///
    /// @param received what a computed write's kept run received; ignored for a blind write
    /// @param read whether the write left the key unchanged, so it took effect at the sequence it read at
    private record Event(String key, long sequence, boolean computed, boolean read, String received, String left) {
    }

    @ParameterizedTest(name = "{0}, virtual: {1}, {2}")
    @MethodSource("cases")
    @DisplayName("every key's history is linearizable among computes, blind writes, range removals, transactions and checkpoints")
    void linearizable(String store, boolean virtual, Keyspace<String, String> keyspace, @TempDir Path directory) throws Exception {
        var keys = 64;
        var writers = 12;
        var steps = 800;
        var events = new ConcurrentLinkedQueue<Event>();
        var maxRuns = new AtomicInteger();
        var stop = new java.util.concurrent.atomic.AtomicBoolean();
        try (var ledger = open(store, directory);
             var executor = Executors.newThreadPerTaskExecutor(threads(virtual))) {
            // A reader that keeps snapshots open, whose reads stay what they were
            var reader = executor.submit(() -> {
                var random = new Random(99);
                var reads = 0;
                while (!stop.get()) {
                    // Checkpoints drop the chains the commits wrote, and swap the trees under the reads
                    if (store.equals("directory") && ++reads % 200 == 0) {
                        ledger.checkpoint();
                    }
                    try (var snapshot = ledger.snapshot()) {
                        var key = "k" + random.nextInt(keys);
                        var before = snapshot.get(keyspace, key);
                        Thread.yield();
                        assertEquals(before, snapshot.get(keyspace, key));
                    }
                }
                return null;
            });
            var futures = new ArrayList<Future<?>>();
            for (var writer = 0; writer < writers; writer++) {
                var id = writer;
                futures.add(executor.submit(() -> {
                    var random = new Random(id * 31L + 7);
                    for (var step = 0; step < steps; step++) {
                        // Skewed toward the first keys, as a few chats get most messages
                        var key = "k" + Math.min(keys - 1, (int) Math.abs(random.nextGaussian() * keys / 4));
                        var choice = random.nextInt(100);
                        if (choice < 55) {
                            var runs = new AtomicInteger();
                            var unchanged = random.nextInt(8) == 0;
                            var outcome = ledger.compute(keyspace, key, value -> {
                                runs.incrementAndGet();
                                return unchanged ? value : increment(value, id);
                            });
                            maxRuns.accumulateAndGet(runs.get(), Math::max);
                            events.add(new Event(key, outcome.sequence(), true, !outcome.changed(),
                                    outcome.previous().orElse(null), outcome.value().orElse(null)));
                        } else if (choice < 75) {
                            // Two computes and a blind write of other keys, atomically
                            var other = "k" + random.nextInt(keys);
                            var blind = "k" + random.nextInt(keys);
                            if (other.equals(key) || blind.equals(key) || blind.equals(other)) {
                                continue;
                            }
                            var batch = ledger.newBatch();
                            var runs = new AtomicInteger();
                            var otherRuns = new AtomicInteger();
                            var first = batch.compute(keyspace, key, value -> {
                                runs.incrementAndGet();
                                return increment(value, id);
                            });
                            var second = batch.compute(keyspace, other, value -> {
                                otherRuns.incrementAndGet();
                                return value == null ? null : increment(value, id);
                            });
                            var blindValue = "0|b" + id;
                            batch.put(keyspace, blind, blindValue);
                            Commit commit = ledger.commit(batch);
                            maxRuns.accumulateAndGet(Math.max(runs.get(), otherRuns.get()), Math::max);
                            events.add(new Event(key, commit.sequence(), true, false, first.previous().orElse(null), first.value().orElse(null)));
                            events.add(new Event(other, commit.sequence(), true, false, second.previous().orElse(null), second.value().orElse(null)));
                            events.add(new Event(blind, commit.sequence(), false, false, null, blindValue));
                        } else if (choice < 88) {
                            var value = random.nextBoolean() ? "0|p" + id : null;
                            var commit = ledger.commit(value != null
                                    ? ledger.newBatch().put(keyspace, key, value)
                                    : ledger.newBatch().delete(keyspace, key));
                            events.add(new Event(key, commit.sequence(), false, false, null, value));
                        } else if (choice < 92) {
                            var from = random.nextInt(keys);
                            var to = from + 1 + random.nextInt(6);
                            var commit = ledger.commit(ledger.newBatch().deleteRange(keyspace, KeyRange.between("k" + from, "k" + to)));
                            for (var k = 0; k < keys; k++) {
                                var name = "k" + k;
                                if (name.compareTo("k" + from) >= 0 && name.compareTo("k" + to) < 0) {
                                    events.add(new Event(name, commit.sequence(), false, false, null, null));
                                }
                            }
                        } else {
                            var received = new String[1];
                            var left = new String[1];
                            var commit = ledger.update(transaction -> {
                                received[0] = transaction.get(keyspace, key).orElse(null);
                                left[0] = increment(received[0], id);
                                transaction.put(keyspace, key, left[0]);
                            });
                            events.add(new Event(key, commit.sequence(), true, false, received[0], left[0]));
                        }
                    }
                    return null;
                }));
            }
            for (var future : futures) {
                future.get();
            }
            stop.set(true);
            reader.get();
            check(events, ledger, keyspace);
            assertTrue(maxRuns.get() <= 2, "a function ran " + maxRuns.get() + " times");
        }
    }

    /// Replays the history of each key in the order of the commits and fails at the first computed write whose kept run
    /// received anything but what the write before it left.
    private static void check(ConcurrentLinkedQueue<Event> recorded, Ledger ledger, Keyspace<String, String> keyspace) {
        var byKey = new HashMap<String, List<Event>>();
        for (var event : recorded) {
            byKey.computeIfAbsent(event.key(), _ -> new ArrayList<>()).add(event);
        }
        for (var entry : byKey.entrySet()) {
            var history = entry.getValue();
            // A read at a sequence follows the write of that sequence
            history.sort(Comparator.comparingLong(Event::sequence).thenComparing(Event::read));
            String state = null;
            for (var event : history) {
                if (event.computed() && !Objects.equals(state, event.received())) {
                    fail("Key " + entry.getKey() + " at " + event.sequence() + " received " + event.received()
                            + " where the write before left " + state + ": " + history);
                }
                state = event.left();
            }
            assertEquals(Optional.ofNullable(state), ledger.get(keyspace, entry.getKey()), "final value of " + entry.getKey());
        }
    }
}
