package com.github.auties00.ledger.engine;

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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The contract of [Ledger#compute] and [com.github.auties00.ledger.WriteBatch#compute] on one thread: what a function
/// receives and leaves, when it runs again, what a failure leaves, what it may not call, and its large values, on a
/// store in memory and on stores in a directory with and without encryption.
class ComputeTest {
    private static final byte[] KEY = new byte[32];
    static final Keyspace<String, String> PAGED = Keyspace.paged("paged", KeyCodec.utf8(), ValueCodec.utf8());
    static final Keyspace<String, StringBuilder> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.ofTrusted(
            StringBuilder::length,
            (value, target) -> target.copyFrom(java.lang.foreign.MemorySegment.ofArray(value.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII))),
            source -> new StringBuilder(new String(source.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), java.nio.charset.StandardCharsets.US_ASCII))));
    static final Keyspace<String, byte[]> BLOBS = Keyspace.paged("blobs", KeyCodec.utf8(), ValueCodec.bytes());

    enum Store {
        MEMORY, PLAIN, SEALED
    }

    static Ledger open(Store store, Path directory) {
        var options = store == Store.MEMORY ? Ledger.builder().memory()
                : Ledger.builder().directory(directory).encryption(store == Store.SEALED ? Encryption.key(KEY) : Encryption.none());
        return options.keyspace(PAGED).keyspace(RESIDENT).keyspace(BLOBS).largeValues(4_096).build();
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("creates, replaces and removes a value, and reports what it received and left")
    void basics(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var created = ledger.compute(PAGED, "a", value -> {
                assertEquals(null, value);
                return "1";
            });
            assertTrue(created.changed());
            assertEquals(Optional.empty(), created.previous());
            assertEquals(Optional.of("1"), created.value());
            assertEquals(Optional.of("1"), ledger.get(PAGED, "a"));
            assertEquals(1, ledger.size(PAGED));

            var replaced = ledger.compute(PAGED, "a", value -> value + "2");
            assertEquals(Optional.of("1"), replaced.previous());
            assertEquals(Optional.of("12"), replaced.value());
            assertTrue(replaced.sequence() > created.sequence());

            var removed = ledger.compute(PAGED, "a", _ -> null);
            assertTrue(removed.changed());
            assertEquals(Optional.of("12"), removed.previous());
            assertEquals(Optional.empty(), removed.value());
            assertEquals(Optional.empty(), ledger.get(PAGED, "a"));
            assertEquals(0, ledger.size(PAGED));

            var absent = ledger.compute(PAGED, "a", _ -> null);
            assertFalse(absent.changed());
            assertEquals(0, ledger.size(PAGED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a function that returns what it received commits nothing, and its result waits for the state it read")
    void unchanged(Store store, @TempDir Path directory) throws InterruptedException {
        try (var ledger = open(store, directory)) {
            var written = ledger.commit(ledger.newBatch().put(PAGED, "a", "1"));
            var unchanged = ledger.compute(PAGED, "a", value -> value);
            assertFalse(unchanged.changed());
            assertEquals(written.sequence(), unchanged.sequence());
            assertEquals(Optional.of("1"), unchanged.value());
            assertEquals(Optional.of("1"), unchanged.previous());
            unchanged.awaitDurable();
            assertTrue(unchanged.isDurable());
            // Nothing was committed: the next commit takes the next sequence
            assertEquals(written.sequence() + 1, ledger.commit(ledger.newBatch().put(PAGED, "b", "2")).sequence());
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a resident value is the instance the function returned, which an unchanged compute receives again")
    void residentInstances(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var first = new StringBuilder("one");
            ledger.compute(RESIDENT, "a", _ -> first);
            assertSame(first, ledger.get(RESIDENT, "a").orElseThrow());
            var received = new ArrayList<StringBuilder>();
            var unchanged = ledger.compute(RESIDENT, "a", value -> {
                received.add(value);
                return value;
            });
            assertSame(first, received.getFirst());
            assertFalse(unchanged.changed());
            assertSame(first, unchanged.value().orElseThrow());
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a computed write of a batch whose key another commit wrote first runs again, in order, on that value")
    void rerunsAfterAnotherWrite(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(PAGED, "a", "1"));
            var received = new ArrayList<String>();
            var batch = ledger.newBatch().put(PAGED, "other", "x");
            var computation = batch.compute(PAGED, "a", value -> {
                received.add(value);
                return value + "+";
            });
            ledger.commit(ledger.newBatch().put(PAGED, "a", "2"));
            ledger.commit(batch);
            assertEquals(List.of("1", "2"), received);
            assertEquals(Optional.of("2"), computation.previous());
            assertEquals(Optional.of("2+"), computation.value());
            assertEquals(Optional.of("2+"), ledger.get(PAGED, "a"));
            assertEquals(Optional.of("x"), ledger.get(PAGED, "other"));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a write whose chain the store dropped since the read, by a checkpoint or as a removal, still sends the function to run again")
    void droppedChains(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(PAGED, "a", "1").put(PAGED, "b", "1"));
            var batch = ledger.newBatch();
            var outcome = batch.compute(PAGED, "a", value -> value == null ? "absent" : value + "+");
            var guarded = ledger.newBatch();
            guarded.guard(PAGED, "b");
            guarded.put(PAGED, "c", "guarded");
            // A removal leaves a chain of nothing but itself, which a store in memory drops at once; a checkpoint
            // drops the chains of a store in a directory
            ledger.commit(ledger.newBatch().delete(PAGED, "a").put(PAGED, "b", "2"));
            ledger.checkpoint();
            ledger.commit(ledger.newBatch().put(PAGED, "other", "x"));
            ledger.commit(batch);
            assertEquals(Optional.empty(), outcome.previous());
            assertEquals(Optional.of("absent"), ledger.get(PAGED, "a"));
            assertThrows(com.github.auties00.ledger.LedgerConflictException.class, () -> ledger.commit(guarded));
            assertEquals(Optional.empty(), ledger.get(PAGED, "c"));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("an unchanged compute in a batch with other writes runs again when its key moved, and then writes")
    void validatesUnchangedInBatch(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var batch = ledger.newBatch().put(PAGED, "other", "x");
            var computation = batch.compute(PAGED, "a", value -> value == null ? null : value + "!");
            ledger.commit(ledger.newBatch().put(PAGED, "a", "1"));
            ledger.commit(batch);
            assertTrue(computation.changed());
            assertEquals(Optional.of("1!"), ledger.get(PAGED, "a"));
            assertEquals(2, ledger.size(PAGED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a batch of unchanged computes commits nothing")
    void unchangedBatch(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var written = ledger.commit(ledger.newBatch().put(PAGED, "a", "1").put(PAGED, "b", "2"));
            var batch = ledger.newBatch();
            var a = batch.compute(PAGED, "a", value -> value);
            var b = batch.compute(PAGED, "missing", _ -> null);
            var commit = ledger.commit(batch);
            assertEquals(written.sequence(), commit.sequence());
            assertFalse(a.changed());
            assertFalse(b.changed());
            assertTrue(batch.isEmpty());
            assertEquals(written.sequence() + 1, ledger.commit(ledger.newBatch().put(PAGED, "c", "3")).sequence());
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a compute after a write of its key in the same batch sees that write, and two computes of a key chain")
    void sameKeyInBatch(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var batch = ledger.newBatch().put(PAGED, "a", "put");
            var first = batch.compute(PAGED, "a", value -> value + "+1");
            var second = batch.compute(PAGED, "a", value -> value + "+2");
            ledger.commit(batch);
            assertEquals(Optional.of("put"), first.previous());
            assertEquals(Optional.of("put+1"), first.value());
            assertEquals(Optional.of("put+1"), second.previous());
            assertEquals(Optional.of("put+1+2"), ledger.get(PAGED, "a"));
            assertEquals(1, ledger.size(PAGED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a range removal earlier in the batch, or committed since the read, leaves the key absent for the function")
    void rangeRemovals(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(PAGED, "a", "1").put(PAGED, "b", "2"));
            var batch = ledger.newBatch().deleteRange(PAGED, KeyRange.all());
            var computation = batch.compute(PAGED, "a", value -> value == null ? "fresh" : value + "!");
            ledger.commit(batch);
            assertEquals(Optional.empty(), computation.previous());
            assertEquals(Optional.of("fresh"), ledger.get(PAGED, "a"));
            assertEquals(Optional.empty(), ledger.get(PAGED, "b"));
            assertEquals(1, ledger.size(PAGED));

            var later = ledger.newBatch();
            var outcome = later.compute(PAGED, "a", value -> value == null ? "new" : value + "!");
            ledger.commit(ledger.newBatch().deleteRange(PAGED, KeyRange.between("a", "b")));
            ledger.commit(later);
            assertEquals(Optional.empty(), outcome.previous());
            assertEquals(Optional.of("new"), ledger.get(PAGED, "a"));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a function that throws on its first run leaves the batch as it was; on its run in order, takes the batch back")
    void failures(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var batch = ledger.newBatch().put(PAGED, "kept", "1");
            assertThrows(ArithmeticException.class, () -> batch.compute(PAGED, "a", _ -> {
                throw new ArithmeticException("first");
            }));
            assertEquals(1, batch.size());
            assertThrows(ArithmeticException.class, () -> ledger.compute(PAGED, "a", _ -> {
                throw new ArithmeticException("single");
            }));

            var runs = new AtomicInteger();
            var failing = ledger.newBatch().put(PAGED, "side", "s");
            var outcome = failing.compute(PAGED, "a", value -> {
                if (runs.incrementAndGet() == 2) {
                    throw new ArithmeticException("second");
                }
                return "first";
            });
            ledger.commit(ledger.newBatch().put(PAGED, "a", "moved"));
            assertThrows(ArithmeticException.class, () -> ledger.commit(failing));
            assertThrows(IllegalStateException.class, outcome::value);
            assertEquals(Optional.of("moved"), ledger.get(PAGED, "a"));
            assertEquals(Optional.empty(), ledger.get(PAGED, "side"));
            // The store goes on
            ledger.commit(ledger.newBatch().put(PAGED, "after", "ok"));
            assertEquals(Optional.of("moved!"), ledger.compute(PAGED, "a", value -> value + "!").value());
            assertEquals(2, ledger.size(PAGED));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a function may read the store, but not commit, compute, wait for durability, checkpoint or close it")
    void reentrancy(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(PAGED, "b", "read"));
            List<Consumer<Ledger>> forbidden = List.of(
                    l -> l.commit(l.newBatch().put(PAGED, "x", "y")),
                    l -> l.update(_ -> { }),
                    l -> l.compute(PAGED, "x", v -> v),
                    l -> {
                        try {
                            l.awaitDurable();
                        } catch (InterruptedException exception) {
                            throw new AssertionError(exception);
                        }
                    },
                    Ledger::checkpoint,
                    Ledger::close,
                    l -> l.newBatch().compute(PAGED, "x", v -> v),
                    l -> l.newBatch().guard(PAGED, "x"));
            for (var call : forbidden) {
                assertThrows(IllegalStateException.class, () -> ledger.compute(PAGED, "a", value -> {
                    call.accept(ledger);
                    return value;
                }));
            }
            assertEquals(Optional.of("read"), ledger.compute(PAGED, "a", _ -> ledger.get(PAGED, "b").orElseThrow()).value());
            // The same in the run in order
            var batch = ledger.newBatch();
            var runs = new AtomicInteger();
            batch.compute(PAGED, "a", value -> {
                if (runs.incrementAndGet() == 2) {
                    ledger.commit(ledger.newBatch().put(PAGED, "x", "y"));
                }
                return value + "!";
            });
            ledger.commit(ledger.newBatch().put(PAGED, "a", "moved"));
            assertThrows(IllegalStateException.class, () -> ledger.commit(batch));
            assertEquals(Optional.of("moved"), ledger.get(PAGED, "a"));
        }
    }

    @Test
    @DisplayName("a transaction cannot compute")
    void transactions() {
        try (var ledger = open(Store.MEMORY, null)) {
            assertThrows(IllegalStateException.class, () -> ledger.update(_ -> ledger.compute(PAGED, "a", v -> v)));
        }
    }

    @Test
    @DisplayName("an outcome cannot be read before its batch is committed")
    void outcomeBeforeCommit() {
        try (var ledger = open(Store.MEMORY, null)) {
            var batch = ledger.newBatch();
            var outcome = batch.compute(PAGED, "a", _ -> "1");
            assertThrows(IllegalStateException.class, outcome::value);
            assertThrows(IllegalStateException.class, outcome::changed);
            ledger.commit(batch);
            assertEquals(Optional.of("1"), outcome.value());
        }
    }

    @ParameterizedTest
    @EnumSource(value = Store.class, names = {"PLAIN", "SEALED"})
    @DisplayName("a large value goes to its file on the first run, and to the log when the function runs again, both recovered")
    void largeValues(Store store, @TempDir Path directory) {
        var big = new byte[20_000];
        var bigger = new byte[30_000];
        java.util.Arrays.fill(big, (byte) 1);
        java.util.Arrays.fill(bigger, (byte) 2);
        try (var ledger = open(store, directory)) {
            ledger.compute(BLOBS, "first", _ -> big);
            var batch = ledger.newBatch();
            var runs = new AtomicInteger();
            batch.compute(BLOBS, "second", value -> runs.incrementAndGet() == 1 ? big : bigger);
            ledger.commit(ledger.newBatch().put(BLOBS, "second", new byte[]{9}));
            ledger.commit(batch);
            assertEquals(2, runs.get());
            assertArrayEquals(bigger, ledger.get(BLOBS, "second").orElseThrow());
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(store, directory)) {
            assertArrayEquals(big, ledger.get(BLOBS, "first").orElseThrow());
            assertArrayEquals(bigger, ledger.get(BLOBS, "second").orElseThrow());
            ledger.checkpoint();
        }
        try (var ledger = open(store, directory)) {
            assertArrayEquals(bigger, ledger.get(BLOBS, "second").orElseThrow());
            assertEquals(2, ledger.size(BLOBS));
        }
    }

    @ParameterizedTest
    @EnumSource(value = Store.class, names = {"PLAIN", "SEALED"})
    @DisplayName("computed writes survive a reopen, a checkpoint and a crash before it")
    void recovery(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            for (var i = 0; i < 50; i++) {
                ledger.compute(PAGED, "counter", value -> String.valueOf(value == null ? 1 : Integer.parseInt(value) + 1));
                ledger.compute(RESIDENT, "counter", value -> new StringBuilder(String.valueOf(value == null ? 1 : Integer.parseInt(value.toString()) + 1)));
            }
            ((LiveLedger) ledger).closeWithoutCheckpoint();
        }
        try (var ledger = open(store, directory)) {
            assertEquals(Optional.of("50"), ledger.get(PAGED, "counter"));
            assertEquals("50", ledger.get(RESIDENT, "counter").orElseThrow().toString());
            ledger.compute(PAGED, "counter", value -> String.valueOf(Integer.parseInt(value) + 1));
            ledger.checkpoint();
            ledger.compute(PAGED, "counter", value -> String.valueOf(Integer.parseInt(value) + 1));
        }
        try (var ledger = open(store, directory)) {
            assertEquals(Optional.of("52"), ledger.get(PAGED, "counter"));
        }
    }
}
