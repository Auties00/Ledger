package com.github.auties00.ledger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The in-memory engine against the contract of the API: the semantics every store, durable or not, must keep.
class MemoryLedgerTest {
    private static final ValueLayout.OfLong BIG_ENDIAN = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueCodec<Long> LONGS = ValueCodec.ofTrusted(
            value -> Long.BYTES,
            (value, target) -> target.set(BIG_ENDIAN, 0, value),
            source -> source.get(BIG_ENDIAN, 0));

    private static final Keyspace<String, String> NAMES = Keyspace.paged("names", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<Long, Long> ACCOUNTS = Keyspace.paged("accounts", KeyCodec.int64(), LONGS);
    private static final Keyspace<String, StringBuilder> RESIDENT = Keyspace.resident("resident", KeyCodec.utf8(), ValueCodec.ofTrusted(
            value -> value.length(),
            (value, target) -> MemorySegment.copy(value.toString().getBytes(StandardCharsets.US_ASCII), 0, target, ValueLayout.JAVA_BYTE, 0, value.length()),
            source -> new StringBuilder(new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII))));

    private Ledger ledger;

    @BeforeEach
    void open() {
        ledger = Ledger.builder().memory().keyspace(NAMES).keyspace(ACCOUNTS).keyspace(RESIDENT).build();
    }

    @AfterEach
    void close() {
        ledger.close();
    }

    private List<String> keys(View view, KeyRange<String> range) {
        return view.range(NAMES, range, 1_000).stream().map(Entry::key).toList();
    }

    @Nested
    @DisplayName("writes and reads")
    class WritesAndReads {
        @Test
        @DisplayName("puts, overwrites and deletes, counting the entries")
        void basics() {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "1").put(NAMES, "b", "2"));
            assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
            assertEquals(2, ledger.size(NAMES));
            ledger.commit(ledger.newBatch().put(NAMES, "a", "one").delete(NAMES, "b").delete(NAMES, "missing"));
            assertEquals(Optional.of("one"), ledger.get(NAMES, "a"));
            assertEquals(Optional.empty(), ledger.get(NAMES, "b"));
            assertEquals(1, ledger.size(NAMES));
        }

        @Test
        @DisplayName("ranges read in both directions, within bounds and limits")
        void ranges() {
            var batch = ledger.newBatch();
            for (var key : List.of("a", "b", "c", "d", "e")) {
                batch.put(NAMES, key, key.toUpperCase());
            }
            ledger.commit(batch);
            assertEquals(List.of("b", "c", "d"), keys(ledger, KeyRange.between("b", "e")));
            assertEquals(List.of("d", "c", "b"), keys(ledger, KeyRange.between("b", "e").reversed()));
            assertEquals(List.of("a", "b"), ledger.range(NAMES, KeyRange.all(), 2).stream().map(Entry::key).toList());
            assertEquals(List.of("e", "d"), ledger.range(NAMES, KeyRange.<String>all().reversed(), 2).stream().map(Entry::key).toList());
            assertEquals(List.of("c", "d", "e"), keys(ledger, KeyRange.atLeast("c")));
            assertEquals(List.of("a", "b"), keys(ledger, KeyRange.below("c")));
            assertEquals(List.of(), keys(ledger, KeyRange.between("d", "b")));
            assertEquals(Optional.of(new Entry<>("e", "E")), ledger.first(NAMES, KeyRange.<String>all().reversed()));
            var visited = new ArrayList<String>();
            assertEquals(2, ledger.forEach(NAMES, KeyRange.all(), (key, value) -> visited.add(key) && visited.size() < 2));
            assertEquals(List.of("a", "b"), visited);
        }

        @Test
        @DisplayName("later writes of a batch override earlier ones, range removals included")
        void batchOrder() {
            ledger.commit(ledger.newBatch()
                    .put(NAMES, "a", "1")
                    .deleteRange(NAMES, KeyRange.between("a", "b"))
                    .deleteRange(NAMES, KeyRange.between("x", "z"))
                    .put(NAMES, "y", "2"));
            assertEquals(Optional.empty(), ledger.get(NAMES, "a"));
            assertEquals(Optional.of("2"), ledger.get(NAMES, "y"));
            assertEquals(1, ledger.size(NAMES));
        }

        @Test
        @DisplayName("a range removal hides what it covers, counts it, and lets later writes through")
        void rangeRemoval() {
            var batch = ledger.newBatch();
            for (var i = 0; i < 100; i++) {
                batch.put(ACCOUNTS, (long) i, (long) i);
            }
            ledger.commit(batch);
            ledger.commit(ledger.newBatch().deleteRange(ACCOUNTS, KeyRange.between(10L, 60L)));
            assertEquals(50, ledger.size(ACCOUNTS));
            assertEquals(Optional.empty(), ledger.get(ACCOUNTS, 30L));
            assertEquals(Optional.of(60L), ledger.get(ACCOUNTS, 60L));
            ledger.commit(ledger.newBatch().put(ACCOUNTS, 30L, 300L));
            assertEquals(Optional.of(300L), ledger.get(ACCOUNTS, 30L));
            assertEquals(51, ledger.size(ACCOUNTS));
            assertEquals(51, ledger.forEach(ACCOUNTS, KeyRange.all(), (key, value) -> true));
            // Negative keys sort first
            ledger.commit(ledger.newBatch().put(ACCOUNTS, -5L, -5L));
            assertEquals(Optional.of(new Entry<>(-5L, -5L)), ledger.first(ACCOUNTS, KeyRange.all()));
        }

        @Test
        @DisplayName("encoded reads and writes skip the codecs")
        void encoded() {
            var key = MemorySegment.ofArray("k".getBytes(StandardCharsets.UTF_8));
            var value = MemorySegment.ofArray("v".getBytes(StandardCharsets.UTF_8));
            ledger.commit(ledger.newBatch().putEncoded(NAMES, key, value));
            assertEquals(Optional.of("v"), ledger.get(NAMES, "k"));
            assertEquals(Optional.of(1L), ledger.readEncoded(NAMES, "k", MemorySegment::byteSize));
            var seen = new ArrayList<Long>();
            ledger.forEachEncoded(NAMES, KeyRange.all(), (encodedKey, encodedValue) -> seen.add(encodedKey.byteSize() + encodedValue.byteSize()));
            assertEquals(List.of(2L), seen);
        }

        @Test
        @DisplayName("a resident key space returns the instance it holds")
        void resident() {
            var value = new StringBuilder("held");
            ledger.commit(ledger.newBatch().put(RESIDENT, "k", value));
            assertSame(value, ledger.get(RESIDENT, "k").orElseThrow());
            assertSame(ledger.get(RESIDENT, "k").orElseThrow(), ledger.get(RESIDENT, "k").orElseThrow());
            ledger.commit(ledger.newBatch().putEncoded(RESIDENT, MemorySegment.ofArray(new byte[]{'e'}), MemorySegment.ofArray(new byte[]{'x'})));
            assertEquals("x", ledger.get(RESIDENT, "e").orElseThrow().toString());
        }
    }

    @Nested
    @DisplayName("isolation")
    class Isolation {
        @Test
        @DisplayName("a snapshot keeps reading the state it opened on")
        void snapshot() {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "old").put(NAMES, "b", "kept"));
            try (var snapshot = ledger.snapshot()) {
                ledger.commit(ledger.newBatch().put(NAMES, "a", "new").delete(NAMES, "b").put(NAMES, "c", "added"));
                ledger.commit(ledger.newBatch().deleteRange(NAMES, KeyRange.all()));
                assertEquals(Optional.of("old"), snapshot.get(NAMES, "a"));
                assertEquals(List.of("a", "b"), keys(snapshot, KeyRange.all()));
                assertEquals(2, snapshot.size(NAMES));
                assertEquals(0, ledger.size(NAMES));
            }
            assertEquals(List.of(), keys(ledger, KeyRange.all()));
        }

        @Test
        @DisplayName("a transaction reads its own writes and applies them atomically")
        void transaction() {
            ledger.commit(ledger.newBatch().put(ACCOUNTS, 1L, 10L));
            var commit = ledger.update(transaction -> {
                var balance = transaction.get(ACCOUNTS, 1L).orElseThrow();
                transaction.put(ACCOUNTS, 1L, balance + 5).put(ACCOUNTS, 2L, 1L);
                assertEquals(Optional.of(15L), transaction.get(ACCOUNTS, 1L));
                assertEquals(2, transaction.size(ACCOUNTS));
                transaction.deleteRange(ACCOUNTS, KeyRange.atLeast(2L));
                assertEquals(1, transaction.size(ACCOUNTS));
                assertEquals(Optional.empty(), transaction.get(ACCOUNTS, 2L));
            });
            assertTrue(commit.isDurable());
            assertEquals(Optional.of(15L), ledger.get(ACCOUNTS, 1L));
            assertEquals(1, ledger.size(ACCOUNTS));
        }

        @Test
        @DisplayName("a transaction that throws leaves no trace")
        void rollback() {
            ledger.commit(ledger.newBatch().put(NAMES, "a", "1"));
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> {
                transaction.put(NAMES, "a", "changed").put(NAMES, "b", "new").deleteRange(NAMES, KeyRange.all());
                throw new IllegalStateException("abort");
            }));
            assertEquals(Optional.of("1"), ledger.get(NAMES, "a"));
            assertEquals(List.of("a"), keys(ledger, KeyRange.all()));
            assertEquals(1, ledger.size(NAMES));
            ledger.commit(ledger.newBatch().put(NAMES, "c", "3"));
            assertEquals(List.of("a", "c"), keys(ledger, KeyRange.all()));
        }

        @Test
        @DisplayName("a transaction cannot commit or nest, and dies with its function")
        void transactionMisuse() {
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> ledger.commit(ledger.newBatch().put(NAMES, "a", "1"))));
            assertThrows(IllegalStateException.class, () -> ledger.update(transaction -> ledger.update(inner -> { })));
            var escaped = new Transaction[1];
            ledger.update(transaction -> escaped[0] = transaction);
            assertThrows(IllegalStateException.class, () -> escaped[0].get(NAMES, "a"));
        }
    }

    @Nested
    @DisplayName("misuse")
    class Misuse {
        @Test
        @DisplayName("batches belong to their store and key spaces to the stores opened with them")
        void ownership() {
            try (var other = Ledger.builder().memory().keyspace(NAMES).build()) {
                assertThrows(IllegalArgumentException.class, () -> ledger.commit(other.newBatch().put(NAMES, "a", "1")));
                assertThrows(IllegalArgumentException.class, () -> other.newBatch().put(ACCOUNTS, 1L, 1L));
                assertThrows(IllegalArgumentException.class, () -> other.get(ACCOUNTS, 1L));
            }
            var sameName = Keyspace.resident("names", KeyCodec.utf8(), ValueCodec.utf8());
            assertThrows(IllegalArgumentException.class, () -> ledger.get(sameName, "a"));
        }

        @Test
        @DisplayName("a closed store refuses every call")
        void closed() {
            ledger.close();
            assertThrows(IllegalStateException.class, () -> ledger.get(NAMES, "a"));
            assertThrows(IllegalStateException.class, () -> ledger.newBatch());
            assertThrows(IllegalStateException.class, () -> ledger.snapshot());
            ledger.close();
        }
    }

    @Nested
    @DisplayName("concurrency")
    class Concurrency {
        @Test
        @DisplayName("concurrent transfers never show a reader a total other than the one they keep")
        void concurrentTransfers() throws Exception {
            var accounts = 64;
            var initial = ledger.newBatch();
            for (var account = 0; account < accounts; account++) {
                initial.put(ACCOUNTS, (long) account, 1_000L);
            }
            ledger.commit(initial);
            var expected = accounts * 1_000L;
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<String>();
            var transfers = new AtomicLong();
            var checks = new AtomicLong();
            try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
                for (var writer = 0; writer < 8; writer++) {
                    executor.execute(() -> {
                        var random = ThreadLocalRandom.current();
                        while (!stop.get()) {
                            var from = (long) random.nextInt(accounts);
                            var to = (long) random.nextInt(accounts);
                            var amount = (long) random.nextInt(1, 10);
                            ledger.update(transaction -> {
                                transaction.put(ACCOUNTS, from, transaction.get(ACCOUNTS, from).orElseThrow() - amount);
                                transaction.put(ACCOUNTS, to, transaction.get(ACCOUNTS, to).orElseThrow() + amount);
                            });
                            transfers.incrementAndGet();
                        }
                    });
                }
                for (var reader = 0; reader < 8; reader++) {
                    var useSnapshot = reader % 2 == 0;
                    executor.execute(() -> {
                        while (!stop.get()) {
                            var total = new long[1];
                            if (useSnapshot) {
                                try (var snapshot = ledger.snapshot()) {
                                    snapshot.forEach(ACCOUNTS, KeyRange.all(), (key, value) -> {
                                        total[0] += value;
                                        return true;
                                    });
                                    if (snapshot.size(ACCOUNTS) != accounts) {
                                        failures.add("snapshot size " + snapshot.size(ACCOUNTS));
                                    }
                                }
                            } else {
                                ledger.forEach(ACCOUNTS, KeyRange.all(), (key, value) -> {
                                    total[0] += value;
                                    return true;
                                });
                            }
                            if (total[0] != expected) {
                                failures.add("total " + total[0]);
                            }
                            checks.incrementAndGet();
                        }
                    });
                }
                Thread.sleep(TimeUnit.SECONDS.toMillis(2));
                stop.set(true);
            }
            assertTrue(failures.isEmpty(), () -> failures.size() + " inconsistent reads, first: " + failures.peek());
            assertTrue(transfers.get() > 100 && checks.get() > 100, () -> transfers + " transfers, " + checks + " checks");
            assertEquals(expected, sum());
        }
    }

    private long sum() {
        var total = new long[1];
        ledger.forEach(ACCOUNTS, KeyRange.all(), (key, value) -> {
            total[0] += value;
            return true;
        });
        return total[0];
    }
}
