package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerConflictException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/// Guards: a batch holds to the value a guard read, and fails with [LedgerConflictException], applying nothing, when a
/// write of the key came in between, unless the key holds what was read again.
class GuardTest {
    private static final Keyspace<String, String> IDS = Keyspace.paged("ids", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> COLLECTIONS = Keyspace.resident("collections", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> BODIES = Keyspace.paged("bodies", KeyCodec.utf8(), ValueCodec.utf8());

    enum Store {
        MEMORY, DIRECTORY
    }

    private static Ledger open(Store store, Path directory) {
        var options = store == Store.MEMORY ? Ledger.builder().memory()
                : Ledger.builder().directory(directory).encryption(Encryption.none());
        return options.keyspace(IDS).keyspace(COLLECTIONS).keyspace(BODIES).build();
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("an insert guarded as absent applies once; a second one conflicts and applies nothing")
    void insertOnce(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            var first = ledger.newBatch();
            assertEquals(Optional.empty(), first.guard(IDS, "m1"));
            first.put(IDS, "m1", "1").put(BODIES, "b1", "body");
            var second = ledger.newBatch();
            assertEquals(Optional.empty(), second.guard(IDS, "m1"));
            second.put(IDS, "m1", "2").put(BODIES, "b2", "other");
            ledger.commit(first);
            assertThrows(LedgerConflictException.class, () -> ledger.commit(second));
            assertEquals(Optional.of("1"), ledger.get(IDS, "m1"));
            assertEquals(Optional.empty(), ledger.get(BODIES, "b2"));
            assertEquals(1, ledger.size(BODIES));
            // A new batch reads again
            var retry = ledger.newBatch();
            assertEquals(Optional.of("1"), retry.guard(IDS, "m1"));
            ledger.commit(retry.put(BODIES, "b2", "other"));
            assertEquals(2, ledger.size(BODIES));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a guarded version passes unchanged, conflicts once moved, and passes when the key holds the same value again")
    void versions(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(COLLECTIONS, "regular", "v1"));
            var unchanged = ledger.newBatch();
            unchanged.guard(COLLECTIONS, "regular");
            ledger.commit(unchanged.put(COLLECTIONS, "regular", "v2"));
            assertEquals(Optional.of("v2"), ledger.get(COLLECTIONS, "regular"));

            var stale = ledger.newBatch();
            stale.guard(COLLECTIONS, "regular");
            stale.put(COLLECTIONS, "regular", "v3");
            ledger.commit(ledger.newBatch().put(COLLECTIONS, "regular", "other"));
            assertThrows(LedgerConflictException.class, () -> ledger.commit(stale));
            assertEquals(Optional.of("other"), ledger.get(COLLECTIONS, "regular"));

            var again = ledger.newBatch();
            again.guard(IDS, "k");
            again.put(BODIES, "x", "y");
            ledger.commit(ledger.newBatch().put(IDS, "k", "tmp"));
            ledger.commit(ledger.newBatch().delete(IDS, "k"));
            ledger.commit(again);
            assertEquals(Optional.of("y"), ledger.get(BODIES, "x"));

            ledger.commit(ledger.newBatch().put(IDS, "p", "same"));
            var equal = ledger.newBatch();
            equal.guard(IDS, "p");
            ledger.commit(ledger.newBatch().put(IDS, "p", "other"));
            ledger.commit(ledger.newBatch().put(IDS, "p", "same"));
            ledger.commit(equal.put(BODIES, "z", "ok"));
            assertEquals(Optional.of("ok"), ledger.get(BODIES, "z"));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a range removal covering the key fails a guard of a present value, a write of the key earlier in the batch too")
    void removalsAndOwnWrites(Store store, @TempDir Path directory) {
        try (var ledger = open(store, directory)) {
            ledger.commit(ledger.newBatch().put(IDS, "a", "1"));
            var batch = ledger.newBatch();
            batch.guard(IDS, "a");
            batch.put(BODIES, "x", "y");
            ledger.commit(ledger.newBatch().deleteRange(IDS, KeyRange.all()));
            assertThrows(LedgerConflictException.class, () -> ledger.commit(batch));
            assertEquals(Optional.empty(), ledger.get(BODIES, "x"));

            var own = ledger.newBatch().put(IDS, "b", "1");
            own.guard(IDS, "b");
            assertThrows(LedgerConflictException.class, () -> ledger.commit(own));
            assertEquals(Optional.empty(), ledger.get(IDS, "b"));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("of many threads inserting the same ids guarded as absent, exactly one wins each id")
    void concurrentInserts(Store store, @TempDir Path directory) throws Exception {
        var winners = new ConcurrentHashMap<String, String>();
        var conflicts = new AtomicInteger();
        try (var ledger = open(store, directory);
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (var thread = 0; thread < 16; thread++) {
                var name = "t" + thread;
                futures.add(executor.submit(() -> {
                    for (var id = 0; id < 200; id++) {
                        var key = "m" + id;
                        var batch = ledger.newBatch();
                        if (batch.guard(IDS, key).isPresent()) {
                            continue;
                        }
                        batch.put(IDS, key, name);
                        batch.compute(COLLECTIONS, "count", value -> String.valueOf(value == null ? 1 : Integer.parseInt(value) + 1));
                        try {
                            ledger.commit(batch);
                            if (winners.putIfAbsent(key, name) != null) {
                                throw new AssertionError("two winners of " + key);
                            }
                        } catch (LedgerConflictException _) {
                            conflicts.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            for (var future : futures) {
                future.get();
            }
            assertEquals(200, winners.size());
            assertEquals(200, ledger.size(IDS));
            assertEquals(Optional.of("200"), ledger.get(COLLECTIONS, "count"));
            winners.forEach((key, name) -> assertEquals(Optional.of(name), ledger.get(IDS, key)));
        }
    }

    @ParameterizedTest
    @EnumSource(Store.class)
    @DisplayName("a guard compares the stored bytes at the commit's turn, running no codec there")
    void noCodecAtTurn(Store store, @TempDir Path directory) {
        var encoding = new AtomicBoolean(true);
        var codec = ValueCodec.<String>of(value -> value.length(), (value, target) -> {
            if (!encoding.get()) {
                throw new AssertionError("encoded in the order of the commits");
            }
            target.copyFrom(MemorySegment.ofArray(value.getBytes(StandardCharsets.ISO_8859_1)));
        }, source -> new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.ISO_8859_1));
        var resident = Keyspace.resident("strict.resident", KeyCodec.utf8(), codec);
        var paged = Keyspace.paged("strict.paged", KeyCodec.utf8(), codec);
        var options = store == Store.MEMORY ? Ledger.builder().memory()
                : Ledger.builder().directory(directory).encryption(Encryption.none());
        try (var ledger = options.keyspace(resident).keyspace(paged).build()) {
            for (var keyspace : java.util.List.of(resident, paged)) {
                ledger.commit(ledger.newBatch().put(keyspace, "k", "same"));
                var same = ledger.newBatch();
                same.guard(keyspace, "k");
                // Another instance of the same bytes: the guard holds
                ledger.commit(ledger.newBatch().put(keyspace, "k", new String("same")));
                encoding.set(false);
                try {
                    ledger.commit(same);
                } finally {
                    encoding.set(true);
                }
                var moved = ledger.newBatch();
                moved.guard(keyspace, "k");
                ledger.commit(ledger.newBatch().put(keyspace, "k", "diff"));
                encoding.set(false);
                try {
                    assertThrows(LedgerConflictException.class, () -> ledger.commit(moved));
                } finally {
                    encoding.set(true);
                }
            }
        }
    }
}
