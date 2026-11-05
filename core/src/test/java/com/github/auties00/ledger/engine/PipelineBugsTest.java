package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerCorruptedException;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/// Races and failure paths of the commit pipeline, each set up with the package-private hooks of the engine so it
/// fails deterministically where the pipeline has the bug.
class PipelineBugsTest {
    private static final byte[] KEY = new byte[32];
    private static final Keyspace<String, String> VALUES = Keyspace.paged("values", KeyCodec.utf8(), ValueCodec.utf8());

    private static LiveLedger memory() {
        return (LiveLedger) Ledger.builder().memory().keyspace(VALUES).build();
    }

    private static MemorySegment key(String key) {
        return MemorySegment.ofArray(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("a key opened while the publisher drops its chain, between the release and the mark, gets a chain")
    void reopenDuringDrop() {
        try (var ledger = memory()) {
            var state = ledger.state(VALUES);
            var chain = state.open(key("k"));
            // The first step of KeyspaceState.drop: the publisher is descheduled before it marks the chain dropped
            assertTrue(chain.release(chain.claimed()));
            assertDoesNotThrow(() -> state.open(key("k")));
        }
    }

    @Test
    @DisplayName("a range removal applied while a commit holds a write of a key it removed keeps hiding the key")
    void removalBesideAClaim() {
        try (var ledger = memory()) {
            ledger.commit(ledger.newBatch().put(VALUES, "k", "1"));
            var snapshot = ledger.snapshot();
            // Held back by the snapshot: the removal stays a record
            ledger.commit(ledger.newBatch().deleteRange(VALUES, KeyRange.all()));
            assertEquals(Optional.empty(), ledger.get(VALUES, "k"));
            // A commit of "k" given its sequence, its write not applied yet
            var claimer = new PendingCommit(null, null, null, null, null, null);
            ledger.state(VALUES).chain(key("k")).claim(claimer, ledger.latest() + 100);
            snapshot.close();
            // Its publication applies the removal
            ledger.commit(ledger.newBatch().put(VALUES, "other", "2"));
            assertEquals(Optional.empty(), ledger.get(VALUES, "k"));
        }
    }

    @Test
    @DisplayName("removals of keys the store does not hold leave no chains behind")
    void absentRemovals() {
        try (var ledger = memory()) {
            for (var i = 0; i < 10_000; i++) {
                ledger.commit(ledger.newBatch().delete(VALUES, "absent" + i));
            }
            ledger.commit(ledger.newBatch().put(VALUES, "k", "1"));
            var chains = ledger.state(VALUES).entries.size();
            assertTrue(chains < 1_000, () -> chains + " chains for one key");
        }
    }

    @Test
    @DisplayName("the size of a key space never goes back while commits only add keys")
    void sizeWhileCountsMove() throws Exception {
        try (var ledger = memory()) {
            ledger.commit(ledger.newBatch().put(VALUES, "first", "1"));
            var stop = new AtomicBoolean();
            var next = new AtomicLong();
            var failures = new ConcurrentLinkedQueue<String>();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 4; t++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    while (!stop.get()) {
                        ledger.commit(ledger.newBatch().put(VALUES, "key" + next.getAndIncrement(), "v"));
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                var previous = 1L;
                while (!stop.get()) {
                    var size = ledger.size(VALUES);
                    if (size < previous) {
                        failures.add("size " + size + " after " + previous);
                    }
                    previous = Math.max(previous, size);
                }
            }));
            Thread.sleep(2_000);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            assertTrue(failures.isEmpty(), () -> failures.size() + " sizes went back, first: " + failures.peek());
        }
    }

    @Test
    @DisplayName("platform and virtual threads committing to hot keys beside transactions, range removals and snapshots all finish, with exact counts")
    void stress() throws Exception {
        try (var ledger = memory()) {
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<String>();
            var commits = new AtomicLong();
            var threads = new ArrayList<Thread>();
            for (var t = 0; t < 72; t++) {
                var id = t;
                Runnable task = () -> {
                    var random = new java.util.Random(id);
                    try {
                        while (!stop.get()) {
                            var choice = random.nextInt(100);
                            if (choice < 2) {
                                ledger.update(transaction -> transaction.put(VALUES, "k" + random.nextInt(64), "t"));
                            } else if (choice < 3) {
                                var from = random.nextInt(64);
                                ledger.commit(ledger.newBatch().deleteRange(VALUES, KeyRange.between("k" + from, "k" + (from + 4))));
                            } else if (choice < 5) {
                                try (var snapshot = ledger.snapshot()) {
                                    var seen = snapshot.range(VALUES, KeyRange.all(), 10_000).size();
                                    if (seen != snapshot.size(VALUES)) {
                                        failures.add("snapshot read " + seen + " entries where it counts " + snapshot.size(VALUES));
                                    }
                                }
                            } else {
                                var batch = ledger.newBatch();
                                for (var i = 0; i < 1 + random.nextInt(3); i++) {
                                    var key = "k" + random.nextInt(64);
                                    if (random.nextInt(4) == 0) {
                                        batch.delete(VALUES, key);
                                    } else {
                                        batch.put(VALUES, key, "v" + id);
                                    }
                                }
                                ledger.commit(batch);
                            }
                            commits.incrementAndGet();
                        }
                    } catch (RuntimeException | Error failure) {
                        if (failures.isEmpty()) {
                            failure.printStackTrace();
                        }
                        failures.add(failure.toString());
                    }
                };
                threads.add(id < 8 ? Thread.ofPlatform().start(task) : Thread.ofVirtual().start(task));
            }
            Thread.sleep(3_000);
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            assertTrue(failures.isEmpty(), () -> failures.size() + " failures, first: " + failures.peek());
            assertTrue(commits.get() > 1_000, () -> commits + " commits");
            assertEquals(ledger.range(VALUES, KeyRange.all(), 10_000).size(), ledger.size(VALUES));
        }
    }

    @Test
    @DisplayName("a commit that runs alone, whose tree read fails, takes back the new keys it wrote before")
    void rolledBackNewKey(@TempDir Path directory) throws Exception {
        var options = Ledger.builder().directory(directory).encryption(Encryption.key(KEY)).keyspace(VALUES);
        try (var ledger = options.build()) {
            var batch = ledger.newBatch();
            for (var i = 0; i < 4_000; i++) {
                batch.put(VALUES, "m%05d".formatted(i), "value " + i);
            }
            ledger.commit(batch);
        }
        var file = directory.resolve(PageFile.FILE);
        var original = Files.readAllBytes(file);
        var pages = original.length / PageFormat.SIZE;
        for (var page = pages / 2; page < pages; page++) {
            var bytes = original.clone();
            bytes[page * PageFormat.SIZE + PageFormat.HEADER + 100] ^= 1;
            Files.write(file, bytes);
            LiveLedger ledger;
            try {
                ledger = (LiveLedger) options.build();
            } catch (LedgerCorruptedException damagedOnOpen) {
                continue;
            }
            try {
                // "a" sorts before every key, in a leaf the damage spares
                if (ledger.get(VALUES, "a").isPresent()) {
                    fail("a");
                }
                String damaged = null;
                for (var i = 0; i < 4_000 && damaged == null; i++) {
                    var candidate = "m%05d".formatted(i);
                    try {
                        ledger.get(VALUES, candidate);
                    } catch (LedgerCorruptedException failed) {
                        damaged = candidate;
                    }
                }
                if (damaged == null) {
                    continue;
                }
                // The range removal makes the commit run alone, which looks its keys up as it applies; a commit of
                // the pipeline writes them blind and reads no tree
                var failing = ledger.newBatch().put(VALUES, "a", "new").put(VALUES, damaged, "x")
                        .deleteRange(VALUES, KeyRange.between("b", "c"));
                assertThrows(LedgerCorruptedException.class, () -> ledger.commit(failing));
                // The commit failed whole: its first write is not visible
                assertEquals(Optional.empty(), ledger.get(VALUES, "a"));
                return;
            } catch (LedgerCorruptedException unrelated) {
                // The damage reached a page every read needs
            } finally {
                try {
                    ledger.closeWithoutCheckpoint();
                } catch (RuntimeException ignored) {
                    // The damaged store may fail to close
                }
            }
        }
        fail("No page whose damage fails one read and spares another");
    }
}
