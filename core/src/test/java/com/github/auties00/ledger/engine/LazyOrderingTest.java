package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The list of chains a key space orders lazily: what a read of a range sees beside a prune of the list, and the
/// dropped chains the list lets go of without a checkpoint.
class LazyOrderingTest {
    private static final Keyspace<String, String> CHATS = Keyspace.resident("chats", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> MESSAGES = Keyspace.paged("messages", KeyCodec.utf8(), ValueCodec.utf8());

    private static int listed(KeyspaceState state) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("unordered");
        field.setAccessible(true);
        var count = 0;
        for (var chain = (Chain) field.get(state); chain != null; chain = chain.nextUnordered) {
            count++;
        }
        return count;
    }

    @Test
    @DisplayName("a read of a range beside a prune of the list sees every chain, though it found the lock free only once the prune ended")
    void rangeBesidePrune() throws Exception {
        var state = new KeyspaceState(CHATS);
        state.lazy = true;
        var chains = 300_000;
        for (var i = 0; i < chains; i++) {
            state.open(Segments.encodeKey(KeyCodec.utf8(), "k%07d".formatted(i)));
        }
        var unordered = KeyspaceState.class.getDeclaredField("unordered");
        unordered.setAccessible(true);
        var pruned = new CountDownLatch(1);
        var reader = Thread.currentThread();
        // The reader is descheduled between its read of the list and its read of the lock, the gap of the read's fast
        // path, until the prune lets the lock go
        var lock = new ReentrantLock() {
            @Override
            public boolean isLocked() {
                if (Thread.currentThread() == reader) {
                    try {
                        pruned.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException exception) {
                        throw new AssertionError(exception);
                    }
                }
                return super.isLocked();
            }
        };
        var ordering = KeyspaceState.class.getDeclaredField("ordering");
        ordering.setAccessible(true);
        ordering.set(state, lock);
        var pruner = Thread.ofPlatform().start(() -> {
            state.pruneListed(Long.MAX_VALUE);
            pruned.countDown();
        });
        // A prune that took the list whole would leave it empty during its walk
        var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        while (unordered.get(state) != null && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        var slice = state.slice(null, null);
        pruner.join();
        assertEquals(chains, slice.size(), "the read of the range saw " + slice.size() + " of " + chains + " chains");
    }

    @Test
    @DisplayName("a prune keeps the resident chains listed and takes the dropped ones off, a chain listed during it included")
    void pruneKeepsResidentChains() throws Exception {
        var state = new KeyspaceState(CHATS);
        state.lazy = true;
        for (var i = 0; i < 1_000; i++) {
            state.open(Segments.encodeKey(KeyCodec.utf8(), "k%04d".formatted(i)));
        }
        state.pruneListed(Long.MAX_VALUE);
        assertEquals(1_000, listed(state));
        assertEquals(0, state.entries.size());
        assertEquals(1_000, state.slice(null, null).size());
        assertEquals(0, listed(state));
    }

    @Test
    @DisplayName("commits that remove keys memory does not hold leave a bounded number of dropped chains listed, with no checkpoint")
    void absentDeletes(@TempDir Path directory) throws Exception {
        try (var ledger = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(MESSAGES).keyspace(CHATS).checkpointEvery(1L << 30).build()) {
            var live = (LiveLedger) ledger;
            for (var i = 0; i < 20_000; i++) {
                ledger.commit(ledger.newBatch().delete(CHATS, "absent").delete(MESSAGES, "absent"));
            }
            assertEquals(0, live.state(CHATS).chains());
            var resident = listed(live.state(CHATS));
            var paged = listed(live.state(MESSAGES));
            assertTrue(resident <= 4_096 + 1, resident + " dropped resident chains listed");
            assertTrue(paged <= 4_096 + 1, paged + " dropped paged chains listed");
        }
    }
}
