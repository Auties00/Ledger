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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
        for (var chain = (Chain) field.get(state); chain != null; chain = chain.link) {
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
    @DisplayName("a read of a range finds a chain a commit found in the index before the thread that created it listed it")
    void chainNotListedYet() throws Exception {
        var state = new KeyspaceState(MESSAGES);
        state.lazy = true;
        var key = Segments.encodeKey(KeyCodec.utf8(), "k");
        // The creator is descheduled between its insert into the index and its push on the list
        var created = new Chain(key, Segments.hash(key));
        var index = KeyspaceState.class.getDeclaredField("index");
        index.setAccessible(true);
        ((ChainIndex) index.get(state)).putIfAbsent(created);
        // Another commit opens the key, and must not publish a version of it before a read of a range can find it
        try (var other = Executors.newSingleThreadExecutor()) {
            var opened = other.submit(() -> state.open(key));
            assertThrows(TimeoutException.class, () -> opened.get(200, TimeUnit.MILLISECONDS));
            // The creator goes on, and lists the chain with no other thread writing its link
            var list = KeyspaceState.class.getDeclaredMethod("list", Chain.class);
            list.setAccessible(true);
            list.invoke(state, created);
            assertSame(created, opened.get(30, TimeUnit.SECONDS));
        }
        assertSame(created, state.slice(null, null).get(key));
        assertEquals(1, state.slice(null, null).size());
        assertEquals(0, listed(state));
    }

    /// Runs `task` on a daemon thread of its own, which a test that fails leaves behind rather than waits for.
    private static <T> java.util.concurrent.Future<T> daemon(java.util.concurrent.Callable<T> task) {
        var future = new java.util.concurrent.FutureTask<>(task);
        Thread.ofPlatform().daemon().start(future);
        return future;
    }

    /// Puts a chain of `key` in the index of `state` without listing it, as a creator stopped between the two leaves it.
    private static Chain unlisted(KeyspaceState state, java.lang.foreign.MemorySegment key) throws ReflectiveOperationException {
        var created = new Chain(key, Segments.hash(key));
        var index = KeyspaceState.class.getDeclaredField("index");
        index.setAccessible(true);
        ((ChainIndex) index.get(state)).putIfAbsent(created);
        return created;
    }

    @Test
    @DisplayName("a commit waiting for the creator of a chain to list it goes on once the creator, failing, drops it, and the key gets a chain of its own")
    void creatorFails() throws Exception {
        var state = new KeyspaceState(MESSAGES);
        state.lazy = true;
        var key = Segments.encodeKey(KeyCodec.utf8(), "k");
        var created = unlisted(state, key);
        var opened = daemon(() -> state.open(key));
        assertThrows(TimeoutException.class, () -> opened.get(200, TimeUnit.MILLISECONDS));
        // What the creator does as its push on the list throws
        var abandon = KeyspaceState.class.getDeclaredMethod("abandon", Chain.class);
        abandon.setAccessible(true);
        abandon.invoke(state, created);
        assertSame(created, opened.get(30, TimeUnit.SECONDS));
        assertTrue(created.released());
        var reopened = state.open(key);
        assertNotSame(created, reopened);
        assertTrue(reopened.reachable());
        assertSame(reopened, state.slice(null, null).get(key));
        assertEquals(1, state.slice(null, null).size());
    }

    @Test
    @DisplayName("a close ends the wait of a commit for the creator of a chain that never lists it, and the commit it acknowledged survives")
    void closeEndsWait(@TempDir Path directory) throws Exception {
        var options = Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32])).keyspace(MESSAGES);
        var ledger = (LiveLedger) options.build();
        unlisted(ledger.state(MESSAGES), Segments.encodeKey(KeyCodec.utf8(), "k"));
        var acknowledged = new java.util.concurrent.atomic.AtomicBoolean();
        var commit = daemon(() -> {
            try {
                ledger.commit(ledger.newBatch().put(MESSAGES, "k", "v")).awaitDurable();
                acknowledged.set(true);
            } catch (IllegalStateException _) {
                // The store closed before the commit had a sequence
            }
            return null;
        });
        assertThrows(TimeoutException.class, () -> commit.get(200, TimeUnit.MILLISECONDS));
        daemon(() -> {
            ledger.close();
            return null;
        }).get(60, TimeUnit.SECONDS);
        commit.get(60, TimeUnit.SECONDS);
        try (var reopened = options.build()) {
            assertEquals(acknowledged.get() ? java.util.Optional.of("v") : java.util.Optional.empty(), reopened.get(MESSAGES, "k"));
        }
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
