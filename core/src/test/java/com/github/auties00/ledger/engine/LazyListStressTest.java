package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.ValueCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// Pushers, droppers, in-place unlinks and range reads on one lazily ordered key space at once.
class LazyListStressTest {
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("r", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAGED = Keyspace.paged("p", KeyCodec.utf8(), ValueCodec.utf8());

    private static int droppedListed(KeyspaceState state) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("droppedListed");
        field.setAccessible(true);
        return ((java.util.concurrent.atomic.AtomicInteger) field.get(state)).get();
    }

    private static Chain head(KeyspaceState state) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("unordered");
        field.setAccessible(true);
        return (Chain) field.get(state);
    }

    @org.junit.jupiter.api.Test
    @DisplayName("a push that lands while the head is being ordered sends its unlink through the predecessor search")
    void headRace() throws Exception {
        var state = new KeyspaceState(PAGED);
        state.lazy = true;
        var dropped = state.open(Segments.encodeKey(KeyCodec.utf8(), "x"));
        state.drop(dropped, null, () -> Long.MAX_VALUE);
        var head = state.open(Segments.encodeKey(KeyCodec.utf8(), "a"));
        var pushed = new Chain[2];
        var hooked = new java.util.concurrent.ConcurrentSkipListMap<java.lang.foreign.MemorySegment, Chain>(Segments.UNSIGNED) {
            @Override
            public Chain putIfAbsent(java.lang.foreign.MemorySegment key, Chain value) {
                if (pushed[0] == null) {
                    // Two commits list a chain each while the walk orders the head
                    pushed[0] = state.open(Segments.encodeKey(KeyCodec.utf8(), "b"));
                    pushed[1] = state.open(Segments.encodeKey(KeyCodec.utf8(), "c"));
                }
                return super.putIfAbsent(key, value);
            }
        };
        var entries = KeyspaceState.class.getDeclaredField("entries");
        entries.setAccessible(true);
        entries.set(state, hooked);
        state.pruneListed(0);
        var listed = new ArrayList<Chain>();
        for (var chain = head(state); chain != null; chain = chain.link) {
            listed.add(chain);
        }
        assertEquals(java.util.List.of(pushed[1], pushed[0]), listed);
        assertTrue(hooked.get(head.key()) == head);
        assertTrue(head.link == null && dropped.link == null);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("concurrent pushes, drops, in-place unlinks and range reads lose no live chain and leave the list sound")
    void stress(boolean resident) throws Exception {
        for (var round = 0; round < 6; round++) {
            round(resident);
        }
    }

    private void round(boolean resident) throws Exception {
        var state = new KeyspaceState(resident ? RESIDENT : PAGED);
        state.lazy = true;
        var pushers = 4;
        var perPusher = Integer.getInteger("stress.perPusher", 150_000);
        var made = new Chain[pushers][perPusher];
        var published = new AtomicIntegerArray(pushers);
        var failures = new ConcurrentLinkedQueue<String>();
        var stop = new AtomicBoolean();
        var threads = new ArrayList<Thread>();
        for (var p = 0; p < pushers; p++) {
            var pusher = p;
            threads.add(Thread.ofPlatform().start(() -> {
                var random = ThreadLocalRandom.current();
                for (var i = 0; i < perPusher; i++) {
                    var chain = state.open(Segments.encodeKey(KeyCodec.utf8(), "p%d-%07d".formatted(pusher, i)));
                    if (!resident && random.nextInt(3) == 0) {
                        // Written after the cut the prunes below pass: kept listed
                        chain.dirtyCut = 1_000_000;
                    }
                    made[pusher][i] = chain;
                    published.set(pusher, i + 1);
                    if (random.nextInt(10) < 6) {
                        state.drop(chain, null, () -> Long.MAX_VALUE);
                    }
                }
            }));
        }
        var dropper = Thread.ofPlatform().start(() -> {
            var random = ThreadLocalRandom.current();
            while (!stop.get()) {
                var p = random.nextInt(pushers);
                var count = published.get(p);
                if (count > 0) {
                    var chain = made[p][Math.max(0, count - 1 - random.nextInt(Math.min(count, 64)))];
                    state.drop(chain, null, () -> Long.MAX_VALUE);
                }
            }
        });
        var unlinker = Thread.ofPlatform().start(() -> {
            var turn = 0;
            while (!stop.get()) {
                if ((turn++ & 1) == 0) {
                    state.pruneListed(-1);
                } else {
                    state.pruneListed(0);
                }
                state.unlinkDropped();
            }
        });
        var reader = Thread.ofPlatform().start(() -> {
            var random = ThreadLocalRandom.current();
            while (!stop.get()) {
                var counts = new int[pushers];
                for (var p = 0; p < pushers; p++) {
                    counts[p] = published.get(p);
                }
                var slice = state.slice(null, null);
                for (var p = 0; p < pushers; p++) {
                    for (var k = 0; k < 256 && k < counts[p]; k++) {
                        var chain = made[p][counts[p] - 1 - k];
                        if (slice.get(chain.key()) != chain && !chain.released()) {
                            failures.add("live chain " + p + "/" + (counts[p] - 1 - k) + " missing from the range");
                        }
                    }
                }
                try {
                    Thread.sleep(random.nextInt(3));
                } catch (InterruptedException exception) {
                    return;
                }
            }
        });
        for (var thread : threads) {
            thread.join();
        }
        stop.set(true);
        dropper.join();
        unlinker.join();
        reader.join();
        assertTrue(failures.isEmpty(), failures.size() + " failures, first: " + failures.peek());

        // The list after the run: no cycle, no chain twice, every live chain listed or ordered
        Set<Chain> listed = Collections.newSetFromMap(new IdentityHashMap<>());
        var droppedInList = 0;
        var steps = 0;
        for (var chain = head(state); chain != null; chain = chain.link) {
            assertTrue(++steps <= pushers * perPusher, "cycle in the list");
            assertTrue(listed.add(chain), "a chain listed twice");
            if (chain.released()) {
                droppedInList++;
            }
        }
        var lost = 0;
        for (var row : made) {
            for (var chain : row) {
                if (!chain.released() && !listed.contains(chain) && state.entries.get(chain.key()) != chain) {
                    lost++;
                }
            }
        }
        assertEquals(0, lost, "live chains neither listed nor ordered");
        assertTrue(droppedListed(state) >= droppedInList,
                "counter " + droppedListed(state) + " below the " + droppedInList + " dropped chains listed");
        state.pruneListed(-1);
        for (var chain = head(state); chain != null; chain = chain.link) {
            assertTrue(!chain.released(), "a dropped chain stayed listed after a prune");
        }
    }
}
