package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Encryption;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.testkit.CrashFileSystem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The links from each chain of a lazily ordered key space to the one below it, which descending scans walk: what a
/// walk sees beside chains entering the map one at a time or in bulk, replacing dropped chains and leaving it, the
/// chains that left that the links keep reachable, with and without checkpoints, and a store's descending scans beside
/// its commits and checkpoints.
class ChainLinksTest {
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("r", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAGED = Keyspace.paged("p", KeyCodec.utf8(), ValueCodec.utf8());

    private static MemorySegment key(String key) {
        return Segments.encodeKey(KeyCodec.utf8(), key);
    }

    private static KeyspaceState lazy(Keyspace<?, ?> keyspace) {
        var state = new KeyspaceState(keyspace);
        state.lazy = true;
        return state;
    }

    private static List<Chain> descending(KeyspaceState state, MemorySegment from, MemorySegment to) {
        return collect(state.walk(from, to, true));
    }

    private static List<Chain> collect(KeyspaceState.Walk walk) {
        var chains = new ArrayList<Chain>();
        while (walk.next()) {
            assertTrue(walk.chain().keyEquals(walk.key()), "a walk's key is not its chain's");
            chains.add(walk.chain());
        }
        return chains;
    }

    private static int links(KeyspaceState state) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("links");
        field.setAccessible(true);
        return (int) field.get(state);
    }

    private static int left(KeyspaceState state) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("leftCount");
        field.setAccessible(true);
        return ((AtomicInteger) field.get(state)).get();
    }

    /// Replaces the ordered map of `state`, still empty, with one that calls `afterPut` once each insert of a chain
    /// returns, before the store links it from above.
    private static void hook(KeyspaceState state, BiConsumer<MemorySegment, Chain> afterPut) throws ReflectiveOperationException {
        var hooked = new ConcurrentSkipListMap<MemorySegment, Chain>(Segments.UNSIGNED) {
            @Override
            public Chain putIfAbsent(MemorySegment key, Chain value) {
                var previous = super.putIfAbsent(key, value);
                if (previous == null) {
                    afterPut.accept(key, value);
                }
                return previous;
            }

            @Override
            public boolean replace(MemorySegment key, Chain oldValue, Chain newValue) {
                var replaced = super.replace(key, oldValue, newValue);
                if (replaced) {
                    afterPut.accept(key, newValue);
                }
                return replaced;
            }
        };
        replaceEntries(state, hooked);
    }

    /// Replaces the ordered map of `state`, still empty, with one that counts the walks down it, each of which the entry
    /// of a chain makes for one of its neighbours or for its place.
    private static AtomicInteger countWalks(KeyspaceState state) throws ReflectiveOperationException {
        var walks = new AtomicInteger();
        replaceEntries(state, new ConcurrentSkipListMap<MemorySegment, Chain>(Segments.UNSIGNED) {
            @Override
            public Map.Entry<MemorySegment, Chain> lowerEntry(MemorySegment key) {
                walks.incrementAndGet();
                return super.lowerEntry(key);
            }

            @Override
            public Map.Entry<MemorySegment, Chain> higherEntry(MemorySegment key) {
                walks.incrementAndGet();
                return super.higherEntry(key);
            }

            @Override
            public Chain putIfAbsent(MemorySegment key, Chain value) {
                walks.incrementAndGet();
                return super.putIfAbsent(key, value);
            }

            @Override
            public boolean replace(MemorySegment key, Chain oldValue, Chain newValue) {
                walks.incrementAndGet();
                return super.replace(key, oldValue, newValue);
            }
        });
        return walks;
    }

    private static void replaceEntries(KeyspaceState state, ConcurrentSkipListMap<MemorySegment, Chain> entries) throws ReflectiveOperationException {
        var field = KeyspaceState.class.getDeclaredField("entries");
        field.setAccessible(true);
        field.set(state, entries);
    }

    /// Asserts that the links of each chain of the ordered map lead strictly down, through chains the map no longer
    /// holds, to the chain the map holds right below it.
    private static void assertLinksLead(KeyspaceState state) {
        var below = Chain.BOTTOM;
        var position = 0;
        for (var chain : state.entries.values()) {
            var at = chain;
            while (at.below() != below) {
                var next = at.below();
                assertNotNull(next, "the links of chain " + position + " of the map end at a cleared link");
                assertTrue(next != Chain.BOTTOM, "the links of chain " + position + " of the map skip the chain below it");
                assertTrue(Chain.compareKeys(next, at) < 0, "the links of chain " + position + " of the map go up");
                assertTrue(state.entries.get(next.key()) != next, "the links of chain " + position + " of the map skip the chain below it");
                at = next;
            }
            below = chain;
            position++;
        }
    }

    /// Asserts that each chain of the ordered map links the one the map holds right below it.
    private static void assertExactLinks(KeyspaceState state) {
        var below = Chain.BOTTOM;
        var position = 0;
        for (var chain : state.entries.values()) {
            assertSame(below, chain.below(), "chain " + position + " of the map links another than the one below it");
            below = chain;
            position++;
        }
    }

    /// {@return the chains the links reach from the chains of the ordered map that the map no longer holds}
    private static Set<Chain> leftOnLinks(KeyspaceState state) {
        Set<Chain> reached = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var chain : state.entries.values()) {
            for (var below = chain.below(); below != null && below != Chain.BOTTOM; below = below.below()) {
                if (state.entries.get(below.key()) == below || !reached.add(below)) {
                    break;
                }
            }
        }
        return reached;
    }

    private static void assertStrictlyDescending(List<Chain> chains) {
        for (var i = 1; i < chains.size(); i++) {
            assertTrue(Chain.compareKeys(chains.get(i - 1), chains.get(i)) > 0, "a walk went up at step " + i);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("the first descending walk links the map, and every chain ordered after links the one below it")
    void links(boolean resident) throws Exception {
        var state = lazy(resident ? RESIDENT : PAGED);
        var opened = new ArrayList<Chain>();
        for (var i = 0; i < 500; i++) {
            opened.add(state.open(key("k%04d".formatted(i * 2))));
        }
        assertEquals(0, links(state));
        assertEquals(500, state.slice(null, null).size());
        // Ordered unlinked, then linked by the first descending walk
        var walked = descending(state, null, null);
        assertEquals(2, links(state));
        assertEquals(opened.reversed(), walked);
        assertExactLinks(state);
        for (var i = 0; i < 100; i++) {
            opened.add(state.open(key("k%04d".formatted(i * 10 + 1))));
        }
        var all = descending(state, null, null);
        assertEquals(600, all.size());
        assertStrictlyDescending(all);
        assertExactLinks(state);
        var range = descending(state, key("k0100"), key("k0200"));
        assertEquals(60, range.size());
        assertTrue(range.getFirst().keyEquals(key("k0198")) && range.getLast().keyEquals(key("k0100")));
        assertTrue(descending(state, key("k2000"), null).isEmpty());
        assertEquals(1, descending(state, null, key("k0001")).size());
    }

    @Test
    @DisplayName("the chains a checkpoint's prune orders off the list are linked as they enter the map, the list beside them intact")
    void pruneLinks() throws Exception {
        var state = lazy(PAGED);
        for (var i = 0; i < 100; i++) {
            state.open(key("k%03d".formatted(i * 3)));
        }
        descending(state, null, null);
        var kept = new ArrayList<Chain>();
        for (var i = 0; i < 20; i++) {
            var chain = state.open(key("k%03d".formatted(i * 3 + 1)));
            if (i % 2 == 0) {
                // Written after the cut: kept listed
                chain.dirtyCut = 5;
                kept.add(chain);
            }
        }
        state.pruneListed(0);
        assertEquals(110, state.entries.size());
        assertExactLinks(state);
        var listed = new ArrayList<Chain>();
        var head = KeyspaceState.class.getDeclaredField("unordered");
        head.setAccessible(true);
        for (var chain = (Chain) head.get(state); chain != null; chain = chain.link) {
            listed.add(chain);
        }
        assertEquals(kept.reversed(), listed);
        assertEquals(120, descending(state, null, null).size());
        assertExactLinks(state);
    }

    @Test
    @DisplayName("a walk that starts at a chain the map just published, before the chain above links it, walks every chain below")
    void startInsideInsertWindow() throws Exception {
        var state = lazy(PAGED);
        var inside = new ArrayList<List<Chain>>();
        var linkedBelow = new Chain[1];
        hook(state, (key, chain) -> {
            if (chain.keyEquals(key("k0101"))) {
                linkedBelow[0] = chain.below();
                // A reader that found the list empty before this hold: it starts at the new chain, or above it
                inside.add(collect(new KeyspaceState.Walk(state.entries, null, key("k0102"))));
                inside.add(collect(new KeyspaceState.Walk(state.entries, null, null)));
            }
        });
        var even = new ArrayList<Chain>();
        for (var i = 0; i < 200; i++) {
            even.add(state.open(key("k%04d".formatted(i * 2))));
        }
        assertEquals(200, descending(state, null, null).size());
        var odd = state.open(key("k0101"));
        var after = descending(state, null, null);
        assertNotNull(linkedBelow[0], "the chain entered the map unlinked");
        assertSame(even.get(50), linkedBelow[0]);
        var fromNew = inside.get(0);
        assertSame(odd, fromNew.getFirst());
        assertEquals(even.subList(0, 51).reversed(), fromNew.subList(1, fromNew.size()));
        var fromTop = inside.get(1);
        assertStrictlyDescending(fromTop);
        assertTrue(fromTop.containsAll(even), "a walk from the top lost a chain the map held before the insert");
        assertEquals(201, after.size());
        assertExactLinks(state);
    }

    @Test
    @DisplayName("a chain that enters between two chains of the map walks it for the chain above and its place, and takes the chain below from the link above")
    void entersInTwoWalks() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        for (var i = 0; i < 100; i++) {
            state.open(key("k%03d".formatted(i * 2)));
        }
        descending(state, null, null);
        for (var i = 0; i < 99; i += 7) {
            walks.set(0);
            var entered = state.open(key("k%03d".formatted(i * 2 + 1)));
            state.slice(null, null);
            assertEquals(2, walks.get(), "the chain walked the map more than for its neighbours and its place");
            assertSame(state.chain(key("k%03d".formatted(i * 2))), entered.below());
            assertSame(entered, state.chain(key("k%03d".formatted(i * 2 + 2))).below());
        }
        assertExactLinks(state);
    }

    @Test
    @DisplayName("a chain that enters below every chain takes the bottom from the link above; one above every chain asks the map")
    void entersAtTheEnds() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        state.open(key("k010"));
        state.open(key("k020"));
        descending(state, null, null);
        walks.set(0);
        var bottom = state.open(key("k005"));
        state.slice(null, null);
        assertEquals(2, walks.get());
        assertSame(Chain.BOTTOM, bottom.below());
        walks.set(0);
        var top = state.open(key("k030"));
        state.slice(null, null);
        assertEquals(3, walks.get());
        assertSame(state.chain(key("k020")), top.below());
        assertExactLinks(state);
        assertEquals(4, descending(state, null, null).size());
    }

    /// Makes the ordered map of `state` hold k010, k020 and k040, linked, with k040 linking k030, which left.
    ///
    /// @return the chain of k030
    private static Chain leftBetween(KeyspaceState state) {
        state.open(key("k010"));
        state.open(key("k020"));
        var left = state.open(key("k030"));
        state.open(key("k040"));
        descending(state, null, null);
        state.drop(left, null, () -> Long.MAX_VALUE);
        state.removeDropped(Integer.MAX_VALUE);
        assertSame(left, state.chain(key("k040")).below());
        return left;
    }

    @Test
    @DisplayName("a chain that enters above one that left links it, in two walks, and a repair then points it past")
    void entersAboveLeft() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        var left = leftBetween(state);
        walks.set(0);
        var entered = state.open(key("k035"));
        state.slice(null, null);
        assertEquals(2, walks.get());
        assertSame(left, entered.below());
        assertSame(entered, state.chain(key("k040")).below());
        assertLinksLead(state);
        assertEquals(List.of(state.chain(key("k040")), entered, left, state.chain(key("k020")), state.chain(key("k010"))),
                descending(state, null, null));
        state.pruneListed(-1);
        assertNull(left.below());
        assertEquals(0, left(state));
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
    }

    @Test
    @DisplayName("a chain that enters below one that left, as the link of the chain above stops short of it, asks the map for the chain below")
    void entersBelowLeft() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        leftBetween(state);
        walks.set(0);
        var entered = state.open(key("k025"));
        state.slice(null, null);
        assertEquals(3, walks.get());
        assertSame(state.chain(key("k020")), entered.below());
        assertSame(entered, state.chain(key("k040")).below());
        assertExactLinks(state);
        assertEquals(4, descending(state, null, null).size());
        state.pruneListed(-1);
        assertEquals(0, left(state));
        assertExactLinks(state);
    }

    @Test
    @DisplayName("a chain that enters at the key of one that left, which the link of the chain above holds, asks the map for the chain below")
    void entersAtLeftKey() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        leftBetween(state);
        walks.set(0);
        var entered = state.open(key("k030"));
        state.slice(null, null);
        assertEquals(3, walks.get());
        assertSame(state.chain(key("k020")), entered.below());
        assertSame(entered, state.chain(key("k040")).below());
        assertExactLinks(state);
        state.pruneListed(-1);
        assertSame(entered, state.chain(key("k040")).below(), "a repair of the chain that left unlinked the one that took its key");
        assertExactLinks(state);
    }

    @Test
    @DisplayName("a chain that takes the place of a dropped chain the link of the chain above holds asks the map for the chain below")
    void replacesLinkAbove() throws Exception {
        var state = lazy(PAGED);
        var walks = countWalks(state);
        state.open(key("k010"));
        var dropped = state.open(key("k020"));
        var above = state.open(key("k030"));
        descending(state, null, null);
        state.drop(dropped, null, () -> Long.MAX_VALUE);
        assertSame(dropped, above.below());
        walks.set(0);
        var entered = state.open(key("k020"));
        state.slice(null, null);
        assertEquals(4, walks.get());
        assertSame(state.chain(key("k010")), entered.below());
        assertSame(entered, above.below());
        assertExactLinks(state);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("a chain dropped as it enters the map leaves no link above it, nor to the dropped chain it replaced")
    void leavesAtInsert(boolean replaces) throws Exception {
        var state = lazy(PAGED);
        var armed = new AtomicBoolean();
        hook(state, (key, chain) -> {
            if (armed.get()) {
                state.drop(chain, null, () -> Long.MAX_VALUE);
            }
        });
        state.open(key("k010"));
        var replaced = replaces ? state.open(key("k020")) : null;
        var above = state.open(key("k030"));
        descending(state, null, null);
        if (replaces) {
            state.drop(replaced, null, () -> Long.MAX_VALUE);
        }
        armed.set(true);
        var entered = state.open(key("k020"));
        state.slice(null, null);
        armed.set(false);
        assertTrue(entered.dropped());
        assertNull(state.entries.get(key("k020")));
        assertSame(state.chain(key("k010")), above.below());
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
        assertEquals(2, descending(state, null, null).size());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 2_000})
    @DisplayName("the chains that left that entering chains link are garbage after a checkpoint, through repairs of their links or a relink")
    void enteringKeepsNoLeft(int rounds) throws Exception {
        var state = lazy(PAGED);
        var references = new ArrayList<WeakReference<Chain>>();
        for (var i = 0; i < 10_000; i++) {
            state.open(key("k%05d".formatted(i * 2)));
        }
        descending(state, null, null);
        for (var i = 1; i <= rounds; i++) {
            var leaving = state.chain(key("k%05d".formatted(i * 2)));
            references.add(new WeakReference<>(leaving));
            state.drop(leaving, null, () -> Long.MAX_VALUE);
            state.removeDropped(Integer.MAX_VALUE);
            // Above the chain that left, below the next: the chain that enters links it
            var entered = state.open(key("k%05d".formatted(i * 2 + 1)));
            state.slice(null, null);
            assertSame(leaving, entered.below());
        }
        assertLinksLead(state);
        state.pruneListed(0);
        assertEquals(0, left(state));
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
        assertEquals(10_000, descending(state, null, null).size());
        assertEquals(0, retained(references, 0), "chains that left stayed reachable after a checkpoint");
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4})
    @DisplayName("random opens, drops, removals, repairs and bulks: the links of every chain lead to the chain below it, and a walk holds every chain of the map")
    void linksLeadUnderChurn(long seed) throws Exception {
        var state = lazy(PAGED);
        var random = new Random(seed);
        var keys = 3_000;
        descending(state, null, null);
        for (var step = 0; step < 30_000; step++) {
            var encoded = key("k%05d".formatted(random.nextInt(keys)));
            switch (random.nextInt(100)) {
                case 0 -> state.pruneListed(random.nextBoolean() ? -1 : Long.MAX_VALUE);
                case 1 -> {
                    state.removeDropped();
                    state.unlinkDropped();
                }
                case 2 -> {
                    if (random.nextInt(20) == 0) {
                        // More than a hold links one at a time
                        for (var i = 0; i < 1_500; i++) {
                            state.open(key("k%05d".formatted(random.nextInt(keys))));
                        }
                    }
                }
                default -> {
                    var kind = random.nextInt(10);
                    if (kind < 5) {
                        state.open(encoded);
                    } else if (kind < 8) {
                        var chain = state.chain(encoded);
                        if (chain != null) {
                            state.drop(chain, null, () -> Long.MAX_VALUE);
                        }
                    } else if (kind < 9) {
                        state.removeDropped(1 + random.nextInt(8));
                    } else {
                        state.slice(null, null);
                    }
                }
            }
            if (step % 200 == 0) {
                var walked = descending(state, null, null);
                assertStrictlyDescending(walked);
                assertLinksLead(state);
                var inMap = walked.stream().filter(chain -> state.entries.get(chain.key()) == chain).toList();
                assertEquals(new ArrayList<>(state.entries.values()).reversed(), inMap, "a walk differs from the map");
            }
        }
        state.removeDropped();
        state.pruneListed(-1);
        assertEquals(0, left(state));
        descending(state, null, null);
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
    }

    @Test
    @DisplayName("a hold that orders more chains than it links one at a time inserts the rest unlinked, which walks begun before it or landing on them still read whole, and relinks the map")
    void bulk() throws Exception {
        var state = lazy(PAGED);
        var armed = new AtomicBoolean();
        var inserts = new AtomicInteger();
        var during = new ArrayList<List<Chain>>();
        var landed = new ArrayList<List<Chain>>();
        var linkStates = new ArrayList<Integer>();
        var old = new ArrayList<Chain>();
        hook(state, (key, chain) -> {
            // Past the 1,024 chains the hold links one at a time
            if (armed.get() && inserts.incrementAndGet() == 2_000) {
                try {
                    linkStates.add(links(state));
                } catch (ReflectiveOperationException exception) {
                    throw new AssertionError(exception);
                }
                // A reader that took a walk along the links before this hold
                during.add(collect(new KeyspaceState.Walk(state.entries, null, null)));
                // One that lands on this chain, unlinked
                landed.add(collect(new KeyspaceState.Walk(state.entries, null, Segments.encodeKey(KeyCodec.utf8(),
                        KeyCodec.utf8().decode(key) + "\0"))));
            }
        });
        for (var i = 0; i < 2_000; i++) {
            old.add(state.open(key("k%05d".formatted(i * 2))));
        }
        assertEquals(2_000, descending(state, null, null).size());
        assertEquals(2, links(state));
        armed.set(true);
        var added = new ArrayList<Chain>();
        for (var i = 0; i < 3_000; i++) {
            added.add(state.open(key("k%05d".formatted(i * 2 + 1))));
        }
        var all = descending(state, null, null);
        assertEquals(List.of(1), linkStates, "the hold linked past its share of the chains one at a time");
        var before = during.getFirst();
        assertStrictlyDescending(before);
        assertTrue(before.containsAll(old), "a walk begun before the bulk lost a chain the map held");
        var fromUnlinked = landed.getFirst();
        assertStrictlyDescending(fromUnlinked);
        assertTrue(added.contains(fromUnlinked.getFirst()), "the walk did not start at the unlinked chain");
        assertTrue(fromUnlinked.getFirst().below() != null, "the relink left the chain unlinked");
        var start = fromUnlinked.getFirst();
        for (var chain : old) {
            if (Chain.compareKeys(chain, start) < 0) {
                assertTrue(fromUnlinked.contains(chain), "a walk from an unlinked chain lost a chain below it");
            }
        }
        assertEquals(5_000, all.size());
        assertStrictlyDescending(all);
        assertEquals(2, links(state));
        assertExactLinks(state);
        assertTrue(all.containsAll(added));
    }

    @Test
    @DisplayName("a chain that takes the place of a dropped chain of its key in the map is linked in its place")
    void replaceDropped() throws Exception {
        var state = lazy(PAGED);
        for (var i = 0; i < 100; i++) {
            state.open(key("k%03d".formatted(i)));
        }
        descending(state, null, null);
        var dropped = state.chain(key("k050"));
        state.drop(dropped, null, () -> Long.MAX_VALUE);
        // Still in the map, emptied, until a commit takes it out
        assertSame(dropped, state.entries.get(key("k050")));
        var reopened = state.open(key("k050"));
        var walked = descending(state, null, null);
        assertEquals(100, walked.size());
        assertSame(reopened, walked.get(49));
        assertSame(reopened, state.entries.get(key("k050")));
        assertExactLinks(state);
        state.removeDropped(Integer.MAX_VALUE);
        assertEquals(0, left(state), "a chain replaced in the map was listed as one that left it");
        assertSame(reopened, state.entries.get(key("k050")));
        // Dropped again, unseen by the map until it is reordered: the map takes it out at the insert
        state.drop(reopened, null, () -> Long.MAX_VALUE);
        var third = state.open(key("k050"));
        state.drop(third, null, () -> Long.MAX_VALUE);
        descending(state, null, null);
        state.removeDropped(Integer.MAX_VALUE);
        state.pruneListed(-1);
        assertEquals(99, descending(state, null, null).size());
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
    }

    @Test
    @DisplayName("chains that left the map stay on the links within a thousand without a checkpoint, and none stays after one; then they are garbage")
    void leftChainsLetGo() throws Exception {
        var state = lazy(PAGED);
        var count = 20_000;
        var references = new ArrayList<WeakReference<Chain>>();
        for (var i = 0; i < count; i++) {
            state.open(key("k%05d".formatted(i)));
        }
        descending(state, null, null);
        for (var i = 0; i < count; i++) {
            // Every tenth stays, so the links span what left
            if (i % 10 != 0) {
                var chain = state.chain(key("k%05d".formatted(i)));
                references.add(new WeakReference<>(chain));
                state.drop(chain, null, () -> Long.MAX_VALUE);
            }
        }
        // The commits alone, as while checkpoints fail: each takes out its share and repairs once a thousand left
        for (var commit = 0; commit < 200; commit++) {
            state.removeDropped(256);
            state.unlinkDropped();
        }
        var onLinks = leftOnLinks(state).size();
        assertTrue(onLinks <= 1_024, onLinks + " chains that left stayed on the links");
        assertTrue(left(state) < 1_024, left(state) + " chains that left listed for a repair");
        assertEquals(count / 10, live(descending(state, null, null)));
        var retained = retained(references, 1_024);
        assertTrue(retained <= 1_024, retained + " chains that left stayed reachable");
        // A checkpoint's prune lets go of the rest
        state.pruneListed(0);
        assertEquals(0, left(state));
        assertTrue(leftOnLinks(state).isEmpty());
        assertExactLinks(state);
        assertEquals(count / 10, descending(state, null, null).size());
        assertEquals(0, retained(references, 0), "chains that left stayed reachable after a checkpoint");
    }

    @Test
    @DisplayName("a walk standing on a chain that left, whose link a repair cleared, asks the map for the next chain and misses none")
    void walkOnClearedChain() throws Exception {
        var state = lazy(PAGED);
        for (var i = 0; i < 100; i++) {
            state.open(key("k%03d".formatted(i)));
        }
        descending(state, null, null);
        var walk = state.walk(null, null, true);
        while (walk.next() && !walk.chain().keyEquals(key("k050"))) {
            Thread.onSpinWait();
        }
        var standing = walk.chain();
        state.drop(standing, null, () -> Long.MAX_VALUE);
        state.removeDropped(Integer.MAX_VALUE);
        assertEquals(1, left(state));
        state.pruneListed(-1);
        assertEquals(0, left(state));
        assertNull(standing.below());
        assertExactLinks(state);
        var rest = collect(walk);
        assertEquals(50, rest.size());
        assertStrictlyDescending(rest);
        assertTrue(rest.getFirst().keyEquals(key("k049")) && rest.getLast().keyEquals(key("k000")));
    }

    @Test
    @DisplayName("a checkpoint that takes most of the map out relinks the map rather than repair each link, and lets go of every chain")
    void checkpointRelinks() throws Exception {
        var state = lazy(PAGED);
        var references = new ArrayList<WeakReference<Chain>>();
        for (var i = 0; i < 10_000; i++) {
            state.open(key("k%05d".formatted(i)));
        }
        descending(state, null, null);
        for (var i = 0; i < 10_000; i++) {
            if (i % 100 != 0) {
                var chain = state.chain(key("k%05d".formatted(i)));
                references.add(new WeakReference<>(chain));
                state.drop(chain, null, () -> Long.MAX_VALUE);
            }
        }
        state.removeDropped();
        assertEquals(0, left(state), "chains that left at once were listed one by one");
        // Until the prune, a walk steps over them, emptied
        assertEquals(100, live(descending(state, null, null)));
        state.pruneListed(0);
        assertExactLinks(state);
        assertTrue(leftOnLinks(state).isEmpty());
        assertEquals(100, descending(state, null, null).size());
        assertEquals(0, retained(references, 0));
    }

    private static int live(List<Chain> chains) {
        var live = 0;
        for (var chain : chains) {
            if (!chain.released()) {
                live++;
            }
        }
        return live;
    }

    /// {@return how many of `references` the collector has not cleared, once at most `bound` or after a few
    /// collections}
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("descending walks and removals between new keys of 20 bytes keep no copies of keys beside the map's: the key space's slab holds about the bytes of the keys it ordered")
    void walksKeepNoKeyCopies(boolean resident) throws Exception {
        var state = lazy(resident ? RESIDENT : PAGED);
        descending(state, null, null);
        var chunks = new ArrayList<WeakReference<byte[]>>();
        var ordered = 0;
        for (var round = 0; round < 400; round++) {
            // 20 bytes, a key a chain holds in its words, in no order, so each enters among the others
            state.open(key("k%019d".formatted(round * 2_654_435_761L % 1_000_000_000_000L)));
            ordered++;
            if (!resident && round % 4 == 3) {
                // A key that leaves, as after a checkpoint: its removal and the repair of the link above it look it up
                var leaving = state.entries.firstEntry().getValue();
                state.drop(leaving, null, () -> Long.MAX_VALUE);
                state.removeDropped(Integer.MAX_VALUE);
            }
            var walk = state.walk(null, null, true);
            slabChunks(state, chunks);
            var walked = 0;
            while (walk.next()) {
                assertTrue(walk.chain().keyEquals(walk.key()), "a walk's key is not its chain's");
                walked++;
            }
            assertTrue(walked >= state.entries.size(), "the walk missed chains of the map");
            slabChunks(state, chunks);
        }
        var keyBytes = 20L * ordered;
        var held = 0L;
        for (var attempt = 0; attempt < 10; attempt++) {
            System.gc();
            Thread.sleep(20);
            held = 0;
            for (var reference : chunks) {
                var array = reference.get();
                if (array != null) {
                    held += array.length;
                }
            }
            if (held <= 2 * keyBytes + 4_096) {
                break;
            }
        }
        // The map's copies fill chunks that double up to 4 KiB, the last one partly
        assertTrue(held <= 2 * keyBytes + 4_096, "the slab's chunks keep " + held + " bytes for " + keyBytes + " bytes of keys");
        Reference.reachabilityFence(state);
    }

    // Adds to `chunks` the arrays of the chunks the stripes of the key space's slab hold, once each
    private static void slabChunks(KeyspaceState state, List<WeakReference<byte[]>> chunks) throws ReflectiveOperationException {
        var keysField = KeyspaceState.class.getDeclaredField("keys");
        keysField.setAccessible(true);
        var slab = keysField.get(state);
        var stripesField = KeySlab.class.getDeclaredField("stripes");
        stripesField.setAccessible(true);
        var stripes = (AtomicReferenceArray<?>) stripesField.get(slab);
        for (var i = 0; i < stripes.length(); i++) {
            var chunk = stripes.get(i);
            if (chunk == null) {
                continue;
            }
            var arrayField = chunk.getClass().getDeclaredField("array");
            arrayField.setAccessible(true);
            var array = (byte[]) arrayField.get(chunk);
            if (chunks.stream().noneMatch(reference -> reference.refersTo(array))) {
                chunks.add(new WeakReference<>(array));
            }
        }
    }

    private static int retained(List<WeakReference<Chain>> references, int bound) throws InterruptedException {
        var retained = references.size();
        for (var attempt = 0; attempt < 10 && retained > bound; attempt++) {
            System.gc();
            Thread.sleep(20);
            retained = 0;
            for (var reference : references) {
                if (reference.get() != null) {
                    retained++;
                }
            }
        }
        return retained;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("concurrent opens, drops, removals, repairs, prunes and descending walks: every walk goes down and holds every chain live before and after it")
    void stress(boolean resident) throws Exception {
        for (var round = 0; round < 3; round++) {
            round(resident);
        }
    }

    private void round(boolean resident) throws Exception {
        var state = lazy(resident ? RESIDENT : PAGED);
        var pushers = 3;
        var keys = 6_000;
        var opened = new AtomicReferenceArray<Chain>(keys);
        var encoded = new MemorySegment[keys];
        for (var i = 0; i < keys; i++) {
            encoded[i] = key("k%05d".formatted(i));
        }
        var failures = new ConcurrentLinkedQueue<String>();
        var stop = new AtomicBoolean();
        var walks = new AtomicInteger();
        var threads = new ArrayList<Thread>();
        for (var p = 0; p < pushers; p++) {
            var pusher = p;
            threads.add(Thread.ofPlatform().start(() -> {
                var random = ThreadLocalRandom.current();
                while (!stop.get()) {
                    // Each pusher opens its own keys, so the chain it records is listed before any walk reads it
                    var i = random.nextInt(keys / pushers) * pushers + pusher;
                    var chain = state.open(encoded[i]);
                    opened.set(i, chain);
                    if (random.nextInt(4) == 0) {
                        state.drop(chain, null, () -> Long.MAX_VALUE);
                    }
                    if (random.nextInt(2_000) == 0) {
                        // A burst for a walk to order in bulk
                        for (var burst = 0; burst < 2_000; burst++) {
                            var j = random.nextInt(keys / pushers) * pushers + pusher;
                            opened.set(j, state.open(encoded[j]));
                        }
                    }
                }
            }));
        }
        threads.add(Thread.ofPlatform().start(() -> {
            var random = ThreadLocalRandom.current();
            while (!stop.get()) {
                var chain = opened.get(random.nextInt(keys));
                if (chain != null) {
                    state.drop(chain, null, () -> Long.MAX_VALUE);
                }
            }
        }));
        // The commits' maintenance
        threads.add(Thread.ofPlatform().start(() -> {
            while (!stop.get()) {
                state.removeDropped(256);
                state.unlinkDropped();
            }
        }));
        // The checkpoints' end
        threads.add(Thread.ofPlatform().start(() -> {
            var random = ThreadLocalRandom.current();
            while (!stop.get()) {
                state.removeDropped();
                state.pruneListed(switch (random.nextInt(3)) {
                    case 0 -> -1;
                    case 1 -> 0;
                    default -> Long.MAX_VALUE;
                });
                try {
                    Thread.sleep(random.nextInt(4));
                } catch (InterruptedException exception) {
                    return;
                }
            }
        }));
        for (var r = 0; r < 2; r++) {
            threads.add(Thread.ofPlatform().start(() -> {
                var random = ThreadLocalRandom.current();
                while (!stop.get()) {
                    var low = random.nextInt(4) == 0 ? -1 : random.nextInt(keys);
                    var high = random.nextInt(4) == 0 ? -1 : low + 1 + random.nextInt(keys / 4);
                    var from = low < 0 ? null : encoded[low];
                    var to = high < 0 || high >= keys ? null : encoded[high];
                    var before = new Chain[keys];
                    for (var i = 0; i < keys; i++) {
                        before[i] = opened.get(i);
                    }
                    var walked = descending(state, from, to);
                    Set<Chain> seen = Collections.newSetFromMap(new IdentityHashMap<>());
                    for (var i = 0; i < walked.size(); i++) {
                        var chain = walked.get(i);
                        if (!seen.add(chain)) {
                            failures.add("a chain walked twice");
                        }
                        if (i > 0 && Chain.compareKeys(walked.get(i - 1), chain) <= 0) {
                            failures.add("a walk went up");
                        }
                        if (from != null && chain.compareKey(from) < 0 || to != null && chain.compareKey(to) >= 0) {
                            failures.add("a chain out of the range");
                        }
                    }
                    var start = low < 0 ? 0 : low;
                    var end = to == null ? keys : high;
                    for (var i = start; i < end; i++) {
                        var chain = before[i];
                        // Live before the walk and still after it: never out of the map meanwhile
                        if (chain != null && !chain.released() && !seen.contains(chain)) {
                            failures.add("live chain " + i + " missing from a walk of [" + low + ", " + high + ")");
                        }
                    }
                    walks.incrementAndGet();
                    if (random.nextInt(3) == 0) {
                        Thread.onSpinWait();
                    }
                }
            }));
        }
        Thread.sleep(1_500);
        stop.set(true);
        for (var thread : threads) {
            thread.join();
        }
        assertTrue(failures.isEmpty(), failures.size() + " failures in " + walks.get() + " walks, first: " + failures.peek());
        assertTrue(walks.get() > 20, "only " + walks.get() + " walks");

        // Quiet: every live chain on one walk down the map, which links each to the one below it
        state.removeDropped();
        state.pruneListed(-1);
        var walked = descending(state, null, null);
        Set<Chain> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        seen.addAll(walked);
        for (var i = 0; i < keys; i++) {
            var chain = opened.get(i);
            if (chain != null && !chain.released()) {
                assertTrue(seen.contains(chain), "live chain " + i + " missing once quiet");
            }
        }
        state.removeDropped();
        state.pruneListed(-1);
        assertEquals(0, left(state));
        assertTrue(leftOnLinks(state).isEmpty(), "chains that left stayed on the links once quiet");
        assertExactLinks(state);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("a descending scan of a snapshot, beside commits, range removals and checkpoints, returns what its ascending scan does, reversed")
    void scansBesideCommits(boolean resident) throws Exception {
        var keyspace = resident ? RESIDENT : PAGED;
        var disk = CrashFileSystem.create();
        try (var ledger = Ledger.builder().directory(disk.getPath("/store")).encryption(Encryption.key(new byte[32]))
                .keyspace(RESIDENT).keyspace(PAGED).checkpointEvery(1 << 20).build()) {
            var model = new TreeMap<String, String>();
            var stop = new AtomicBoolean();
            var failures = new ConcurrentLinkedQueue<String>();
            var scans = new AtomicInteger();
            var threads = new ArrayList<Thread>();
            for (var r = 0; r < 2; r++) {
                threads.add(Thread.ofPlatform().start(() -> {
                    var random = ThreadLocalRandom.current();
                    while (!stop.get()) {
                        var low = "k%04d".formatted(random.nextInt(3_000));
                        var high = "k%04d".formatted(random.nextInt(3_000));
                        KeyRange<String> range = switch (random.nextInt(4)) {
                            case 0 -> KeyRange.all();
                            case 1 -> KeyRange.atLeast(low);
                            case 2 -> KeyRange.below(high);
                            default -> low.compareTo(high) < 0 ? KeyRange.between(low, high) : KeyRange.between(high, low);
                        };
                        try (var snapshot = ledger.snapshot()) {
                            var ascending = new ArrayList<Map.Entry<String, String>>();
                            snapshot.forEach(keyspace, range, (key, value) -> ascending.add(Map.entry(key, value)));
                            var descending = new ArrayList<Map.Entry<String, String>>();
                            snapshot.forEach(keyspace, range.reversed(), (key, value) -> descending.add(Map.entry(key, value)));
                            if (!descending.equals(ascending.reversed())) {
                                failures.add("a descending scan of " + range + " returned " + descending.size()
                                        + " entries where the ascending one returned " + ascending.size());
                            }
                            var page = snapshot.range(keyspace, range.reversed(), 50);
                            var expected = ascending.reversed().subList(0, Math.min(50, ascending.size()));
                            if (page.size() != expected.size()) {
                                failures.add("a page of " + range + " held " + page.size() + " entries, not " + expected.size());
                            } else {
                                for (var i = 0; i < page.size(); i++) {
                                    if (!page.get(i).key().equals(expected.get(i).getKey())
                                            || !page.get(i).value().equals(expected.get(i).getValue())) {
                                        failures.add("a page of " + range + " differs at " + i);
                                        break;
                                    }
                                }
                            }
                        }
                        scans.incrementAndGet();
                    }
                }));
            }
            threads.add(Thread.ofPlatform().start(() -> {
                while (!stop.get()) {
                    ledger.checkpoint();
                    try {
                        Thread.sleep(15);
                    } catch (InterruptedException exception) {
                        return;
                    }
                }
            }));
            var random = new Random(7);
            var deadline = System.nanoTime() + 2_000_000_000L;
            while (System.nanoTime() < deadline) {
                var batch = ledger.newBatch();
                var writes = 1 + random.nextInt(30);
                for (var i = 0; i < writes; i++) {
                    var key = "k%04d".formatted(random.nextInt(3_000));
                    var kind = random.nextInt(40);
                    if (kind == 0) {
                        var to = "k%04d".formatted(random.nextInt(3_000));
                        batch.deleteRange(keyspace, KeyRange.between(key, to));
                        if (key.compareTo(to) < 0) {
                            model.subMap(key, to).clear();
                        }
                    } else if (kind < 12) {
                        batch.delete(keyspace, key);
                        model.remove(key);
                    } else {
                        var value = "v" + random.nextInt(1_000_000);
                        batch.put(keyspace, key, value);
                        model.put(key, value);
                    }
                }
                ledger.commit(batch);
            }
            stop.set(true);
            for (var thread : threads) {
                thread.join();
            }
            assertTrue(failures.isEmpty(), failures.size() + " failures in " + scans.get() + " scans, first: " + failures.peek());
            assertTrue(scans.get() > 10, "only " + scans.get() + " scans");
            var descending = new ArrayList<Map.Entry<String, String>>();
            ledger.forEach(keyspace, KeyRange.<String>all().reversed(), (key, value) -> descending.add(Map.entry(key, value)));
            assertEquals(new ArrayList<>(model.entrySet()).reversed(), descending);
        }
    }
}
