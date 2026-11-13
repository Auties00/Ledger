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
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/// The ordered map of a key space keyed by the chains themselves: its comparator against the plain bytes for chains of
/// every form and segments on either side, its range views over segment bounds against a map of plain keys, the
/// dropped chains a replace leaves as keys, and a store with keys short enough for a chain's words and too long for
/// them, through scans, counts, checkpoints and a loss of power.
class OrderedMapKeysTest {
    private static final Keyspace<String, String> RESIDENT = Keyspace.resident("r", KeyCodec.utf8(), ValueCodec.utf8());
    private static final Keyspace<String, String> PAGED = Keyspace.paged("p", KeyCodec.utf8(), ValueCodec.utf8());

    private static MemorySegment key(String key) {
        return Segments.encodeKey(KeyCodec.utf8(), key);
    }

    // The empty key, every length up to past the longest a chain holds in its words, keys sharing all but their last
    // bytes, and keys longer than a slab's chunk takes
    private static List<byte[]> keys(SplittableRandom random) {
        var keys = new ArrayList<byte[]>();
        keys.add(new byte[0]);
        var base = bytes(random, 300);
        for (var length = 1; length <= 40; length++) {
            keys.add(Arrays.copyOf(base, length));
            var zeros = Arrays.copyOf(base, length);
            Arrays.fill(zeros, Math.max(0, length - 3), length, (byte) 0);
            keys.add(zeros);
            var ones = Arrays.copyOf(base, length);
            Arrays.fill(ones, Math.max(0, length - 2), length, (byte) 0xFF);
            keys.add(ones);
        }
        for (var length : new int[]{256, 257, 280, 300}) {
            keys.add(Arrays.copyOf(base, length));
            var last = Arrays.copyOf(base, length);
            last[length - 1] ^= (byte) 0x80;
            keys.add(last);
        }
        for (var i = 0; i < 40; i++) {
            keys.add(bytes(random, random.nextInt(41)));
        }
        return keys;
    }

    private static byte[] bytes(SplittableRandom random, int length) {
        var bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    // `key` in every form the map's keys and probes take: a chain of the slab (in its words up to 32 bytes, in a chunk
    // or an array of its own past them), a chain at an offset of a shared chunk, a chain of a segment of its own, and
    // segments of an array, read-only and a slice of a larger array
    private static List<Object> forms(KeySlab slab, byte[] key, SplittableRandom random) {
        var segment = MemorySegment.ofArray(key.clone());
        var hash = Segments.hash(segment);
        var offset = random.nextInt(1, 64);
        var chunk = new byte[offset + key.length + random.nextInt(8)];
        Arrays.fill(chunk, (byte) random.nextInt(256));
        System.arraycopy(key, 0, chunk, offset, key.length);
        var larger = new byte[key.length + 11];
        Arrays.fill(larger, (byte) 0xFF);
        System.arraycopy(key, 0, larger, 3, key.length);
        var forms = new ArrayList<Object>();
        forms.add(slab.chain(segment, hash));
        if (offset + key.length <= 4_096) {
            forms.add(new Chain(MemorySegment.ofArray(chunk).asReadOnly(), offset, key.length, hash));
        }
        forms.add(new Chain(MemorySegment.ofArray(key.clone()).asReadOnly(), hash));
        forms.add(MemorySegment.ofArray(key.clone()));
        forms.add(MemorySegment.ofArray(key.clone()).asReadOnly());
        forms.add(MemorySegment.ofArray(larger).asSlice(3, key.length));
        return forms;
    }

    @Test
    @DisplayName("orders chains and segments, on either side, as their bytes, and orders each pair both ways the opposite")
    void comparatorAgainstBytes() {
        var random = new SplittableRandom(0xC0FFEEL);
        var keys = keys(random);
        var slab = new KeySlab();
        var forms = new ArrayList<List<Object>>();
        for (var key : keys) {
            forms.add(forms(slab, key, random));
        }
        for (var i = 0; i < keys.size(); i++) {
            for (var j = 0; j < keys.size(); j++) {
                var expected = Integer.signum(Arrays.compareUnsigned(keys.get(i), keys.get(j)));
                var name = HexFormat.of().formatHex(keys.get(i)) + " against " + HexFormat.of().formatHex(keys.get(j));
                for (var left : forms.get(i)) {
                    for (var right : forms.get(j)) {
                        var order = KeyspaceState.BY_KEY.compare(left, right);
                        assertEquals(expected, Integer.signum(order), name + ", " + left.getClass().getSimpleName()
                                + " against " + right.getClass().getSimpleName());
                        assertEquals(-Integer.signum(order), Integer.signum(KeyspaceState.BY_KEY.compare(right, left)), name);
                    }
                }
            }
        }
    }

    @Test
    @DisplayName("orders random triples of chains and segments transitively")
    void comparatorTransitive() {
        var random = new SplittableRandom(91);
        var keys = keys(random);
        var slab = new KeySlab();
        var all = new ArrayList<Object>();
        for (var key : keys) {
            all.addAll(forms(slab, key, random));
        }
        var comparator = KeyspaceState.BY_KEY;
        for (var round = 0; round < 200_000; round++) {
            var a = all.get(random.nextInt(all.size()));
            var b = all.get(random.nextInt(all.size()));
            var c = all.get(random.nextInt(all.size()));
            if (comparator.compare(a, b) <= 0 && comparator.compare(b, c) <= 0) {
                assertTrue(comparator.compare(a, c) <= 0, "a <= b <= c, yet a > c");
            }
            if (comparator.compare(a, b) == 0) {
                assertEquals(Integer.signum(comparator.compare(a, c)), Integer.signum(comparator.compare(b, c)), "equal keys order apart");
            }
        }
    }

    @Test
    @DisplayName("the range views, the neighbours and the lookups of a map of chains over segment bounds and chain probes match a map of plain keys")
    void rangeViews() {
        var random = new SplittableRandom(4242);
        var slab = new KeySlab();
        var map = new ConcurrentSkipListMap<Object, Chain>(KeyspaceState.BY_KEY);
        var model = new TreeMap<byte[], Chain>(Arrays::compareUnsigned);
        var pool = keys(random);
        for (var i = 0; i < 600; i++) {
            var key = random.nextInt(3) == 0 ? pool.get(random.nextInt(pool.size())) : bytes(random, random.nextInt(48));
            var segment = MemorySegment.ofArray(key.clone());
            var chain = slab.chain(segment, Segments.hash(segment));
            if (map.putIfAbsent(chain, chain) == null) {
                model.put(key.clone(), chain);
            }
        }
        assertEquals(model.size(), map.size());
        assertEquals(new ArrayList<>(model.values()), new ArrayList<>(map.values()));
        for (var round = 0; round < 3_000; round++) {
            var low = random.nextBoolean() ? pool.get(random.nextInt(pool.size())) : bytes(random, random.nextInt(48));
            var high = random.nextBoolean() ? pool.get(random.nextInt(pool.size())) : bytes(random, random.nextInt(48));
            if (Arrays.compareUnsigned(low, high) > 0) {
                var swap = low;
                low = high;
                high = swap;
            }
            var from = MemorySegment.ofArray(low.clone());
            var to = MemorySegment.ofArray(high.clone());
            assertSameChains(model.subMap(low, true, high, false), map.subMap(from, true, to, false));
            assertSameChains(model.tailMap(low, true), map.tailMap(from, true));
            assertSameChains(model.headMap(high, false), map.headMap(to, false));
            assertSameChains(model.subMap(low, true, high, false).descendingMap(), map.subMap(from, true, to, false).descendingMap());
            assertSame(value(model.lowerEntry(high)), value(map.lowerEntry(to)));
            assertSame(value(model.higherEntry(low)), value(map.higherEntry(from)));
            assertSame(value(model.floorEntry(low)), value(map.floorEntry(from)));
            assertSame(value(model.ceilingEntry(high)), value(map.ceilingEntry(to)));
            assertSame(model.get(low), map.get(from));
            // A chain of the same bytes, as a walk or a removal probes the map with
            var probe = new Chain(MemorySegment.ofArray(low.clone()).asReadOnly(), 0);
            assertSame(model.get(low), map.get(probe));
            assertSame(value(model.lowerEntry(low)), value(map.lowerEntry(probe)));
            assertSame(value(model.higherEntry(low)), value(map.higherEntry(probe)));
        }
    }

    private static void assertSameChains(NavigableMap<byte[], Chain> expected, NavigableMap<Object, Chain> actual) {
        var expectedChains = new ArrayList<>(expected.values());
        var actualChains = new ArrayList<>(actual.values());
        assertEquals(expectedChains.size(), actualChains.size());
        for (var i = 0; i < expectedChains.size(); i++) {
            assertSame(expectedChains.get(i), actualChains.get(i), "chain " + i + " of a view");
        }
    }

    private static Chain value(Map.Entry<?, Chain> entry) {
        return entry == null ? null : entry.getValue();
    }

    private static KeyspaceState lazy(Keyspace<?, ?> keyspace) {
        var state = new KeyspaceState(keyspace);
        state.lazy = true;
        return state;
    }

    private static List<Chain> descending(KeyspaceState state) {
        var walk = state.walk(null, null, true);
        var chains = new ArrayList<Chain>();
        while (walk.next()) {
            assertTrue(walk.chain().keyEquals(walk.key()), "a walk's key is not its chain's");
            chains.add(walk.chain());
        }
        return chains;
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("a dropped chain a replace leaves as the key of its node keeps none of the chains that left the map in bulk below it")
    void replacedKeyKeepsNoChains(boolean longKeys) throws Exception {
        var state = lazy(PAGED);
        var count = 1_000;
        var format = longKeys ? "k%04d-%040d" : "k%04d";
        for (var i = 0; i < count; i++) {
            state.open(key(format.formatted(i, i)));
        }
        descending(state);
        var references = new ArrayList<WeakReference<Chain>>();
        Chain keyOfNode = null;
        for (var i = 0; i < count; i++) {
            var chain = state.chain(key(format.formatted(i, i)));
            if (i == count / 2) {
                keyOfNode = chain;
            } else {
                references.add(new WeakReference<>(chain));
            }
            // As a checkpoint drops every chain whose versions its tree holds
            state.drop(chain, null, () -> Long.MAX_VALUE);
        }
        // A commit writes the middle key again while its dropped chain is still in the map: the new chain replaces it,
        // which stays as the node's key
        var reopened = state.open(key(format.formatted(count / 2, count / 2)));
        state.slice(null, null);
        assertSame(reopened, state.entries.get(key(format.formatted(count / 2, count / 2))));
        assertNull(keyOfNode.below(), "the replaced chain kept its link");
        // The checkpoint's end: every other chain leaves the map at once, unlisted, and the map is relinked
        state.removeDropped();
        state.pruneListed(0);
        assertEquals(List.of(reopened), descending(state));
        assertSame(keyOfNode, state.entries.firstKey());
        var retained = retained(references);
        assertEquals(0, retained, retained + " chains that left stayed reachable");
        // The walk that stands on the replaced chain's place, and the walks after, still see the key
        assertEquals(List.of(reopened), descending(state));
    }

    private static int retained(List<WeakReference<Chain>> references) throws InterruptedException {
        var retained = references.size();
        for (var attempt = 0; attempt < 10 && retained > 0; attempt++) {
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

    @Test
    @DisplayName("a walk standing on a dropped chain that a replace took out of the map asks the map for the chain below")
    void walkOnReplaced() {
        var state = lazy(PAGED);
        for (var i = 0; i < 100; i++) {
            state.open(key("k%03d".formatted(i)));
        }
        descending(state);
        var walk = state.walk(null, null, true);
        while (walk.next() && !walk.chain().keyEquals(key("k050"))) {
            Thread.onSpinWait();
        }
        var standing = walk.chain();
        state.drop(standing, null, () -> Long.MAX_VALUE);
        var below = state.chain(key("k049"));
        state.drop(below, null, () -> Long.MAX_VALUE);
        var reopenedBelow = state.open(key("k049"));
        var reopened = state.open(key("k050"));
        state.slice(null, null);
        assertNull(standing.below());
        var rest = new ArrayList<Chain>();
        while (walk.next()) {
            assertTrue(walk.chain().keyEquals(walk.key()));
            rest.add(walk.chain());
        }
        assertEquals(50, rest.size());
        assertSame(reopenedBelow, rest.getFirst());
        assertTrue(rest.getLast().keyEquals(key("k000")));
        var all = descending(state);
        assertEquals(100, all.size());
        assertSame(reopened, all.get(49));
        assertSame(reopenedBelow, all.get(50));
    }

    // Keys of 5 to 64 bytes: in a chain's words up to 32, in the slab past them
    private static String storeKey(int number) {
        return "%04d".formatted(number) + "x".repeat(number * 7 % 61);
    }

    private static Ledger open(Path directory) {
        return Ledger.builder().directory(directory).encryption(Encryption.key(new byte[32]))
                .keyspace(RESIDENT).keyspace(PAGED).checkpointEvery(1 << 20).build();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @DisplayName("a store with keys in a chain's words and in the slab scans, counts and recovers after a loss of power what its commits wrote")
    void storeWithMixedKeys(boolean resident) throws Exception {
        var keyspace = resident ? RESIDENT : PAGED;
        var disk = CrashFileSystem.create();
        var model = new TreeMap<String, String>();
        var random = new Random(resident ? 11 : 12);
        CrashFileSystem survivor;
        try (var ledger = open(disk.getPath("/store"))) {
            for (var commit = 0; commit < 400; commit++) {
                var batch = ledger.newBatch();
                for (var i = 0; i < 20; i++) {
                    var key = storeKey(random.nextInt(2_000));
                    var kind = random.nextInt(60);
                    if (kind == 0) {
                        var to = storeKey(random.nextInt(2_000));
                        batch.deleteRange(keyspace, KeyRange.between(key, to));
                        if (key.compareTo(to) < 0) {
                            model.subMap(key, to).clear();
                        }
                    } else if (kind < 15) {
                        batch.delete(keyspace, key);
                        model.remove(key);
                    } else {
                        var value = "v" + random.nextInt(1_000_000);
                        batch.put(keyspace, key, value);
                        model.put(key, value);
                    }
                }
                ledger.commit(batch).awaitDurable();
                if (commit % 97 == 96) {
                    ledger.checkpoint();
                }
                if (commit % 50 == 49) {
                    check(ledger, keyspace, model, random);
                }
            }
            check(ledger, keyspace, model, random);
            survivor = disk.afterPowerLoss(new Random(random.nextLong()));
        }
        try (var ledger = open(survivor.getPath("/store"))) {
            check(ledger, keyspace, model, random);
            ledger.checkpoint();
            check(ledger, keyspace, model, random);
        }
    }

    // Checks the count, both whole scans and a few ranges of `ledger` against `model`
    private static void check(Ledger ledger, Keyspace<String, String> keyspace, TreeMap<String, String> model, Random random) {
        assertEquals(model.size(), ledger.size(keyspace), "count");
        var ascending = new ArrayList<Map.Entry<String, String>>();
        ledger.forEach(keyspace, KeyRange.all(), (key, value) -> ascending.add(Map.entry(key, value)));
        assertEquals(new ArrayList<>(model.entrySet()), ascending, "ascending scan");
        var descending = new ArrayList<Map.Entry<String, String>>();
        ledger.forEach(keyspace, KeyRange.<String>all().reversed(), (key, value) -> descending.add(Map.entry(key, value)));
        assertEquals(new ArrayList<>(model.entrySet()).reversed(), descending, "descending scan");
        for (var round = 0; round < 20; round++) {
            var low = storeKey(random.nextInt(2_000));
            var high = storeKey(random.nextInt(2_000));
            if (low.compareTo(high) > 0) {
                var swap = low;
                low = high;
                high = swap;
            }
            if (low.equals(high)) {
                continue;
            }
            var expected = new ArrayList<>(model.subMap(low, high).entrySet());
            var range = KeyRange.between(low, high);
            var page = ledger.range(keyspace, range.reversed(), 30);
            var expectedPage = expected.reversed().subList(0, Math.min(30, expected.size()));
            assertEquals(expectedPage.size(), page.size(), "a descending page of " + range);
            for (var i = 0; i < page.size(); i++) {
                assertEquals(expectedPage.get(i).getKey(), page.get(i).key(), "a descending page of " + range);
            }
            var visited = new ArrayList<String>();
            ledger.forEach(keyspace, range, (key, _) -> visited.add(key));
            assertEquals(expected.stream().map(Map.Entry::getKey).toList(), visited, "an ascending scan of " + range);
        }
    }
}
