package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

/// The reads of [com.github.auties00.ledger.View], over the entries a [ReadPoint] sees: the versions in memory up
/// to its bound, over the tree of its checkpoint.
///
/// A stamp is a commit sequence shifted left by [#OP_BITS], plus the position of the write in its batch: later writes
/// of one batch override earlier ones, range removals included. A reader at sequence `s` reads with bound
/// [#boundOf(long)] of `s`, which takes in every write of every commit up to `s`.
///
/// A key with a version in memory at or below the bound reads that version; any other key reads the tree, unless a
/// range removal stamped after the tree's checkpoint and within the bound covers it.
///
/// A resident key space reads memory alone, but for the keys its loader has not put there yet ([ResidentLoader]):
/// those it reads from the tree alone, where every read finds them as it sees them. A read checks where a key is
/// before it reads memory, so the chains the loader installed before it moved past the key are there. A scan does so
/// as it advances ([ResidentScan]).
final class Reads {
    /// The bits of a stamp that number the writes of one batch.
    static final int OP_BITS = 24;
    /// The most writes one batch holds.
    static final int MAX_OPS = (1 << OP_BITS) - 1;

    private Reads() {
    }

    /// {@return the stamp of write `op` of commit `sequence`}
    static long stamp(long sequence, int op) {
        return (sequence << OP_BITS) | op;
    }

    /// {@return the bound of a reader at commit `sequence`}
    static long boundOf(long sequence) {
        return (sequence << OP_BITS) | MAX_OPS;
    }

    /// {@return the value of an entry a scan found: the decoded value of a resident version, or `value` decoded}
    ///
    /// @param decoded the decoded value of a resident version, or `null` for an entry of a tree
    @SuppressWarnings("unchecked")
    private static <V> V value(KeyspaceState state, Keyspace<?, V> keyspace, MemorySegment value, Object decoded) {
        return state.resident && decoded != null ? (V) decoded : keyspace.values().decode(value);
    }

    /// {@return whether a range removal the tree of `point` does not hold, and `point` sees, covers `key`}
    static boolean hiddenSinceTree(MemorySegment key, ReadPoint point, RangeRemoval[] removals) {
        var treeBound = point.treeBound();
        for (var removal : removals) {
            if (removal.stamp() > treeBound && removal.stamp() <= point.bound() && removal.covers(key)) {
                return true;
            }
        }
        return false;
    }

    /// {@return whether a range removal the tree of `point` does not hold, and `point` sees, covers the key whose bytes
    /// are `key`}
    private static boolean hiddenSinceTree(byte[] key, ReadPoint point, RangeRemoval[] removals) {
        var treeBound = point.treeBound();
        for (var removal : removals) {
            if (removal.stamp() > treeBound && removal.stamp() <= point.bound() && removal.covers(key)) {
                return true;
            }
        }
        return false;
    }

    /// {@return the value of `key` in the tree of `point` under `root`, or `null` if it holds none or a later removal
    /// hides it}
    ///
    /// @param root the root of the key space's tree, or `null` if the read uses none
    private static MemorySegment treeValue(byte[] key, ReadPoint point, TreeRoot root, RangeRemoval[] removals) {
        if (root == null) {
            return null;
        }
        var unit = TreeSearch.leaf(point.pages(), root, key);
        var entry = TreeSearch.entryOf(unit, key);
        return entry == -1 || hiddenSinceTree(key, point, removals) ? null : TreeSearch.value(point.pages(), unit, entry);
    }

    /// {@return whether the tree of `point` holds `key` and no later removal `point` sees hides it}
    static boolean inTree(KeyspaceState state, MemorySegment key, ReadPoint point, RangeRemoval[] removals) {
        var root = point.root(state);
        return root != null && TreeSearch.contains(point.pages(), root, key) && !hiddenSinceTree(key, point, removals);
    }

    /// {@return whether the tree of `point` holds `key`, whatever range removal since hides it}
    static boolean inTreeOnly(KeyspaceState state, MemorySegment key, ReadPoint point) {
        var root = point.root(state);
        return root != null && TreeSearch.contains(point.pages(), root, key);
    }

    /// {@return whether `point` sees `key`, whose newest version in memory is `head`, as [TreeSearch#FOUND] or
    /// [TreeSearch#ABSENT] when memory or a range removal since the tree tells, or [TreeSearch#UNKNOWN] when only the
    /// tree does}
    static byte existsInMemory(KeyspaceState state, Version head, MemorySegment key, ReadPoint point) {
        var removals = state.removals;
        var version = KeyspaceState.resolve(head, key, point.bound(), removals);
        if (version != null) {
            return version != Version.HIDDEN ? TreeSearch.FOUND : TreeSearch.ABSENT;
        }
        if (point.root(state) == null || hiddenSinceTree(key, point, removals)) {
            return TreeSearch.ABSENT;
        }
        return TreeSearch.UNKNOWN;
    }

    /// {@return whether `point` sees `key`, whose newest version in memory is `head`, as [TreeSearch#FOUND] or
    /// [TreeSearch#ABSENT], or [TreeSearch#UNKNOWN] for a blind write: one hinted [TreeSearch#BLIND] that only the tree
    /// could answer}
    ///
    /// @param inTree whether the tree of `point` holds `key`, as a lookup of that same tree found it,
    ///               [TreeSearch#UNKNOWN] to look it up, or [TreeSearch#BLIND] not to
    static byte existence(KeyspaceState state, Version head, MemorySegment key, ReadPoint point, byte inTree) {
        if (inTree == TreeSearch.BLIND) {
            return existsInMemory(state, head, key, point);
        }
        return exists(state, head, key, point, inTree) ? TreeSearch.FOUND : TreeSearch.ABSENT;
    }

    /// {@return whether `point` sees `key`}
    static boolean exists(KeyspaceState state, MemorySegment key, ReadPoint point) {
        return exists(state, state.head(key), key, point, TreeSearch.UNKNOWN);
    }

    /// {@return whether `point` sees `key`, whose newest version in memory is `head`}
    ///
    /// @param head the newest version of `key` in memory, or `null` if there is none
    /// @param inTree whether the tree `point` reads holds `key`, as a lookup of that same tree found it, or
    ///               [TreeSearch#UNKNOWN] to look it up
    static boolean exists(KeyspaceState state, Version head, MemorySegment key, ReadPoint point, byte inTree) {
        var removals = state.removals;
        var version = KeyspaceState.resolve(head, key, point.bound(), removals);
        if (version != null) {
            return version != Version.HIDDEN;
        }
        if (inTree == TreeSearch.UNKNOWN) {
            return inTree(state, key, point, removals);
        }
        return inTree == TreeSearch.FOUND && !hiddenSinceTree(key, point, removals);
    }

    /// {@return the entries `point` sees from `from`, included, to `to`, excluded, either `null` for no bound}
    ///
    /// It reads the tree's nodes on the paths to the bounds, and looks up in the tree each key of the range that has a
    /// version in memory.
    static long count(KeyspaceState state, MemorySegment from, MemorySegment to, ReadPoint point) {
        var removals = state.removals;
        var root = point.root(state);
        var total = 0L;
        // The keys looked up in the tree, made for the chains that need one
        KeyCopies copies = null;
        for (var chain : state.slice(from, to).values()) {
            var version = KeyspaceState.resolve(chain.head, chain, point.bound(), removals);
            if (version == null) {
                // Counted with the tree
                continue;
            }
            var counted = false;
            if (root != null) {
                if (copies == null) {
                    copies = new KeyCopies();
                }
                counted = inTree(state, copies.of(chain), point, removals);
            }
            total += (version == Version.HIDDEN ? 0 : 1) - (counted ? 1 : 0);
        }
        if (root == null) {
            return total;
        }
        total += TreeSearch.count(point.pages(), root, from, to);
        var hidden = new ArrayList<TreeUpdate.Range>();
        var treeBound = point.treeBound();
        for (var removal : removals) {
            if (removal.stamp() > treeBound && removal.stamp() <= point.bound()) {
                hidden.add(new TreeUpdate.Range(removal.from(), removal.to()));
            }
        }
        for (var range : TreeUpdate.merge(hidden)) {
            var low = from == null ? range.from() : range.from() == null || Segments.compare(range.from(), from) < 0 ? from : range.from();
            var high = to == null ? range.to() : range.to() == null || Segments.compare(range.to(), to) > 0 ? to : range.to();
            if (low == null || high == null || Segments.compare(low, high) < 0) {
                total -= TreeSearch.count(point.pages(), root, low, high);
            }
        }
        return total;
    }

    /// {@return the value `point` sees under `key`}
    ///
    /// A resident key space returns the decoded value its version holds. Any other key space with byte array values
    /// copies the value out of the array that holds it, a version's own or a page's, rather than decoding a segment
    /// of it: the decoder's copy would zero the new array before filling it ([Segments#toBytes(byte\[\], int, int)]).
    @SuppressWarnings("unchecked")
    static <K, V> Optional<V> get(KeyspaceState state, Keyspace<K, V> keyspace, K key, ReadPoint point) {
        var encoded = Segments.lookupKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var removals = state.removals;
        // Its decoder copies the encoding whole, so the copy here returns the same bytes
        var bytes = keyspace.values() == BuiltinCodecs.BYTES_VALUE;
        if (state.resident && !state.loaded(encoded)) {
            return treeGet(keyspace, encoded, point, point.treeRoot(state), removals, bytes);
        }
        var version = KeyspaceState.resolve(state.head(encoded), encoded, point.bound(), removals);
        if (version != null) {
            if (version == Version.HIDDEN) {
                return Optional.empty();
            }
            // A resident key space reads the decoded value: no slice of the encoded one
            if (state.resident) {
                return Optional.of((V) version.decoded);
            }
            return Optional.of(bytes ? (V) version.copyBytes() : keyspace.values().decode(version.value()));
        }
        return treeGet(keyspace, encoded, point, point.root(state), removals, bytes);
    }

    /// {@return the value of `key` in the tree of `point` under `root`, decoded, or empty if it holds none or a later
    /// removal hides it}
    ///
    /// @param root the root of the key space's tree, or `null` if the read uses none
    /// @param bytes whether the values are byte arrays, which copy out of the array that holds them
    @SuppressWarnings("unchecked")
    private static <V> Optional<V> treeGet(Keyspace<?, V> keyspace, byte[] key, ReadPoint point, TreeRoot root,
                                           RangeRemoval[] removals, boolean bytes) {
        if (root == null) {
            return Optional.empty();
        }
        var unit = TreeSearch.leaf(point.pages(), root, key);
        var entry = TreeSearch.entryOf(unit, key);
        if (entry == -1 || hiddenSinceTree(key, point, removals)) {
            return Optional.empty();
        }
        return Optional.of(bytes
                ? (V) TreeSearch.valueBytes(point.pages(), unit, entry)
                : keyspace.values().decode(TreeSearch.value(point.pages(), unit, entry)));
    }

    static <K, V> List<Entry<K, V>> range(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, int limit, ReadPoint point) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        var entries = new ArrayList<Entry<K, V>>(Math.min(limit, 64));
        scan(state, keyspace, range, point, limit, (key, value, decoded) -> {
            entries.add(new Entry<>(keyspace.keys().decode(key), value(state, keyspace, value, decoded)));
            return entries.size() < limit;
        });
        return entries;
    }

    static <K, V> long forEach(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, ReadPoint point,
                               BiPredicate<? super K, ? super V> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        return scan(state, keyspace, range, point, (key, value, decoded) ->
                visitor.test(keyspace.keys().decode(key), value(state, keyspace, value, decoded)));
    }

    static <K, V> Optional<Entry<K, V>> first(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, ReadPoint point) {
        var found = range(state, keyspace, range, 1, point);
        return found.isEmpty() ? Optional.empty() : Optional.of(found.getFirst());
    }

    static <K, R> Optional<R> readEncoded(KeyspaceState state, Keyspace<K, ?> keyspace, K key, ReadPoint point,
                                          Function<? super MemorySegment, ? extends R> reader) {
        Objects.requireNonNull(reader, "reader");
        var encoded = Segments.lookupKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var removals = state.removals;
        var loaded = !state.resident || state.loaded(encoded);
        var version = loaded ? KeyspaceState.resolve(state.head(encoded), encoded, point.bound(), removals) : null;
        MemorySegment value;
        if (version != null) {
            if (version == Version.HIDDEN) {
                return Optional.empty();
            }
            value = version.value();
        } else {
            value = treeValue(encoded, point, loaded ? point.root(state) : point.treeRoot(state), removals);
            if (value == null) {
                return Optional.empty();
            }
        }
        return Optional.of(Objects.requireNonNull(reader.apply(value), "reader returned null"));
    }

    static <K> long forEachEncoded(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, ReadPoint point,
                                   BiPredicate<? super MemorySegment, ? super MemorySegment> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        return scan(state, keyspace, range, point, (key, value, _) -> visitor.test(key, value));
    }

    /// Passes the entries of `range` `point` sees to `consumer`, in the order of the range, until it returns `false`.
    ///
    /// It merges the versions in memory with the tree: a key with a version in memory at or below the bound takes it,
    /// and shadows the tree's entry for the same key. A descending scan steps down the chains in memory along their
    /// links ([KeyspaceState#walk]). A resident key space whose loader has not put every entry in memory reads through a
    /// [ResidentScan].
    ///
    /// @return the number of entries passed
    static <K> long scan(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, ReadPoint point, Visitor consumer) {
        return scan(state, keyspace, range, point, Long.MAX_VALUE, consumer);
    }

    /// Passes the entries of `range` `point` sees to `consumer`, as the scan without `wanted` does, for a consumer
    /// expected to take `wanted` of them, which bounds the values the tree's cursor reads ahead.
    ///
    /// @param wanted the entries `consumer` is expected to take, at least 1, or [Long#MAX_VALUE] if it may take them all
    /// @return the number of entries passed
    static <K> long scan(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, ReadPoint point, long wanted,
                         Visitor consumer) {
        Objects.requireNonNull(range, "range");
        // Copies, whatever the codec: the cursor keeps its bounds while a visitor runs, which may change a caller's array
        var fromBytes = range.from().map(key -> Segments.encodeKeyBytes(keyspace.keys(), key)).orElse(null);
        var toBytes = range.to().map(key -> Segments.encodeKeyBytes(keyspace.keys(), key)).orElse(null);
        var from = fromBytes == null ? null : MemorySegment.ofArray(fromBytes);
        var to = toBytes == null ? null : MemorySegment.ofArray(toBytes);
        if (from != null && to != null && Segments.compare(from, to) >= 0) {
            return 0;
        }
        var descending = range.descending();
        var removals = state.removals;
        // Read before memory, as a read of one key reads it
        var loader = state.resident ? state.loading : null;
        if (loader != null && loader.limit() != null) {
            return new ResidentScan(state, loader, point, removals, from, to, descending, consumer).run();
        }
        var root = point.root(state);
        var memory = state.walk(from, to, descending);
        var tree = root == null ? null : new TreeCursor(point.pages(), root, fromBytes, toBytes, descending, wanted);
        try {
            var inTree = tree != null && tree.next();
            var held = memory.next();
            var visited = 0L;
            while (held || inTree) {
                int order;
                if (!held) {
                    order = 1;
                } else if (!inTree) {
                    order = -1;
                } else {
                    // Where the key in memory sorts against the tree's, in the order of the scan
                    order = -tree.compareKey(memory.key());
                    if (descending) {
                        order = -order;
                    }
                }
                if (order <= 0) {
                    var chain = memory.chain();
                    var version = KeyspaceState.resolve(chain.head, chain, point.bound(), removals);
                    MemorySegment value = null;
                    Object decoded = null;
                    if (version != null && version != Version.HIDDEN) {
                        value = version.value();
                        decoded = version.decoded;
                    } else if (version == null && order == 0 && !hiddenSinceTree(memory.key(), point, removals)) {
                        value = tree.value();
                    }
                    // Made only for an entry passed on: a chain the reader sees nothing of needs no key
                    var key = value != null ? memory.key() : null;
                    if (order == 0) {
                        inTree = tree.next();
                    }
                    held = memory.next();
                    if (value != null) {
                        visited++;
                        if (tree != null) {
                            tree.took();
                        }
                        if (!consumer.visit(key, value, decoded)) {
                            break;
                        }
                    }
                } else {
                    var key = tree.key();
                    if (!hiddenSinceTree(key, point, removals)) {
                        visited++;
                        tree.took();
                        if (!consumer.visit(key, tree.value(), null)) {
                            break;
                        }
                    }
                    inTree = tree.next();
                }
            }
            return visited;
        } finally {
            if (tree != null) {
                // Its reads ahead lie in arrays of the store's pool until it gives them back
                tree.release();
            }
        }
    }

    /// Receives the entries of a scan.
    @FunctionalInterface
    interface Visitor {
        /// Receives an entry.
        ///
        /// @param key the encoded key, read-only
        /// @param value the encoded value, read-only
        /// @param decoded the decoded value of a resident key space, or `null`
        /// @return whether to go on
        boolean visit(MemorySegment key, MemorySegment value, Object decoded);
    }
}
