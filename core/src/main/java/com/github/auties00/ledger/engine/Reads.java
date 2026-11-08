package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
/// before it reads memory, so the chains the loader installed before it moved past the key are there.
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

    /// {@return the value of `key` in the tree of `point` under `root`, or `null` if it holds none or a later removal
    /// hides it}
    ///
    /// @param root the root of the key space's tree, or `null` if the read uses none
    private static MemorySegment treeValue(MemorySegment key, ReadPoint point, TreeRoot root, RangeRemoval[] removals) {
        if (root == null) {
            return null;
        }
        var hit = TreeSearch.find(point.pages(), root, key);
        if (hit == null || hiddenSinceTree(key, point, removals)) {
            return null;
        }
        return TreeSearch.value(point.pages(), hit.unit(), hit.entry());
    }

    /// {@return whether the tree of `point` holds `key` and no later removal `point` sees hides it}
    static boolean inTree(KeyspaceState state, MemorySegment key, ReadPoint point, RangeRemoval[] removals) {
        var root = point.root(state);
        return root != null && TreeSearch.find(point.pages(), root, key) != null && !hiddenSinceTree(key, point, removals);
    }

    /// {@return whether the tree of `point` holds `key`, whatever range removal since hides it}
    static boolean inTreeOnly(KeyspaceState state, MemorySegment key, ReadPoint point) {
        var root = point.root(state);
        return root != null && TreeSearch.find(point.pages(), root, key) != null;
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
        for (var entry : state.slice(from, to).entrySet()) {
            var version = KeyspaceState.resolve(entry.getValue().head, entry.getKey(), point.bound(), removals);
            if (version == null) {
                // Counted with the tree
                continue;
            }
            var counted = root != null && inTree(state, entry.getKey(), point, removals);
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

    static <K, V> Optional<V> get(KeyspaceState state, Keyspace<K, V> keyspace, K key, ReadPoint point) {
        var encoded = Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var removals = state.removals;
        if (state.resident && !state.loaded(encoded)) {
            var value = treeValue(encoded, point, point.treeRoot(state), removals);
            return value == null ? Optional.empty() : Optional.of(keyspace.values().decode(value));
        }
        var version = KeyspaceState.resolve(state.head(encoded), encoded, point.bound(), removals);
        if (version != null) {
            if (version == Version.HIDDEN) {
                return Optional.empty();
            }
            // A resident key space reads the decoded value: no slice of the encoded one
            @SuppressWarnings("unchecked")
            var found = state.resident ? (V) version.decoded : keyspace.values().decode(version.value());
            return Optional.of(found);
        }
        var value = treeValue(encoded, point, point.root(state), removals);
        return value == null ? Optional.empty() : Optional.of(keyspace.values().decode(value));
    }

    static <K, V> List<Entry<K, V>> range(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, int limit, ReadPoint point) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        var entries = new ArrayList<Entry<K, V>>(Math.min(limit, 64));
        scan(state, keyspace, range, point, (key, value, decoded) -> {
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
        var encoded = Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
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
    /// and shadows the tree's entry for the same key. A resident key space whose loader has not put every entry in
    /// memory reads the keys below the loader's position from memory alone, and the others from the tree alone.
    ///
    /// @return the number of entries passed
    static <K> long scan(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, ReadPoint point, Visitor consumer) {
        Objects.requireNonNull(range, "range");
        var from = range.from().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        var to = range.to().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        if (from != null && to != null && Segments.compare(from, to) >= 0) {
            return 0;
        }
        var descending = range.descending();
        var removals = state.removals;
        // Read before memory, as a read of one key reads it
        var loader = state.resident ? state.loading : null;
        var limit = loader == null ? null : loader.limit();
        TreeRoot root;
        var memoryTo = to;
        var treeFrom = from;
        if (limit == null) {
            root = point.root(state);
        } else {
            root = point.treeRoot(state);
            if (to == null || Segments.compare(limit, to) < 0) {
                memoryTo = limit;
            }
            if (from == null || Segments.compare(limit, from) > 0) {
                treeFrom = limit;
            }
            if (to != null && Segments.compare(treeFrom, to) >= 0) {
                root = null;
            }
        }
        var slice = state.slice(from, memoryTo);
        var memory = (descending ? slice.descendingMap() : slice).entrySet().iterator();
        var tree = root == null ? null : new TreeCursor(point.pages(), root, treeFrom, to, descending);
        var inTree = tree != null && tree.next();
        Map.Entry<MemorySegment, Chain> held = memory.hasNext() ? memory.next() : null;
        var visited = 0L;
        while (held != null || inTree) {
            int order;
            if (held == null) {
                order = 1;
            } else if (!inTree) {
                order = -1;
            } else {
                // Where the key in memory sorts against the tree's, in the order of the scan
                order = -tree.compareKey(held.getKey());
                if (descending) {
                    order = -order;
                }
            }
            if (order <= 0) {
                var key = held.getKey();
                var version = KeyspaceState.resolve(held.getValue().head, key, point.bound(), removals);
                MemorySegment value = null;
                Object decoded = null;
                if (version != null && version != Version.HIDDEN) {
                    value = version.value();
                    decoded = version.decoded;
                } else if (version == null && order == 0 && !hiddenSinceTree(key, point, removals)) {
                    value = tree.value();
                }
                if (order == 0) {
                    inTree = tree.next();
                }
                held = memory.hasNext() ? memory.next() : null;
                if (value != null) {
                    visited++;
                    if (!consumer.visit(key, value, decoded)) {
                        break;
                    }
                }
            } else {
                var key = tree.key();
                if (!hiddenSinceTree(key, point, removals)) {
                    visited++;
                    if (!consumer.visit(key, tree.value(), null)) {
                        break;
                    }
                }
                inTree = tree.next();
            }
        }
        return visited;
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
