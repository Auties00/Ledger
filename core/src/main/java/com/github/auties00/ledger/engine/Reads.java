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

/// The reads of [com.github.auties00.ledger.View], over the entries a reader with a given bound sees.
///
/// A stamp is a commit sequence shifted left by [#OP_BITS], plus the position of the write in its batch: later writes
/// of one batch override earlier ones, range removals included. A reader at sequence `s` reads with bound
/// [#boundOf(long)] of `s`, which takes in every write of every commit up to `s`.
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

    @SuppressWarnings("unchecked")
    private static <V> V value(KeyspaceState state, Keyspace<?, V> keyspace, Version version) {
        return state.resident ? (V) version.decoded : keyspace.values().decode(version.value);
    }

    static <K, V> Optional<V> get(KeyspaceState state, Keyspace<K, V> keyspace, K key, long bound) {
        var encoded = Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var version = KeyspaceState.visible(state.entries.get(encoded), encoded, bound, state.removals);
        return version == null ? Optional.empty() : Optional.of(value(state, keyspace, version));
    }

    static <K, V> List<Entry<K, V>> range(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, int limit, long bound) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive: " + limit);
        }
        var entries = new ArrayList<Entry<K, V>>(Math.min(limit, 64));
        scan(state, keyspace, range, bound, (key, version) -> {
            entries.add(new Entry<>(keyspace.keys().decode(key), value(state, keyspace, version)));
            return entries.size() < limit;
        });
        return entries;
    }

    static <K, V> long forEach(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, long bound,
                               BiPredicate<? super K, ? super V> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        return scan(state, keyspace, range, bound, (key, version) ->
                visitor.test(keyspace.keys().decode(key), value(state, keyspace, version)));
    }

    static <K, V> Optional<Entry<K, V>> first(KeyspaceState state, Keyspace<K, V> keyspace, KeyRange<K> range, long bound) {
        var found = range(state, keyspace, range, 1, bound);
        return found.isEmpty() ? Optional.empty() : Optional.of(found.getFirst());
    }

    static <K, R> Optional<R> readEncoded(KeyspaceState state, Keyspace<K, ?> keyspace, K key, long bound,
                                          Function<? super MemorySegment, ? extends R> reader) {
        Objects.requireNonNull(reader, "reader");
        var encoded = Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var version = KeyspaceState.visible(state.entries.get(encoded), encoded, bound, state.removals);
        return version == null ? Optional.empty() : Optional.of(Objects.requireNonNull(reader.apply(version.value), "reader returned null"));
    }

    static <K> long forEachEncoded(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, long bound,
                                   BiPredicate<? super MemorySegment, ? super MemorySegment> visitor) {
        Objects.requireNonNull(visitor, "visitor");
        return scan(state, keyspace, range, bound, (key, version) -> visitor.test(key, version.value));
    }

    /// Passes the entries of `range` a reader with `bound` sees to `consumer`, in the order of the range, until it
    /// returns `false`.
    ///
    /// @return the number of entries passed
    static <K> long scan(KeyspaceState state, Keyspace<K, ?> keyspace, KeyRange<K> range, long bound, Visitor consumer) {
        Objects.requireNonNull(range, "range");
        var from = range.from().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        var to = range.to().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        var removals = state.removals;
        var slice = state.slice(from, to);
        var ordered = range.descending() ? slice.descendingMap() : slice;
        var visited = 0L;
        for (var entry : ordered.entrySet()) {
            var version = KeyspaceState.visible(entry.getValue(), entry.getKey(), bound, removals);
            if (version == null) {
                continue;
            }
            visited++;
            if (!consumer.visit(entry.getKey(), version)) {
                break;
            }
        }
        return visited;
    }

    /// Receives the entries of a scan, as encoded keys and their visible versions.
    @FunctionalInterface
    interface Visitor {
        boolean visit(MemorySegment key, Version version);
    }
}
