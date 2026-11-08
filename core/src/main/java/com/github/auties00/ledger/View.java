package com.github.auties00.ledger;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

/// The reads a [Ledger], a [Snapshot] and a [Transaction] share.
///
/// Values come back as their [ValueCodec] decoded them. Those of a [Keyspace.Residency#RESIDENT] key space are the
/// instances the store holds, so every read of a key returns the same instance until it is written again, once the
/// store holds the key's entry in memory; until then, as after an open that has not loaded the key space yet, each
/// read decodes an instance of its own.
///
/// The `Encoded` reads skip the codecs: they hand the encoded bytes, in the store's own memory, to a function that
/// runs while the memory is valid, for a caller that copies the bytes elsewhere or reads a few of them.
public sealed interface View permits Ledger, Snapshot, Transaction {
    /// {@return the value stored under `key`, or an empty optional if there is none}
    ///
    /// @param keyspace the key space to read
    /// @param key the key
    /// @param <K> the key type
    /// @param <V> the value type
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the page holding the value fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K, V> Optional<V> get(Keyspace<K, V> keyspace, K key);

    /// {@return up to `limit` entries of `range`, in the order of the range}
    ///
    /// @param keyspace the key space to read
    /// @param range the keys to read and their order
    /// @param limit the most entries to return, at least 1
    /// @param <K> the key type
    /// @param <V> the value type
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`, or `limit` is not positive
    /// @throws LedgerCorruptedException if a page of the range fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K, V> List<Entry<K, V>> range(Keyspace<K, V> keyspace, KeyRange<K> range, int limit);

    /// Passes the entries of `range` to `visitor` in the order of the range, until there are no more or the visitor
    /// returns `false`.
    ///
    /// The whole visit reads one state, as a [Snapshot] would.
    ///
    /// @apiNote Use it to read more entries than a list should hold. The state stays pinned while the visitor runs,
    /// as a snapshot pins it.
    /// @param keyspace the key space to read
    /// @param range the keys to read and their order
    /// @param visitor receives each key and value, and returns whether to go on
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return the number of entries passed to the visitor
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if a page of the range fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K, V> long forEach(Keyspace<K, V> keyspace, KeyRange<K> range, BiPredicate<? super K, ? super V> visitor);

    /// {@return the first entry of `range` in the order of the range, or an empty optional if it has none}
    ///
    /// @param keyspace the key space to read
    /// @param range the keys to read and their order
    /// @param <K> the key type
    /// @param <V> the value type
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the page holding the entry fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K, V> Optional<Entry<K, V>> first(Keyspace<K, V> keyspace, KeyRange<K> range);

    /// Passes the encoded value stored under `key` to `reader` and returns what `reader` returns.
    ///
    /// The segment is the store's own memory, read-only and valid only while `reader` runs. A
    /// [Keyspace.Residency#RESIDENT] key space holds its values decoded, so for one of those the value is encoded for
    /// the call.
    ///
    /// @param keyspace the key space to read
    /// @param key the key
    /// @param reader reads the encoded value; it must not return `null` or keep the segment
    /// @param <K> the key type
    /// @param <R> the result type
    /// @return what `reader` returned, or an empty optional if there is no value under `key`
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the page holding the value fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K, R> Optional<R> readEncoded(Keyspace<K, ?> keyspace, K key, Function<? super MemorySegment, ? extends R> reader);

    /// Passes the encoded keys and values of `range` to `visitor` in the order of the range, until there are no more
    /// or the visitor returns `false`.
    ///
    /// The segments are the store's own memory, read-only and valid only during the call that receives them. The
    /// whole visit reads one state, as [#forEach] does.
    ///
    /// @param keyspace the key space to read
    /// @param range the keys to read and their order
    /// @param visitor receives each encoded key and value, and returns whether to go on; it must not keep them
    /// @param <K> the key type
    /// @return the number of entries passed to the visitor
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if a page of the range fails authentication
    /// @throws IllegalStateException if the store or view is closed
    <K> long forEachEncoded(Keyspace<K, ?> keyspace, KeyRange<K> range,
                            BiPredicate<? super MemorySegment, ? super MemorySegment> visitor);

    /// {@return the number of entries in `keyspace`}
    ///
    /// The count is exact. The store keeps it as it writes, except for a write of a key it holds no version of in
    /// memory, which does not read the file to learn whether the key existed: the next checkpoint learns it. Until
    /// then this looks up each such key, which may read the file after many of them; once a checkpoint has taken them,
    /// it reads nothing. A range removal of a batch is counted after its commit, on a thread of the store's own; a
    /// count that comes first counts the removal itself, which reads the pages on the paths to the range's bounds, or
    /// waits for the thread counting it.
    ///
    /// @param keyspace the key space to count
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if a page the count reads fails authentication
    /// @throws LedgerIoException if the file cannot be read
    /// @throws IllegalStateException if the store or view is closed
    long size(Keyspace<?, ?> keyspace);
}
