package com.github.auties00.ledger;

import java.lang.foreign.MemorySegment;

/// The reads and writes of a [Ledger#update] transaction.
///
/// Reads see the latest state and the transaction's own writes, in the order it made them. A scan
/// ([#forEach], [#forEachEncoded]) is one read, made when it is called: it passes the entries as they stood then, the
/// writes the transaction made before the call included, and none of the writes its visitor makes, wherever they fall
/// in the range; the visitor's own [#get] and [#size], and every read after the scan, see them. The writes apply
/// atomically when the transaction returns, and not at all if it throws. A transaction is valid only while its
/// function runs and only on the thread that runs it.
///
/// @apiNote A visitor may be handed an entry it removed or overwrote earlier in the same scan, with the value it had
/// at the call, and the count a scan returns is of the entries it passed, which [#size] read inside the visitor or
/// after need not match. A visitor that must skip what it already changed checks the key with [#get], which reads its
/// writes.
public non-sealed interface Transaction extends View {
    /// Stores `value` under `key`, replacing any value already there.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param value the value, which must not change after this call
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return this transaction
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the entry of `key` of a [resident][Keyspace.Residency#RESIDENT] key space could not be read
    ///                                  into memory; the next write tries again
    <K, V> Transaction put(Keyspace<K, V> keyspace, K key, V value);

    /// Stores an encoded `value` under an encoded `key`, copying both.
    ///
    /// The bytes must be what the key space's codecs produce.
    ///
    /// @param keyspace the key space to write
    /// @param key the encoded key
    /// @param value the encoded value
    /// @return this transaction
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`, `key` is empty or longer than
    ///                                  [KeyCodec#MAX_LENGTH], or `value` is longer than [ValueCodec#MAX_LENGTH]
    /// @throws LedgerCorruptedException if the entry of `key` of a [resident][Keyspace.Residency#RESIDENT] key space could not be read
    ///                                  into memory; the next write tries again
    Transaction putEncoded(Keyspace<?, ?> keyspace, MemorySegment key, MemorySegment value);

    /// Removes the value under `key`, if there is one.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param <K> the key type
    /// @return this transaction
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the entry of `key` of a [resident][Keyspace.Residency#RESIDENT] key space could not be read
    ///                                  into memory; the next write tries again
    <K> Transaction delete(Keyspace<K, ?> keyspace, K key);

    /// Removes every entry of `range`.
    ///
    /// The cost does not grow with the number of entries removed: the store records the range and drops its pages
    /// when it next writes them.
    ///
    /// @param keyspace the key space to write
    /// @param range the keys to remove; its order does not matter
    /// @param <K> the key type
    /// @return this transaction
    /// @throws IllegalArgumentException if this store was not opened with `keyspace`
    /// @throws LedgerCorruptedException if the entries of a [resident][Keyspace.Residency#RESIDENT] key space could not be read
    ///                                  into memory; the next write tries again
    <K> Transaction deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range);
}
