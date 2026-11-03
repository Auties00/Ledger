package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.ArrayWriteBatch;

import java.lang.foreign.MemorySegment;

/// Writes that a [Ledger] applies atomically, in the order they were added.
///
/// A batch belongs to the store that created it ([Ledger#newBatch()]) and writes only that store's key spaces. It
/// encodes keys and values as they are added, on the thread that adds them, into memory from the store's pools, so
/// the work is done before the batch waits for the writer. When the store applies the batch it takes that memory
/// over to serve the writes until its next checkpoint, rather than copying them, and the batch continues in fresh
/// memory: a batch can be reused after [Ledger#commit(WriteBatch)] or [#clear()]. A batch is not thread-safe.
public sealed interface WriteBatch permits ArrayWriteBatch {
    /// Adds a write of `value` under `key`, replacing any value already there.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param value the value, which must not change after this call
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return this batch
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`, the key encodes to no bytes or
    ///                                  more than [KeyCodec#MAX_LENGTH], or the value to more than
    ///                                  [ValueCodec#MAX_LENGTH]
    <K, V> WriteBatch put(Keyspace<K, V> keyspace, K key, V value);

    /// Adds a write of an encoded `value` under an encoded `key`, copying both into the batch.
    ///
    /// The bytes must be what the key space's codecs produce; the store decodes the value only if the key space is
    /// [Keyspace.Residency#RESIDENT], when it applies the batch.
    ///
    /// @param keyspace the key space to write
    /// @param key the encoded key
    /// @param value the encoded value
    /// @return this batch
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`, `key` is empty or longer than
    ///                                  [KeyCodec#MAX_LENGTH], or `value` is longer than [ValueCodec#MAX_LENGTH]
    WriteBatch putEncoded(Keyspace<?, ?> keyspace, MemorySegment key, MemorySegment value);

    /// Adds a removal of the value under `key`, if there is one.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param <K> the key type
    /// @return this batch
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`, or the key encodes to no bytes
    ///                                  or more than [KeyCodec#MAX_LENGTH]
    <K> WriteBatch delete(Keyspace<K, ?> keyspace, K key);

    /// Adds a removal of every entry of `range`.
    ///
    /// The cost does not grow with the number of entries removed: the store records the range and drops its pages
    /// when it next writes them.
    ///
    /// @param keyspace the key space to write
    /// @param range the keys to remove; its order does not matter
    /// @param <K> the key type
    /// @return this batch
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`
    <K> WriteBatch deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range);

    /// {@return the number of writes added}
    int size();

    /// {@return whether no writes were added}
    boolean isEmpty();

    /// Removes every write, keeping the memory for reuse.
    void clear();
}
