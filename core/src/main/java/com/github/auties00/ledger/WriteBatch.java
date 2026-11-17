package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.ArrayWriteBatch;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/// Writes that a [Ledger] applies atomically, in the order they were added.
///
/// A batch belongs to the store that created it ([Ledger#newBatch()]) and writes only that store's key spaces. It
/// encodes keys and values as they are added, on the thread that adds them, into memory from the store's pools, so
/// the work is done before the batch waits for the writer. When the store applies the batch it takes that memory
/// over to serve the writes until its next checkpoint, rather than copying them, and the batch continues in fresh
/// memory: a batch can be reused after [Ledger#commit(WriteBatch)] or [#clear()]. A batch is not thread-safe.
///
/// A byte array [put][#put(Keyspace, Object, Object)] to a [Keyspace.Residency#PAGED] key space of a store in a
/// directory, long enough to go to the store's file of large values, is the exception: the batch neither encodes nor
/// copies it, and the commit seals it into that file straight from the caller's array.
///
/// A batch holds at most 16,777,215 writes, each [compute][#compute] and [guard][#guard] counting as one, and each
/// [Companion] of a compute as one more. Keys encode to 1 to [KeyCodec#MAX_LENGTH] bytes, and values to at most
/// [ValueCodec#MAX_LENGTH], zero included.
///
/// A [compute][#compute] reads the latest value of its key when it is added, and a [guard][#guard] the value it holds
/// the batch to: the commit checks, at the batch's turn, that no write of the key came in between. A batch that holds
/// either is checked again if it is committed again after a commit of it failed.
public sealed interface WriteBatch permits ArrayWriteBatch {
    /// Adds a write of `value` under `key`, replacing any value already there.
    ///
    /// A byte array put to a [Keyspace.Residency#PAGED] key space, whose values of its length go to the store's file of
    /// large values, is read by the store, not copied, until a [commit][Ledger#commit(WriteBatch)] of the batch returns
    /// or the batch is [cleared][#clear()]; a commit that fails keeps it for a retry of the same batch. The store keeps
    /// no reference to it once the commit returns, so the caller may reuse the array from then on. An array changed
    /// before then is stored with the bytes it has when the commit seals it, without an error: one array filled again
    /// is never put under several keys of one batch.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param value the value, which must not change after this call; a byte array the store reads at the commit, as
    ///              said above, may change once the commit returns
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
    /// when it next writes them. A store in a directory counts the entries a removal of a paged key space takes away
    /// once the commit is visible, on a virtual thread of its own: the count reads the pages on the paths to the
    /// range's bounds, so a page there that fails authentication, or a file that cannot be read, fails the next
    /// [View#size(Keyspace)] of the key space and the next checkpoint, not the commit.
    ///
    /// @param keyspace the key space to write
    /// @param range the keys to remove; its order does not matter
    /// @param <K> the key type
    /// @return this batch
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`
    <K> WriteBatch deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range);

    /// Adds a write of what `function` makes of the value under `key`, applied in order with the other writes of the
    /// batch.
    ///
    /// `function` runs once now, on the calling thread, against the latest committed value (`null` if none), and its
    /// result is encoded as [#put] would encode it: `null` removes the key, and the instance it received leaves the key
    /// unchanged. When the batch commits, that result is written only if no write of the key came between the value it
    /// read and the batch's turn; otherwise `function` runs again, in the order of commits, on the value that write
    /// left, and its result is written instead. A write of the same key earlier in this batch counts as such a write:
    /// the function then sees it. When a write of the key is already under way as it is added, the function does not
    /// run now, only in the order of commits.
    ///
    /// `function` follows the rules of [Ledger#compute]: it may run twice, must depend only on the value it receives,
    /// and must not commit, start a transaction, wait for durability or close the store. If it throws now, the batch is
    /// left as it was and the exception propagates; if it throws in the order of commits, none of the batch's writes
    /// apply and the commit throws it.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param function makes the new value from the latest one
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return the outcome of this write, readable once [Ledger#commit(WriteBatch)] returns
    /// @throws IllegalArgumentException as [#put] does
    /// @throws LedgerCorruptedException if the page holding the value fails authentication
    /// @throws IllegalStateException if the store is closed, or if called from a compute function of the store
    <K, V> Computation<V> compute(Keyspace<K, V> keyspace, K key, Function<? super V, ? extends V> function);

    /// Adds a write of what `function` makes of the value under `key`, as [#compute(Keyspace, Object, Function)]
    /// does, and keeps each of `companions` in step with it.
    ///
    /// Each companion holds a zero-length entry under the same key bytes while the value is a member: the commit puts
    /// it when the value the write leaves is a member and the one it replaced was not, and removes it in the opposite
    /// case, from the value of the function's last run. The companions' writes apply with the computed one, ordered
    /// with every other write of their keys. They stay in step with the values only if every write of `keyspace` is
    /// such a compute naming all of them, and every range removal of `keyspace` removes the same range of each of them
    /// in the same batch, as [Companion] describes.
    ///
    /// @param keyspace the key space to write
    /// @param key the key
    /// @param function makes the new value from the latest one
    /// @param companions the key spaces that index the values, each once, none of them `keyspace`
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return the outcome of this write, readable once [Ledger#commit(WriteBatch)] returns
    /// @throws IllegalArgumentException as [#put] does, or if a companion's key space is `keyspace`, appears twice, or
    ///                                  the store was not opened with it
    /// @throws LedgerCorruptedException if the page holding the value fails authentication
    /// @throws IllegalStateException if the store is closed, or if called from a compute function of the store
    <K, V> Computation<V> compute(Keyspace<K, V> keyspace, K key, Function<? super V, ? extends V> function,
                                  List<Companion<K, ? super V>> companions);

    /// Holds the batch to the latest value under `key`, which it returns.
    ///
    /// When the batch commits, it applies only if no write of the key came between this read and the batch's turn, or
    /// if the key holds again what this read found: no value, or a value that encodes to the same bytes. Otherwise none
    /// of its writes apply and the commit throws [LedgerConflictException]. A write of the same key earlier in this
    /// batch counts as such a write, so a guard goes before the batch's writes of its key. The guard writes nothing,
    /// and the check costs the commit no read unless a write of the key, or a removal of a range of its key space, came
    /// after the read.
    ///
    /// @param keyspace the key space to read
    /// @param key the key
    /// @param <K> the key type
    /// @param <V> the value type
    /// @return the value under `key`, or an empty optional if there is none
    /// @throws IllegalArgumentException if the store was not opened with `keyspace`, or the key encodes to no bytes or
    ///                                  more than [KeyCodec#MAX_LENGTH]
    /// @throws LedgerCorruptedException if the page holding the value fails authentication
    /// @throws IllegalStateException if the store is closed, or if called from a compute function of the store
    <K, V> Optional<V> guard(Keyspace<K, V> keyspace, K key);

    /// {@return the number of writes added}
    int size();

    /// {@return whether no writes were added}
    boolean isEmpty();

    /// Removes every write, keeping the memory for reuse.
    void clear();
}
