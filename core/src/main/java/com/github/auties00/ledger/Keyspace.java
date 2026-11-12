package com.github.auties00.ledger;

import java.util.Objects;
import java.util.regex.Pattern;

/// A named, ordered set of entries in a store, and how its keys and values are encoded.
///
/// Entries are ordered by the unsigned bytes their keys encode to ([KeyCodec]). Two key spaces are the same key
/// space of a store when their names are equal; a store refuses a key space whose residency or codecs differ from
/// those it was opened with.
///
/// @param name the name the store knows the key space by: 1 to 64 lowercase letters, digits, `_`, `-` or `.`
/// @param residency where the entries are kept
/// @param keys encodes and decodes the keys
/// @param values encodes and decodes the values
/// @param <K> the key type
/// @param <V> the value type
public record Keyspace<K, V>(String name, Residency residency, KeyCodec<K> keys, ValueCodec<V> values) {
    private static final Pattern NAME = Pattern.compile("[a-z0-9_.-]{1,64}");

    /// Checks the components.
    ///
    /// @throws IllegalArgumentException if `name` is not 1 to 64 lowercase letters, digits, `_`, `-` or `.`
    public Keyspace {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(residency, "residency");
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(values, "values");
        if (!NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("Key space names are 1 to 64 of [a-z0-9_.-]: " + name);
        }
    }

    /// {@return a key space whose entries are all held in memory, decoded}
    ///
    /// @param name the name
    /// @param keys encodes and decodes the keys
    /// @param values encodes and decodes the values
    /// @param <K> the key type
    /// @param <V> the value type
    /// @throws IllegalArgumentException if `name` is not valid
    public static <K, V> Keyspace<K, V> resident(String name, KeyCodec<K> keys, ValueCodec<V> values) {
        return new Keyspace<>(name, Residency.RESIDENT, keys, values);
    }

    /// {@return a key space whose entries are read from disk through the page cache}
    ///
    /// @param name the name
    /// @param keys encodes and decodes the keys
    /// @param values encodes and decodes the values
    /// @param <K> the key type
    /// @param <V> the value type
    /// @throws IllegalArgumentException if `name` is not valid
    public static <K, V> Keyspace<K, V> paged(String name, KeyCodec<K> keys, ValueCodec<V> values) {
        return new Keyspace<>(name, Residency.PAGED, keys, values);
    }

    /// Where a key space keeps its entries between reads.
    public enum Residency {
        /// Every entry is held in memory, decoded: a read costs no I/O and no decoding, and returns the instance the
        /// store holds. That instance must not be modified: every read returns it, and a byte array the store holds is
        /// also the value its next checkpoint writes, and the value the log writes for the commit that stored it until
        /// that commit's frame is written, so a change to one could reach the store's files without a commit. The store
        /// reads the entries its files hold into memory from the first use of the key space after it opens, in the
        /// order of their keys, or while it opens with [Ledger.Options#loadResidentAtOpen(boolean)]; until an entry is
        /// in memory a read of it costs what a read of a [#PAGED] entry costs and returns an instance of its own, and a
        /// write of it waits. The key space uses memory in proportion to its entries. For key spaces that are small and
        /// read often: sessions, chats, settings.
        RESIDENT,
        /// Entries are read from the store's files through the page cache and decoded on each read. For key spaces
        /// larger than memory should hold: messages, history.
        PAGED
    }
}
