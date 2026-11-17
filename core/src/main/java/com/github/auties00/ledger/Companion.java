package com.github.auties00.ledger;

import java.util.Objects;
import java.util.function.Predicate;

/// A key space that holds one zero-length entry under the key of each entry of another key space whose value is a
/// member, which [WriteBatch#compute(Keyspace, Object, java.util.function.Function, java.util.List)] keeps in step with
/// the values it computes.
///
/// A companion is an index of a property of the values: the entries of a key space of messages that are starred, for
/// example, which a range read or [View#count] of the companion lists or counts without reading a message. The
/// companion's entries have the same key bytes as the computed entries, so its key codec must encode the keys as the
/// computed key space's codec does, and its value codec must decode a zero-length value.
///
/// A computed write writes the companion only when membership changes: it puts the companion's entry when the value it
/// leaves is a member and the value it replaced was not, and removes it in the opposite case. The writes of the
/// companion are ordered with every other write of its keys, as the computed write is with those of its own key. A
/// companion written in any other way is the caller's to keep in step.
///
/// @param keyspace the key space of the zero-length entries
/// @param member whether a value has an entry in the companion; it must depend only on the value, and must not commit,
///               start a transaction, wait for durability or close the store
/// @param <K> the key type
/// @param <V> the value type of the computed key space
public record Companion<K, V>(Keyspace<K, ?> keyspace, Predicate<? super V> member) {
    /// Creates a companion.
    ///
    /// @param keyspace the key space of the zero-length entries
    /// @param member whether a value has an entry in the companion
    public Companion {
        Objects.requireNonNull(keyspace, "keyspace");
        Objects.requireNonNull(member, "member");
    }
}
