package com.github.auties00.ledger;

import java.util.Objects;
import java.util.Optional;

/// The keys between a lower bound, included, and an upper bound, excluded, read in ascending or descending order.
///
/// Either bound may be absent, which leaves that side of the range open. Bounds are compared as their encoded bytes
/// are ([KeyCodec]).
///
/// @param <K> the key type
public final class KeyRange<K> {
    private static final KeyRange<?> ALL = new KeyRange<>(null, null, false);

    private final K from;
    private final K to;
    private final boolean descending;

    private KeyRange(K from, K to, boolean descending) {
        this.from = from;
        this.to = to;
        this.descending = descending;
    }

    /// {@return every key, in ascending order}
    ///
    /// @param <K> the key type
    @SuppressWarnings("unchecked")
    public static <K> KeyRange<K> all() {
        return (KeyRange<K>) ALL;
    }

    /// {@return the keys from `from` on, in ascending order}
    ///
    /// @param from the lowest key included
    /// @param <K> the key type
    public static <K> KeyRange<K> atLeast(K from) {
        return new KeyRange<>(Objects.requireNonNull(from, "from"), null, false);
    }

    /// {@return the keys before `to`, in ascending order}
    ///
    /// @param to the first key excluded
    /// @param <K> the key type
    public static <K> KeyRange<K> below(K to) {
        return new KeyRange<>(null, Objects.requireNonNull(to, "to"), false);
    }

    /// {@return the keys from `from` up to `to`, in ascending order}
    ///
    /// A range whose `to` does not come after `from` is empty.
    ///
    /// @param from the lowest key included
    /// @param to the first key excluded
    /// @param <K> the key type
    public static <K> KeyRange<K> between(K from, K to) {
        return new KeyRange<>(Objects.requireNonNull(from, "from"), Objects.requireNonNull(to, "to"), false);
    }

    /// {@return the same keys, in the opposite order}
    public KeyRange<K> reversed() {
        return new KeyRange<>(from, to, !descending);
    }

    /// {@return the lowest key included, or an empty optional if the range has no lower bound}
    public Optional<K> from() {
        return Optional.ofNullable(from);
    }

    /// {@return the first key excluded, or an empty optional if the range has no upper bound}
    public Optional<K> to() {
        return Optional.ofNullable(to);
    }

    /// {@return whether the range is read from its highest key down}
    public boolean descending() {
        return descending;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof KeyRange<?> that
                && Objects.equals(from, that.from)
                && Objects.equals(to, that.to)
                && descending == that.descending;
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to, descending);
    }

    @Override
    public String toString() {
        return "KeyRange[" + (from == null ? "" : from) + ", " + (to == null ? "" : to) + ")" + (descending ? " descending" : "");
    }
}
