package com.github.auties00.ledger;

/// A key and the value stored under it.
///
/// @param key the key
/// @param value the value
/// @param <K> the key type
/// @param <V> the value type
public record Entry<K, V>(K key, V value) {
}
