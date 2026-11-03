package com.github.auties00.ledger.benchmark.engine;

import java.util.Arrays;
import java.util.Objects;

/// An ordered list of puts and deletes an engine applies atomically.
///
/// A batch is reusable: [#clear()] empties it without releasing its arrays.
public final class WriteBatch {
    private Keyspace[] keyspaces;
    private byte[][] keys;
    private byte[][] values;
    private int size;

    /// Creates an empty batch.
    public WriteBatch() {
        this.keyspaces = new Keyspace[16];
        this.keys = new byte[16][];
        this.values = new byte[16][];
    }

    /// Adds a put of `value` under `key`.
    ///
    /// @param keyspace the key space to write
    /// @param key the key, which the batch keeps without copying
    /// @param value the value, which the batch keeps without copying
    /// @return this batch
    public WriteBatch put(Keyspace keyspace, byte[] key, byte[] value) {
        add(keyspace, key, Objects.requireNonNull(value, "value"));
        return this;
    }

    /// Adds a delete of `key`.
    ///
    /// @param keyspace the key space to write
    /// @param key the key, which the batch keeps without copying
    /// @return this batch
    public WriteBatch delete(Keyspace keyspace, byte[] key) {
        add(keyspace, key, null);
        return this;
    }

    private void add(Keyspace keyspace, byte[] key, byte[] value) {
        Objects.requireNonNull(keyspace, "keyspace");
        Objects.requireNonNull(key, "key");
        if (size == keys.length) {
            var capacity = size * 2;
            keyspaces = Arrays.copyOf(keyspaces, capacity);
            keys = Arrays.copyOf(keys, capacity);
            values = Arrays.copyOf(values, capacity);
        }
        keyspaces[size] = keyspace;
        keys[size] = key;
        values[size] = value;
        size++;
    }

    /// {@return the number of operations}
    public int size() {
        return size;
    }

    /// {@return the key space of operation `index`}
    ///
    /// @param index the operation index
    public Keyspace keyspace(int index) {
        return keyspaces[Objects.checkIndex(index, size)];
    }

    /// {@return the key of operation `index`}
    ///
    /// @param index the operation index
    public byte[] key(int index) {
        return keys[Objects.checkIndex(index, size)];
    }

    /// {@return the value of operation `index`, or `null` when it is a delete}
    ///
    /// @param index the operation index
    public byte[] value(int index) {
        return values[Objects.checkIndex(index, size)];
    }

    /// Removes every operation.
    public void clear() {
        Arrays.fill(keys, 0, size, null);
        Arrays.fill(values, 0, size, null);
        size = 0;
    }
}
