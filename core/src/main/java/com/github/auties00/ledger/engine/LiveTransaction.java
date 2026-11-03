package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.ValueCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

/// A [Transaction] of a [LiveLedger].
///
/// It runs holding the writer lock and applies each write at once under a sequence no reader sees yet; its own reads
/// use that sequence's bound, so they see the latest commit and the transaction's writes so far. Every key and value
/// is encoded into memory of its own, which stays valid once the transaction publishes.
final class LiveTransaction implements Transaction {
    private final LiveLedger ledger;
    private final LiveLedger.Applier applier;
    private final Thread owner;
    private boolean valid;

    LiveTransaction(LiveLedger ledger, LiveLedger.Applier applier) {
        this.ledger = ledger;
        this.applier = applier;
        this.owner = Thread.currentThread();
        this.valid = true;
    }

    void invalidate() {
        valid = false;
    }

    private KeyspaceState state(Keyspace<?, ?> keyspace) {
        if (!valid || Thread.currentThread() != owner) {
            throw new IllegalStateException("The transaction is not running on this thread");
        }
        return ledger.state(keyspace);
    }

    @Override
    public <K, V> Optional<V> get(Keyspace<K, V> keyspace, K key) {
        return Reads.get(state(keyspace), keyspace, key, ledger.writerPoint(applier.bound()));
    }

    @Override
    public <K, V> List<Entry<K, V>> range(Keyspace<K, V> keyspace, KeyRange<K> range, int limit) {
        return Reads.range(state(keyspace), keyspace, range, limit, ledger.writerPoint(applier.bound()));
    }

    @Override
    public <K, V> long forEach(Keyspace<K, V> keyspace, KeyRange<K> range, BiPredicate<? super K, ? super V> visitor) {
        return Reads.forEach(state(keyspace), keyspace, range, ledger.writerPoint(applier.bound()), visitor);
    }

    @Override
    public <K, V> Optional<Entry<K, V>> first(Keyspace<K, V> keyspace, KeyRange<K> range) {
        return Reads.first(state(keyspace), keyspace, range, ledger.writerPoint(applier.bound()));
    }

    @Override
    public <K, R> Optional<R> readEncoded(Keyspace<K, ?> keyspace, K key, Function<? super MemorySegment, ? extends R> reader) {
        return Reads.readEncoded(state(keyspace), keyspace, key, ledger.writerPoint(applier.bound()), reader);
    }

    @Override
    public <K> long forEachEncoded(Keyspace<K, ?> keyspace, KeyRange<K> range,
                                   BiPredicate<? super MemorySegment, ? super MemorySegment> visitor) {
        return Reads.forEachEncoded(state(keyspace), keyspace, range, ledger.writerPoint(applier.bound()), visitor);
    }

    @Override
    public long size(Keyspace<?, ?> keyspace) {
        return applier.count(state(keyspace));
    }

    @Override
    public <K, V> Transaction put(Keyspace<K, V> keyspace, K key, V value) {
        var state = state(keyspace);
        Objects.requireNonNull(value, "value");
        var encodedKey = Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key"));
        var size = keyspace.values().sizeOf(value);
        if (size < 0 || size > ValueCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Values are at most " + ValueCodec.MAX_LENGTH + " bytes: " + size);
        }
        var encodedValue = MemorySegment.ofArray(new byte[Math.toIntExact(size)]);
        keyspace.values().encode(value, encodedValue);
        applier.put(state, encodedKey, encodedValue.asReadOnly(), state.resident ? value : null);
        return this;
    }

    @Override
    public Transaction putEncoded(Keyspace<?, ?> keyspace, MemorySegment key, MemorySegment value) {
        var state = state(keyspace);
        var keyLength = key.byteSize();
        if (keyLength < 1 || keyLength > com.github.auties00.ledger.KeyCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Keys are 1 to " + com.github.auties00.ledger.KeyCodec.MAX_LENGTH + " bytes: " + keyLength);
        }
        if (value.byteSize() > ValueCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Values are at most " + ValueCodec.MAX_LENGTH + " bytes: " + value.byteSize());
        }
        var ownKey = MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
        var ownValue = MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
        applier.put(state, ownKey, ownValue, state.resident ? keyspace.values().decode(ownValue) : null);
        return this;
    }

    @Override
    public <K> Transaction delete(Keyspace<K, ?> keyspace, K key) {
        var state = state(keyspace);
        applier.delete(state, Segments.encodeKey(keyspace.keys(), Objects.requireNonNull(key, "key")));
        return this;
    }

    @Override
    public <K> Transaction deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range) {
        var state = state(keyspace);
        Objects.requireNonNull(range, "range");
        var from = range.from().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        var to = range.to().map(key -> Segments.encodeKey(keyspace.keys(), key)).orElse(null);
        applier.deleteRange(state, from, to);
        return this;
    }
}
