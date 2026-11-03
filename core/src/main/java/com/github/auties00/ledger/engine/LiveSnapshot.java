package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Snapshot;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Function;

/// A [Snapshot] of a [LiveLedger], registered with its reader table at the sequence it reads.
final class LiveSnapshot implements Snapshot {
    private final LiveLedger ledger;
    private final long sequence;
    private final long bound;
    private final AtomicBoolean closed;

    LiveSnapshot(LiveLedger ledger, long sequence) {
        this.ledger = ledger;
        this.sequence = sequence;
        this.bound = Reads.boundOf(sequence);
        this.closed = new AtomicBoolean();
    }

    private KeyspaceState state(Keyspace<?, ?> keyspace) {
        if (closed.get()) {
            throw new IllegalStateException("The snapshot is closed");
        }
        ledger.ensureOpen();
        return ledger.state(keyspace);
    }

    @Override
    public long sequence() {
        return sequence;
    }

    @Override
    public <K, V> Optional<V> get(Keyspace<K, V> keyspace, K key) {
        return Reads.get(state(keyspace), keyspace, key, bound);
    }

    @Override
    public <K, V> List<Entry<K, V>> range(Keyspace<K, V> keyspace, KeyRange<K> range, int limit) {
        return Reads.range(state(keyspace), keyspace, range, limit, bound);
    }

    @Override
    public <K, V> long forEach(Keyspace<K, V> keyspace, KeyRange<K> range, BiPredicate<? super K, ? super V> visitor) {
        return Reads.forEach(state(keyspace), keyspace, range, bound, visitor);
    }

    @Override
    public <K, V> Optional<Entry<K, V>> first(Keyspace<K, V> keyspace, KeyRange<K> range) {
        return Reads.first(state(keyspace), keyspace, range, bound);
    }

    @Override
    public <K, R> Optional<R> readEncoded(Keyspace<K, ?> keyspace, K key, Function<? super MemorySegment, ? extends R> reader) {
        return Reads.readEncoded(state(keyspace), keyspace, key, bound, reader);
    }

    @Override
    public <K> long forEachEncoded(Keyspace<K, ?> keyspace, KeyRange<K> range,
                                   BiPredicate<? super MemorySegment, ? super MemorySegment> visitor) {
        return Reads.forEachEncoded(state(keyspace), keyspace, range, bound, visitor);
    }

    @Override
    public long size(Keyspace<?, ?> keyspace) {
        return state(keyspace).count(sequence);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            ledger.readers().unregister(sequence);
        }
    }
}
