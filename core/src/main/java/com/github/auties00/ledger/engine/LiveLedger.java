package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.WriteBatch;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;

/// A store whose entries live in memory: the engine of [Ledger.Builder#memory()], and the part of every store that
/// holds the writes since its last checkpoint.
///
/// One writer at a time applies a commit under [#writeLock], stamping its writes, then publishes it by advancing
/// [#latest]; readers see a commit whole or not at all. After each commit the writer prunes the versions no reader can
/// need any more, applies the range removals every reader has passed, and sweeps a few keys of the whole store.
public final class LiveLedger implements Ledger {
    private static final int READER_SLOTS = 256;
    private static final int SWEEP_BUDGET = 64;

    private final Map<String, KeyspaceState> states;
    private final List<KeyspaceState> sweepOrder;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private final HeapPool pool;
    private final ReentrantLock writeLock;
    private final ReaderTable readers;
    private volatile long latest;
    private long nextSequence;
    private volatile boolean closed;
    private int sweepIndex;
    private MemorySegment sweepAfter;

    /// Creates an empty store with `keyspaces`.
    ///
    /// @param keyspaces the key spaces, with unique names
    /// @param pool the memory of the store's write batches
    public LiveLedger(Collection<Keyspace<?, ?>> keyspaces, HeapPool pool) {
        this.states = new LinkedHashMap<>();
        this.keyspaces = new LinkedHashMap<>();
        for (var keyspace : keyspaces) {
            this.states.put(keyspace.name(), new KeyspaceState(keyspace));
            this.keyspaces.put(keyspace.name(), keyspace);
        }
        this.sweepOrder = List.copyOf(states.values());
        this.pool = pool;
        this.writeLock = new ReentrantLock();
        this.readers = new ReaderTable(READER_SLOTS, () -> latest);
        this.nextSequence = 1;
    }

    KeyspaceState state(Keyspace<?, ?> keyspace) {
        Objects.requireNonNull(keyspace, "keyspace");
        var state = states.get(keyspace.name());
        if (state == null || !state.keyspace.equals(keyspace)) {
            throw new IllegalArgumentException("The store was not opened with " + keyspace);
        }
        return state;
    }

    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("The store is closed");
        }
    }

    ReaderTable readers() {
        return readers;
    }

    long latest() {
        return latest;
    }

    @Override
    public <K, V> Optional<V> get(Keyspace<K, V> keyspace, K key) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.get(state, keyspace, key, Reads.boundOf(lease.sequence()));
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public <K, V> List<Entry<K, V>> range(Keyspace<K, V> keyspace, KeyRange<K> range, int limit) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.range(state, keyspace, range, limit, Reads.boundOf(lease.sequence()));
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public <K, V> long forEach(Keyspace<K, V> keyspace, KeyRange<K> range, BiPredicate<? super K, ? super V> visitor) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.forEach(state, keyspace, range, Reads.boundOf(lease.sequence()), visitor);
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public <K, V> Optional<Entry<K, V>> first(Keyspace<K, V> keyspace, KeyRange<K> range) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.first(state, keyspace, range, Reads.boundOf(lease.sequence()));
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public <K, R> Optional<R> readEncoded(Keyspace<K, ?> keyspace, K key, Function<? super MemorySegment, ? extends R> reader) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.readEncoded(state, keyspace, key, Reads.boundOf(lease.sequence()), reader);
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public <K> long forEachEncoded(Keyspace<K, ?> keyspace, KeyRange<K> range,
                                   BiPredicate<? super MemorySegment, ? super MemorySegment> visitor) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.forEachEncoded(state, keyspace, range, Reads.boundOf(lease.sequence()), visitor);
        } finally {
            readers.exit(lease);
        }
    }

    @Override
    public long size(Keyspace<?, ?> keyspace) {
        ensureOpen();
        return state(keyspace).count(latest);
    }

    @Override
    public WriteBatch newBatch() {
        ensureOpen();
        return new ArrayWriteBatch(this, pool, keyspaces);
    }

    @Override
    public Commit commit(WriteBatch batch) {
        Objects.requireNonNull(batch, "batch");
        if (!(batch instanceof ArrayWriteBatch writes) || writes.owner() != this) {
            throw new IllegalArgumentException("Another store created the batch");
        }
        ensureOpen();
        if (writes.size() > Reads.MAX_OPS) {
            throw new IllegalArgumentException("A batch holds at most " + Reads.MAX_OPS + " writes: " + writes.size());
        }
        if (writes.isEmpty()) {
            return new CompletedCommit(latest);
        }
        checkNotInTransaction();
        writeLock.lock();
        try {
            ensureOpen();
            var applier = new Applier(nextSequence);
            try {
                for (var i = 0; i < writes.size(); i++) {
                    var state = states.get(writes.keyspace(i).name());
                    switch (writes.kind(i)) {
                        case ArrayWriteBatch.PUT -> {
                            var value = writes.value(i);
                            var decoded = state.resident ? residentValue(state, writes.decodedValue(i), value) : null;
                            applier.put(state, writes.key(i), value, decoded);
                        }
                        case ArrayWriteBatch.DELETE -> applier.delete(state, writes.key(i));
                        case ArrayWriteBatch.DELETE_RANGE -> applier.deleteRange(state, writes.key(i), writes.value(i));
                        default -> throw new IllegalStateException("Unknown write kind " + writes.kind(i));
                    }
                }
            } catch (RuntimeException | Error throwable) {
                // A resident value whose decoder throws stops the batch: none of its writes apply
                applier.rollback();
                throw throwable;
            }
            nextSequence++;
            // The versions reference the batch's array, which the batch no longer writes
            writes.detach();
            return new CompletedCommit(applier.publish());
        } finally {
            writeLock.unlock();
        }
    }

    private static Object residentValue(KeyspaceState state, Object decoded, MemorySegment value) {
        return decoded != null ? decoded : state.keyspace.values().decode(value);
    }

    @Override
    public Commit update(Consumer<Transaction> transaction) {
        Objects.requireNonNull(transaction, "transaction");
        ensureOpen();
        checkNotInTransaction();
        writeLock.lock();
        try {
            ensureOpen();
            var applier = new Applier(nextSequence);
            var view = new LiveTransaction(this, applier);
            try {
                transaction.accept(view);
            } catch (RuntimeException | Error throwable) {
                applier.rollback();
                throw throwable;
            } finally {
                view.invalidate();
            }
            nextSequence++;
            return new CompletedCommit(applier.publish());
        } finally {
            writeLock.unlock();
        }
    }

    private void checkNotInTransaction() {
        if (writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("A transaction cannot commit or start another transaction");
        }
    }

    @Override
    public void awaitDurable() {
        ensureOpen();
    }

    @Override
    public Snapshot snapshot() {
        ensureOpen();
        return new LiveSnapshot(this, readers.register());
    }

    @Override
    public void checkpoint() {
        ensureOpen();
    }

    @Override
    public void close() {
        closed = true;
    }

    /// Unlinks the versions of `key` that no reader can see: it keeps, for each bound of `bounds`, the newest version
    /// at or below it, and removes the key once all it keeps is a removal.
    ///
    /// A reader walking the chain while it changes is not disturbed: an unlinked version keeps its own link to the
    /// versions after it, and no reader needs the versions that leaves out.
    ///
    /// @param bounds the bounds of every active reader, highest first; the first covers the newest version
    private static void prune(KeyspaceState state, MemorySegment key, long[] bounds) {
        var head = state.entries.get(key);
        if (head == null) {
            return;
        }
        Version kept = null;
        var version = head;
        for (var bound : bounds) {
            while (version != null && version.stamp > bound) {
                version = version.older;
            }
            if (version == null) {
                break;
            }
            if (version != kept) {
                if (kept != null) {
                    kept.older = version;
                }
                kept = version;
            }
        }
        if (kept == null) {
            return;
        }
        kept.older = null;
        if (kept == head && head.removes()) {
            state.entries.remove(key, head);
        }
    }

    /// Applies to the entries the range removals every reader has passed, then drops them.
    private static void applyRemovals(KeyspaceState state, long bound) {
        var removals = state.removals;
        if (removals.length == 0) {
            return;
        }
        var kept = new ArrayList<RangeRemoval>(removals.length);
        for (var removal : removals) {
            if (removal.stamp() > bound) {
                kept.add(removal);
                continue;
            }
            for (var entry : state.slice(removal.from(), removal.to()).entrySet()) {
                var head = entry.getValue();
                Version newest = null;
                var version = head;
                while (version != null && version.stamp > removal.stamp()) {
                    newest = version;
                    version = version.older;
                }
                if (newest == null) {
                    state.entries.remove(entry.getKey(), head);
                } else {
                    newest.older = null;
                }
            }
        }
        // Published after the entries changed, so a reader without the removal sees its effect instead
        if (kept.size() != removals.length) {
            state.removals = kept.toArray(RangeRemoval.NONE);
        }
    }

    /// Prunes up to `budget` keys, resuming where the last sweep stopped.
    private void sweep(long[] bounds, int budget) {
        var visitedKeyspaces = 0;
        while (budget > 0 && visitedKeyspaces <= sweepOrder.size() && !sweepOrder.isEmpty()) {
            var state = sweepOrder.get(sweepIndex % sweepOrder.size());
            var keys = (sweepAfter == null ? state.entries : state.entries.tailMap(sweepAfter, false)).keySet().iterator();
            while (budget > 0 && keys.hasNext()) {
                var key = keys.next();
                prune(state, key, bounds);
                sweepAfter = key;
                budget--;
            }
            if (!keys.hasNext()) {
                sweepIndex = (sweepIndex + 1) % sweepOrder.size();
                sweepAfter = null;
                visitedKeyspaces++;
            }
        }
    }

    /// The writes of one commit or transaction, applied as they come and published together.
    final class Applier {
        private final long sequence;
        private int op;
        private final IdentityHashMap<KeyspaceState, Long> deltas;
        private final List<Touched> touched;
        private final IdentityHashMap<KeyspaceState, RangeRemoval[]> removalsBefore;

        Applier(long sequence) {
            this.sequence = sequence;
            this.deltas = new IdentityHashMap<>();
            this.touched = new ArrayList<>();
            this.removalsBefore = new IdentityHashMap<>();
        }

        /// {@return the bound of reads that see the latest commit and this one's writes so far}
        long bound() {
            return Reads.boundOf(sequence);
        }

        /// {@return the number of entries of `state` once this commit applies}
        long count(KeyspaceState state) {
            return state.counts.lastEntry().getValue() + deltas.getOrDefault(state, 0L);
        }

        private long nextStamp() {
            if (op > Reads.MAX_OPS) {
                throw new IllegalStateException("A commit holds at most " + Reads.MAX_OPS + " writes");
            }
            return Reads.stamp(sequence, op++);
        }

        void put(KeyspaceState state, MemorySegment key, MemorySegment value, Object decoded) {
            var stamp = nextStamp();
            var head = state.entries.get(key);
            var existed = KeyspaceState.visible(head, key, stamp - 1, state.removals) != null;
            state.entries.put(key, new Version(stamp, value, decoded, head));
            touched.add(new Touched(state, key));
            if (!existed) {
                deltas.merge(state, 1L, Long::sum);
            }
        }

        void delete(KeyspaceState state, MemorySegment key) {
            var stamp = nextStamp();
            var head = state.entries.get(key);
            if (KeyspaceState.visible(head, key, stamp - 1, state.removals) == null) {
                return;
            }
            state.entries.put(key, new Version(stamp, null, null, head));
            touched.add(new Touched(state, key));
            deltas.merge(state, -1L, Long::sum);
        }

        void deleteRange(KeyspaceState state, MemorySegment from, MemorySegment to) {
            var stamp = nextStamp();
            var removals = state.removals;
            var removed = 0L;
            for (var entry : state.slice(from, to).entrySet()) {
                if (KeyspaceState.visible(entry.getValue(), entry.getKey(), stamp - 1, removals) != null) {
                    removed++;
                }
            }
            removalsBefore.putIfAbsent(state, removals);
            var grown = Arrays.copyOf(removals, removals.length + 1);
            grown[removals.length] = new RangeRemoval(from, to, stamp);
            state.removals = grown;
            if (removed != 0) {
                deltas.merge(state, -removed, Long::sum);
            }
        }

        /// Makes the commit visible to every read that starts afterward, then maintains the store.
        ///
        /// @return the sequence of the commit
        long publish() {
            for (var delta : deltas.entrySet()) {
                var state = delta.getKey();
                state.counts.put(sequence, state.counts.lastEntry().getValue() + delta.getValue());
            }
            latest = sequence;
            maintain();
            return sequence;
        }

        /// Takes back every write this commit applied; no reader saw them, since the commit was never published.
        void rollback() {
            for (var write : touched) {
                var head = write.state().entries.get(write.key());
                var version = head;
                while (version != null && version.stamp >>> Reads.OP_BITS == sequence) {
                    version = version.older;
                }
                if (version == null) {
                    write.state().entries.remove(write.key());
                } else if (version != head) {
                    write.state().entries.put(write.key(), version);
                }
            }
            for (var restored : removalsBefore.entrySet()) {
                restored.getKey().removals = restored.getValue();
            }
        }

        private void maintain() {
            var active = readers.active();
            var bounds = new long[active.length];
            for (var i = 0; i < active.length; i++) {
                bounds[i] = Reads.boundOf(active[i]);
            }
            var oldest = active[active.length - 1];
            for (var write : touched) {
                prune(write.state(), write.key(), bounds);
            }
            for (var state : states.values()) {
                applyRemovals(state, bounds[bounds.length - 1]);
                var floor = state.counts.floorKey(oldest);
                if (floor != null) {
                    state.counts.headMap(floor, false).clear();
                }
            }
            sweep(bounds, SWEEP_BUDGET);
        }
    }

    /// A key a commit wrote.
    private record Touched(KeyspaceState state, MemorySegment key) {
    }
}
