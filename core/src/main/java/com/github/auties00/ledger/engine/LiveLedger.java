package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/// A store: the engine of [Ledger.Builder#memory()], and of a store in a directory, which also journals every commit
/// to a [LogWriter] and checkpoints it into the trees of a [PageFile].
///
/// One writer at a time applies a commit under [#writeLock], stamping its writes, then publishes it by advancing
/// [#latest]; readers see a commit whole or not at all. After each commit the writer prunes the versions no reader can
/// need any more, applies the range removals every reader has passed, and sweeps a few keys of the whole store.
///
/// A store in a directory reads each key space's tree under the writes in memory ([ReadPoint]). Once every reader's
/// tree holds a version, the writer drops it from memory, except in resident key spaces, which keep every entry; a
/// range removal stays as a record while a reader's tree may hold keys it removed.
public final class LiveLedger implements Ledger {
    private static final int READER_SLOTS = 256;
    private static final int SWEEP_BUDGET = 64;
    private static final int PRUNE_CHUNK = 1_024;

    private final Map<String, KeyspaceState> states;
    private List<KeyspaceState> sweepOrder;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private final HeapPool pool;
    private final ReentrantLock writeLock;
    private final ReaderTable readers;
    private volatile long latest;
    private long nextSequence;
    private volatile boolean closed;
    private int sweepIndex;
    private MemorySegment sweepAfter;
    private final Map<Integer, KeyspaceState> byId;
    private int maxId;
    private LogWriter log;
    private AutoCloseable[] resources;
    private PageFile pages;
    private FreeSpace space;
    private Checkpointer checkpointer;
    // The trees of the latest published checkpoint, or null
    private volatile TreeVersion tree;

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
        this.byId = new HashMap<>();
        this.resources = new AutoCloseable[0];
    }

    /// Makes this store one in a directory, whose checkpoints are in `pages`; called before anything is recovered.
    void useDataFile(PageFile pages, FreeSpace space) {
        this.pages = pages;
        this.space = space;
    }

    /// Records that the store knows the key space `name` as `id`.
    ///
    /// A key space the store was not opened with is kept as a hidden one, so its entries survive the checkpoints of
    /// this open.
    void bind(int id, String name) {
        maxId = Math.max(maxId, id);
        var state = states.get(name);
        if (state == null) {
            if (pages == null) {
                return;
            }
            state = new KeyspaceState(Keyspace.paged(name, KeyCodec.bytes(), ValueCodec.bytes()));
            states.put(name, state);
            sweepOrder = List.copyOf(states.values());
        }
        state.id = id;
        byId.put(id, state);
    }

    /// Starts from the checkpoint `superblock` names, whose catalog is `catalog`: binds its key spaces, publishes its
    /// trees and loads the entries of the resident key spaces.
    void loadCheckpoint(Superblock superblock, Catalog.Contents catalog) {
        maxId = Math.max(maxId, catalog.maxId());
        var roots = new TreeRoot[catalog.maxId() + 1];
        for (var entry : catalog.entries()) {
            bind(entry.id(), entry.name());
            roots[entry.id()] = entry.root();
        }
        var sequence = superblock.sequence();
        tree = new TreeVersion(superblock.number(), sequence, sequence, roots, superblock.catalog(), superblock.freeList(), null);
        latest = sequence;
        nextSequence = sequence + 1;
        var stamp = Reads.stamp(sequence, 0);
        for (var entry : catalog.entries()) {
            var state = byId.get(entry.id());
            state.counts.clear();
            state.counts.put(sequence, entry.root() == null ? 0 : entry.root().count());
            if (!state.resident || entry.root() == null) {
                continue;
            }
            var cursor = new TreeCursor(pages, entry.root(), null, null, false);
            while (cursor.next()) {
                var value = cursor.value();
                state.entries.put(cursor.key(), new Version(stamp, value, state.keyspace.values().decode(value), null));
            }
        }
    }

    /// Replays the commit `sequence` the log holds.
    void replay(long sequence, byte[] body) {
        var applier = new Applier(sequence, false);
        FrameBody.readCommit(body, new FrameBody.Visitor() {
            @Override
            public void put(int keyspace, MemorySegment key, MemorySegment value) {
                var state = byId.get(keyspace);
                if (state != null) {
                    applier.put(state, key, value, state.resident ? residentValue(state, null, value) : null);
                }
            }

            @Override
            public void delete(int keyspace, MemorySegment key) {
                var state = byId.get(keyspace);
                if (state != null) {
                    applier.delete(state, key);
                }
            }

            @Override
            public void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
                var state = byId.get(keyspace);
                if (state != null) {
                    applier.deleteRange(state, from, to);
                }
            }
        });
        applier.publish();
        nextSequence = sequence + 1;
    }

    /// Gives an id to every key space the store does not know yet.
    ///
    /// @return the body of the catalog frame naming them, or `null` if there is none
    FrameBody assignIds() {
        FrameBody catalog = null;
        for (var state : states.values()) {
            if (state.id == -1) {
                state.id = ++maxId;
                byId.put(state.id, state);
                if (catalog == null) {
                    catalog = new FrameBody();
                }
                catalog.catalog(state.id, state.keyspace.name());
            }
        }
        return catalog;
    }

    /// Starts journaling to `log`, which holds every commit replayed so far, checkpointing through `checkpointer`, and
    /// makes the store close `resources` after the log when it closes.
    void attach(LogWriter log, FrameBody catalog, Checkpointer checkpointer, AutoCloseable... resources) {
        this.log = log;
        this.checkpointer = checkpointer;
        this.resources = resources;
        if (catalog != null) {
            log.append(FrameBody.CATALOG, latest, catalog);
        }
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

    /// {@return the log the store journals to, or `null` for a store in memory}
    LogWriter log() {
        return log;
    }

    /// {@return the trees of the latest published checkpoint, or `null`}
    TreeVersion tree() {
        return tree;
    }

    /// {@return what a read registered at `sequence` sees}
    ///
    /// The read must be registered before the call, so the tree it takes stays pinned.
    ReadPoint point(long sequence) {
        var version = tree;
        while (version != null && version.sequence > sequence) {
            version = version.older;
        }
        return new ReadPoint(Reads.boundOf(sequence), version, pages);
    }

    /// {@return what the writer sees at `bound`, over the latest trees}
    ReadPoint writerPoint(long bound) {
        return new ReadPoint(bound, tree, pages);
    }

    @Override
    public <K, V> Optional<V> get(Keyspace<K, V> keyspace, K key) {
        ensureOpen();
        var state = state(keyspace);
        var lease = readers.enter();
        try {
            return Reads.get(state, keyspace, key, point(lease.sequence()));
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
            return Reads.range(state, keyspace, range, limit, point(lease.sequence()));
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
            return Reads.forEach(state, keyspace, range, point(lease.sequence()), visitor);
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
            return Reads.first(state, keyspace, range, point(lease.sequence()));
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
            return Reads.readEncoded(state, keyspace, key, point(lease.sequence()), reader);
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
            return Reads.forEachEncoded(state, keyspace, range, point(lease.sequence()), visitor);
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
            return ticket(latest);
        }
        checkNotInTransaction();
        Commit commit;
        writeLock.lock();
        try {
            ensureOpen();
            checkHealthy();
            var applier = new Applier(nextSequence, log != null);
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
                // A resident value whose decoder throws, or a tree that cannot be read, stops the batch
                applier.rollback();
                throw throwable;
            }
            // The versions reference the batch's array, which the batch no longer writes
            writes.detach();
            var sequence = applier.publish();
            nextSequence++;
            commit = ticket(sequence);
        } finally {
            writeLock.unlock();
        }
        afterCommit();
        return commit;
    }

    private static Object residentValue(KeyspaceState state, Object decoded, MemorySegment value) {
        return decoded != null ? decoded : state.keyspace.values().decode(value);
    }

    @Override
    public Commit update(Consumer<Transaction> transaction) {
        Objects.requireNonNull(transaction, "transaction");
        ensureOpen();
        checkNotInTransaction();
        Commit commit;
        writeLock.lock();
        try {
            ensureOpen();
            checkHealthy();
            var applier = new Applier(nextSequence, log != null);
            var view = new LiveTransaction(this, applier);
            try {
                transaction.accept(view);
            } catch (RuntimeException | Error throwable) {
                applier.rollback();
                throw throwable;
            } finally {
                view.invalidate();
            }
            var sequence = applier.publish();
            nextSequence++;
            commit = ticket(sequence);
        } finally {
            writeLock.unlock();
        }
        afterCommit();
        return commit;
    }

    private void afterCommit() {
        if (checkpointer != null) {
            checkpointer.maybeStart();
        }
    }

    private Commit ticket(long sequence) {
        return log == null ? new CompletedCommit(sequence) : new LogCommit(sequence, log);
    }

    private void checkHealthy() {
        if (log != null) {
            log.checkHealthy();
        }
    }

    private void checkNotInTransaction() {
        if (writeLock.isHeldByCurrentThread()) {
            throw new IllegalStateException("A transaction cannot commit or start another transaction");
        }
    }

    @Override
    public void awaitDurable() throws InterruptedException {
        ensureOpen();
        if (log != null) {
            log.awaitDurable(latest);
        }
    }

    @Override
    public Snapshot snapshot() {
        ensureOpen();
        return new LiveSnapshot(this, readers.register());
    }

    @Override
    public void checkpoint() {
        ensureOpen();
        if (checkpointer != null) {
            checkpointer.run(false);
        }
    }

    @Override
    public void close() {
        close(true);
    }

    /// Closes the store as a process that stopped once its commits were durable would leave it: without a final
    /// checkpoint, so the next open replays the log. For tests of recovery.
    void closeWithoutCheckpoint() {
        close(false);
    }

    private void close(boolean checkpoint) {
        writeLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            writeLock.unlock();
        }
        try {
            if (log != null) {
                log.close();
            }
            if (checkpoint && checkpointer != null) {
                checkpointer.run(true);
            }
        } finally {
            for (var resource : resources) {
                try {
                    resource.close();
                } catch (Exception exception) {
                    // Everything is durable by now: a file, lock or pool that fails to close loses nothing
                }
            }
        }
    }

    /// The writes a checkpoint takes: everything up to its sequence, cut from the store under the writer lock.
    ///
    /// @param sequence the sequence of the last commit the checkpoint holds
    /// @param logStart the first log segment a later commit may be in
    /// @param logBytes the bytes the log had queued at the cut
    /// @param base the trees the checkpoint updates, or `null`
    /// @param maxId the highest key space id
    /// @param keyspaces what changed in each key space
    record Cut(long sequence, long logStart, long logBytes, TreeVersion base, int maxId, List<KeyspaceCut> keyspaces) {
    }

    /// What changed in one key space since the last checkpoint.
    ///
    /// @param state the key space
    /// @param dirty the keys written
    /// @param removals the range removals as of the cut
    /// @param count the entries as of the cut
    record KeyspaceCut(KeyspaceState state, TreeSet<MemorySegment> dirty, RangeRemoval[] removals, long count) {
    }

    /// Cuts the writes of a checkpoint and registers a read at its sequence, which keeps the versions it needs until
    /// [ReaderTable#unregister(long)].
    ///
    /// @param logStart the first log segment a commit after the cut may be in
    /// @return the cut, or `null` if no commit was made since the last checkpoint
    Cut cutCheckpoint(LongSupplier logStart) {
        writeLock.lock();
        try {
            var base = tree;
            var sequence = latest;
            if (sequence == (base == null ? 0 : base.sequence)) {
                return null;
            }
            var registered = readers.register();
            if (registered != sequence) {
                throw new IllegalStateException("The latest sequence moved under the writer lock");
            }
            var cuts = new ArrayList<KeyspaceCut>(states.size());
            for (var state : states.values()) {
                cuts.add(new KeyspaceCut(state, state.dirty, state.removals, state.count(sequence)));
                state.dirty = new TreeSet<>(Segments.UNSIGNED);
            }
            return new Cut(sequence, logStart.getAsLong(), log.appendedBytes(), base, maxId, cuts);
        } finally {
            writeLock.unlock();
        }
    }

    /// Gives a checkpoint that failed its keys back, so the next one writes them.
    void restoreCut(Cut cut) {
        writeLock.lock();
        try {
            for (var keyspace : cut.keyspaces()) {
                keyspace.state().dirty.addAll(keyspace.dirty());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /// {@return the key spaces as a checkpoint's catalog records them, with `roots` by id}
    List<Catalog.Entry> catalog(Cut cut, TreeRoot[] roots) {
        var entries = new ArrayList<Catalog.Entry>(cut.keyspaces().size());
        for (var keyspace : cut.keyspaces()) {
            var id = keyspace.state().id;
            entries.add(new Catalog.Entry(id, keyspace.state().keyspace.name(), id < roots.length ? roots[id] : null));
        }
        return entries;
    }

    /// Publishes the trees of a checkpoint made durable: later reads use them, and the units it freed wait for the
    /// reads of the trees before it.
    void publish(Cut cut, long number, TreeRoot[] roots, PageRef catalog, PageRef freeList, List<PageRef> freed) {
        writeLock.lock();
        try {
            tree = new TreeVersion(number, cut.sequence(), latest, roots, catalog, freeList, tree);
            space.pend(latest, freed);
        } finally {
            writeLock.unlock();
        }
    }

    /// Drops from memory the versions of the keys a published checkpoint wrote that every reader's tree holds, a few
    /// keys per hold of the writer lock.
    void pruneCut(Cut cut) {
        for (var keyspace : cut.keyspaces()) {
            var state = keyspace.state();
            if (state.resident) {
                continue;
            }
            var keys = keyspace.dirty().iterator();
            while (keys.hasNext()) {
                writeLock.lock();
                try {
                    var bounds = bounds(readers.active());
                    var floor = floor(readers.oldest());
                    for (var i = 0; i < PRUNE_CHUNK && keys.hasNext(); i++) {
                        prune(state, keys.next(), bounds, Reads.boundOf(floor));
                    }
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

    private static long[] bounds(long[] active) {
        var bounds = new long[active.length];
        for (var i = 0; i < active.length; i++) {
            bounds[i] = Reads.boundOf(active[i]);
        }
        return bounds;
    }

    /// {@return the sequence of the checkpoint every reader's tree holds, present and future: the trees in memory
    /// above it hold nothing older, or 0 if some reader may read no tree}
    ///
    /// A read at `s` uses at least the newest tree published before `s`; reads that start later use the latest tree.
    private long floor(long oldestReader) {
        var current = tree;
        if (current == null) {
            return 0;
        }
        if (oldestReader == Long.MAX_VALUE) {
            return current.sequence;
        }
        for (var version = current; version != null; version = version.older) {
            if (version.published < oldestReader) {
                return version.sequence;
            }
        }
        return 0;
    }

    /// Lets go of the trees, and the pages, no reader can use any more.
    private void releaseTrees(long oldestReader) {
        if (space == null) {
            return;
        }
        space.releasePassed(oldestReader);
        for (var version = tree; version != null; version = version.older) {
            if (version.published < oldestReader) {
                version.older = null;
                break;
            }
        }
    }

    /// Unlinks the versions of `key` that no reader can see: it keeps, for each bound of `bounds`, the newest version
    /// at or below it, and removes the key once all it keeps is a removal. In a paged key space with a tree, it also
    /// cuts the versions at or below `cutBound`, which every reader's tree holds.
    ///
    /// A reader walking the chain while it changes is not disturbed: an unlinked version keeps its own link to the
    /// versions after it, and no reader needs the versions that leaves out.
    ///
    /// @param bounds the bounds of every active reader, highest first; the first covers the newest version
    private void prune(KeyspaceState state, MemorySegment key, long[] bounds, long cutBound) {
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
        if (pages == null || state.resident) {
            // Memory holds every entry: a key whose only version is a removal has none
            if (kept == head && head.removes()) {
                state.entries.remove(key, head);
            }
            return;
        }
        // A removal stays until the trees hold it, since it hides what they hold; then, like any version, it leaves
        Version previous = null;
        for (var cut = head; cut != null; cut = cut.older) {
            if (cut.stamp <= cutBound) {
                if (previous == null) {
                    state.entries.remove(key, head);
                } else {
                    previous.older = null;
                }
                return;
            }
            previous = cut;
        }
    }

    /// Applies to the entries in memory the range removals every reader has passed, and drops the records of those
    /// no reader's tree, nor the next checkpoint, needs.
    ///
    /// @param dropBound the stamps at or below which a record goes
    private static void applyRemovals(KeyspaceState state, long bound, long dropBound) {
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
            if (!removal.applied) {
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
                removal.applied = true;
            }
            if (removal.stamp() > dropBound) {
                kept.add(removal);
            }
        }
        // Published after the entries changed, so a reader without the removal sees its effect instead
        if (kept.size() != removals.length) {
            state.removals = kept.toArray(RangeRemoval.NONE);
        }
    }

    /// Prunes up to `budget` keys, resuming where the last sweep stopped.
    private void sweep(long[] bounds, long cutBound, int budget) {
        var visitedKeyspaces = 0;
        while (budget > 0 && visitedKeyspaces <= sweepOrder.size() && !sweepOrder.isEmpty()) {
            var state = sweepOrder.get(sweepIndex % sweepOrder.size());
            var keys = (sweepAfter == null ? state.entries : state.entries.tailMap(sweepAfter, false)).keySet().iterator();
            while (budget > 0 && keys.hasNext()) {
                var key = keys.next();
                prune(state, key, bounds, cutBound);
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
        // The writes as the log records them, or null when nothing is journaled
        private final FrameBody frame;

        Applier(long sequence, boolean journaled) {
            this.sequence = sequence;
            this.deltas = new IdentityHashMap<>();
            this.touched = new ArrayList<>();
            this.removalsBefore = new IdentityHashMap<>();
            this.frame = journaled ? new FrameBody() : null;
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
            var existed = Reads.exists(state, key, writerPoint(stamp - 1));
            var head = state.entries.get(key);
            state.entries.put(key, new Version(stamp, value, decoded, head));
            touched.add(new Touched(state, key));
            if (!existed) {
                deltas.merge(state, 1L, Long::sum);
            }
            if (pages != null) {
                state.dirty.add(key);
            }
            if (frame != null) {
                frame.put(state.id, key, value);
            }
        }

        void delete(KeyspaceState state, MemorySegment key) {
            var stamp = nextStamp();
            if (!Reads.exists(state, key, writerPoint(stamp - 1))) {
                return;
            }
            var head = state.entries.get(key);
            state.entries.put(key, new Version(stamp, null, null, head));
            touched.add(new Touched(state, key));
            deltas.merge(state, -1L, Long::sum);
            if (pages != null) {
                state.dirty.add(key);
            }
            if (frame != null) {
                frame.delete(state.id, key);
            }
        }

        void deleteRange(KeyspaceState state, MemorySegment from, MemorySegment to) {
            var stamp = nextStamp();
            var removed = Reads.count(state, from, to, writerPoint(stamp - 1));
            var removals = state.removals;
            removalsBefore.putIfAbsent(state, removals);
            var grown = Arrays.copyOf(removals, removals.length + 1);
            grown[removals.length] = new RangeRemoval(from, to, stamp);
            state.removals = grown;
            if (removed != 0) {
                deltas.merge(state, -removed, Long::sum);
            }
            if (frame != null) {
                frame.deleteRange(state.id, from, to);
            }
        }

        /// Makes the commit visible to every read that starts afterward, then maintains the store.
        ///
        /// @return the sequence of the commit
        long publish() {
            // Queued before it is visible, so the log holds every commit a reader can see, in order
            if (frame != null) {
                try {
                    log.append(FrameBody.COMMIT, sequence, frame);
                } catch (RuntimeException exception) {
                    rollback();
                    throw exception;
                }
            }
            for (var delta : deltas.entrySet()) {
                var state = delta.getKey();
                state.counts.put(sequence, state.counts.lastEntry().getValue() + delta.getValue());
            }
            latest = sequence;
            maintain();
            return sequence;
        }

        /// Takes back every write this commit applied; no reader saw them, since the commit was never published.
        ///
        /// The keys it wrote stay marked for the next checkpoint, which writes their unchanged state.
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
            var bounds = bounds(active);
            var oldestReader = readers.oldest();
            var floor = floor(oldestReader);
            releaseTrees(oldestReader);
            var cutBound = Reads.boundOf(floor);
            // Without a data file a record goes once applied; with one, once every reader's tree holds it
            var dropBound = pages == null ? Long.MAX_VALUE : cutBound;
            var oldest = active[active.length - 1];
            for (var write : touched) {
                prune(write.state(), write.key(), bounds, cutBound);
            }
            for (var state : states.values()) {
                applyRemovals(state, bounds[bounds.length - 1], dropBound);
                var countFloor = state.counts.floorKey(oldest);
                if (countFloor != null) {
                    state.counts.headMap(countFloor, false).clear();
                }
            }
            sweep(bounds, cutBound, SWEEP_BUDGET);
        }
    }

    /// A key a commit wrote.
    private record Touched(KeyspaceState state, MemorySegment key) {
    }
}
