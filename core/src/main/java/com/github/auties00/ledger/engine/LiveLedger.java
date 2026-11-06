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
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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
import java.util.Set;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/// A store: the engine of [Ledger.Builder#memory()], and of a store in a directory, which also journals every commit
/// to a [LogWriter] and checkpoints it into the trees of a [PageFile].
///
/// A batch commits through a pipeline that holds no lock ([PendingCommit]): the committing thread prepares it, the
/// thread holding the sequencing role gives it the next sequence in the order commits arrive, the committing thread
/// applies its writes once the commits before it on the same keys have applied theirs, and the thread holding the
/// publishing role publishes the applied commits in the order of their sequences by advancing [#latest]. Readers see
/// a commit whole or not at all. Transactions and batches with a range removal run alone instead: they hold both roles
/// once every commit given a sequence has published.
///
/// After publishing, the publisher prunes the versions no reader can need any more, applies the range removals every
/// reader has passed, and sweeps a few keys of the whole store; prunes and drops of chains happen only while holding
/// the publishing role, so a chain is dropped only once its last writer has published.
///
/// A store in a directory reads each key space's tree under the writes in memory ([ReadPoint]). Once every reader's
/// tree holds a version, the writer drops it from memory, except in resident key spaces, which keep every entry; a
/// range removal stays as a record while a reader's tree may hold keys it removed.
public final class LiveLedger implements Ledger {
    /// The bytes by which the array a resident value came in may exceed it before the value takes an array of its own.
    private static final int RESIDENT_SLACK = 64;
    private static final int READER_SLOTS = 256;
    private static final int SWEEP_BUDGET = 64;
    private static final int PRUNE_CHUNK = 1_024;
    /// The dropped chains a commit takes out of the ordered maps, more than the sweep and its own writes drop.
    private static final int COMMIT_REMOVALS = 256;
    /// The passes of the publisher between two maintenances when nothing asks for one sooner: the counts and trees
    /// readers no longer need wait that long to go.
    private static final int MAINTAIN_EVERY = 16;
    private static final VarHandle LAST_PUBLISHED;
    private static final VarHandle LATEST;

    static {
        try {
            LAST_PUBLISHED = MethodHandles.lookup().findVarHandle(Padded.PublishedFields.class, "last", PendingCommit.class);
            LATEST = MethodHandles.lookup().findVarHandle(Padded.PublishedFields.class, "latest", long.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }
    /// The most threads a commit reads tree pages with before it is given a sequence, about the queue depth that
    /// keeps a solid-state drive busy.
    private static final int PREFETCH_THREADS = 32;

    private final Map<String, KeyspaceState> states;
    private List<KeyspaceState> sweepOrder;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private final HeapPool pool;
    // The last commit to arrive for a sequence: an arrival swaps itself in, then links the commit it replaced to
    // itself through PendingCommit.arrival, so the arrivals form a list in the order of the swaps
    private final Padded.CommitReference arrivalTail;
    // The last arrival the sequencer took off the list, from which it follows the links; written holding the
    // sequencing role
    private final Padded.CommitReference arrivalCursor;
    // Held by the thread giving sequences to the arrived commits, or by an exclusive operation
    private final Padded.Role sequencing;
    // Held by the thread publishing applied commits in order, by maintenance, or by an exclusive operation
    private final Padded.Role publishing;
    // The last commit given a sequence and the next sequence; guarded by sequencing
    private final Padded.Sequenced sequenced;
    // The latest sequence and the last commit published, which an exclusive operation waits to reach the last
    // sequenced; written under publishing
    private final Padded.Published published;
    // Raised by the thread that waits for the sequencing role, or holds it, for an exclusive operation: the commits
    // then leave the role to it, so it is not starved by threads that take the role again and again
    private final Padded.Role sequencingWanted;
    // Raised by the thread that waits for the publishing role, or holds it, for an exclusive operation or the work of
    // a checkpoint
    private final Padded.Role publishingWanted;
    // Taken by the threads that want the roles for exclusive operations and checkpoints; barging, so transactions
    // that follow each other hand it on without parking, except while a checkpoint waits for it
    private final ReentrantLock requests;
    // Raised while the work of a checkpoint waits for the requests lock: transactions then stop barging
    private final Padded.Role checkpointWaiting;
    // publishApplied, which waiting commits run to help, made once
    private final Runnable publishHelper;
    // The frames of a pass of the publisher, queued in the log together; guarded by the publishing role
    private final LogWriter.Batch passFrames;
    // The passes of the publisher since the last maintenance; guarded by the publishing role
    private int passesSinceMaintenance;
    // The thread running an exclusive operation, or null
    private volatile Thread exclusiveOwner;
    // The failure of the log that stopped publication, after which no commit publishes, or null
    private volatile RuntimeException broken;
    private final ReaderTable readers;
    // The latest sequence, for the drops of chains
    private final LongSupplier latestSequence;
    // Raised by a prune that left a chain a later one may shorten, so the sweep starts a pass; lowered when it does
    private final Padded.Role sweepWanted;
    private volatile boolean closed;
    private int sweepIndex;
    private MemorySegment sweepAfter;
    // Whether a prune left a chain that a later one may shorten, so the sweep has work; guarded by the publishing role
    private boolean sweepPending;
    // Whether the sweep's current pass over the store found such a chain
    private boolean sweepFound;
    // The reader bounds and floor the current pass started under
    private long[] passBounds;
    private long passCutBound;
    // The bounds and floor of the last complete pass that left chains, or null: under the same ones a pass frees nothing
    private long[] sweptBounds;
    private long sweptCutBound;
    private final Map<Integer, KeyspaceState> byId;
    private int maxId;
    private LogWriter log;
    private AutoCloseable[] resources;
    private PageFile pages;
    private FreeSpace space;
    private Checkpointer checkpointer;
    // The trees of the latest published checkpoint, or null
    private volatile TreeVersion tree;
    // The file of large values, or null for a store in memory
    private BlobFile blobs;
    // The threshold of the key spaces the store finds in its catalog without being opened with them
    private long largeValues = Long.MAX_VALUE;
    // The versions whose units of the file of large values no published checkpoint has taken; guarded by the
    // publishing role
    private final ArrayList<InFlight> inFlight;

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
        var arrivalStart = new PendingCommit(0);
        this.arrivalTail = new Padded.CommitReference(arrivalStart);
        this.arrivalCursor = new Padded.CommitReference(arrivalStart);
        this.sequencing = new Padded.Role();
        this.publishing = new Padded.Role();
        this.sequenced = new Padded.Sequenced();
        this.published = new Padded.Published();
        sequenced.last = new PendingCommit(0);
        published.last = sequenced.last;
        this.latestSequence = this::latest;
        this.readers = new ReaderTable(READER_SLOTS, latestSequence);
        this.sweepWanted = new Padded.Role();
        this.sequencingWanted = new Padded.Role();
        this.publishingWanted = new Padded.Role();
        this.requests = new ReentrantLock();
        this.checkpointWaiting = new Padded.Role();
        this.publishHelper = this::publishApplied;
        this.passFrames = new LogWriter.Batch();
        sequenced.next = 1;
        this.byId = new HashMap<>();
        this.resources = new AutoCloseable[0];
        this.inFlight = new ArrayList<>();
    }

    /// Makes this store one in a directory, whose checkpoints are in `pages`; called before anything is recovered.
    void useDataFile(PageFile pages, FreeSpace space) {
        this.pages = pages;
        this.space = space;
        for (var state : states.values()) {
            state.lazy = !state.resident;
        }
    }

    /// Makes `blobs` the file of large values: a batch writes there each value of at least `threshold` bytes, or of
    /// the bytes `overrides` gives for its key space by name; called before anything is recovered.
    void useBlobs(BlobFile blobs, long threshold, Map<String, Long> overrides) {
        this.blobs = blobs;
        this.largeValues = threshold;
        for (var state : states.values()) {
            state.largeValues = overrides.getOrDefault(state.keyspace.name(), threshold);
        }
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
            state.largeValues = largeValues;
            state.lazy = true;
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
        tree = new TreeVersion(superblock.number(), sequence, sequence, roots, superblock.catalog(), superblock.freeList(),
                superblock.blobFreeList(), null);
        published.latest = sequence;
        sequenced.next = sequence + 1;
        var stamp = Reads.stamp(sequence, 0);
        for (var entry : catalog.entries()) {
            var state = byId.get(entry.id());
            state.counts.clear();
            state.counts.put(sequence, entry.root() == null ? 0 : entry.root().count());
            if (!state.resident || entry.root() == null) {
                continue;
            }
            for (var leaf : TreeSearch.leaves(pages, entry.root())) {
                for (var i = 0; i < TreeNodes.count(leaf); i++) {
                    var offset = TreeNodes.entry(leaf, i);
                    var value = TreeSearch.value(pages, leaf, offset, false);
                    state.open(TreeNodes.leafKey(leaf, offset)).head = new Version(stamp, value, state.keyspace.values().decode(value), null);
                }
            }
        }
    }

    /// Replays the commit `sequence` the log holds, unless the file of large values lacks one of its units.
    ///
    /// The units are read before anything applies, and taken again from the free pages once the commit applies.
    ///
    /// @return whether it replayed the commit
    boolean replay(long sequence, byte[] body) {
        var units = new ArrayList<byte[]>();
        var missing = new boolean[1];
        FrameBody.readCommit(body, new FrameBody.Visitor() {
            @Override
            public void put(int keyspace, MemorySegment key, MemorySegment value) {
            }

            @Override
            public void delete(int keyspace, MemorySegment key) {
            }

            @Override
            public void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
            }

            @Override
            public void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
                var unit = blobs == null || missing[0] ? null : blobs.readUnit(ref);
                if (unit == null || Bytes.i32(unit, PageFormat.LENGTH) != length) {
                    missing[0] = true;
                } else {
                    units.add(unit);
                }
            }
        });
        if (missing[0]) {
            return false;
        }
        var next = new int[1];
        var applier = new Applier(sequence, false);
        FrameBody.readCommit(body, new FrameBody.Visitor() {
            @Override
            public void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
                var unit = units.get(next[0]++);
                var state = byId.get(keyspace);
                if (state != null) {
                    var value = MemorySegment.ofArray(unit).asSlice(PageFormat.HEADER, length).asReadOnly();
                    applier.put(state, key, value, state.resident ? residentValue(state, null, value) : null, TreeSearch.UNKNOWN, ref);
                    blobs.claim(ref);
                }
            }

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
        sequenced.next = sequence + 1;
        return true;
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
            log.append(FrameBody.CATALOG, latest(), catalog);
        }
        if (blobs != null) {
            // Durable before this open's first unit, so no later open reuses its nonces; it goes to the log with the
            // first commit, whose force it then shares, and a close with no commit drops it, as no unit took the epoch
            log.defer(FrameBody.EPOCH, latest(), FrameBody.epoch(blobs.epoch()));
            blobs.epochGate(() -> log.awaitFrames(log.flushDeferred()));
            log.useBlobs(blobs);
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
        return published.latest;
    }

    /// {@return the log the store journals to, or `null` for a store in memory}
    LogWriter log() {
        return log;
    }

    /// {@return the checkpointer of a store in a directory, or `null` for a store in memory}
    Checkpointer checkpointer() {
        return checkpointer;
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
        var state = state(keyspace);
        // Registered as a read: the publisher drops the counts below the oldest read, and the latest may move on
        var lease = readers.enter();
        try {
            return state.count(point(lease.sequence()));
        } finally {
            readers.exit(lease);
        }
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
            return ticket(latest());
        }
        checkNotInTransaction();
        if (checkpointer != null) {
            checkpointer.throttle();
        }
        writes.compact();
        var units = writeUnits(writes);
        // Needed only to release the units of large values no log frame names
        var outcome = units == null ? null : new Applier[1];
        Commit commit;
        try {
            commit = rangeRemoval(writes) ? commitAlone(writes, units, outcome) : commitPipelined(writes, units, outcome);
        } finally {
            if (outcome != null && (outcome[0] == null || !outcome[0].logged())) {
                // No log frame names them
                for (var unit : units) {
                    if (unit != null) {
                        blobs.release(unit);
                    }
                }
            }
        }
        afterCommit();
        return commit;
    }

    /// {@return whether `writes` holds a range removal}
    private static boolean rangeRemoval(ArrayWriteBatch writes) {
        for (var i = 0; i < writes.size(); i++) {
            if (writes.kind(i) == ArrayWriteBatch.DELETE_RANGE) {
                return true;
            }
        }
        return false;
    }

    /// Commits `writes`, which holds a range removal, as an exclusive operation: the removal counts the keys it
    /// removes, which needs every commit before it applied and none after it started.
    ///
    /// @param outcome receives the writes as applied, or `null`
    private Commit commitAlone(ArrayWriteBatch writes, PageRef[] units, Applier[] outcome) {
        var hints = prefetch(writes, null, null);
        enterExclusive();
        try {
            ensureOpen();
            checkHealthy();
            var applier = new Applier(sequenced.next, log != null);
            if (outcome != null) {
                outcome[0] = applier;
            }
            try {
                for (var i = 0; i < writes.size(); i++) {
                    var state = states.get(writes.keyspace(i).name());
                    switch (writes.kind(i)) {
                        case ArrayWriteBatch.PUT -> {
                            var value = writes.value(i);
                            var decoded = state.resident ? residentValue(state, writes.decodedValue(i), value) : null;
                            applier.put(state, applier.claim(state, writes.key(i)), writes.key(i), value, writes.view(), writes.valueOffset(i), decoded,
                                    hints == null ? TreeSearch.UNKNOWN : hints.of(i, tree), units == null ? null : units[i]);
                        }
                        case ArrayWriteBatch.DELETE ->
                                applier.delete(state, writes.key(i), hints == null ? TreeSearch.UNKNOWN : hints.of(i, tree));
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
            sequenced.next++;
            return ticket(sequence);
        } finally {
            exitExclusive();
        }
    }

    /// Commits `writes` through the pipeline: prepared here, given a sequence in the order commits arrive, applied
    /// here once the commits before it that write the same keys have, and published in the order of the sequences.
    ///
    /// No lock is held at any point: commits on different keys apply at once, and a commit waits only for the
    /// commits before it on its own keys, then for its turn to become visible.
    ///
    /// @param outcome receives the writes as applied, or `null`
    private Commit commitPipelined(ArrayWriteBatch writes, PageRef[] units, Applier[] outcome) {
        var size = writes.size();
        var chains = new Chain[size];
        var keyspaceStates = new KeyspaceState[size];
        var hints = prefetch(writes, chains, keyspaceStates);
        Object[] decoded = null;
        for (var i = 0; i < size; i++) {
            if (writes.kind(i) == ArrayWriteBatch.PUT && keyspaceStates[i].resident) {
                if (decoded == null) {
                    decoded = new Object[size];
                }
                // A decoder that throws stops the batch before it has a sequence
                decoded[i] = residentValue(keyspaceStates[i], writes.decodedValue(i), writes.value(i));
            }
        }
        var pending = new PendingCommit(writes, chains, keyspaceStates, decoded, units, hints);
        if (arrivalTail.commit == arrivalCursor.commit && !sequencingWanted.held() && sequencing.tryAcquireBriefly()) {
            // No commit arrived before this one: it takes its sequence without the queue, and lets the role go with
            // a release store, looking for no arrival: one that finds the role held briefly sequences itself
            try {
                sequence(pending);
            } finally {
                sequencing.releaseLazily();
            }
        } else {
            arrive(pending);
        }
        awaitSequenced(pending);
        if (pending.failure != null) {
            throw rethrow(pending.failure);
        }
        apply(pending);
        awaitPublished(pending);
        // Forgotten here rather than by the publisher, whose cache would have to fetch each chain's line
        for (var chain : pending.chains) {
            if (chain != null) {
                chain.forget(pending);
            }
        }
        pending.predecessors = null;
        if (outcome != null) {
            outcome[0] = pending.applier;
        }
        if (pending.failure != null) {
            throw rethrow(pending.failure);
        }
        pruneWritten(pending.applier.touched);
        // The versions reference the batch's array, which the batch no longer writes
        writes.detach();
        return ticket(pending.sequence);
    }

    /// Waits until `pending` has a sequence, giving the arrived commits their sequences whenever no other thread does.
    ///
    /// A thread lets a role go with a release store, which does not wait for the reads after it: it may miss a commit
    /// that arrived as it let go, whose own thread then takes the role here.
    private void awaitSequenced(PendingCommit pending) {
        // One try: a thread that holds the role, or leaves it with a volatile store, sees the commit after
        drainArrivals();
        // The wait gives up, so this thread helps, while the role is free or held by a commit sequencing itself; a
        // park that ends early (a spurious wake-up, or the guard's timeout) helps again too
        while (pending.state < PendingCommit.SEQUENCED
                && !pending.await(PendingCommit.SEQUENCED, sequencing, sequencingWanted)) {
            if (sequencing.heldBriefly()) {
                Thread.onSpinWait();
            }
            drainArrivals();
        }
    }

    /// Waits until `pending` is published, publishing the applied commits whenever no other thread does.
    private void awaitPublished(PendingCommit pending) {
        // One try, after the volatile store of APPLIED: a thread that holds the role, or leaves it with a volatile
        // store, sees the commit after
        publishApplied();
        // The wait helps every few spins while the role is free: a commit is then published by the first thread that
        // notices it applied, not by the next pass of a holder
        pending.awaitHelping(PendingCommit.DONE, publishing, publishingWanted, publishHelper);
    }

    /// Adds `pending` to the commits waiting for a sequence.
    ///
    /// Between the swap and the link, the sequencer cannot reach the commit, and stops before it; the commit's own
    /// thread then takes the role and sequences it, since it announces a park with a full fence and parks only while
    /// another thread holds the role, which re-reads the link after letting the role go.
    private void arrive(PendingCommit pending) {
        arrivalTail.getAndSet(pending).linkArrival(pending);
    }

    /// {@return whether a commit is linked after the last one the sequencer took off the arrival list}
    private boolean arrivalLinked() {
        return arrivalCursor.commit.arrival != null;
    }

    /// Gives a sequence to every arrived commit, in the order they arrived, unless another thread or an exclusive
    /// operation holds the role.
    private void drainArrivals() {
        while (arrivalLinked() && !sequencingWanted.held() && sequencing.tryAcquire()) {
            PendingCommit first = null;
            PendingCommit last = null;
            var cursor = arrivalCursor.commit;
            try {
                PendingCommit pending;
                while ((pending = cursor.arrival) != null) {
                    cursor.unlinkArrival();
                    cursor = pending;
                    if (sequence(pending)) {
                        // A list of this thread's own, which outlives the role
                        if (last == null) {
                            first = pending;
                        } else {
                            last.sequencedNext = pending;
                        }
                        last = pending;
                    }
                }
            } finally {
                arrivalCursor.setLazily(cursor);
                sequencing.releaseLazily();
                // One fence after every move of the pass and the release: each committing thread that parks is seen
                // parking, and the queue is read again after the role went
                VarHandle.fullFence();
                for (var commit = first; commit != null; ) {
                    var next = commit.sequencedNext;
                    commit.sequencedNext = null;
                    commit.wake();
                    commit = next;
                }
            }
        }
    }

    /// Gives `pending` the next sequence and, for each of its keys, the commit given a write of it just before.
    ///
    /// @return whether it did, with a release store the caller follows by a full fence and [PendingCommit#wake()];
    ///         `false` if the commit failed instead, which woke its thread
    private boolean sequence(PendingCommit pending) {
        if (closed) {
            pending.fail(new IllegalStateException("The store is closed"));
            return false;
        }
        var stopped = broken;
        if (stopped != null) {
            pending.fail(stopped);
            return false;
        }
        try {
            checkHealthy();
        } catch (RuntimeException failure) {
            pending.fail(failure);
            return false;
        }
        var sequence = sequenced.next;
        var chains = pending.chains;
        var predecessors = new PendingCommit[chains.length];
        for (var i = 0; i < chains.length; i++) {
            var chain = chains[i];
            if (chain == null) {
                continue;
            }
            while (true) {
                var previous = chain.claim(pending, sequence);
                if (previous != Chain.DROPPED_WRITER) {
                    predecessors[i] = previous instanceof PendingCommit commit && commit != pending ? commit : null;
                    break;
                }
                // Dropped since the commit opened it: the key gets a chain of its own
                chain = pending.states[i].open(pending.writes.key(i));
                chains[i] = chain;
            }
        }
        pending.predecessors = predecessors;
        pending.sequence = sequence;
        sequenced.next = sequence + 1;
        sequenced.last.follow(pending);
        sequenced.last = pending;
        pending.advanceLazily(PendingCommit.SEQUENCED);
        return true;
    }

    /// Applies the writes of `pending` to their chains, each once the commit before it on the same key has.
    ///
    /// A write whose tree cannot be read takes back the commit's writes: it publishes empty, so the commits after it
    /// go on, and its committing thread throws the failure.
    private void apply(PendingCommit pending) {
        var writes = pending.writes;
        var applier = new Applier(pending.sequence, log != null, pending.writes.size());
        try {
            for (var i = 0; i < writes.size(); i++) {
                var predecessor = pending.predecessors[i];
                if (predecessor != null) {
                    predecessor.awaitApplied();
                }
                var inTree = pending.hints == null ? TreeSearch.UNKNOWN : pending.hints.of(i, tree);
                if (writes.kind(i) == ArrayWriteBatch.PUT) {
                    applier.put(pending.states[i], pending.chains[i], writes.key(i), writes.value(i), writes.view(), writes.valueOffset(i),
                            pending.decoded == null ? null : pending.decoded[i], inTree, pending.units == null ? null : pending.units[i]);
                } else {
                    applier.delete(pending.states[i], pending.chains[i], writes.key(i), inTree);
                }
            }
        } catch (Throwable failure) {
            applier.rollback();
            pending.failure = failure;
        }
        pending.applier = applier;
        // Read by the publisher on the commit's own line, so it reads the applier only when there is work in it
        pending.light = pending.failure == null && applier.light();
        pending.advanceApplied();
    }

    /// Publishes the applied commits after the last published one, in order, unless another thread holds the role.
    private void publishApplied() {
        while (true) {
            var next = published.last.next;
            if (next == null || next.state != PendingCommit.APPLIED || publishingWanted.held() || !publishing.tryAcquire()) {
                return;
            }
            PendingCommit first = null;
            try {
                first = publishInOrder();
            } finally {
                publishing.releaseLazily();
                // One fence after every move of the pass and the release: each committing thread that parks is seen
                // parking, and the next commit is read again after the role went
                VarHandle.fullFence();
                wakePublished(first);
            }
        }
    }

    /// Wakes the committing threads parked for the commits of a pass of the publisher, after a full fence.
    private static void wakePublished(PendingCommit first) {
        for (var commit = first; commit != null; ) {
            var next = commit.publishedNext;
            commit.publishedNext = null;
            commit.wake();
            commit = next;
        }
    }

    /// Publishes, holding the publishing role, every applied commit after the last published one, up to the first
    /// that has not applied, then maintains the store for their writes when [#maintenanceDue()].
    ///
    /// Once the log fails, no commit publishes: each fails with the log's failure, its versions above the latest
    /// sequence, where no read sees them. Each commit moves to [PendingCommit#DONE] right after the latest sequence
    /// covers it, with release stores; the caller wakes the parked ones after a full fence.
    ///
    /// @return the first of the commits it published, linked through [PendingCommit#publishedNext], or `null`
    private PendingCommit publishInOrder() {
        // The pass ends at the last applied commit found now: a commit that applies later waits for the next pass,
        // since its frame is not in this pass's batch
        PendingCommit end = null;
        var commit = published.last.next;
        while (commit != null && commit.state == PendingCommit.APPLIED) {
            end = commit;
            commit = commit.next;
        }
        if (end == null) {
            return null;
        }
        var wrote = false;
        PendingCommit first = null;
        PendingCommit last = null;
        // The frames of the pass go to the log in one batch, before the latest sequence covers any of its commits
        var logged = log != null && broken == null && queueFrames(end);
        PendingCommit next;
        do {
            next = published.last.next;
            if (broken == null) {
                if (next.failure != null) {
                    LATEST.setRelease(published, next.sequence);
                } else {
                    // A light commit is published from its own line: the applier is not read
                    if (next.light) {
                        LATEST.setRelease(published, next.sequence);
                    } else {
                        next.applier.publishQueued(logged);
                    }
                    wrote = true;
                }
            }
            if (broken != null && next.failure == null) {
                next.failure = broken;
            }
            var previous = published.last;
            LAST_PUBLISHED.setRelease(published, next);
            previous.unlink();
            next.advanceLazily(PendingCommit.DONE);
            if (last == null) {
                first = next;
            } else {
                last.publishedNext = next;
            }
            last = next;
        } while (next != end);
        if (wrote && maintenanceDue()) {
            // The committing threads prune the chains their commits wrote; maintain fences before it scans
            maintain(List.of());
        }
        return first;
    }

    /// Queues in the log, in one batch, the frames of the commits after the last published one, up to `end`, all
    /// applied: the commits [#publishInOrder()] then publishes.
    ///
    /// A commit whose writes were taken back gets an empty frame, which keeps the sequences of the log consecutive. If
    /// the log has stopped, no frame is queued, the commits with writes take them back, and the store stops
    /// publishing.
    ///
    /// @return whether the frames are queued
    private boolean queueFrames(PendingCommit end) {
        var batch = passFrames;
        var commit = published.last;
        do {
            commit = commit.next;
            if (commit.failure != null) {
                batch.add(FrameBody.COMMIT, commit.sequence, new FrameBody(), 0);
            } else {
                commit.applier.queueFrame(batch);
            }
        } while (commit != end);
        try {
            log.appendAll(batch);
            return true;
        } catch (RuntimeException failure) {
            broken = failure;
            // Newest first, so each rollback finds its own versions at the head of their chains
            var applied = new ArrayList<PendingCommit>();
            commit = published.last;
            do {
                commit = commit.next;
                applied.add(commit);
            } while (commit != end);
            for (var i = applied.size() - 1; i >= 0; i--) {
                if (applied.get(i).failure == null) {
                    applied.get(i).applier.rollback();
                }
            }
            return false;
        }
    }

    /// {@return whether the publisher maintains the store after this pass}: every [#MAINTAIN_EVERY] passes, or at
    /// once while a range removal waits to be applied, the sweep has work, or older trees wait to be released.
    private boolean maintenanceDue() {
        // A checkpoint published since the last one leaves older trees, whose pages the next checkpoints reuse once
        // no reader needs them
        var trees = tree;
        var due = ++passesSinceMaintenance >= MAINTAIN_EVERY || sweepPending || sweepWanted.held()
                || (trees != null && trees.older != null);
        if (!due) {
            for (var i = 0; i < sweepOrder.size() && !due; i++) {
                due = sweepOrder.get(i).removals.length != 0;
            }
        }
        if (due) {
            passesSinceMaintenance = 0;
        }
        return due;
    }

    /// Starts an operation that runs alone: no commit is given a sequence until [#exitExclusive()], and every commit
    /// given one has published.
    private void enterExclusive() {
        lockRequests();
        sequencingWanted.raise();
        acquire(sequencing);
        publishingWanted.raise();
        acquire(publishing);
        // The commits leave the publishing role to this thread, which publishes them as they apply
        for (var spin = 0; published.last != sequenced.last; spin++) {
            var first = publishInOrder();
            VarHandle.fullFence();
            wakePublished(first);
            if (published.last != sequenced.last) {
                pause(spin);
            }
        }
        exclusiveOwner = Thread.currentThread();
    }

    /// Ends an operation [#enterExclusive()] started, and gives sequences to the commits that arrived meanwhile.
    private void exitExclusive() {
        exclusiveOwner = null;
        publishingWanted.release();
        publishing.release();
        sequencingWanted.release();
        sequencing.release();
        requests.unlock();
        // Volatile stores before: a commit that arrived and found the roles wanted relies on this thread to see it
        drainArrivals();
    }

    /// Takes the publishing role, for work on the published state that applies may run beside: the cut of a
    /// checkpoint, the publication of its trees and the pruning after it.
    private void enterPublishing() {
        // A checkpoint goes before the transactions that keep taking the lock one after another
        checkpointWaiting.raise();
        try {
            requests.lock();
        } finally {
            checkpointWaiting.release();
        }
        publishingWanted.raise();
        acquire(publishing);
    }

    /// Lets the publishing role go, and publishes what applied meanwhile.
    private void exitPublishing() {
        publishingWanted.release();
        publishing.release();
        requests.unlock();
        publishApplied();
    }

    private static void acquire(Padded.Role role) {
        for (var spin = 0; !role.tryAcquire(); spin++) {
            pause(spin);
        }
    }

    /// Takes the requests lock for an exclusive operation, spinning for it first: the operation before usually lets it
    /// go within microseconds, and a parked waiter would cost every release a wake-up.
    private void lockRequests() {
        var virtual = Thread.currentThread().isVirtual();
        var spins = virtual ? 32 : 1_024;
        var yields = spins + (virtual ? 8 : 1_024);
        for (var spin = 0; spin < yields; spin++) {
            if (!checkpointWaiting.held() && requests.tryLock()) {
                return;
            }
            if (spin < spins) {
                Thread.onSpinWait();
            } else {
                Thread.yield();
            }
        }
        requests.lock();
    }

    /// Waits a while for a role whose holder lets it go soon: the commits leave a wanted role to the waiting thread.
    private static void pause(int spin) {
        if (spin < 256) {
            Thread.onSpinWait();
        } else if (spin < 1_024) {
            Thread.yield();
        } else {
            LockSupport.parkNanos(100_000);
        }
    }

    /// {@return `failure`, to throw, after throwing it if it is an error}
    private static RuntimeException rethrow(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        return (RuntimeException) failure;
    }

    /// Writes to the file of large values, before the commit is given a sequence, each value `writes` puts that is
    /// at least its key space's threshold.
    ///
    /// The commit's frame then records where each unit is instead of the value, which no checkpoint copies again: a
    /// large value reaches the device once, where the log and a checkpoint would write it twice.
    ///
    /// @return the unit of each write, `null` for one without; or `null` if no write has one
    private PageRef[] writeUnits(ArrayWriteBatch writes) {
        if (blobs == null) {
            return null;
        }
        PageRef[] units = null;
        try {
            for (var i = 0; i < writes.size(); i++) {
                if (writes.kind(i) != ArrayWriteBatch.PUT) {
                    continue;
                }
                var value = writes.value(i);
                if (value.byteSize() < states.get(writes.keyspace(i).name()).largeValues) {
                    continue;
                }
                if (units == null) {
                    units = new PageRef[writes.size()];
                }
                units[i] = blobs.write(value);
            }
            return units;
        } catch (RuntimeException | Error throwable) {
            if (units != null) {
                for (var unit : units) {
                    if (unit != null) {
                        blobs.release(unit);
                    }
                }
            }
            throw throwable;
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
        if (checkpointer != null) {
            checkpointer.throttle();
        }
        Commit commit;
        enterExclusive();
        try {
            ensureOpen();
            checkHealthy();
            var applier = new Applier(sequenced.next, log != null);
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
            sequenced.next++;
            commit = ticket(sequence);
        } finally {
            exitExclusive();
        }
        afterCommit();
        return commit;
    }

    /// Finishes a commit's maintenance after it published, and starts a checkpoint if one is due.
    ///
    /// A commit takes out of the ordered maps as many dropped chains as a commit drops, not those of a checkpoint,
    /// which the checkpoint takes out itself: a commit never pays for a whole checkpoint's.
    private void afterCommit() {
        for (var state : sweepOrder) {
            state.removeDropped(COMMIT_REMOVALS);
        }
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
        if (exclusiveOwner == Thread.currentThread()) {
            throw new IllegalStateException("A transaction cannot commit or start another transaction");
        }
    }

    @Override
    public void awaitDurable() throws InterruptedException {
        ensureOpen();
        if (log != null) {
            log.awaitDurable(latest());
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
            // It forces the files on this thread: an interrupt would close their channels under the store's own threads
            var interrupted = Thread.interrupted();
            try {
                checkpointer.run(false);
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
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

    /// {@return the file of large values, or `null` for a store in memory; for tests}
    BlobFile blobsForTests() {
        return blobs;
    }

    /// Drops every unit the page cache holds, so the next reads go to the file. For tests of reads that miss it.
    void clearPageCacheForTests() {
        if (pages != null) {
            pages.cache().clearForTests();
        }
    }

    private void close(boolean checkpoint) {
        enterExclusive();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            exitExclusive();
        }
        // The final checkpoint forces the files on this thread: an interrupt would close their channels under it
        var interrupted = Thread.interrupted();
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
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// The writes a checkpoint takes: everything up to its sequence, cut from the store holding the publishing role.
    ///
    /// @param sequence the sequence of the last commit the checkpoint holds
    /// @param logStart the first log segment a later commit may be in
    /// @param logBytes the bytes the log had queued at the cut
    /// @param base the trees the checkpoint updates, or `null`
    /// @param maxId the highest key space id
    /// @param keyspaces what changed in each key space
    /// @param superseded the units of the file of large values whose versions commits at or below the cut replaced
    ///                   before any checkpoint took them: no tree points to them, and no recovery from this checkpoint
    ///                   on replays their commits
    record Cut(long sequence, long logStart, long logBytes, TreeVersion base, int maxId, List<KeyspaceCut> keyspaces,
               List<PageRef> superseded) {
    }

    /// What changed in one key space since the last checkpoint.
    ///
    /// @param state the key space
    /// @param dirty the keys written
    /// @param removals the range removals as of the cut
    /// @param count the entries as of the cut, leaving out the blind writes ([BlindWrites])
    /// @param number the number of the cut in the key space, which [KeyspaceState#markDirty] gave the chains of `dirty`
    record KeyspaceCut(KeyspaceState state, List<Chain> dirty, RangeRemoval[] removals, long count, long number) {
    }

    /// Cuts the writes of a checkpoint and registers a read at its sequence, which keeps the versions it needs until
    /// [ReaderTable#unregister(long)].
    ///
    /// @param logStart the first log segment a commit after the cut may be in
    /// @return the cut, or `null` if no commit was made since the last checkpoint
    Cut cutCheckpoint(LongSupplier logStart) {
        enterPublishing();
        try {
            var base = tree;
            var sequence = latest();
            if (sequence == (base == null ? 0 : base.sequence)) {
                return null;
            }
            var registered = readers.register();
            if (registered != sequence) {
                throw new IllegalStateException("The latest sequence moved while the checkpoint held the publishing role");
            }
            var cuts = new ArrayList<KeyspaceCut>(states.size());
            for (var state : states.values()) {
                var number = state.dirtyNumber();
                cuts.add(new KeyspaceCut(state, state.cutDirty(), state.removals, state.known(sequence), number));
            }
            var bound = Reads.boundOf(sequence);
            var superseded = new ArrayList<PageRef>();
            for (var unit : inFlight) {
                if (unit.version().stamp <= bound && unit.version().replacedAt <= sequence) {
                    superseded.add(unit.version().unit);
                }
            }
            return new Cut(sequence, logStart.getAsLong(), log.appendedBytes(), base, maxId, cuts, superseded);
        } finally {
            exitPublishing();
        }
    }

    /// Gives a checkpoint that failed its keys back, so the next one writes them.
    void restoreCut(Cut cut) {
        enterPublishing();
        try {
            for (var keyspace : cut.keyspaces()) {
                for (var chain : keyspace.dirty()) {
                    keyspace.state().markDirty(chain);
                }
            }
        } finally {
            exitPublishing();
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
    ///
    /// The units of the file of large values its trees took leave the units in flight; those the commits it holds
    /// replaced before a checkpoint took them are free at once, since only memory reads them; those its trees no longer
    /// point to wait for the reads of the trees before it.
    ///
    /// @param freedExternal the units of the file of large values the trees before pointed to and these do not
    /// @param adopted the units of the file of large values these trees took
    void publish(Cut cut, long number, TreeRoot[] roots, PageRef catalog, PageRef freeList, PageRef blobFreeList,
                 List<PageRef> freed, List<PageRef> freedExternal, Set<PageRef> adopted) {
        enterPublishing();
        try {
            tree = new TreeVersion(number, cut.sequence(), latest(), roots, catalog, freeList, blobFreeList, tree);
            space.pend(latest(), freed);
            if (blobs != null) {
                blobs.pend(latest(), freedExternal);
                for (var unit : cut.superseded()) {
                    blobs.release(unit);
                }
                for (var unit : adopted) {
                    blobs.adopt(unit);
                }
                var bound = Reads.boundOf(cut.sequence());
                inFlight.removeIf(unit -> unit.version().stamp <= bound
                        && (adopted.contains(unit.version().unit) || unit.version().replacedAt <= cut.sequence()));
            }
        } finally {
            exitPublishing();
        }
    }

    /// Drops from memory the versions of the keys a published checkpoint wrote that every reader's tree holds, a few
    /// keys per hold of the publishing role.
    ///
    /// A resident key keeps its newest version, but loses those its writes kept for the checkpoint, which read as of
    /// its cut: left to the sweep, which visits a few chains a commit, a resident key written in a burst kept one older
    /// version until enough commits came after.
    void pruneCut(Cut cut) {
        for (var keyspace : cut.keyspaces()) {
            var state = keyspace.state();
            var chains = keyspace.dirty().iterator();
            while (chains.hasNext()) {
                enterPublishing();
                try {
                    var scan = readers.scan();
                    var bounds = bounds(scan.active());
                    var floor = floor(scan.oldest());
                    for (var i = 0; i < PRUNE_CHUNK && chains.hasNext(); i++) {
                        var chain = chains.next();
                        // A chain dropped since the cut may have been followed by another for the same key
                        var current = chain.dropped ? state.chain(chain.key) : chain;
                        if (current != null) {
                            if (prune(state, current, current.head, bounds, Reads.boundOf(floor))) {
                                requestSweep();
                            }
                        }
                    }
                } finally {
                    exitPublishing();
                }
            }
            state.removeDropped();
            state.pruneListed(keyspace.number());
        }
    }

    /// Prepares the writes of `writes` before the commit is given a sequence: opens the chain of every key the batch
    /// puts, and says how each write learns whether its key existed.
    ///
    /// A commit of the pipeline (`chains` given) writes blind: a write of a paged key space whose key memory holds no
    /// version does not look the key up, and the checkpoint that takes it settles what it does to the count
    /// ([BlindWrites]). With a tree larger than the page cache the lookup would read the file, and every later commit
    /// on the key would wait for the read.
    ///
    /// An operation that runs alone counts exactly as it applies, so here its lookups run before its turn, those that
    /// reach the file on up to [#PREFETCH_THREADS] threads at once, and what they found serves the writes as long as
    /// the trees are still the ones they read. The lookups do not register as a read: a lease per commit would hold
    /// versions back and change the readers the sweep waits for at every commit. Without one, a checkpoint may reuse a
    /// page of the trees they read once newer trees replace them; the page then fails authentication, since its nonce
    /// is not the one its parent records, and the lookup leaves its write to the writer. A lookup that succeeds read
    /// the trees as they were, and its answer is used only while they are still the latest.
    ///
    /// Opening the chains here grows the ordered map of the key space, which a new key costs a walk down, outside the
    /// lock; commits that run at once insert their keys at once.
    ///
    /// @return how each write learns whether its key existed, or `null` if the store has no trees
    ///
    /// @param chains receives the chain of each put and removal, opened for the pipeline, or `null`
    /// @param keyspaceStates receives the key space of each write when `chains` does, or `null`
    private Hints prefetch(ArrayWriteBatch writes, Chain[] chains, KeyspaceState[] keyspaceStates) {
        var trees = pages == null ? null : tree;
        try {
            var point = trees == null || chains != null ? null : new ReadPoint(Reads.boundOf(latest()), trees, pages);
            var found = trees == null ? null : new byte[writes.size()];
            int[] missing = null;
            var misses = 0;
            for (var i = 0; i < writes.size(); i++) {
                var kind = writes.kind(i);
                if (kind != ArrayWriteBatch.PUT && kind != ArrayWriteBatch.DELETE) {
                    continue;
                }
                var state = states.get(writes.keyspace(i).name());
                var key = writes.key(i);
                var chain = chains != null || kind == ArrayWriteBatch.PUT ? state.open(key) : state.chain(key);
                if (chains != null) {
                    chains[i] = chain;
                    keyspaceStates[i] = state;
                }
                if (found == null) {
                    continue;
                }
                if (chains != null) {
                    if (!state.resident) {
                        found[i] = TreeSearch.BLIND;
                    }
                    continue;
                }
                if (chain != null && chain.head != null) {
                    continue;
                }
                var root = point.root(state);
                if (root == null) {
                    continue;
                }
                found[i] = TreeSearch.lookupCached(pages, root, key);
                if (found[i] == TreeSearch.UNKNOWN) {
                    if (missing == null) {
                        missing = new int[16];
                    } else if (misses == missing.length) {
                        missing = Arrays.copyOf(missing, misses * 2);
                    }
                    missing[misses++] = i;
                }
            }
            if (misses == 1) {
                lookUp(writes, point, found, missing, 1, 0, 1);
            } else if (misses > 1) {
                var threads = Math.min(PREFETCH_THREADS, misses);
                var started = new Thread[threads];
                var indexes = missing;
                var count = misses;
                for (var thread = 0; thread < threads; thread++) {
                    var first = thread;
                    started[thread] = Thread.ofVirtual().start(() -> lookUp(writes, point, found, indexes, count, first, threads));
                }
                for (var thread : started) {
                    thread.join();
                }
            }
            return found == null ? null : new Hints(trees, found);
        } catch (InterruptedException _) {
            // The commit goes on without the hints, and its lookups read the trees as it applies
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /// Looks up the keys of every `step`-th write of `missing` from `first` in the trees of `point`, reading the file,
    /// and records in `found` what each lookup found.
    private void lookUp(ArrayWriteBatch writes, ReadPoint point, byte[] found, int[] missing, int count, int first, int step) {
        for (var i = first; i < count; i += step) {
            var write = missing[i];
            try {
                var root = point.root(states.get(writes.keyspace(write).name()));
                found[write] = TreeSearch.find(pages, root, writes.key(write)) != null ? TreeSearch.FOUND : TreeSearch.ABSENT;
            } catch (RuntimeException _) {
                // Left unknown: the lookup as the write applies reads the page again and reports the failure
            }
        }
    }

    /// Whether the trees held the keys of a batch's writes, as a commit found before it was given a sequence.
    ///
    /// @param tree the trees the lookups read
    /// @param found per write, [TreeSearch#FOUND], [TreeSearch#ABSENT], or [TreeSearch#UNKNOWN] for one not looked up
    record Hints(TreeVersion tree, byte[] found) {
        /// {@return what the lookups found for write `write`, or [TreeSearch#UNKNOWN] if the writer reads trees
        /// other than theirs}
        ///
        /// @param current the trees the writer reads
        byte of(int write, TreeVersion current) {
            var hint = found[write];
            return hint == TreeSearch.BLIND || tree == current ? hint : TreeSearch.UNKNOWN;
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
        if (blobs != null) {
            blobs.releasePassed(oldestReader);
        }
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
    /// @return whether the key keeps a version a later prune may drop, once the readers that need it are gone
    /// Prunes `chain` from `head`, its newest version or one a later version of the same commit replaced, and drops
    /// the chain once nothing in it is needed.
    ///
    /// @return whether the chain keeps versions a later prune may drop
    private boolean prune(KeyspaceState state, Chain chain, Version head, long[] bounds, long cutBound) {
        if (head == null) {
            // Left empty, by a commit that rolled back, that removed a key it did not find, or that opened it and has
            // not applied it yet, which keeps it
            state.drop(chain, null, latestSequence);
            return false;
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
                    kept.link(version);
                }
                kept = version;
            }
        }
        if (kept == null) {
            return false;
        }
        kept.link(null);
        if (pages == null || state.resident) {
            // Memory holds every entry: a key whose only version is a removal has none
            if (kept == head && head.removes()) {
                state.drop(chain, head, latestSequence);
                return false;
            }
            return head.older != null;
        }
        // A removal stays until the trees hold it, since it hides what they hold; then, like any version, it leaves
        Version previous = null;
        for (var cut = head; cut != null; cut = cut.older) {
            if (cut.stamp <= cutBound) {
                if (previous == null) {
                    state.drop(chain, head, latestSequence);
                    return false;
                }
                previous.link(null);
                break;
            }
            previous = cut;
        }
        // A version the latest tree holds leaves once no reader reads an older tree
        var current = tree;
        return head.older != null || (current != null && head.stamp <= Reads.boundOf(current.sequence));
    }

    /// Applies to the entries in memory the range removals every reader has passed, and drops the records of those
    /// no reader's tree, nor the next checkpoint, needs.
    ///
    /// @param dropBound the stamps at or below which a record goes
    private static void applyRemovals(KeyspaceState state, long bound, long dropBound, LongSupplier latest) {
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
                var complete = true;
                for (var chain : state.slice(removal.from(), removal.to()).values()) {
                    var head = chain.head;
                    Version newest = null;
                    var version = head;
                    while (version != null && version.stamp > removal.stamp()) {
                        newest = version;
                        version = version.older;
                    }
                    if (newest == null) {
                        // A chain a commit has a write of, not published yet, stays: the record must hide its
                        // versions until a later pass drops them
                        if (!state.drop(chain, head, latest) && head != null) {
                            complete = false;
                        }
                    } else {
                        newest.link(null);
                    }
                }
                if (!complete) {
                    kept.add(removal);
                    continue;
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

/// Prunes the chains `touched` wrote, applies the range removals every reader has passed, drops the counts no
/// reader needs and sweeps a few chains; holding the publishing role.
    private void maintain(List<Touched> touched) {
        // The latest sequence was stored with a release store: the scan must not miss a read that saw the one before
        VarHandle.fullFence();
        var scan = readers.scan();
        var active = scan.active();
        var bounds = bounds(active);
        var oldestReader = scan.oldest();
        var floor = floor(oldestReader);
        releaseTrees(oldestReader);
        var cutBound = Reads.boundOf(floor);
        // Without a data file a record goes once applied; with one, once every reader's tree holds it
        var dropBound = pages == null ? Long.MAX_VALUE : cutBound;
        var oldest = active[active.length - 1];
        for (var write : touched) {
            if (prune(write.state(), write.chain(), write.version(), bounds, cutBound)) {
                requestSweep();
            }
        }
        var order = sweepOrder;
        for (var i = 0; i < order.size(); i++) {
            var state = order.get(i);
            applyRemovals(state, bounds[bounds.length - 1], dropBound, latestSequence);
            var countFloor = state.counts.floorKey(oldest);
            // Most commits find nothing to drop; a view of the map would cost more than the check
            if (countFloor != null && state.counts.firstKey() < countFloor) {
                state.counts.headMap(countFloor, false).clear();
            }
            state.blind.fold(oldest);
        }
        sweep(bounds, cutBound, SWEEP_BUDGET);
    }

    /// Asks the sweep for a pass over the store, for a chain a prune left that a later one may shorten.
    private void requestSweep() {
        // Read first: written only when it changes, so commits that keep finding such chains share its line
        if (!sweepWanted.held()) {
            sweepWanted.raise();
        }
    }

    /// Prunes the chains a published commit wrote, on its committing thread, beside the other commits and the
    /// publisher.
    ///
    /// Prunes of one chain may run at once: each keeps, below the newest version it finds at or below the latest
    /// sequence it scanned, every version a reader of its scan or a later one needs, and only bypasses or cuts the
    /// versions below; a chain is dropped only through [KeyspaceState#drop], whose claim check orders it after the
    /// commits given its writes.
    private void pruneWritten(List<Touched> touched) {
        if (touched.isEmpty()) {
            return;
        }
        long[] bounds;
        long cutBound;
        // Read before the reader table, as a scan reads it
        var current = latest();
        if (readers.idle()) {
            // The usual case, without the scan's arrays: every read is at the latest sequence or later
            bounds = new long[]{Reads.boundOf(current)};
            cutBound = Reads.boundOf(floor(Long.MAX_VALUE));
        } else {
            var scan = readers.scanConcurrently();
            bounds = bounds(scan.active());
            cutBound = Reads.boundOf(floor(scan.oldest()));
        }
        for (var i = 0; i < touched.size(); i++) {
            var write = touched.get(i);
            if (prune(write.state(), write.chain(), write.version(), bounds, cutBound)) {
                requestSweep();
            }
        }
    }

    /// Prunes up to `budget` keys, resuming where the last sweep stopped, while some prune left a chain a later one may
    /// shorten; a whole pass over the store that finds none ends the sweeping.
    private void sweep(long[] bounds, long cutBound, int budget) {
        if (sweepOrder.isEmpty()) {
            return;
        }
        if (!sweepPending) {
            if (!sweepWanted.held()) {
                return;
            }
            // Lowered before the pass: a prune that raises it after has its chain visited by this pass or the next
            sweepWanted.release();
            sweepPending = true;
            sweptBounds = null;
        }
        // A chain a pass left is left again under the same readers and floor, such as while a checkpoint's own read
        // holds the floor back
        if (sweptBounds != null && sameReaders(bounds, cutBound, sweptBounds, sweptCutBound)) {
            return;
        }
        var visitedKeyspaces = 0;
        while (budget > 0 && visitedKeyspaces <= sweepOrder.size()) {
            if (sweepIndex == 0 && sweepAfter == null) {
                sweepFound = false;
                passBounds = bounds;
                passCutBound = cutBound;
            }
            var state = sweepOrder.get(sweepIndex % sweepOrder.size());
            // An apply may have put a newer version above the entry's value: the prune leaves it, and drops nothing
            var entries = (sweepAfter == null ? state.entries : state.entries.tailMap(sweepAfter, false)).entrySet().iterator();
            while (budget > 0 && entries.hasNext()) {
                var entry = entries.next();
                var chain = entry.getValue();
                sweepFound |= prune(state, chain, chain.head, bounds, cutBound);
                sweepAfter = entry.getKey();
                budget--;
            }
            if (!entries.hasNext()) {
                sweepIndex = (sweepIndex + 1) % sweepOrder.size();
                sweepAfter = null;
                visitedKeyspaces++;
                if (sweepIndex == 0) {
                    if (!sweepFound) {
                        sweepPending = false;
                        sweptBounds = null;
                        return;
                    }
                    sweptBounds = passBounds;
                    sweptCutBound = passCutBound;
                    if (sameReaders(bounds, cutBound, sweptBounds, sweptCutBound)) {
                        return;
                    }
                }
            }
        }
    }

    /// {@return whether a prune under `bounds` and `cutBound` keeps what one under `otherBounds` and `otherCutBound`
    /// keeps, for a chain no commit has touched since}
    ///
    /// The first bound is the latest commit's: a commit prunes the keys it writes, so it changes nothing for the
    /// others.
    private static boolean sameReaders(long[] bounds, long cutBound, long[] otherBounds, long otherCutBound) {
        return cutBound == otherCutBound && Arrays.equals(bounds, 1, bounds.length, otherBounds, 1, otherBounds.length);
    }

    /// The writes of one commit or transaction, applied as they come and published together.
    final class Applier {
        private final long sequence;
        private int op;
        // The key spaces whose counts the commit changes, and by how much, in the first deltaCount slots
        private KeyspaceState[] deltaStates;
        private long[] deltas;
        private int deltaCount;
        private final List<Touched> touched;
        // The removals of each key space before this commit's first range removal there, or null before any
        private IdentityHashMap<KeyspaceState, RangeRemoval[]> removalsBefore;
        // The writes as the log records them, or null when nothing is journaled
        private final FrameBody frame;
        // The bytes of the units of the file of large values this commit's values are in
        private long unitBytes;
        // Whether the commit's frame is queued in the log, which then names its units
        private boolean logged;
        // The versions this commit added whose value a unit of the file of large values holds
        private int externals;
        // The versions in flight this commit replaced, which a rollback marks current again, or null before any
        private List<Version.External> replaced;

        Applier(long sequence, boolean journaled) {
            this(sequence, journaled, 4);
        }

        /// Creates the applier of a commit of about `writes` writes.
        Applier(long sequence, boolean journaled, int writes) {
            this.sequence = sequence;
            // Most commits write one or two key spaces
            this.deltaStates = new KeyspaceState[2];
            this.deltas = new long[2];
            this.touched = new ArrayList<>(writes);
            this.frame = journaled ? new FrameBody() : null;

        }

        /// {@return whether the commit's frame is queued in the log}
        boolean logged() {
            return logged;
        }

        /// {@return the change this commit makes to the count of `state`}
        private long delta(KeyspaceState state) {
            for (var i = 0; i < deltaCount; i++) {
                if (deltaStates[i] == state) {
                    return deltas[i];
                }
            }
            return 0;
        }

        /// Adds `delta` to the change this commit makes to the count of `state`.
        private void addDelta(KeyspaceState state, long delta) {
            for (var i = 0; i < deltaCount; i++) {
                if (deltaStates[i] == state) {
                    deltas[i] += delta;
                    return;
                }
            }
            if (deltaCount == deltaStates.length) {
                deltaStates = Arrays.copyOf(deltaStates, deltaCount * 2);
                deltas = Arrays.copyOf(deltas, deltaCount * 2);
            }
            deltaStates[deltaCount] = state;
            deltas[deltaCount++] = delta;
        }

        /// Records on `chain` that this commit writes its key blind: the tree settles what the write does to the count.
        private void markBlind(Chain chain) {
            chain.blindSequence = sequence;
            chain.blindState = TreeSearch.UNKNOWN;
        }

        /// Records that this commit replaces `version`, if a unit no checkpoint has taken holds its value.
        private void replace(Version version) {
            if (version instanceof Version.External external && external.replacedAt == Long.MAX_VALUE) {
                external.replacedAt = sequence;
                if (replaced == null) {
                    replaced = new ArrayList<>(2);
                }
                replaced.add(external);
            }
        }

        /// {@return the bound of reads that see the latest commit and this one's writes so far}
        long bound() {
            return Reads.boundOf(sequence);
        }

        /// {@return the number of entries of `state` once this commit applies}
        ///
        /// It looks up the keys of the blind writes whose effect is not known yet, in the latest trees: the commits
        /// before this one have published, as this one runs alone.
        long count(KeyspaceState state) {
            var point = writerPoint(Reads.stamp(sequence, op) - 1);
            var blind = state.blind.correction(sequence - 1, key -> Reads.inTreeOnly(state, key, point));
            return state.counts.lastEntry().getValue() + blind + delta(state);
        }

        private long nextStamp() {
            if (op > Reads.MAX_OPS) {
                throw new IllegalStateException("A commit holds at most " + Reads.MAX_OPS + " writes");
            }
            return Reads.stamp(sequence, op++);
        }

        void put(KeyspaceState state, MemorySegment key, MemorySegment value, Object decoded) {
            put(state, key, value, decoded, TreeSearch.UNKNOWN, null);
        }

        /// Applies a put of `key`.
        ///
        /// @param inTree whether the latest tree holds `key`, as [Hints] give it, or [TreeSearch#UNKNOWN]
        /// @param external the unit of the file of large values that holds `value` too, or `null`
        void put(KeyspaceState state, MemorySegment key, MemorySegment value, Object decoded, byte inTree, PageRef external) {
            // Usually opened already by the commit's prefetch
            put(state, claim(state, key), key, value, null, 0, decoded, inTree, external);
        }

        /// {@return the chain of `key`, claimed for a write of an operation that runs alone}
        ///
        /// The claim keeps a prune that runs beside it, of a commit published before, from dropping the chain.
        Chain claim(KeyspaceState state, MemorySegment key) {
            while (true) {
                var chain = state.open(key);
                if (chain.claimAlone(sequence)) {
                    return chain;
                }
            }
        }

        /// Applies a put of `key`, whose chain is `chain`, in the order and without the race the caller ensures.
        ///
        /// @param source the read-only segment `value` is part of, which the version keeps in its place, or `null` for a
        ///               value of its own
        /// @param offset where `value` starts in `source`
        void put(KeyspaceState state, Chain chain, MemorySegment key, MemorySegment value, MemorySegment source, int offset,
                 Object decoded, byte inTree, PageRef external) {
            var stamp = nextStamp();
            var head = chain.head;
            var existed = Reads.existence(state, head, key, writerPoint(stamp - 1), inTree);
            var length = Math.toIntExact(value.byteSize());
            if (state.resident && external == null && (source == null || source.byteSize() > 2L * length + RESIDENT_SLACK)) {
                // A resident version lives as long as its key does: on a value of its own, not on the array of the
                // batch or the log frame it came in, which a session written beside ten messages held 12 times over
                value = MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
                source = null;
            }
            var version = source == null ? Version.of(stamp, value, 0, length, decoded, head, external)
                    : Version.of(stamp, source, offset, length, decoded, head, external);
            var blind = existed == TreeSearch.UNKNOWN;
            if (blind) {
                markBlind(chain);
            }
            chain.attach(version);
            if (external != null) {
                externals++;
            }
            touched.add(new Touched(state, chain, version, blind));
            replace(head);
            if (existed == TreeSearch.ABSENT) {
                addDelta(state, 1);
            }
            if (frame != null) {
                if (external != null) {
                    frame.putExternal(state.id, key, value.byteSize(), external);
                    unitBytes += value.byteSize();
                } else {
                    frame.put(state.id, key, value);
                }
            }
        }

        void delete(KeyspaceState state, MemorySegment key) {
            delete(state, key, TreeSearch.UNKNOWN);
        }

        /// Applies a removal of `key`.
        ///
        /// @param inTree whether the latest tree holds `key`, as [Hints] give it, or [TreeSearch#UNKNOWN]
        void delete(KeyspaceState state, MemorySegment key, byte inTree) {
            var chain = state.chain(key);
            if (chain == null || !chain.ordered) {
                var head = chain == null ? null : chain.head;
                if (!Reads.exists(state, head, key, writerPoint(Reads.stamp(sequence, op) - 1), inTree)) {
                    nextStamp();
                    return;
                }
                chain = state.open(key);
            }
            if (!chain.claimAlone(sequence)) {
                chain = claim(state, key);
            }
            delete(state, chain, key, inTree);
        }

        /// Applies a removal of `key`, whose chain is `chain`, in the order and without the race the caller ensures.
        void delete(KeyspaceState state, Chain chain, MemorySegment key, byte inTree) {
            var stamp = nextStamp();
            var head = chain.head;
            var existed = Reads.existence(state, head, key, writerPoint(stamp - 1), inTree);
            if (existed == TreeSearch.ABSENT) {
                if (head == null) {
                    // Opened for this removal: the prune after the commit publishes drops it, if it stays empty
                    touched.add(new Touched(state, chain, null, false));
                }
                return;
            }
            var version = new Version(stamp, null, null, head);
            var blind = existed == TreeSearch.UNKNOWN;
            if (blind) {
                markBlind(chain);
            }
            chain.attach(version);
            touched.add(new Touched(state, chain, version, blind));
            replace(head);
            if (!blind) {
                addDelta(state, -1);
            }
            if (frame != null) {
                frame.delete(state.id, key);
            }
        }

        void deleteRange(KeyspaceState state, MemorySegment from, MemorySegment to) {
            var stamp = nextStamp();
            var removed = Reads.count(state, from, to, writerPoint(stamp - 1));
            var removals = state.removals;
            if (removalsBefore == null) {
                removalsBefore = new IdentityHashMap<>(2);
            }
            removalsBefore.putIfAbsent(state, removals);
            var grown = Arrays.copyOf(removals, removals.length + 1);
            var removal = new RangeRemoval(from, to, stamp);
            grown[removals.length] = removal;
            state.removals = grown;
            for (var unit : inFlight) {
                if (unit.state() == state && removal.covers(unit.chain().key)) {
                    replace(unit.version());
                }
            }
            for (var write : touched) {
                if (write.state() == state && removal.covers(write.chain().key)) {
                    replace(write.version());
                }
            }
            if (removed != 0) {
                addDelta(state, -removed);
            }
            if (frame != null) {
                frame.deleteRange(state.id, from, to);
            }
        }

        /// Makes the commit visible to every read that starts afterward, then maintains the store.
        ///
        /// @return the sequence of the commit
        long publish() {
            publishWrites();
            maintain(touched);
            return sequence;
        }

        /// Makes the commit visible to every read that starts afterward: queues its frame, then publishes its writes,
        /// its counts and its sequence; the caller maintains the store.
        ///
        /// Called in the order of the sequences, holding the publishing role.
        ///
        /// @throws com.github.auties00.ledger.LedgerIoException if the log has stopped; the writes are taken back
        void publishWrites() {
            publishOrdered();
            // A release store; the thread then fences before it reads the reader table, or wakes a commit
            LATEST.setRelease(published, sequence);
        }

        /// {@return whether publishing the commit is only moving the latest sequence: nothing to log, to count, to
        /// list for a checkpoint or as a unit in flight}
        boolean light() {
            return frame == null && externals == 0 && deltaCount == 0 && pages == null;
        }

        /// Does what [#publishWrites()] does but move the latest sequence, which the caller moves, after, for a whole
        /// pass of commits.
        void publishOrdered() {
            // Queued before it is visible, so the log holds every commit a reader can see, in order
            if (frame != null) {
                try {
                    log.append(FrameBody.COMMIT, sequence, frame, unitBytes);
                    logged = true;
                } catch (RuntimeException exception) {
                    rollback();
                    throw exception;
                }
            }
            publishCounted();
        }

        /// Adds the commit's frame, if it is journaled, to `batch`.
        void queueFrame(LogWriter.Batch batch) {
            if (frame != null) {
                batch.add(FrameBody.COMMIT, sequence, frame, unitBytes);
            }
        }

        /// Publishes the commit as [#publishWrites()] does, once [#queueFrame(LogWriter.Batch)] queued its frame in a
        /// batch.
        ///
        /// @param queued whether the batch is queued; `false` for a store with no log
        void publishQueued(boolean queued) {
            logged = queued && frame != null;
            publishCounted();
            LATEST.setRelease(published, sequence);
        }

        /// Publishes what the commit adds to the units in flight, the keys the next checkpoint writes and the counts.
        private void publishCounted() {
            if (externals != 0 || pages != null) {
                for (var write : touched) {
                    if (write.version() == null) {
                        continue;
                    }
                    if (write.version() instanceof Version.External external) {
                        inFlight.add(new InFlight(write.state(), write.chain(), external));
                    }
                    if (pages != null) {
                        write.state().markDirty(write.chain());
                    }
                    if (write.blind()) {
                        write.state().blind.add(write.chain(), sequence, write.version().removes());
                    }
                }
            }
            for (var i = 0; i < deltaCount; i++) {
                var state = deltaStates[i];
                state.counts.put(sequence, state.counts.lastEntry().getValue() + deltas[i]);
            }
        }

        /// Takes back every write this commit applied; no reader saw them, since the commit was never published.
        ///
        /// The keys it wrote stay marked for the next checkpoint, which writes their unchanged state.
        void rollback() {
            for (var write : touched) {
                var chain = write.chain();
                var head = chain.head;
                var version = head;
                while (version != null && version.stamp >>> Reads.OP_BITS == sequence) {
                    version = version.older;
                }
                if (version == null) {
                    // Emptied whether or not it can be dropped: once the commit publishes empty, its sequence is
                    // visible, and so would be a version left at the head
                    if (head != null) {
                        chain.attach(null);
                    }
                    write.state().dropRolledBack(chain, sequence);
                } else if (version != head) {
                    chain.head = version;
                }
            }
            if (removalsBefore != null) {
                for (var restored : removalsBefore.entrySet()) {
                    restored.getKey().removals = restored.getValue();
                }
            }
            for (var version : replaced == null ? List.<Version.External>of() : replaced) {
                version.replacedAt = Long.MAX_VALUE;
            }
        }

    }

    /// A version whose value a unit of the file of large values holds, which no published checkpoint has taken.
    ///
    /// @param chain the chain of its key
    private record InFlight(KeyspaceState state, Chain chain, Version.External version) {
    }

    /// A key a commit wrote.
    ///
    /// @param chain the key's chain
    /// @param version the version the write added
    /// @param blind whether the write is blind: the tree settles what it does to the count ([BlindWrites])
    private record Touched(KeyspaceState state, Chain chain, Version version, boolean blind) {
    }
}
