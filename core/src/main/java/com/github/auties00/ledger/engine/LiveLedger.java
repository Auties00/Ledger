package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Commit;
import com.github.auties00.ledger.Entry;
import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.Ledger;
import com.github.auties00.ledger.LedgerException;
import com.github.auties00.ledger.Snapshot;
import com.github.auties00.ledger.Transaction;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;

import java.lang.foreign.MemorySegment;
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
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;

/// A store: the engine of [Ledger.Builder#memory()], and of a store in a directory, which also journals every commit
/// to a [LogWriter] and checkpoints it into the trees of a [PageFile].
///
/// A batch commits through a pipeline that holds no lock ([PendingCommit]): the committing thread prepares it, the
/// thread holding the sequencing role gives it the next sequence in the order commits arrive, the committing thread
/// applies its writes once the commits before it on the same keys have applied theirs, and the thread holding the
/// publishing role publishes the applied commits in the order of their sequences by advancing [#latest]. Readers see
/// a commit whole or not at all. Transactions and batches with a range removal run alone instead: they hold both roles
/// once every commit given a sequence has published. A batch's range removal counts the entries it removes after the
/// commit, on a thread of its own, against a read registered at the commit before ([PendingRanges]).
///
/// After publishing, the publisher prunes the versions no reader can need any more, applies the range removals every
/// reader has passed, and sweeps a few keys of the whole store; prunes and drops of chains happen only while holding
/// the publishing role, so a chain is dropped only once its last writer has published.
///
/// A store in a directory reads each key space's tree under the writes in memory ([ReadPoint]). Once every reader's
/// tree holds a version, the writer drops it from memory, except in resident key spaces, which keep every entry; a
/// range removal stays as a record while a reader's tree may hold keys it removed. A resident key space gets the
/// entries of the checkpoint the store opened from through its [ResidentLoader], from its first use on a thread of the
/// loader's own, or while the store opens; a write of a key the loader has not reached waits for it.
public final class LiveLedger implements Ledger {
    /// The bytes by which the array a resident value came in may exceed it before the value takes an array of its own.
    private static final int RESIDENT_SLACK = 64;
    private static final int READER_SLOTS = 256;
    private static final int SWEEP_BUDGET = 64;
    /// The chains [#finishSweep()] prunes per hold of the publishing role: a commit that finds the role held waits for at
    /// most this many, a few tens of microseconds.
    private static final int SWEEP_CHUNK = 256;
    /// The ends of a pass [#finishSweep()] goes to: the pass it finds under way began under older readers, and only the
    /// next one is whole under those of now.
    private static final int SWEEP_PASSES = 2;
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
    // Where the sweep stands in the index of a key space it walks by index ([KeyspaceState#sweepsIndex()]), or null
    // before it starts there
    private ChainIndex.Cursor sweepCursor;
    // The log bytes, and the heap by the log's estimate, of the commits the open replayed, which the trees lack
    private long replayedBytes;
    private long replayedHeap;
    // Whether a prune left a chain that a later one may shorten, so the sweep has work; guarded by the publishing role
    private boolean sweepPending;
    // Whether the sweep's current pass over the store found such a chain
    private boolean sweepFound;
    // The passes the sweep has ended; guarded by the publishing role
    private long sweepEnds;
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
    // The smallest threshold of a key space of the store: a batch whose values are all smaller writes none to the
    // file of large values
    private long minLargeValues = Long.MAX_VALUE;
    // The versions whose units of the file of large values no published checkpoint has taken; guarded by the
    // publishing role
    private final ArrayList<InFlight> inFlight;
    // The resident key spaces whose loaders have not put every entry in memory yet: while there is none, a commit
    // looks for no key to wait for
    private final AtomicInteger unloaded;
    // Held shared by each count of a range removal that runs on a thread of its own, and exclusively by a close once its
    // last checkpoint is done, so no such count reads the files as they close
    private final ReentrantReadWriteLock rangeCountGate;
    // Starts the counts of range removals that run on threads of their own
    private volatile Executor rangeCounts;
    // The chunks the values of the fastest commits of a store with pages share
    private final ValueSlab valueSlab;

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
        this.unloaded = new AtomicInteger();
        this.rangeCountGate = new ReentrantReadWriteLock();
        this.rangeCounts = task -> Thread.ofVirtual().name("ledger-range-count").start(task);
        this.valueSlab = new ValueSlab();
    }

    /// Makes this store one in a directory, whose checkpoints are in `pages`; called before anything is recovered.
    void useDataFile(PageFile pages, FreeSpace space) {
        this.pages = pages;
        this.space = space;
        for (var state : states.values()) {
            state.lazy = true;
        }
    }

    /// Makes `blobs` the file of large values: a batch writes there each value of at least `threshold` bytes, or of
    /// the bytes `overrides` gives for its key space by name; called before anything is recovered.
    void useBlobs(BlobFile blobs, long threshold, Map<String, Long> overrides) {
        this.blobs = blobs;
        this.largeValues = threshold;
        var smallest = Long.MAX_VALUE;
        for (var state : states.values()) {
            state.largeValues = overrides.getOrDefault(state.keyspace.name(), threshold);
            smallest = Math.min(smallest, state.largeValues);
        }
        this.minLargeValues = smallest;
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
            minLargeValues = Math.min(minLargeValues, largeValues);
            state.lazy = true;
            states.put(name, state);
            sweepOrder = List.copyOf(states.values());
        }
        state.id = id;
        byId.put(id, state);
    }

    /// Starts from the checkpoint `superblock` names, whose catalog is `catalog`: binds its key spaces, publishes its
    /// trees and gives each resident key space with entries its [ResidentLoader].
    ///
    /// @param residentAtOpen whether the loaders put the entries in memory now, rather than from the first use of
    ///                       their key spaces
    /// @param failures receives the failures of the loaders that run beside the store
    void loadCheckpoint(Superblock superblock, Catalog.Contents catalog, boolean residentAtOpen,
                        Consumer<? super LedgerException> failures) {
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
            state.counts.reset(sequence, entry.root() == null ? 0 : entry.root().count());
            if (!state.resident || entry.root() == null) {
                continue;
            }
            var loader = new ResidentLoader(this, pages, state, stamp, failures);
            state.loading = loader;
            unloaded.incrementAndGet();
            if (residentAtOpen) {
                loader.loadNow();
            }
        }
    }

    /// Records that a loader put every entry of its key space in memory.
    void residentLoaded() {
        unloaded.decrementAndGet();
    }

    /// Makes the loaders of the resident key spaces call `hook` after they register their read, with 0, and after
    /// each run of leaves they install, with the runs installed so far; for tests that pause them there.
    void hookResidentLoadsForTests(IntConsumer hook) {
        for (var state : states.values()) {
            var loader = state.loading;
            if (loader != null) {
                loader.stepHookForTests = hook;
            }
        }
    }

    /// {@return whether memory holds every entry of the key space `keyspace`, for tests of its loader}
    boolean residentLoadedForTests(Keyspace<?, ?> keyspace) {
        return states.get(keyspace.name()).loading == null;
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
                loadForReplay(keyspace);
            }

            @Override
            public void delete(int keyspace, MemorySegment key) {
                loadForReplay(keyspace);
            }

            @Override
            public void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
                loadForReplay(keyspace);
            }

            @Override
            public void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
                loadForReplay(keyspace);
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
        var unitBytes = 0L;
        for (var unit : units) {
            unitBytes += unit.length;
        }
        replayedBytes += 13 + body.length + unitBytes;
        replayedHeap += LogWriter.heapOf(body.length, unitBytes, applier.touched.size(), false);
        return true;
    }

    /// Puts in memory every entry of the key space `id` if it is a resident one whose loader has not, before the log's
    /// replay writes it: the replay writes as an exclusive operation does, without waiting for keys.
    private void loadForReplay(int id) {
        if (unloaded.get() == 0) {
            return;
        }
        var state = byId.get(id);
        var loader = state == null ? null : state.loading;
        if (loader != null) {
            loader.loadNow();
        }
    }

    /// {@return the log bytes of the commits the open replayed, which the trees lack}
    long replayedBytes() {
        return replayedBytes;
    }

    /// {@return the heap the commits the open replayed hold until a checkpoint takes them, as [LogWriter#appendedHeap()]
    /// estimates it}
    long replayedHeap() {
        return replayedHeap;
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
        // The first use of a resident key space starts its loader
        var loader = state.loading;
        if (loader != null) {
            loader.start();
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
        writes.prepare(pages == null, valueSlab, states);
        var units = writeUnits(writes);
        // Needed only to release the units of large values no log frame names
        var outcome = units == null ? null : new Applier[1];
        Commit commit;
        try {
            if (unloaded.get() != 0) {
                awaitLoaded(writes);
            }
            commit = writes.removesRanges() ? commitAlone(writes, units, outcome) : commitPipelined(writes, units, outcome);
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

    /// Commits `writes`, which holds a range removal, as an exclusive operation: the removal counts the keys it
    /// removes, which needs every commit before it applied and none after it started.
    ///
    /// In a store in a directory, a removal of a paged key space with a tree counts them after the commit instead,
    /// unless an earlier write of the batch wrote the key space ([PendingRanges]): the count reads the nodes of the tree
    /// on the paths to the range's bounds, which the page cache holds none of after an open, and would make the commit
    /// wait for those reads before its frame is queued. Each such count starts on a thread of its own once the commit
    /// is published, beside the log's force.
    ///
    /// @param outcome receives the writes as applied, or `null`
    private Commit commitAlone(ArrayWriteBatch writes, PageRef[] units, Applier[] outcome) {
        var hints = prefetch(writes);
        Applier applier = null;
        enterExclusive();
        try {
            ensureOpen();
            checkHealthy();
            applier = new Applier(sequenced.next, log != null, writes.size(), log == null ? 0 : FrameBody.capacityFor(writes, units),
                    pages != null && log != null, writes.sharesChunk());
            if (outcome != null) {
                outcome[0] = applier;
            }
            try {
                var size = writes.size();
                for (var i = 0; i < size; i++) {
                    applyAlone(writes, i, applier, hints, units);
                }
            } catch (RuntimeException | Error throwable) {
                // A resident value whose decoder throws, or a tree that cannot be read, stops the batch
                applier.rollback();
                throw throwable;
            }
            // The versions reference the batch's array, if any does, which the batch then no longer writes
            writes.finish();
            var sequence = applier.publish();
            sequenced.next++;
            return ticket(sequence);
        } finally {
            exitExclusive();
            if (applier != null) {
                applier.countRangesLater();
            }
        }
    }

    /// Applies write `index` of `writes`, a batch that runs alone, holding both roles.
    ///
    /// @param hints what the lookups before the batch's turn found, or `null`
    /// @param units the unit of the file of large values of each put, or `null`
    private void applyAlone(ArrayWriteBatch writes, int index, Applier applier, Hints hints, PageRef[] units) {
        var state = states.get(writes.keyspace(index).name());
        switch (writes.kind(index)) {
            case ArrayWriteBatch.PUT -> {
                var value = writes.value(index);
                var decoded = state.resident ? residentValue(state, writes.decodedValue(index), value) : null;
                applier.put(state, applier.claim(state, writes.key(index)), writes.key(index), value, writes.ownValue(index), writes.home(index), writes.homeOffset(index), decoded,
                        hints == null ? TreeSearch.UNKNOWN : hints.of(index, tree), units == null ? null : units[index]);
            }
            case ArrayWriteBatch.DELETE ->
                    applier.delete(state, writes.key(index), hints == null ? TreeSearch.UNKNOWN : hints.of(index, tree));
            // The removal keeps its bounds until a checkpoint, and the batch's array may go back to the pool
            case ArrayWriteBatch.DELETE_RANGE -> applier.deleteRange(state, ownCopy(writes.key(index)), ownCopy(writes.value(index)));
            default -> throw new IllegalStateException("Unknown write kind " + writes.kind(index));
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
        // Only a batch that puts to a resident key space has values to decode
        var decoded = writes.residentPuts() > 0 ? new Object[size] : null;
        var trees = pages == null ? null : tree;
        var found = trees == null ? null : new byte[size];
        for (var i = 0; i < size; i++) {
            // A decoder that throws stops the batch before it has a sequence
            openWrite(writes, i, chains, keyspaceStates, found, decoded);
        }
        var hints = found == null ? null : new Hints(trees, found);
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
        // The versions reference the batch's array, if any does, which the batch no longer writes once finished; a
        // commit that failed published none of them, so the finish is due only here, before anything after
        // publication can throw
        writes.finish();
        pruneWritten(pending.applier.touched);
        return ticket(pending.sequence);
    }

    /// Prepares write `index` of a commit of the pipeline before the commit is given a sequence: opens the chain of
    /// its key, records its key space, and decodes the value it puts to a resident key space.
    ///
    /// The commit writes blind: a write of a paged key space whose key memory holds no version does not look the key
    /// up, and the checkpoint that takes it settles what it does to the count ([BlindWrites]). With a tree larger than
    /// the page cache the lookup would read the file, and every later commit on the key would wait for the read.
    ///
    /// Opening the chains here grows the ordered map of the key space, which a new key costs a walk down, outside the
    /// lock; commits that run at once insert their keys at once.
    ///
    /// @param chains receives the chain of the write's key
    /// @param keyspaceStates receives the key space of the write
    /// @param found receives, for a write of a paged key space, that it is blind; `null` if the store has no trees
    /// @param decoded receives the decoded value of a put to a resident key space; `null` if the batch has none
    private void openWrite(ArrayWriteBatch writes, int index, Chain[] chains, KeyspaceState[] keyspaceStates,
                           byte[] found, Object[] decoded) {
        var state = states.get(writes.keyspace(index).name());
        chains[index] = state.open(writes.key(index));
        keyspaceStates[index] = state;
        if (state.resident) {
            if (decoded != null && writes.kind(index) == ArrayWriteBatch.PUT) {
                decoded[index] = residentValue(state, writes, index);
            }
        } else if (found != null) {
            found[index] = TreeSearch.BLIND;
        }
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
        var predecessors = new PendingCommit[pending.chains.length];
        for (var i = 0; i < predecessors.length; i++) {
            predecessors[i] = claimWrite(pending, i, sequence);
        }
        pending.predecessors = predecessors;
        pending.sequence = sequence;
        sequenced.next = sequence + 1;
        sequenced.last.follow(pending);
        sequenced.last = pending;
        pending.advanceLazily(PendingCommit.SEQUENCED);
        return true;
    }

    /// Claims the chain of the key of write `index` of `pending` for its write at `sequence`, holding the sequencing
    /// role; a chain dropped since the commit opened it gives way to a new one, which the commit then writes.
    ///
    /// @return the commit of the pipeline given a write of the key before, which applies its write first, or `null`
    private static PendingCommit claimWrite(PendingCommit pending, int index, long sequence) {
        var chain = pending.chains[index];
        if (chain == null) {
            return null;
        }
        while (true) {
            var previous = chain.claim(pending, sequence);
            if (previous != Chain.DROPPED_WRITER) {
                return previous instanceof PendingCommit commit && commit != pending ? commit : null;
            }
            // Dropped since the commit opened it: the key gets a chain of its own
            chain = pending.states[index].open(pending.writes.key(index));
            pending.chains[index] = chain;
        }
    }

    /// Applies the writes of `pending` to their chains, each once the commit before it on the same key has.
    ///
    /// A write whose tree cannot be read takes back the commit's writes: it publishes empty, so the commits after it
    /// go on, and its committing thread throws the failure.
    private void apply(PendingCommit pending) {
        var writes = pending.writes;
        var applier = new Applier(pending.sequence, log != null, pending.writes.size(),
                log == null ? 0 : FrameBody.capacityFor(pending.writes, pending.units), false, writes.sharesChunk());
        try {
            var size = writes.size();
            for (var i = 0; i < size; i++) {
                applyWrite(pending, applier, i);
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

    /// Applies write `index` of `pending` to its chain once the commit before it on the same key has.
    private void applyWrite(PendingCommit pending, Applier applier, int index) {
        var predecessor = pending.predecessors[index];
        if (predecessor != null) {
            predecessor.awaitApplied();
        }
        var writes = pending.writes;
        var inTree = pending.hints == null ? TreeSearch.UNKNOWN : pending.hints.of(index, tree);
        if (writes.kind(index) == ArrayWriteBatch.PUT) {
            applier.put(pending.states[index], pending.chains[index], writes.key(index), writes.value(index), writes.ownValue(index),
                    writes.home(index), writes.homeOffset(index), pending.decoded == null ? null : pending.decoded[index],
                    inTree, pending.units == null ? null : pending.units[index]);
        } else {
            applier.delete(pending.states[index], pending.chains[index], writes.key(index), inTree);
        }
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
                batch.add(FrameBody.COMMIT, commit.sequence, new FrameBody(), 0, 0, false);
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
        if (blobs == null || writes.largestValueBytes() < minLargeValues) {
            return null;
        }
        PageRef[] units = null;
        try {
            for (var i = 0; i < writes.size(); i++) {
                if (writes.kind(i) != ArrayWriteBatch.PUT) {
                    continue;
                }
                var length = writes.valueBytes(i);
                if (length < states.get(writes.keyspace(i).name()).largeValues) {
                    continue;
                }
                if (units == null) {
                    units = new PageRef[writes.size()];
                }
                units[i] = blobs.write(writes.valueArray(i), writes.valueArrayOffset(i), (int) length);
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

    /// {@return a read-only copy of `segment` on an array of its own, or `null` if it is `null`}
    private static MemorySegment ownCopy(MemorySegment segment) {
        return segment == null ? null : MemorySegment.ofArray(Segments.toBytes(segment)).asReadOnly();
    }

    /// {@return the decoded value of a put of `value` to the resident key space `state`: `decoded` if the put has it or
    /// the key space is of byte arrays, whose versions the applier gives an array of the store's in place of any
    /// decoded value, and `value` decoded otherwise}
    private static Object residentValue(KeyspaceState state, Object decoded, MemorySegment value) {
        return decoded != null || state.keyspace.values() == BuiltinCodecs.BYTES_VALUE
                ? decoded
                : state.keyspace.values().decode(value);
    }

    /// {@return the decoded value of write `index` of `writes`, a put to the resident key space `state`, as
    /// [#residentValue(KeyspaceState, Object, MemorySegment)] gives it, reading the encoded value only to decode it}
    private static Object residentValue(KeyspaceState state, ArrayWriteBatch writes, int index) {
        var decoded = writes.decodedValue(index);
        return decoded != null || state.keyspace.values() == BuiltinCodecs.BYTES_VALUE
                ? decoded
                : state.keyspace.values().decode(writes.value(index));
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
            state.unlinkDropped();
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

    /// {@return the chunks the values of the fastest commits of a store with pages share}
    ValueSlab valueSlab() {
        return valueSlab;
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
        // Before the roles: a commit waiting for the creator of a chain to list it may hold the sequencing role
        for (var state : states.values()) {
            state.closing = true;
        }
        enterExclusive();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            exitExclusive();
        }
        // Before the files close: a loader reads them, and needs no role, so none waits for this thread
        for (var state : states.values()) {
            var loader = state.loading;
            if (loader != null) {
                loader.stop();
            }
        }
        // The final checkpoint forces the files on this thread: an interrupt would close their channels under it
        var interrupted = Thread.interrupted();
        try {
            if (checkpointer != null) {
                checkpointer.beginClose();
            }
            if (log != null) {
                log.close();
            }
            if (checkpoint && checkpointer != null) {
                checkpointer.run(true);
            }
        } finally {
            if (checkpointer != null) {
                checkpointer.close();
            }
            // Waits for the counts of range removals running on their own threads, which read the files; one that
            // starts after finds the store closed. The final checkpoint counted those it needed itself
            var gate = rangeCountGate.writeLock();
            gate.lock();
            try {
                HeapPool.leave(this);
                for (var resource : resources) {
                    try {
                        resource.close();
                    } catch (Exception exception) {
                        // Everything is durable by now: a file, lock or pool that fails to close loses nothing
                    }
                }
            } finally {
                gate.unlock();
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /// Makes `executor` start the counts of range removals that run on threads of their own; for tests that hold them
    /// back, or run them at a chosen point.
    void rangeCountsForTests(Executor executor) {
        this.rangeCounts = executor;
    }

    /// Counts `removal` on a thread of its own; a count that does not start is left to the first count of its key
    /// space, or the next checkpoint, that needs it.
    private void startRangeCount(PendingRanges.Removal removal) {
        try {
            rangeCounts.execute(() -> countInBackground(removal));
        } catch (Throwable _) {
            // As under a limit of threads: the removal keeps its read, and the next count that needs it counts it
        }
    }

    /// Counts `removal`, then maintains the store, whose removal and older trees the count's read held back; nothing
    /// once the store is closing.
    ///
    /// A count that fails is left pending with its read, for the next count of the key space or the next checkpoint,
    /// which count it again on their own threads and report what fails.
    private void countInBackground(PendingRanges.Removal removal) {
        var gate = rangeCountGate.readLock();
        if (!gate.tryLock()) {
            return;
        }
        try {
            if (closed) {
                return;
            }
            removal.settle();
            maintainAfterCount();
        } catch (Throwable _) {
            // Reported by the count that tries again
        } finally {
            gate.unlock();
        }
    }

    /// Maintains the store once the count of a range removal is known, unless another thread holds or wants the
    /// publishing role: the read the count held kept the removal from applying, and older trees and their pages from
    /// leaving, which a store that commits nothing more kept until its next commit.
    private void maintainAfterCount() {
        if (closed || publishingWanted.held() || !publishing.tryAcquire()) {
            return;
        }
        try {
            maintain(List.of());
        } finally {
            publishing.release();
        }
        // What applied while this thread held the role, whose committing threads may have parked for it
        publishApplied();
        for (var state : sweepOrder) {
            state.removeDropped(COMMIT_REMOVALS);
            state.unlinkDropped();
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
    record Cut(long sequence, long logStart, long logBytes, long logHeap, TreeVersion base, int maxId,
               List<KeyspaceCut> keyspaces, List<PageRef> superseded) {
    }

    /// What changed in one key space since the last checkpoint.
    ///
    /// @param state the key space
    /// @param dirty the keys written
    /// @param removals the range removals as of the cut
    /// @param count the entries as of the cut, leaving out the blind writes ([BlindWrites]) and the range removals
    ///              counted later ([PendingRanges])
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
            var bound = Reads.boundOf(sequence);
            var cuts = new ArrayList<KeyspaceCut>(states.size());
            for (var state : states.values()) {
                var number = state.dirtyNumber();
                cuts.add(new KeyspaceCut(state, state.cutDirty(), state.removals, state.known(sequence), number));
                if (state.overwrites != null) {
                    state.overwrites.cut(bound);
                }
            }
            var superseded = new ArrayList<PageRef>();
            for (var unit : inFlight) {
                if (unit.version().stamp <= bound && unit.version().replacedAt <= sequence) {
                    superseded.add(unit.version().unit);
                }
            }
            return new Cut(sequence, logStart.getAsLong(), log.appendedBytes(), log.appendedHeap(), base, maxId, cuts, superseded);
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
                        var current = chain.dropped() ? state.chain(chain.key()) : chain;
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

    /// Prepares the writes of `writes`, a batch that runs alone, before its turn: opens the chain of every key the
    /// batch puts, and says how each write learns whether its key existed.
    ///
    /// An operation that runs alone counts exactly as it applies, so here its lookups run before its turn, those that
    /// reach the file on up to [#PREFETCH_THREADS] threads at once, and what they found serves the writes as long as
    /// the trees are still the ones they read. The lookups do not register as a read: a lease per commit would hold
    /// versions back and change the readers the sweep waits for at every commit. Without one, a checkpoint may reuse a
    /// page of the trees they read once newer trees replace them; the page then fails authentication, since its nonce
    /// is not the one its parent records, and the lookup leaves its write to the writer. A lookup that succeeds read
    /// the trees as they were, and its answer is used only while they are still the latest.
    ///
    /// Opening the chains here grows the ordered map of the key space, which a new key costs a walk down, before the
    /// turn.
    ///
    /// @return how each write learns whether its key existed, or `null` if the store has no trees
    private Hints prefetch(ArrayWriteBatch writes) {
        var trees = pages == null ? null : tree;
        try {
            var point = trees == null ? null : new ReadPoint(Reads.boundOf(latest()), trees, pages);
            var size = writes.size();
            var found = trees == null ? null : new byte[size];
            int[] missing = null;
            var misses = 0;
            for (var i = 0; i < size; i++) {
                if (!prefetchWrite(writes, i, point, found)) {
                    continue;
                }
                if (missing == null) {
                    missing = new int[16];
                } else if (misses == missing.length) {
                    missing = Arrays.copyOf(missing, misses * 2);
                }
                missing[misses++] = i;
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

    /// Opens the chain of the key of write `index` if it puts, and looks the key up in the pages of the trees of
    /// `point` the cache holds, unless its chain holds a version, recording in `found` what the lookup found.
    ///
    /// @param point the latest trees, or `null` if the store has none
    /// @param found receives what the lookup found, or `null` if the store has no trees
    /// @return whether the lookup has to read the file
    private boolean prefetchWrite(ArrayWriteBatch writes, int index, ReadPoint point, byte[] found) {
        var kind = writes.kind(index);
        if (kind != ArrayWriteBatch.PUT && kind != ArrayWriteBatch.DELETE) {
            return false;
        }
        var state = states.get(writes.keyspace(index).name());
        var key = writes.key(index);
        var chain = kind == ArrayWriteBatch.PUT ? state.open(key) : state.chain(key);
        if (found == null || chain != null && chain.head != null) {
            return false;
        }
        var root = point.root(state);
        if (root == null) {
            return false;
        }
        found[index] = TreeSearch.lookupCached(pages, root, key);
        return found[index] == TreeSearch.UNKNOWN;
    }

    /// Waits until memory holds the entry of every key `writes` writes in a resident key space, and every entry of a
    /// resident key space it removes a range of, before any chain of them is opened.
    private void awaitLoaded(ArrayWriteBatch writes) {
        var size = writes.size();
        for (var i = 0; i < size; i++) {
            awaitLoaded(writes, i);
        }
    }

    /// Waits until memory holds what write `index` of `writes` writes, if it writes a resident key space.
    private void awaitLoaded(ArrayWriteBatch writes, int index) {
        var state = states.get(writes.keyspace(index).name());
        if (state.loading != null) {
            state.awaitLoaded(writes.kind(index) == ArrayWriteBatch.DELETE_RANGE ? null : writes.key(index));
        }
    }

    /// Looks up the keys of every `step`-th write of `missing` from `first` in the trees of `point`, reading the file,
    /// and records in `found` what each lookup found.
    private void lookUp(ArrayWriteBatch writes, ReadPoint point, byte[] found, int[] missing, int count, int first, int step) {
        for (var i = first; i < count; i += step) {
            var write = missing[i];
            try {
                var root = point.root(states.get(writes.keyspace(write).name()));
                found[write] = TreeSearch.contains(pages, root, writes.key(write)) ? TreeSearch.FOUND : TreeSearch.ABSENT;
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

/// Prunes the chains `touched` wrote, applies the range removals every reader has passed, drops and folds the counts no
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
            state.counts.dropBelow(oldest);
            state.blind.fold(oldest);
            state.ranges.fold(oldest);
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

    /// {@return whether the sweep has chains to visit}
    ///
    /// A read without the publishing role: one that misses a chain just left is made again by the next tick.
    boolean sweepWaiting() {
        return sweepPending || sweepWanted.held();
    }

    /// Lets go of the last commit the arrival list and the sequence hold on to, putting an empty one of the same
    /// sequence in its place, unless a thread holds or wants a role or a commit is under way; at a tick of the
    /// [IdleTicker], so no commit pays for it.
    ///
    /// The next commit given a sequence takes the last one's place, and the next one that arrives while another
    /// sequences takes the arrival list's: until then, in a store gone quiet or one whose commits stopped meeting,
    /// the last one kept its writes as applied, and with them the chunk of the [ValueSlab] its value is on, after a
    /// checkpoint dropped every chain of it. The commit is not changed, so its committing thread may still be pruning
    /// its chains.
    void forgetLastCommits() {
        if (closed || sequencingWanted.held() || !sequencing.tryAcquire()) {
            return;
        }
        try {
            if (!publishingWanted.held() && publishing.tryAcquire()) {
                try {
                    var last = sequenced.last;
                    // Every commit given a sequence is published, and the last is not an empty one already
                    if (last == published.last && last.writes != null) {
                        var start = new PendingCommit(last.sequence);
                        sequenced.last = start;
                        LAST_PUBLISHED.setRelease(published, start);
                    }
                } finally {
                    publishing.release();
                }
            }
            // No commit swapped itself in after the last one the sequencer took: one that swaps itself in after the
            // compare-and-set links itself to the new start, which the cursor moves to before the role goes
            var tail = arrivalTail.commit;
            if (tail.writes != null && arrivalCursor.commit == tail && tail.arrival == null) {
                var start = new PendingCommit(0);
                if (arrivalTail.compareAndSet(tail, start)) {
                    arrivalCursor.setLazily(start);
                }
            }
        } finally {
            sequencing.release();
        }
        // As after an exclusive operation: the commits that arrived or applied while this thread held a role may have
        // parked for it
        drainArrivals();
        publishApplied();
    }

    /// Runs the sweep through a whole pass over the store, or until it has nothing left to do, on the caller's thread.
    ///
    /// A commit sweeps a few chains, so what a snapshot kept through a checkpoint stayed in memory long after it closed:
    /// for hours on a store of a commit a second, for good on one with none, and every chain's value holds a whole chunk
    /// of the [ValueSlab] for as long as one value on it is left. The role is taken only when no thread holds or wants it,
    /// a few chains at a time, and while commits publish this thread sleeps between chunks, so they pay at most a small
    /// share of its time; at most [#SWEEP_PASSES] pass ends a call, so a store whose readers change at every commit
    /// never has it walk its chains all the time.
    ///
    /// @throws InterruptedException if the thread is interrupted while it sleeps
    void finishSweep() throws InterruptedException {
        var endsBefore = -1L;
        var seen = latest();
        while (!closed) {
            if (publishingWanted.held() || !publishing.tryAcquire()) {
                Thread.sleep(1);
                continue;
            }
            boolean more;
            try {
                // After the role, which a close of the store holds as it marks it closed
                if (closed) {
                    return;
                }
                endsBefore = endsBefore < 0 ? sweepEnds : endsBefore;
                // As in a maintenance: the scan must not miss a read that saw the latest sequence before
                VarHandle.fullFence();
                var scan = readers.scan();
                var bounds = bounds(scan.active());
                var cutBound = Reads.boundOf(floor(scan.oldest()));
                sweep(bounds, cutBound, SWEEP_CHUNK);
                more = sweepEnds - endsBefore < SWEEP_PASSES && (sweepPending
                        ? sweptBounds == null || !sameReaders(bounds, cutBound, sweptBounds, sweptCutBound)
                        : sweepWanted.held());
            } finally {
                publishing.release();
            }
            // What applied while this thread held the role, whose committing threads may have parked for it
            publishApplied();
            for (var state : sweepOrder) {
                state.removeDropped(COMMIT_REMOVALS);
                state.unlinkDropped();
            }
            if (!more) {
                break;
            }
            var current = latest();
            if (current != seen) {
                seen = current;
                Thread.sleep(1);
            }
        }
        for (var state : sweepOrder) {
            state.removeDropped();
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
        var size = touched.size();
        for (var i = 0; i < size; i++) {
            pruneTouched(touched, i, bounds, cutBound);
        }
    }

    /// Prunes the chain of write `index` of `touched`, the writes of a published commit, and asks for a sweep if the
    /// prune leaves a chain a later one may shorten.
    private void pruneTouched(List<Touched> touched, int index, long[] bounds, long cutBound) {
        var write = touched.get(index);
        if (prune(write.state(), write.chain(), write.version(), bounds, cutBound)) {
            requestSweep();
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
            if (sweepIndex == 0 && sweepAfter == null && sweepCursor == null) {
                sweepFound = false;
                passBounds = bounds;
                passCutBound = cutBound;
            }
            var state = sweepOrder.get(sweepIndex % sweepOrder.size());
            boolean finished;
            if (state.sweepsIndex()) {
                if (sweepCursor == null) {
                    sweepCursor = new ChainIndex.Cursor();
                }
                finished = false;
                while (budget > 0) {
                    var chain = state.next(sweepCursor);
                    if (chain == null) {
                        finished = true;
                        break;
                    }
                    sweepFound |= prune(state, chain, chain.head, bounds, cutBound);
                    budget--;
                }
            } else {
                // An apply may have put a newer version above the entry's value: the prune leaves it, and drops nothing
                var entries = (sweepAfter == null ? state.entries : state.entries.tailMap(sweepAfter, false)).entrySet().iterator();
                while (budget > 0 && entries.hasNext()) {
                    var entry = entries.next();
                    var chain = entry.getValue();
                    sweepFound |= prune(state, chain, chain.head, bounds, cutBound);
                    sweepAfter = entry.getKey();
                    budget--;
                }
                finished = !entries.hasNext();
            }
            if (finished) {
                sweepIndex = (sweepIndex + 1) % sweepOrder.size();
                sweepAfter = null;
                sweepCursor = null;
                visitedKeyspaces++;
                if (sweepIndex == 0) {
                    sweepEnds++;
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
    ///
    /// Each write adds a version of its own, linked to the head it replaces, and nothing drops one before the commit
    /// publishes. A transaction's scan relies on it: it reads at the bound of its call ([#scanBound()]) while its
    /// visitor writes the keys ahead of it, so it needs the versions under those writes. Writes of one key within a
    /// transaction may share a version only while none of its scans runs.
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
        // Whether a range removal of a paged key space with a tree may count after the commit ([PendingRanges])
        private final boolean deferredRanges;
        // Whether the commit's values are on one chunk of the slab, as the log's estimate of its heap takes it
        private final boolean sharesChunk;
        // The range removals this commit counts after it is published, or null before any
        private ArrayList<PendingRanges.Removal> deferred;
        // Whether the publication of the commit listed them with their key spaces
        private boolean deferredListed;

        Applier(long sequence, boolean journaled) {
            this(sequence, journaled, 4, 256, false, false);
        }

        /// Creates the applier of a commit of about `writes` writes, whose frame body takes about `frameBytes`.
        ///
        /// @param deferredRanges whether its range removals of paged key spaces with a tree may count after the
        ///                       commit: only for a batch that runs alone in a store in a directory
        /// @param sharesChunk whether the batch put the values the store keeps on one chunk of the [ValueSlab], as the
        ///                    log's estimate of the commit's heap takes it
        Applier(long sequence, boolean journaled, int writes, int frameBytes, boolean deferredRanges, boolean sharesChunk) {
            this.sequence = sequence;
            // Most commits write one or two key spaces
            this.deltaStates = new KeyspaceState[2];
            this.deltas = new long[2];
            this.touched = new ArrayList<>(writes);
            this.frame = journaled ? new FrameBody(frameBytes) : null;
            this.deferredRanges = deferredRanges;
            this.sharesChunk = sharesChunk;
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
            chain.markBlind();
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

        /// {@return the bound of a read that sees the latest commit and this one's writes so far, and none it makes
        /// after: the bound of a scan that starts now}
        ///
        /// After the last write a commit can hold, the next stamp would carry into the next sequence, so the bound is
        /// then that of the whole sequence.
        long scanBound() {
            return op > Reads.MAX_OPS ? Reads.boundOf(sequence) : Reads.stamp(sequence, op) - 1;
        }

        /// {@return the number of entries of `state` once this commit applies}
        ///
        /// It looks up the keys of the blind writes whose effect is not known yet, in the latest trees: the commits
        /// before this one have published, as this one runs alone. It counts the range removals of those commits whose
        /// count is not known yet, in the trees their reads keep.
        long count(KeyspaceState state) {
            var point = writerPoint(scanBound());
            var blind = state.blind.correction(sequence - 1, key -> Reads.inTreeOnly(state, key, point));
            return state.counts.last() + blind + state.ranges.correction(sequence - 1) + delta(state);
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
            put(state, claim(state, key), key, value, null, null, 0, decoded, inTree, external);
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
        /// @param own the array that holds `value` alone, which no one writes and the version keeps in its place, or `null`
        /// @param source the read-only segment `value` is part of, which the version keeps in its place, or `null` for a
        ///               value of its own
        /// @param offset where `value` starts in `source`
        void put(KeyspaceState state, Chain chain, MemorySegment key, MemorySegment value, byte[] own, MemorySegment source, int offset,
                 Object decoded, byte inTree, PageRef external) {
            var stamp = nextStamp();
            var head = chain.head;
            var existed = Reads.existence(state, head, key, writerPoint(stamp - 1), inTree);
            var length = Math.toIntExact(value.byteSize());
            if (source != null && !state.resident && ValueSlab.Overwrites.sampled(sequence)) {
                state.overwrites.sample(head, length);
            }
            Version version;
            var bytes = state.resident && state.keyspace.values() == BuiltinCodecs.BYTES_VALUE;
            if (own != null) {
                version = Version.ofOwn(stamp, own, bytes ? own : decoded, head, external);
            } else if (bytes) {
                // A byte array is its own encoding: one copy of the store's serves the reads, which return the instance
                // the store holds, and the checkpoints, where the caller's array and a copy of it served them apart
                var copy = Segments.toBytes(value);
                version = Version.ofOwn(stamp, copy, copy, head, external);
            } else if (state.resident && (source == null || source.byteSize() > 2L * length + RESIDENT_SLACK)) {
                // A resident version lives as long as its key does: on a value of its own, not on the array of the
                // batch or the log frame it came in, which a session written beside ten messages held 12 times over
                version = Version.ofOwn(stamp, Segments.toBytes(value), decoded, head, external);
            } else if (source == null && pages == null) {
                // A store in memory keeps a version until a write replaces it, so on an array of its own: on a batch's,
                // a value that outlived the writes beside it kept them all, three times the bytes of each key
                version = Version.ofOwn(stamp, Segments.toBytes(value), decoded, head, external);
            } else if (source == null) {
                version = Version.of(stamp, value, 0, length, decoded, head, external);
            } else {
                version = Version.of(stamp, source, offset, length, decoded, head, external);
            }
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
            if (chain == null || !chain.ordered()) {
                var head = chain == null ? null : chain.head;
                if (!Reads.exists(state, head, key, writerPoint(scanBound()), inTree)) {
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
            // A recovery's removals count for nothing: the puts of its commits are not sampled either
            if (frame != null && !state.resident && ValueSlab.Overwrites.sampled(sequence)) {
                state.overwrites.sampleRemoval(head);
            }
            var version = new Version(stamp, (MemorySegment) null, null, head);
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
            var removed = 0L;
            if (countsLater(state)) {
                deferCount(state, from, to, stamp);
            } else {
                removed = Reads.count(state, from, to, writerPoint(stamp - 1));
            }
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
                if (unit.state() == state && removal.covers(unit.chain().key())) {
                    replace(unit.version());
                }
            }
            for (var write : touched) {
                if (write.state() == state && removal.covers(write.chain().key())) {
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

        /// {@return whether the range removal of `state` this commit applies next counts its entries after the commit}
        ///
        /// Only one of a paged key space with a tree does, as only its count reads pages. Nor does one of a key space an
        /// earlier write of the commit wrote: the read it registers is at the commit before, so its count would miss
        /// that write's version, which the commit's own count takes in.
        private boolean countsLater(KeyspaceState state) {
            if (!deferredRanges || state.resident) {
                return false;
            }
            var trees = tree;
            if (trees == null || trees.root(state.id) == null) {
                return false;
            }
            for (var write : touched) {
                if (write.state() == state) {
                    return false;
                }
            }
            return true;
        }

        /// Registers a read at the sequence before this commit's, and keeps the removal of `from` to `to` in `state`,
        /// stamped `stamp`, to count against it once the commit is published.
        ///
        /// The commit runs alone, so the read is at the latest sequence: it keeps the trees the commit reads, the
        /// versions in memory at or below their bound, none of `state` being above it, and the range removals of the
        /// commit stamped before `stamp`, which it counts against as the commit would have.
        private void deferCount(KeyspaceState state, MemorySegment from, MemorySegment to, long stamp) {
            var registered = readers.register();
            if (registered != sequence - 1) {
                readers.unregister(registered);
                throw new IllegalStateException("The latest sequence moved while a range removal ran alone");
            }
            if (deferred == null) {
                deferred = new ArrayList<>(2);
            }
            deferred.add(new PendingRanges.Removal(sequence, state, from, to, writerPoint(stamp - 1), readers, registered));
        }

        /// Starts the counts of the range removals the published commit counts later, each on a thread of its own.
        void countRangesLater() {
            if (deferred == null || !deferredListed) {
                return;
            }
            for (var removal : deferred) {
                startRangeCount(removal);
            }
            deferred = null;
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
                    log.append(FrameBody.COMMIT, sequence, frame, unitBytes, touched.size(), sharesChunk);
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
                batch.add(FrameBody.COMMIT, sequence, frame, unitBytes, touched.size(), sharesChunk);
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

        /// Publishes what the commit adds to the units in flight, the keys the next checkpoint writes, the counts and the
        /// range removals it counts later.
        private void publishCounted() {
            if (externals != 0 || pages != null) {
                var size = touched.size();
                for (var i = 0; i < size; i++) {
                    publishTouched(i);
                }
            }
            for (var i = 0; i < deltaCount; i++) {
                var state = deltaStates[i];
                state.counts.append(sequence, state.counts.last() + deltas[i]);
            }
            if (deferred != null) {
                for (var removal : deferred) {
                    removal.state.ranges.add(removal);
                }
                deferredListed = true;
            }
        }

        /// Publishes what the commit's write `index` adds to the units in flight, the keys the next checkpoint writes
        /// and the blind writes.
        private void publishTouched(int index) {
            var write = touched.get(index);
            var version = write.version();
            if (version == null) {
                return;
            }
            if (version instanceof Version.External external) {
                inFlight.add(new InFlight(write.state(), write.chain(), external));
            }
            if (pages != null) {
                write.state().markDirty(write.chain());
            }
            if (write.blind()) {
                write.state().blind.add(write.chain(), sequence, version.removes());
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
            if (deferred != null && !deferredListed) {
                for (var removal : deferred) {
                    removal.abandon();
                }
                deferred = null;
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
