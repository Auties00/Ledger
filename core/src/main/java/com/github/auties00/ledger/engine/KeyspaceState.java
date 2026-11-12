package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/// The entries of one key space in memory: a version chain per key, the range removals readers may still need, and
/// the number of entries as of each recent commit.
///
/// In a store in a directory, the entries of a [Keyspace.Residency#PAGED] key space are only those written since the
/// checkpoint every reader's tree holds; a key with no version in memory is read from the tree. A resident key space
/// holds every entry, once its [loader][ResidentLoader] has put in memory those of the checkpoint the store opened from;
/// until then a key the loader has not reached is read from the tree, and no commit writes it.
///
/// The chains of the keys are in two maps: an ordered one for scans, and a hash index ([ChainIndex]) for lookups of
/// one key, which at a million keys costs a fraction of a walk down the ordered one. A chain enters the index first, then the ordered
/// map, and leaves them in the same order.
///
/// A key space of a store in a directory orders its chains lazily: a new chain is only listed, and the reads of a
/// range order the listed ones before they walk the map ([#slice]). A paged key space's chains leave memory once a
/// checkpoint takes them, so with no read of a range in between, as when a store takes writes it does not scan, the
/// walk down the map to insert each new key, and the one to remove it, are never paid; a commit that wrote a new key
/// paid both. A resident key space keeps its chains, so one read only by key, as most are, never orders them: the
/// ordered map cost each entry 73 bytes, its node, its index and its key's segment, a third of an entry of a 20-byte
/// key and a 24-byte value, and an open walked down it for every entry it loaded. The sweep reaches its chains through
/// the index instead ([#next(ChainIndex.Cursor)]).
///
/// The first descending scan of such a key space links each chain of the ordered map to the one below it
/// ([Chain#below()]), and the key space keeps the links from then on, so a descending scan steps from one chain to the
/// next with a load and a comparison of keys ([#walk]): the map's descending iterator has no links back, and finds each
/// next chain again from the top of the map, a comparison of keys for each of its levels, each a miss on a key's bytes,
/// half the time of a chat's newest page beside a thousand buffered messages. Chains enter the map, and their links
/// change, under one lock; a chain that enters links the one below it before the map publishes it, then the one above
/// links it. Chains leave the map without the lock: one that left stays on the links, emptied, until a repair points the
/// link above past it and clears its own, which a commit runs once a thousand left and a checkpoint runs at its end; a
/// scan that steps onto it reads the tree for its key, as for an empty chain still in the map, and one that finds its
/// link cleared asks the map for the next chain.
///
/// Readers use it concurrently with the commits. Commits create chains before they are given a sequence; everything
/// else changes only under it.
final class KeyspaceState {
    /// The dropped chains from which [#removeDropped()] spreads the removals over the processors.
    private static final int PARALLEL_REMOVALS = 4_096;
    /// The dropped chains the list may hold, at least, before a commit sweeps them off it; past it, as many as memory
    /// holds chains, so a sweep's steps stay within twice the drops.
    private static final int LISTED_DROPS = 4_096;
    /// The chains one hold of the links lock links one at a time as it orders them, at least; past it, or an eighth
    /// of the map, the rest go in unlinked and one walk up the map links them all before the hold ends: a chain linked
    /// alone takes one more walk down the map than a chain not linked, to the chain above, about 35 comparisons of keys
    /// at 200,000 chains, where the walk up the map costs each chain it holds about two misses.
    private static final int LINKED_ORDERS = 1_024;
    /// The chains that left the ordered map, still on the links, past which a commit points the links past them, and
    /// the most one commit does: what they keep reachable until then stays within a thousand chains and their keys.
    private static final int LEFT_REPAIRS = 1_024;
    /// How many walks of the map, each down its levels, a repair of the link above a chain that left costs, against
    /// one step of a walk up the map that relinks it whole.
    private static final int RELINK_RATIO = 32;
    // The states of the links: no chain is linked, as before the first descending scan; some chains of the map are not
    // linked, while a hold of the links lock orders many chains or links the map the first time; every chain is linked
    private static final int UNLINKED = 0;
    private static final int RELINKING = 1;
    private static final int LINKED = 2;

    final Keyspace<?, ?> keyspace;
    final boolean resident;
    // The id the log knows the key space by, set while the store opens
    int id;
    // The chains in the order of their keys; a chain the writer dropped may stay a while, empty
    final ConcurrentSkipListMap<MemorySegment, Chain> entries;
    // The chains by the bytes of their keys
    private final ChainIndex index;
    // The chains the writer dropped that the ordered map may still hold
    private final ConcurrentLinkedQueue<Chain> leaving;
    // Copy-on-write: the writer publishes a new array, readers keep the one they loaded
    volatile RangeRemoval[] removals;
    // The number of entries visible as of each commit sequence that changed it, leaving out the blind writes and the
    // range removals counted later
    final CountHistory counts;
    // The writes whose effect on the count the trees settle
    final BlindWrites blind;
    // The range removals whose count is not known yet
    final PendingRanges ranges;
    // The copies of the keys the chains hold, and of the map's keys of the chains that hold their keys in words of
    // their own; nothing else, as a copy here stays reachable while another in its chunk does
    private final KeySlab keys;
    /// What the commits say about whether the values may share a [ValueSlab]'s chunks; `null` in a resident key space,
    /// whose values never do.
    final ValueSlab.Overwrites overwrites;
    // The chains written since the last checkpoint was cut, each listed once per cut; guarded by the publishing role
    ArrayList<Chain> dirty;
    // The number of the cut `dirty` collects for, guarded by the publishing role
    private int dirtyCut;
    // The bytes from which a value a batch puts goes to the file of large values, set while the store opens
    long largeValues = Long.MAX_VALUE;
    // Whether the key space orders its chains lazily, set while the store opens, before any chain is opened
    boolean lazy;
    // The chains opened and not yet ordered, newest first, linked through Chain.link
    @SuppressWarnings("unused")
    private volatile Chain unordered;
    // Held while the listed chains are ordered or pruned, so a read of a range that finds the list empty waits for
    // the chains another thread took from it
    private final ReentrantLock ordering;
    // The chains dropped while listed since the list was last pruned, which only a prune of the list lets go of
    private final AtomicInteger droppedListed;
    // The dropped chains listed past which a commit sweeps them off, as of the last prune of the list
    private volatile int listedDropsLimit = LISTED_DROPS;
    /// What puts in memory the entries of the checkpoint the store opened from, in a resident key space, or `null`
    /// once memory holds them all, or in a key space that holds every entry from the start; set while the store opens,
    /// cleared by the loader after its last run.
    volatile ResidentLoader loading;
    // Held, in a key space that orders lazily, while chains enter the ordered map and while the links between its
    // chains change; taken after `ordering`, or alone by a repair of the links, which a read of a range that finds the
    // list empty does not wait for
    private final ReentrantLock linking;
    // UNLINKED, RELINKING or LINKED; written holding `linking`
    private volatile int links;
    // The chains the hold of `linking` may still link one at a time as it orders them; guarded by it
    private int linkedOrders;
    // The chains that left the ordered map, linked, since a repair last pointed the links past them
    private final ConcurrentLinkedQueue<Chain> left;
    // The chains `left` holds, about
    private final AtomicInteger leftCount;
    // Whether so many chains left the map at once, unlisted, that the next repair relinks the map
    private volatile boolean relinkWanted;
    /// Whether the store is closing, after which a commit no longer waits for the creator of a chain to list it; set
    /// by the store before its close waits for the commits under way.
    volatile boolean closing;

    private static final VarHandle UNORDERED;

    static {
        try {
            UNORDERED = MethodHandles.lookup().findVarHandle(KeyspaceState.class, "unordered", Chain.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    KeyspaceState(Keyspace<?, ?> keyspace) {
        this.keyspace = keyspace;
        this.resident = keyspace.residency() == Keyspace.Residency.RESIDENT;
        this.id = -1;
        this.entries = new ConcurrentSkipListMap<>(Segments.UNSIGNED);
        this.index = new ChainIndex();
        this.leaving = new ConcurrentLinkedQueue<>();
        this.removals = RangeRemoval.NONE;
        this.counts = new CountHistory(0, 0);
        this.blind = new BlindWrites();
        this.ranges = new PendingRanges();
        this.dirty = new ArrayList<>();
        this.keys = new KeySlab();
        this.overwrites = resident ? null : new ValueSlab.Overwrites();
        this.ordering = new ReentrantLock();
        this.droppedListed = new AtomicInteger();
        this.linking = new ReentrantLock();
        this.left = new ConcurrentLinkedQueue<>();
        this.leftCount = new AtomicInteger();
    }

    /// {@return the chain of `key`, or `null` if memory holds none}
    Chain chain(MemorySegment key) {
        return index.holdsNone() ? null : index.get(key, Segments.hash(key));
    }

    /// {@return the bytes of the copies of the keys in the chunks of the key space's slab; for tests}
    ///
    /// @see KeySlab#usedForTests()
    int keyBytesForTests() {
        return keys.usedForTests();
    }

    /// {@return the chains memory holds, ordered or not}
    int chains() {
        return index.size();
    }

    /// {@return whether the sweep reaches the chains through the index, as the ordered map may lack them: those of a
    /// resident key space ordered lazily}
    boolean sweepsIndex() {
        return lazy && resident;
    }

    /// {@return the next chain of a walk over the index from `cursor`, or `null` once it visited them all}
    ///
    /// @see ChainIndex#next(ChainIndex.Cursor)
    Chain next(ChainIndex.Cursor cursor) {
        return index.next(cursor);
    }

    /// {@return whether a read of `key` reads memory as the key space's residency says, which is `false` only in a
    /// resident key space, for a key its loader has not reached}
    ///
    /// A read of a resident key space calls it before it reads memory, and reads a key it returns `false` for from
    /// its own tree alone: no commit wrote the key.
    boolean loaded(byte[] key) {
        var loader = loading;
        return loader == null || loader.holds(key);
    }

    /// Waits until memory holds the entry of `key`, if it has one, or every entry if `key` is `null`, before a write
    /// of it: no commit writes a key of a resident key space its loader has not passed.
    ///
    /// @throws IllegalStateException if the store closes meanwhile
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the load failed, which the next write retries
    void awaitLoaded(MemorySegment key) {
        var loader = loading;
        if (loader != null) {
            loader.await(key);
        }
    }

    /// Puts in memory the chain of `key`, whose only version is `head`, unless memory holds a chain of it; for the
    /// loader of a resident key space, which no commit writes beside.
    ///
    /// The chain has its version before it is in the index, so the sweep, which walks the index, never finds it empty.
    void install(MemorySegment key, Version head) {
        var hash = Segments.hash(key);
        var chain = index.get(key, hash);
        if (chain != null) {
            if (!chain.released()) {
                return;
            }
            index.remove(chain);
        }
        var created = keys.chain(key, hash);
        created.head = head;
        if (index.putIfAbsent(created) != null) {
            return;
        }
        if (lazy) {
            listCreated(created);
        } else {
            order(created);
        }
    }

    /// {@return the newest version of `key` in memory, or `null`}
    Version head(MemorySegment key) {
        var chain = chain(key);
        return chain == null ? null : chain.head;
    }

    /// {@return the newest version in memory of the key whose bytes are `key`, or `null`}
    Version head(byte[] key) {
        if (index.holdsNone()) {
            return null;
        }
        var chain = index.get(key, Segments.hash(key));
        return chain == null ? null : chain.head;
    }

    /// {@return the chain of `key`, created if memory holds none, and in the ordered map}
    ///
    /// Safe for concurrent use: commits call it for the keys they are about to write, so the ordered map, the costly
    /// one, grows before their turn. A chain dropped meanwhile is returned dropped; the sequencer, which finds it so
    /// when it gives the commit its writes, opens the key again.
    ///
    /// A read of a range finds the chain it returns. A chain enters the index before the thread that created it lists
    /// or orders it, and a commit that finds it there may publish before that thread goes on. So one that finds it
    /// neither listed nor ordered orders it itself where chains order eagerly, and otherwise waits the few instructions
    /// until its creator lists it: the link of a chain holds the list until a thread orders it and the chain below it
    /// after, so the creator alone may enter it on the list. A creator that fails to list it drops it, and the chain is
    /// returned dropped; once the store is closing, the wait gives up and returns the chain unlisted, which a read of a
    /// range during the close may miss while the log and the final checkpoint hold its writes.
    Chain open(MemorySegment key) {
        return open(key, Segments.hash(key));
    }

    /// {@return the chain of `key`, whose hash is `hash`, as [#open(MemorySegment)] gives it}
    Chain open(MemorySegment key, int hash) {
        var chain = index.get(key, hash);
        if (chain != null && chain.released()) {
            // Dropped, and maybe not out of the index yet: the key gets a chain of its own
            index.remove(chain);
            chain = null;
        }
        if (chain == null) {
            var created = keys.chain(key, hash);
            chain = index.putIfAbsent(created);
            if (chain == null) {
                chain = created;
                if (lazy) {
                    listCreated(created);
                    return created;
                }
            }
        }
        return reach(chain);
    }

    /// Adds to `lookups` the lookup of `key`, whose hash is `hash`, which [#open(MemorySegment, int, Chain)] then
    /// opens with what it found.
    void lookUp(ChainIndex.Lookups lookups, MemorySegment key, int hash) {
        lookups.add(index.holdsNone() ? null : index, key, hash);
    }

    /// {@return the chain of `key`, whose hash is `hash`, as [#open(MemorySegment)] gives it, from `found`, what a
    /// lookup of the key made in a block of [ChainIndex.Lookups] found}
    ///
    /// The lookup may be stale: a chain it found that is dropped by now, or no chain, which a chain created since for
    /// the key, by an earlier write of the same commit as well, may have replaced, are looked up again before a chain
    /// is created, so the key gets no second copy in the slab.
    Chain open(MemorySegment key, int hash, Chain found) {
        if (found == null || found.released()) {
            return open(key, hash);
        }
        return reach(found);
    }

    /// {@return `chain`, which the index holds or held, once a read of a range finds it: ordered where chains order
    /// eagerly, or listed by its creator}
    private Chain reach(Chain chain) {
        if (!chain.reachable()) {
            if (lazy) {
                awaitListed(chain);
            } else {
                order(chain);
            }
        }
        return chain;
    }

    /// Waits until the thread that created `chain`, which entered the index and is about to enter the list, listed it,
    /// or dropped it as it failed to, or until the store is closing.
    ///
    /// Only a virtual machine error, a stack overflow or a failure to link a call site on its first use, stops a
    /// creator between the two, and a creator that throws drops the chain; the close is the bound if its handler
    /// fails too, as a commit that waits here holds the close back.
    private void awaitListed(Chain chain) {
        while (!chain.reachable() && !chain.released() && !closing) {
            Thread.yield();
        }
    }

    /// Lists `chain`, which this thread created and entered in the index, or drops it if that fails, so no commit that
    /// found it in the index waits for it for good.
    private void listCreated(Chain chain) {
        try {
            list(chain);
        } catch (Throwable failure) {
            abandon(chain);
            throw failure;
        }
    }

    /// Drops `chain`, which this thread created and entered in the index and failed to list or to give its first
    /// version: no write was claimed on it, so the claim of the first is still 0.
    private void abandon(Chain chain) {
        if (chain.release(0)) {
            empty(chain);
        }
    }

    /// Lists `chain` among those a read of a range orders first.
    private void list(Chain chain) {
        while (true) {
            var head = unordered;
            chain.link = head;
            if (UNORDERED.compareAndSet(this, head, chain)) {
                chain.markListed();
                return;
            }
        }
    }

    /// Orders every listed chain that is not dropped, so the ordered map holds every chain a read of a range may need,
    /// linked if the key space links its chains; safe for concurrent use.
    private void orderListed() {
        // Both read in this order: a thread that takes the list holds the lock until it ordered what it took
        if (unordered == null && !ordering.isLocked()) {
            return;
        }
        ordering.lock();
        try {
            resetDropped();
            var chain = (Chain) UNORDERED.getAndSet(this, null);
            if (chain == null) {
                return;
            }
            lockLinks();
            try {
                while (chain != null) {
                    var next = chain.link;
                    // Off the list before it is ordered, where its link points down the map
                    chain.link = null;
                    order(chain);
                    chain = next;
                }
            } finally {
                unlockLinks();
            }
        } finally {
            ordering.unlock();
        }
    }

    /// Takes the dropped chains off the list, once a checkpoint dropped most of those it holds, and, in a paged key
    /// space, orders the chains a prune left that no commit wrote since `cut`, so the sweep, which walks the ordered map
    /// there, shortens them; safe for concurrent use. A resident key space keeps the others listed: its sweep walks the
    /// index.
    ///
    /// It then points the links past every chain that left the ordered map, or relinks the map if most of it left, as
    /// after a checkpoint, under the links lock alone, which a read of a range that finds the list empty does not wait
    /// for.
    ///
    /// @param cut the number of the cut whose keys the prune went through, as [#markDirty] gave it to the chains
    void pruneListed(long cut) {
        if (unordered != null) {
            ordering.lock();
            try {
                lockLinks();
                try {
                    unlinkListed(cut);
                } finally {
                    unlockLinks();
                }
            } finally {
                ordering.unlock();
            }
        }
        if (links != UNLINKED && (relinkWanted || leftCount.get() > 0)) {
            lockLinks();
            try {
                skipLeft(Integer.MAX_VALUE);
            } finally {
                unlockLinks();
            }
        }
    }

    /// Takes the dropped chains off the list once the commits dropped more of them than a sweep of it costs, and points
    /// the links past the chains that left the ordered map once a thousand did, a thousand at most, unless another
    /// thread holds the list or the links; safe for concurrent use.
    ///
    /// Only a checkpoint's prune and a read of a range took them off before, so a key space whose commits removed keys
    /// memory did not hold kept a chain for each such commit, with its key's copy, until the next checkpoint: millions
    /// of them, or for good while checkpoints failed. The links keep as long, unless repaired, the chains the commits
    /// take out of the ordered map, once the sweep dropped them or a range removal emptied them.
    void unlinkDropped() {
        if (droppedListed.get() >= listedDropsLimit && ordering.tryLock()) {
            try {
                if (tryLockLinks()) {
                    try {
                        unlinkListed(-1);
                    } finally {
                        unlockLinks();
                    }
                }
            } finally {
                ordering.unlock();
            }
        }
        if (leftCount.get() >= LEFT_REPAIRS && links == LINKED && tryLockLinks()) {
            try {
                skipLeft(LEFT_REPAIRS);
            } finally {
                unlockLinks();
            }
        }
    }

    /// Counts the dropped chains listed from 0 again, and sets how many make a commit sweep them off: the index's size
    /// is summed here, as summing its segments at every commit cost more than the sweep; holding the ordering lock.
    private void resetDropped() {
        droppedListed.set(0);
        listedDropsLimit = Math.max(LISTED_DROPS, index.size());
    }

    /// Unlinks from the list, in place, the chains that are dropped and, in a paged key space, those it orders, the
    /// ones no commit wrote since `cut`; holding the ordering lock and the links lock.
    ///
    /// The list is never taken whole: a read of a range that finds it empty does not wait for the lock, so a list put
    /// back after a walk let a read that came during the walk miss every chain the walk kept, all of a resident key
    /// space's. A chain leaves the list only once it is dropped, or to enter the ordered map before the lock goes.
    /// Commits only push chains on the head, and only holders of the lock unlink, so the links past the head change
    /// under this thread alone.
    private void unlinkListed(long cut) {
        resetDropped();
        Chain previous = null;
        var chain = unordered;
        while (chain != null) {
            var next = chain.link;
            var dropped = chain.dropped() || chain.released();
            // A plain read off the publishing role: a chain marked meanwhile is only ordered early
            var orders = !dropped && !resident && chain.dirtyCut <= cut;
            if (!dropped && !orders) {
                previous = chain;
            } else {
                if (previous == null && !UNORDERED.compareAndSet(this, chain, next)) {
                    // A commit listed a chain before this one since the walk started: the link to unlink is its
                    previous = unordered;
                    while (previous.link != chain) {
                        previous = previous.link;
                    }
                }
                if (previous != null) {
                    previous.link = next;
                }
                // Off the list before it is ordered, where its link points down the map
                chain.link = null;
                if (orders) {
                    order(chain);
                }
            }
            chain = next;
        }
    }

    /// Inserts `chain` into the ordered map, in place of a dropped chain of the same key, unless it is dropped; in a
    /// key space that orders lazily, holding the links lock.
    ///
    /// A chain the hold links links the chain below it before the map publishes it, so a scan that starts at it, as one
    /// that came before the hold, and wants none of its chains, may, still walks every chain below; the chain above
    /// links it after. Only removals run beside the writes the lock serialises, so a link set to the chain below a key
    /// leads to the map's chain below, through chains that left meanwhile.
    ///
    /// One walk down the map finds the chain above, whose link gives the chain below as well
    /// ([#below(MemorySegment, Chain)]), so a chain linked as it enters costs the scan that orders it one walk more than
    /// a chain that does not, rather than one for each neighbour.
    private void order(Chain chain) {
        var linked = lazy && linksEach();
        // The map's key, made once for every try: a copy in the slab for a key in the words of its chain
        MemorySegment key = null;
        // Released counts as dropped on both sides: another thread may have used the chain, and a prune dropped it,
        // while this one was still inserting it; a later chain of the key then holds the place
        while (!chain.dropped() && !chain.released()) {
            if (key == null) {
                key = keys.key(chain);
            }
            Chain above = null;
            if (linked) {
                var higher = entries.higherEntry(key);
                above = higher == null ? null : higher.getValue();
                chain.linkBelow(below(key, above));
            }
            var previous = entries.putIfAbsent(key, chain);
            if (previous == null || previous == chain) {
                inserted(key, chain, above);
                return;
            }
            // A chain being dropped is released before it is marked dropped
            if (!previous.dropped() && !previous.released()) {
                if (chain.released()) {
                    // Dropped since the check above, and replaced: the later chain is the key's
                    return;
                }
                throw new IllegalStateException("Two chains hold one key of " + keyspace.name());
            }
            if (entries.replace(key, previous, chain)) {
                // The dropped chain left the map with the replace: the chain above links this one now
                inserted(key, chain, above);
                return;
            }
        }
    }

    /// Records that the ordered map holds `chain`, at `key`, and takes it out again if it was dropped meanwhile, unseen
    /// by [#empty], which lists for removal only the chains it finds ordered; then points the link of the chain above
    /// at it, or past it if it left.
    ///
    /// @param above the chain the map held right above `key` before the insert, or `null` if the hold does not link the
    ///              chain or none is above it; no chain entered between them since, and if it left since, the chain
    ///              above it reaches the one that entered through it
    private void inserted(MemorySegment key, Chain chain, Chain above) {
        chain.markOrdered();
        var stays = !chain.dropped() || !entries.remove(key, chain);
        if (above != null) {
            if (stays) {
                above.linkBelow(chain);
            } else {
                linkAbove(key);
            }
        }
    }

    /// {@return whether the chain the hold orders next links its neighbours as it enters the map, holding the links
    /// lock}
    ///
    /// Past the chains a hold links one at a time, the others go in unlinked, scans stop walking the links, and the
    /// hold relinks the whole map before it lets the lock go ([#unlockLinks()]).
    private boolean linksEach() {
        if (links != LINKED) {
            return false;
        }
        if (linkedOrders > 0) {
            linkedOrders--;
            return true;
        }
        links = RELINKING;
        return false;
    }

    /// {@return the chain the ordered map holds right below `key`, or [Chain#BOTTOM]}
    private Chain below(MemorySegment key) {
        var lower = entries.lowerEntry(key);
        return lower == null ? Chain.BOTTOM : lower.getValue();
    }

    /// {@return what a chain entering the ordered map at `key` links as the one below it, holding the links lock: the
    /// link of `above`, the chain the map held right above `key` or `null` for none, if it leads below `key`; otherwise
    /// the chain the map holds right below `key`, or [Chain#BOTTOM]}
    ///
    /// Under the lock no chain enters the map and no repair clears a link, so the link of `above` leads to the chain the
    /// map holds right below `above`, through chains that left. Its first step may stop short of that, at a chain that
    /// left between `key` and `above`, or at a chain of `key` itself; one below `key` may be a chain that left, listed
    /// for the repair that points the links past it, whose own link leads on down the map.
    private Chain below(MemorySegment key, Chain above) {
        if (above != null) {
            var link = above.below();
            if (link == Chain.BOTTOM || link != null && link.compareKey(key) < 0) {
                return link;
            }
        }
        return below(key);
    }

    /// Points the link of the chain the ordered map holds right above `key` at the chain the map holds below that
    /// chain; holding the links lock.
    private void linkAbove(MemorySegment key) {
        var higher = entries.higherEntry(key);
        if (higher != null) {
            higher.getValue().linkBelow(below(higher.getKey()));
        }
    }

    /// Takes the links lock, under which chains enter the ordered map and the links change, and sets how many chains
    /// the hold links one at a time.
    private void lockLinks() {
        linking.lock();
        linkedOrders = linkedOrders();
    }

    /// {@return whether it took the links lock, as [#lockLinks()] does, which another thread held otherwise}
    private boolean tryLockLinks() {
        if (!linking.tryLock()) {
            return false;
        }
        linkedOrders = linkedOrders();
        return true;
    }

    /// {@return the chains a hold of the links lock links one at a time, of a map whose chains are linked}
    private int linkedOrders() {
        return links == LINKED ? Math.max(LINKED_ORDERS, entries.size() / 8) : 0;
    }

    /// Relinks the ordered map if the hold left chains of it unlinked, or a relink failed, and lets the links lock go.
    private void unlockLinks() {
        try {
            if (links == RELINKING) {
                relink();
            }
        } finally {
            linking.unlock();
        }
    }

    /// {@return whether the chains of the ordered map are linked, which links them first unless another thread holds
    /// the links lock}
    ///
    /// The first descending scan of the key space links them, in one walk up the map; from then on the chains that
    /// enter link as they do.
    private boolean linked() {
        if (links == LINKED) {
            return true;
        }
        if (!tryLockLinks()) {
            return false;
        }
        try {
            if (links != LINKED) {
                relink();
            }
            return true;
        } finally {
            linking.unlock();
        }
    }

    /// Links every chain of the ordered map to the one below it, in one walk up the map, holding the links lock.
    ///
    /// No chain enters the map meanwhile, so a scan walking the links beside it follows old links and new ones, and
    /// either kind leads, through chains that left the map, to the next chain the map holds below. The chains that left
    /// before the walk are on no link once it ends, so their list is cleared first, with their own links.
    private void relink() {
        if (links == UNLINKED) {
            // From here on the chains that leave the map are listed, so none the walk links stays on the links unseen
            links = RELINKING;
        }
        relinkWanted = false;
        var cleared = 0;
        Chain gone;
        while ((gone = left.poll()) != null) {
            gone.linkBelow(null);
            cleared++;
        }
        leftCount.addAndGet(-cleared);
        var below = Chain.BOTTOM;
        for (var chain : entries.values()) {
            chain.linkBelow(below);
            below = chain;
        }
        links = LINKED;
        // Those that left during the walk: it may have linked a chain to them, or passed them by with their old links
        repairLeft(Integer.MAX_VALUE);
    }

    /// Points the links past the chains that left the ordered map, up to `limit` of them; holding the links lock. With
    /// [Integer#MAX_VALUE], when so many left that one walk up the map costs less, it relinks the map instead.
    private void skipLeft(int limit) {
        if (links != LINKED) {
            // Unlinked, no chain that left is listed; relinking, the hold relinks the map before it ends
            return;
        }
        if (limit == Integer.MAX_VALUE && (relinkWanted || (long) leftCount.get() * RELINK_RATIO >= entries.size())) {
            relink();
            return;
        }
        repairLeft(limit);
    }

    /// Points the link of the chain above each chain that left the ordered map, up to `limit` of them, at the chain
    /// the map holds below it, and clears the link of the chain that left; holding the links lock.
    ///
    /// A chain that left keeps its link until then, so the chains that left below it stay reachable through it, each
    /// through the one above it: cleared, what the links keep stays within the chains that wait for a repair. A scan
    /// standing on one that finds its link cleared asks the map.
    private void repairLeft(int limit) {
        var repaired = 0;
        Chain chain;
        while (repaired < limit && (chain = left.poll()) != null) {
            repaired++;
            var higher = entries.higherEntry(chain.key());
            if (higher != null) {
                var above = higher.getValue();
                var link = above.below();
                // A link to the chain that left, to one leaving, or none: it takes the chain the map holds below
                if (link == chain || link == null || link != Chain.BOTTOM && link.dropped()) {
                    above.linkBelow(below(higher.getKey()));
                }
            }
            chain.linkBelow(null);
        }
        leftCount.addAndGet(-repaired);
    }

    /// Takes `chain` out of the index and empties it, unless a commit given a write of the key has yet to apply it;
    /// [#removeDropped()] takes it out of the ordered map.
    ///
    /// The store drops a chain when the trees every reader reads hold what it had, or when it is empty; a reader that
    /// found the chain before then reads the tree, as a scan does for an empty chain still in the ordered map. A walk
    /// down that map would cost the publication of a small commit more than the rest of it.
    ///
    /// @return whether it dropped the chain
    boolean drop(Chain chain, Version head, LongSupplier latest) {
        // Read in this order: the claim, then the latest sequence, then the head
        var claimed = chain.claimed();
        if (claimed == Chain.DROPPED) {
            return true;
        }
        if (claimed > latest.getAsLong() || chain.head != head || !chain.release(claimed)) {
            return false;
        }
        empty(chain);
        return true;
    }

    /// Drops `chain`, which a rolled-back write at `sequence` left empty, unless a later write was claimed since.
    boolean dropRolledBack(Chain chain, long sequence) {
        if (chain.head != null || !chain.release(sequence)) {
            return false;
        }
        empty(chain);
        return true;
    }

    private void empty(Chain chain) {
        chain.markDropped();
        chain.head = null;
        index.remove(chain);
        // Read after the store above, as [#inserted] reads the flag after its own store: one of them takes it out
        if (chain.ordered()) {
            leaving.add(chain);
        } else if (lazy) {
            droppedListed.incrementAndGet();
        }
    }

    /// Takes up to `limit` of the dropped chains out of the ordered map; safe for concurrent use.
    ///
    /// A chain created since for the same key may have taken its place, and stays.
    void removeDropped(int limit) {
        Chain chain;
        for (var i = 0; i < limit && (chain = leaving.poll()) != null; i++) {
            removeLinked(chain);
        }
    }

    /// Takes `chain` out of the ordered map, and lists it among those the links may still reach if the key space links
    /// its chains.
    private void removeLinked(Chain chain) {
        // Read after the removal, as a relink writes it before its walk: if the walk linked the chain, this sees it
        if (entries.remove(chain.key(), chain) && links != UNLINKED) {
            leftCount.incrementAndGet();
            left.add(chain);
        }
    }

    /// Takes every chain the writer dropped out of the ordered map, on every processor when there are many, as there
    /// are after a checkpoint; safe for concurrent use.
    ///
    /// When they are most of the map, the next repair of the links relinks the map instead of the link above each.
    void removeDropped() {
        var chains = new ArrayList<Chain>();
        Chain chain;
        while ((chain = leaving.poll()) != null) {
            chains.add(chain);
        }
        if (chains.isEmpty()) {
            return;
        }
        var relinks = (long) chains.size() * RELINK_RATIO >= entries.size();
        // Each removal walks down the map, a few microseconds of cache misses at a million keys
        var stream = chains.size() < PARALLEL_REMOVALS ? chains.stream() : chains.parallelStream();
        if (relinks) {
            stream.forEach(dropped -> entries.remove(dropped.key(), dropped));
            // Read after the removals, as for one chain
            if (links != UNLINKED) {
                relinkWanted = true;
            }
        } else {
            stream.forEach(this::removeLinked);
        }
    }

    /// Lists `chain` among the keys the next checkpoint writes, holding the publishing role.
    void markDirty(Chain chain) {
        if (chain.dirtyCut != dirtyCut) {
            chain.dirtyCut = dirtyCut;
            dirty.add(chain);
        }
    }

    /// {@return the number [#markDirty] gives the chains it lists, the cut's that [#cutDirty()] takes next}
    ///
    /// Called holding the publishing role.
    long dirtyNumber() {
        return dirtyCut;
    }

    /// {@return the chains written since the last cut, and starts listing those of the next one}
    ///
    /// Called holding the publishing role. A key may appear in two chains, an old one dropped and the one
    /// created since.
    ArrayList<Chain> cutDirty() {
        var cut = dirty;
        dirty = new ArrayList<>();
        dirtyCut++;
        return cut;
    }

    /// {@return the version of `key` a reader with `bound` sees, [Version#HIDDEN] if it sees the key removed, or `null`
    /// if no version in memory is at or below its bound, so the key is read from the tree}
    ///
    /// @param head the newest version of `key`, or `null`
    /// @param key the key, which only the range removals read: `null` if there are none
    /// @param removals the range removals the reader loaded
    static Version resolve(Version head, MemorySegment key, long bound, RangeRemoval[] removals) {
        var version = head;
        while (version != null && version.stamp > bound) {
            version = version.older;
        }
        if (version == null) {
            return null;
        }
        if (version.removes()) {
            return Version.HIDDEN;
        }
        for (var removal : removals) {
            if (removal.stamp() > version.stamp && removal.stamp() <= bound && removal.covers(key)) {
                return Version.HIDDEN;
            }
        }
        return version;
    }

    /// {@return the version of the key whose bytes are `key` a reader with `bound` sees, as
    /// [#resolve(Version, MemorySegment, long, RangeRemoval\[\])] gives it}
    static Version resolve(Version head, byte[] key, long bound, RangeRemoval[] removals) {
        var version = head;
        while (version != null && version.stamp > bound) {
            version = version.older;
        }
        if (version == null) {
            return null;
        }
        if (version.removes()) {
            return Version.HIDDEN;
        }
        for (var removal : removals) {
            if (removal.stamp() > version.stamp && removal.stamp() <= bound && removal.covers(key)) {
                return Version.HIDDEN;
            }
        }
        return version;
    }

    /// {@return the version of `key` a reader with `bound` sees in memory, or `null` if it sees none there}
    ///
    /// @param head the newest version of `key`, or `null`
    /// @param removals the range removals the reader loaded
    static Version visible(Version head, MemorySegment key, long bound, RangeRemoval[] removals) {
        var version = resolve(head, key, bound, removals);
        return version == Version.HIDDEN ? null : version;
    }

    /// {@return the chains between `from`, included, and `to`, excluded, either `null` for no bound}
    ///
    /// It orders the listed chains first, so the map holds every chain a read of the range may need.
    NavigableMap<MemorySegment, Chain> slice(MemorySegment from, MemorySegment to) {
        orderListed();
        return range(from, to);
    }

    /// {@return a walk over the chains between `from`, included, and `to`, excluded, either `null` for no bound, in
    /// the order of their keys, or the reverse if `descending`}
    ///
    /// It orders the listed chains first, as [#slice] does. A descending walk of a key space that orders lazily steps
    /// along the links of the chains, which it links first if no descending walk did; while another thread links them,
    /// and for any other walk, it iterates the ordered map.
    Walk walk(MemorySegment from, MemorySegment to, boolean descending) {
        orderListed();
        if (descending && lazy && linked()) {
            return new Walk(entries, from, to);
        }
        var range = range(from, to);
        return new Walk((descending ? range.descendingMap() : range).entrySet().iterator());
    }

    /// {@return the chains of the ordered map between `from`, included, and `to`, excluded, either `null` for no bound}
    private NavigableMap<MemorySegment, Chain> range(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return Segments.compare(from, to) >= 0 ? entries.subMap(from, true, from, false) : entries.subMap(from, true, to, false);
        }
        if (from != null) {
            return entries.tailMap(from, true);
        }
        if (to != null) {
            return entries.headMap(to, false);
        }
        return entries;
    }

    /// {@return the number of entries a reader at `sequence` sees, leaving out the blind writes and the range removals
    /// counted later}
    long known(long sequence) {
        return counts.at(sequence);
    }

    /// {@return the number of entries `point` sees}
    ///
    /// It looks up in the trees of `point` the keys of the blind writes up to its sequence whose effect is not known
    /// yet, which reads the file for those of a tree larger than the page cache, and counts the range removals up to
    /// its sequence whose count is not known yet, which reads the nodes on the paths to their bounds.
    long count(ReadPoint point) {
        var sequence = point.bound() >>> Reads.OP_BITS;
        return known(sequence) + blind.correction(sequence, key -> Reads.inTreeOnly(this, key, point))
                + ranges.correction(sequence);
    }

    /// The chains of a range in memory, one at a time, in the order of a scan.
    ///
    /// A walk along the links steps from a chain to the one it links ([Chain#below()]), which is always [Chain#BOTTOM]
    /// or a chain of a lower key, so the walk takes it without comparing the two keys. As an iterator of the map does,
    /// it returns every chain the map held when it started that does not leave meanwhile, and may miss the chains that
    /// enter during the walk: those belong to commits after its reader's point, or replace dropped chains whose versions
    /// the reader's tree holds. It may return chains that left the map, emptied, whose keys the reader reads from the
    /// tree.
    static final class Walk {
        /// The bytes of a walk's first array of key copies, which holds the longest key a chain holds in its words.
        private static final int FIRST_COPIES = 8 * Chain.INLINE;
        /// The bytes of a walk's arrays of key copies, at most.
        private static final int LAST_COPIES = 4_096;
        // The map the walk asks for the chain below one with no link; null when it iterates it
        private final ConcurrentSkipListMap<MemorySegment, Chain> entries;
        // The map's entries, or null when the walk steps along the links
        private final Iterator<Map.Entry<MemorySegment, Chain>> iterator;
        // Where a walk along the links ends, or null for no bound
        private final MemorySegment from;
        private Chain chain;
        // The key of `chain`: the map's for a chain the walk took from the map, and otherwise made when first asked
        // for, as the key of a chain whose key is in its own words is a copy
        private MemorySegment key;
        // The array the walk copies the keys of chains that hold them in words of their own into, one after another,
        // and its read-only view: the walk's own, so the copies are garbage once the walk and its caller drop them,
        // where a copy in the key space's slab would stay reachable while a key of the map in its chunk did
        private byte[] copies;
        private MemorySegment copiesView;
        // The bytes of `copies` taken
        private int copied;
        // Whether the walk along the links returned the chain it starts at
        private boolean started;

        /// Makes a walk over the entries `iterator` returns.
        Walk(Iterator<Map.Entry<MemorySegment, Chain>> iterator) {
            this.entries = null;
            this.iterator = iterator;
            this.from = null;
        }

        /// Makes a walk down the linked chains of `entries` between `from`, included, and `to`, excluded, either `null`
        /// for no bound.
        Walk(ConcurrentSkipListMap<MemorySegment, Chain> entries, MemorySegment from, MemorySegment to) {
            this.entries = entries;
            this.iterator = null;
            this.from = from;
            var start = to == null ? entries.lastEntry() : entries.lowerEntry(to);
            if (start != null && (from == null || Segments.compare(start.getKey(), from) >= 0)) {
                this.chain = start.getValue();
                this.key = start.getKey();
            }
        }

        /// Moves to the next chain of the range.
        ///
        /// @return whether there is one, which [#chain()] and [#key()] then return
        boolean next() {
            if (iterator != null) {
                if (!iterator.hasNext()) {
                    chain = null;
                    key = null;
                    return false;
                }
                var entry = iterator.next();
                chain = entry.getValue();
                key = entry.getKey();
                return true;
            }
            if (!started) {
                started = true;
                return chain != null;
            }
            if (chain == null) {
                return false;
            }
            var below = chain.below();
            // A chain not linked yet, or one that left whose link a repair cleared: the map gives the chain below, so
            // the walk neither misses an entry nor returns one twice
            MemorySegment belowKey = null;
            if (below == null) {
                var lower = entries.lowerEntry(key());
                below = lower == null ? Chain.BOTTOM : lower.getValue();
                belowKey = lower == null ? null : lower.getKey();
            }
            assert below == Chain.BOTTOM || Chain.compareKeys(below, chain) < 0 : "A link leads up";
            if (below == Chain.BOTTOM || from != null && below.compareKey(from) < 0) {
                chain = null;
                key = null;
                return false;
            }
            chain = below;
            key = belowKey;
            return true;
        }

        /// {@return the chain the walk is at}
        Chain chain() {
            return chain;
        }

        /// {@return the key of the chain the walk is at, read-only}
        MemorySegment key() {
            var key = this.key;
            if (key == null && chain != null) {
                key = copy(chain);
                this.key = key;
            }
            return key;
        }

        // A read-only copy of the key of `chain`, reached through a link: a view of the segment that holds it, or a
        // span of the walk's array for a key in the chain's words, which costs a view and the key's bytes, as the map's
        // keys do, rather than an array and two views of its own
        private MemorySegment copy(Chain chain) {
            if (!chain.inlineKey()) {
                return chain.key();
            }
            var length = chain.keyLength();
            if (copies == null || copied + length > copies.length) {
                // A short scan takes a small array, a long one arrays up to 4 KiB, each holding at least one key
                copies = new byte[copies == null ? FIRST_COPIES : Math.min(LAST_COPIES, 2 * copies.length)];
                copiesView = MemorySegment.ofArray(copies).asReadOnly();
                copied = 0;
            }
            chain.copyKey(copies, copied);
            var copy = copiesView.asSlice(copied, length);
            copied += length;
            return copy;
        }
    }
}
