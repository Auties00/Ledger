package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/// The entries of one key space in memory: a version chain per key, the range removals readers may still need, and
/// the number of entries as of each recent commit.
///
/// In a store in a directory, the entries of a [Keyspace.Residency#PAGED] key space are only those written since the
/// checkpoint every reader's tree holds; a key with no version in memory is read from the tree. A resident key space
/// holds every entry.
///
/// The chains of the keys are in two maps: an ordered one for scans, and a hash index ([ChainIndex]) for lookups of
/// one key, which at a million keys costs a fraction of a walk down the ordered one. A chain enters the index first, then the ordered
/// map, and leaves them in the same order.
///
/// A paged key space of a store in a directory orders its chains lazily: a new chain is only listed, and the reads of
/// a range order the listed ones before they walk the map ([#slice]). Its chains leave memory once a checkpoint takes
/// them, so with no read of a range in between, as when a store takes writes it does not scan, the walk down the map
/// to insert each new key, and the one to remove it, are never paid; a commit that wrote a new key paid both.
///
/// Readers use it concurrently with the commits. Commits create chains before they are given a sequence; everything
/// else changes only under it.
final class KeyspaceState {
    /// The dropped chains from which [#removeDropped()] spreads the removals over the processors.
    private static final int PARALLEL_REMOVALS = 4_096;

    final Keyspace<?, ?> keyspace;
    final boolean resident;
    // The id the log knows the key space by, set while the store opens
    int id;
    // The chains in the order of their keys, each keyed by a chain of its key: the one that entered its node, which
    // stays its key when a later chain of the key replaces a dropped one, as their bytes are the same. A segment of
    // each key as the map's key was 40 bytes of every resident entry. A chain the writer dropped may stay a while,
    // empty
    final ConcurrentSkipListMap<Chain, Chain> entries;
    // The chains by the bytes of their keys
    private final ChainIndex index;
    // The chains the writer dropped that the ordered map may still hold
    private final ConcurrentLinkedQueue<Chain> leaving;
    // Copy-on-write: the writer publishes a new array, readers keep the one they loaded
    volatile RangeRemoval[] removals;
    // The number of entries visible as of each commit sequence that changed it, leaving out the blind writes
    final CountHistory counts;
    // The writes whose effect on the count the trees settle
    final BlindWrites blind;
    // The copies of the keys the chains hold
    private final KeySlab keys;
    // The chains written since the last checkpoint was cut, each listed once per cut; guarded by the publishing role
    ArrayList<Chain> dirty;
    // The number of the cut `dirty` collects for, guarded by the publishing role
    private int dirtyCut;
    // The bytes from which a value a batch puts goes to the file of large values, set while the store opens
    long largeValues = Long.MAX_VALUE;
    // Whether the key space orders its chains lazily, set while the store opens, before any chain is opened
    boolean lazy;
    // The chains opened and not yet ordered, newest first, linked through Chain.nextUnordered
    @SuppressWarnings("unused")
    private volatile Chain unordered;
    // Held while the listed chains are ordered or pruned, so a read of a range that finds the list empty waits for
    // the chains another thread took from it
    private final ReentrantLock ordering;

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
        this.entries = new ConcurrentSkipListMap<>(Chain.BY_KEY);
        this.index = new ChainIndex();
        this.leaving = new ConcurrentLinkedQueue<>();
        this.removals = RangeRemoval.NONE;
        this.counts = new CountHistory(0, 0);
        this.blind = new BlindWrites();
        this.dirty = new ArrayList<>();
        this.keys = new KeySlab();
        this.ordering = new ReentrantLock();
    }

    /// {@return the chain of `key`, or `null` if memory holds none}
    Chain chain(MemorySegment key) {
        return index.get(key, Segments.hash(key));
    }

    /// {@return the chains memory holds, ordered or not}
    int chains() {
        return index.size();
    }

    /// {@return the newest version of `key` in memory, or `null`}
    Version head(MemorySegment key) {
        var chain = chain(key);
        return chain == null ? null : chain.head;
    }

    /// {@return the chain of `key`, created if memory holds none, and in the ordered map}
    ///
    /// Safe for concurrent use: commits call it for the keys they are about to write, so the ordered map, the costly
    /// one, grows before their turn. A chain dropped meanwhile is returned dropped; the sequencer, which finds it so
    /// when it gives the commit its writes, opens the key again.
    Chain open(MemorySegment key) {
        var hash = Segments.hash(key);
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
                    list(created);
                    return created;
                }
            }
        }
        if (!lazy && !chain.ordered()) {
            order(chain);
        }
        return chain;
    }

    /// Lists `chain` among those a read of a range orders first.
    private void list(Chain chain) {
        while (true) {
            var head = unordered;
            chain.nextUnordered = head;
            if (UNORDERED.compareAndSet(this, head, chain)) {
                return;
            }
        }
    }

    /// Orders every listed chain that is not dropped, so the ordered map holds every chain a read of a range may need;
    /// safe for concurrent use.
    private void orderListed() {
        // Both read in this order: a thread that takes the list holds the lock until it ordered what it took
        if (unordered == null && !ordering.isLocked()) {
            return;
        }
        ordering.lock();
        try {
            var chain = (Chain) UNORDERED.getAndSet(this, null);
            while (chain != null) {
                var next = chain.nextUnordered;
                chain.nextUnordered = null;
                order(chain);
                chain = next;
            }
        } finally {
            ordering.unlock();
        }
    }

    /// Takes the dropped chains off the list, once a checkpoint dropped most of those it holds, and orders the chains
    /// a prune left that no commit wrote since `cut`, so the sweep, which walks the ordered map, shortens them; safe
    /// for concurrent use.
    ///
    /// @param cut the number of the cut whose keys the prune went through, as [#markDirty] gave it to the chains
    void pruneListed(long cut) {
        if (unordered == null) {
            return;
        }
        ordering.lock();
        try {
            var chain = (Chain) UNORDERED.getAndSet(this, null);
            Chain first = null;
            Chain last = null;
            while (chain != null) {
                var next = chain.nextUnordered;
                chain.nextUnordered = null;
                if (!chain.dropped() && !chain.released()) {
                    // A plain read off the publishing role: a chain marked meanwhile is only ordered early
                    if (chain.dirtyCut <= cut) {
                        order(chain);
                    } else {
                        if (first == null) {
                            first = chain;
                        } else {
                            last.nextUnordered = chain;
                        }
                        last = chain;
                    }
                }
                chain = next;
            }
            if (first != null) {
                while (true) {
                    var head = unordered;
                    last.nextUnordered = head;
                    if (UNORDERED.compareAndSet(this, head, first)) {
                        break;
                    }
                }
            }
        } finally {
            ordering.unlock();
        }
    }

    /// Inserts `chain` into the ordered map, in place of a dropped chain of the same key, unless it is dropped.
    private void order(Chain chain) {
        // Released counts as dropped on both sides: another thread may have used the chain, and a prune dropped it,
        // while this one was still inserting it; a later chain of the key then holds the place
        while (!chain.dropped() && !chain.released()) {
            var previous = entries.putIfAbsent(chain, chain);
            if (previous == null || previous == chain) {
                inserted(chain);
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
            if (entries.replace(chain, previous, chain)) {
                inserted(chain);
                return;
            }
        }
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
        }
    }

    /// Records that the ordered map holds `chain`, and takes it out again if it was dropped meanwhile, unseen by
    /// [#empty], which lists for removal only the chains it finds ordered.
    private void inserted(Chain chain) {
        chain.markOrdered();
        if (chain.dropped()) {
            entries.remove(chain, chain);
        }
    }

    /// Takes up to `limit` of the dropped chains out of the ordered map; safe for concurrent use.
    ///
    /// A chain created since for the same key may have taken its place, and stays.
    void removeDropped(int limit) {
        Chain chain;
        for (var i = 0; i < limit && (chain = leaving.poll()) != null; i++) {
            entries.remove(chain, chain);
        }
    }

    /// Takes every chain the writer dropped out of the ordered map, on every processor when there are many, as there
    /// are after a checkpoint; safe for concurrent use.
    void removeDropped() {
        var chains = new ArrayList<Chain>();
        Chain chain;
        while ((chain = leaving.poll()) != null) {
            chains.add(chain);
        }
        // Each removal walks down the map, a few microseconds of cache misses at a million keys
        var stream = chains.size() < PARALLEL_REMOVALS ? chains.stream() : chains.parallelStream();
        stream.forEach(dropped -> entries.remove(dropped, dropped));
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
    NavigableMap<Chain, Chain> slice(MemorySegment from, MemorySegment to) {
        orderListed();
        var low = from == null ? null : Chain.probe(from);
        var high = to == null ? null : Chain.probe(to);
        if (low != null && high != null) {
            return Segments.compare(from, to) >= 0 ? entries.subMap(low, true, low, false) : entries.subMap(low, true, high, false);
        }
        if (low != null) {
            return entries.tailMap(low, true);
        }
        if (high != null) {
            return entries.headMap(high, false);
        }
        return entries;
    }

    /// {@return the number of entries a reader at `sequence` sees, leaving out the blind writes}
    long known(long sequence) {
        return counts.at(sequence);
    }

    /// {@return the number of entries `point` sees}
    ///
    /// It looks up in the trees of `point` the keys of the blind writes up to its sequence whose effect is not known
    /// yet, which reads the file for those of a tree larger than the page cache.
    long count(ReadPoint point) {
        var sequence = point.bound() >>> Reads.OP_BITS;
        return known(sequence) + blind.correction(sequence, key -> Reads.inTreeOnly(this, key, point));
    }
}
