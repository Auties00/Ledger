package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;

/// The entries of one key space in memory: a version chain per key, the range removals readers may still need, and
/// the number of entries as of each recent commit.
///
/// In a store in a directory, the entries of a [Keyspace.Residency#PAGED] key space are only those written since the
/// checkpoint every reader's tree holds; a key with no version in memory is read from the tree. A resident key space
/// holds every entry.
///
/// The chains of the keys are in two maps: an ordered one for scans, and a hash index for lookups of one key, which
/// at a million keys costs a fraction of a walk down the ordered one. A chain enters the index first, then the ordered
/// map, and leaves them in the same order.
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
    // The chains in the order of their keys; a chain the writer dropped may stay a while, empty
    final ConcurrentSkipListMap<MemorySegment, Chain> entries;
    // The chains by the bytes of their keys
    private final ConcurrentHashMap<IndexKey, Chain> index;
    // The chains the writer dropped that the ordered map may still hold
    private final ConcurrentLinkedQueue<Chain> leaving;
    // Copy-on-write: the writer publishes a new array, readers keep the one they loaded
    volatile RangeRemoval[] removals;
    // The number of entries visible as of each commit sequence that changed it, leaving out the blind writes
    final ConcurrentSkipListMap<Long, Long> counts;
    // The writes whose effect on the count the trees settle
    final BlindWrites blind;
    // The chains written since the last checkpoint was cut, each listed once per cut; guarded by the publishing role
    ArrayList<Chain> dirty;
    // The number of the cut `dirty` collects for, guarded by the publishing role
    private long dirtyCut;
    // The bytes from which a value a batch puts goes to the file of large values, set while the store opens
    long largeValues = Long.MAX_VALUE;

    KeyspaceState(Keyspace<?, ?> keyspace) {
        this.keyspace = keyspace;
        this.resident = keyspace.residency() == Keyspace.Residency.RESIDENT;
        this.id = -1;
        this.entries = new ConcurrentSkipListMap<>(Segments.UNSIGNED);
        this.index = new ConcurrentHashMap<>();
        this.leaving = new ConcurrentLinkedQueue<>();
        this.removals = RangeRemoval.NONE;
        this.counts = new ConcurrentSkipListMap<>();
        this.blind = new BlindWrites();
        this.dirty = new ArrayList<>();
        counts.put(0L, 0L);
    }

    /// {@return the chain of `key`, or `null` if memory holds none}
    Chain chain(MemorySegment key) {
        return index.get(new IndexKey(key, Segments.hash(key)));
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
        var chain = index.get(new IndexKey(key, hash));
        if (chain != null && chain.released()) {
            // Dropped, and maybe not out of the index yet: the key gets a chain of its own
            index.remove(new IndexKey(chain.key, chain.hash), chain);
            chain = null;
        }
        if (chain == null) {
            var created = new Chain(Segments.copy(key), hash);
            chain = index.putIfAbsent(new IndexKey(created.key, hash), created);
            if (chain == null) {
                chain = created;
            }
        }
        if (!chain.ordered) {
            order(chain);
        }
        return chain;
    }

    /// Inserts `chain` into the ordered map, in place of a dropped chain of the same key, unless it is dropped.
    private void order(Chain chain) {
        // Released counts as dropped on both sides: another thread may have used the chain, and a prune dropped it,
        // while this one was still inserting it; a later chain of the key then holds the place
        while (!chain.dropped && !chain.released()) {
            var previous = entries.putIfAbsent(chain.key, chain);
            if (previous == null || previous == chain) {
                chain.ordered = true;
                return;
            }
            // A chain being dropped is released before it is marked dropped
            if (!previous.dropped && !previous.released()) {
                if (chain.released()) {
                    // Dropped since the check above, and replaced: the later chain is the key's
                    return;
                }
                throw new IllegalStateException("Two chains hold one key of " + keyspace.name());
            }
            if (entries.replace(chain.key, previous, chain)) {
                chain.ordered = true;
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
        chain.dropped = true;
        chain.head = null;
        index.remove(new IndexKey(chain.key, chain.hash), chain);
        leaving.add(chain);
    }

    /// Takes up to `limit` of the dropped chains out of the ordered map; safe for concurrent use.
    ///
    /// A chain created since for the same key may have taken its place, and stays.
    void removeDropped(int limit) {
        Chain chain;
        for (var i = 0; i < limit && (chain = leaving.poll()) != null; i++) {
            entries.remove(chain.key, chain);
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
        stream.forEach(dropped -> entries.remove(dropped.key, dropped));
    }

    /// Lists `chain` among the keys the next checkpoint writes, holding the publishing role.
    void markDirty(Chain chain) {
        if (chain.dirtyCut != dirtyCut) {
            chain.dirtyCut = dirtyCut;
            dirty.add(chain);
        }
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

    /// A key of the hash index: the bytes of a key, compared by content.
    ///
    /// @param bytes the key
    /// @param hash [Segments#hash(MemorySegment)] of the key
    private record IndexKey(MemorySegment bytes, int hash) {
        @Override
        public boolean equals(Object other) {
            return other instanceof IndexKey key && key.hash == hash && Segments.compare(key.bytes, bytes) == 0;
        }

        @Override
        public int hashCode() {
            return hash;
        }
    }

    /// {@return the version of `key` a reader with `bound` sees, [Version#HIDDEN] if it sees the key removed, or `null`
    /// if no version in memory is at or below its bound, so the key is read from the tree}
    ///
    /// @param head the newest version of `key`, or `null`
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
    NavigableMap<MemorySegment, Chain> slice(MemorySegment from, MemorySegment to) {
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

    /// {@return the number of entries a reader at `sequence` sees, leaving out the blind writes}
    long known(long sequence) {
        var entry = counts.floorEntry(sequence);
        return entry == null ? 0 : entry.getValue();
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
