package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;

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
/// Readers use it concurrently with the writer. Commits may create chains before they take the writer lock; everything
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
    // The number of entries visible as of each commit sequence that changed it
    final ConcurrentSkipListMap<Long, Long> counts;
    // The chains written since the last checkpoint was cut, each listed once per cut; guarded by the writer lock
    ArrayList<Chain> dirty;
    // The number of the cut `dirty` collects for, guarded by the writer lock
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
    /// Safe without the writer lock: commits call it for the keys they are about to write, so the ordered map, the
    /// costly one, grows outside the lock. A chain the writer drops meanwhile is returned dropped; the writer, which
    /// calls it again under the lock, never gets one.
    Chain open(MemorySegment key) {
        var hash = Segments.hash(key);
        var chain = index.get(new IndexKey(key, hash));
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
        while (!chain.dropped) {
            var previous = entries.putIfAbsent(chain.key, chain);
            if (previous == null || previous == chain) {
                chain.ordered = true;
                return;
            }
            if (!previous.dropped) {
                throw new IllegalStateException("Two chains hold one key of " + keyspace.name());
            }
            if (entries.replace(chain.key, previous, chain)) {
                chain.ordered = true;
                return;
            }
        }
    }

    /// Takes `chain` out of the index and empties it, under the writer lock; [#removeDropped()] takes it out of the
    /// ordered map.
    ///
    /// The writer drops a chain when the trees every reader reads hold what it had, or when it is empty; a reader that
    /// found the chain before then reads the tree, as a scan does for an empty chain still in the ordered map. A walk
    /// down that map would cost the writer lock more than the rest of a small commit.
    void drop(Chain chain) {
        chain.dropped = true;
        chain.head = null;
        index.remove(new IndexKey(chain.key, chain.hash), chain);
        leaving.add(chain);
    }

    /// Takes up to `limit` of the chains the writer dropped out of the ordered map; safe without the writer lock.
    ///
    /// A chain created since for the same key may have taken its place, and stays.
    void removeDropped(int limit) {
        Chain chain;
        for (var i = 0; i < limit && (chain = leaving.poll()) != null; i++) {
            entries.remove(chain.key, chain);
        }
    }

    /// Takes every chain the writer dropped out of the ordered map, on every processor when there are many, as there
    /// are after a checkpoint; safe without the writer lock.
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

    /// Lists `chain` among the keys the next checkpoint writes, under the writer lock.
    void markDirty(Chain chain) {
        if (chain.dirtyCut != dirtyCut) {
            chain.dirtyCut = dirtyCut;
            dirty.add(chain);
        }
    }

    /// {@return the chains written since the last cut, and starts listing those of the next one}
    ///
    /// Called under the writer lock. A key may appear in two chains, an old one the writer dropped and the one it
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

    /// {@return the number of entries a reader at `sequence` sees}
    long count(long sequence) {
        var entry = counts.floorEntry(sequence);
        return entry == null ? 0 : entry.getValue();
    }
}
