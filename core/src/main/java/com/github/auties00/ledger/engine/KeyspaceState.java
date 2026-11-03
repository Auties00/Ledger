package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;

import java.lang.foreign.MemorySegment;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/// The entries of one key space: a version chain per key, the range removals not applied yet, and the number of
/// entries as of each recent commit.
///
/// Readers use it concurrently with the writer; only the writer changes it.
final class KeyspaceState {
    final Keyspace<?, ?> keyspace;
    final boolean resident;
    final ConcurrentSkipListMap<MemorySegment, Version> entries;
    // Copy-on-write: the writer publishes a new array, readers keep the one they loaded
    volatile RangeRemoval[] removals;
    // The number of entries visible as of each commit sequence that changed it
    final ConcurrentSkipListMap<Long, Long> counts;

    KeyspaceState(Keyspace<?, ?> keyspace) {
        this.keyspace = keyspace;
        this.resident = keyspace.residency() == Keyspace.Residency.RESIDENT;
        this.entries = new ConcurrentSkipListMap<>(Segments.UNSIGNED);
        this.removals = RangeRemoval.NONE;
        this.counts = new ConcurrentSkipListMap<>();
        counts.put(0L, 0L);
    }

    /// {@return the version of `key` a reader with `bound` sees, or `null` if it sees none}
    ///
    /// @param head the newest version of `key`, or `null`
    /// @param removals the range removals the reader loaded
    static Version visible(Version head, MemorySegment key, long bound, RangeRemoval[] removals) {
        var version = head;
        while (version != null && version.stamp > bound) {
            version = version.older;
        }
        if (version == null || version.removes()) {
            return null;
        }
        for (var removal : removals) {
            if (removal.stamp() > version.stamp && removal.stamp() <= bound && removal.covers(key)) {
                return null;
            }
        }
        return version;
    }

    /// {@return the entries between `from`, included, and `to`, excluded, either `null` for no bound}
    NavigableMap<MemorySegment, Version> slice(MemorySegment from, MemorySegment to) {
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
