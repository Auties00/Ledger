package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// A removal of every key of a range, as of a stamp.
///
/// It hides every version of a covered key stamped before it from readers whose bound is at or past it. Once every
/// reader is past it the writer applies it to the entries in memory; it stays as a record while a reader may read a
/// tree checkpointed before it, or no checkpoint has made it durable, since the tree still holds the keys it removed.
final class RangeRemoval {
    static final RangeRemoval[] NONE = new RangeRemoval[0];

    private final MemorySegment from;
    private final MemorySegment to;
    private final long stamp;
    // Set by the writer once it has cut the versions the removal hides
    boolean applied;

    /// Creates a removal.
    ///
    /// @param from the lowest key removed, or `null` for no lower bound
    /// @param to the first key kept, or `null` for no upper bound
    /// @param stamp the stamp of the removal
    RangeRemoval(MemorySegment from, MemorySegment to, long stamp) {
        this.from = from;
        this.to = to;
        this.stamp = stamp;
    }

    /// {@return the lowest key removed, or `null` for no lower bound}
    MemorySegment from() {
        return from;
    }

    /// {@return the first key kept, or `null` for no upper bound}
    MemorySegment to() {
        return to;
    }

    /// {@return the stamp of the removal}
    long stamp() {
        return stamp;
    }

    /// {@return whether `key` is in the range}
    boolean covers(MemorySegment key) {
        return (from == null || Segments.compare(key, from) >= 0) && (to == null || Segments.compare(key, to) < 0);
    }
}
