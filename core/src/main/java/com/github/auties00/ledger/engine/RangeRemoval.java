package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// A removal of every key of a range, as of a stamp.
///
/// It hides every version of a covered key stamped before it from readers whose bound is at or past it, until the
/// writer applies it to the entries and drops it.
///
/// @param from the lowest key removed, or `null` for no lower bound
/// @param to the first key kept, or `null` for no upper bound
/// @param stamp the stamp of the removal
record RangeRemoval(MemorySegment from, MemorySegment to, long stamp) {
    static final RangeRemoval[] NONE = new RangeRemoval[0];

    /// {@return whether `key` is in the range}
    boolean covers(MemorySegment key) {
        return (from == null || Segments.compare(key, from) >= 0) && (to == null || Segments.compare(key, to) < 0);
    }
}
