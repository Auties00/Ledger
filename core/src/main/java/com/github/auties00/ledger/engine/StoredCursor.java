package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// A scan's walk over the entries a read finds below memory, in the scan's range and order: those of its tree
/// ([TreeCursor]), or those of a walk below with the entries of a run over them ([PackedRun.Cursor]): the tree the
/// store opened from with the writes the open replayed, or a tree with the runs of the bulk commits it lacks, one
/// cursor per run.
sealed interface StoredCursor permits TreeCursor, PackedRun.Cursor {
    /// Moves to the next entry in the cursor's order.
    ///
    /// @return whether there is one
    boolean next();

    /// {@return the key of the current entry, read-only}
    MemorySegment key();

    /// {@return a negative number, zero or a positive number as the current entry's key sorts before, with or after
    /// `key`}
    int compareKey(MemorySegment key);

    /// {@return the value of the current entry, read-only}
    MemorySegment value();

    /// Counts an entry the scan took, from the cursor or from memory, which bounds what the cursor reads ahead.
    void took();

    /// Gives back what the cursor read ahead and has not used.
    void release();
}
