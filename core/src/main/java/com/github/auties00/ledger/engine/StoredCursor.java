package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// A scan's walk over the entries a read finds below memory, in the scan's range and order: those of its tree
/// ([TreeCursor]), or those of the tree the store opened from with the writes the open replayed over them
/// ([PackedRun.Cursor]).
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

    /// {@return the key of the current entry in a new array, copied out of the array that holds it}
    ///
    /// The copy fills the new array whole, so it is not zeroed first, as a copy of the segment [#key()] returns would
    /// have it.
    byte[] keyBytes();

    /// {@return the value of the current entry in a new array, read as [#value()] reads it and copied out of the array
    /// that holds it}
    ///
    /// It reads and caches what [#value()] would, so a scan that takes one or the other of each entry leaves the page
    /// cache and its reads ahead as they would be.
    byte[] valueBytes();

    /// Counts an entry the scan took, from the cursor or from memory, which bounds what the cursor reads ahead.
    void took();

    /// Gives back what the cursor read ahead and has not used.
    void release();
}
