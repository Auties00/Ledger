package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// Read-only segments of the keys of chains, for a reader of the ordered map of a key space that needs a key as a
/// segment: the map keys its entries by the chains themselves ([KeyspaceState#entries]).
///
/// The key of a chain that holds it in a segment is a view of that segment. One a chain holds in its words is copied
/// into an array of the reader's own, one key after another, so a copy costs a view and the key's bytes rather than an
/// array and two views of its own ([Chain#key()]). The copies are garbage once the reader drops them, where a copy in a
/// key space's [KeySlab] would stay reachable while any other copy in its chunk did.
///
/// A copy is never overwritten: a full array is followed by a new one, never rewound, so a copy stays valid as long as
/// its holder keeps it, as a scan keeps the key of a chain in memory while the tree's cursor moves past it. Not safe for
/// concurrent use: each reader makes its own.
final class KeyCopies {
    /// The bytes of the first array, which holds the longest key a chain holds in its words.
    private static final int FIRST_COPIES = 8 * Chain.INLINE;
    /// The bytes of an array, at most.
    private static final int LAST_COPIES = 4_096;

    // The array the keys are copied into, and its read-only view
    private byte[] copies;
    private MemorySegment view;
    // The bytes of `copies` taken
    private int copied;

    /// {@return the key of `chain`, read-only, valid for as long as the caller keeps it}
    MemorySegment of(Chain chain) {
        if (!chain.inlineKey()) {
            return chain.key();
        }
        var length = chain.keyLength();
        if (copies == null || copied + length > copies.length) {
            // A short scan takes a small array, a long one arrays up to 4 KiB, each holding at least one key
            copies = new byte[copies == null ? FIRST_COPIES : Math.min(LAST_COPIES, 2 * copies.length)];
            view = MemorySegment.ofArray(copies).asReadOnly();
            copied = 0;
        }
        chain.copyKey(copies, copied);
        var copy = view.asSlice(copied, length);
        copied += length;
        return copy;
    }
}
