package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

/// The versions of one key in memory, newest first, as both maps of its key space hold them.
///
/// A chain is created the first time a key is written since the store last dropped its chain, possibly by a commit
/// that has not taken the writer lock yet; only the writer attaches versions, under the lock. The writer drops a chain
/// once the trees hold every version it had, or when it is left empty; a reader that still holds a dropped chain reads
/// the tree instead, which holds what the chain had.
final class Chain {
    /// Orders chains as their keys: by [#prefix] first, which decides most comparisons without reading the keys.
    static final Comparator<Chain> BY_KEY = (left, right) -> {
        var order = Long.compareUnsigned(left.prefix, right.prefix);
        return order != 0 ? order : Segments.compare(left.key, right.key);
    };

    /// The key, in an array of its own.
    final MemorySegment key;
    /// The hash of the key's bytes, as the hash index uses it.
    final int hash;
    /// The first 8 bytes of the key, big-endian, zero after a shorter key's last.
    final long prefix;
    /// The newest version, or `null` before the writer attaches the first one and once it drops the chain.
    volatile Version head;
    /// Whether the ordered map holds the chain: the writer attaches no version before it does, so every committed
    /// version a scan may see is in the map the scan walks.
    volatile boolean ordered;
    /// Whether the writer took the chain out of the maps; no version is attached to it again.
    volatile boolean dropped;
    /// The cut whose dirty keys list the chain, so a chain is listed once per checkpoint; guarded by the writer lock.
    long dirtyCut;

    Chain(MemorySegment key, int hash) {
        this.key = key;
        this.hash = hash;
        this.prefix = prefix(key);
        this.dirtyCut = -1;
    }

    private static long prefix(MemorySegment key) {
        var prefix = 0L;
        var length = (int) Math.min(Long.BYTES, key.byteSize());
        for (var i = 0; i < length; i++) {
            prefix |= (key.get(ValueLayout.JAVA_BYTE, i) & 0xFFL) << (56 - 8 * i);
        }
        return prefix;
    }
}
