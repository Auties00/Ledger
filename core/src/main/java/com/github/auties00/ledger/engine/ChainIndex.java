package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// The chains of a key space by the bytes of their keys: the hash index of [KeyspaceState].
///
/// An open-addressing table of the chains themselves, split into segments by hash. A map of the JDK holds a node and a
/// key object per chain besides the slot of its table, 56 bytes a buffered write, and a lookup follows both before it
/// reaches the chain: here a lookup goes from the slot to the chain, which holds its key and hash.
///
/// Lookups take no lock and read the slots with acquire loads; insertions and removals take their segment's lock and
/// publish a slot with a release store. A removal leaves a tombstone, so no chain ever moves while readers probe past
/// it, and a lookup never misses a chain that was in the index when it started and still is; it may find a chain
/// removed meanwhile, which a lookup of the JDK's maps may as well. A segment that fills grows by building a new table
/// and publishing it: a reader on the old one sees the chains of the moment it was copied, and a chain inserted later
/// is published by the commit that wrote it, which the reader synchronizes with before it looks.
final class ChainIndex {
    private static final int SEGMENTS = 64;
    private static final int INITIAL = 16;
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(Chain[].class);
    // Marks a slot whose chain was removed; it ends no probe
    private static final Chain TOMBSTONE = new Chain(MemorySegment.ofArray(new byte[0]), 0);

    private final Segment[] segments;

    ChainIndex() {
        this.segments = new Segment[SEGMENTS];
        for (var i = 0; i < SEGMENTS; i++) {
            segments[i] = new Segment();
        }
    }

    /// {@return the chain of the key of `hash` whose bytes are `key`, or `null` if the index holds none}
    Chain get(MemorySegment key, int hash) {
        var table = segment(hash).table;
        var mask = table.length - 1;
        for (var slot = slot(hash) & mask; ; slot = (slot + 1) & mask) {
            var chain = (Chain) SLOTS.getAcquire(table, slot);
            if (chain == null) {
                return null;
            }
            if (chain != TOMBSTONE && chain.hash == hash && Segments.equal(chain.key, key)) {
                return chain;
            }
        }
    }

    /// Adds `chain` unless the index holds a chain of the same key.
    ///
    /// @return the chain the index held for the key, or `null` if it took `chain`
    Chain putIfAbsent(Chain chain) {
        var segment = segment(chain.hash);
        synchronized (segment.lock) {
            while (true) {
                var table = segment.table;
                var mask = table.length - 1;
                var free = -1;
                for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                    var held = table[slot];
                    if (held == null) {
                        if (free < 0) {
                            free = slot;
                        }
                        break;
                    }
                    if (held == TOMBSTONE) {
                        if (free < 0) {
                            free = slot;
                        }
                    } else if (held.hash == chain.hash && Segments.equal(held.key, chain.key)) {
                        return held;
                    }
                }
                if (table[free] == null && (segment.taken + 1) * 4 > table.length * 3) {
                    // A slot no tombstone frees would fill the table past three quarters: probes would lengthen, and
                    // one always needs an empty slot to end at
                    segment.rebuild();
                    continue;
                }
                if (table[free] == null) {
                    segment.taken++;
                }
                SLOTS.setRelease(table, free, chain);
                segment.size++;
                return null;
            }
        }
    }

    /// Takes `chain` itself out of the index, if it is there.
    void remove(Chain chain) {
        var segment = segment(chain.hash);
        synchronized (segment.lock) {
            var table = segment.table;
            var mask = table.length - 1;
            for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                var held = table[slot];
                if (held == null) {
                    return;
                }
                if (held == chain) {
                    SLOTS.setRelease(table, slot, TOMBSTONE);
                    segment.size--;
                    // Tombstones end no probe: once they outnumber the chains, and fill a quarter of the table, they
                    // go, and with them the length a churn of insertions and removals left the table at
                    var tombstones = segment.taken - segment.size;
                    if (tombstones > segment.size && tombstones * 4 > table.length) {
                        segment.rebuild();
                    }
                    return;
                }
            }
        }
    }

    /// {@return the number of chains the index holds, as of some moment of the call}
    int size() {
        var size = 0;
        for (var segment : segments) {
            size += segment.size;
        }
        return size;
    }

    private Segment segment(int hash) {
        return segments[(hash >>> 26) & (SEGMENTS - 1)];
    }

    // Orders the slots by the hash's low bits, apart from the high ones that pick the segment; it separates no keys the
    // hash does not, which the seed of the hash keeps others from choosing
    private static int slot(int hash) {
        return hash * 0x9E3779B9;
    }

    /// One part of the index: its table, and the lock its writers take.
    private static final class Segment {
        private final Object lock = new Object();
        // Replaced whole when it grows; read by lookups without the lock
        private volatile Chain[] table = new Chain[INITIAL];
        // The chains held; written under the lock
        private volatile int size;
        // The slots that are not empty, tombstones included; guarded by the lock
        private int taken;

        /// Copies the chains into a table of twice as many slots as they need, without tombstones, and publishes it.
        private void rebuild() {
            var old = table;
            var capacity = INITIAL;
            while (capacity < (size + 1) * 2) {
                capacity <<= 1;
            }
            var fresh = new Chain[capacity];
            var mask = capacity - 1;
            for (var chain : old) {
                if (chain != null && chain != TOMBSTONE) {
                    var slot = slot(chain.hash) & mask;
                    while (fresh[slot] != null) {
                        slot = (slot + 1) & mask;
                    }
                    fresh[slot] = chain;
                }
            }
            taken = size;
            // The volatile store publishes the copied slots with the table
            table = fresh;
        }
    }
}
