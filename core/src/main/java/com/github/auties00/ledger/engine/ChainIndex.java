package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// The chains of a key space by the bytes of their keys: the hash index of [KeyspaceState].
///
/// An open-addressing table of the chains themselves, split into segments by hash, with the hash of each slot's chain
/// in an array beside it. A map of the JDK holds a node and a key object per chain besides the slot of its table, 56
/// bytes a buffered write, and a lookup follows both before it reaches the chain. Here a lookup reads the hashes of
/// consecutive slots, and goes to a chain only when its hash matches: a lookup of a key memory does not hold, the most
/// common in a store larger than memory, reads a line or two of hashes. A table is at most half full, where a lookup
/// of a missing key passes about two taken slots.
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
        var chains = table.chains;
        var hashes = table.hashes;
        var mask = chains.length - 1;
        for (var slot = slot(hash) & mask; ; slot = (slot + 1) & mask) {
            var chain = (Chain) SLOTS.getAcquire(chains, slot);
            if (chain == null) {
                return null;
            }
            // Written before the release store of the chain, so read after its acquire load
            if (hashes[slot] == hash && chain != TOMBSTONE && Segments.equal(chain.key, key)) {
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
                var chains = table.chains;
                var hashes = table.hashes;
                var mask = chains.length - 1;
                var free = -1;
                for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                    var held = chains[slot];
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
                    } else if (hashes[slot] == chain.hash && Segments.equal(held.key, chain.key)) {
                        return held;
                    }
                }
                if (chains[free] == null && (segment.taken + 1) * 2 > chains.length) {
                    // A slot no tombstone frees would fill the table past half: probes would lengthen
                    segment.rebuild();
                    continue;
                }
                if (chains[free] == null) {
                    segment.taken++;
                }
                hashes[free] = chain.hash;
                SLOTS.setRelease(chains, free, chain);
                segment.size++;
                return null;
            }
        }
    }

    /// Takes `chain` itself out of the index, if it is there.
    void remove(Chain chain) {
        var segment = segment(chain.hash);
        synchronized (segment.lock) {
            var chains = segment.table.chains;
            var mask = chains.length - 1;
            for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                var held = chains[slot];
                if (held == null) {
                    return;
                }
                if (held == chain) {
                    SLOTS.setRelease(chains, slot, TOMBSTONE);
                    segment.size--;
                    // Tombstones end no probe: once they outnumber the chains, and fill a quarter of the table, they
                    // go; and a table a sixteenth full shrinks, so a key space whose chains a checkpoint dropped does
                    // not keep the slots of its busiest moment, while a table that just grew, an eighth full, does not
                    // shrink back at the next removals
                    var tombstones = segment.taken - segment.size;
                    if ((tombstones > segment.size && tombstones * 4 > chains.length)
                            || (chains.length > INITIAL && segment.size * 16 < chains.length)) {
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

    /// The slots of a segment, and the hashes of their chains, published together.
    ///
    /// @param chains the chain of each slot, [#TOMBSTONE] for one removed, `null` for an empty one
    /// @param hashes the hash of each slot's chain, written before the chain is
    private record Table(Chain[] chains, int[] hashes) {
        static Table of(int capacity) {
            return new Table(new Chain[capacity], new int[capacity]);
        }
    }

    /// One part of the index: its table, and the lock its writers take.
    private static final class Segment {
        private final Object lock = new Object();
        // Replaced whole when it grows; read by lookups without the lock
        private volatile Table table = Table.of(INITIAL);
        // The chains held; written under the lock
        private volatile int size;
        // The slots that are not empty, tombstones included; guarded by the lock
        private int taken;

        /// Copies the chains into a table of four times as many slots as they need, without tombstones, and publishes
        /// it.
        private void rebuild() {
            var old = table.chains;
            var capacity = INITIAL;
            while (capacity < (size + 1) * 4) {
                capacity <<= 1;
            }
            var fresh = Table.of(capacity);
            var mask = capacity - 1;
            for (var chain : old) {
                if (chain != null && chain != TOMBSTONE) {
                    var slot = slot(chain.hash) & mask;
                    while (fresh.chains[slot] != null) {
                        slot = (slot + 1) & mask;
                    }
                    fresh.chains[slot] = chain;
                    fresh.hashes[slot] = chain.hash;
                }
            }
            taken = size;
            // The volatile store publishes the copied slots with the table
            table = fresh;
        }
    }
}
