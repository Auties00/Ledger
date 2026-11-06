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
    private static final VarHandle SEGMENT_SLOTS = MethodHandles.arrayElementVarHandle(Segment[].class);
    // Marks a slot whose chain was removed; it ends no probe
    private static final Chain TOMBSTONE = new Chain(MemorySegment.ofArray(new byte[0]), 0);
    // The table of every segment that holds no chain, which no one writes: a key space's segments took 12 KB of
    // tables before its first write, and kept them once a checkpoint had dropped every chain
    private static final Table EMPTY = Table.of(INITIAL);

    // Each made with the first chain its hashes take, so a key space of a few keys keeps a few: 64 up front, and the
    // locks beside them, cost every key space 3 KB
    private final Segment[] segments;

    ChainIndex() {
        this.segments = new Segment[SEGMENTS];
    }

    /// {@return the chain of the key of `hash` whose bytes are `key`, or `null` if the index holds none}
    Chain get(MemorySegment key, int hash) {
        var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, segmentOf(hash));
        if (segment == null) {
            return null;
        }
        var table = segment.table;
        var chains = table.chains;
        var hashes = table.hashes;
        var mask = chains.length - 1;
        for (var slot = slot(hash) & mask; ; slot = (slot + 1) & mask) {
            var chain = (Chain) SLOTS.getAcquire(chains, slot);
            if (chain == null) {
                return null;
            }
            // Written before the release store of the chain, so read after its acquire load
            if (hashes[slot] == hash && chain != TOMBSTONE && chain.keyEquals(key)) {
                return chain;
            }
        }
    }

    /// Adds `chain` unless the index holds a chain of the same key.
    ///
    /// @return the chain the index held for the key, or `null` if it took `chain`
    Chain putIfAbsent(Chain chain) {
        var segment = segmentFor(chain.hash);
        synchronized (segment) {
            while (true) {
                var table = segment.table;
                if (table == EMPTY) {
                    segment.table = Table.of(INITIAL);
                    continue;
                }
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
                    } else if (hashes[slot] == chain.hash && Chain.sameKey(held, chain)) {
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
        var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, segmentOf(chain.hash));
        if (segment == null) {
            return;
        }
        synchronized (segment) {
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
                    if (segment.size == 0) {
                        // A segment a checkpoint emptied keeps no table
                        segment.taken = 0;
                        segment.table = EMPTY;
                        return;
                    }
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
        for (var i = 0; i < SEGMENTS; i++) {
            var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, i);
            if (segment != null) {
                size += segment.size;
            }
        }
        return size;
    }

    private static int segmentOf(int hash) {
        return (hash >>> 26) & (SEGMENTS - 1);
    }

    /// {@return the segment of `hash`, made if no chain of its hashes came before}
    private Segment segmentFor(int hash) {
        var at = segmentOf(hash);
        var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, at);
        if (segment != null) {
            return segment;
        }
        var made = new Segment();
        var witness = (Segment) SEGMENT_SLOTS.compareAndExchangeRelease(segments, at, null, made);
        return witness == null ? made : witness;
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

    /// One part of the index: its table. Its writers lock the segment itself, which no code outside the index reaches.
    private static final class Segment {
        // Replaced whole when it grows; read by lookups without the lock
        private volatile Table table = EMPTY;
        // The chains held; written under the lock
        private volatile int size;
        // The slots that are not empty, tombstones included; guarded by the lock
        private int taken;

        /// Copies the chains into a table of four times as many slots as they need, without tombstones, and publishes
        /// it.
        private void rebuild() {
            if (size == 0) {
                taken = 0;
                table = EMPTY;
                return;
            }
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
