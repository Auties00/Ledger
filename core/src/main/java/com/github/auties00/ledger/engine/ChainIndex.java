package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// The chains of a key space by the bytes of their keys: the hash index of [KeyspaceState].
///
/// An open-addressing table of the chains themselves, split into segments by hash, with a tag of 8 bits of each slot's
/// hash in an array beside it, 0 for an empty slot. A map of the JDK holds a node and a key object per chain besides
/// the slot of its table, 56 bytes a buffered write, and a lookup follows both before it reaches the chain. Here a
/// lookup reads the tags of consecutive slots, and goes to a chain only when its tag matches, one in 255 for another
/// key's: a lookup of a key memory does not hold, the most common in a store larger than memory, reads a line of tags
/// and no chain. A table is at most three quarters full and doubles when it fills, five bytes a slot: hashes of 4 bytes
/// a slot in tables at most half full, which grew four times over, took 28 bytes a buffered write, 7 now.
///
/// Lookups take no lock; insertions and removals take their segment's lock, write a slot's tag and then publish its
/// chain with a release store, which a lookup that matches the tag loads with an acquire. A removal leaves a tombstone
/// and the tag, so no chain ever moves while readers probe past it, and a lookup never misses a chain that was in the
/// index when it started and still is; it may find a chain removed meanwhile, which a lookup of the JDK's maps may as
/// well. A segment that fills grows by building a new table and publishing it: a reader on the old one sees the chains
/// of the moment it was copied, and a chain inserted later is published by the commit that wrote it, which the reader
/// synchronizes with before it looks, and so after its tag.
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
        var tags = table.tags;
        var chains = table.chains;
        var mask = tags.length - 1;
        var tag = tag(hash);
        for (var slot = slot(hash) & mask; ; slot = (slot + 1) & mask) {
            var held = tags[slot];
            if (held == 0) {
                return null;
            }
            if (held == tag) {
                // Null while an insertion that wrote the tag has not published its chain, which this lookup need not see
                var chain = (Chain) SLOTS.getAcquire(chains, slot);
                if (chain != null && chain.hash == hash && chain != TOMBSTONE && chain.keyEquals(key)) {
                    return chain;
                }
            }
        }
    }

    /// Adds `chain` unless the index holds a chain of the same key.
    ///
    /// @return the chain the index held for the key, or `null` if it took `chain`
    Chain putIfAbsent(Chain chain) {
        var segment = segmentFor(chain.hash);
        var tag = tag(chain.hash);
        synchronized (segment) {
            while (true) {
                var table = segment.table;
                if (table == EMPTY) {
                    segment.table = Table.of(INITIAL);
                    continue;
                }
                var tags = table.tags;
                var chains = table.chains;
                var mask = tags.length - 1;
                var free = -1;
                for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                    var held = tags[slot];
                    if (held == 0) {
                        if (free < 0) {
                            free = slot;
                        }
                        break;
                    }
                    var other = chains[slot];
                    if (other == TOMBSTONE) {
                        if (free < 0) {
                            free = slot;
                        }
                    } else if (held == tag && other.hash == chain.hash && Chain.sameKey(other, chain)) {
                        return other;
                    }
                }
                var fresh = tags[free] == 0;
                if (fresh && (segment.taken + 1) * 4 > tags.length * 3) {
                    // A slot no tombstone frees would fill the table past three quarters: probes would lengthen
                    segment.rebuild();
                    continue;
                }
                if (fresh) {
                    segment.taken++;
                }
                tags[free] = tag;
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
            var table = segment.table;
            var tags = table.tags;
            var chains = table.chains;
            var mask = tags.length - 1;
            for (var slot = slot(chain.hash) & mask; ; slot = (slot + 1) & mask) {
                if (tags[slot] == 0) {
                    return;
                }
                if (chains[slot] == chain) {
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
                    // not keep the slots of its busiest moment, while a table that just grew, three eighths full, does
                    // not shrink back at the next removals
                    var tombstones = segment.taken - segment.size;
                    if ((tombstones > segment.size && tombstones * 4 > tags.length)
                            || (tags.length > INITIAL && segment.size * 16 < tags.length)) {
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

    // Bits 18 to 25 of the hash: below those that pick the segment and above those that pick the slot of a table of up
    // to 2^18 slots, which the low bits of the product above depend on alone; never 0, which marks an empty slot
    private static byte tag(int hash) {
        var tag = (hash >>> 18) & 0xFF;
        return (byte) (tag + ((tag - 1) >>> 31));
    }

    /// The slots of a segment, and the tags of their chains, published together.
    ///
    /// @param chains the chain of each slot, [#TOMBSTONE] for one removed, `null` for an empty one or one whose chain
    ///               is not published yet
    /// @param tags the tag of each slot's chain, written before the chain is, and kept by a tombstone; 0 for a slot
    ///             never taken
    private record Table(Chain[] chains, byte[] tags) {
        static Table of(int capacity) {
            return new Table(new Chain[capacity], new byte[capacity]);
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

        /// Copies the chains into a table at most half full, without tombstones, and publishes it: one that filled
        /// doubles, to three eighths full.
        private void rebuild() {
            if (size == 0) {
                taken = 0;
                table = EMPTY;
                return;
            }
            var old = table.chains;
            var capacity = INITIAL;
            while (capacity < (size + 1) * 2) {
                capacity <<= 1;
            }
            var fresh = Table.of(capacity);
            var mask = capacity - 1;
            for (var chain : old) {
                if (chain != null && chain != TOMBSTONE) {
                    var slot = slot(chain.hash) & mask;
                    while (fresh.tags[slot] != 0) {
                        slot = (slot + 1) & mask;
                    }
                    fresh.chains[slot] = chain;
                    fresh.tags[slot] = tag(chain.hash);
                }
            }
            taken = size;
            // The volatile store publishes the copied slots with the table
            table = fresh;
        }
    }
}
