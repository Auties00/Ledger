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
///
/// The index counts the segments that hold a table of their own, so a key space with no chain, as a paged one is once a
/// checkpoint took them all, answers [#holdsNone()] without hashing the key. A segment counts from before the first
/// chain of its table is published until after the table is dropped, so a reader that synchronized with the commit of a
/// chain the index still holds never reads 0: every segment adds to the count before it takes from it, in the order of
/// the count's own changes.
final class ChainIndex {
    private static final int SEGMENTS = 64;
    private static final int INITIAL = 16;
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(Chain[].class);
    private static final VarHandle SEGMENT_SLOTS = MethodHandles.arrayElementVarHandle(Segment[].class);
    private static final VarHandle OCCUPIED;
    // Marks a slot whose chain was removed; it ends no probe
    private static final Chain TOMBSTONE = new Chain(MemorySegment.ofArray(new byte[0]), 0);
    // The table of every segment that holds no chain, which no one writes: a key space's segments took 12 KB of
    // tables before its first write, and kept them once a checkpoint had dropped every chain
    private static final Table EMPTY = Table.of(INITIAL);

    static {
        try {
            OCCUPIED = MethodHandles.lookup().findVarHandle(ChainIndex.class, "occupied", int.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    // Each made with the first chain its hashes take, so a key space of a few keys keeps a few: 64 up front, and the
    // locks beside them, cost every key space 3 KB
    private final Segment[] segments;
    // The segments whose table is not EMPTY, changed through setTable alone
    @SuppressWarnings("unused")
    private volatile int occupied;

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
        return find(table, slot(hash) & (table.tags.length - 1), key, hash);
    }

    /// {@return the chain of the key of `hash` whose bytes are `key` that `table` holds, probing from `slot`, or `null`
    /// once the probe reaches an empty slot}
    ///
    /// Every lookup by a segment probes through it, [Lookups] from where its own probe stopped.
    private static Chain find(Table table, int slot, MemorySegment key, int hash) {
        var tags = table.tags;
        var chains = table.chains;
        var mask = tags.length - 1;
        var tag = tag(hash);
        for (; ; slot = (slot + 1) & mask) {
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

    /// {@return the chain of the key of `hash` whose bytes are `key`, or `null` if the index holds none}
    ///
    /// A lookup by an array, for a read: it compares `key` with a chain whose tag and hash match, in words for a chain
    /// whose key is in its own words, and makes a segment of `key` only for one whose key is in a segment.
    Chain get(byte[] key, int hash) {
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

    /// {@return whether no segment holds a table of its own, so no lookup can find a chain}
    ///
    /// Like a lookup, it never hides a chain that was in the index when it was called and still is.
    boolean holdsNone() {
        return occupied == 0;
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
                    setTable(segment, Table.of(INITIAL));
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
                    rebuild(segment);
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
                        setTable(segment, EMPTY);
                        return;
                    }
                    // Tombstones end no probe: once they outnumber the chains, and fill a quarter of the table, they
                    // go; and a table a sixteenth full shrinks, so a key space whose chains a checkpoint dropped does
                    // not keep the slots of its busiest moment, while a table that just grew, three eighths full, does
                    // not shrink back at the next removals
                    var tombstones = segment.taken - segment.size;
                    if ((tombstones > segment.size && tombstones * 4 > tags.length)
                            || (tags.length > INITIAL && segment.size * 16 < tags.length)) {
                        rebuild(segment);
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

    /// {@return the segments the count behind [#holdsNone()] takes to hold a table of their own}
    int occupiedForTests() {
        return occupied;
    }

    /// {@return the segments that hold a table of their own, each looked at in turn}
    int tablesForTests() {
        var tables = 0;
        for (var i = 0; i < SEGMENTS; i++) {
            var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, i);
            if (segment != null && segment.table != EMPTY) {
                tables++;
            }
        }
        return tables;
    }

    /// {@return the next chain of a walk over the index from `cursor`, which it advances, or `null` once the walk has
    /// visited every segment}
    ///
    /// The walk reads each segment's table as it reaches the segment and keeps to it, though the segment may grow and
    /// publish another: it visits every chain the index held when it started and still held when it reached the chain's
    /// segment, and may visit chains inserted or removed since. Safe beside insertions and removals.
    Chain next(Cursor cursor) {
        while (cursor.segment < SEGMENTS) {
            if (cursor.chains == null) {
                var segment = (Segment) SEGMENT_SLOTS.getAcquire(segments, cursor.segment);
                if (segment == null) {
                    cursor.segment++;
                    continue;
                }
                cursor.chains = segment.table.chains;
                cursor.slot = 0;
            }
            var chains = cursor.chains;
            while (cursor.slot < chains.length) {
                var chain = (Chain) SLOTS.getAcquire(chains, cursor.slot++);
                if (chain != null && chain != TOMBSTONE) {
                    return chain;
                }
            }
            cursor.chains = null;
            cursor.segment++;
        }
        return null;
    }

    /// Where a walk over the index stands: the segment it walks, the slots of that segment's table it keeps to, and the
    /// next slot. A new cursor starts at the first segment.
    static final class Cursor {
        private int segment;
        private Chain[] chains;
        private int slot;
    }

    /// Writes the tag of `chain` into the slot an insertion would give it, and leaves the chain unpublished, as an
    /// insertion is between its two stores, until [#publishForTests(Chain, int)]; for tests, which insert nothing into
    /// the segment meanwhile.
    ///
    /// @return the slot
    int tagForTests(Chain chain) {
        var segment = segmentFor(chain.hash);
        synchronized (segment) {
            if (segment.table == EMPTY) {
                setTable(segment, Table.of(INITIAL));
            }
            var table = segment.table;
            var mask = table.tags.length - 1;
            var slot = slot(chain.hash) & mask;
            while (table.tags[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            segment.taken++;
            table.tags[slot] = tag(chain.hash);
            return slot;
        }
    }

    /// Publishes `chain` in the slot [#tagForTests(Chain)] wrote its tag into; for tests.
    void publishForTests(Chain chain, int slot) {
        var segment = segmentFor(chain.hash);
        synchronized (segment) {
            SLOTS.setRelease(segment.table.chains, slot, chain);
            segment.size++;
        }
    }

    /// The lookups of a block of up to [#BLOCK] keys, each in the index of its key space, made together so the misses
    /// of one key's lookup overlap those of the others.
    ///
    /// A lookup in a large index is a chain of dependent loads that each miss the processor's caches: the tags of the
    /// key's slot, the slot's chain, the chain, and the bytes of its key if it is longer than [Chain#INLINE], since a
    /// shorter one is in words of the chain's own. Lookups made one after another, between the work done for each key,
    /// wait for each miss in turn; here each step of every lookup of the block runs in a loop of its own, small enough
    /// that the processor issues the loads of the next lookups while the first miss is still in flight, and the misses
    /// of the block cost about as much as those of one lookup.
    ///
    /// Each lookup reads its segment's table once, in the first step, and probes that table to the end, as [#get]
    /// probes the table it read: it answers what a lookup made at the first step would have, which may be a chain
    /// removed since. A table replaced meanwhile is never written again, and a chain is removed only once it is
    /// released, so a caller that checks [Chain#released()] before it uses a chain, and opens the key again when a
    /// writer finds the chain dropped later, as [KeyspaceState#open(MemorySegment, int, Chain)] and the sequencer do,
    /// gets what a lookup made then would give it. A lookup that finds no chain may miss one inserted since its first
    /// step, by the caller itself for an earlier key of the block, so the caller looks again before it creates one.
    ///
    /// For one thread at a time; a commit takes one for all its blocks.
    static final class Lookups {
        /// The lookups of a block: as many misses as a core keeps in flight, in loops that a commit of a few hundred
        /// writes calls often enough to compile.
        static final int BLOCK = 16;

        // The index of each lookup, null for one known to hold no chain
        private final ChainIndex[] indexes;
        private final MemorySegment[] keys;
        private final int[] hashes;
        // The table each lookup probes, read by the first step; null once the lookup found the key absent
        private final Table[] tables;
        // The slot each lookup's probe stands at
        private final int[] slots;
        // The tag of each lookup's first slot, read by the first step, which the probe starts from
        private final byte[] firstTags;
        // The chain each lookup found, a candidate until the comparison of the keys
        private final Chain[] chains;
        // The first word of each candidate's key and the stamp of each found chain's newest version, loaded only to
        // bring their lines into the caches: stored, so the loads are not taken out as unused, and never read
        private final long[] keyWords;
        private final long[] headStamps;
        // Whether the last step brings in the newest version of each chain found
        private final boolean touchHeads;
        // The lookups added
        private int count;
        // The lookups added with an index
        private int probing;

        /// Makes the lookups of the blocks of one commit.
        ///
        /// @param touchHeads whether a lookup that finds a chain also loads its newest version, for a caller that reads
        ///                   it soon after, before the lines of the block leave the caches
        Lookups(boolean touchHeads) {
            this.indexes = new ChainIndex[BLOCK];
            this.keys = new MemorySegment[BLOCK];
            this.hashes = new int[BLOCK];
            this.tables = new Table[BLOCK];
            this.slots = new int[BLOCK];
            this.firstTags = new byte[BLOCK];
            this.chains = new Chain[BLOCK];
            this.keyWords = new long[BLOCK];
            this.headStamps = new long[BLOCK];
            this.touchHeads = touchHeads;
        }

        /// Empties the block.
        void clear() {
            count = 0;
            probing = 0;
        }

        /// Adds the lookup of `key`, whose hash is `hash`, in `index`, or in no index if `index` is `null`, which finds
        /// no chain; the block holds fewer than [#BLOCK] lookups.
        void add(ChainIndex index, MemorySegment key, int hash) {
            var at = count++;
            indexes[at] = index;
            keys[at] = key;
            hashes[at] = hash;
            if (index != null) {
                probing++;
            }
        }

        /// Looks up every key added since the block was emptied.
        void run() {
            if (probing == 0) {
                for (var at = 0; at < count; at++) {
                    chains[at] = null;
                }
                return;
            }
            locate();
            resolve();
        }

        /// Looks up every key added since the block was emptied, running `between` after the first step, as a change
        /// of the indexes that lands between the steps; for tests.
        void runForTests(Runnable between) {
            locate();
            between.run();
            resolve();
        }

        /// {@return the hash of the key of lookup `at`, in the order of the additions}
        int hash(int at) {
            return hashes[at];
        }

        /// {@return the chain that lookup `at`, in the order of the additions, found, or `null` if it found none}
        Chain found(int at) {
            return chains[at];
        }

        /// Runs the steps after the first, each of whose loads but one the step before brought into the caches.
        private void resolve() {
            probe();
            check();
            touchKeys();
            compare();
            if (touchHeads) {
                touchHeads();
            }
        }

        /// Reads each lookup's table, once, and the tag of its first slot: the first miss.
        private void locate() {
            for (var at = 0; at < count; at++) {
                chains[at] = null;
                var index = indexes[at];
                if (index == null) {
                    tables[at] = null;
                    continue;
                }
                var hash = hashes[at];
                var segment = (Segment) SEGMENT_SLOTS.getAcquire(index.segments, segmentOf(hash));
                var table = segment == null ? null : segment.table;
                if (table == null || table == EMPTY) {
                    tables[at] = null;
                    continue;
                }
                tables[at] = table;
                var slot = slot(hash) & (table.tags.length - 1);
                slots[at] = slot;
                firstTags[at] = table.tags[slot];
            }
        }

        /// Probes each table from the first slot to the first whose tag matches, ending the lookups that reach an
        /// empty slot first, and loads the matching slot's chain with a plain read, which only brings the slot's line
        /// in: the second miss. On processors where an acquiring load holds back the loads after it, the acquire of
        /// the next step then finds the line.
        private void probe() {
            for (var at = 0; at < count; at++) {
                var table = tables[at];
                if (table == null) {
                    continue;
                }
                var tag = tag(hashes[at]);
                var held = firstTags[at];
                if (held != tag && held != 0) {
                    var tags = table.tags;
                    var mask = tags.length - 1;
                    var slot = slots[at];
                    do {
                        slot = (slot + 1) & mask;
                        held = tags[slot];
                    } while (held != tag && held != 0);
                    slots[at] = slot;
                }
                if (held == 0) {
                    tables[at] = null;
                    continue;
                }
                chains[at] = table.chains[slots[at]];
            }
        }

        /// Loads each matching slot's chain with an acquire, the load the lookup's answer rests on, and its hash: the
        /// third miss. A slot whose chain is not published yet, was removed or has another hash leaves no candidate,
        /// and the comparison probes past it.
        private void check() {
            for (var at = 0; at < count; at++) {
                var table = tables[at];
                if (table == null) {
                    continue;
                }
                var chain = (Chain) SLOTS.getAcquire(table.chains, slots[at]);
                chains[at] = chain != null && chain != TOMBSTONE && chain.hash == hashes[at] ? chain : null;
            }
        }

        /// Loads the first word of each candidate's key held out of its chain, longer than [Chain#INLINE]: the fourth
        /// miss. A shorter key is in words of the chain's own, which the step before loaded.
        private void touchKeys() {
            for (var at = 0; at < count; at++) {
                var chain = chains[at];
                if (chain != null && !chain.inlineKey()) {
                    keyWords[at] = chain.word(0);
                }
            }
        }

        /// Compares each candidate's key, and probes past a slot that left no candidate or holds another key as [#get]
        /// probes past it, on the same table.
        private void compare() {
            for (var at = 0; at < count; at++) {
                var table = tables[at];
                if (table == null) {
                    continue;
                }
                var chain = chains[at];
                if (chain == null || !chain.keyEquals(keys[at])) {
                    chains[at] = find(table, (slots[at] + 1) & (table.tags.length - 1), keys[at], hashes[at]);
                }
            }
        }

        /// Loads the newest version of each chain found, and its stamp, which the caller's writes read next.
        private void touchHeads() {
            for (var at = 0; at < count; at++) {
                var chain = chains[at];
                if (chain != null) {
                    var head = chain.head;
                    if (head != null) {
                        headStamps[at] = head.stamp;
                    }
                }
            }
        }
    }

    /// Makes `table` the table of `segment`, whose lock the caller holds, and counts the segment as it starts or stops
    /// holding a table of its own: before the table is published, whose chains come after it, and after it is dropped.
    private void setTable(Segment segment, Table table) {
        var had = segment.table != EMPTY;
        var has = table != EMPTY;
        if (has && !had) {
            OCCUPIED.getAndAdd(this, 1);
        }
        segment.table = table;
        if (had && !has) {
            OCCUPIED.getAndAdd(this, -1);
        }
    }

    /// Copies the chains of `segment`, whose lock the caller holds, into a table at most half full, without tombstones,
    /// and publishes it: one that filled doubles, to three eighths full.
    private void rebuild(Segment segment) {
        if (segment.size == 0) {
            segment.taken = 0;
            setTable(segment, EMPTY);
            return;
        }
        var old = segment.table.chains;
        var capacity = INITIAL;
        while (capacity < (segment.size + 1) * 2) {
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
        segment.taken = segment.size;
        // The volatile store publishes the copied slots with the table
        setTable(segment, fresh);
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
        // A volatile exchange: a thread that loses the race reads the winner's segment as it was made
        var witness = (Segment) SEGMENT_SLOTS.compareAndExchange(segments, at, null, made);
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
    }
}
