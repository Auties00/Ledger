package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

/// The opened units of the store's files, by file and first page, within a limit of bytes it shares with the caches of
/// the process's other stores ([Budget]); it evicts with the CLOCK algorithm.
///
/// While the caches of the process hold more than the limit, a cache that holds more than its share of it, the limit
/// over the open caches, evicts its own entries; one within its share takes the room from the cache that holds the
/// most. So one busy store may use the whole limit while the others are idle, and a store that starts reading takes
/// back its share. A limit of each store's own let a process of many stores keep many limits of pages.
///
/// An entry holds the plaintext of the unit, which may end with its content ([PageFormat#cachedLength(int, int)]), and
/// is never written again, so a reader keeps using an array the cache has evicted: the array goes to the collector, not
/// back to a pool, and no read pins anything. The limit counts the bytes of the arrays, not of the units' pages. An
/// entry is found only under the nonce and the pages its unit was sealed with ([PageRef]), so a page a later checkpoint
/// reused never returns the unit it held before. An entry the cache drops lets go of its array at once: the memory the
/// cache keeps reachable stays within its limit.
///
/// An entry no read used for a whole period of the [IdleTicker] leaves at its next tick, however full the caches are:
/// the clock evicts only past the limit, so a store that stopped reading kept a limit of pages, mostly the ones its last
/// checkpoint wrote and no one read, for as long as it stayed open.
///
/// The entries live in the parallel arrays of an open-addressing [Table], so a read finds a unit with one probe of
/// four bytes a slot instead of walking a map's node, boxed key and entry, and allocates nothing: a read that misses,
/// each of a scan's past the limit, touches a line of an array that fits a core's cache, and one that hits reads the
/// nonce and pages in the unit's own header, which it reads anyway. Reads take no lock, and a read that races a writer
/// only misses, which costs a read of the file. Whatever changes the cache holds its lock, and the eviction of another
/// cache's entries happens outside it, so no thread holds the locks of two caches.
///
/// The clock passes the entries in the order they came, as a list through the slots rather than in the order of the
/// slots: a unit a checkpoint wrote and no one read yet waits its turn behind the older ones, wherever its key hashes,
/// and is not the next to go because it landed just ahead of the hand.
final class PageCache implements IdleTicker.Watched {
    /// The file of the trees, `ledger.data`.
    static final int DATA = 0;
    /// The file of the values written at commit, `ledger.blobs`.
    static final int BLOBS = 1;
    /// The first of the keys of the leaves delta records make, one per slot of a delta page of `ledger.data`.
    private static final int RECORDS = 2;
    /// The slots of a new table, and the fewest a table shrinks to.
    private static final int MIN_CAPACITY = 64;
    /// The most slots of a table, as three longs per slot must fit an array. At half of them, 2^28 entries of at least a
    /// page each, a terabyte of heap, the cache evicts rather than grow.
    private static final int MAX_CAPACITY = 1 << 29;
    /// The tag of a slot no entry used since its table was built; an entry's tag is odd ([Table#tag(long)]).
    private static final int EMPTY = 0;
    /// The tag of a slot whose entry left, which a probe passes over.
    private static final int TOMBSTONE = 2;
    /// The flag of an entry a read used since the clock last passed it.
    private static final byte REFERENCED = 1;
    /// The flag a tick of the idle ticker sets and a read clears: a tick that finds it set evicts the entry.
    private static final byte IDLE = 2;
    /// 2^64 over the golden ratio: multiplying by it spreads the consecutive pages of a file evenly over the slots.
    private static final long HASH = 0x9E3779B97F4A7C15L;
    private static final VarHandle TAGS = MethodHandles.arrayElementVarHandle(int[].class);
    private static final VarHandle META = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle UNITS = MethodHandles.arrayElementVarHandle(byte[][].class);

    // Guards the changes to the table and to closed
    private final Object lock;
    // Replaced whole by a rebuild, which leaves the old one as it was for the readers still on it
    private volatile Table table;
    private final AtomicLong bytes;
    private final Budget budget;
    // Set once the store closes: a read that ends after it caches nothing, or its bytes would stay in the budget
    private boolean closed;

    private PageCache(Budget budget) {
        this.lock = new Object();
        this.table = new Table(MIN_CAPACITY);
        this.bytes = new AtomicLong();
        this.budget = budget;
    }

    /// {@return an empty cache that shares the limit of the process's caches, which `limit` raises to it if lower}
    static PageCache shared(long limit) {
        return in(Budget.PROCESS, limit);
    }

    /// {@return an empty cache of at most `limit` bytes of its own}
    static PageCache isolated(long limit) {
        return in(new Budget(), limit);
    }

    /// {@return an empty cache that shares `budget`, which `limit` raises to it if lower}
    static PageCache in(Budget budget, long limit) {
        var cache = new PageCache(budget);
        budget.join(cache, limit);
        IdleTicker.watch(cache);
        return cache;
    }

    /// {@return the key of page `page` of `file`, [#DATA] or [#BLOBS]}
    static long key(int file, int page) {
        return (long) file << 32 | (page & 0xFFFF_FFFFL);
    }

    /// {@return the key of the unit at page `page` of the data file, or of the leaf delta record `slot` of it makes}
    ///
    /// @param slot the record of the delta page, from 0, or -1 for the unit itself
    static long dataKey(int page, int slot) {
        return key(slot >= 0 ? RECORDS + slot : DATA, page);
    }

    /// {@return the plaintext of the unit `ref` of the data file names, or of the leaf its delta record makes, or
    /// `null` if the cache does not hold it}
    byte[] get(PageRef ref) {
        return get(dataKey(ref.page(), ref.slot()), ref);
    }

    /// {@return the plaintext of the unit `ref` names, at `key`, or `null` if the cache does not hold it}
    byte[] get(long key, PageRef ref) {
        return get(key, ref.nonceHigh(), ref.nonceLow(), ref.pages());
    }

    /// {@return the plaintext of the unit of `pages` pages sealed under the nonce `nonceHigh` and `nonceLow`, at `key`,
    /// or `null` if the cache does not hold it}
    ///
    /// It takes no lock and allocates nothing; a put, removal or rebuild it races makes it return `null` at worst.
    byte[] get(long key, long nonceHigh, int nonceLow, int pages) {
        var table = this.table;
        var slot = table.find(key);
        if (slot < 0) {
            return null;
        }
        var unit = table.unit(slot, key, nonceHigh, nonceLow, pages);
        if (unit != null) {
            // Written only when it changes: a store on every hit made the readers of one page, the root first, take
            // the line of its flag from each other. One that lands on a slot refilled meanwhile only gives its new
            // entry one more pass of the clock
            var flags = table.flags;
            if (flags[slot] != REFERENCED) {
                flags[slot] = REFERENCED;
            }
        }
        return unit;
    }

    /// {@return the plaintext of the unit `ref` of the data file names, or of the leaf its delta record makes, or
    /// `null` if the cache does not hold it, without counting as a read of the entry}
    byte[] peek(PageRef ref) {
        var key = dataKey(ref.page(), ref.slot());
        var table = this.table;
        var slot = table.find(key);
        return slot < 0 ? null : table.unit(slot, key, ref.nonceHigh(), ref.nonceLow(), ref.pages());
    }

    /// Caches `unit`, the plaintext of the unit `ref` of the data file names, replacing whatever the cache held for its
    /// page.
    ///
    /// @param unit an array as [#put(long, PageRef, byte\[\], boolean)] requires
    /// @param referenced whether the unit counts as read, rather than written and maybe never read
    void put(PageRef ref, byte[] unit, boolean referenced) {
        put(dataKey(ref.page(), ref.slot()), ref, unit, referenced);
    }

    /// Caches `unit`, the plaintext of the unit `ref` names, at `key`, replacing whatever the cache held there.
    ///
    /// @param key a key of [#key(int, int)] or [#dataKey(int, int)]
    /// @param unit an array the cache never held under another key or nonce and never will, whose content no one writes
    ///             again. A reader of a delta record's leaf tells that a slot changed under it by the slot's array, so an
    ///             array cached again under other metadata could pass as the leaf of a nonce it does not hold
    /// @param referenced whether the unit counts as read, rather than written and maybe never read
    void put(long key, PageRef ref, byte[] unit, boolean referenced) {
        if (unit.length > budget.limit() / 4) {
            // A unit this large would evict most of the cache for one read; what the page held is out of date
            remove(key);
            return;
        }
        synchronized (lock) {
            if (closed) {
                return;
            }
            fill(key, ref, unit, referenced);
        }
        evict();
    }

    /// Stores `unit` at `key`, in the slot that holds the key or a free one, as the newest entry of the clock.
    private void fill(long key, PageRef ref, byte[] unit, boolean referenced) {
        var table = this.table;
        var slot = table.locate(key);
        if (slot >= 0) {
            drain(table, slot);
        } else {
            table = roomFor(table);
            slot = table.vacancy(key);
            if (table.tags[slot] == TOMBSTONE) {
                table.clock.tombstones--;
            }
        }
        // The null first, then the metadata, then the unit: a reader that read the old array and any of the new
        // metadata reads the array again after it and finds it gone
        UNITS.setRelease(table.units, slot, null);
        VarHandle.storeStoreFence();
        META.setOpaque(table.meta, 3 * slot + 1, ref.nonceHigh());
        META.setOpaque(table.meta, 3 * slot + 2, identity(ref.nonceLow(), ref.pages()));
        META.setOpaque(table.meta, 3 * slot, key);
        TAGS.setOpaque(table.tags, slot, Table.tag(key * HASH));
        table.flags[slot] = referenced ? REFERENCED : 0;
        UNITS.setRelease(table.units, slot, unit);
        table.append(slot);
        table.clock.live++;
        account(unit.length);
    }

    /// Takes the entry of `slot` out of the clock and the accounts, leaving its array and metadata to the caller.
    private void drain(Table table, int slot) {
        table.unlink(slot);
        table.clock.live--;
        account(-table.units[slot].length);
    }

    /// {@return the table to add an entry to: `table`, or one that replaces it, larger if the entries would pass half of
    /// its slots and of the same size if the slots of departed entries took the room}
    private Table roomFor(Table table) {
        var clock = table.clock;
        var capacity = table.capacity();
        if (clock.live + 1 > capacity / 2) {
            if (capacity < MAX_CAPACITY) {
                return rebuild(table, capacity * 2);
            }
            evictFrom(table);
        }
        // Three quarters at most in use or gone: the slots a probe passes over cost every read that crosses them
        return clock.live + clock.tombstones + 1 > capacity - capacity / 4 ? rebuild(table, capacity) : table;
    }

    /// Publishes a table of `capacity` slots with the entries of `old`, in the clock's order.
    ///
    /// A reader still on `old` finds what it held, which is correct for the nonce it asks for.
    ///
    /// @return the new table
    private Table rebuild(Table old, int capacity) {
        var table = new Table(capacity);
        for (var slot = old.clock.oldest; slot != -1; slot = old.next(slot)) {
            var at = table.vacancy(old.meta[3 * slot]);
            table.tags[at] = old.tags[slot];
            table.meta[3 * at] = old.meta[3 * slot];
            table.meta[3 * at + 1] = old.meta[3 * slot + 1];
            table.meta[3 * at + 2] = old.meta[3 * slot + 2];
            table.units[at] = old.units[slot];
            table.flags[at] = old.flags[slot];
            table.append(at);
            table.clock.live++;
        }
        this.table = table;
        return table;
    }

    /// Drops whatever the cache holds for page `page` of the data file.
    void remove(int page) {
        remove(key(DATA, page));
    }

    /// Drops the unit `ref` of the data file names, or the leaf its delta record makes, if the cache holds it; a unit
    /// of another nonce the cache holds for its page stays.
    void remove(PageRef ref) {
        var key = dataKey(ref.page(), ref.slot());
        synchronized (lock) {
            var table = this.table;
            var slot = table.locate(key);
            if (slot >= 0 && table.meta[3 * slot + 1] == ref.nonceHigh()
                    && (int) (table.meta[3 * slot + 2] >>> 32) == ref.nonceLow()) {
                drop(table, slot);
            }
        }
    }

    /// Drops whatever the cache holds at `key`.
    void remove(long key) {
        synchronized (lock) {
            var table = this.table;
            var slot = table.locate(key);
            if (slot >= 0) {
                drop(table, slot);
            }
        }
    }

    /// Takes the entry of `slot` out of the table and the accounts, and lets go of its array.
    private void drop(Table table, int slot) {
        drain(table, slot);
        // The slot never goes back to empty, or a probe would stop before the keys past it
        UNITS.setRelease(table.units, slot, null);
        VarHandle.storeStoreFence();
        TAGS.setOpaque(table.tags, slot, TOMBSTONE);
        table.clock.tombstones++;
    }

    private void account(long delta) {
        bytes.addAndGet(delta);
        budget.total.addAndGet(delta);
    }

    /// Drops every entry and leaves the budget, as the store closes; a read that ends after it caches nothing.
    void close() {
        synchronized (lock) {
            closed = true;
            dropAll();
        }
        budget.leave(this);
        IdleTicker.unwatch(this);
    }

    /// Replaces the table with an empty one and gives back the bytes of its entries.
    private void dropAll() {
        account(-bytes.get());
        this.table = new Table(MIN_CAPACITY);
    }

    /// Evicts the entries no read used since the last tick, and marks the others to be evicted at the next unless a read
    /// uses them; then shrinks a table that fewer than a sixteenth of its slots use.
    @Override
    public void tick() {
        synchronized (lock) {
            var table = this.table;
            var units = table.units;
            var flags = table.flags;
            for (var slot = 0; slot < units.length; slot++) {
                if (units[slot] == null) {
                    continue;
                }
                var state = flags[slot];
                if ((state & IDLE) == 0) {
                    flags[slot] = (byte) (state | IDLE);
                } else {
                    drop(table, slot);
                }
            }
            var live = table.clock.live;
            if (table.capacity() > MIN_CAPACITY && live < table.capacity() / 16) {
                // At least four slots an entry, so it takes many puts to grow the table again
                rebuild(table, Math.max(MIN_CAPACITY, Integer.highestOneBit(Math.max(live, 1) * 8 - 1)));
            }
        }
    }

    /// {@return the bytes the cache holds}
    long size() {
        return bytes.get();
    }

    /// {@return whether the caches of the budget hold `bytes` fewer bytes than its limit, so caching them would evict
    /// nothing}
    boolean hasRoom(long bytes) {
        return budget.total.get() + bytes <= budget.limit();
    }

    /// {@return the entries the clock holds, for tests that bound it}
    int clockLengthForTests() {
        synchronized (lock) {
            var length = 0;
            var table = this.table;
            for (var slot = table.clock.oldest; slot != -1; slot = table.next(slot)) {
                length++;
            }
            return length;
        }
    }

    /// {@return the slots of the table, for tests that bound it}
    int capacityForTests() {
        return table.capacity();
    }

    /// {@return the slots whose entry left and that no entry reused, for tests that bound them}
    int tombstonesForTests() {
        synchronized (lock) {
            return table.clock.tombstones;
        }
    }

    /// Drops every entry, so tests read units from the file.
    void clearForTests() {
        synchronized (lock) {
            dropAll();
        }
    }

    /// Runs `action` holding the lock of the cache, so tests can make whatever changes the cache wait for it.
    void whileLockedForTests(Runnable action) {
        synchronized (lock) {
            action.run();
        }
    }

    /// Evicts entries while the caches of the budget hold more than its limit, from this cache while it holds more than
    /// its share and from the one that holds the most otherwise.
    ///
    /// It holds no lock between evictions: two caches evicting from each other would otherwise wait for each other.
    private void evict() {
        while (budget.over()) {
            var target = bytes.get() > budget.share() ? this : budget.largest();
            if ((target == null || !target.evictOne()) && (target == this || !evictOne())) {
                break;
            }
        }
    }

    /// Evicts the next entry the clock finds unreferenced, giving each referenced one another pass.
    ///
    /// @return whether it evicted one; `false` if the cache holds none
    private boolean evictOne() {
        synchronized (lock) {
            return evictFrom(table);
        }
    }

    /// Evicts the next entry of `table`, the current one, the clock finds unreferenced.
    private boolean evictFrom(Table table) {
        // Every entry gets one more pass when referenced, so twice the entries bound the sweep
        var steps = 2 * (long) table.clock.live + 1;
        var flags = table.flags;
        while (steps-- > 0) {
            var slot = table.clock.oldest;
            if (slot == -1) {
                return false;
            }
            var state = flags[slot];
            if ((state & REFERENCED) == 0) {
                drop(table, slot);
                return true;
            }
            flags[slot] = (byte) (state & ~REFERENCED);
            table.unlink(slot);
            table.append(slot);
        }
        return false;
    }

    /// {@return the third long of a slot's metadata}
    private static long identity(int nonceLow, int pages) {
        return (long) nonceLow << 32 | (pages & 0xFFFF_FFFFL);
    }

    /// The slots of a cache, in arrays indexed by the hash of the key, probed linearly.
    ///
    /// A slot's tag starts [#EMPTY], which ends a probe, and in a published table never goes back to it: a slot whose
    /// entry left holds [#TOMBSTONE] until a later entry reuses it, and a rebuild clears them by publishing a new table.
    /// Readers read the fields of the table and the slots; the fields that change at every put are in a [Padded.Clock]
    /// away from them. Everything but [#find(long)] and [#unit(int, long, long, int, int)] runs under the lock of the
    /// cache, or on a table not published yet.
    private static final class Table {
        // What a probe reads, per slot: an odd hash of the entry's key, or EMPTY or TOMBSTONE. It keeps a miss to these 4
        // bytes a slot, as a map's table was: at three longs a slot a scan past the limit paid a cold line per unit
        final int[] tags;
        // Three longs per slot: the key, the first 8 bytes of the nonce, and the last 4 with the unit's pages; read by a
        // reader only for the leaf of a delta record, whose header is its base's
        final long[] meta;
        // The plaintext of each slot, or null when the slot holds no entry or a writer is changing it
        final byte[][] units;
        // REFERENCED and IDLE per slot, apart from the metadata so that a read that sets them dirties no line of it
        final byte[] flags;
        // Two ints per slot: the next newer and the next older entry of the clock, or -1
        final int[] links;
        final int mask;
        final int shift;
        final Padded.Clock clock;

        Table(int capacity) {
            this.tags = new int[capacity];
            this.meta = new long[3 * capacity];
            this.units = new byte[capacity][];
            this.flags = new byte[capacity];
            this.links = new int[2 * capacity];
            this.mask = capacity - 1;
            this.shift = Long.numberOfLeadingZeros(capacity - 1L);
            this.clock = new Padded.Clock();
        }

        int capacity() {
            return mask + 1;
        }

        /// {@return the tag of the key whose product by [#HASH] is `hash`}
        ///
        /// Both halves, as the lower is the same for a page of each file. Two keys of one probe share a tag once in 2^31:
        /// the later then misses until one of them leaves, as [#unit(int, long, long, int, int)] refuses the other's unit.
        static int tag(long hash) {
            return (int) (hash ^ hash >>> 32) | 1;
        }

        /// {@return the slot whose tag is that of `key`, or -1; one that holds another unit, or that a writer is changing,
        /// fails [#unit(int, long, long, int, int)]}
        int find(long key) {
            var tags = this.tags;
            var mask = this.mask;
            var hash = key * HASH;
            var tag = tag(hash);
            var slot = (int) (hash >>> shift);
            for (var probes = 0; probes <= mask; probes++) {
                var found = (int) TAGS.getOpaque(tags, slot);
                if (found == tag) {
                    return slot;
                }
                if (found == EMPTY) {
                    return -1;
                }
                slot = (slot + 1) & mask;
            }
            return -1;
        }

        /// {@return the slot that holds `key`, or -1, holding the lock of the cache or on a table not published yet}
        int locate(long key) {
            var hash = key * HASH;
            var tag = tag(hash);
            var slot = (int) (hash >>> shift);
            for (var probes = 0; probes <= mask; probes++) {
                var found = tags[slot];
                if (found == tag && meta[3 * slot] == key) {
                    return slot;
                }
                if (found == EMPTY) {
                    return -1;
                }
                slot = (slot + 1) & mask;
            }
            return -1;
        }

        /// {@return the array of `slot` if it holds the unit the arguments name, or `null`}
        ///
        /// A unit of a file says what it is: its header, which never changes, names its page, nonce and pages, and a nonce
        /// is never used twice, so one that matches is the unit whatever the slot's race.
        ///
        /// The leaf of a delta record has the header of its base leaf, so a read of the base refuses it by its origin: the
        /// leaf can land in the slot the base just left, or share its tag, under a reader that found the slot by the base's
        /// tag, and it holds other entries than the base. For a read of the leaf the slot's metadata speaks. Its array is
        /// read first and again after the metadata: a writer takes it out before it changes the metadata and puts the new
        /// one in after, and no array is ever cached under two identities, so an array read the same both times held the
        /// metadata read between them.
        byte[] unit(int slot, long key, long nonceHigh, int nonceLow, int pages) {
            var units = this.units;
            var unit = (byte[]) UNITS.getAcquire(units, slot);
            if (unit == null) {
                return null;
            }
            if (key >>> 32 < RECORDS) {
                return describes(unit, (int) key, nonceHigh, nonceLow, pages) ? unit : null;
            }
            var meta = this.meta;
            var at = 3 * slot;
            var same = (long) META.getOpaque(meta, at) == key
                    & (long) META.getOpaque(meta, at + 1) == nonceHigh
                    & (long) META.getOpaque(meta, at + 2) == identity(nonceLow, pages);
            VarHandle.loadLoadFence();
            return same && (byte[]) UNITS.getOpaque(units, slot) == unit ? unit : null;
        }

        private static boolean describes(byte[] unit, int page, long nonceHigh, int nonceLow, int pages) {
            return Bytes.i32(unit, PageFormat.PAGE) == page
                    && Bytes.i64(unit, PageFormat.NONCE) == nonceHigh
                    && Bytes.i32(unit, PageFormat.NONCE + 8) == nonceLow
                    && Bytes.i32(unit, PageFormat.PAGES) == pages
                    && !TreeNodes.madeFromDelta(unit);
        }

        /// {@return the first slot on the probe of `key`, which the table does not hold, that an entry may take}
        int vacancy(long key) {
            var slot = (int) (key * HASH >>> shift);
            while (true) {
                var found = tags[slot];
                if (found == EMPTY || found == TOMBSTONE) {
                    return slot;
                }
                slot = (slot + 1) & mask;
            }
        }

        /// {@return the next newer entry of the clock after `slot`, or -1}
        int next(int slot) {
            return links[2 * slot];
        }

        /// Makes `slot` the newest entry of the clock.
        void append(int slot) {
            var newest = clock.newest;
            links[2 * slot] = -1;
            links[2 * slot + 1] = newest;
            if (newest == -1) {
                clock.oldest = slot;
            } else {
                links[2 * newest] = slot;
            }
            clock.newest = slot;
        }

        /// Takes `slot` out of the clock.
        void unlink(int slot) {
            var next = links[2 * slot];
            var previous = links[2 * slot + 1];
            if (previous == -1) {
                clock.oldest = next;
            } else {
                links[2 * previous] = next;
            }
            if (next == -1) {
                clock.newest = previous;
            } else {
                links[2 * next + 1] = previous;
            }
        }
    }

    /// The bytes some caches may hold together: those of the stores of the process, or a cache's own.
    ///
    /// Its limit is the largest any open cache asked for. It is safe for concurrent use: the total is one atomic
    /// number, and the open caches a snapshot their joins and leaves replace. It holds the caches weakly: a store
    /// never closed leaves when the collector takes its cache, which gives its bytes back, as it could before the
    /// stores shared a budget.
    static final class Budget {
        /// The budget of the caches of the stores of the process.
        static final Budget PROCESS = new Budget();
        // Gives back the bytes of the caches the collector took
        private static final Cleaner CLEANER = Cleaner.create(Thread.ofVirtual().name("ledger-cache-cleaner").factory());

        // The bytes the caches hold together
        private final AtomicLong total;
        private final Object lock;
        // The open caches and the limits they asked for, weakly; guarded by the lock
        private final Map<PageCache, Long> asked;
        // The open caches, replaced on each join and leave
        private volatile WeakReference<PageCache>[] members;
        private volatile long limit;

        /// Makes a budget no cache shares yet.
        @SuppressWarnings("unchecked")
        Budget() {
            this.total = new AtomicLong();
            this.lock = new Object();
            this.asked = new WeakHashMap<>();
            this.members = new WeakReference[0];
        }

        private void join(PageCache cache, long requested) {
            synchronized (lock) {
                asked.put(cache, requested);
                refresh();
            }
            var budget = this;
            var bytes = cache.bytes;
            CLEANER.register(cache, () -> {
                // Unreachable, so nothing adds to its bytes any more; 0 once it closed
                budget.total.addAndGet(-bytes.getAndSet(0));
                synchronized (budget.lock) {
                    budget.refresh();
                }
            });
        }

        private void leave(PageCache cache) {
            synchronized (lock) {
                asked.remove(cache);
                refresh();
            }
        }

        @SuppressWarnings("unchecked")
        private void refresh() {
            var largest = 0L;
            var open = new WeakReference[asked.size()];
            var count = 0;
            for (var entry : asked.entrySet()) {
                largest = Math.max(largest, entry.getValue());
                if (count < open.length) {
                    open[count++] = new WeakReference<>(entry.getKey());
                }
            }
            limit = largest;
            members = count == open.length ? open : java.util.Arrays.copyOf(open, count);
        }

        /// {@return the bytes the caches may hold together}
        long limit() {
            return limit;
        }

        /// {@return the bytes the caches hold together}
        long total() {
            return total.get();
        }

        private boolean over() {
            return total.get() > limit;
        }

        // The limit over the open caches
        private long share() {
            var caches = members.length;
            return caches == 0 ? limit : limit / caches;
        }

        // The open cache that holds the most bytes, or null if none is open
        private PageCache largest() {
            PageCache largest = null;
            var most = -1L;
            for (var member : members) {
                var cache = member.get();
                if (cache == null) {
                    continue;
                }
                var held = cache.bytes.get();
                if (held > most) {
                    most = held;
                    largest = cache;
                }
            }
            return largest;
        }
    }
}
