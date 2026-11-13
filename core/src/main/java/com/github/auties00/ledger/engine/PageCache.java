package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
/// A key's probe starts at the slot of its page, offset by its file, so the units a scan reads, which checkpoints
/// write in runs of consecutive pages, take consecutive slots and share the lines of the arrays and the pages of
/// memory that hold them, as the buckets of a map keyed by the page did. A start taken from a hash of the key sent each
/// of them to a line and a page of each array of its own, so a scan of 200,000 messages read a new line of each of the
/// three arrays a hit reads for every one of its 50,000 units, and ran at 0.87 times the speed of the map on Ubuntu.
/// The probe goes on by a stride of the key's hash, so the keys a run of pages pushes out of their slots, a page of
/// another file or one a table's size apart, scatter over the table instead of walking the run.
///
/// The clock passes the entries in the order they came, as a list through the slots rather than in the order of the
/// slots: a unit a checkpoint wrote and no one read yet waits its turn behind the older ones, wherever its key hashes,
/// and is not the next to go because it landed just ahead of the hand.
///
/// A read of a key space's tree that misses a leaf or a value is screened ([#putScreened(long, PageRef, byte\[\])]): the
/// cache takes the unit only at its second read, the first one recorded by a [Doorkeeper] of the unit's key and nonce.
/// Caching each unit at its first read, the leaves and values of every chat a client opened once, kept most of the
/// cache's bytes for units no one read again. The doorkeeper takes at most 4% of the units it never saw for seen ones,
/// which the cache takes at their first read. A unit the clock evicts is recorded as it leaves, so its next read caches
/// it at once, and a branch is never screened, as every read starts from the branches. Screening does not depend on
/// how fast the device reads: the reads of the files go through the OS cache, so the first read of a unit, from the
/// device or not, leaves its pages there, and its second read costs a read of the OS cache and an open, a few
/// microseconds, on a store copied past the OS cache as much as on any other.
///
/// The array of a unit the cache refused stays reachable weakly, in one of two slots of a table of up to
/// [#MAX_REFUSALS] by a hash of the key and nonce, so a read that misses the unit before the collector takes it caches
/// that array rather than reading and opening the unit again, whether it is the second read of a screened read or a
/// lookup of a commit: what the cache holds stays what reads used twice, and the first reads of a session allocate no
/// more than they did when the cache took every unit at once, as long as the collector has not run between the two
/// reads. Writes, the lookups of a commit, the reads of a checkpoint and of a resident key space, the root of a tree,
/// and the units of a scan past its first leaves and values cache as they would unscreened.
final class PageCache implements IdleTicker.Watched {
    /// The file of the trees, `ledger.data`.
    static final int DATA = 0;
    /// The file of the values written at commit, `ledger.blobs`.
    static final int BLOBS = 1;
    /// The first of the keys of the leaves delta records make, one per slot of a delta page of `ledger.data`.
    private static final int RECORDS = 2;
    /// The least bytes [#admission()] gives while the limit allows it: 4,096 leaves.
    private static final long MIN_ADMISSION = 16L << 20;
    /// The slots of a new table, and the fewest a table shrinks to.
    private static final int MIN_CAPACITY = 64;
    /// The most slots of a table, as three longs per slot must fit an array. At half of them, 2^28 entries of at least a
    /// page each, a terabyte of heap, the cache evicts rather than grow.
    private static final int MAX_CAPACITY = 1 << 29;
    /// The tag of a slot no entry used since its table was built; an entry's tag is odd ([Table#tag(long)]).
    private static final int EMPTY = 0;
    /// The tag of a slot whose entry left, which a probe passes over.
    private static final int TOMBSTONE = 2;
    /// The references [#removeAll(int, List)] drops per hold of the lock: at a probe or two each, a fraction of a
    /// millisecond.
    private static final int REMOVE_CHUNK = 2_048;
    /// The flag of an entry a read used since the clock last passed it.
    private static final byte REFERENCED = 1;
    /// The flag a tick of the idle ticker sets and a read clears: a tick that finds it set evicts the entry.
    private static final byte IDLE = 2;
    /// 2^64 over the golden ratio: the product of a key by it gives the key's tag and the stride of its probe.
    private static final long HASH = 0x9E3779B97F4A7C15L;
    /// 2^32 over the golden ratio: how far apart, in slots, the pages of each file start, so that the same page of two
    /// files rarely shares a slot.
    private static final int FILE_OFFSET = 0x9E3779B9;
    private static final VarHandle TAGS = MethodHandles.arrayElementVarHandle(int[].class);
    private static final VarHandle META = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle UNITS = MethodHandles.arrayElementVarHandle(byte[][].class);
    /// The slots of the table of the refused units at its first refusal, 1 KiB of references, so a store that reads
    /// little keeps little.
    static final int MIN_REFUSALS = 256;
    /// The most slots of the table of the refused units: it doubles while it took more refusals since it was built than
    /// a quarter of its slots, so it keeps about four slots a unit up to this many, in pairs, and a second read
    /// thousands of refusals after the first seldom finds both slots of its pair taken by newer units. 64 KiB of
    /// references, and as many references as slots at most, as the slot of a unit the collector took is emptied at the
    /// next refusal.
    static final int MAX_REFUSALS = 16_384;
    private static final VarHandle REFUSED = MethodHandles.arrayElementVarHandle(Refused[].class);

    // Guards the changes to the table and to closed
    private final Object lock;
    // Replaced whole by a rebuild, which leaves the old one as it was for the readers still on it
    private volatile Table table;
    private final AtomicLong bytes;
    private final Budget budget;
    // Set once the store closes, under the lock: a read that ends after it caches nothing, or its bytes would stay in
    // the budget
    private volatile boolean closed;
    // The units screened reads missed and the clock evicted
    private final Doorkeeper doorkeeper;
    // Whether the cache screens what reads miss, which the data file turns on
    private volatile boolean screening;
    // The arrays of the units the cache refused lately, held weakly, by a hash of their key and nonce; null until the
    // first refusal and after the close
    private volatile Refused[] refused;
    // The refusals since the table of the refused units was built
    private final AtomicInteger refusals;
    // The refused units whose arrays the collector took, whose slots the next refusal empties
    private final ReferenceQueue<byte[]> collected;

    private PageCache(Budget budget) {
        this.lock = new Object();
        this.table = new Table(MIN_CAPACITY);
        this.bytes = new AtomicLong();
        this.budget = budget;
        this.doorkeeper = new Doorkeeper();
        this.refusals = new AtomicInteger();
        this.collected = new ReferenceQueue<>();
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
        if (!holds(unit.length)) {
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

    /// Caches `unit`, the plaintext of the unit `ref` of the data file names, as a read that missed it, if the cache
    /// [admits][#admits(long, PageRef)] it or it is a branch.
    ///
    /// @param unit an array as [#put(long, PageRef, byte\[\], boolean)] requires
    void putScreened(PageRef ref, byte[] unit) {
        putScreened(dataKey(ref.page(), ref.slot()), ref, unit);
    }

    /// Caches `unit`, the plaintext of the unit `ref` names, at `key`, as a read that missed it, as an entry a read
    /// used, if the cache [admits][#admits(long, PageRef)] it or it is a branch; otherwise the read that missed it
    /// keeps the array, which the cache holds only weakly, for a [recall][#recall(long, PageRef)].
    ///
    /// @param key a key of [#key(int, int)] or [#dataKey(int, int)]
    /// @param unit an array as [#put(long, PageRef, byte\[\], boolean)] requires
    void putScreened(long key, PageRef ref, byte[] unit) {
        if (unit[PageFormat.KIND] == PageFormat.BRANCH || admits(key, ref)) {
            put(key, ref, unit, true);
        } else {
            refuse(key, ref, unit);
        }
    }

    /// {@return whether a read that missed the unit `ref` names, at `key`, caches what it reads}
    ///
    /// A cache the data file did not [turn screening on][#screen(boolean)] for always does. One that screens does if
    /// the doorkeeper saw the unit, read or evicted, since its generations last forgot it, or takes it for one it saw,
    /// at most 4% of the units it never saw; otherwise the doorkeeper records it and the cache drops the entry it held
    /// at `key` for another unit, which no read finds any more. A cache that closed records nothing.
    boolean admits(long key, PageRef ref) {
        if (!screening) {
            return true;
        }
        if (closed) {
            return false;
        }
        if (doorkeeper.admit(key, ref.nonceHigh(), doorkeeperKeys())) {
            return true;
        }
        removeOther(key, ref);
        return false;
    }

    /// {@return whether the cache screens what reads miss and its doorkeeper saw the unit `ref` of the data file names:
    /// a read of the file or the clock's eviction of it, which leaves its pages in the OS cache, or one of the units
    /// it takes for seen ones}
    boolean seen(PageRef ref) {
        return screening && !closed && doorkeeper.holds(dataKey(ref.page(), ref.slot()), ref.nonceHigh());
    }

    /// Makes the cache screen what reads miss, or cache it at the first read; a cache no call reached screens nothing.
    void screen(boolean screening) {
        this.screening = screening;
    }

    /// {@return the array of the unit `ref` names, at `key`, if the cache refused it lately and the collector has not
    /// taken it, which the cache then holds as an entry a read used; or `null`}
    ///
    /// A read that missed the unit and would cache what it reads as read asks for it before it reads the file: the
    /// unit's second read, which the cache admits, or a lookup of a commit, then neither reads nor opens it again, nor
    /// reads the base of a delta record's leaf. It takes no lock and allocates nothing when it finds nothing.
    ///
    /// @param key a key of [#key(int, int)] or [#dataKey(int, int)]
    byte[] recall(long key, PageRef ref) {
        var refused = this.refused;
        if (refused == null) {
            return null;
        }
        var index = refusal(key, ref.nonceHigh(), refused.length);
        var held = (Refused) REFUSED.getAcquire(refused, index);
        if (held == null || !held.names(key, ref)) {
            held = (Refused) REFUSED.getAcquire(refused, ++index);
            if (held == null || !held.names(key, ref)) {
                return null;
            }
        }
        var unit = held.get();
        if (unit == null) {
            return null;
        }
        REFUSED.compareAndSet(refused, index, held, null);
        put(key, ref, unit, true);
        return unit;
    }

    /// {@return what [#recall(long, PageRef)] finds for the unit `ref` of the data file names, or the leaf its delta
    /// record makes}
    byte[] recall(PageRef ref) {
        return recall(dataKey(ref.page(), ref.slot()), ref);
    }

    /// Keeps `unit`, which the cache refused at `key`, reachable weakly for a [recall][#recall(long, PageRef)], in
    /// the first of the two slots of its key and nonce, whose unit moves to the second in place of the one there,
    /// unless either is free.
    ///
    /// @param unit an array as [#put(long, PageRef, byte\[\], boolean)] requires
    void refuse(long key, PageRef ref, byte[] unit) {
        var refused = this.refused;
        if (refused == null) {
            refused = refusals();
            if (refused == null) {
                return;
            }
        }
        forgetCollected();
        place(refused, new Refused(unit, key, ref, collected));
        if (refusals.incrementAndGet() > refused.length / 4 && refused.length < MAX_REFUSALS) {
            grow(refused);
        }
    }

    /// {@return the slots of the refused units, allocated at the first refusal, or `null` once the cache closed}
    private Refused[] refusals() {
        synchronized (lock) {
            if (refused == null && !closed) {
                refused = new Refused[MIN_REFUSALS];
                refusals.set(0);
            }
            return refused;
        }
    }

    /// Empties the slots of the refused units whose arrays the collector took, so the table never keeps more
    /// references than the units it can still give back and those the last collection took.
    private void forgetCollected() {
        Object gone;
        while ((gone = collected.poll()) != null) {
            var refused = this.refused;
            if (refused == null) {
                return;
            }
            var held = (Refused) gone;
            var slot = refusal(held.key, held.nonceHigh, refused.length);
            if (!REFUSED.compareAndSet(refused, slot, held, null)) {
                REFUSED.compareAndSet(refused, slot + 1, held, null);
            }
        }
    }

    /// Replaces `from`, the table of the refused units, by one of twice its slots holding the units whose arrays the
    /// collector has not taken; a refusal that races it may be lost, which costs that unit one more read.
    private void grow(Refused[] from) {
        synchronized (lock) {
            if (refused != from) {
                return;
            }
            var to = new Refused[from.length * 2];
            var held = 0;
            // The second slot of each pair first, so the units that moved there stay behind the newer ones
            for (var slot = 1; slot >= 0; slot--) {
                for (var pair = slot; pair < from.length; pair += 2) {
                    var unit = from[pair];
                    if (unit != null && !unit.refersTo(null)) {
                        place(to, unit);
                        held++;
                    }
                }
            }
            refused = to;
            refusals.set(held);
        }
    }

    /// Puts `unit` in the first slot of its pair in `refused`, moving the unit there to the second slot unless the
    /// first is free or holds a unit the collector took; races lose a unit at worst, which costs it one more read.
    private static void place(Refused[] refused, Refused unit) {
        var slot = refusal(unit.key, unit.nonceHigh, refused.length);
        var first = (Refused) REFUSED.getAcquire(refused, slot);
        if (first != null && !first.refersTo(null)) {
            REFUSED.setRelease(refused, slot + 1, first);
        }
        REFUSED.setRelease(refused, slot, unit);
    }

    /// {@return the first of the two slots, in a table of `slots` slots, of the unit at `key` sealed under a nonce
    /// whose first 8 bytes are `nonceHigh`}
    private static int refusal(long key, long nonceHigh, int slots) {
        return (int) Doorkeeper.hash(key, nonceHigh) & (slots - 2);
    }

    /// The array of a unit the cache refused, held weakly, and what names the unit: its key and the nonce and pages it
    /// was sealed with, which no other unit shares, as a nonce is never used twice. Named by its fields rather than by
    /// its header, as the leaf of a delta record has the header of its base.
    private static final class Refused extends WeakReference<byte[]> {
        private final long key;
        private final long nonceHigh;
        private final long identity;

        Refused(byte[] unit, long key, PageRef ref, ReferenceQueue<byte[]> collected) {
            super(unit, collected);
            this.key = key;
            this.nonceHigh = ref.nonceHigh();
            this.identity = identity(ref.nonceLow(), ref.pages());
        }

        /// {@return whether it holds the unit `ref` names at `key`}
        boolean names(long key, PageRef ref) {
            return this.key == key && nonceHigh == ref.nonceHigh() && identity == identity(ref.nonceLow(), ref.pages());
        }
    }

    /// {@return the most keys a generation of the doorkeeper holds: one per page of the limit, as a power of two}
    private int doorkeeperKeys() {
        var pages = Math.min(budget.limit() / PageFormat.SIZE, Doorkeeper.MAX_KEYS);
        return Integer.highestOneBit((int) Math.max(Doorkeeper.FIRST_KEYS, pages));
    }

    /// Drops the entry at `key` if it holds a unit other than the one `ref` names.
    ///
    /// A key no slot tags returns without the lock. The entry of `ref` itself stays: of two reads that missed it at
    /// once, one may find the doorkeeper saw it, through the other, and cache it before the other gets here.
    private void removeOther(long key, PageRef ref) {
        if (table.find(key) < 0) {
            return;
        }
        synchronized (lock) {
            var table = this.table;
            var slot = table.locate(key);
            if (slot >= 0 && !table.holdsUnit(slot, ref)) {
                drop(table, slot);
            }
        }
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
            if (slot >= 0 && table.holdsNonce(slot, ref)) {
                drop(table, slot);
            }
        }
    }

    /// Drops the units of `file`, [#DATA] or [#BLOBS], that the lists of `released` name, and for the data file the
    /// leaves of the delta records they name, if the cache holds them; a unit of another nonce the cache holds for one
    /// of their pages stays.
    ///
    /// It takes the lock once per [#REMOVE_CHUNK] references, and a reference whose key no slot tags costs no hold of
    /// it: a checkpoint frees hundreds of thousands of units, and holding the lock for all of them at once would keep
    /// every read that misses from caching what it read for as long.
    void removeAll(int file, List<List<PageRef>> released) {
        var held = new long[REMOVE_CHUNK];
        var refs = new PageRef[REMOVE_CHUNK];
        var count = 0;
        for (var list : released) {
            for (var ref : list) {
                var key = file == DATA ? dataKey(ref.page(), ref.slot()) : key(file, ref.page());
                if (table.find(key) < 0) {
                    continue;
                }
                held[count] = key;
                refs[count++] = ref;
                if (count == REMOVE_CHUNK) {
                    removeHeld(held, refs, count);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            removeHeld(held, refs, count);
        }
    }

    /// Drops, holding the lock once, the units the first `count` of `refs` name at the keys of `keys`.
    private void removeHeld(long[] keys, PageRef[] refs, int count) {
        synchronized (lock) {
            var table = this.table;
            for (var i = 0; i < count; i++) {
                var slot = table.locate(keys[i]);
                if (slot >= 0 && table.holdsNonce(slot, refs[i])) {
                    drop(table, slot);
                }
                refs[i] = null;
            }
        }
    }

    /// Drops whatever the cache holds at `key`.
    ///
    /// A key no slot tags returns without the lock, as a checkpoint drops the place of every unit it writes and does
    /// not cache: a put of `key` that happened before the call is seen, and one that races it may stay either way.
    void remove(long key) {
        if (table.find(key) < 0) {
            return;
        }
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
            refused = null;
            dropAll();
        }
        doorkeeper.close();
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

    /// {@return whether the cache would keep a unit of `bytes` bytes: one above a quarter of the limit it never does}
    boolean holds(long bytes) {
        return bytes <= budget.limit() / 4;
    }

    /// {@return the bytes of leaves and delta pages one checkpoint may cache at the cost of evicting others}
    ///
    /// Half of this cache's share of the limit, so a checkpoint that writes more than the cache holds leaves it the
    /// units it wrote first and the other half to the reads, rather than a turnover of units no one reads before the
    /// clock takes them. At least [#MIN_ADMISSION] while that is at most half the limit, as a process of many stores
    /// would otherwise cache next to nothing of what each of them writes.
    long admission() {
        return Math.max(budget.share() / 2, Math.min(budget.limit() / 2, MIN_ADMISSION));
    }

    /// {@return whether the cache holds the unit `ref` of the data file names, or the leaf its delta record makes, as
    /// an entry a read used since the clock last passed it; for tests of what a checkpoint promotes}
    boolean referencedForTests(PageRef ref) {
        var key = dataKey(ref.page(), ref.slot());
        synchronized (lock) {
            var table = this.table;
            var slot = table.locate(key);
            return slot >= 0 && table.holdsNonce(slot, ref) && (table.flags[slot] & REFERENCED) != 0;
        }
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

    /// {@return the doorkeeper of the cache, for tests that bound or count it}
    Doorkeeper doorkeeperForTests() {
        return doorkeeper;
    }

    /// {@return the slots of the table of the refused units, 0 before the first refusal; for tests that bound it}
    int refusalSlotsForTests() {
        var refused = this.refused;
        return refused == null ? 0 : refused.length;
    }

    /// {@return the slots of the table of the refused units that hold one; for tests that bound it}
    int refusedForTests() {
        var refused = this.refused;
        var held = 0;
        if (refused != null) {
            for (var slot = 0; slot < refused.length; slot++) {
                if (REFUSED.getAcquire(refused, slot) != null) {
                    held++;
                }
            }
        }
        return held;
    }

    /// {@return the slots of the table, for tests that bound it}
    int capacityForTests() {
        return table.capacity();
    }

    /// {@return the first slot of the probe of `key` in a table of `capacity` slots, for tests that make two keys share
    /// it}
    static int homeForTests(long key, int capacity) {
        return Table.home(key, capacity - 1);
    }

    /// {@return the slots whose entry left and that no entry reused, for tests that bound them}
    int tombstonesForTests() {
        synchronized (lock) {
            return table.clock.tombstones;
        }
    }

    /// Drops every entry and forgets the units it refused, so tests read units from the file.
    void clearForTests() {
        synchronized (lock) {
            refused = null;
            refusals.set(0);
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
                // A unit the clock pushed out goes back in at its next read. One the ticker found idle needs none: its
                // bits stay set unless the doorkeeper forgot them since
                doorkeeper.evicted(table.meta[3 * slot], table.meta[3 * slot + 1], doorkeeperKeys());
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

    /// The slots of a cache, in arrays a key's probe visits from the slot of its page ([#home(long, int)]) by a stride
    /// of its hash ([#stride(long)]).
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

        /// {@return the first slot of the probe of `key` in a table whose slots less one are `mask`: the slot of its
        /// page, offset by its file}
        static int home(long key, int mask) {
            return (int) key + (int) (key >>> 32) * FILE_OFFSET & mask;
        }

        /// {@return the step of the probe of the key whose product by [#HASH] is `hash`}
        ///
        /// It is odd, so a probe passes every slot before any twice, and it is the key's own, so the keys of a run of
        /// slots other keys took first go on to slots all over the table rather than one after another along the run.
        int stride(long hash) {
            return (int) (hash >>> shift) | 1;
        }

        /// {@return the slot whose tag is that of `key`, or -1; one that holds another unit, or that a writer is changing,
        /// fails [#unit(int, long, long, int, int)]}
        int find(long key) {
            var tags = this.tags;
            var mask = this.mask;
            var hash = key * HASH;
            var tag = tag(hash);
            var slot = home(key, mask);
            var stride = stride(hash);
            for (var probes = 0; probes <= mask; probes++) {
                var found = (int) TAGS.getOpaque(tags, slot);
                if (found == tag) {
                    return slot;
                }
                if (found == EMPTY) {
                    return -1;
                }
                slot = (slot + stride) & mask;
            }
            return -1;
        }

        /// {@return the slot that holds `key`, or -1, holding the lock of the cache or on a table not published yet}
        int locate(long key) {
            var hash = key * HASH;
            var tag = tag(hash);
            var slot = home(key, mask);
            var stride = stride(hash);
            for (var probes = 0; probes <= mask; probes++) {
                var found = tags[slot];
                if (found == tag && meta[3 * slot] == key) {
                    return slot;
                }
                if (found == EMPTY) {
                    return -1;
                }
                slot = (slot + stride) & mask;
            }
            return -1;
        }

        /// {@return whether the entry of `slot` was cached under the nonce of `ref`, holding the lock of the cache}
        boolean holdsNonce(int slot, PageRef ref) {
            return meta[3 * slot + 1] == ref.nonceHigh() && (int) (meta[3 * slot + 2] >>> 32) == ref.nonceLow();
        }

        /// {@return whether the entry of `slot` was cached under the nonce and the pages of `ref`, holding the lock of
        /// the cache}
        boolean holdsUnit(int slot, PageRef ref) {
            return meta[3 * slot + 1] == ref.nonceHigh() && meta[3 * slot + 2] == identity(ref.nonceLow(), ref.pages());
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
            var slot = home(key, mask);
            var stride = stride(key * HASH);
            while (true) {
                var found = tags[slot];
                if (found == EMPTY || found == TOMBSTONE) {
                    return slot;
                }
                slot = (slot + stride) & mask;
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
