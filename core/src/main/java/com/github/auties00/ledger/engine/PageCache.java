package com.github.auties00.ledger.engine;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
/// An entry holds the plaintext of the unit and is never written again, so a reader keeps using an array the cache has
/// evicted: the array goes to the collector, not back to a pool, and no read pins anything. An entry is found only
/// under the nonce its unit was sealed with ([PageRef]), so a page a later checkpoint reused never returns the unit it
/// held before. An entry the cache drops lets go of its array at once, though the clock reaches it later: the memory
/// the cache keeps reachable stays within its limit.
///
/// An entry no read used for a whole period of the [IdleTicker] leaves at its next tick, however full the caches are:
/// the clock evicts only past the limit, so a store that stopped reading kept a limit of pages, mostly the ones its last
/// checkpoint wrote and no one read, for as long as it stayed open.
final class PageCache implements IdleTicker.Watched {
    /// The dropped entries the clock may hold beyond the current ones before a sweep takes them out.
    private static final int SLACK = 64;
    /// The file of the trees, `ledger.data`.
    static final int DATA = 0;
    /// The file of the values written at commit, `ledger.blobs`.
    static final int BLOBS = 1;
    /// The first of the keys of the leaves delta records make, one per slot of a delta page of `ledger.data`.
    private static final int RECORDS = 2;

    private final ConcurrentHashMap<Long, Entry> entries;
    // The entries in eviction order, current or dropped
    private final ConcurrentLinkedQueue<Entry> clock;
    // The entries in the clock, which may briefly differ from its length while threads race
    private final AtomicInteger clockSize;
    private final AtomicLong bytes;
    private final Budget budget;
    // Set once the store closes: a read that ends after it caches nothing, or its bytes would stay in the budget
    private volatile boolean closed;

    private PageCache(Budget budget) {
        this.entries = new ConcurrentHashMap<>();
        this.clock = new ConcurrentLinkedQueue<>();
        this.clockSize = new AtomicInteger();
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

    /// {@return the plaintext of the unit `ref` of the data file names, or of the leaf its delta record makes, or
    /// `null` if the cache does not hold it}
    byte[] get(PageRef ref) {
        return get(key(ref), ref);
    }

    /// {@return the key of the unit `ref` of the data file names, or of the leaf its delta record makes}
    private static long key(PageRef ref) {
        return key(ref.isRecord() ? RECORDS + ref.slot() : DATA, ref.page());
    }

    /// {@return the plaintext of the unit `ref` of the data file names if the cache holds it, or `null`, which counts as
    /// no read of it: the clock and the idle ticker treat the entry as they would have}
    byte[] peek(PageRef ref) {
        var entry = entries.get(key(ref));
        if (entry == null || entry.nonceHigh != ref.nonceHigh() || entry.nonceLow != ref.nonceLow()) {
            return null;
        }
        var unit = entry.unit;
        return unit == null || unit.length != ref.pages() * PageFormat.SIZE ? null : unit;
    }

    /// {@return the plaintext of the unit `ref` names, at `key`, or `null` if the cache does not hold it}
    byte[] get(long key, PageRef ref) {
        var entry = entries.get(key);
        if (entry == null || entry.nonceHigh != ref.nonceHigh() || entry.nonceLow != ref.nonceLow()) {
            return null;
        }
        var unit = entry.unit;
        if (unit == null || unit.length != ref.pages() * PageFormat.SIZE) {
            return null;
        }
        // Written only when they change: a volatile store on every hit made the readers of one page, the root
        // first, take its entry's line from each other
        if (!entry.referenced) {
            entry.referenced = true;
        }
        if (entry.idle) {
            entry.idle = false;
        }
        return unit;
    }

    /// Caches `unit`, the plaintext of the unit `ref` of the data file names, replacing whatever the cache held for its
    /// page.
    ///
    /// @param referenced whether the unit counts as read, rather than written and maybe never read
    void put(PageRef ref, byte[] unit, boolean referenced) {
        put(key(ref), ref, unit, referenced);
    }

    /// Caches `unit`, the plaintext of the unit `ref` names, at `key`, replacing whatever the cache held there.
    ///
    /// @param referenced whether the unit counts as read, rather than written and maybe never read
    void put(long key, PageRef ref, byte[] unit, boolean referenced) {
        if (unit.length > budget.limit() / 4) {
            // A unit this large would evict most of the cache for one read; what the page held is out of date
            remove(key);
            return;
        }
        if (closed) {
            return;
        }
        var entry = new Entry(key, ref.nonceHigh(), ref.nonceLow(), unit);
        entry.referenced = referenced;
        account(unit.length);
        var previous = entries.put(key, entry);
        if (previous != null) {
            drop(previous);
        }
        if (closed) {
            // The close may have looked at the entries before this one went in
            if (entries.remove(key, entry)) {
                drop(entry);
            }
            return;
        }
        clock.offer(entry);
        clockSize.incrementAndGet();
        evict();
    }

    /// Drops whatever the cache holds for page `page` of the data file.
    void remove(int page) {
        remove(key(DATA, page));
    }

    /// Drops whatever the cache holds at `key`.
    void remove(long key) {
        var previous = entries.remove(key);
        if (previous != null) {
            drop(previous);
        }
    }

    /// Accounts for `entry`, which this thread took out of the map, and lets go of its array.
    private void drop(Entry entry) {
        account(-entry.length);
        entry.unit = null;
    }

    private void account(long delta) {
        bytes.addAndGet(delta);
        budget.total.addAndGet(delta);
    }

    /// Drops every entry and leaves the budget, as the store closes; a read that ends after it caches nothing.
    void close() {
        closed = true;
        budget.leave(this);
        IdleTicker.unwatch(this);
        for (var entry : entries.values()) {
            if (entries.remove(entry.key, entry)) {
                drop(entry);
            }
        }
        clock.clear();
        clockSize.set(0);
    }

    /// Evicts the entries no read used since the last tick, and marks the others to be evicted at the next unless a read
    /// uses them; then takes the dropped entries out of the clock.
    @Override
    public void tick() {
        for (var entry : entries.values()) {
            if (!entry.idle) {
                entry.idle = true;
            } else if (entries.remove(entry.key, entry)) {
                drop(entry);
            }
        }
        compact();
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

    /// {@return the entries the clock holds, current or dropped, for tests that bound it}
    int clockLengthForTests() {
        return clock.size();
    }

    /// {@return the keys of the current entries, each with whether it counts as read, for tests that compare the state
    /// of the clock}
    Map<Long, Boolean> referencedForTests() {
        var referenced = new java.util.TreeMap<Long, Boolean>();
        for (var entry : entries.values()) {
            referenced.put(entry.key, entry.referenced);
        }
        return referenced;
    }

    /// Drops every entry, so tests read units from the file.
    void clearForTests() {
        for (var entry : entries.values()) {
            if (entries.remove(entry.key, entry)) {
                drop(entry);
            }
        }
        clock.clear();
        clockSize.set(0);
    }

    /// Evicts entries while the caches of the budget hold more than its limit, from this cache while it holds more
    /// than its share and from the one that holds the most otherwise, and takes dropped entries out of the clock once
    /// they outnumber the current ones.
    ///
    /// Each dropped entry costs one step of a later sweep, so the work stays proportional to the puts.
    private void evict() {
        while (budget.over()) {
            var target = bytes.get() > budget.share() ? this : budget.largest();
            if ((target == null || !target.evictOne()) && (target == this || !evictOne())) {
                break;
            }
        }
        compact();
    }

    /// Evicts the next entry the clock finds unreferenced, giving each referenced one another pass.
    ///
    /// @return whether it evicted one; `false` if the clock holds none it could
    private boolean evictOne() {
        // Every entry gets one more pass when referenced, so twice the clock bounds the sweep
        var steps = 2 * clockSize.get() + 1;
        while (steps-- > 0) {
            var entry = clock.poll();
            if (entry == null) {
                return false;
            }
            clockSize.decrementAndGet();
            if (entries.get(entry.key) != entry) {
                // Replaced or removed already
                continue;
            }
            if (!entry.referenced) {
                if (entries.remove(entry.key, entry)) {
                    drop(entry);
                    return true;
                }
                continue;
            }
            entry.referenced = false;
            clock.offer(entry);
            clockSize.incrementAndGet();
        }
        return false;
    }

    /// Takes dropped entries out of the clock once they outnumber the current ones.
    private void compact() {
        var steps = 2 * clockSize.get() + 1;
        while (steps-- > 0 && clockSize.get() > 2 * entries.size() + SLACK) {
            var entry = clock.poll();
            if (entry == null) {
                return;
            }
            clockSize.decrementAndGet();
            if (entries.get(entry.key) == entry) {
                clock.offer(entry);
                clockSize.incrementAndGet();
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

    private static final class Entry {
        final long key;
        final long nonceHigh;
        final int nonceLow;
        final int length;
        // The plaintext, or null once the cache dropped the entry
        volatile byte[] unit;
        volatile boolean referenced;
        // Set by a tick of the idle ticker, cleared by a read: a tick that finds it set evicts the entry. Plain, as a
        // stale read only delays an eviction by a tick or evicts an entry just read, which the next read caches again
        boolean idle;

        Entry(long key, long nonceHigh, int nonceLow, byte[] unit) {
            this.key = key;
            this.nonceHigh = nonceHigh;
            this.nonceLow = nonceLow;
            this.length = unit.length;
            this.unit = unit;
        }
    }
}
