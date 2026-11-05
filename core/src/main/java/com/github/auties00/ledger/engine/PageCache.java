package com.github.auties00.ledger.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/// The opened units of the store's files, by file and first page, within one limit of bytes; it evicts with the CLOCK
/// algorithm.
///
/// An entry holds the plaintext of the unit and is never written again, so a reader keeps using an array the cache has
/// evicted: the array goes to the collector, not back to a pool, and no read pins anything. An entry is found only
/// under the nonce its unit was sealed with ([PageRef]), so a page a later checkpoint reused never returns the unit it
/// held before. An entry the cache drops lets go of its array at once, though the clock reaches it later: the memory
/// the cache keeps reachable stays within its limit.
final class PageCache {
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
    private final long limit;

    /// Creates an empty cache of at most `limit` bytes.
    PageCache(long limit) {
        this.entries = new ConcurrentHashMap<>();
        this.clock = new ConcurrentLinkedQueue<>();
        this.clockSize = new AtomicInteger();
        this.bytes = new AtomicLong();
        this.limit = limit;
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
        entry.referenced = true;
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
        if (unit.length > limit / 4) {
            // A unit this large would evict most of the cache for one read; what the page held is out of date
            remove(key);
            return;
        }
        var entry = new Entry(key, ref.nonceHigh(), ref.nonceLow(), unit);
        entry.referenced = referenced;
        bytes.addAndGet(unit.length);
        var previous = entries.put(key, entry);
        if (previous != null) {
            drop(previous);
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
        bytes.addAndGet(-entry.length);
        entry.unit = null;
    }

    /// {@return the bytes the cache holds}
    long size() {
        return bytes.get();
    }

    /// {@return whether the cache holds `bytes` fewer bytes than its limit, so caching them would evict nothing}
    boolean hasRoom(long bytes) {
        return this.bytes.get() + bytes <= limit;
    }

    /// {@return the entries the clock holds, current or dropped, for tests that bound it}
    int clockLengthForTests() {
        return clock.size();
    }

    /// Drops every entry, so tests read units from the file.
    void clearForTests() {
        entries.clear();
        clock.clear();
        clockSize.set(0);
        bytes.set(0);
    }

    /// Evicts entries while the cache holds more than its limit, and takes dropped entries out of the clock once they
    /// outnumber the current ones.
    ///
    /// Each dropped entry costs one step of a later sweep, so the work stays proportional to the puts.
    private void evict() {
        // Every entry gets one more pass when referenced, so twice the clock bounds the sweep
        var budget = 2 * clockSize.get() + 1;
        while (budget-- > 0) {
            var over = bytes.get() > limit;
            if (!over && clockSize.get() <= 2 * entries.size() + SLACK) {
                return;
            }
            var entry = clock.poll();
            if (entry == null) {
                return;
            }
            clockSize.decrementAndGet();
            if (entries.get(entry.key) != entry) {
                // Replaced or removed already
                continue;
            }
            if (over && !entry.referenced) {
                if (entries.remove(entry.key, entry)) {
                    drop(entry);
                }
                continue;
            }
            if (over) {
                entry.referenced = false;
            }
            clock.offer(entry);
            clockSize.incrementAndGet();
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

        Entry(long key, long nonceHigh, int nonceLow, byte[] unit) {
            this.key = key;
            this.nonceHigh = nonceHigh;
            this.nonceLow = nonceLow;
            this.length = unit.length;
            this.unit = unit;
        }
    }
}
