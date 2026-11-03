package com.github.auties00.ledger.engine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/// The opened units of the data file, by first page, within a limit of bytes; it evicts with the CLOCK algorithm.
///
/// An entry holds the plaintext of the unit and is never written again, so a reader keeps using an array the cache has
/// evicted: the array goes to the collector, not back to a pool, and no read pins anything. An entry is found only
/// under the nonce its unit was sealed with ([PageRef]), so a page a later checkpoint reused never returns the unit it
/// held before.
final class PageCache {
    private final ConcurrentHashMap<Integer, Entry> entries;
    private final ConcurrentLinkedQueue<Entry> clock;
    private final AtomicLong bytes;
    private final long limit;

    /// Creates an empty cache of at most `limit` bytes.
    PageCache(long limit) {
        this.entries = new ConcurrentHashMap<>();
        this.clock = new ConcurrentLinkedQueue<>();
        this.bytes = new AtomicLong();
        this.limit = limit;
    }

    /// {@return the plaintext of the unit `ref` names, or `null` if the cache does not hold it}
    byte[] get(PageRef ref) {
        var entry = entries.get(ref.page());
        if (entry == null || entry.nonceHigh != ref.nonceHigh() || entry.nonceLow != ref.nonceLow() || entry.unit.length != ref.pages() * PageFormat.SIZE) {
            return null;
        }
        entry.referenced = true;
        return entry.unit;
    }

    /// Caches `unit`, the plaintext of the unit `ref` names, replacing whatever the cache held for its page.
    ///
    /// @param referenced whether the unit counts as read, rather than written and maybe never read
    void put(PageRef ref, byte[] unit, boolean referenced) {
        if (unit.length > limit / 4) {
            // A unit this large would evict most of the cache for one read
            return;
        }
        var entry = new Entry(ref.page(), ref.nonceHigh(), ref.nonceLow(), unit);
        entry.referenced = referenced;
        var previous = entries.put(ref.page(), entry);
        if (previous != null) {
            bytes.addAndGet(-previous.unit.length);
        }
        bytes.addAndGet(unit.length);
        clock.offer(entry);
        evict();
    }

    /// Drops whatever the cache holds for page `page`.
    void remove(int page) {
        var previous = entries.remove(page);
        if (previous != null) {
            bytes.addAndGet(-previous.unit.length);
        }
    }

    /// {@return the bytes the cache holds}
    long size() {
        return bytes.get();
    }

    /// Drops every entry, so tests read units from the file.
    void clearForTests() {
        entries.clear();
        clock.clear();
        bytes.set(0);
    }

    private void evict() {
        // Every entry gets one more pass when referenced, so twice the entries bound the sweep
        var budget = 2 * entries.size() + 1;
        while (bytes.get() > limit && budget-- > 0) {
            var entry = clock.poll();
            if (entry == null) {
                return;
            }
            if (entries.get(entry.page) != entry) {
                // Replaced or removed already
                continue;
            }
            if (entry.referenced) {
                entry.referenced = false;
                clock.offer(entry);
            } else if (entries.remove(entry.page, entry)) {
                bytes.addAndGet(-entry.unit.length);
            }
        }
    }

    private static final class Entry {
        final int page;
        final long nonceHigh;
        final int nonceLow;
        final byte[] unit;
        volatile boolean referenced;

        Entry(int page, long nonceHigh, int nonceLow, byte[] unit) {
            this.page = page;
            this.nonceHigh = nonceHigh;
            this.nonceLow = nonceLow;
            this.unit = unit;
        }
    }
}
