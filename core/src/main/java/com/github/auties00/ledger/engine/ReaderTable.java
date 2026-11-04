package com.github.auties00.ledger.engine;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

/// The sequences that reads in progress and open snapshots read at, so the writer prunes no version one of them can
/// still see.
///
/// A short read claims a slot of a fixed table with one compare-and-set; snapshots, and reads that find every slot
/// busy, register in a locked multiset instead.
///
/// The writer computes its bound by reading the latest sequence before it scans the slots and the multiset, and a
/// read publishes its sequence, then checks the latest sequence is still that one, retrying if not. Either the
/// writer's scan sees the published sequence, or the read's check came after the scan, so the read's sequence is at
/// or past the latest sequence the writer read: no read ever reads at a sequence below the bound.
final class ReaderTable {
    private final AtomicLongArray slots;
    private final int mask;
    private final LongSupplier latest;
    private final Object registryLock;
    // Sequence to the number of registrations at it
    private final TreeMap<Long, Integer> registry;
    // What scan() collects into: only the writer scans, holding the writer lock
    private long[] scanned;

    /// Creates a table of `slots` slots.
    ///
    /// @param slots a power of two
    /// @param latest returns the latest visible sequence
    ReaderTable(int slots, LongSupplier latest) {
        if (Integer.bitCount(slots) != 1) {
            throw new IllegalArgumentException("slots must be a power of two: " + slots);
        }
        this.slots = new AtomicLongArray(slots);
        this.mask = slots - 1;
        this.latest = latest;
        this.registryLock = new Object();
        this.registry = new TreeMap<>();
        this.scanned = new long[slots + 1];
    }

    /// Registers a read at the latest sequence.
    ///
    /// @return the lease to release with [#exit(Lease)]
    Lease enter() {
        var start = (int) Thread.currentThread().threadId() & mask;
        for (var probe = 0; probe <= mask; probe++) {
            var slot = (start + probe) & mask;
            if (slots.get(slot) != 0) {
                continue;
            }
            var sequence = latest.getAsLong();
            // Slots hold the sequence plus one, so zero means free
            if (slots.compareAndSet(slot, 0, sequence + 1)) {
                while (true) {
                    var now = latest.getAsLong();
                    if (now == sequence) {
                        return new Lease(slot, sequence);
                    }
                    sequence = now;
                    slots.set(slot, sequence + 1);
                }
            }
        }
        return new Lease(-1, register());
    }

    /// Releases a read.
    void exit(Lease lease) {
        if (lease.slot >= 0) {
            slots.set(lease.slot, 0);
        } else {
            unregister(lease.sequence);
        }
    }

    /// Registers a snapshot at the latest sequence, which holds until [#unregister(long)].
    ///
    /// @return the sequence of the snapshot
    long register() {
        synchronized (registryLock) {
            var sequence = latest.getAsLong();
            registry.merge(sequence, 1, Integer::sum);
            return sequence;
        }
    }

    /// Releases a registration [#register()] made at `sequence`.
    void unregister(long sequence) {
        synchronized (registryLock) {
            registry.computeIfPresent(sequence, (_, count) -> count == 1 ? null : count - 1);
        }
    }

    /// What one pass over the table found.
    ///
    /// @param active every sequence a read or snapshot may be reading at, distinct, highest first; the first is the
    ///               latest
    /// @param oldest the lowest sequence a read or snapshot is reading at, the latest included, or [Long#MAX_VALUE]
    record Scan(long[] active, long oldest) {
    }

    /// {@return [#active()] and [#oldest()] from one pass over the slots and the registry}
    ///
    /// Only the writer calls it, holding the writer lock.
    Scan scan() {
        var current = latest.getAsLong();
        var sequences = scanned;
        var count = 0;
        sequences[count++] = current;
        var oldest = Long.MAX_VALUE;
        for (var slot = 0; slot <= mask; slot++) {
            var value = slots.get(slot);
            if (value != 0) {
                oldest = Math.min(oldest, value - 1);
                if (value - 1 < current) {
                    sequences[count++] = value - 1;
                }
            }
        }
        synchronized (registryLock) {
            if (!registry.isEmpty()) {
                oldest = Math.min(oldest, registry.firstKey());
            }
            for (var sequence : registry.keySet()) {
                if (sequence >= current) {
                    break;
                }
                if (count == sequences.length) {
                    sequences = Arrays.copyOf(sequences, count * 2);
                    scanned = sequences;
                }
                sequences[count++] = sequence;
            }
        }
        return new Scan(distinctDescending(sequences, count, current), oldest);
    }

    private static long[] distinctDescending(long[] sequences, int count, long current) {
        if (count == 1) {
            return new long[]{current};
        }
        Arrays.sort(sequences, 1, count);
        var result = new long[count];
        result[0] = current;
        var size = 1;
        for (var i = count - 1; i >= 1; i--) {
            if (sequences[i] != result[size - 1]) {
                result[size++] = sequences[i];
            }
        }
        return Arrays.copyOf(result, size);
    }

    /// {@return the lowest sequence a read or snapshot is reading at, or [Long#MAX_VALUE] if none is}
    ///
    /// Unlike [#active()], it counts reads at the latest sequence: they may have pinned the tree published before it.
    /// Only the writer calls it, holding the writer lock.
    long oldest() {
        var oldest = Long.MAX_VALUE;
        for (var slot = 0; slot <= mask; slot++) {
            var value = slots.get(slot);
            if (value != 0) {
                oldest = Math.min(oldest, value - 1);
            }
        }
        synchronized (registryLock) {
            if (!registry.isEmpty()) {
                oldest = Math.min(oldest, registry.firstKey());
            }
        }
        return oldest;
    }

    /// {@return every sequence a read or snapshot may be reading at, distinct, highest first; the first is the latest}
    ///
    /// Only the writer calls it, holding the writer lock, so the latest sequence does not change during the call.
    long[] active() {
        var current = latest.getAsLong();
        var sequences = new long[slots.length() + 1];
        var count = 0;
        sequences[count++] = current;
        for (var slot = 0; slot <= mask; slot++) {
            var value = slots.get(slot);
            if (value != 0 && value - 1 < current) {
                sequences[count++] = value - 1;
            }
        }
        synchronized (registryLock) {
            for (var sequence : registry.keySet()) {
                if (sequence >= current) {
                    break;
                }
                if (count == sequences.length) {
                    sequences = Arrays.copyOf(sequences, count * 2);
                }
                sequences[count++] = sequence;
            }
        }
        Arrays.sort(sequences, 1, count);
        // Highest first, without duplicates
        var result = new long[count];
        result[0] = current;
        var size = 1;
        for (var i = count - 1; i >= 1; i--) {
            if (sequences[i] != result[size - 1]) {
                result[size++] = sequences[i];
            }
        }
        return Arrays.copyOf(result, size);
    }

    /// A registered read: the slot it holds, or -1, and the sequence it reads at.
    ///
    /// @param slot the slot, or -1 for a read registered in the multiset
    /// @param sequence the sequence the read sees
    record Lease(int slot, long sequence) {
    }
}
