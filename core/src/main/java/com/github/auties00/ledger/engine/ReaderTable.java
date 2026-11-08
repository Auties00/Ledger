package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.LongSupplier;

/// The sequences that reads in progress and open snapshots read at, so the writer prunes no version one of them can
/// still see.
///
/// A short read claims a slot of a fixed table with one compare-and-set; snapshots, and reads that find every slot
/// busy, register in a locked multiset instead.
///
/// The build of a checkpoint registers on its own ([#registerCheckpoint()]): it needs the versions at its sequence, as
/// a snapshot does, but reads only the trees of the checkpoint before it and the versions above them, so it keeps no
/// older tree in use, nor the versions only an older tree lacks.
///
/// The writer computes its bound by reading the latest sequence before it scans the slots and the multiset, and a
/// read publishes its sequence, then checks the latest sequence is still that one, retrying if not. Either the
/// writer's scan sees the published sequence, or the read's check came after the scan, so the read's sequence is at
/// or past the latest sequence the writer read: no read ever reads at a sequence below the bound.
///
/// The writer finds the busy slots by comparing the table with one of zeros, which the processor does several slots at
/// a time, then reads each busy slot again. Those comparisons are plain reads, made after the volatile read of the
/// latest sequence, which the writer stored with a volatile store: they cannot see the table as it was before either.
final class ReaderTable {
    private static final VarHandle SLOTS = MethodHandles.arrayElementVarHandle(long[].class);

    private final long[] slots;
    // As long as slots, and all zeros
    private final long[] free;
    private final int mask;
    private final LongSupplier latest;
    private final Object registryLock;
    // Sequence to the number of registrations at it
    private final TreeMap<Long, Integer> registry;
    // The registrations in the registry, or about to be: raised before a registration reads the latest sequence, so a
    // writer that reads it zero after reading the latest sequence skips the registry, as it would skip a free slot
    private volatile int registrations;
    // The sequence the build of a checkpoint reads at, or -1 if none is building; guarded by registryLock, and counted
    // in registrations
    private long checkpointRead;
    // What scan() collects into: only the holder of the store's publishing role scans
    private long[] scanned;

    /// Creates a table of `slots` slots.
    ///
    /// @param slots a power of two
    /// @param latest returns the latest visible sequence
    ReaderTable(int slots, LongSupplier latest) {
        if (Integer.bitCount(slots) != 1) {
            throw new IllegalArgumentException("slots must be a power of two: " + slots);
        }
        this.slots = new long[slots];
        this.free = new long[slots];
        this.mask = slots - 1;
        this.latest = latest;
        this.registryLock = new Object();
        this.registry = new TreeMap<>();
        this.checkpointRead = -1;
        this.scanned = new long[slots + 1];
    }

    /// Registers a read at the latest sequence.
    ///
    /// @return the lease to release with [#exit(Lease)]
    Lease enter() {
        var start = (int) Thread.currentThread().threadId() & mask;
        for (var probe = 0; probe <= mask; probe++) {
            var slot = (start + probe) & mask;
            if ((long) SLOTS.getOpaque(slots, slot) != 0) {
                continue;
            }
            var sequence = latest.getAsLong();
            // Slots hold the sequence plus one, so zero means free
            if (SLOTS.compareAndSet(slots, slot, 0L, sequence + 1)) {
                while (true) {
                    var now = latest.getAsLong();
                    if (now == sequence) {
                        return new Lease(slot, sequence);
                    }
                    sequence = now;
                    SLOTS.setVolatile(slots, slot, sequence + 1);
                }
            }
        }
        return new Lease(-1, register());
    }

    /// Releases a read.
    void exit(Lease lease) {
        if (lease.slot >= 0) {
            // A writer that still sees the slot busy keeps a version longer, nothing worse
            SLOTS.setRelease(slots, lease.slot, 0L);
        } else {
            unregister(lease.sequence);
        }
    }

    /// Registers a snapshot at the latest sequence, which holds until [#unregister(long)].
    ///
    /// @return the sequence of the snapshot
    long register() {
        synchronized (registryLock) {
            registrations++;
            var sequence = latest.getAsLong();
            registry.merge(sequence, 1, Integer::sum);
            return sequence;
        }
    }

    /// Releases a registration [#register()] made at `sequence`.
    void unregister(long sequence) {
        synchronized (registryLock) {
            var count = registry.get(sequence);
            if (count != null) {
                if (count == 1) {
                    registry.remove(sequence);
                } else {
                    registry.put(sequence, count - 1);
                }
                registrations--;
            }
        }
    }

    /// Registers the read of a checkpoint's build at the latest sequence, which holds until [#unregisterCheckpoint()].
    /// Only one checkpoint builds at a time.
    ///
    /// Like a snapshot it is among the sequences a pass finds active, so the versions at its sequence stay, but it is
    /// not the oldest reader: the build reads the trees it updates, which the next publication alone replaces, and the
    /// chains written since their cut.
    ///
    /// @return the sequence of the read
    long registerCheckpoint() {
        synchronized (registryLock) {
            registrations++;
            checkpointRead = latest.getAsLong();
            return checkpointRead;
        }
    }

    /// Releases the read [#registerCheckpoint()] registered.
    void unregisterCheckpoint() {
        synchronized (registryLock) {
            checkpointRead = -1;
            registrations--;
        }
    }

    /// What one pass over the table found.
    ///
    /// @param active every sequence a read, a snapshot or a checkpoint's build may be reading at, distinct, highest
    ///               first; the first is the latest
    /// @param oldest the lowest sequence a read or snapshot is reading at, the latest included, or [Long#MAX_VALUE]; the
    ///               trees older than the newest one published before it, and what they alone use, have no reader
    record Scan(long[] active, long oldest) {
    }

    /// {@return [#active()] and [#oldest()] from one pass over the slots and the registry}
    ///
    /// Only the holder of the store's publishing role calls it.
    Scan scan() {
        var current = latest.getAsLong();
        var sequences = scanned;
        var count = 0;
        sequences[count++] = current;
        var oldest = Long.MAX_VALUE;
        for (var slot = busy(0); slot >= 0; slot = busy(slot + 1)) {
            var value = (long) SLOTS.getVolatile(slots, slot);
            if (value != 0) {
                oldest = Math.min(oldest, value - 1);
                if (value - 1 < current) {
                    sequences[count++] = value - 1;
                }
            }
        }
        if (registrations != 0) {
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
                if (checkpointRead >= 0 && checkpointRead < current) {
                    if (count == sequences.length) {
                        sequences = Arrays.copyOf(sequences, count * 2);
                        scanned = sequences;
                    }
                    sequences[count++] = checkpointRead;
                }
            }
        }
        return new Scan(distinctDescending(sequences, count, current), oldest);
    }

    /// {@return [#active()] and [#oldest()] from one pass over the slots and the registry, safe for concurrent use}
    ///
    /// Unlike [#scan()], it collects into memory of its own, which it allocates only for the busy slots. A thread
    /// that stored the latest sequence itself must have stored it with a volatile store, as the publisher does.
    Scan scanConcurrently() {
        var current = latest.getAsLong();
        var oldest = Long.MAX_VALUE;
        long[] sequences = null;
        var count = 1;
        for (var slot = busy(0); slot >= 0; slot = busy(slot + 1)) {
            var value = (long) SLOTS.getVolatile(slots, slot);
            if (value != 0) {
                oldest = Math.min(oldest, value - 1);
                if (value - 1 < current) {
                    if (sequences == null) {
                        sequences = new long[8];
                        sequences[0] = current;
                    } else if (count == sequences.length) {
                        sequences = Arrays.copyOf(sequences, count * 2);
                    }
                    sequences[count++] = value - 1;
                }
            }
        }
        if (registrations != 0) {
            synchronized (registryLock) {
                if (!registry.isEmpty()) {
                    oldest = Math.min(oldest, registry.firstKey());
                }
                for (var sequence : registry.keySet()) {
                    if (sequence >= current) {
                        break;
                    }
                    if (sequences == null) {
                        sequences = new long[8];
                        sequences[0] = current;
                    } else if (count == sequences.length) {
                        sequences = Arrays.copyOf(sequences, count * 2);
                    }
                    sequences[count++] = sequence;
                }
                if (checkpointRead >= 0 && checkpointRead < current) {
                    if (sequences == null) {
                        sequences = new long[8];
                        sequences[0] = current;
                    } else if (count == sequences.length) {
                        sequences = Arrays.copyOf(sequences, count * 2);
                    }
                    sequences[count++] = checkpointRead;
                }
            }
        }
        if (sequences == null) {
            return new Scan(new long[]{current}, oldest);
        }
        return new Scan(distinctDescending(sequences, count, current), oldest);
    }

    /// {@return whether no read, snapshot nor checkpoint's build is registered, as one pass over the slots and the
    /// registry finds}
    ///
    /// Safe for concurrent use, under the rule of [#scanConcurrently()]: the caller reads the latest sequence first,
    /// and if this returns `true`, every read is at that sequence or later.
    boolean idle() {
        return busy(0) < 0 && registrations == 0;
    }

    /// {@return the first slot at or after `from` that a plain read finds busy, or -1}
    private int busy(int from) {
        var found = Arrays.mismatch(slots, from, slots.length, free, from, slots.length);
        return found < 0 ? -1 : from + found;
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
    /// Only the holder of the store's publishing role calls it.
    long oldest() {
        var oldest = Long.MAX_VALUE;
        for (var slot = busy(0); slot >= 0; slot = busy(slot + 1)) {
            var value = (long) SLOTS.getVolatile(slots, slot);
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

    /// {@return every sequence a read, a snapshot or a checkpoint's build may be reading at, distinct, highest first;
    /// the first is the latest}
    ///
    /// Only the holder of the store's publishing role calls it, so the latest sequence does not change during the call.
    long[] active() {
        var current = latest.getAsLong();
        var sequences = new long[slots.length + 1];
        var count = 0;
        sequences[count++] = current;
        for (var slot = busy(0); slot >= 0; slot = busy(slot + 1)) {
            var value = (long) SLOTS.getVolatile(slots, slot);
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
            if (checkpointRead >= 0 && checkpointRead < current) {
                if (count == sequences.length) {
                    sequences = Arrays.copyOf(sequences, count * 2);
                }
                sequences[count++] = checkpointRead;
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
