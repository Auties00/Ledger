package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

/// The units a cache's reads missed recently, by key and nonce, which [PageCache] asks before it caches a unit a
/// screened read missed: a unit it has not seen is recorded rather than cached, and cached at its next read.
///
/// It keeps two generations, a young one that takes the records and the old one before it, and a key is seen when
/// either holds it. A generation is a scalable Bloom filter: a list of levels, each a Bloom filter of its own sized
/// for a number of keys, of which only the last takes records. The first generation starts with one level of
/// [#LEVEL_KEYS] keys, so a store that reads little holds about 2 KiB, and while it holds fewer keys than the cache
/// asks for it grows by a new, empty level of as many keys as the levels before it, keeping theirs: growing forgets no
/// key and leaves the bits of the earlier levels as sparse as they were. Each level is given a share of
/// [#FALSE_POSITIVES] in proportion to its keys, which takes the fewest bits for the whole, so a full generation takes
/// at most that share of keys it never saw for seen ones, however many times it grew. A generation that holds the
/// most keys the cache asks for becomes the old one, and the young one starts as a single level of that many keys,
/// as the cache has shown it reads them: the filter remembers every key recorded since the last rotation and in the
/// generation before it, takes at most twice [#FALSE_POSITIVES] of the keys it never saw for seen ones, and holds
/// about 8 bits a key of each generation, or 11 for the first one, whose small levels take a small share each: at
/// most 160 KiB for a cache of 256 MiB.
///
/// Records and tests take no lock: a bit is set with an atomic or, and a test reads the words as they are. A record
/// that races a growth or a rotation may lose its bits, which costs the unit one more read before it is cached, and a
/// test that races one may miss a key, the same. The filter is never persisted.
final class Doorkeeper {
    /// The fewest keys a generation holds before it rotates, which a cache asks for at the least.
    static final int FIRST_KEYS = 2_048;
    /// The most keys a generation holds: about 16 MiB of bits, for a cache of 64 GiB of pages.
    static final int MAX_KEYS = 1 << 24;
    /// The keys of the first level of a filter, about 2 KiB of bits for a cache of 256 MiB.
    static final int LEVEL_KEYS = 1_024;
    /// The share of the keys it never saw that a full generation takes for seen ones.
    static final double FALSE_POSITIVES = 0.02;
    private static final VarHandle WORDS = MethodHandles.arrayElementVarHandle(long[].class);

    // Guards the growths, the rotations, the first record and the close
    private final Object lock;
    // The generations, replaced whole by a growth or a rotation; null until the first record and after the close
    private volatile Filter filter;
    // Set once the cache closes: no record allocates a filter after it
    private boolean closed;

    /// Makes a doorkeeper that has seen no key and holds no bits until its first record.
    Doorkeeper() {
        this.lock = new Object();
    }

    /// {@return whether it has seen the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh`, which
    /// it records if not}
    ///
    /// @param most the most keys a generation may hold, at which it rotates rather than grow
    boolean admit(long key, long nonceHigh, int most) {
        var hash = hash(key, nonceHigh);
        var filter = this.filter;
        if (filter == null) {
            filter = start(most);
            if (filter == null) {
                return false;
            }
        } else if (filter.holds(hash)) {
            return true;
        }
        add(filter, hash, most);
        return false;
    }

    /// Records the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh` as seen, if the doorkeeper
    /// recorded any unit before: the cache evicted it, or a checkpoint wrote it in place of a unit a read used and left
    /// it out of the cache, and the cache takes it at its next read.
    ///
    /// @param most the most keys a generation may hold, at which it rotates rather than grow
    void remember(long key, long nonceHigh, int most) {
        var filter = this.filter;
        if (filter != null) {
            add(filter, hash(key, nonceHigh), most);
        }
    }

    /// Lets go of the bits, as the cache closes; a record after it does nothing.
    void close() {
        synchronized (lock) {
            closed = true;
            filter = null;
        }
    }

    /// {@return whether it has seen the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh`,
    /// recording nothing}
    boolean holds(long key, long nonceHigh) {
        var filter = this.filter;
        return filter != null && filter.holds(hash(key, nonceHigh));
    }

    /// {@return the bytes the generations take, for tests that bound them}
    long bytesForTests() {
        var filter = this.filter;
        if (filter == null) {
            return 0;
        }
        var bytes = 0L;
        for (var level : filter.young) {
            bytes += 8L * level.words.length;
        }
        if (filter.old != null) {
            for (var level : filter.old) {
                bytes += 8L * level.words.length;
            }
        }
        return bytes;
    }

    /// {@return the keys the young generation holds before it grows or rotates, or 0 before the first record; for
    /// tests}
    int keysForTests() {
        var filter = this.filter;
        return filter == null ? 0 : filter.keys;
    }

    /// {@return the rotations so far, for tests that count them}
    int rotationsForTests() {
        var filter = this.filter;
        return filter == null ? 0 : filter.rotations;
    }

    /// {@return the filter of the first record, or `null` once the cache closed}
    private Filter start(int most) {
        synchronized (lock) {
            if (filter == null && !closed) {
                var level = Level.of(LEVEL_KEYS, Math.max(LEVEL_KEYS, cap(most)));
                filter = new Filter(new Level[]{level}, null, LEVEL_KEYS, 0, 0);
            }
            return filter;
        }
    }

    /// Sets the bits of the key whose hash is `hash` in the last level of the young generation of `filter`, and grows
    /// or rotates the generations once that set the bits of the level's last key.
    private void add(Filter filter, long hash, int most) {
        var last = filter.young[filter.young.length - 1];
        if (last.set(hash) && filter.added.incrementAndGet() == last.keys) {
            // Only the record that filled the level gets here; one that adds the same key again sets no bit and does
            // not count, or keys read again and again would rotate the generations early and forget the others
            synchronized (lock) {
                if (this.filter == filter) {
                    var cap = cap(most);
                    this.filter = filter.keys < cap ? filter.grown(cap) : filter.rotated(cap);
                }
            }
        }
    }

    /// {@return the most keys a generation holds, out of the most the cache asks for}
    private static int cap(int most) {
        return Math.clamp(most, 1, MAX_KEYS);
    }

    /// {@return a hash of the unit at `key` sealed under `nonceHigh`}
    static long hash(long key, long nonceHigh) {
        // The finalizer of SplitMix64, as a key's file and page and a nonce's counter differ in few bits
        var hash = key ^ Long.rotateLeft(nonceHigh, 29);
        hash = (hash ^ hash >>> 30) * 0xBF58476D1CE4E5B9L;
        hash = (hash ^ hash >>> 27) * 0x94D049BB133111EBL;
        return hash ^ hash >>> 31;
    }

    /// The two generations of a [Doorkeeper].
    ///
    /// @param young the levels of the generation records set bits in, the last of which takes them
    /// @param old the levels of the generation before it, which tests still read, or `null` before the first rotation
    /// @param keys the keys of the levels of the young generation
    /// @param rotations the rotations of the filter this one descends from, for tests
    /// @param added the keys whose record set a bit of the last level of the young generation, which the level holds
    ///              before the generation grows or rotates
    private record Filter(Level[] young, Level[] old, int keys, int rotations, AtomicInteger added) {
        Filter(Level[] young, Level[] old, int keys, int rotations, int added) {
            this(young, old, keys, rotations, new AtomicInteger(added));
        }

        /// {@return whether a level of either generation holds the bits of the key whose hash is `hash`}
        boolean holds(long hash) {
            return holds(young, hash) || old != null && holds(old, hash);
        }

        /// {@return the filter whose young generation has a new, empty level of as many keys as its levels hold}
        ///
        /// @param cap the most keys a generation holds, of which the new level takes its share of false positives
        Filter grown(int cap) {
            var levels = new Level[young.length + 1];
            System.arraycopy(young, 0, levels, 0, young.length);
            levels[young.length] = Level.of(keys, Math.max(cap, 2 * keys));
            return new Filter(levels, old, 2 * keys, rotations, 0);
        }

        /// {@return the filter whose old generation is this one's young one, and whose young one is a single, empty
        /// level of `cap` keys}
        Filter rotated(int cap) {
            return new Filter(new Level[]{Level.of(cap, cap)}, young, cap, rotations + 1, 0);
        }

        private static boolean holds(Level[] levels, long hash) {
            // The last levels are the largest and the most recent, where a key seen again most likely is
            for (var i = levels.length - 1; i >= 0; i--) {
                if (levels[i].holds(hash)) {
                    return true;
                }
            }
            return false;
        }
    }

    /// A Bloom filter of a level of a generation.
    ///
    /// @param words the bits
    /// @param keys the keys it holds before it is full
    /// @param probes the bits a key sets
    private record Level(long[] words, int keys, int probes) {
        /// {@return an empty level of `keys` keys, whose share of the false positives of a generation of `total` keys
        /// is its share of the keys}
        static Level of(int keys, int total) {
            var falsePositives = FALSE_POSITIVES * keys / total;
            // The bits a key takes and the probes that give the least false positives for them
            var bitsPerKey = -Math.log(falsePositives) / (Math.log(2) * Math.log(2));
            var words = (int) Math.ceil(bitsPerKey * keys / 64);
            var probes = Math.max(1, (int) Math.round(bitsPerKey * Math.log(2)));
            return new Level(new long[words], keys, probes);
        }

        /// {@return whether it holds the bits of the key whose hash is `hash`}
        boolean holds(long hash) {
            var words = this.words;
            var bits = (long) words.length * 64;
            var step = step(hash);
            for (var probe = 0; probe < probes; probe++) {
                var bit = Math.unsignedMultiplyHigh(hash + probe * step, bits);
                if (((long) WORDS.getOpaque(words, (int) (bit >>> 6)) & 1L << bit) == 0) {
                    return false;
                }
            }
            return true;
        }

        /// {@return whether it set a bit of the key whose hash is `hash` that was clear}
        boolean set(long hash) {
            var words = this.words;
            var bits = (long) words.length * 64;
            var step = step(hash);
            var changed = false;
            for (var probe = 0; probe < probes; probe++) {
                var bit = Math.unsignedMultiplyHigh(hash + probe * step, bits);
                var word = (int) (bit >>> 6);
                var flag = 1L << bit;
                // Read first: the atomic or takes the line for itself even when the bit is set
                if (((long) WORDS.getOpaque(words, word) & flag) == 0
                        && ((long) WORDS.getAndBitwiseOr(words, word, flag) & flag) == 0) {
                    changed = true;
                }
            }
            return changed;
        }

        // The second hash of the probes' double hashing, odd and as wide as the first, as a probe's bit is the high
        // word of its hash times the bits
        private static long step(long hash) {
            return Long.rotateLeft(hash, 32) | 1;
        }
    }
}
