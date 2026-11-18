package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

/// The units a cache's reads missed recently, by key and nonce, and whether they missed them once, twice or three
/// times, which [PageCache] asks before it caches a unit a screened read missed: a unit it has not seen often enough is
/// recorded rather than cached, and cached at the read that follows the reads it asks for
/// ([#admit(long, long, int, int)]).
///
/// It counts to three with three filters of the same design: one takes the units read at least once, the second the
/// units read at least twice, which a point read's branch of level 1 asks for, and the third the units read at least
/// three times, which a scan asks for. A unit the cache evicted after a read used it goes to the first, and to the
/// others once they started ([#remember(long, long, int)]); the first also takes, each under a nonce salted its own
/// way, the units a checkpoint wrote in place of units reads used, which count as read once, twice or three times, but
/// as seen by none of the reads that ask what the doorkeeper saw. Each filter keeps two generations, a young one that
/// takes the records and the old one before it, and a key is in the filter when either holds it. A generation is a
/// scalable Bloom filter: a list of levels, each a Bloom filter of its own sized for a number of keys, of which only
/// the last takes records. The first generation starts with one level of [#LEVEL_KEYS] keys, so a store that reads
/// little holds about 2 KiB, and while it holds fewer keys than the cache asks for it grows by a new, empty level of as
/// many keys as the levels before it, keeping theirs: growing forgets no key and leaves the bits of the earlier levels
/// as sparse as they were. Each level is given a share of [#FALSE_POSITIVES] in proportion to its keys, which takes the
/// fewest bits for the whole, so a full generation takes at most that share of keys it never saw for seen ones, however
/// many times it grew. A generation that holds the most keys the cache asks for becomes the old one, and the young one
/// starts as a single level of that many keys, as the cache has shown it reads them: a filter remembers every key
/// recorded since its last rotation and in the generation before it, takes at most twice [#FALSE_POSITIVES] of the keys
/// it never saw for seen ones, and holds about 8 bits a key of each generation, or 11 for the first one, whose small
/// levels take a small share each. Each filter grows and rotates on its own, and the second and third start only at
/// their first record: the second at a scan's second read of a unit or a point read's second read of a branch of level
/// 1, the third at a scan's third read. A store read by point reads of leaves alone so holds one filter, and one read
/// by point reads alone two: at most 160 KiB a filter, 480 KiB in all, for a cache of 256 MiB.
///
/// Records and tests take no lock: a bit is set with an atomic or, and a test reads the words as they are. A record
/// that races a growth or a rotation may lose its bits, which costs the unit one more read before it is cached, and a
/// test that races one may miss a key, the same; of two reads of a unit that race, each may count the unit as the
/// other found it, which costs or saves the unit one read. The filters are never persisted.
final class Doorkeeper {
    /// The fewest keys a generation holds before it rotates, which a cache asks for at the least.
    static final int FIRST_KEYS = 2_048;
    /// The most keys a generation holds: about 16 MiB of bits, for a cache of 64 GiB of pages.
    static final int MAX_KEYS = 1 << 24;
    /// The keys of the first level of a filter, about 2 KiB of bits for a cache of 256 MiB.
    static final int LEVEL_KEYS = 1_024;
    /// The share of the keys it never saw that a full generation takes for seen ones.
    static final double FALSE_POSITIVES = 0.02;
    /// The most reads it counts, those a scan asks for before the one that caches a unit.
    static final int MOST_READS = 3;
    /// The salt of the nonce under which a unit is recorded as expected after three reads of the unit it replaces
    /// ([#expect(long, long, int, int)]): the fractional part of the golden ratio, which no nonce of the unit's page is
    /// but by chance.
    private static final long EXPECTED = 0x9E3779B97F4A7C15L;
    /// The salt of the nonce under which a unit is recorded as expected after two reads of the unit it replaces: the
    /// third prime of xxHash64, which no nonce of the unit's page is but by chance, nor another salt.
    private static final long EXPECTED_TWICE = 0x165667B19E3779F9L;
    /// The salt of the nonce under which a unit is recorded as expected after one read of the unit it replaces: the
    /// second prime of xxHash64, which no nonce of the unit's page is but by chance, nor another salt.
    private static final long EXPECTED_ONCE = 0xC2B2AE3D27D4EB4FL;
    private static final VarHandle WORDS = MethodHandles.arrayElementVarHandle(long[].class);

    // Guards the growths, the rotations, the first record of each filter and the close
    private final Object lock;
    // The units read at least once, and the salted records of the expected ones
    private final Counted once;
    // The units read at least twice
    private final Counted twice;
    // The units read at least three times
    private final Counted thrice;
    // Set once the cache closes, under the lock: no record allocates a filter after it
    private boolean closed;

    /// Makes a doorkeeper that has seen no key and holds no bits until its first record.
    Doorkeeper() {
        this.lock = new Object();
        this.once = new Counted();
        this.twice = new Counted();
        this.thrice = new Counted();
    }

    /// {@return whether the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh` had `reads` reads
    /// before this one, or is expected after as many, and records this read if not}
    ///
    /// The reads it counts for a unit are the most of those a filter holds it for, 3, 2 or 1, and of those it is
    /// expected after. A read is admitted if they are at least `reads`; otherwise the filter of one read more records
    /// it, or the first if it counts none. A unit only the third filter holds, as one whose record in the second a
    /// rotation forgot, so still counts three reads. A unit admitted because it was expected is not recorded as seen:
    /// the cache takes it as an entry a read used, which is what tells the next checkpoint that a read used it.
    ///
    /// @param most the most keys a generation may hold, at which it rotates rather than grow
    /// @param reads the reads before this one the unit needs, 1, 2 or 3
    boolean admit(long key, long nonceHigh, int most, int reads) {
        var hash = hash(key, nonceHigh);
        var once = this.once.filter;
        if (once == null) {
            once = this.once.start(most);
            if (once != null) {
                this.once.add(once, hash, most);
            }
            return false;
        }
        var seen = reads < 2 ? seen(once, hash, key, nonceHigh) : counted(once, hash, key, nonceHigh);
        if (seen >= reads) {
            return true;
        }
        var next = seen == 0 ? this.once : seen == 1 ? this.twice : this.thrice;
        var filter = seen == 0 ? once : next.filter;
        if (filter == null) {
            filter = next.start(most);
        }
        if (filter != null) {
            next.add(filter, hash, most);
        }
        return false;
    }

    /// {@return 1 if a filter holds the unit whose hash is `hash` or it is expected, after any number of reads, and 0
    /// if not}
    ///
    /// The test of a read that needs one read before it, which stops at the first filter or salt that holds the unit.
    private int seen(Filter once, long hash, long key, long nonceHigh) {
        if (once.holds(hash)) {
            return 1;
        }
        var twice = this.twice.filter;
        if (twice != null && twice.holds(hash)) {
            return 1;
        }
        var thrice = this.thrice.filter;
        return thrice != null && thrice.holds(hash) || once.holds(hash(key, nonceHigh ^ EXPECTED))
                || once.holds(hash(key, nonceHigh ^ EXPECTED_TWICE)) || once.holds(hash(key, nonceHigh ^ EXPECTED_ONCE))
                ? 1 : 0;
    }

    /// {@return the reads it counts for the unit whose hash is `hash`: 3, 2, 1 or 0, the most of those a filter holds
    /// it for and of those it is expected after}
    private int counted(Filter once, long hash, long key, long nonceHigh) {
        var thrice = this.thrice.filter;
        if (thrice != null && thrice.holds(hash) || once.holds(hash(key, nonceHigh ^ EXPECTED))) {
            return 3;
        }
        var twice = this.twice.filter;
        if (twice != null && twice.holds(hash) || once.holds(hash(key, nonceHigh ^ EXPECTED_TWICE))) {
            return 2;
        }
        return once.holds(hash) || once.holds(hash(key, nonceHigh ^ EXPECTED_ONCE)) ? 1 : 0;
    }

    /// Records the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh` as read as often as any read
    /// asks for, if the doorkeeper recorded any unit before: the cache evicted it after a read used it, and takes it
    /// back at its next read.
    ///
    /// The second and third filters take it only once a read started them, so a store read by point reads of leaves
    /// alone, whose cache evicts, still holds a single filter; a read that starts a filter after evictions takes the
    /// units the cache evicted before at one read more.
    ///
    /// @param most the most keys a generation may hold, at which it rotates rather than grow
    void remember(long key, long nonceHigh, int most) {
        var once = this.once.filter;
        if (once == null) {
            return;
        }
        var hash = hash(key, nonceHigh);
        this.once.add(once, hash, most);
        var twice = this.twice.filter;
        if (twice != null) {
            this.twice.add(twice, hash, most);
        }
        var thrice = this.thrice.filter;
        if (thrice != null) {
            this.thrice.add(thrice, hash, most);
        }
    }

    /// Records the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh` as expected, if the
    /// doorkeeper recorded any unit before: a checkpoint wrote it in place of a unit that reads used `reads` times, and
    /// left it out of the cache, and [#admit(long, long, int, int)] counts it as read as often. A point read of a leaf
    /// so caches it at its first read, as does a scan after three reads of the unit it replaces; after one, a scan
    /// caches it at its third read.
    ///
    /// The record is kept in the first filter apart from those of the units it saw, under a nonce salted by the reads,
    /// so neither [#holds(long, long)] nor [#reads(long, long)] finds it. A checkpoint that asks whether reads used the
    /// unit it replaces would otherwise take an expected unit for a read one and expect its replacement in turn, at
    /// every checkpoint that rewrites it with no read between them, and each such record would take a key of the
    /// generations that no read touched, rotating them so that they forget the first reads they hold.
    ///
    /// @param most the most keys a generation may hold, at which it rotates rather than grow
    /// @param reads the reads of the unit it replaces, 1, 2 or 3; 3 for three or more, and for one the cache held as
    ///              read
    void expect(long key, long nonceHigh, int most, int reads) {
        var once = this.once.filter;
        if (once != null) {
            var salt = reads < 2 ? EXPECTED_ONCE : reads < MOST_READS ? EXPECTED_TWICE : EXPECTED;
            this.once.add(once, hash(key, nonceHigh ^ salt), most);
        }
    }

    /// Lets go of the bits, as the cache closes; a record after it does nothing.
    void close() {
        synchronized (lock) {
            closed = true;
            once.filter = null;
            twice.filter = null;
            thrice.filter = null;
        }
    }

    /// {@return whether it has seen the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh`,
    /// recording nothing; a unit it only expects is not one it has seen}
    boolean holds(long key, long nonceHigh) {
        var once = this.once.filter;
        if (once == null) {
            return false;
        }
        // The first filter holds nearly every unit the others do, so a unit seen takes one test
        var hash = hash(key, nonceHigh);
        if (once.holds(hash)) {
            return true;
        }
        var twice = this.twice.filter;
        if (twice != null && twice.holds(hash)) {
            return true;
        }
        var thrice = this.thrice.filter;
        return thrice != null && thrice.holds(hash);
    }

    /// {@return the reads it has seen of the unit at `key` sealed under a nonce whose first 8 bytes are `nonceHigh`: 3
    /// for three or more, 2, 1 or 0, recording nothing; a unit it only expects has none}
    int reads(long key, long nonceHigh) {
        var once = this.once.filter;
        if (once == null) {
            return 0;
        }
        var hash = hash(key, nonceHigh);
        var thrice = this.thrice.filter;
        if (thrice != null && thrice.holds(hash)) {
            return 3;
        }
        var twice = this.twice.filter;
        if (twice != null && twice.holds(hash)) {
            return 2;
        }
        return once.holds(hash) ? 1 : 0;
    }

    /// {@return the bytes the generations of the three filters take, for tests that bound them}
    long bytesForTests() {
        return once.bytes() + twice.bytes() + thrice.bytes();
    }

    /// {@return the keys the young generations of the three filters hold before they grow or rotate, or 0 before the
    /// first record; for tests}
    int keysForTests() {
        return onceKeysForTests() + twiceKeysForTests() + thriceKeysForTests();
    }

    /// {@return the keys the young generation of the filter of the units read once holds before it grows or rotates,
    /// or 0 before its first record; for tests}
    int onceKeysForTests() {
        return once.keys();
    }

    /// {@return the keys the young generation of the filter of the units read twice holds before it grows or rotates,
    /// or 0 before its first record; for tests}
    int twiceKeysForTests() {
        return twice.keys();
    }

    /// {@return the keys the young generation of the filter of the units read three times holds before it grows or
    /// rotates, or 0 before its first record; for tests}
    int thriceKeysForTests() {
        return thrice.keys();
    }

    /// {@return the rotations of the three filters so far, for tests that count them}
    int rotationsForTests() {
        return once.rotations() + twice.rotations() + thrice.rotations();
    }

    /// Rotates the filter of the units read twice twice over, so that it forgets every key it held, as two rotations
    /// of a busy store would; for tests.
    void forgetTwiceForTests(int most) {
        synchronized (lock) {
            var twice = this.twice.filter;
            if (twice != null) {
                this.twice.filter = twice.rotated(cap(most)).rotated(cap(most));
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

    /// One of the three filters of a [Doorkeeper], whose generations a growth or a rotation replaces whole.
    private final class Counted {
        // The generations; null until the filter's first record and after the close
        private volatile Filter filter;

        /// {@return the filter of the first record, or `null` once the cache closed}
        Filter start(int most) {
            synchronized (lock) {
                if (filter == null && !closed) {
                    var level = Level.of(LEVEL_KEYS, Math.max(LEVEL_KEYS, cap(most)));
                    filter = new Filter(new Level[]{level}, null, LEVEL_KEYS, 0, 0);
                }
                return filter;
            }
        }

        /// Sets the bits of the key whose hash is `hash` in the last level of the young generation of `filter`, and
        /// grows or rotates the generations once that set the bits of the level's last key.
        void add(Filter filter, long hash, int most) {
            var last = filter.young[filter.young.length - 1];
            if (last.set(hash) && filter.added.incrementAndGet() == last.keys) {
                // Only the record that filled the level gets here; one that adds the same key again sets no bit and
                // does not count, or keys read again and again would rotate the generations early and forget the
                // others
                synchronized (lock) {
                    if (this.filter == filter) {
                        var cap = cap(most);
                        this.filter = filter.keys < cap ? filter.grown(cap) : filter.rotated(cap);
                    }
                }
            }
        }

        /// {@return the keys of its young generation, or 0 before its first record}
        int keys() {
            var filter = this.filter;
            return filter == null ? 0 : filter.keys;
        }

        /// {@return its rotations so far, or 0 before its first record}
        int rotations() {
            var filter = this.filter;
            return filter == null ? 0 : filter.rotations;
        }

        /// {@return the bytes its generations take}
        long bytes() {
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
    }

    /// The two generations of a filter of a [Doorkeeper].
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
