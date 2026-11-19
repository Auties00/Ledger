package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;

/// The writes an open replays into one paged key space from the frames before the log's last budget, in arrays with no
/// object per entry: each key once, with its newest value or its removal, in the order of the keys and by the hash of
/// their bytes. Readers take it as part of the tree the store opened from, so it is the in-memory counterpart of an
/// immutable memtable: the keys, values and removals the open's tree lacks, read through until the first checkpoint
/// after the open writes them.
///
/// Every write of the run is older than every reader the store has: none exists while the open replays, and every one
/// after reads at a sequence above the last one replayed. A reader whose tree is the one the store opened from (or that
/// reads no tree, after a store crashed before its first checkpoint) reads the run over that tree: a key of the run takes
/// the run's entry, a removal of the run hides the tree's, and any other key reads the tree. A reader of a later tree
/// reads that tree alone, as every checkpoint after the open takes the whole run. A key space with a range removal in the
/// replayed log builds its run at that removal and replays the rest of the log to chains, so every range removal a reader
/// of the open's tree applies is newer than every entry of the run: a removal stamped after the open's tree hides a key of
/// the run as it hides a key of the tree.
///
/// The writes of the run are listed in no [BlindWrites]: the run's effect on the count of the key space, against the
/// open's tree, is [#correction()], which the first checkpoint finds as a by-product of merging the run into the tree
/// ([#settle(byte\[\], int, boolean)]), and a count before then by looking each key up. A write the replay sent to a
/// chain whose key the run holds takes the run's entry as the key's state before it, as the builder's entry while the
/// replay runs and the run's after, so it is exact, never blind.
///
/// Per entry it keeps the key's bytes and their end, a word of the value's place and kind, a slot of the hash table and an
/// index of the key order; the keys lie in chunks of [#KEY_CHUNK] bytes, a value shorter than
/// [SpillQueue#LOG_VALUE_MIN] in the replay's chunks ([ReplayValues]), and a longer one stays in the log, where the run
/// keeps its place, its tag and its stamp, which the tag binds, as a [LogValue] would. A message that took a chain, a version, a [LogValue], an entry of
/// the hash index, of the dirty and blind lists, and a node of the ordered map once scanned, about 300 bytes over its key
/// and value, takes about 50 here.
///
/// A bulk commit makes a run too ([#committed()]): the puts of new keys of a paged key space whose latest tree holds
/// none of its entries, at least [LiveLedger#BULK_RUN_WRITES] of them in one batch, which the commit counts as it
/// applies them, so the run makes no [#correction()] to the count. Such a run is part of the tree of every reader at or
/// after its commit ([#minStamp()]) and of none before, until a checkpoint at or after the commit takes it, as every
/// checkpoint after an open takes a replay's. Its values lie where the batch put them, on the arrays the commit's
/// versions would have sliced (its homes), until the queue of values that may leave the heap moves them as it moves
/// the values of versions ([#spill]): a value of at least [SpillQueue#LOG_VALUE_MIN] bytes to the log, a shorter one
/// to a chunk of the run's own. Once no entry is on a home any more, the run lets go of it. The order of the keys of
/// such a run is made at the first scan, count or checkpoint that needs it, not by the commit.
///
/// A bulk commit whose frame was split ([FrameBody#split()]) and whose values do not pay to move one by one may have
/// its run let go of its homes instead, once the frame is durable ([#dropHomes]): a home whose values a value frame of
/// the commit holds at the offsets they have on the home, from its start, takes that frame's place in the log
/// ([FramedHome]) in its stead. The first read of a value on such a home reads the value frame back, authenticated by
/// its own tag, into an array of its own that becomes the home again, so the read costs one positioned read of the
/// frame and an open of it, and later reads of the home cost nothing more; the checkpoint that takes the run reads
/// every such home back first, each on a thread of its own ([#readBackHomes()]).
///
/// Immutable once built but for the place of the values of a bulk commit's run, which the spill switches with one
/// release store of the entry's word after it published where the value went, and its homes, which a drop or a read
/// back sets with a release store, so readers use it without synchronization: every read of a word or of a home is an
/// acquire. [#correction()], the tree lookups it caches and the order of the keys made late take a lock of their own.
final class PackedRun {
    /// The entries a run holds at most: the low bits of a slot of the hash table hold an entry's index plus one.
    static final int MAX_ENTRIES = (1 << 24) - 2;
    /// The log segments the logged values of a run may lie in at most: the high bits of a value's place in the log.
    static final int MAX_SEGMENTS = 1 << 16;
    /// The longest value a run takes: the bits of the word of an entry that hold a logged value's length.
    static final int MAX_VALUE_LENGTH = (1 << 30) - 1;
    /// The bytes the keys of a run take at most, with the ends of the chunks no key fit: the builder takes no key that
    /// could pass it once moved to the start of the next chunk.
    static final int MAX_KEY_BYTES = 1 << 30;
    /// The bytes of a chunk of the keys: below half the smallest region of the G1 collector, as the replay's chunks
    /// of values are, so no array of a run's keys is humongous whatever the heap. A key never crosses a chunk: one that
    /// does not fit the rest of a chunk starts the next.
    static final int KEY_CHUNK = 1 << 18;
    private static final int KEY_CHUNK_BITS = Integer.numberOfTrailingZeros(KEY_CHUNK);
    private static final int KEY_OFFSET_MASK = KEY_CHUNK - 1;
    /// The longest value of a bulk commit's run on its home: the bits of the word of an entry that hold its length.
    static final int MAX_HOME_LENGTH = (1 << 20) - 1;
    /// The farthest a value of a bulk commit's run starts in its home: the bits of the word that hold its offset.
    static final int MAX_HOME_OFFSET = (1 << 21) - 1;
    /// The homes a bulk commit's run may have at most: the bits of the word that hold the index of a value's home.
    static final int MAX_HOMES = 1 << 20;
    // The kind and place of a value, in the word of an entry: the sign bit for a removal; then a bit for a value in the
    // log, whose length and index among the logged values follow; then a bit for a value on a home of a bulk commit's
    // run, whose home, offset and length follow; otherwise the length, the chunk and the offset of a value in a chunk
    private static final long REMOVES = 1L << 63;
    private static final long LOGGED = 1L << 62;
    private static final long HOME = 1L << 61;
    private static final int OFFSET_BITS = 20;
    private static final int CHUNK_BITS = 24;
    private static final int LENGTH_SHIFT = OFFSET_BITS + CHUNK_BITS;
    private static final long LENGTH_MASK = (1L << (61 - LENGTH_SHIFT)) - 1;
    private static final long LOGGED_LENGTH_MASK = (1L << 30) - 1;
    private static final int HOME_OFFSET_SHIFT = 20;
    private static final int HOME_INDEX_SHIFT = 41;
    private static final VarHandle WORDS = MethodHandles.arrayElementVarHandle(long[].class);
    private static final VarHandle HOMES = MethodHandles.arrayElementVarHandle(MemorySegment[].class);
    // A value's place in the log: the index of its segment in the table above the bits of where its frame starts
    private static final int FRAME_BITS = 48;
    private static final long FRAME_MASK = (1L << FRAME_BITS) - 1;
    // A slot of the hash table: the top byte of the key's hash over the entry's index plus one, 0 for none
    private static final int ENTRY_MASK = (1 << 24) - 1;
    private static final int TAG_MASK = ~ENTRY_MASK;
    private static final MemorySegment EMPTY = MemorySegment.ofArray(new byte[0]).asReadOnly();

    static {
        if (ReplayValues.CHUNK_BYTES > 1 << OFFSET_BITS || SpillQueue.LOG_VALUE_MIN > LENGTH_MASK
                || ArrayWriteBatch.CHUNK_BYTES > 1 << OFFSET_BITS) {
            throw new ExceptionInInitializerError("A value in a chunk does not fit the word of its entry");
        }
        if (KeyCodec.MAX_LENGTH > KEY_CHUNK) {
            throw new ExceptionInInitializerError("A key does not fit a chunk of the keys");
        }
    }

    // The chunks of the keys, and where each key ends, as its chunk times KEY_CHUNK plus its end in the chunk: entry
    // i's key starts where entry i - 1's ends, or at the start of the next chunk if it did not fit the rest of that one
    private final byte[][] keys;
    private final int[] keyEnds;
    // Read-only views of the chunks of `keys`, which the slices of a key are made of
    private final MemorySegment[] keyViews;
    // The kind and place of each entry's value, read with an acquire: the spill of a bulk commit's run switches it
    private final long[] values;
    // The chunks and the places in the log the values lie in; replaced, by the spill of a bulk commit's run alone, before
    // any word refers to what it adds
    private volatile Places places;
    // The read-only views of the arrays a bulk commit put the values of its run on, by the index a word holds; an
    // element is set to null, with a release store, once no word refers to it. Null for a replay's run
    private final MemorySegment[] homes;
    // Per home, the entries whose value is still on it; written by the spill alone
    private final int[] homeEntries;
    // Per home of a commit whose frame was split, where the home's start lies in the section, plus one, as its values
    // place it: 0 if no value said, -1 if two said otherwise; and where its last value ends on it. Null for any other
    // run
    private final int[] homeSections;
    private final int[] homeEnds;
    // Per home, the value frame that holds it once the run let go of it, null for a home it keeps; null until a drop.
    // Written once, before any home is let go of
    private volatile FramedHome[] framed;
    // Open addressing on the hash of the keys, at most three quarters full
    private final int[] slots;
    // The entries in the order of their keys; null until the first need for a bulk commit's run
    private volatile int[] order;
    private final int size;
    // The stamps of the oldest and of the newest write the run holds: a reader below the oldest does not read the run,
    // and one whose tree is at or above the newest reads the tree alone
    private final long minStamp;
    private final long maxStamp;
    // Whether the commit that made the run counted its writes, as a bulk commit does
    private final boolean committed;
    // The root of the key space's tree the store opened from, or null, and the file it is in: what the run's writes are
    // counted against
    private final TreeRoot base;
    private final PageFile pages;
    // Guards the lookups of the keys in `base` that the count of the run makes, and the late sort of a bulk commit's run
    private final Object lock;
    // Per entry, whether `base` holds its key, as TreeSearch.FOUND or ABSENT, or UNKNOWN; made at the first need
    private volatile byte[] inBase;

    /// Creates the run of the writes `builder` recorded, which lets go of its arrays as it hands each over, so the arrays
    /// the builder grew and their trimmed copies are never all live at once; an array the builder filled exactly is
    /// handed over as it is.
    ///
    /// @param committed whether the writes are a bulk commit's, which counted them and whose order is made late
    private PackedRun(Builder builder, TreeRoot base, PageFile pages, boolean committed) {
        var size = builder.size;
        this.size = size;
        // Every chunk but the last is whole; the last is trimmed to the bytes its keys take
        var keys = builder.keyChunks.toArray(byte[][]::new);
        var last = keys.length - 1;
        var lastUsed = builder.keyBytes - last * KEY_CHUNK;
        if (lastUsed < keys[last].length) {
            keys[last] = Arrays.copyOf(keys[last], lastUsed);
        }
        this.keys = keys;
        builder.keyChunks = null;
        builder.keys = null;
        this.keyEnds = trim(builder.keyEnds, size);
        builder.keyEnds = null;
        this.keyViews = new MemorySegment[keys.length];
        for (var i = 0; i < keys.length; i++) {
            keyViews[i] = MemorySegment.ofArray(keys[i]).asReadOnly();
        }
        this.values = trim(builder.values, size);
        builder.values = null;
        var logged = builder.logged;
        this.places = new Places(builder.chunks.toArray(MemorySegment[]::new), trim(builder.logStamps, logged),
                trim(builder.logPlaces, logged), trim(builder.logBodies, logged), trim(builder.tagHighs, logged),
                trim(builder.tagLows, logged), builder.segments.toArray(LogSegmentView[]::new));
        builder.logStamps = null;
        builder.logPlaces = null;
        builder.logBodies = null;
        builder.tagHighs = null;
        builder.tagLows = null;
        if (builder.homes == null) {
            this.homes = committed ? new MemorySegment[0] : null;
            this.homeEntries = committed ? new int[0] : null;
        } else {
            this.homes = builder.homes.toArray(MemorySegment[]::new);
            this.homeEntries = Arrays.copyOf(builder.homeEntries, homes.length);
            builder.homes = null;
            builder.homeEntries = null;
        }
        if (builder.homeSections == null || homes == null) {
            this.homeSections = null;
            this.homeEnds = null;
        } else {
            this.homeSections = Arrays.copyOf(builder.homeSections, homes.length);
            this.homeEnds = Arrays.copyOf(builder.homeEnds, homes.length);
        }
        builder.homeSections = null;
        builder.homeEnds = null;
        // Kept as the builder filled it: the entries keep their indexes, and the table is at most three quarters full
        this.slots = builder.slots;
        builder.slots = null;
        this.minStamp = builder.minStamp;
        this.maxStamp = builder.maxStamp;
        this.committed = committed;
        this.base = base;
        this.pages = pages;
        this.lock = new Object();
        this.order = committed ? null : sortedOrder();
    }

    private static byte[] trim(byte[] array, int length) {
        return array.length == length ? array : Arrays.copyOf(array, length);
    }

    private static int[] trim(int[] array, int length) {
        return array.length == length ? array : Arrays.copyOf(array, length);
    }

    private static long[] trim(long[] array, int length) {
        return array.length == length ? array : Arrays.copyOf(array, length);
    }

    /// {@return the stamp of the oldest write the run holds}
    ///
    /// A reader below it reads no write of the run: only a bulk commit's run has readers older than its writes, and
    /// they see none of the commit.
    long minStamp() {
        return minStamp;
    }

    /// {@return whether the commit that made the run counted its writes, so the run makes no [#correction()] to the
    /// count of its key space: a bulk commit's run}
    boolean committed() {
        return committed;
    }

    /// {@return the stamp of the newest write the run holds}
    ///
    /// A reader takes the run as part of its tree while that tree's bound is below it: then the tree is the one the
    /// store opened from, or one older than the bulk commit that made the run, or there is none.
    long maxStamp() {
        return maxStamp;
    }

    /// {@return the entries of the run}
    int size() {
        return size;
    }

    /// {@return the entry of `key`, or -1 if the run holds none}
    int find(MemorySegment key) {
        return find(key, Segments.hash(key));
    }

    /// {@return the entry of `key`, whose hash is `hash` ([Segments#hash(MemorySegment)]), or -1 if the run holds none}
    int find(MemorySegment key, int hash) {
        var mask = slots.length - 1;
        var tag = hash & TAG_MASK;
        for (var i = hash & mask; ; i = (i + 1) & mask) {
            var slot = slots[i];
            if (slot == 0) {
                return -1;
            }
            if ((slot & TAG_MASK) == tag) {
                var entry = (slot & ENTRY_MASK) - 1;
                var start = keyStart(entry);
                var length = keyEnds[entry] - start;
                if (length == key.byteSize() && Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, length, key) == 0) {
                    return entry;
                }
            }
        }
    }

    /// {@return the entry of the key whose bytes are `key`, or -1 if the run holds none}
    int find(byte[] key) {
        var hash = Segments.hash(key);
        var mask = slots.length - 1;
        var tag = hash & TAG_MASK;
        for (var i = hash & mask; ; i = (i + 1) & mask) {
            var slot = slots[i];
            if (slot == 0) {
                return -1;
            }
            if ((slot & TAG_MASK) == tag) {
                var entry = (slot & ENTRY_MASK) - 1;
                var start = keyStart(entry);
                var length = keyEnds[entry] - start;
                if (length == key.length
                        && Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, length, key, 0, key.length) == 0) {
                    return entry;
                }
            }
        }
    }

    /// {@return whether `entry` removes its key}
    boolean removes(int entry) {
        return word(entry) < 0;
    }

    // The word of `entry`, as the last switch of a spill left it
    private long word(int entry) {
        return (long) WORDS.getAcquire(values, entry);
    }

    /// {@return the key of `entry`, a read-only slice of the run's keys}
    MemorySegment key(int entry) {
        var start = keyStart(entry);
        return keyViews[start >>> KEY_CHUNK_BITS].asSlice(start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `chain` and that of `entry`, as [Segments#compare(MemorySegment, MemorySegment)]
    /// gives, without a view of the entry's key}
    int compareChain(Chain chain, int entry) {
        var start = keyStart(entry);
        return chain.compareKey(keyViews[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `entry` and `key`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(int entry, MemorySegment key) {
        var start = keyStart(entry);
        return Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start, key);
    }

    /// {@return the value of `entry`, which puts its key, read-only: a slice of a chunk or of a home, or read from the
    /// log into an array of its own}
    ///
    /// @throws IllegalStateException if the value is in a log segment a checkpoint retired, which only a reader of a
    ///                               later tree than the run's can find
    /// @throws com.github.auties00.ledger.LedgerIoException if the value is in the log and cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the value is in the log and the log changed
    MemorySegment value(int entry) {
        var value = word(entry);
        while (true) {
            if ((value & LOGGED) != 0) {
                return MemorySegment.ofArray(readLogged(value)).asReadOnly();
            }
            if ((value & HOME) != 0) {
                var home = homeOrFrame(value);
                if (home == null) {
                    // Every value on it moved before the run let go of it: the word says where this one went
                    value = word(entry);
                    continue;
                }
                return home.asSlice(homeOffset(value), homeLength(value));
            }
            var length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
            if (length == 0) {
                return EMPTY;
            }
            return places.chunks()[chunkOf(value)].asSlice(value & ((1L << OFFSET_BITS) - 1), length);
        }
    }

    /// {@return the value of `entry`, which puts its key, in a new array}
    ///
    /// @throws IllegalStateException as [#value(int)] does
    byte[] valueBytes(int entry) {
        var value = word(entry);
        while (true) {
            if ((value & LOGGED) != 0) {
                return readLogged(value);
            }
            MemorySegment source;
            long offset;
            int length;
            if ((value & HOME) != 0) {
                source = homeOrFrame(value);
                if (source == null) {
                    value = word(entry);
                    continue;
                }
                offset = homeOffset(value);
                length = homeLength(value);
            } else {
                length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
                source = length == 0 ? null : places.chunks()[chunkOf(value)];
                offset = value & ((1L << OFFSET_BITS) - 1);
            }
            var copy = new byte[length];
            if (length > 0) {
                MemorySegment.copy(source, ValueLayout.JAVA_BYTE, offset, copy, 0, length);
            }
            return copy;
        }
    }

    /// {@return the version a checkpoint writes for `entry`, made for the call: a version of its value in a chunk or on
    /// a home, or in the log under the stamp its tag binds; `null` for a removal}
    Version version(int entry) {
        var value = word(entry);
        while (true) {
            if (value < 0) {
                return null;
            }
            if ((value & LOGGED) != 0) {
                var index = (int) value;
                var places = this.places;
                return Version.ofLogged(places.logStamps()[index], logged(places, index),
                        (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
            }
            if ((value & HOME) != 0) {
                var home = homeOrFrame(value);
                if (home == null) {
                    value = word(entry);
                    continue;
                }
                return new Version(0, home, homeOffset(value), homeLength(value), null, null);
            }
            var length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
            if (length == 0) {
                return new Version(0, EMPTY, null, null);
            }
            return new Version(0, places.chunks()[chunkOf(value)], (int) (value & ((1L << OFFSET_BITS) - 1)), length, null, null);
        }
    }

    /// {@return the position in the key order of the first entry whose key is at or after `key`, or [#size()] if none
    /// is; 0 for `null`}
    int lowerPosition(MemorySegment key) {
        if (key == null) {
            return 0;
        }
        var order = order();
        var low = 0;
        var high = size;
        while (low < high) {
            var middle = (low + high) >>> 1;
            if (compareKey(order[middle], key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    /// {@return the entry at `position` of the key order}
    int entryAt(int position) {
        return order()[position];
    }

    /// {@return the entries in the order of their keys, sorted at the first call for a bulk commit's run}
    private int[] order() {
        var sorted = order;
        if (sorted == null) {
            synchronized (lock) {
                sorted = order;
                if (sorted == null) {
                    sorted = sortedOrder();
                    order = sorted;
                }
            }
        }
        return sorted;
    }

    /// {@return the array a checkpoint records in whether the tree the store opened from holds each entry's key, as it
    /// merges the entries into that tree ([#settle(byte\[\], int, boolean)])}
    byte[] settles() {
        var known = inBase;
        if (known == null) {
            synchronized (lock) {
                known = inBase;
                if (known == null) {
                    known = new byte[size];
                    inBase = known;
                }
            }
        }
        return known;
    }

    /// Records whether the tree the store opened from holds the key of `entry`, as a checkpoint that merges the run into
    /// that tree finds it; every thread that finds out finds the same answer.
    static void settle(byte[] settles, int entry, boolean found) {
        settles[entry] = found ? TreeSearch.FOUND : TreeSearch.ABSENT;
    }

    /// {@return the change the run makes to the count of its key space, against the tree the store opened from: a put of
    /// a key the tree lacks adds an entry, a removal of a key it holds takes one away}
    ///
    /// It looks up in that tree each key no checkpoint merging the run has settled, which reads the file for a tree
    /// larger than the page cache; the answers are kept. Callers keep the result, which no later write changes.
    long correction() {
        return countBetween(0, size);
    }

    /// {@return the change the entries of the run from `from`, included, to `to`, excluded, either `null` for no bound,
    /// make to the count of the tree the store opened from in that range}
    long correction(MemorySegment from, MemorySegment to) {
        var first = lowerPosition(from);
        var last = to == null ? size : lowerPosition(to);
        return first >= last ? 0 : countBetween(first, last);
    }

    // The change the entries at positions first to last, excluded, of the key order make to the count of `base`
    private long countBetween(int first, int last) {
        var known = settles();
        var order = order();
        var total = 0L;
        synchronized (lock) {
            for (var position = first; position < last; position++) {
                var entry = order[position];
                var state = known[entry];
                if (state == TreeSearch.UNKNOWN) {
                    state = base != null && TreeSearch.contains(pages, base, key(entry)) ? TreeSearch.FOUND : TreeSearch.ABSENT;
                    known[entry] = state;
                }
                if (word(entry) < 0) {
                    total -= state == TreeSearch.FOUND ? 1 : 0;
                } else {
                    total += state == TreeSearch.FOUND ? 0 : 1;
                }
            }
        }
        return total;
    }

    /// {@return where the value of `entry` lies, for tests: the [LogValue] of a value in the log, the chunk of one in a
    /// chunk, the home of one on a home, or `null` for an empty value or a removal}
    Object sourceForTests(int entry) {
        var value = word(entry);
        if (value < 0) {
            return null;
        }
        if ((value & LOGGED) != 0) {
            return logged(places, (int) value);
        }
        if ((value & HOME) != 0) {
            return homes[homeIndex(value)];
        }
        return ((value >>> LENGTH_SHIFT) & LENGTH_MASK) == 0 ? null : places.chunks()[chunkOf(value)];
    }

    /// {@return whether the value of `entry` is on a home of a bulk commit's run, for tests}
    boolean onHomeForTests(int entry) {
        return onHome(word(entry));
    }

    /// {@return the homes of a bulk commit's run it let go of for their value frames, read back or not, for tests}
    int framedHomesForTests() {
        var framed = this.framed;
        var count = 0;
        for (var i = 0; framed != null && i < framed.length; i++) {
            if (framed[i] != null) {
                count++;
            }
        }
        return count;
    }

    /// {@return the homes of a bulk commit's run the run still holds, for tests}
    int homesForTests() {
        var held = 0;
        for (var i = 0; homes != null && i < homes.length; i++) {
            if (HOMES.getAcquire(homes, i) != null) {
                held++;
            }
        }
        return held;
    }

    /// {@return whether the order of the keys is made, for tests}
    boolean orderedForTests() {
        return order != null;
    }

    /// {@return the length of each chunk of the run's keys, for tests}
    int[] keyChunkLengthsForTests() {
        var lengths = new int[keys.length];
        for (var i = 0; i < keys.length; i++) {
            lengths[i] = keys[i].length;
        }
        return lengths;
    }

    /// {@return the lengths of the arrays of the entries and of the logged values, for tests: `keyEnds`, the words,
    /// then the stamps, places, bodies and both parts of the tags of the logged values}
    int[] arrayLengthsForTests() {
        return new int[]{keyEnds.length, values.length, places.logStamps().length, places.logPlaces().length,
                places.logBodies().length, places.tagHighs().length, places.tagLows().length};
    }

    private int keyStart(int entry) {
        return keyStart(keyEnds, entry);
    }

    // Where the key of `entry` starts, given where each key ends: where the key before it ends, unless the key did not
    // fit the rest of that chunk and starts the chunk of its last byte; a key has at least one byte
    private static int keyStart(int[] keyEnds, int entry) {
        var previous = entry == 0 ? 0 : keyEnds[entry - 1];
        return Math.max(previous, (keyEnds[entry] - 1) & ~KEY_OFFSET_MASK);
    }

    private static int chunkOf(long value) {
        return (int) ((value >>> OFFSET_BITS) & ((1L << CHUNK_BITS) - 1));
    }

    // Whether the word `value` is of a value on a home: a logged value's length may reach the bit of the kind
    private static boolean onHome(long value) {
        return (value & (REMOVES | LOGGED | HOME)) == HOME;
    }

    private static int homeIndex(long value) {
        return (int) ((value >>> HOME_INDEX_SHIFT) & (MAX_HOMES - 1));
    }

    private static int homeOffset(long value) {
        return (int) ((value >>> HOME_OFFSET_SHIFT) & MAX_HOME_OFFSET);
    }

    private static int homeLength(long value) {
        return (int) (value & MAX_HOME_LENGTH);
    }

    // The home the word `value` of a value on a home names, or null once the run let go of it
    private MemorySegment home(long value) {
        return (MemorySegment) HOMES.getAcquire(homes, homeIndex(value));
    }

    // The home the word `value` of a value on a home names, read back from its value frame if the run let go of it for
    // that frame; null once every value on it moved
    private MemorySegment homeOrFrame(long value) {
        var home = home(value);
        if (home != null) {
            return home;
        }
        var framed = this.framed;
        if (framed == null) {
            return null;
        }
        var index = homeIndex(value);
        var place = framed[index];
        return place == null ? null : readBack(index, place);
    }

    /// {@return home `index`, read back from the value frame `place` names, which the run let go of it for}
    ///
    /// Read into an array of its own, which becomes the home with a compare-and-set, so threads that read it back at
    /// once all use the one that won.
    ///
    /// @throws IllegalStateException if the frame is in a log segment a checkpoint retired, which only a reader of a
    ///                               later tree than the run's can find
    private MemorySegment readBack(int index, FramedHome place) {
        MemorySegment read;
        try {
            read = place.segment().readValueFrame(place.frameOffset(), place.length(), place.sequence());
        } catch (LogSegmentView.Retired _) {
            throw new IllegalStateException("A value in a retired log segment is newer than the tree of its reader");
        }
        var witness = (MemorySegment) HOMES.compareAndExchange(homes, index, (MemorySegment) null, read);
        return witness == null ? read : witness;
    }

    /// Reads back every home the run let go of for its value frame and has not read back yet, each on a virtual
    /// thread of its own, and waits for them: for a checkpoint about to take every entry of the run, which would
    /// otherwise read them one after the other as its merge reaches each home's first entry. A home whose read fails
    /// stays as it was, for the read of an entry on it to fail as it would have.
    void readBackHomes() {
        var framed = this.framed;
        if (framed == null) {
            return;
        }
        var readers = new ArrayList<Thread>();
        for (var home = 0; home < framed.length; home++) {
            var place = framed[home];
            if (place != null && HOMES.getAcquire(homes, home) == null) {
                var index = home;
                try {
                    readers.add(Thread.ofVirtual().name("ledger-home-read").start(() -> {
                        try {
                            readBack(index, place);
                        } catch (RuntimeException _) {
                            // Left to the read of an entry on the home, which fails as it would have
                        }
                    }));
                } catch (RuntimeException | OutOfMemoryError _) {
                    // Without a thread, the merge reads the home at its first entry
                    break;
                }
            }
        }
        var interrupted = false;
        for (var reader : readers) {
            while (true) {
                try {
                    reader.join();
                    break;
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /// {@return per home of this bulk commit's run, the index of the value frame of `starts` and `lengths` that holds
    /// it, or -1: the frame starts in the section where the home's start lies there, its values lie in the section at
    /// their offsets on the home, and the frame holds them up to the end of the last; `null` for a run whose commit's
    /// frame was not split}
    ///
    /// The run may let go of such a home for its frame, which holds the bytes of every value on it where the home does.
    ///
    /// @param starts where each value frame's bytes start in the section, in order
    /// @param lengths the bytes of each value frame
    int[] framedHomes(int[] starts, int[] lengths) {
        if (homeSections == null) {
            return null;
        }
        var frames = new int[homes.length];
        for (var home = 0; home < frames.length; home++) {
            frames[home] = -1;
            var start = homeSections[home] - 1;
            if (start < 0) {
                continue;
            }
            var frame = Arrays.binarySearch(starts, start);
            if (frame >= 0 && homeEnds[home] <= lengths[frame]) {
                frames[home] = frame;
            }
        }
        return frames;
    }

    /// Lets go of each home `frames` names a value frame of ([#framedHomes]), for the place of that frame in the log:
    /// value frame `f` at `offsets[f]` of `segment`, holding `lengths[f]` bytes, of the commit at `sequence`. A home of
    /// fewer than `minLength` bytes in its frame stays. Called once per run, once the frames are durable.
    ///
    /// The places are published before the first home is let go of, each with a release store, so a reader that loads
    /// a home as `null` finds its place and reads it back.
    ///
    /// @return the bytes of the value frames of the homes let go of
    long dropHomes(int[] frames, LogSegmentView segment, long[] offsets, int[] lengths, long sequence, int minLength) {
        var places = new FramedHome[homes.length];
        var bytes = 0L;
        for (var home = 0; home < places.length; home++) {
            var frame = frames[home];
            if (frame >= 0 && lengths[frame] >= minLength && HOMES.getAcquire(homes, home) != null) {
                places[home] = new FramedHome(segment, offsets[frame], lengths[frame], sequence);
                bytes += lengths[frame];
            }
        }
        if (bytes == 0) {
            return 0;
        }
        framed = places;
        for (var home = 0; home < places.length; home++) {
            if (places[home] != null) {
                HOMES.setRelease(homes, home, (MemorySegment) null);
            }
        }
        return bytes;
    }

    /// The value frame a bulk commit's run let go of a home for: the frame at `frameOffset` of `segment`, of the commit
    /// at `sequence`, whose body of `length` bytes holds the home's bytes from its start.
    ///
    /// @param segment the segment use that wrote the frame
    /// @param frameOffset where the frame starts in its segment
    /// @param length the bytes of the frame's body
    /// @param sequence the sequence the frame carries, its commit's
    record FramedHome(LogSegmentView segment, long frameOffset, int length, long sequence) {
    }

    // The place in the log of logged value `index` of `places`, made for the read
    private static LogValue logged(Places places, int index) {
        var place = places.logPlaces()[index];
        return new LogValue(places.segments()[(int) (place >>> FRAME_BITS)], place & FRAME_MASK, places.logBodies()[index],
                places.tagHighs()[index], places.tagLows()[index]);
    }

    // The logged value of an entry whose word is `value`, read from the log
    private byte[] readLogged(long value) {
        var index = (int) value;
        var places = this.places;
        try {
            return logged(places, index).read(places.logStamps()[index], (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
        } catch (LogSegmentView.Retired _) {
            throw new IllegalStateException("A value in a retired log segment is newer than the tree of its reader");
        }
    }

    /// Moves the values of `count` entries of a bulk commit's run off their homes, as [Version#spill] moves the value
    /// of a version: one of at least [SpillQueue#LOG_VALUE_MIN] bytes to its place in the log, where the frame at
    /// `frameOffset` of `segment` holds it durably, a shorter one to a chunk of the run's own; then lets go of each home
    /// no entry is on any more.
    ///
    /// The run publishes where the values go before it switches the word of any entry, each with a release store, so a
    /// reader that loads a switched word finds the value there; one that loaded the word before reads the home it
    /// holds, which no one writes and the store's pool never gets back. Called by the one thread that drains the
    /// commit's record.
    ///
    /// @param crypto the caller's, until it gives it back
    /// @param record the record of the commit, which says where its frames lie: the segment use, or `null` if no value
    ///               goes to the log, and the frame of each value, which a split commit's value frames may hold, the
    ///               bytes of its body 0 if not known, so no value is authenticated by the frame's tag
    ///               ([LogValue#framed()]); and the commit's sequence, which the tags bind with the position of each
    ///               write in the batch
    /// @param entries the entries, each listed once
    /// @param offsets where each entry's value starts in the commit's body, as the record lists it
    /// @param ops the position of each entry's write in the batch
    /// @return the values moved to the log and to a chunk, in this order
    int[] spill(SpillQueue.Crypto crypto, SpillQueue.Record record, int[] entries, int[] offsets, int[] ops, int count) {
        var segment = record.segment();
        var sequence = record.sequence();
        // Where the values go, laid out before anything moves
        var logged = 0;
        var chunkSizes = new int[4];
        var chunkCount = 0;
        var fill = ArrayWriteBatch.CHUNK_BYTES;
        for (var i = 0; i < count; i++) {
            var value = word(entries[i]);
            if (!onHome(value)) {
                continue;
            }
            var length = homeLength(value);
            if (length >= SpillQueue.LOG_VALUE_MIN) {
                if (segment != null) {
                    logged++;
                }
            } else if (length > 0) {
                if (fill + length > ArrayWriteBatch.CHUNK_BYTES) {
                    if (chunkCount == chunkSizes.length) {
                        chunkSizes = Arrays.copyOf(chunkSizes, 2 * chunkCount);
                    }
                    chunkCount++;
                    fill = 0;
                }
                fill += length;
                chunkSizes[chunkCount - 1] = fill;
            }
        }
        if (logged == 0 && chunkCount == 0) {
            return new int[2];
        }
        var before = places;
        var firstChunk = before.chunks().length;
        if (firstChunk + chunkCount > 1 << CHUNK_BITS) {
            return new int[2];
        }
        var chunkArrays = new byte[chunkCount][];
        var chunks = Arrays.copyOf(before.chunks(), firstChunk + chunkCount);
        for (var c = 0; c < chunkCount; c++) {
            chunkArrays[c] = new byte[chunkSizes[c]];
            chunks[firstChunk + c] = MemorySegment.ofArray(chunkArrays[c]).asReadOnly();
        }
        var firstLogged = before.logStamps().length;
        var total = firstLogged + logged;
        var segments = before.segments();
        var segmentIndex = -1;
        if (logged > 0) {
            segmentIndex = Arrays.asList(segments).indexOf(segment);
            if (segmentIndex < 0) {
                segments = Arrays.copyOf(segments, segments.length + 1);
                segmentIndex = segments.length - 1;
                segments[segmentIndex] = segment;
            }
        }
        var after = new Places(chunks, Arrays.copyOf(before.logStamps(), total), Arrays.copyOf(before.logPlaces(), total),
                Arrays.copyOf(before.logBodies(), total), Arrays.copyOf(before.tagHighs(), total),
                Arrays.copyOf(before.tagLows(), total), segments);
        // Published before the first word that refers to what it adds; written below only at indexes no word refers to
        // yet, each before the release store of the word that does
        places = after;
        var nextLogged = firstLogged;
        var chunk = firstChunk - 1;
        fill = ArrayWriteBatch.CHUNK_BYTES;
        var copied = 0;
        for (var i = 0; i < count; i++) {
            var entry = entries[i];
            var value = word(entry);
            if (!onHome(value)) {
                continue;
            }
            var homeIndex = homeIndex(value);
            var home = homes[homeIndex];
            var offset = homeOffset(value);
            var length = homeLength(value);
            long moved;
            if (length >= SpillQueue.LOG_VALUE_MIN) {
                if (segment == null) {
                    continue;
                }
                var listed = offsets[i];
                var frameOffset = record.frameOf(listed);
                var bodyIndex = record.indexOf(listed);
                var frameBody = record.bodyOf(listed);
                var stamp = Reads.stamp(sequence, ops[i]);
                var place = SpillQueue.framesAlone(segment, frameBody, length)
                        ? LogValue.framed(segment, frameOffset, bodyIndex, frameBody)
                        : crypto.logValue(segment, frameOffset, bodyIndex, stamp, home, offset, length);
                var index = nextLogged++;
                after.logStamps()[index] = stamp;
                after.logPlaces()[index] = (long) segmentIndex << FRAME_BITS | place.frameOffset();
                after.logBodies()[index] = place.bodyIndex();
                after.tagHighs()[index] = place.tagHigh();
                after.tagLows()[index] = place.tagLow();
                moved = LOGGED | (long) length << 32 | (index & 0xFFFF_FFFFL);
            } else if (length > 0) {
                if (fill + length > ArrayWriteBatch.CHUNK_BYTES) {
                    chunk++;
                    fill = 0;
                }
                MemorySegment.copy(home, ValueLayout.JAVA_BYTE, offset, chunkArrays[chunk - firstChunk], fill, length);
                moved = (long) length << LENGTH_SHIFT | (long) chunk << OFFSET_BITS | fill;
                fill += length;
                copied++;
            } else {
                continue;
            }
            WORDS.setRelease(values, entry, moved);
            if (--homeEntries[homeIndex] == 0) {
                // No word names it any more: a reader that loaded one before finds it gone and loads the word again
                HOMES.setRelease(homes, homeIndex, (MemorySegment) null);
            }
        }
        return new int[]{nextLogged - firstLogged, copied};
    }

    // The entries sorted by key: a merge sort of their indexes, which moves no key
    private int[] sortedOrder() {
        var sorted = new int[size];
        for (var i = 0; i < size; i++) {
            sorted[i] = i;
        }
        if (size < 2) {
            return sorted;
        }
        var scratch = new int[size];
        var from = sorted;
        var to = scratch;
        for (var width = 1; width < size; width *= 2) {
            for (var low = 0; low < size; low += 2 * width) {
                var middle = Math.min(low + width, size);
                var high = Math.min(low + 2 * width, size);
                merge(from, to, low, middle, high);
            }
            var swap = from;
            from = to;
            to = swap;
        }
        return from;
    }

    private void merge(int[] from, int[] to, int low, int middle, int high) {
        var left = low;
        var right = middle;
        for (var out = low; out < high; out++) {
            if (left < middle && (right >= high || compareEntries(from[left], from[right]) <= 0)) {
                to[out] = from[left++];
            } else {
                to[out] = from[right++];
            }
        }
    }

    private int compareEntries(int left, int right) {
        var leftStart = keyStart(left);
        var rightStart = keyStart(right);
        return Segments.compare(keys[leftStart >>> KEY_CHUNK_BITS], leftStart & KEY_OFFSET_MASK, keyEnds[left] - leftStart,
                keys[rightStart >>> KEY_CHUNK_BITS], rightStart & KEY_OFFSET_MASK, keyEnds[right] - rightStart);
    }

    /// Collects the writes of one paged key space as the open replays them, the newest of each key, into a [PackedRun].
    ///
    /// Used by the replay, on the opening thread, and by a bulk commit, on its committing thread, each of which builds
    /// the run once and uses the builder no more: the run takes its arrays or copies of them, and the builder lets go of
    /// them.
    static final class Builder {
        private static final int FIRST_ENTRIES = 64;
        // The chunks of the keys, the last of which is `keys`: the first doubles from a small array up to a whole
        // chunk, and every later one is allocated whole
        private ArrayList<byte[]> keyChunks;
        private byte[] keys;
        // Where the last key ends, as its chunk times KEY_CHUNK plus its end in the chunk
        private int keyBytes;
        private int[] keyEnds;
        private long[] values;
        private int size;
        private final ArrayList<MemorySegment> chunks;
        private long[] logStamps;
        private long[] logPlaces;
        private int[] logBodies;
        private long[] tagHighs;
        private int[] tagLows;
        private int logged;
        private final ArrayList<LogSegmentView> segments;
        private int[] slots;
        private long minStamp;
        private long maxStamp;
        // The homes of a bulk commit's values, the last one added, and the entries on each; null for a replay's builder
        private ArrayList<MemorySegment> homes;
        private int[] homeEntries;
        private MemorySegment lastHome;
        // The entries the arrays of the entries grow to room for when they are next full, or 0 to double them
        private int reserved;
        // Per home, as PackedRun keeps them, once a value placed in the section of a split frame said; null before
        private int[] homeSections;
        private int[] homeEnds;

        Builder() {
            this(FIRST_ENTRIES, FIRST_ENTRIES * 32L);
        }

        /// Creates a builder with room for `entries` entries of `keyBytes` bytes of keys in all, which it takes without
        /// growing an array: a bulk commit knows both before it applies.
        Builder(int entries, long keyBytes) {
            var capacity = Math.max(1, entries);
            this.keys = new byte[(int) Math.clamp(keyBytes, 1, KEY_CHUNK)];
            this.keyChunks = new ArrayList<>();
            keyChunks.add(keys);
            this.keyEnds = new int[capacity];
            this.values = new long[capacity];
            this.chunks = new ArrayList<>();
            this.logStamps = new long[0];
            this.logPlaces = new long[0];
            this.logBodies = new int[0];
            this.tagHighs = new long[0];
            this.tagLows = new int[0];
            this.segments = new ArrayList<>();
            // The least power of two the entries fill at most three quarters of, as the table grows past that
            var table = 2;
            while (4L * capacity > 3L * table) {
                table *= 2;
            }
            this.slots = new int[table];
            this.minStamp = Long.MAX_VALUE;
            this.maxStamp = -1;
        }

        /// {@return whether the builder holds no entry}
        boolean isEmpty() {
            return size == 0;
        }

        /// {@return the entry of `key`, or -1 if the builder holds none}
        int find(MemorySegment key) {
            return find(key, Segments.hash(key));
        }

        /// {@return the entry of `key`, whose hash is `hash` ([Segments#hash(MemorySegment)]), or -1 if the builder
        /// holds none}
        int find(MemorySegment key, int hash) {
            var slot = slots[slotOf(key, hash)];
            return slot == 0 ? -1 : (slot & ENTRY_MASK) - 1;
        }

        /// {@return whether `entry`, the newest write of its key so far, removes it}
        boolean removes(int entry) {
            return values[entry] < 0;
        }

        /// {@return whether the builder takes a write of `key` with a value of `length` bytes in `segment`, or of no
        /// segment if `null`: past [#MAX_ENTRIES] entries, [#MAX_SEGMENTS] segments or [#MAX_VALUE_LENGTH] bytes, when
        /// the key could end past [#MAX_KEY_BYTES] bytes of keys, and for a key of no bytes or more than
        /// [KeyCodec#MAX_LENGTH], which only a malformed log holds, the replay writes to chains}
        ///
        /// It stops [KeyCodec#MAX_LENGTH] bytes short of [#MAX_KEY_BYTES]: a key that does not fit the rest of a chunk
        /// starts the next one, leaving unused fewer bytes than it has.
        boolean takes(MemorySegment key, LogSegmentView segment, int length) {
            return takes(key.byteSize(), segment, length);
        }

        /// {@return whether the builder takes a write of a key of `keyLength` bytes, as
        /// [#takes(MemorySegment, LogSegmentView, int)] gives it of a key of that length}
        boolean takes(long keyLength, LogSegmentView segment, int length) {
            if (size >= MAX_ENTRIES || length > MAX_VALUE_LENGTH || keyLength < 1 || keyLength > KeyCodec.MAX_LENGTH
                    || keyBytes + keyLength + KeyCodec.MAX_LENGTH > MAX_KEY_BYTES) {
                return false;
            }
            return segment == null || segments.size() < MAX_SEGMENTS
                    || segments.getLast() == segment;
        }

        /// Records a put of the key that is the `keyLength` bytes at `keyFrom` of `keyArray`, whose hash is `hash`
        /// ([Segments#hash(byte\[\], int, int)]), stamped `stamp`, of the `length` bytes at `offset` of `chunk`, a
        /// chunk of the replay, or of an empty value if `length` is 0.
        void put(byte[] keyArray, int keyFrom, int keyLength, int hash, long stamp, MemorySegment chunk, int offset,
                 int length) {
            var entry = entry(keyArray, keyFrom, keyLength, hash, stamp);
            if (length == 0) {
                values[entry] = 0;
                return;
            }
            var index = chunks.size() - 1;
            if (index < 0 || chunks.get(index) != chunk) {
                chunks.add(chunk);
                index++;
            }
            values[entry] = (long) length << LENGTH_SHIFT | (long) index << OFFSET_BITS | offset;
        }

        /// Records a put of the key that is the `keyLength` bytes at `keyFrom` of `keyArray`, whose hash is `hash`
        /// ([Segments#hash(byte\[\], int, int)]), stamped `stamp`, of the `length` bytes in the log at `bodyIndex` of
        /// the body of the frame at `frameOffset` of `logSegment`, under the tag of `tagHigh` and `tagLow`: the fields
        /// of a [LogValue] of the value, which the builder keeps in its arrays.
        void putLogged(byte[] keyArray, int keyFrom, int keyLength, int hash, long stamp, LogSegmentView logSegment,
                       long frameOffset, int bodyIndex, long tagHigh, int tagLow, int length) {
            var entry = entry(keyArray, keyFrom, keyLength, hash, stamp);
            var previous = values[entry];
            int index;
            if (previous >= 0 && (previous & LOGGED) != 0) {
                // An overwrite of a value in the log takes its place in the arrays
                index = (int) previous;
            } else {
                index = this.logged++;
                if (index == logStamps.length) {
                    var capacity = Math.max(FIRST_ENTRIES, 2 * index);
                    logStamps = Arrays.copyOf(logStamps, capacity);
                    logPlaces = Arrays.copyOf(logPlaces, capacity);
                    logBodies = Arrays.copyOf(logBodies, capacity);
                    tagHighs = Arrays.copyOf(tagHighs, capacity);
                    tagLows = Arrays.copyOf(tagLows, capacity);
                }
            }
            var segment = segments.isEmpty() ? -1 : segments.size() - 1;
            if (segment < 0 || segments.get(segment) != logSegment) {
                segments.add(logSegment);
                segment++;
            }
            logStamps[index] = stamp;
            logPlaces[index] = (long) segment << FRAME_BITS | frameOffset;
            logBodies[index] = bodyIndex;
            tagHighs[index] = tagHigh;
            tagLows[index] = tagLow;
            values[entry] = LOGGED | (long) length << 32 | (index & 0xFFFF_FFFFL);
        }

        /// {@return the entries the builder holds}
        int size() {
            return size;
        }

        /// {@return the entries the builder has room for before its arrays of the entries grow}
        int capacity() {
            return values.length;
        }

        /// {@return the lengths of the arrays of the logged values, for tests: their stamps, places, bodies and both
        /// parts of their tags}
        int[] loggedLengthsForTests() {
            return new int[]{logStamps.length, logPlaces.length, logBodies.length, tagHighs.length, tagLows.length};
        }

        /// Makes the next growth of the arrays give them room for `capacity` entries in all, rather than for twice the
        /// entries, if that is more than the builder holds then: the arrays of the entries take that room, and those of
        /// the values in the log the share of it that the entries logged so far are of all the entries and a sixteenth
        /// more, each group in one copy and only to more room than it has. The growths after that one double the arrays
        /// again; a later call replaces the room an earlier one asked for before the arrays grew.
        ///
        /// The hash table keeps doubling as the entries fill it, and [#build] still trims every array to the entries
        /// it holds, so the room changes only how many copies the arrays take on the way, not the run.
        void reserve(int capacity) {
            reserved = Math.min(capacity, MAX_ENTRIES);
        }

        /// {@return whether [#putHome] takes a put of `key` of the `length` bytes at `offset` of `home`: the place of the
        /// value fits the word of an entry, and the builder has room}
        boolean fits(MemorySegment key, MemorySegment home, int offset, int length) {
            if (length > MAX_HOME_LENGTH || offset > MAX_HOME_OFFSET || !takes(key, null, length)) {
                return false;
            }
            return length == 0 || home == lastHome || homes == null || homes.size() < MAX_HOMES;
        }

        /// Records a put of `key`, which the builder holds no entry of, stamped `stamp`, of the `length` bytes at `offset`
        /// of `home`, a read-only view of an array a bulk commit put the value on, which no one writes and the store's
        /// pool never gets back; or of an empty value if `length` is 0.
        ///
        /// @param hash the hash of `key`, as [Segments#hash(MemorySegment)] gives it
        /// @return the entry
        /// @throws IllegalArgumentException if the builder does not [take][#fits] the put
        int putHome(MemorySegment key, int hash, long stamp, MemorySegment home, int offset, int length) {
            if (!fits(key, home, offset, length)) {
                throw new IllegalArgumentException("The run does not take the put");
            }
            if (length == 0) {
                var entry = entry(key, hash, stamp);
                values[entry] = 0;
                return entry;
            }
            if (homes == null) {
                homes = new ArrayList<>();
                homeEntries = new int[8];
            }
            var index = homes.size() - 1;
            if (home != lastHome) {
                homes.add(home);
                lastHome = home;
                index++;
                if (index == homeEntries.length) {
                    homeEntries = Arrays.copyOf(homeEntries, 2 * index);
                }
            }
            var entry = entry(key, hash, stamp);
            homeEntries[index]++;
            values[entry] = HOME | (long) index << HOME_INDEX_SHIFT | (long) offset << HOME_OFFSET_SHIFT | length;
            return entry;
        }

        /// Records that the value of the last [#putHome], of `length` bytes at `offset` of its home, starts at `start`
        /// of the section of the commit's split frame ([FrameBody#putSection]), so the run knows which homes a value
        /// frame holds whole ([PackedRun#framedHomes]).
        void placeInSection(int offset, int length, int start) {
            if (length == 0) {
                return;
            }
            var index = homes.size() - 1;
            if (homeSections == null) {
                homeSections = new int[homeEntries.length];
                homeEnds = new int[homeEntries.length];
            } else if (index >= homeSections.length) {
                homeSections = Arrays.copyOf(homeSections, homeEntries.length);
                homeEnds = Arrays.copyOf(homeEnds, homeEntries.length);
            }
            // Where the home's start lies in the section, as this value places it: before the section, the home holds
            // other bytes before its first value there
            var base = start - offset;
            var known = homeSections[index];
            if (known != -1) {
                if (base < 0 || known != 0 && known != base + 1) {
                    homeSections[index] = -1;
                } else {
                    homeSections[index] = base + 1;
                }
            }
            homeEnds[index] = Math.max(homeEnds[index], offset + length);
        }

        /// Records a removal of the key that is the `keyLength` bytes at `keyFrom` of `keyArray`, whose hash is `hash`
        /// ([Segments#hash(byte\[\], int, int)]), stamped `stamp`.
        void delete(byte[] keyArray, int keyFrom, int keyLength, int hash, long stamp) {
            // Found or added first: adding may replace the array
            var entry = entry(keyArray, keyFrom, keyLength, hash, stamp);
            values[entry] = REMOVES;
        }

        // The slot of the hash table that holds the entry of `key`, whose hash is `hash`, or the empty one it would take
        private int slotOf(MemorySegment key, int hash) {
            var mask = slots.length - 1;
            var tag = hash & TAG_MASK;
            for (var i = hash & mask; ; i = (i + 1) & mask) {
                var slot = slots[i];
                if (slot == 0) {
                    return i;
                }
                if ((slot & TAG_MASK) == tag) {
                    var entry = (slot & ENTRY_MASK) - 1;
                    var start = keyStart(keyEnds, entry);
                    var length = keyEnds[entry] - start;
                    if (length == key.byteSize()
                            && Segments.compare(keyChunks.get(start >>> KEY_CHUNK_BITS), start & KEY_OFFSET_MASK, length, key) == 0) {
                        return i;
                    }
                }
            }
        }

        // As slotOf for the key that is the keyLength bytes at keyFrom of keyArray
        private int slotOf(byte[] keyArray, int keyFrom, int keyLength, int hash) {
            var mask = slots.length - 1;
            var tag = hash & TAG_MASK;
            for (var i = hash & mask; ; i = (i + 1) & mask) {
                var slot = slots[i];
                if (slot == 0) {
                    return i;
                }
                if ((slot & TAG_MASK) == tag) {
                    var entry = (slot & ENTRY_MASK) - 1;
                    var start = keyStart(keyEnds, entry);
                    var length = keyEnds[entry] - start;
                    if (length == keyLength && Segments.compare(keyChunks.get(start >>> KEY_CHUNK_BITS),
                            start & KEY_OFFSET_MASK, length, keyArray, keyFrom, keyLength) == 0) {
                        return i;
                    }
                }
            }
        }

        // The entry of `key`, whose hash is `hash`, added if the builder holds none, whose write stamped `stamp` is the
        // newest
        private int entry(MemorySegment key, int hash, long stamp) {
            minStamp = Math.min(minStamp, stamp);
            maxStamp = Math.max(maxStamp, stamp);
            var i = slotOf(key, hash);
            var found = slots[i];
            if (found != 0) {
                return (found & ENTRY_MASK) - 1;
            }
            var length = Math.toIntExact(key.byteSize());
            var entry = place(length);
            MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, keys, (keyBytes - length) & KEY_OFFSET_MASK, length);
            return add(i, hash, entry);
        }

        // As entry for the key that is the length bytes at from of array
        private int entry(byte[] array, int from, int length, int hash, long stamp) {
            minStamp = Math.min(minStamp, stamp);
            maxStamp = Math.max(maxStamp, stamp);
            var i = slotOf(array, from, length, hash);
            var found = slots[i];
            if (found != 0) {
                return (found & ENTRY_MASK) - 1;
            }
            var entry = place(length);
            System.arraycopy(array, from, keys, (keyBytes - length) & KEY_OFFSET_MASK, length);
            return add(i, hash, entry);
        }

        // Adds an entry of a key of `length` bytes, whose bytes the caller then copies into the last `length` bytes
        // `keyBytes` reaches in `keys`, a chunk of its own or the rest of the last one
        private int place(int length) {
            var entry = size;
            if (entry == values.length) {
                growEntries();
            }
            var start = keyBytes;
            if ((start & KEY_OFFSET_MASK) + length > KEY_CHUNK) {
                // The rest of the chunk is too short: the key starts the next one
                start = (start + KEY_OFFSET_MASK) & ~KEY_OFFSET_MASK;
            }
            var offset = start & KEY_OFFSET_MASK;
            if (start >>> KEY_CHUNK_BITS == keyChunks.size()) {
                keys = new byte[KEY_CHUNK];
                keyChunks.add(keys);
            } else if (offset + length > keys.length) {
                // Only the first chunk is shorter than a chunk, and it is the last until it is whole
                keys = Arrays.copyOf(keys, Math.min(KEY_CHUNK, Math.max(offset + length, 2 * keys.length)));
                keyChunks.set(keyChunks.size() - 1, keys);
            }
            keyBytes = start + length;
            keyEnds[entry] = keyBytes;
            size = entry + 1;
            return entry;
        }

        // Puts `entry`, whose key's hash is `hash`, in the empty slot `i` of the hash table, which doubles past three
        // quarters full
        private int add(int i, int hash, int entry) {
            slots[i] = (hash & TAG_MASK) | (entry + 1);
            if (4L * size > 3L * slots.length) {
                grow();
            }
            return entry;
        }

        // Grows the arrays of the entries, full, to the room reserve asked for if that is more, and the arrays of the
        // logged values to its logged share if that is more than their room; else doubles the arrays of the entries
        private void growEntries() {
            var capacity = reserved > size ? reserved : 2 * size;
            keyEnds = Arrays.copyOf(keyEnds, capacity);
            values = Arrays.copyOf(values, capacity);
            if (reserved > size && logged > 0) {
                // A sixteenth more than the share, which may grow; all of it when overwrites by short values left more
                // logged values than entries
                var loggedCapacity = (int) Math.min(capacity, (long) capacity * logged / size + capacity / 16);
                if (loggedCapacity > logStamps.length) {
                    logStamps = Arrays.copyOf(logStamps, loggedCapacity);
                    logPlaces = Arrays.copyOf(logPlaces, loggedCapacity);
                    logBodies = Arrays.copyOf(logBodies, loggedCapacity);
                    tagHighs = Arrays.copyOf(tagHighs, loggedCapacity);
                    tagLows = Arrays.copyOf(tagLows, loggedCapacity);
                }
            }
            reserved = 0;
        }

        // Doubles the hash table, placing every entry again by the hash of its key
        private void grow() {
            var grown = new int[2 * slots.length];
            var mask = grown.length - 1;
            for (var entry = 0; entry < size; entry++) {
                var start = keyStart(keyEnds, entry);
                var hash = Segments.hash(keyChunks.get(start >>> KEY_CHUNK_BITS), start & KEY_OFFSET_MASK, keyEnds[entry] - start);
                var i = hash & mask;
                while (grown[i] != 0) {
                    i = (i + 1) & mask;
                }
                grown[i] = (hash & TAG_MASK) | (entry + 1);
            }
            slots = grown;
        }

        /// {@return the run of the writes recorded, counted against `base`, the root of the key space's tree the store
        /// opened from in `pages`, or `null`}
        ///
        /// The builder holds nothing after.
        PackedRun build(TreeRoot base, PageFile pages) {
            return new PackedRun(this, base, pages, false);
        }

        /// {@return the run of the puts of new keys a bulk commit recorded and counted, none of which the latest tree of
        /// `pages` held when the commit applied, nor any tree a reader that reads the run reads}
        ///
        /// The builder holds nothing after.
        PackedRun buildCommitted(PageFile pages) {
            return new PackedRun(this, null, pages, true);
        }
    }

    /// A scan's walk over the entries of the tree the store opened from with the run over them: a key of the run takes
    /// the run's entry and shadows the tree's, a removal of the run hides the tree's entry, and the others are the
    /// tree's.
    ///
    /// The scan applies the range removals since that tree to every entry it returns, those of the run included, as
    /// each removal it applies is newer than every entry of the run.
    static final class Cursor implements StoredCursor {
        // Whether the entry the cursor is at is the run's, the tree's, or neither
        private static final int NONE = 0;
        private static final int RUN = 1;
        private static final int TREE = 2;
        private final PackedRun run;
        // The run's entries in the order of their keys
        private final int[] order;
        private final TreeCursor tree;
        private final boolean descending;
        // The positions of the key order in the range: from `first`, included, to `last`, excluded
        private final int first;
        private final int last;
        // The position of the next entry of the run in the scan's order
        private int position;
        private int current;
        // Whether the tree's cursor is at an entry not returned yet, and whether the run's current entry shadows it
        private boolean treeHas;
        private boolean shadows;
        private boolean started;
        // The key of the run's current entry, made once per entry
        private MemorySegment runKey;
        private int runEntry;

        /// Creates the walk over the run's entries from `from`, included, to `to`, excluded, either `null` for no
        /// bound, merged over `tree`, a cursor of the same range and order, or none if `null`.
        Cursor(PackedRun run, TreeCursor tree, MemorySegment from, MemorySegment to, boolean descending) {
            this.run = run;
            this.order = run.order();
            this.tree = tree;
            this.descending = descending;
            this.first = run.lowerPosition(from);
            this.last = to == null ? run.size : Math.max(first, run.lowerPosition(to));
            this.position = descending ? last - 1 : first;
        }

        @Override
        public boolean next() {
            if (!started) {
                started = true;
                treeHas = tree != null && tree.next();
            } else if (current == TREE) {
                treeHas = tree.next();
            } else if (current == RUN) {
                position += descending ? -1 : 1;
                if (shadows) {
                    treeHas = tree.next();
                }
            }
            while (true) {
                if (position < first || position >= last) {
                    current = treeHas ? TREE : NONE;
                    return treeHas;
                }
                var entry = order[position];
                var key = run.key(entry);
                // Where the run's entry sorts against the tree's, in the order of the scan
                var order = -1;
                if (treeHas) {
                    order = -tree.compareKey(key);
                    if (descending) {
                        order = -order;
                    }
                }
                if (order > 0) {
                    current = TREE;
                    return true;
                }
                if (run.removes(entry)) {
                    position += descending ? -1 : 1;
                    if (order == 0) {
                        treeHas = tree.next();
                    }
                    continue;
                }
                current = RUN;
                shadows = order == 0;
                runEntry = entry;
                runKey = key;
                return true;
            }
        }

        @Override
        public MemorySegment key() {
            return current == RUN ? runKey : tree.key();
        }

        @Override
        public int compareKey(MemorySegment key) {
            return current == RUN ? Segments.compare(runKey, key) : tree.compareKey(key);
        }

        @Override
        public MemorySegment value() {
            return current == RUN ? run.value(runEntry) : tree.value();
        }

        @Override
        public void took() {
            if (tree != null) {
                tree.took();
            }
        }

        @Override
        public void release() {
            if (tree != null) {
                tree.release();
            }
        }
    }

    /// Where the values of a run that are not on a home lie.
    ///
    /// @param chunks the read-only views of the chunks the short values lie in
    /// @param logStamps per logged value, the stamp of its write, which its tag binds
    /// @param logPlaces per logged value, the index of its segment in `segments` over where its frame starts
    /// @param logBodies per logged value, where it lies in its frame's body
    /// @param tagHighs per logged value, the first part of its tag ([LogValue])
    /// @param tagLows per logged value, the second part of its tag
    /// @param segments the segment uses the logged values are in
    private record Places(MemorySegment[] chunks, long[] logStamps, long[] logPlaces, int[] logBodies, long[] tagHighs,
                          int[] tagLows, LogSegmentView[] segments) {
    }
}
