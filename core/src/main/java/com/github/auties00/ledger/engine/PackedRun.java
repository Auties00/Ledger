package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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
/// keeps its place, its tag and its stamp, which the tag binds, as a [LogValue] would. A message that took a chain, a
/// version, a [LogValue], an entry of the hash index, of the dirty and blind lists, and a node of the ordered map once
/// scanned, about 300 bytes over its key and value, takes about 50 here.
///
/// A [RunFreezer] builds the same arrays, between checkpoints, from the published chains of a paged key space whose
/// values left their batches' arrays, merged with the run it replaces: a frozen run, whose entries are already in the
/// order of their keys. Its readers are those of the latest tree, every one of which sees each of its writes, and every
/// range removal they apply over it is newer than the entries it covers, so it reads as the replay's run does. Its
/// writes were counted as their chains' were, by [KeyspaceState#counts] and [BlindWrites], which also holds what the
/// blind ones among them do to the count ([BlindWrites#correction(long, java.util.function.Predicate)]), so
/// [#correction()] does not count it; [#correction(MemorySegment, MemorySegment)] does, for the counts of ranges.
///
/// Immutable once built, so readers use it without synchronization; [#correction()] and the tree lookups it caches
/// take a lock of their own.
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
    // The kind and place of a value, in the word of an entry: the sign bit for a removal; then a bit for a value in the
    // log, whose length and index among the logged values follow; otherwise the length, the chunk and the offset of a
    // value in a chunk of the replay
    private static final long REMOVES = 1L << 63;
    private static final long LOGGED = 1L << 62;
    private static final int OFFSET_BITS = 20;
    private static final int CHUNK_BITS = 24;
    private static final int LENGTH_SHIFT = OFFSET_BITS + CHUNK_BITS;
    private static final long LENGTH_MASK = (1L << (62 - LENGTH_SHIFT)) - 1;
    private static final long LOGGED_LENGTH_MASK = (1L << 30) - 1;
    // A value's place in the log: the index of its segment in the table above the bits of where its frame starts
    private static final int FRAME_BITS = 48;
    private static final long FRAME_MASK = (1L << FRAME_BITS) - 1;
    // A slot of the hash table: the top byte of the key's hash over the entry's index plus one, 0 for none
    private static final int ENTRY_MASK = (1 << 24) - 1;
    /// What [BlindWrites] counts for an entry of a frozen run: nothing, as its chain's writes were counted as they
    /// committed, or its blind write's slot still counts it.
    static final byte COUNTED = 0;
    /// What [BlindWrites] counts for an entry of a frozen run: its chain's blind write, a put, which adds an entry
    /// unless the tree under the run holds the key.
    static final byte BLIND_PUT = 1;
    /// What [BlindWrites] counts for an entry of a frozen run: its chain's blind write, a removal, which takes an entry
    /// away if the tree under the run holds the key.
    static final byte BLIND_REMOVE = 2;
    /// What [BlindWrites] counts for an entry of a frozen run: nothing, as the chain it was frozen from may still hold
    /// the key and its blind write's slot, if it has one, counts it; a later freeze that drops the chain takes the
    /// write over.
    static final byte SHADOWED = 3;
    private static final int TAG_MASK = ~ENTRY_MASK;
    private static final MemorySegment EMPTY = MemorySegment.ofArray(new byte[0]).asReadOnly();

    static {
        if (ReplayValues.CHUNK_BYTES > 1 << OFFSET_BITS || SpillQueue.LOG_VALUE_MIN > LENGTH_MASK) {
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
    // The kind and place of each entry's value
    private final long[] values;
    // The read-only views of the replay's chunks the short values lie in
    private final MemorySegment[] chunks;
    // Per logged value: the stamp of its write, which its tag binds; the segment index and frame offset; where it lies in
    // the frame's body; and its tag ([LogValue])
    private final long[] logStamps;
    private final long[] logPlaces;
    private final int[] logBodies;
    private final long[] tagHighs;
    private final int[] tagLows;
    private final LogSegmentView[] segments;
    // Open addressing on the hash of the keys, at most three quarters full
    private final int[] slots;
    // The entries in the order of their keys, or null for a frozen run, whose entries are in that order
    private final int[] order;
    // Whether a freeze built the run, rather than an open's replay
    private final boolean frozen;
    private final int size;
    // The stamp of the newest write the run holds: a reader whose tree is at or above it reads the tree alone
    private final long maxStamp;
    // The root of the key space's tree the store opened from, or null, and the file it is in: what the run's writes are
    // counted against
    private final TreeRoot base;
    private final PageFile pages;
    // Guards the lookups of the keys in `base` that the count of the run makes
    private final Object lock;
    // Per entry, whether `base` holds its key, as TreeSearch.FOUND or ABSENT, or UNKNOWN; made at the first need
    private volatile byte[] inBase;

    /// Creates the run of the writes `builder` recorded, which lets go of its arrays as it copies each, so the arrays
    /// the builder grew and their trimmed copies are never all live at once.
    private PackedRun(Builder builder, TreeRoot base, PageFile pages) {
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
        this.keyEnds = Arrays.copyOf(builder.keyEnds, size);
        builder.keyEnds = null;
        this.keyViews = new MemorySegment[keys.length];
        for (var i = 0; i < keys.length; i++) {
            keyViews[i] = MemorySegment.ofArray(keys[i]).asReadOnly();
        }
        this.values = Arrays.copyOf(builder.values, size);
        builder.values = null;
        this.chunks = builder.chunks.toArray(MemorySegment[]::new);
        var logged = builder.logged;
        this.logStamps = Arrays.copyOf(builder.logStamps, logged);
        builder.logStamps = null;
        this.logPlaces = Arrays.copyOf(builder.logPlaces, logged);
        builder.logPlaces = null;
        this.logBodies = Arrays.copyOf(builder.logBodies, logged);
        builder.logBodies = null;
        this.tagHighs = Arrays.copyOf(builder.tagHighs, logged);
        builder.tagHighs = null;
        this.tagLows = Arrays.copyOf(builder.tagLows, logged);
        builder.tagLows = null;
        this.segments = builder.segments.toArray(LogSegmentView[]::new);
        // Kept as the builder filled it: the entries keep their indexes, and the table is at most three quarters full
        this.slots = builder.slots;
        builder.slots = null;
        this.maxStamp = builder.maxStamp;
        this.base = base;
        this.pages = pages;
        this.lock = new Object();
        this.order = sortedOrder();
        this.frozen = false;
    }

    /// Creates a frozen run of the arrays `merge` filled, which it takes as they are.
    private PackedRun(Merge merge, TreeRoot base, PageFile pages) {
        this.size = merge.size;
        this.keys = merge.keys;
        this.keyEnds = merge.keyEnds;
        this.keyViews = new MemorySegment[keys.length];
        for (var i = 0; i < keys.length; i++) {
            keyViews[i] = MemorySegment.ofArray(keys[i]).asReadOnly();
        }
        this.values = merge.values;
        this.chunks = merge.chunks.toArray(MemorySegment[]::new);
        this.logStamps = merge.logStamps;
        this.logPlaces = merge.logPlaces;
        this.logBodies = merge.logBodies;
        this.tagHighs = merge.tagHighs;
        this.tagLows = merge.tagLows;
        this.segments = merge.segments.toArray(LogSegmentView[]::new);
        this.slots = merge.slots;
        this.maxStamp = merge.maxStamp;
        this.base = base;
        this.pages = pages;
        this.lock = new Object();
        this.order = null;
        this.frozen = true;
        this.inBase = merge.inBase;
    }

    /// {@return whether a freeze built the run ([RunFreezer]), rather than an open's replay}
    boolean frozen() {
        return frozen;
    }

    /// {@return the root of the tree the run's writes are counted against, or `null` for none}
    TreeRoot base() {
        return base;
    }

    /// {@return the stamp of the newest write the run holds}
    ///
    /// A reader takes the run as part of its tree while that tree's bound is below it: then the tree is the one the
    /// store opened from, or there is none.
    long maxStamp() {
        return maxStamp;
    }

    /// {@return the entries of the run}
    int size() {
        return size;
    }

    /// {@return the entry of `key`, or -1 if the run holds none}
    int find(MemorySegment key) {
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
        return values[entry] < 0;
    }

    /// {@return the key of `entry`, a read-only slice of the run's keys}
    MemorySegment key(int entry) {
        var start = keyStart(entry);
        return keyViews[start >>> KEY_CHUNK_BITS].asSlice(start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `chain` and the key of `entry`, read in place, as
    /// [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareChain(Chain chain, int entry) {
        var start = keyStart(entry);
        return chain.compareKey(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `entry` and `key`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(int entry, MemorySegment key) {
        var start = keyStart(entry);
        return Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start, key);
    }

    /// {@return the value of `entry`, which puts its key, read-only: a slice of a chunk of the replay, or read from the
    /// log into an array of its own}
    ///
    /// @throws IllegalStateException if the value is in a log segment a checkpoint retired, which only a reader of a
    ///                               later tree than the run's can find
    /// @throws com.github.auties00.ledger.LedgerIoException if the value is in the log and cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the value is in the log and the log changed
    MemorySegment value(int entry) {
        var value = values[entry];
        if ((value & LOGGED) != 0) {
            return MemorySegment.ofArray(readLogged(value)).asReadOnly();
        }
        var length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
        if (length == 0) {
            return EMPTY;
        }
        return chunks[chunkOf(value)].asSlice(value & ((1L << OFFSET_BITS) - 1), length);
    }

    /// {@return the value of `entry`, which puts its key, in a new array}
    ///
    /// @throws IllegalStateException as [#value(int)] does
    byte[] valueBytes(int entry) {
        var value = values[entry];
        if ((value & LOGGED) != 0) {
            return readLogged(value);
        }
        var length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
        var copy = new byte[length];
        if (length > 0) {
            MemorySegment.copy(chunks[chunkOf(value)], ValueLayout.JAVA_BYTE, value & ((1L << OFFSET_BITS) - 1), copy, 0, length);
        }
        return copy;
    }

    /// {@return the version a checkpoint writes for `entry`, made for the call: a version of its value in a chunk, or in
    /// the log under the stamp its tag binds; `null` for a removal}
    ///
    /// A checkpoint makes it only as it writes the entry into its leaf ([TreeUpdate.Op]), so an update holds no version
    /// per entry of the run.
    Version version(int entry) {
        var value = values[entry];
        if (value < 0) {
            return null;
        }
        if ((value & LOGGED) != 0) {
            var index = (int) value;
            return Version.ofLogged(logStamps[index], logged(index), (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
        }
        var length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
        if (length == 0) {
            return new Version(0, EMPTY, null, null);
        }
        return new Version(0, chunks[chunkOf(value)], (int) (value & ((1L << OFFSET_BITS) - 1)), length, null, null);
    }

    /// {@return the position in the key order of the first entry whose key is at or after `key`, or [#size()] if none
    /// is; 0 for `null`}
    int lowerPosition(MemorySegment key) {
        if (key == null) {
            return 0;
        }
        var low = 0;
        var high = size;
        while (low < high) {
            var middle = (low + high) >>> 1;
            if (compareKey(entryAt(middle), key) < 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        return low;
    }

    /// {@return the entry at `position` of the key order}
    int entryAt(int position) {
        return order == null ? position : order[position];
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

    /// Records whether the tree the run's writes are counted against holds the key of `entry`, as a checkpoint that
    /// merges the run into that tree finds it; every thread that finds out finds the same answer.
    void settle(int entry, boolean found) {
        settles()[entry] = found ? TreeSearch.FOUND : TreeSearch.ABSENT;
    }

    /// {@return whether the tree the run's writes are counted against holds the key of `entry`, as [TreeSearch#FOUND]
    /// or [TreeSearch#ABSENT], looked up in that tree unless a checkpoint or an earlier call settled it}
    ///
    /// A lookup reads the file for a tree larger than the page cache; its answer is kept.
    byte baseState(int entry) {
        var known = settles();
        var state = known[entry];
        if (state != TreeSearch.UNKNOWN) {
            return state;
        }
        synchronized (lock) {
            state = known[entry];
            if (state == TreeSearch.UNKNOWN) {
                state = base != null && TreeSearch.contains(pages, base, key(entry)) ? TreeSearch.FOUND : TreeSearch.ABSENT;
                known[entry] = state;
            }
            return state;
        }
    }

    /// {@return whether the value of `entry` is in a log segment a checkpoint retired}
    boolean inRetiredSegment(int entry) {
        var value = values[entry];
        if (value < 0 || (value & LOGGED) == 0) {
            return false;
        }
        return segments[(int) (logPlaces[(int) value] >>> FRAME_BITS)].retired();
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
        var total = 0L;
        synchronized (lock) {
            for (var position = first; position < last; position++) {
                var entry = entryAt(position);
                var state = known[entry];
                if (state == TreeSearch.UNKNOWN) {
                    state = base != null && TreeSearch.contains(pages, base, key(entry)) ? TreeSearch.FOUND : TreeSearch.ABSENT;
                    known[entry] = state;
                }
                if (values[entry] < 0) {
                    total -= state == TreeSearch.FOUND ? 1 : 0;
                } else {
                    total += state == TreeSearch.FOUND ? 0 : 1;
                }
            }
        }
        return total;
    }

    /// {@return where the value of `entry` lies, for tests: the [LogValue] of a value in the log, the chunk of one in a
    /// chunk of the replay, or `null` for an empty value or a removal}
    Object sourceForTests(int entry) {
        var value = values[entry];
        if (value < 0) {
            return null;
        }
        if ((value & LOGGED) != 0) {
            return logged((int) value);
        }
        return ((value >>> LENGTH_SHIFT) & LENGTH_MASK) == 0 ? null : chunks[chunkOf(value)];
    }

    /// {@return the length of each chunk of the run's keys, for tests}
    int[] keyChunkLengthsForTests() {
        var lengths = new int[keys.length];
        for (var i = 0; i < keys.length; i++) {
            lengths[i] = keys[i].length;
        }
        return lengths;
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

    // The place in the log of logged value `index`, made for the read
    private LogValue logged(int index) {
        var place = logPlaces[index];
        return new LogValue(segments[(int) (place >>> FRAME_BITS)], place & FRAME_MASK, logBodies[index], tagHighs[index], tagLows[index]);
    }

    // The logged value of an entry whose word is `value`, read from the log
    private byte[] readLogged(long value) {
        var index = (int) value;
        try {
            return logged(index).read(logStamps[index], (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
        } catch (LogSegmentView.Retired _) {
            throw new IllegalStateException("A value in a retired log segment is newer than the tree of its reader");
        }
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

    /// {@return the frozen run of `previous`, a frozen run or `null`, with the newest versions of the chains a freeze
    /// takes merged in, over `base`, the root of the key space's latest tree in `pages`, or `null` if a limit of a run
    /// would be passed: [#MAX_ENTRIES], [#MAX_KEY_BYTES] or [#MAX_SEGMENTS]}
    ///
    /// The entries come in the order of their keys, a chain's in place of the entry of `previous` of the same key. The
    /// arrays are sized to the entries exactly, and the entries of `previous` keep their words: the new run shares
    /// `previous`'s chunks and copies its log fields where they were, so only the values of the chains are copied, the
    /// short ones into chunks of [ReplayValues#CHUNK_BYTES], the ones in the log as their places and tags.
    ///
    /// @param previousEffects what [BlindWrites] counts for each entry of `previous` ([#COUNTED], [#BLIND_PUT],
    ///                        [#BLIND_REMOVE] or [#SHADOWED]), or `null` without one
    /// @param chains the chains, sorted by key, of distinct keys
    /// @param heads the newest version of each chain: a removal, or a value in the log or on an array of its own shorter
    ///              than [SpillQueue#LOG_VALUE_MIN], at most [#MAX_VALUE_LENGTH] bytes, with no unit of [BlobFile]
    /// @param count the chains of `chains` and `heads` to merge
    static Merge merge(PackedRun previous, byte[] previousEffects, Chain[] chains, Version[] heads, int count,
                       TreeRoot base, PageFile pages) {
        if (previous != null && !previous.frozen) {
            throw new IllegalArgumentException("Only a frozen run takes more chains");
        }
        var merge = new Merge(previous, chains, heads, count);
        return merge.fill(previousEffects) ? merge.build(base, pages) : null;
    }

    /// The arrays of a frozen run, which [#merge] fills from the run it replaces and the chains a freeze takes, and what
    /// the freeze needs to know of them.
    static final class Merge {
        private final PackedRun previous;
        private final Chain[] chains;
        private final Version[] heads;
        private final int count;
        // Whether the keys of the chains fit a run, read from the chains in place: they are copied once, into `keys`
        private final boolean chainKeysFit;
        private byte[][] keys;
        private int[] keyEnds;
        private long[] values;
        private byte[] inBase;
        private final ArrayList<MemorySegment> chunks;
        private long[] logStamps;
        private long[] logPlaces;
        private int[] logBodies;
        private long[] tagHighs;
        private int[] tagLows;
        private final ArrayList<LogSegmentView> segments;
        private int[] slots;
        private long maxStamp;
        private int size;
        private byte[] effects;
        private int[] entries;
        private int replaced;
        private PackedRun run;

        private Merge(PackedRun previous, Chain[] chains, Version[] heads, int count) {
            this.previous = previous;
            this.chains = chains;
            this.heads = heads;
            this.count = count;
            var bytes = 0L;
            for (var i = 0; i < count; i++) {
                bytes += chains[i].keyLength();
            }
            // Past the limit, no run takes them: fill() refuses before it reads a key
            this.chainKeysFit = bytes <= MAX_KEY_BYTES;
            this.chunks = previous == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(previous.chunks));
            this.segments = previous == null ? new ArrayList<>() : new ArrayList<>(Arrays.asList(previous.segments));
            this.maxStamp = previous == null ? -1 : previous.maxStamp;
        }

        /// {@return the run the merge made}
        PackedRun run() {
            return run;
        }

        /// {@return what [BlindWrites] counts for each entry of the run: that of the entry of the run it replaces for a
        /// key of both, [#SHADOWED] for the others of a chain, which the freeze changes for the chains it drops}
        byte[] effects() {
            return effects;
        }

        /// {@return the entry of the run that holds the version of chain `index`}
        int entry(int index) {
            return entries[index];
        }

        /// {@return how many of the chains replaced an entry of the run before whose chain the freeze that made it
        /// dropped, so a commit wrote the key again since}
        int replaced() {
            return replaced;
        }

        // Fills the arrays, two passes over the keys of the previous run and of the chains in their order: one to size
        // them, one to fill them; whether the limits of a run allowed it
        private boolean fill(byte[] previousEffects) {
            if (!chainKeysFit) {
                return false;
            }
            var previousSize = previous == null ? 0 : previous.size;
            var keyBytes = 0L;
            var logged = 0;
            // The chunks the short values of the chains take, and the bytes of the last, which is allocated as long
            var valueChunks = 0;
            var lastFill = 0;
            var total = 0;
            var p = 0;
            var c = 0;
            while (p < previousSize || c < count) {
                var order = compare(p, previousSize, c);
                if (order < 0) {
                    var length = previous.keyEnds[p] - previous.keyStart(p);
                    keyBytes = keyPlace(keyBytes, length) + length;
                    p++;
                } else {
                    var length = chains[c].keyLength();
                    keyBytes = keyPlace(keyBytes, length) + length;
                    if (order == 0) {
                        p++;
                    }
                    var head = heads[c];
                    if (head.logValue() != null) {
                        logged++;
                    } else if (!head.removes()) {
                        var own = head.ownArray();
                        var ownLength = own == null ? 0 : own.length;
                        if (ownLength > 0) {
                            if (valueChunks == 0 || lastFill > ReplayValues.CHUNK_BYTES - ownLength) {
                                valueChunks++;
                                lastFill = 0;
                            }
                            lastFill += ownLength;
                        }
                    }
                    c++;
                }
                total++;
            }
            if (total > MAX_ENTRIES || keyBytes > MAX_KEY_BYTES) {
                return false;
            }
            this.size = total;
            // Whole chunks but the last, which is as long as the keys it holds
            var keyChunks = (int) ((keyBytes + KEY_OFFSET_MASK) >>> KEY_CHUNK_BITS);
            this.keys = new byte[keyChunks][];
            for (var i = 0; i < keyChunks; i++) {
                keys[i] = new byte[i < keyChunks - 1 ? KEY_CHUNK : (int) (keyBytes - (long) i * KEY_CHUNK)];
            }
            this.keyEnds = new int[total];
            this.values = new long[total];
            this.inBase = new byte[total];
            this.effects = new byte[total];
            this.entries = new int[count];
            var previousLogged = previous == null ? 0 : previous.logStamps.length;
            this.logStamps = previous == null ? new long[logged] : Arrays.copyOf(previous.logStamps, previousLogged + logged);
            this.logPlaces = previous == null ? new long[logged] : Arrays.copyOf(previous.logPlaces, previousLogged + logged);
            this.logBodies = previous == null ? new int[logged] : Arrays.copyOf(previous.logBodies, previousLogged + logged);
            this.tagHighs = previous == null ? new long[logged] : Arrays.copyOf(previous.tagHighs, previousLogged + logged);
            this.tagLows = previous == null ? new int[logged] : Arrays.copyOf(previous.tagLows, previousLogged + logged);
            this.slots = new int[tableFor(total)];
            var mask = slots.length - 1;
            var previousKnown = previous == null ? null : previous.inBase;
            var nextLogged = previousLogged;
            var keyAt = 0;
            // The chunks the short values of the chains are copied into, one after another
            var copies = new ArrayList<byte[]>();
            byte[] chunk = null;
            var fill = 0;
            var firstChunk = chunks.size();
            // The index of the segment of the last value in the log, which the next one is most likely in
            var lastSegment = -1;
            p = 0;
            c = 0;
            for (var out = 0; out < total; out++) {
                var order = compare(p, previousSize, c);
                // The key comes from the previous run's entry at `start`, or from chain `chain`
                var start = -1;
                var chain = -1;
                int length;
                if (order < 0) {
                    start = previous.keyStart(p);
                    length = previous.keyEnds[p] - start;
                    values[out] = previous.values[p];
                    inBase[out] = previousKnown == null ? TreeSearch.UNKNOWN : previousKnown[p];
                    effects[out] = previousEffects == null ? COUNTED : previousEffects[p];
                    p++;
                } else {
                    chain = c;
                    length = chains[c].keyLength();
                    var head = heads[c];
                    maxStamp = Math.max(maxStamp, head.stamp);
                    if (order == 0) {
                        inBase[out] = previousKnown == null ? TreeSearch.UNKNOWN : previousKnown[p];
                        // A chain the previous freeze dropped had its blind write counted by its entry, which the
                        // chain written since, counted against that entry, leaves to this one; a chain it left beside
                        // its entry still counts its own
                        var before = previousEffects == null ? COUNTED : previousEffects[p];
                        effects[out] = before;
                        if (before != SHADOWED) {
                            // Written again since the previous freeze dropped its chain: one it did not drop comes
                            // back unwritten
                            replaced++;
                        }
                        p++;
                    } else {
                        // Not the chain's blind state, which a write taken by an earlier tree settled against that tree
                        inBase[out] = TreeSearch.UNKNOWN;
                        effects[out] = SHADOWED;
                    }
                    if (head.removes()) {
                        values[out] = REMOVES;
                    } else {
                        var logValue = head.logValue();
                        var valueLength = head.valueLength();
                        if (logValue != null) {
                            var segment = lastSegment >= 0 && segments.get(lastSegment) == logValue.segment()
                                    ? lastSegment : segmentIndex(logValue.segment());
                            if (segment < 0) {
                                return false;
                            }
                            lastSegment = segment;
                            var index = nextLogged++;
                            logStamps[index] = head.stamp;
                            logPlaces[index] = (long) segment << FRAME_BITS | logValue.frameOffset();
                            logBodies[index] = logValue.bodyIndex();
                            tagHighs[index] = logValue.tagHigh();
                            tagLows[index] = logValue.tagLow();
                            values[out] = LOGGED | (long) valueLength << 32 | (index & 0xFFFF_FFFFL);
                        } else {
                            var own = head.ownArray();
                            if (own == null) {
                                throw new IllegalStateException("A version frozen into a run left the array of its own");
                            }
                            if (own.length == 0) {
                                values[out] = 0;
                            } else {
                                if (chunk == null || fill > chunk.length - own.length) {
                                    var chunkLength = copies.size() + 1 == valueChunks ? lastFill : ReplayValues.CHUNK_BYTES;
                                    chunk = new byte[Math.max(chunkLength, own.length)];
                                    copies.add(chunk);
                                    fill = 0;
                                }
                                System.arraycopy(own, 0, chunk, fill, own.length);
                                var chunkIndex = firstChunk + copies.size() - 1;
                                values[out] = (long) own.length << LENGTH_SHIFT | (long) chunkIndex << OFFSET_BITS | fill;
                                fill += own.length;
                            }
                        }
                    }
                    entries[c] = out;
                    c++;
                }
                var place = keyPlace(keyAt, length);
                var keyChunk = keys[place >>> KEY_CHUNK_BITS];
                var offset = place & KEY_OFFSET_MASK;
                if (chain < 0) {
                    System.arraycopy(previous.keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyChunk, offset, length);
                } else {
                    chains[chain].copyKey(keyChunk, offset);
                }
                var hash = Segments.hash(keyChunk, offset, length);
                keyAt = place + length;
                keyEnds[out] = keyAt;
                var slot = hash & mask;
                while (slots[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                slots[slot] = (hash & TAG_MASK) | (out + 1);
            }
            for (var i = 0; i < copies.size(); i++) {
                var copy = copies.get(i);
                // The last chunk only as long as its values, as it was sized: a freeze of a few short values takes no whole
                // chunk
                if (i == copies.size() - 1 && fill < copy.length) {
                    copy = Arrays.copyOf(copy, fill);
                }
                chunks.add(MemorySegment.ofArray(copy).asReadOnly());
            }
            return true;
        }

        private Merge build(TreeRoot base, PageFile pages) {
            run = new PackedRun(this, base, pages);
            return this;
        }

        // The order of the previous run's entry `p` and the key of chain `c`, as a merge takes them: -1 to take the
        // entry, 1 to take the chain, 0 for both of one key
        private int compare(int p, int previousSize, int c) {
            if (p >= previousSize) {
                return 1;
            }
            if (c >= count) {
                return -1;
            }
            var start = previous.keyStart(p);
            return -Integer.signum(chains[c].compareKey(previous.keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK,
                    previous.keyEnds[p] - start));
        }

        // Where a key of `length` bytes goes after the keys that end at `end`: there, or at the start of the next
        // chunk if it does not fit the rest of the chunk
        private static int keyPlace(int end, int length) {
            return (end & KEY_OFFSET_MASK) + length > KEY_CHUNK ? (end + KEY_OFFSET_MASK) & ~KEY_OFFSET_MASK : end;
        }

        private static long keyPlace(long end, int length) {
            return (end & KEY_OFFSET_MASK) + length > KEY_CHUNK ? (end + KEY_OFFSET_MASK) & ~(long) KEY_OFFSET_MASK : end;
        }

        // The index of `segment` among the run's segments, added if new, or -1 past MAX_SEGMENTS
        private int segmentIndex(LogSegmentView segment) {
            for (var i = segments.size() - 1; i >= 0; i--) {
                if (segments.get(i) == segment) {
                    return i;
                }
            }
            if (segments.size() >= MAX_SEGMENTS) {
                return -1;
            }
            segments.add(segment);
            return segments.size() - 1;
        }

        // A power of two of at least two slots, at most three quarters full with `entries`
        private static int tableFor(int entries) {
            var least = Math.max(2, entries + entries / 3 + 1);
            return Integer.highestOneBit(least - 1) << 1;
        }
    }

    /// Collects the writes of one paged key space as the open replays them, the newest of each key, into a [PackedRun].
    ///
    /// Used by the replay alone, on the opening thread, which builds the run once and uses the builder no more: the run
    /// takes its arrays or copies of them, and the builder lets go of them.
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
        private long maxStamp;

        Builder() {
            this.keys = new byte[FIRST_ENTRIES * 32];
            this.keyChunks = new ArrayList<>();
            keyChunks.add(keys);
            this.keyEnds = new int[FIRST_ENTRIES];
            this.values = new long[FIRST_ENTRIES];
            this.chunks = new ArrayList<>();
            this.logStamps = new long[0];
            this.logPlaces = new long[0];
            this.logBodies = new int[0];
            this.tagHighs = new long[0];
            this.tagLows = new int[0];
            this.segments = new ArrayList<>();
            this.slots = new int[2 * FIRST_ENTRIES];
            this.maxStamp = -1;
        }

        /// {@return whether the builder holds no entry}
        boolean isEmpty() {
            return size == 0;
        }

        /// {@return the entry of `key`, or -1 if the builder holds none}
        int find(MemorySegment key) {
            var slot = slots[slotOf(key, Segments.hash(key))];
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
            var keyLength = key.byteSize();
            if (size >= MAX_ENTRIES || length > MAX_VALUE_LENGTH || keyLength < 1 || keyLength > KeyCodec.MAX_LENGTH
                    || keyBytes + keyLength + KeyCodec.MAX_LENGTH > MAX_KEY_BYTES) {
                return false;
            }
            return segment == null || segments.size() < MAX_SEGMENTS
                    || segments.getLast() == segment;
        }

        /// Records a put of `key`, stamped `stamp`, of the `length` bytes at `offset` of `chunk`, a chunk of the
        /// replay, or of an empty value if `length` is 0.
        void put(MemorySegment key, long stamp, MemorySegment chunk, int offset, int length) {
            var entry = entry(key, stamp);
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

        /// Records a put of `key`, stamped `stamp`, of the `length` bytes `logged` holds in the log.
        void putLogged(MemorySegment key, long stamp, LogValue logged, int length) {
            var entry = entry(key, stamp);
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
            if (segment < 0 || segments.get(segment) != logged.segment()) {
                segments.add(logged.segment());
                segment++;
            }
            logStamps[index] = stamp;
            logPlaces[index] = (long) segment << FRAME_BITS | logged.frameOffset();
            logBodies[index] = logged.bodyIndex();
            tagHighs[index] = logged.tagHigh();
            tagLows[index] = logged.tagLow();
            values[entry] = LOGGED | (long) length << 32 | (index & 0xFFFF_FFFFL);
        }

        /// Records a removal of `key`, stamped `stamp`.
        void delete(MemorySegment key, long stamp) {
            // Found or added first: adding may replace the array
            var entry = entry(key, stamp);
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

        // The entry of `key`, added if the builder holds none, whose write stamped `stamp` is the newest
        private int entry(MemorySegment key, long stamp) {
            maxStamp = Math.max(maxStamp, stamp);
            var hash = Segments.hash(key);
            var i = slotOf(key, hash);
            var found = slots[i];
            if (found != 0) {
                return (found & ENTRY_MASK) - 1;
            }
            var tag = hash & TAG_MASK;
            var entry = size;
            if (entry == values.length) {
                var capacity = 2 * entry;
                keyEnds = Arrays.copyOf(keyEnds, capacity);
                values = Arrays.copyOf(values, capacity);
            }
            var length = Math.toIntExact(key.byteSize());
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
            MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, keys, offset, length);
            keyBytes = start + length;
            keyEnds[entry] = keyBytes;
            size = entry + 1;
            slots[i] = tag | (entry + 1);
            if (4L * size > 3L * slots.length) {
                grow();
            }
            return entry;
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
            return new PackedRun(this, base, pages);
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
                var entry = run.entryAt(position);
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
}
