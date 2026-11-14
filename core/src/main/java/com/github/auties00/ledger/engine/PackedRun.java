package com.github.auties00.ledger.engine;

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
/// ([#settle(int, boolean)]), and a count before then by looking each key up. A write the replay sent to a chain whose key
/// the run holds learns from the run's entry what it did ([BlindWrites#settle]); later writes of such a key read the run
/// as their tree.
///
/// Per entry it keeps the key's bytes and their end, a word of the value's place and kind, a slot of the hash table and an
/// index of the key order; a value shorter than [SpillQueue#LOG_VALUE_MIN] lies in the replay's chunks
/// ([ReplayValues]), and a longer one stays in the log, where the run keeps its place, its tag and its stamp, which the
/// tag binds, as a [LogValue] would. A message that took a chain, a version, a [LogValue], an entry of the hash index, of
/// the dirty and blind lists, and a node of the ordered map once scanned, about 300 bytes over its key and value, takes
/// about 50 here.
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
    private static final int TAG_MASK = ~ENTRY_MASK;
    private static final MemorySegment EMPTY = MemorySegment.ofArray(new byte[0]).asReadOnly();

    static {
        if (ReplayValues.CHUNK_BYTES > 1 << OFFSET_BITS || SpillQueue.LOG_VALUE_MIN > LENGTH_MASK) {
            throw new ExceptionInInitializerError("A value in a chunk does not fit the word of its entry");
        }
    }

    // The bytes of the keys, one after another, and where each ends: entry i's key starts where entry i - 1's ends
    private final byte[] keys;
    private final int[] keyEnds;
    // A read-only view of `keys`, which the slices of a key are made of
    private final MemorySegment keyView;
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
    // The entries in the order of their keys
    private final int[] order;
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

    private PackedRun(Builder builder, TreeRoot base, PageFile pages) {
        var size = builder.size;
        this.size = size;
        this.keys = Arrays.copyOf(builder.keys, builder.keyBytes);
        this.keyEnds = Arrays.copyOf(builder.keyEnds, size);
        this.keyView = MemorySegment.ofArray(keys).asReadOnly();
        this.values = Arrays.copyOf(builder.values, size);
        this.chunks = builder.chunks.toArray(MemorySegment[]::new);
        var logged = builder.logged;
        this.logStamps = Arrays.copyOf(builder.logStamps, logged);
        this.logPlaces = Arrays.copyOf(builder.logPlaces, logged);
        this.logBodies = Arrays.copyOf(builder.logBodies, logged);
        this.tagHighs = Arrays.copyOf(builder.tagHighs, logged);
        this.tagLows = Arrays.copyOf(builder.tagLows, logged);
        this.segments = builder.segments.toArray(LogSegmentView[]::new);
        // Kept as the builder filled it: the entries keep their indexes, and the table is at most three quarters full
        this.slots = builder.slots;
        this.maxStamp = builder.maxStamp;
        this.base = base;
        this.pages = pages;
        this.lock = new Object();
        this.order = sortedOrder();
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
                if (length == key.byteSize() && Segments.compare(keys, start, length, key) == 0) {
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
                if (length == key.length && Segments.compare(keys, start, length, key, 0, key.length) == 0) {
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
        return keyView.asSlice(start, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `entry` and `key`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(int entry, MemorySegment key) {
        var start = keyStart(entry);
        return Segments.compare(keys, start, keyEnds[entry] - start, key);
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

    /// {@return a chain of the key of `entry` that holds a slice of the run's keys, made for a checkpoint, which keys
    /// its states by chains ([TreeUpdate.Op])}
    Chain keyed(int entry) {
        return new Chain(key(entry), 0);
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
        return order[position];
    }

    /// {@return the array a checkpoint records in whether the tree the store opened from holds each entry's key, as it
    /// merges the entries into that tree ([#settle(int, boolean)])}
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
        var total = 0L;
        synchronized (lock) {
            for (var position = first; position < last; position++) {
                var entry = order[position];
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

    private int keyStart(int entry) {
        return entry == 0 ? 0 : keyEnds[entry - 1];
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
        return Segments.compare(keys, leftStart, keyEnds[left] - leftStart, keys, rightStart, keyEnds[right] - rightStart);
    }

    /// Collects the writes of one paged key space as the open replays them, the newest of each key, into a [PackedRun].
    ///
    /// Used by the replay alone, on the opening thread.
    static final class Builder {
        private static final int FIRST_ENTRIES = 64;
        private byte[] keys;
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

        /// {@return whether the builder takes a write of a value of `length` bytes in `segment`, or of no segment if
        /// `null`: past [#MAX_ENTRIES] entries, [#MAX_SEGMENTS] segments or [#MAX_VALUE_LENGTH] bytes, the replay writes
        /// to chains}
        boolean takes(LogSegmentView segment, int length) {
            if (size >= MAX_ENTRIES || length > MAX_VALUE_LENGTH) {
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

        // The entry of `key`, added if the builder holds none, whose write stamped `stamp` is the newest
        private int entry(MemorySegment key, long stamp) {
            maxStamp = Math.max(maxStamp, stamp);
            var hash = Segments.hash(key);
            var mask = slots.length - 1;
            var tag = hash & TAG_MASK;
            var i = hash & mask;
            for (; ; i = (i + 1) & mask) {
                var slot = slots[i];
                if (slot == 0) {
                    break;
                }
                if ((slot & TAG_MASK) == tag) {
                    var entry = (slot & ENTRY_MASK) - 1;
                    var start = entry == 0 ? 0 : keyEnds[entry - 1];
                    var length = keyEnds[entry] - start;
                    if (length == key.byteSize() && Segments.compare(keys, start, length, key) == 0) {
                        return entry;
                    }
                }
            }
            var entry = size;
            if (entry == values.length) {
                var capacity = 2 * entry;
                keyEnds = Arrays.copyOf(keyEnds, capacity);
                values = Arrays.copyOf(values, capacity);
            }
            var length = Math.toIntExact(key.byteSize());
            if (keyBytes + length > keys.length) {
                keys = Arrays.copyOf(keys, Math.max(keyBytes + length, 2 * keys.length));
            }
            MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, keys, keyBytes, length);
            keyBytes += length;
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
                var start = entry == 0 ? 0 : keyEnds[entry - 1];
                var hash = Segments.hash(keys, start, keyEnds[entry] - start);
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
                var entry = run.order[position];
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
