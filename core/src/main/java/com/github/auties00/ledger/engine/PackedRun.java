package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
/// index of the key order; the keys lie in chunks of [#KEY_CHUNK] bytes and the words in chunks of [#WORD_CHUNK], a value
/// shorter than [SpillQueue#LOG_VALUE_MIN] in the replay's chunks ([ReplayValues]), and a longer one stays in the log,
/// where the run keeps its place, its tag and its stamp, which the tag binds, as a [LogValue] would. A message that took a
/// chain, a version, a [LogValue], an entry of the hash index, of the dirty and blind lists, and a node of the ordered
/// map once scanned, about 300 bytes over its key and value, takes about 50 here.
///
/// A bulk commit makes a run too ([#committed()]): the puts of new keys of a paged key space whose latest tree holds
/// none of its entries, at least [LiveLedger#BULK_RUN_WRITES] of them in one batch, which the commit counts as it
/// applies them, so the run makes no [#correction()] to the count. A later bulk commit into the same key space appends
/// its puts of new keys to the arrays of that run, until the cut of a checkpoint closes it to appends
/// ([#closeToAppends()]): each commit's puts are a range of entries after those of the commits before, and the commit
/// publishes a view of the run that holds every entry up to its own ([#size()]), whose [#previous()] view holds those
/// before. A reader reads the newest view whose commit it sees ([#at(long)]): a key of a later commit is never one it
/// finds, a slot of the hash table naming an entry past the view's last being skipped, so it reads as of its sequence.
/// The run is part of the tree of every reader at or after its first commit ([#minStamp()]) and of none before, until
/// a checkpoint at or after its last commit takes it, which is one cut after the run was closed: no checkpoint takes
/// part of a run. Its values lie where the batches put them, on the arrays the commits' versions would have sliced (its
/// homes), until the queue of values that may leave the heap moves them as it moves the values of versions ([#spill]):
/// a value of at least [SpillQueue#LOG_VALUE_MIN] bytes to the log, a shorter one to a chunk of the run's own. Once no
/// entry is on a home any more, the run lets go of it. The order of the keys of a view is made at the first scan, count
/// or checkpoint that needs it, not by the commit, from the longest order any view of the run made ([Storage]): the
/// entries after that order's are sorted and merged into it, or, for a view older than it, its entries are picked out
/// of it, so an ingest that scans after every append sorts each entry once whether or not older views are still read.
///
/// The arrays of a run are shared by its views and are only ever added to: a commit that appends writes past the
/// entries of every view, the hash table only gains slots (a table grown or rebuilt is a new array, published before
/// any entry it adds), and the words, homes and places in the log the spill writes lie in chunks that never move, so
/// the spill of an earlier commit's values runs beside a later commit's append. A view is immutable but for the place
/// of the values of a bulk commit's run, which the spill switches with one release store of the entry's word after it
/// published where the value went, so readers use it without synchronization: every read of a word is an acquire.
/// [#correction()], the tree lookups it caches and the order of the keys made late take a lock of the view's own.
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
    /// does not fit the rest of a chunk starts the next, and the keys of each commit that appends to a run start a
    /// chunk of their own.
    static final int KEY_CHUNK = 1 << 18;
    private static final int KEY_CHUNK_BITS = Integer.numberOfTrailingZeros(KEY_CHUNK);
    private static final int KEY_OFFSET_MASK = KEY_CHUNK - 1;
    /// The words of the entries a chunk holds: 64 KiB, which never moves once a bulk commit's run has it, as the spill
    /// switches the words of earlier commits while a later one appends.
    static final int WORD_CHUNK = 1 << 13;
    private static final int WORD_CHUNK_BITS = Integer.numberOfTrailingZeros(WORD_CHUNK);
    private static final int WORD_MASK = WORD_CHUNK - 1;
    // The homes of a chunk, and the counts of their entries: as the words, never moved, since the spill writes them
    private static final int HOME_CHUNK = 1 << 8;
    private static final int HOME_CHUNK_BITS = Integer.numberOfTrailingZeros(HOME_CHUNK);
    private static final int HOME_MASK = HOME_CHUNK - 1;
    // The places in the log of a chunk of logged values, four longs each: the stamp, the segment's index over where the
    // frame starts, the first part of the tag, and where the value lies in the frame's body over the second part
    private static final int LOG_CHUNK = 1 << 11;
    private static final int LOG_CHUNK_BITS = Integer.numberOfTrailingZeros(LOG_CHUNK);
    private static final int LOG_MASK = LOG_CHUNK - 1;
    private static final int LOG_LONGS = 4;
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
    private static final Places NO_PLACES = new Places(new MemorySegment[0], new long[0][], 0, new LogSegmentView[0]);

    static {
        if (ReplayValues.CHUNK_BYTES > 1 << OFFSET_BITS || SpillQueue.LOG_VALUE_MIN > LENGTH_MASK
                || ArrayWriteBatch.CHUNK_BYTES > 1 << OFFSET_BITS) {
            throw new ExceptionInInitializerError("A value in a chunk does not fit the word of its entry");
        }
        if (KeyCodec.MAX_LENGTH > KEY_CHUNK) {
            throw new ExceptionInInitializerError("A key does not fit a chunk of the keys");
        }
    }

    // The arrays of the run, which every view of it shares
    private final Storage storage;
    // The entries of the view: those of its commit and of every commit before
    private final int size;
    // The stamps of the oldest write of the run, of the oldest of the commit that made the view, and of the newest
    // write the view holds: a reader below the first reads no view, one below the second an older view, and one whose
    // tree is at or above the third reads the tree alone
    private final long minStamp;
    private final long fromStamp;
    private final long maxStamp;
    // Whether the commits that made the run counted their writes, as bulk commits do
    private final boolean committed;
    // The root of the key space's tree the store opened from, or null, and the file it is in: what the run's writes are
    // counted against
    private final TreeRoot base;
    private final PageFile pages;
    // The view of the commits before this one's, or null for the first, or once every reader reads this one or a later
    // one; cleared holding the store's publishing role
    private volatile PackedRun previous;
    // Guards the lookups of the keys in `base` that the count of the run makes, and the late sort of the view's keys
    private final Object lock;
    // The entries in the order of their keys; null until the first need for a view of a bulk commit's run
    private volatile int[] order;
    // Per entry, whether `base` holds its key, as TreeSearch.FOUND or ABSENT, or UNKNOWN; made at the first need
    private volatile byte[] inBase;

    private PackedRun(Storage storage, int size, long minStamp, long fromStamp, long maxStamp, boolean committed,
                      TreeRoot base, PageFile pages, PackedRun previous) {
        this.storage = storage;
        this.size = size;
        this.minStamp = minStamp;
        this.fromStamp = fromStamp;
        this.maxStamp = maxStamp;
        this.committed = committed;
        this.base = base;
        this.pages = pages;
        this.previous = previous;
        this.lock = new Object();
        this.order = committed ? null : sortedOrder();
    }

    /// {@return the stamp of the oldest write the run holds}
    ///
    /// A reader below it reads no write of the run: only a bulk commit's run has readers older than its writes, and
    /// they see none of its commits.
    long minStamp() {
        return minStamp;
    }

    /// {@return the stamp of the oldest write of the commit that made the view, [#minStamp()] for the first}
    ///
    /// A reader below it reads an older view ([#at(long)]).
    long fromStamp() {
        return fromStamp;
    }

    /// {@return whether the commits that made the run counted their writes, so the run makes no [#correction()] to the
    /// count of its key space: a bulk commit's run}
    boolean committed() {
        return committed;
    }

    /// {@return the stamp of the newest write the view holds}
    ///
    /// A reader takes the run as part of its tree while that tree's bound is below it: then the tree is the one the
    /// store opened from, or one older than the bulk commits that made the run, or there is none. No tree holds part
    /// of a run, so the newest view's tells for each of its views.
    long maxStamp() {
        return maxStamp;
    }

    /// {@return the entries of the view}
    int size() {
        return size;
    }

    /// {@return the view of the commits before the one that made this view, or `null` for the first, or once every
    /// reader reads this view or a later one}
    PackedRun previous() {
        return previous;
    }

    /// {@return the view of the run a reader at `bound` reads, this one or an older one: the newest whose commit is at
    /// or below `bound`, or `null` if the run's first commit is above it}
    ///
    /// Called on the newest view a reader found, which takes no step unless a commit appended since the reader's
    /// sequence; every view a reader can need is still linked ([#dropBelow(long)]).
    PackedRun at(long bound) {
        var view = this;
        while (view != null && view.fromStamp > bound) {
            view = view.previous;
        }
        return view;
    }

    /// {@return whether a later bulk commit may append to the run: a bulk commit's that no checkpoint's cut closed}
    boolean appendable() {
        return storage.appendable;
    }

    /// Closes the run to later bulk commits, which then keep their writes on chains until a checkpoint takes the run:
    /// called by the cut of a checkpoint, holding the store's publishing role, which a bulk commit holds from before
    /// it applies to after it publishes, so the checkpoint takes the newest view whole or none of the run.
    void closeToAppends() {
        storage.appendable = false;
    }

    /// Lets go of the views older than the newest of this view and the ones before it whose commit is at or below
    /// `oldestBound`, the bound of the oldest sequence any reader reads at, so they and the orders they made can go:
    /// every reader reads that view or a later one. Called holding the store's publishing role.
    void dropBelow(long oldestBound) {
        for (var view = this; view != null; view = view.previous) {
            if (view.fromStamp <= oldestBound) {
                if (view.previous != null) {
                    view.previous = null;
                }
                return;
            }
        }
    }

    /// {@return the entry of `key`, or -1 if the view holds none}
    int find(MemorySegment key) {
        return find(key, Segments.hash(key));
    }

    /// {@return the entry of `key`, whose hash is `hash` ([Segments#hash(MemorySegment)]), or -1 if the view holds none}
    int find(MemorySegment key, int hash) {
        var storage = this.storage;
        // Loaded after the view, so they hold every entry of it; a slot of an entry past the view is skipped, not an end
        var slots = storage.slots;
        var keyEnds = storage.keyEnds;
        var keys = storage.keys;
        var size = this.size;
        var mask = slots.length - 1;
        var tag = hash & TAG_MASK;
        for (var i = hash & mask; ; i = (i + 1) & mask) {
            var slot = slots[i];
            if (slot == 0) {
                return -1;
            }
            var entry = (slot & ENTRY_MASK) - 1;
            if ((slot & TAG_MASK) == tag && entry < size) {
                var start = keyStart(keyEnds, entry);
                var length = keyEnds[entry] - start;
                if (length == key.byteSize() && Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, length, key) == 0) {
                    return entry;
                }
            }
        }
    }

    /// {@return the entry of the key whose bytes are `key`, or -1 if the view holds none}
    int find(byte[] key) {
        var hash = Segments.hash(key);
        var storage = this.storage;
        var slots = storage.slots;
        var keyEnds = storage.keyEnds;
        var keys = storage.keys;
        var size = this.size;
        var mask = slots.length - 1;
        var tag = hash & TAG_MASK;
        for (var i = hash & mask; ; i = (i + 1) & mask) {
            var slot = slots[i];
            if (slot == 0) {
                return -1;
            }
            var entry = (slot & ENTRY_MASK) - 1;
            if ((slot & TAG_MASK) == tag && entry < size) {
                var start = keyStart(keyEnds, entry);
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
        return (long) WORDS.getAcquire(storage.words[entry >>> WORD_CHUNK_BITS], entry & WORD_MASK);
    }

    /// {@return the key of `entry`, a read-only slice of the run's keys}
    MemorySegment key(int entry) {
        var storage = this.storage;
        var keyEnds = storage.keyEnds;
        var start = keyStart(keyEnds, entry);
        return storage.keyViews[start >>> KEY_CHUNK_BITS].asSlice(start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `chain` and that of `entry`, as [Segments#compare(MemorySegment, MemorySegment)]
    /// gives, without a view of the entry's key}
    int compareChain(Chain chain, int entry) {
        var storage = this.storage;
        var keyEnds = storage.keyEnds;
        var start = keyStart(keyEnds, entry);
        return chain.compareKey(storage.keyViews[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start);
    }

    /// {@return the order of the key of `entry` and `key`, as [Segments#compare(MemorySegment, MemorySegment)] gives}
    int compareKey(int entry, MemorySegment key) {
        var storage = this.storage;
        var keyEnds = storage.keyEnds;
        var start = keyStart(keyEnds, entry);
        return Segments.compare(storage.keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start, key);
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
                var home = home(value);
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
            return storage.places.chunks()[chunkOf(value)].asSlice(value & ((1L << OFFSET_BITS) - 1), length);
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
                source = home(value);
                if (source == null) {
                    value = word(entry);
                    continue;
                }
                offset = homeOffset(value);
                length = homeLength(value);
            } else {
                length = (int) ((value >>> LENGTH_SHIFT) & LENGTH_MASK);
                source = length == 0 ? null : storage.places.chunks()[chunkOf(value)];
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
                var places = storage.places;
                return Version.ofLogged(logStamp(places, index), logged(places, index),
                        (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
            }
            if ((value & HOME) != 0) {
                var home = home(value);
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
            return new Version(0, storage.places.chunks()[chunkOf(value)], (int) (value & ((1L << OFFSET_BITS) - 1)), length, null, null);
        }
    }

    /// {@return the bytes of the value of `entry` if it is in the log, or -1 if it is elsewhere or the entry removes its
    /// key}
    ///
    /// The place of a value in the log never changes, so a value this finds in the log stays there.
    int loggedLength(int entry) {
        var value = word(entry);
        return value >= 0 && (value & LOGGED) != 0 ? (int) ((value >>> 32) & LOGGED_LENGTH_MASK) : -1;
    }

    /// Reads the value of `entry`, which is in the log ([#loggedLength(int)]), into `target` from `offset`, as
    /// [Version#readLogged(byte\[\], int)] reads a version's.
    ///
    /// @throws LogSegmentView.Retired if a checkpoint retired the value's segment
    /// @throws com.github.auties00.ledger.LedgerIoException if the log cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the log changed
    void readLogged(int entry, byte[] target, int offset) {
        var value = word(entry);
        var index = (int) value;
        var places = storage.places;
        logged(places, index).readInto(logStamp(places, index), (int) ((value >>> 32) & LOGGED_LENGTH_MASK), target, offset);
    }

    /// {@return whether the value of `entry` is in a log segment a checkpoint retired, as
    /// [Version#inRetiredSegment()] tells of a version's; `false` for a value elsewhere or a removal}
    boolean inRetiredSegment(int entry) {
        var value = word(entry);
        if (value < 0 || (value & LOGGED) == 0) {
            return false;
        }
        var index = (int) value;
        var places = storage.places;
        var place = places.logs()[index >>> LOG_CHUNK_BITS][(index & LOG_MASK) * LOG_LONGS + 1];
        return places.segments()[(int) (place >>> FRAME_BITS)].retired();
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

    /// {@return the entries in the order of their keys, made at the first call for a view of a bulk commit's run}
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

    /// {@return the change the entries of the view from `from`, included, to `to`, excluded, either `null` for no
    /// bound, make to the count of the tree the store opened from in that range}
    ///
    /// A bulk commit's run puts keys no tree it is read over holds, so each of its entries in the range adds one, which
    /// takes no lookup and records nothing.
    long correction(MemorySegment from, MemorySegment to) {
        var first = lowerPosition(from);
        var last = to == null ? size : lowerPosition(to);
        if (first >= last) {
            return 0;
        }
        return committed ? last - first : countBetween(first, last);
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
        var places = storage.places;
        if ((value & LOGGED) != 0) {
            return logged(places, (int) value);
        }
        if ((value & HOME) != 0) {
            var index = homeIndex(value);
            return storage.homes[index >>> HOME_CHUNK_BITS][index & HOME_MASK];
        }
        return ((value >>> LENGTH_SHIFT) & LENGTH_MASK) == 0 ? null : places.chunks()[chunkOf(value)];
    }

    /// {@return whether the value of `entry` is on a home of a bulk commit's run, for tests}
    boolean onHomeForTests(int entry) {
        return onHome(word(entry));
    }

    /// {@return the homes of a bulk commit's run the run still holds, for tests}
    int homesForTests() {
        var homes = storage.homes;
        var held = 0;
        for (var i = 0; homes != null && i < storage.homeCount; i++) {
            if (HOMES.getAcquire(homes[i >>> HOME_CHUNK_BITS], i & HOME_MASK) != null) {
                held++;
            }
        }
        return held;
    }

    /// {@return whether the order of the view's keys is made, for tests}
    boolean orderedForTests() {
        return order != null;
    }

    /// {@return the entries the orders of the run's views sorted in all, for tests: a view sorts only the entries after
    /// those of the longest order a view of the run made, and none if its own are all in that order}
    long sortedForTests() {
        return storage.sorted.get();
    }

    /// {@return whether this view and `other` are views of the same run, for tests}
    boolean sharesArraysForTests(PackedRun other) {
        return storage == other.storage;
    }

    /// {@return the length of each chunk of the run's keys, for tests}
    int[] keyChunkLengthsForTests() {
        var keys = storage.keys;
        var lengths = new int[keys.length];
        for (var i = 0; i < keys.length; i++) {
            lengths[i] = keys[i].length;
        }
        return lengths;
    }

    // Where the key of `entry` starts, given where each key ends: where the key before it ends, unless the key did not
    // fit the rest of that chunk, or starts the keys of a commit, and starts the chunk of its last byte; a key has at
    // least one byte
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
        var index = homeIndex(value);
        return (MemorySegment) HOMES.getAcquire(storage.homes[index >>> HOME_CHUNK_BITS], index & HOME_MASK);
    }

    // The stamp of logged value `index` of `places`, which its tag binds
    private static long logStamp(Places places, int index) {
        return places.logs()[index >>> LOG_CHUNK_BITS][(index & LOG_MASK) * LOG_LONGS];
    }

    // The place in the log of logged value `index` of `places`, made for the read
    private static LogValue logged(Places places, int index) {
        var chunk = places.logs()[index >>> LOG_CHUNK_BITS];
        var at = (index & LOG_MASK) * LOG_LONGS;
        var place = chunk[at + 1];
        var body = chunk[at + 3];
        return new LogValue(places.segments()[(int) (place >>> FRAME_BITS)], place & FRAME_MASK, (int) (body >>> 32),
                chunk[at + 2], (int) body);
    }

    // Writes the place in the log of the value at `at` of `chunk`, a chunk of the places of logged values
    private static void writeLog(long[] chunk, int at, long stamp, int segment, long frameOffset, int bodyIndex,
                                 long tagHigh, int tagLow) {
        chunk[at] = stamp;
        chunk[at + 1] = (long) segment << FRAME_BITS | frameOffset;
        chunk[at + 2] = tagHigh;
        chunk[at + 3] = (long) bodyIndex << 32 | (tagLow & 0xFFFF_FFFFL);
    }

    // The logged value of an entry whose word is `value`, read from the log
    private byte[] readLogged(long value) {
        var index = (int) value;
        var places = storage.places;
        try {
            return logged(places, index).read(logStamp(places, index), (int) ((value >>> 32) & LOGGED_LENGTH_MASK));
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
    /// holds, which no one writes and the store's pool never gets back. Called by the thread that drains the commit's
    /// record; the spills of the run's commits take turns, as they add to the same places, while a later commit
    /// appends beside them.
    ///
    /// @param crypto the caller's, until it gives it back
    /// @param segment the segment use that holds the commit's frame, or `null` if no value goes to the log
    /// @param commitStart where the commit's body starts in the frame's body
    /// @param frameBody the bytes of the frame's body, or 0 if they are not known, so no value is authenticated by the
    ///                  frame's tag ([LogValue#framed()])
    /// @param sequence the commit's sequence, which the tags bind with the position of each write in the batch
    /// @param entries the entries, each listed once
    /// @param offsets where each entry's value starts in the commit's body
    /// @param ops the position of each entry's write in the batch
    /// @return the values moved to the log and to a chunk, in this order
    int[] spill(SpillQueue.Crypto crypto, LogSegmentView segment, long frameOffset, int commitStart, int frameBody,
                long sequence, int[] entries, int[] offsets, int[] ops, int count) {
        var storage = this.storage;
        synchronized (storage.spillLock) {
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
            var before = storage.places;
            var firstChunk = before.chunks().length;
            if (firstChunk + chunkCount > 1 << CHUNK_BITS) {
                return new int[2];
            }
            var chunkArrays = new byte[chunkCount][];
            var chunks = chunkCount == 0 ? before.chunks() : Arrays.copyOf(before.chunks(), firstChunk + chunkCount);
            for (var c = 0; c < chunkCount; c++) {
                chunkArrays[c] = new byte[chunkSizes[c]];
                chunks[firstChunk + c] = MemorySegment.ofArray(chunkArrays[c]).asReadOnly();
            }
            // The places of the values moved to the log go after those of the values moved before, into the last chunk
            // of the places and chunks added whole, none of which moves
            var firstLogged = before.logged();
            var total = firstLogged + logged;
            var logs = before.logs();
            var logChunks = (total + LOG_MASK) >>> LOG_CHUNK_BITS;
            if (logChunks > logs.length) {
                var grown = Arrays.copyOf(logs, logChunks);
                for (var c = logs.length; c < logChunks; c++) {
                    grown[c] = new long[LOG_CHUNK * LOG_LONGS];
                }
                logs = grown;
            }
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
            var after = new Places(chunks, logs, total, segments);
            // Published before the first word that refers to what it adds; written below only at indexes no word
            // refers to yet, each before the release store of the word that does
            storage.places = after;
            var words = storage.words;
            var homes = storage.homes;
            var homeEntries = storage.homeEntries;
            var nextLogged = firstLogged;
            var chunk = firstChunk - 1;
            fill = ArrayWriteBatch.CHUNK_BYTES;
            var copied = 0;
            for (var i = 0; i < count; i++) {
                var entry = entries[i];
                var wordChunk = words[entry >>> WORD_CHUNK_BITS];
                var value = (long) WORDS.getAcquire(wordChunk, entry & WORD_MASK);
                if (!onHome(value)) {
                    continue;
                }
                var homeIndex = homeIndex(value);
                var homeChunk = homes[homeIndex >>> HOME_CHUNK_BITS];
                var home = homeChunk[homeIndex & HOME_MASK];
                var offset = homeOffset(value);
                var length = homeLength(value);
                long moved;
                if (length >= SpillQueue.LOG_VALUE_MIN) {
                    if (segment == null) {
                        continue;
                    }
                    var bodyIndex = commitStart + offsets[i];
                    var stamp = Reads.stamp(sequence, ops[i]);
                    var place = SpillQueue.framesAlone(segment, frameBody, length)
                            ? LogValue.framed(segment, frameOffset, bodyIndex, frameBody)
                            : crypto.logValue(segment, frameOffset, bodyIndex, stamp, home, offset, length);
                    var index = nextLogged++;
                    writeLog(logs[index >>> LOG_CHUNK_BITS], (index & LOG_MASK) * LOG_LONGS, stamp, segmentIndex,
                            place.frameOffset(), place.bodyIndex(), place.tagHigh(), place.tagLow());
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
                WORDS.setRelease(wordChunk, entry & WORD_MASK, moved);
                var counts = homeEntries[homeIndex >>> HOME_CHUNK_BITS];
                if (--counts[homeIndex & HOME_MASK] == 0) {
                    // No word names it any more: a reader that loaded one before finds it gone and loads the word again
                    HOMES.setRelease(homeChunk, homeIndex & HOME_MASK, (MemorySegment) null);
                }
            }
            return new int[]{nextLogged - firstLogged, copied};
        }
    }

    // The entries of the view sorted by key, from the longest order a view of the run made (Storage.lastOrder): the
    // entries of this view picked out of it in its order if it is longer, else its entries merged with those after them
    // sorted here; all of them sorted here if no view made one. Offered to the storage as the longest order after
    private int[] sortedOrder() {
        var storage = this.storage;
        var made = storage.lastOrder.get();
        int[] result;
        if (made != null && made.length >= size) {
            result = pickOrder(made, size);
        } else {
            result = mergeOrder(storage, made);
        }
        for (var longest = made; longest == null || longest.length < result.length; longest = storage.lastOrder.get()) {
            if (storage.lastOrder.compareAndSet(longest, result)) {
                break;
            }
        }
        return result;
    }

    // The entries below `size` of `order`, an order of the keys of the first order.length entries, in its order: the
    // entries of a view at or before the one that made `order`, sorted without comparing a key
    private static int[] pickOrder(int[] order, int size) {
        if (order.length == size) {
            return order;
        }
        var picked = new int[size];
        var out = 0;
        for (var entry : order) {
            if (entry < size) {
                picked[out++] = entry;
            }
        }
        return picked;
    }

    // The entries of the view sorted by key: those of `made`, an order of the keys of the first made.length entries or
    // null for none, merged with the ones after them sorted here
    private int[] mergeOrder(Storage storage, int[] made) {
        var from = made == null ? 0 : made.length;
        var keys = storage.keys;
        var keyEnds = storage.keyEnds;
        var sorted = KeySort.sort(keys, keyEnds, from, size);
        storage.sorted.addAndGet(size - from);
        if (made == null) {
            return sorted;
        }
        var merged = new int[size];
        var left = 0;
        var right = 0;
        for (var out = 0; out < merged.length; out++) {
            if (left < made.length && (right >= sorted.length || compareEntries(keys, keyEnds, made[left], sorted[right]) < 0)) {
                merged[out] = made[left++];
            } else {
                merged[out] = sorted[right++];
            }
        }
        return merged;
    }

    // The order of the keys of entries `left` and `right`
    private static int compareEntries(byte[][] keys, int[] keyEnds, int left, int right) {
        var leftStart = keyStart(keyEnds, left);
        var rightStart = keyStart(keyEnds, right);
        return Segments.compare(keys[leftStart >>> KEY_CHUNK_BITS], leftStart & KEY_OFFSET_MASK, keyEnds[left] - leftStart,
                keys[rightStart >>> KEY_CHUNK_BITS], rightStart & KEY_OFFSET_MASK, keyEnds[right] - rightStart);
    }

    /// Sorts entries of a run by their keys, which are all different, as [ChainSort] sorts chains: by the first 8 bytes
    /// of their keys with a radix sort, then each run of entries those bytes leave tied by the next 8 bytes, and so on;
    /// a short run, or one of keys that end within the bytes that tie them, goes to a merge sort that compares keys.
    /// Reading each key once per 8 bytes it needs, it sorts the entries of the newest view of a run of a whole ingest,
    /// which a checkpoint merges, without a comparison per entry and step.
    private static final class KeySort {
        // A run this short goes to the comparisons rather than to a radix pass of its own
        private static final int SMALL_RUN = 48;
        private static final int DIGIT_BITS = 8;
        private static final int BUCKETS = 1 << DIGIT_BITS;
        private final byte[][] keys;
        private final int[] keyEnds;
        // Per position of the entries being sorted, 8 bytes of its key; and the arrays a pass moves them through
        private final long[] words;
        private final long[] scratchWords;
        private final int[] scratchEntries;
        private final int[] counts;

        private KeySort(byte[][] keys, int[] keyEnds, int length) {
            this.keys = keys;
            this.keyEnds = keyEnds;
            this.words = new long[length];
            this.scratchWords = new long[length];
            this.scratchEntries = new int[length];
            this.counts = new int[BUCKETS];
        }

        /// {@return the entries from `from`, included, to `to`, excluded, of the run whose keys lie in `keys` and end
        /// at `keyEnds`, in the order of their keys}
        static int[] sort(byte[][] keys, int[] keyEnds, int from, int to) {
            var length = to - from;
            var entries = new int[length];
            for (var i = 0; i < length; i++) {
                entries[i] = from + i;
            }
            if (length < 2) {
                return entries;
            }
            var sort = new KeySort(keys, keyEnds, length);
            if (length <= SMALL_RUN) {
                sort.compareSort(entries, 0, length);
                return entries;
            }
            for (var i = 0; i < length; i++) {
                sort.words[i] = sort.word(entries[i], 0);
            }
            sort.sortRun(entries, 0, length, 0);
            return entries;
        }

        // The 8 bytes of the key of `entry` at `at`, big-endian, zero past its end
        private long word(int entry, int at) {
            var start = keyStart(keyEnds, entry);
            var length = keyEnds[entry] - start;
            if (at >= length) {
                return 0;
            }
            var chunk = keys[start >>> KEY_CHUNK_BITS];
            var offset = (start & KEY_OFFSET_MASK) + at;
            if (at + Long.BYTES <= length) {
                return Bytes.i64(chunk, offset);
            }
            var word = 0L;
            for (var i = 0; i < length - at; i++) {
                word |= (chunk[offset + i] & 0xFFL) << (56 - 8 * i);
            }
            return word;
        }

        // Sorts the entries from `from` to `to`, whose keys are equal before `at`, by their keys from `at`, which
        // `words` holds 8 bytes of for each
        private void sortRun(int[] entries, int from, int to, int at) {
            radix(entries, from, to);
            var start = from;
            while (start < to) {
                var end = start + 1;
                while (end < to && words[end] == words[start]) {
                    end++;
                }
                if (end - start > 1) {
                    tied(entries, start, end, at + Long.BYTES);
                }
                start = end;
            }
        }

        // Sorts the entries from `from` to `to`, whose keys are equal before `at`
        private void tied(int[] entries, int from, int to, int at) {
            if (to - from <= SMALL_RUN) {
                compareSort(entries, from, to);
                return;
            }
            var longer = false;
            for (var i = from; i < to; i++) {
                var entry = entries[i];
                longer |= keyEnds[entry] - keyStart(keyEnds, entry) > at;
                words[i] = word(entry, at);
            }
            if (!longer) {
                // Every key ends before `at`, tied up to its end: only their lengths tell them apart
                compareSort(entries, from, to);
                return;
            }
            sortRun(entries, from, to, at);
        }

        // Sorts the entries from `from` to `to` by `words`, as unsigned numbers: a least significant digit radix sort,
        // which skips a digit every word shares
        private void radix(int[] entries, int from, int to) {
            var length = to - from;
            var inWords = words;
            var inEntries = entries;
            var inFrom = from;
            var outWords = scratchWords;
            var outEntries = scratchEntries;
            var outFrom = from;
            for (var shift = 0; shift < Long.SIZE; shift += DIGIT_BITS) {
                Arrays.fill(counts, 0);
                for (var i = inFrom; i < inFrom + length; i++) {
                    counts[(int) (inWords[i] >>> shift) & (BUCKETS - 1)]++;
                }
                if (counts[(int) (inWords[inFrom] >>> shift) & (BUCKETS - 1)] == length) {
                    continue;
                }
                var position = outFrom;
                for (var digit = 0; digit < BUCKETS; digit++) {
                    var count = counts[digit];
                    counts[digit] = position;
                    position += count;
                }
                for (var i = inFrom; i < inFrom + length; i++) {
                    var digit = (int) (inWords[i] >>> shift) & (BUCKETS - 1);
                    var at = counts[digit]++;
                    outWords[at] = inWords[i];
                    outEntries[at] = inEntries[i];
                }
                var swapWords = inWords;
                var swapEntries = inEntries;
                inWords = outWords;
                inEntries = outEntries;
                outWords = swapWords;
                outEntries = swapEntries;
            }
            if (inWords != words) {
                System.arraycopy(inWords, from, words, from, length);
                System.arraycopy(inEntries, from, entries, from, length);
            }
        }

        // Sorts the entries from `from` to `to` by comparing their keys: a merge sort through the scratch entries
        private void compareSort(int[] entries, int from, int to) {
            if (to - from <= 16) {
                for (var i = from + 1; i < to; i++) {
                    var entry = entries[i];
                    var j = i - 1;
                    while (j >= from && compareEntries(keys, keyEnds, entries[j], entry) > 0) {
                        entries[j + 1] = entries[j];
                        j--;
                    }
                    entries[j + 1] = entry;
                }
                return;
            }
            var middle = (from + to) >>> 1;
            compareSort(entries, from, middle);
            compareSort(entries, middle, to);
            if (compareEntries(keys, keyEnds, entries[middle - 1], entries[middle]) <= 0) {
                return;
            }
            System.arraycopy(entries, from, scratchEntries, from, to - from);
            var left = from;
            var right = middle;
            for (var out = from; out < to; out++) {
                if (left < middle && (right >= to || compareEntries(keys, keyEnds, scratchEntries[left], scratchEntries[right]) <= 0)) {
                    entries[out] = scratchEntries[left++];
                } else {
                    entries[out] = scratchEntries[right++];
                }
            }
        }
    }

    /// Collects the writes of one paged key space into a [PackedRun]: those the open replays, the newest of each key,
    /// or the puts of new keys of a bulk commit, which a later bulk commit's builder appends to
    /// ([#appending(PackedRun, int, long)]).
    ///
    /// Used by the replay, on the opening thread, and by a bulk commit, on its committing thread, each of which builds
    /// one view and uses the builder no more. A builder of a new run writes arrays no reader sees until the run is
    /// built; one that appends writes past the entries of every view of the run, publishing each array it grows or
    /// table it adds to before any entry it adds there.
    static final class Builder {
        private static final int FIRST_ENTRIES = 64;
        private final Storage storage;
        // The view the builder appends to, or null for a new run
        private final PackedRun after;
        private final boolean committed;
        // Where the builder's entries, keys, chunks of keys and homes start in the run's arrays
        private final int firstEntry;
        private final int firstKeyBytes;
        private final int firstChunk;
        private final int firstHome;
        // The bytes of the builder's first chunk of keys, which doubles, up to a whole chunk, while it is the last
        private final int firstChunkBytes;
        // The replay's chunks of short values and the places in the log of its long ones; null for a bulk commit's
        private final ArrayList<MemorySegment> chunks;
        private long[][] logs;
        private int logged;
        private final ArrayList<LogSegmentView> segments;
        // The stamps of the builder's oldest and newest writes
        private long minStamp;
        private long maxStamp;
        // The home the last value went on, which the builder added
        private MemorySegment lastHome;

        /// Creates the builder of the run of an open's replay.
        Builder() {
            this(FIRST_ENTRIES, FIRST_ENTRIES * 32L, false);
        }

        // A builder of a new run with room for `entries` entries of `keyBytes` bytes of keys in all
        private Builder(int entries, long keyBytes, boolean committed) {
            var capacity = Math.max(1, entries);
            this.storage = new Storage();
            this.after = null;
            this.committed = committed;
            this.firstEntry = 0;
            this.firstKeyBytes = 0;
            this.firstChunk = 0;
            this.firstHome = 0;
            this.firstChunkBytes = (int) Math.clamp(keyBytes, 1, KEY_CHUNK);
            storage.keys = new byte[0][];
            storage.keyViews = new MemorySegment[0];
            storage.keyEnds = new int[capacity];
            storage.words = new long[0][];
            if (committed) {
                storage.homes = new MemorySegment[0][];
                storage.homeEntries = new int[0][];
                addWordChunks(capacity);
                this.chunks = null;
                this.segments = null;
            } else {
                this.chunks = new ArrayList<>();
                this.logs = new long[0][];
                this.segments = new ArrayList<>();
            }
            storage.slots = new int[tableFor(capacity)];
            this.minStamp = Long.MAX_VALUE;
            this.maxStamp = -1;
        }

        // A builder that appends to the run of `view`, its newest
        private Builder(PackedRun view, int planned, long keyBytes) {
            this.storage = view.storage;
            this.after = view;
            this.committed = true;
            this.firstEntry = storage.entries;
            this.firstKeyBytes = storage.keyBytes;
            this.firstChunk = storage.keys.length;
            this.firstHome = storage.homeCount;
            this.firstChunkBytes = (int) Math.clamp(keyBytes, 1, KEY_CHUNK);
            this.chunks = null;
            this.segments = null;
            this.minStamp = Long.MAX_VALUE;
            this.maxStamp = -1;
            if (view.size != firstEntry) {
                throw new IllegalStateException("A run's newest view holds " + view.size + " of its " + firstEntry + " entries");
            }
            // Room for the planned entries, made before any is added, so no array a reader may load moves after
            var needed = (int) Math.min(MAX_ENTRIES, (long) firstEntry + Math.max(1, planned));
            var keyEnds = storage.keyEnds;
            if (needed > keyEnds.length) {
                storage.keyEnds = Arrays.copyOf(keyEnds, (int) Math.min(MAX_ENTRIES, Math.max(needed, keyEnds.length + (keyEnds.length >> 1))));
            }
            addWordChunks(needed);
            if (4L * needed > 3L * storage.slots.length) {
                rehash(tableFor(needed));
            }
        }

        /// {@return the builder of the run a bulk commit makes of its puts of new keys, with room for `entries` entries
        /// of `keyBytes` bytes of keys in all, which it takes without growing an array: a bulk commit knows both before
        /// it applies}
        static Builder forCommit(int entries, long keyBytes) {
            return new Builder(entries, keyBytes, true);
        }

        /// {@return the builder of a bulk commit that appends its puts of new keys to the run whose newest view is
        /// `view`, a bulk commit's run still [appendable][PackedRun#appendable()], with room for `planned` more entries
        /// of `keyBytes` bytes of keys in all}
        ///
        /// The commit holds both roles, so no other commit appends, cuts the run or publishes a view of it until it
        /// publishes its own ([#buildCommitted(PageFile)]) or takes its writes back ([#rollback()]).
        static Builder appending(PackedRun view, int planned, long keyBytes) {
            return new Builder(view, planned, keyBytes);
        }

        // The least power of two `entries` fill at most three quarters of
        private static int tableFor(long entries) {
            var table = 2;
            while (4L * entries > 3L * table) {
                table *= 2;
            }
            return table;
        }

        // Adds whole chunks of words until they hold `entries` entries
        private void addWordChunks(int entries) {
            var words = storage.words;
            var wanted = (entries + WORD_MASK) >>> WORD_CHUNK_BITS;
            if (wanted > words.length) {
                var grown = Arrays.copyOf(words, wanted);
                for (var c = words.length; c < wanted; c++) {
                    grown[c] = new long[WORD_CHUNK];
                }
                storage.words = grown;
            }
        }

        /// {@return whether the builder holds no entry of its own}
        boolean isEmpty() {
            return storage.entries == firstEntry;
        }

        /// {@return the entries the builder added, those of the run before it left out}
        int size() {
            return storage.entries - firstEntry;
        }

        /// {@return the entry of `key`, or -1 if the run holds none: one the builder added, or one of a view of the run
        /// it appends to}
        int find(MemorySegment key) {
            return find(key, Segments.hash(key));
        }

        /// {@return the entry of `key`, whose hash is `hash` ([Segments#hash(MemorySegment)]), or -1 if the run holds
        /// none, as [#find(MemorySegment)] gives}
        int find(MemorySegment key, int hash) {
            var slots = storage.slots;
            var slot = slots[slotOf(slots, key, hash)];
            return slot == 0 ? -1 : (slot & ENTRY_MASK) - 1;
        }

        /// {@return whether `entry`, the newest write of its key so far, removes it}
        boolean removes(int entry) {
            return word(entry) < 0;
        }

        /// {@return whether the builder takes a write of `key` with a value of `length` bytes in `segment`, or of no
        /// segment if `null`: past [#MAX_ENTRIES] entries, [#MAX_SEGMENTS] segments or [#MAX_VALUE_LENGTH] bytes, when
        /// the key could end past [#MAX_KEY_BYTES] bytes of keys, and for a key of no bytes or more than
        /// [KeyCodec#MAX_LENGTH], which only a malformed log holds, the write goes to a chain}
        ///
        /// It stops [KeyCodec#MAX_LENGTH] bytes short of [#MAX_KEY_BYTES]: a key that does not fit the rest of a chunk
        /// starts the next one, leaving unused fewer bytes than it has.
        boolean takes(MemorySegment key, LogSegmentView segment, int length) {
            var keyLength = key.byteSize();
            if (storage.entries >= MAX_ENTRIES || length > MAX_VALUE_LENGTH || keyLength < 1 || keyLength > KeyCodec.MAX_LENGTH
                    || nextKeyStart() + keyLength + KeyCodec.MAX_LENGTH > MAX_KEY_BYTES) {
                return false;
            }
            return segment == null || segments.size() < MAX_SEGMENTS
                    || segments.getLast() == segment;
        }

        // Where the next key starts at the earliest: past the keys of the run, at the start of a chunk for the
        // builder's first
        private long nextKeyStart() {
            var start = (long) storage.keyBytes;
            return storage.keys.length == firstChunk ? (start + KEY_OFFSET_MASK) & ~KEY_OFFSET_MASK : start;
        }

        /// Records a put of `key`, stamped `stamp`, of the `length` bytes at `offset` of `chunk`, a chunk of the
        /// replay, or of an empty value if `length` is 0.
        void put(MemorySegment key, long stamp, MemorySegment chunk, int offset, int length) {
            var entry = entry(key, stamp);
            if (length == 0) {
                setWord(entry, 0);
                return;
            }
            var index = chunks.size() - 1;
            if (index < 0 || chunks.get(index) != chunk) {
                chunks.add(chunk);
                index++;
            }
            setWord(entry, (long) length << LENGTH_SHIFT | (long) index << OFFSET_BITS | offset);
        }

        /// Records a put of `key`, stamped `stamp`, of the `length` bytes `logged` holds in the log.
        void putLogged(MemorySegment key, long stamp, LogValue logged, int length) {
            var entry = entry(key, stamp);
            var previous = word(entry);
            int index;
            if (previous >= 0 && (previous & LOGGED) != 0) {
                // An overwrite of a value in the log takes its place in the arrays
                index = (int) previous;
            } else {
                index = this.logged++;
            }
            var segment = segments.isEmpty() ? -1 : segments.size() - 1;
            if (segment < 0 || segments.get(segment) != logged.segment()) {
                segments.add(logged.segment());
                segment++;
            }
            var chunk = index >>> LOG_CHUNK_BITS;
            var at = (index & LOG_MASK) * LOG_LONGS;
            if (chunk == logs.length) {
                logs = Arrays.copyOf(logs, chunk + 1);
                logs[chunk] = new long[(chunk == 0 ? FIRST_ENTRIES : LOG_CHUNK) * LOG_LONGS];
            } else if (at == logs[chunk].length) {
                // Only the first chunk is shorter than a chunk, and it is the last until it is whole
                logs[chunk] = Arrays.copyOf(logs[chunk], Math.min(LOG_CHUNK * LOG_LONGS, 2 * at));
            }
            writeLog(logs[chunk], at, stamp, segment, logged.frameOffset(), logged.bodyIndex(), logged.tagHigh(), logged.tagLow());
            setWord(entry, LOGGED | (long) length << 32 | (index & 0xFFFF_FFFFL));
        }

        /// {@return whether [#putHome] takes a put of `key` of the `length` bytes at `offset` of `home`: the place of the
        /// value fits the word of an entry, and the builder has room}
        boolean fits(MemorySegment key, MemorySegment home, int offset, int length) {
            if (length > MAX_HOME_LENGTH || offset > MAX_HOME_OFFSET || !takes(key, null, length)) {
                return false;
            }
            return length == 0 || home == lastHome || storage.homeCount < MAX_HOMES;
        }

        /// Records a put of `key`, which the run holds no entry of, stamped `stamp`, of the `length` bytes at `offset`
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
                setWord(entry, 0);
                return entry;
            }
            int index;
            if (home != lastHome) {
                // A home of the builder's own, whose entries the spill of no other commit counts down
                index = storage.homeCount++;
                var chunk = index >>> HOME_CHUNK_BITS;
                var homes = storage.homes;
                if (chunk == homes.length) {
                    var grownHomes = Arrays.copyOf(homes, chunk + 1);
                    grownHomes[chunk] = new MemorySegment[HOME_CHUNK];
                    var grownCounts = Arrays.copyOf(storage.homeEntries, chunk + 1);
                    grownCounts[chunk] = new int[HOME_CHUNK];
                    storage.homes = grownHomes;
                    storage.homeEntries = grownCounts;
                    homes = grownHomes;
                }
                homes[chunk][index & HOME_MASK] = home;
                lastHome = home;
            } else {
                index = storage.homeCount - 1;
            }
            var entry = entry(key, hash, stamp);
            storage.homeEntries[index >>> HOME_CHUNK_BITS][index & HOME_MASK]++;
            setWord(entry, HOME | (long) index << HOME_INDEX_SHIFT | (long) offset << HOME_OFFSET_SHIFT | length);
            return entry;
        }

        /// Records a removal of `key`, stamped `stamp`.
        void delete(MemorySegment key, long stamp) {
            // Found or added first: adding may add a chunk of words
            var entry = entry(key, stamp);
            setWord(entry, REMOVES);
        }

        private long word(int entry) {
            return storage.words[entry >>> WORD_CHUNK_BITS][entry & WORD_MASK];
        }

        // Written before the view that holds the entry is published, which no reader reads before
        private void setWord(int entry, long value) {
            storage.words[entry >>> WORD_CHUNK_BITS][entry & WORD_MASK] = value;
        }

        // The slot of `slots`, the run's hash table, that holds the entry of `key`, whose hash is `hash`, or the empty
        // one it would take
        private int slotOf(int[] slots, MemorySegment key, int hash) {
            var keyEnds = storage.keyEnds;
            var keys = storage.keys;
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
                            && Segments.compare(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, length, key) == 0) {
                        return i;
                    }
                }
            }
        }

        // The entry of `key`, added if the run holds none, whose write stamped `stamp` is the newest
        private int entry(MemorySegment key, long stamp) {
            return entry(key, Segments.hash(key), stamp);
        }

        // The entry of `key`, whose hash is `hash`, added if the run holds none, whose write stamped `stamp` is the
        // newest
        private int entry(MemorySegment key, int hash, long stamp) {
            minStamp = Math.min(minStamp, stamp);
            maxStamp = Math.max(maxStamp, stamp);
            var slots = storage.slots;
            var i = slotOf(slots, key, hash);
            var found = slots[i];
            if (found != 0) {
                return (found & ENTRY_MASK) - 1;
            }
            var entry = storage.entries;
            roomFor(entry);
            var length = Math.toIntExact(key.byteSize());
            var start = storage.keyBytes;
            if (storage.keys.length == firstChunk || (start & KEY_OFFSET_MASK) + length > KEY_CHUNK) {
                // The builder's first key, or the rest of the chunk is too short: the key starts the next one
                start = (start + KEY_OFFSET_MASK) & ~KEY_OFFSET_MASK;
            }
            var offset = start & KEY_OFFSET_MASK;
            var chunkIndex = start >>> KEY_CHUNK_BITS;
            var keys = storage.keys;
            byte[] chunk;
            if (chunkIndex == keys.length) {
                chunk = new byte[keys.length == firstChunk ? Math.max(firstChunkBytes, length) : KEY_CHUNK];
                keys = Arrays.copyOf(keys, chunkIndex + 1);
                keys[chunkIndex] = chunk;
                storage.keys = keys;
            } else {
                chunk = keys[chunkIndex];
                if (offset + length > chunk.length) {
                    // Only the builder's first chunk is shorter than a chunk, and it is the last until it is whole; no
                    // view reads it before the builder's is published
                    chunk = Arrays.copyOf(chunk, Math.min(KEY_CHUNK, Math.max(offset + length, 2 * chunk.length)));
                    keys[chunkIndex] = chunk;
                }
            }
            MemorySegment.copy(key, ValueLayout.JAVA_BYTE, 0, chunk, offset, length);
            storage.keyBytes = start + length;
            storage.keyEnds[entry] = start + length;
            storage.entries = entry + 1;
            slots[i] = (hash & TAG_MASK) | (entry + 1);
            if (4L * (entry + 1) > 3L * slots.length) {
                rehash(2 * slots.length);
            }
            return entry;
        }

        // Makes room for `entry` in the ends of the keys and the words
        private void roomFor(int entry) {
            var keyEnds = storage.keyEnds;
            if (entry == keyEnds.length) {
                var capacity = committed ? Math.max(entry + 1, entry + (entry >> 1)) : 2 * entry;
                storage.keyEnds = Arrays.copyOf(keyEnds, Math.min(MAX_ENTRIES, capacity));
            }
            var words = storage.words;
            var chunk = entry >>> WORD_CHUNK_BITS;
            if (chunk == words.length) {
                if (committed) {
                    addWordChunks(entry + 1);
                } else {
                    var grown = Arrays.copyOf(words, chunk + 1);
                    grown[chunk] = new long[chunk == 0 ? Math.min(WORD_CHUNK, storage.keyEnds.length) : WORD_CHUNK];
                    storage.words = grown;
                }
            } else if ((entry & WORD_MASK) == words[chunk].length) {
                // The replay's first chunk, shorter than a chunk while it is the last; a bulk commit's are whole
                words[chunk] = Arrays.copyOf(words[chunk], Math.min(WORD_CHUNK, 2 * words[chunk].length));
            }
        }

        // Places every entry of the run in a new hash table of `length` slots by the hash of its key, and publishes it:
        // a reader that loaded the table before finds every entry of its view there
        private void rehash(int length) {
            var keyEnds = storage.keyEnds;
            var keys = storage.keys;
            var grown = new int[length];
            var mask = length - 1;
            var entries = storage.entries;
            for (var entry = 0; entry < entries; entry++) {
                var start = keyStart(keyEnds, entry);
                var hash = Segments.hash(keys[start >>> KEY_CHUNK_BITS], start & KEY_OFFSET_MASK, keyEnds[entry] - start);
                var i = hash & mask;
                while (grown[i] != 0) {
                    i = (i + 1) & mask;
                }
                grown[i] = (hash & TAG_MASK) | (entry + 1);
            }
            storage.slots = grown;
        }

        /// Takes back every entry the builder added, for a bulk commit that will not publish them: the run is left as
        /// its newest view holds it, with a new hash table of that view's entries, published, so the indexes of the
        /// entries, keys and homes the builder took go to the next commit that appends, and no table a reader loads
        /// names one of them.
        ///
        /// A builder of a new run, which no reader saw, has nothing to take back.
        void rollback() {
            if (after == null) {
                return;
            }
            var storage = this.storage;
            var homes = storage.homes;
            var counts = storage.homeEntries;
            for (var index = firstHome; index < storage.homeCount; index++) {
                homes[index >>> HOME_CHUNK_BITS][index & HOME_MASK] = null;
                counts[index >>> HOME_CHUNK_BITS][index & HOME_MASK] = 0;
            }
            storage.homeCount = firstHome;
            if (storage.keys.length != firstChunk) {
                storage.keys = Arrays.copyOf(storage.keys, firstChunk);
            }
            storage.keyBytes = firstKeyBytes;
            if (storage.entries != firstEntry) {
                storage.entries = firstEntry;
                rehash(storage.slots.length);
            }
            lastHome = null;
        }

        // Trims the last chunk of the builder's keys to the bytes its keys take, and makes the read-only views of the
        // builder's chunks
        private void finishKeys() {
            var keys = storage.keys;
            if (keys.length == firstChunk) {
                return;
            }
            var last = keys.length - 1;
            var used = storage.keyBytes - last * KEY_CHUNK;
            if (used < keys[last].length) {
                keys[last] = Arrays.copyOf(keys[last], used);
            }
            var views = Arrays.copyOf(storage.keyViews, keys.length);
            for (var c = firstChunk; c < keys.length; c++) {
                views[c] = MemorySegment.ofArray(keys[c]).asReadOnly();
            }
            storage.keyViews = views;
        }

        /// {@return the run of the writes recorded, counted against `base`, the root of the key space's tree the store
        /// opened from in `pages`, or `null`}
        ///
        /// The arrays the builder grew are trimmed to the entries, so the run takes no room it does not use. The
        /// builder holds nothing after.
        PackedRun build(TreeRoot base, PageFile pages) {
            var storage = this.storage;
            var size = storage.entries;
            finishKeys();
            var keyEnds = storage.keyEnds;
            if (keyEnds.length != size) {
                storage.keyEnds = Arrays.copyOf(keyEnds, size);
            }
            var words = storage.words;
            if (words.length > 0) {
                var last = words.length - 1;
                var used = size - last * WORD_CHUNK;
                if (used < words[last].length) {
                    words[last] = Arrays.copyOf(words[last], used);
                }
            }
            var logs = this.logs;
            if (logs.length > 0) {
                var last = logs.length - 1;
                var used = (logged - last * LOG_CHUNK) * LOG_LONGS;
                if (used < logs[last].length) {
                    logs[last] = Arrays.copyOf(logs[last], used);
                }
            }
            storage.places = new Places(chunks.toArray(MemorySegment[]::new), logs, logged,
                    segments.toArray(LogSegmentView[]::new));
            this.logs = null;
            chunks.clear();
            return new PackedRun(storage, size, minStamp, minStamp, maxStamp, false, base, pages, null);
        }

        /// {@return the view of the run holding the puts of new keys a bulk commit recorded and counted, none of which
        /// the latest tree of `pages` held when the commit applied, nor any tree a reader that reads the view reads,
        /// with those of the views before if the builder appends}
        ///
        /// A builder of a new run makes it [appendable][PackedRun#appendable()]. The view is published by the commit,
        /// before its sequence is visible.
        PackedRun buildCommitted(PageFile pages) {
            var storage = this.storage;
            finishKeys();
            if (after == null) {
                storage.places = NO_PLACES;
                storage.appendable = true;
                return new PackedRun(storage, storage.entries, minStamp, minStamp, maxStamp, true, null, pages, null);
            }
            return new PackedRun(storage, storage.entries, after.minStamp, minStamp, Math.max(after.maxStamp, maxStamp),
                    true, null, pages, after);
        }
    }

    /// The arrays of a run, which its views share and readers read without synchronization.
    ///
    /// Every array a view reads is loaded through a volatile field after the view, so it holds every entry of the view
    /// whatever was added since. The cursors are written by the one commit that appends at a time, holding both of the
    /// store's roles, and read by the next one.
    private static final class Storage {
        // The chunks of the keys, and where each key ends, as its chunk times KEY_CHUNK plus its end in the chunk:
        // entry i's key starts where entry i - 1's ends, or at the start of the next chunk if it did not fit the rest
        // of that one or starts the keys of a commit
        volatile byte[][] keys;
        // Read-only views of the chunks of `keys`, which the slices of a key are made of, made as each view is built
        volatile MemorySegment[] keyViews;
        volatile int[] keyEnds;
        // The kind and place of each entry's value, in chunks of WORD_CHUNK, read with an acquire: the spill of a bulk
        // commit's run switches it
        volatile long[][] words;
        // The chunks and the places in the log the values lie in; replaced, by the spill of a bulk commit's run alone,
        // before any word refers to what it adds
        volatile Places places;
        // The read-only views of the arrays bulk commits put the values of the run on, in chunks of HOME_CHUNK, by the
        // index a word holds; an element is set to null, with a release store, once no word refers to it. Null for a
        // replay's run
        volatile MemorySegment[][] homes;
        // Per home, the entries whose value is still on it; written by the commit that adds the home and then by the
        // spill alone
        volatile int[][] homeEntries;
        // Open addressing on the hash of the keys, at most three quarters full; replaced by a larger table, or one
        // without the entries of a commit taken back, never changed but to add a slot
        volatile int[] slots;
        // Whether a later bulk commit may append to the run: set by the first, cleared by a checkpoint's cut
        volatile boolean appendable;
        // The entries, where the last key ends, and the homes, as the last commit to append left them
        int entries;
        int keyBytes;
        int homeCount;
        // Lets the spills of the run's commits take turns
        final Object spillLock = new Object();
        // The longest order of the keys a view of the run made, of the first lastOrder.length entries, which no commit
        // changes, as every view's order is made after its commit published it; replaced only by a longer one
        final AtomicReference<int[]> lastOrder = new AtomicReference<>();
        // The entries the orders of the views sorted, for tests
        final AtomicLong sorted = new AtomicLong();
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

        /// Creates the walk over the view's entries from `from`, included, to `to`, excluded, either `null` for no
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

    /// The inline value of a leaf a checkpoint writes that is the value of an entry of a run in the log, which
    /// [TreeNodes#writeEntry(byte\[\], int, TreeNodes.LeafEntry)] reads from the log straight into the leaf it writes,
    /// as it reads a logged version's: the checkpoint holds neither a version nor a place in the log for it until then.
    ///
    /// @param run the run
    /// @param entry the entry, whose value is in the log ([PackedRun#loggedLength(int)])
    record LoggedEntry(PackedRun run, int entry) {
        /// Reads the value into `target` from `offset`, as [PackedRun#readLogged(int, byte\[\], int)] does.
        void readInto(byte[] target, int offset) {
            run.readLogged(entry, target, offset);
        }
    }

    /// Where the values of a run that are not on a home lie.
    ///
    /// @param chunks the read-only views of the chunks the short values lie in
    /// @param logs the places in the log of the logged values, in chunks of 2,048 values of four longs: the stamp
    ///             of its write, which its tag binds; the index of its segment in `segments` over where its frame
    ///             starts; the first part of its tag ([LogValue]); and where it lies in its frame's body over the second
    ///             part of its tag. The chunks never move, so the spill adds places past `logged` while readers read
    ///             those before
    /// @param logged the logged values
    /// @param segments the segment uses the logged values are in
    private record Places(MemorySegment[] chunks, long[][] logs, int logged, LogSegmentView[] segments) {
    }
}
