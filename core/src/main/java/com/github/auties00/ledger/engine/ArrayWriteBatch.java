package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.KeyRange;
import com.github.auties00.ledger.Keyspace;
import com.github.auties00.ledger.ValueCodec;
import com.github.auties00.ledger.WriteBatch;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/// A [WriteBatch] that encodes into one growing array and records each write as offsets into it.
///
/// A batch's first array has the size of its first write, which a batch of one write, the usual one, keeps as is; the
/// array grows by doubling, and from [#INITIAL_CAPACITY] it comes from the store's [HeapPool].
///
/// A range removal stores its bounds, each encoded or absent; the store reads them back through the accessors of
/// this class when it applies the batch, then starts it again with [#finish()].
public final class ArrayWriteBatch implements WriteBatch {
    private static final int INITIAL_CAPACITY = 4_096;
    private static final byte[] EMPTY = new byte[0];
    private static final MemorySegment EMPTY_MEMORY = MemorySegment.ofArray(EMPTY);
    /// The writes a new batch has room for: most batches hold one or two.
    private static final int INITIAL_WRITES = 2;
    /// The most bytes of the arrays [#prepare(boolean)] copies values into: below half a region of the smallest the
    /// G1 collector makes, 1 MiB, so none takes a region of its own, as an object of half a region or more does.
    static final int CHUNK_BYTES = 256 * 1024;
    /// The bytes besides the values the store keeps that a batch may hold and still keep them on its own array.
    private static final int SLACK = 64;
    // No write has a home: the store copies every value it keeps
    private static final byte HOMES_NONE = 0;
    // Every write's home is the batch's own view
    private static final byte HOMES_OWN = 1;
    // A paged write's home is in homes, a resident one has none
    private static final byte HOMES_CHUNKS = 2;

    /// A write of a value under a key.
    public static final byte PUT = 0;
    /// A removal of the value under a key.
    public static final byte DELETE = 1;
    /// A removal of every key in a range.
    public static final byte DELETE_RANGE = 2;

    private static final long ABSENT = -1;

    private final Object owner;
    private final HeapPool pool;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private byte[] data;
    private MemorySegment memory;
    // A read-only view of memory, which the accessors slice: one object per slice, not two
    private MemorySegment view;
    private long used;
    // The batch's own array while the encodings are in a compact copy for the store, or null
    private byte[] spare;
    // Where the store's versions find the values, as prepare decided: one of the HOMES_ constants
    private byte homesMode;
    // In HOMES_CHUNKS, the segment each write's value was copied into, or null, and where in it the value starts
    private MemorySegment[] homes;
    private int[] homeOffsets;

    private byte[] kinds;
    private Keyspace<?, ?>[] targets;
    private Object[] values;
    // Per write: first offset and length, then second offset and length (value, or upper bound of a range)
    private long[] spans;
    private int size;

    /// Creates an empty batch for a store.
    ///
    /// @param owner the store, which alone may apply the batch
    /// @param pool the store's pool, which the batch draws its memory from
    /// @param keyspaces the store's key spaces by name, which the batch may write
    public ArrayWriteBatch(Object owner, HeapPool pool, Map<String, Keyspace<?, ?>> keyspaces) {
        this.owner = owner;
        this.pool = pool;
        this.keyspaces = keyspaces;
        // Taken from the pool with the first write, and given back at the commit
        this.data = EMPTY;
        this.memory = EMPTY_MEMORY;
        this.view = EMPTY_MEMORY;
        this.kinds = new byte[INITIAL_WRITES];
        this.targets = new Keyspace<?, ?>[INITIAL_WRITES];
        this.values = new Object[INITIAL_WRITES];
        this.spans = new long[INITIAL_WRITES * 4];
    }

    @Override
    public <K, V> WriteBatch put(Keyspace<K, V> keyspace, K key, V value) {
        checkOpened(keyspace);
        Objects.requireNonNull(value, "value");
        var keyLength = checkKeyLength(keyspace.keys().sizeOf(Objects.requireNonNull(key, "key")));
        var valueLength = checkValueLength(keyspace.values().sizeOf(value));
        // One reservation for both, so a batch of one write grows its array once
        var keyOffset = reserve(keyLength + valueLength);
        var valueOffset = keyOffset + keyLength;
        keyspace.keys().encode(key, memory.asSlice(keyOffset, keyLength));
        keyspace.values().encode(value, memory.asSlice(valueOffset, valueLength));
        add(PUT, keyspace, keyspace.residency() == Keyspace.Residency.RESIDENT ? value : null,
                keyOffset, keyLength, valueOffset, valueLength);
        return this;
    }

    @Override
    public WriteBatch putEncoded(Keyspace<?, ?> keyspace, MemorySegment key, MemorySegment value) {
        checkOpened(keyspace);
        var keyLength = checkKeyLength(key.byteSize());
        var valueLength = checkValueLength(value.byteSize());
        var keyOffset = reserve(keyLength + valueLength);
        var valueOffset = keyOffset + keyLength;
        MemorySegment.copy(key, 0, memory, keyOffset, keyLength);
        MemorySegment.copy(value, 0, memory, valueOffset, valueLength);
        add(PUT, keyspace, null, keyOffset, keyLength, valueOffset, valueLength);
        return this;
    }

    @Override
    public <K> WriteBatch delete(Keyspace<K, ?> keyspace, K key) {
        checkOpened(keyspace);
        var keyLength = checkKeyLength(keyspace.keys().sizeOf(Objects.requireNonNull(key, "key")));
        var keyOffset = reserve(keyLength);
        keyspace.keys().encode(key, memory.asSlice(keyOffset, keyLength));
        add(DELETE, keyspace, null, keyOffset, keyLength, ABSENT, 0);
        return this;
    }

    @Override
    public <K> WriteBatch deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range) {
        checkOpened(keyspace);
        Objects.requireNonNull(range, "range");
        var from = range.from().map(key -> encodeKey(keyspace, key)).orElse(null);
        var to = range.to().map(key -> encodeKey(keyspace, key)).orElse(null);
        add(DELETE_RANGE, keyspace, null,
                from == null ? ABSENT : from[0], from == null ? 0 : from[1],
                to == null ? ABSENT : to[0], to == null ? 0 : to[1]);
        return this;
    }

    private <K> long[] encodeKey(Keyspace<K, ?> keyspace, K key) {
        var length = checkKeyLength(keyspace.keys().sizeOf(key));
        var offset = reserve(length);
        keyspace.keys().encode(key, memory.asSlice(offset, length));
        return new long[]{offset, length};
    }

    private void checkOpened(Keyspace<?, ?> keyspace) {
        Objects.requireNonNull(keyspace, "keyspace");
        if (!keyspace.equals(keyspaces.get(keyspace.name()))) {
            throw new IllegalArgumentException("The store was not opened with " + keyspace);
        }
    }

    private static int checkKeyLength(long length) {
        if (length < 1 || length > KeyCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Keys are 1 to " + KeyCodec.MAX_LENGTH + " bytes: " + length);
        }
        return (int) length;
    }

    private static long checkValueLength(long length) {
        if (length < 0 || length > ValueCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Values are at most " + ValueCodec.MAX_LENGTH + " bytes: " + length);
        }
        return length;
    }

    private long reserve(long length) {
        var needed = used + length;
        if (needed > data.length) {
            var capacity = Math.max(needed, (long) data.length * 2);
            if (capacity > Integer.MAX_VALUE - 8) {
                throw new IllegalArgumentException("A batch holds at most 2 GiB");
            }
            // Below the pool's smallest array, an array of the size the batch needs: the store keeps it without the
            // copy compact() would make. The pool takes back only its own sizes, so it ignores these arrays
            var grown = capacity < INITIAL_CAPACITY ? new byte[(int) capacity] : pool.acquire((int) capacity);
            System.arraycopy(data, 0, grown, 0, (int) used);
            pool.release(data);
            data = grown;
            memory = MemorySegment.ofArray(data);
            view = memory.asReadOnly();
        }
        var offset = used;
        used = needed;
        return offset;
    }

    private void add(byte kind, Keyspace<?, ?> keyspace, Object value, long firstOffset, long firstLength,
                     long secondOffset, long secondLength) {
        if (size == kinds.length) {
            var capacity = size * 2;
            kinds = Arrays.copyOf(kinds, capacity);
            targets = Arrays.copyOf(targets, capacity);
            values = Arrays.copyOf(values, capacity);
            spans = Arrays.copyOf(spans, capacity * 4);
        }
        kinds[size] = kind;
        targets[size] = keyspace;
        values[size] = value;
        spans[size * 4] = firstOffset;
        spans[size * 4 + 1] = firstLength;
        spans[size * 4 + 2] = secondOffset;
        spans[size * 4 + 3] = secondLength;
        size++;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public void clear() {
        Arrays.fill(targets, 0, size, null);
        Arrays.fill(values, 0, size, null);
        size = 0;
        used = 0;
    }

    /// {@return the kind of write `index`: [#PUT], [#DELETE] or [#DELETE_RANGE]}
    ///
    /// @param index the write
    public byte kind(int index) {
        return kinds[Objects.checkIndex(index, size)];
    }

    /// {@return the key space of write `index`}
    ///
    /// @param index the write
    public Keyspace<?, ?> keyspace(int index) {
        return targets[Objects.checkIndex(index, size)];
    }

    /// {@return the encoded key of write `index`, or the lower bound of a range; `null` if a range has none}
    ///
    /// @param index the write
    public MemorySegment key(int index) {
        return span(index, 0);
    }

    /// {@return the encoded value of write `index`, or the upper bound of a range; `null` for a removal of one key or a
    /// range without an upper bound}
    ///
    /// @param index the write
    public MemorySegment value(int index) {
        return span(index, 2);
    }

    /// {@return the bytes of the key, or lower bound, of write `index`, and of its value, or upper bound, 0 for those it
    /// has not}
    ///
    /// @param index the write
    long spanBytes(int index) {
        Objects.checkIndex(index, size);
        var bytes = 0L;
        if (spans[index * 4] != ABSENT) {
            bytes += spans[index * 4 + 1];
        }
        if (spans[index * 4 + 2] != ABSENT) {
            bytes += spans[index * 4 + 3];
        }
        return bytes;
    }

    /// {@return the bytes of the key of write `index`, or of the lower bound of a range, 0 if it has none}
    ///
    /// @param index the write
    long keyBytes(int index) {
        Objects.checkIndex(index, size);
        return spans[index * 4] == ABSENT ? 0 : spans[index * 4 + 1];
    }

    /// {@return the bytes of the value of write `index`, 0 if it has none}
    ///
    /// @param index the write
    long valueBytes(int index) {
        Objects.checkIndex(index, size);
        return spans[index * 4 + 2] == ABSENT ? 0 : spans[index * 4 + 3];
    }

    /// {@return the read-only segment of the array every key and value of the batch is part of}
    MemorySegment view() {
        return view;
    }

    /// {@return where in [#view()] the value of write `index` starts}
    ///
    /// @param index a write that puts a value
    int valueOffset(int index) {
        return Math.toIntExact(spans[Objects.checkIndex(index, size) * 4 + 2]);
    }

    /// {@return the value of write `index` as it was given, kept for resident key spaces; `null` otherwise}
    ///
    /// @param index the write
    public Object decodedValue(int index) {
        return values[Objects.checkIndex(index, size)];
    }

    private MemorySegment span(int index, int field) {
        Objects.checkIndex(index, size);
        var offset = spans[index * 4 + field];
        return offset == ABSENT ? null : view.asSlice(offset, spans[index * 4 + field + 1]);
    }

    /// {@return the store that created the batch}
    public Object owner() {
        return owner;
    }

    /// {@return the bytes the encodings occupy}
    public long encodedBytes() {
        return used;
    }

    /// Decides where the store's versions find the values of the writes, [#home(int)], before the store reads the
    /// writes through the accessors.
    ///
    /// The store keeps the value of a write to a [Keyspace.Residency#PAGED] key space until checkpoints have written
    /// it and no read needs it, and with it whatever array the value is part of. A batch that holds little but those
    /// values, at most [#SLACK] bytes or a sixteenth besides, keeps them on its own array, [compacted][#compact()];
    /// any other has them copied, alone, into arrays below [#CHUNK_BYTES], and its own array goes back to the pool
    /// once the commit [ends][#finish()]. On the batch's array, a message committed beside a session of 1.5 KiB kept
    /// the session's bytes, which the store held a copy of already, until the next checkpoint, and a batch of a
    /// thousand messages one array of over 2 MiB, which the collector gives regions of its own.
    ///
    /// @param copies whether the store copies every value it keeps, as a store in memory does, so no write has a home
    void prepare(boolean copies) {
        homes = null;
        homeOffsets = null;
        homesMode = HOMES_NONE;
        if (copies) {
            return;
        }
        var paged = 0L;
        var resident = 0L;
        for (var i = 0; i < size; i++) {
            if (kinds[i] == PUT) {
                if (targets[i].residency() == Keyspace.Residency.PAGED) {
                    paged += spans[i * 4 + 3];
                } else {
                    resident += spans[i * 4 + 3];
                }
            }
        }
        // A batch of resident writes alone keeps them on its array as before, where the store's rule for resident
        // values finds it small enough; any other copies them, so the paged values' array holds none
        var kept = paged > 0 ? paged : resident;
        if (kept == 0) {
            return;
        }
        if (used <= CHUNK_BYTES && used - kept <= Math.max(SLACK, used / 16)) {
            compact();
            homesMode = HOMES_OWN;
            return;
        }
        if (paged == 0) {
            return;
        }
        homes = new MemorySegment[size];
        homeOffsets = new int[size];
        homesMode = HOMES_CHUNKS;
        var chunk = (byte[]) null;
        var chunkView = (MemorySegment) null;
        var fill = 0;
        var remaining = 0L;
        for (var i = 0; i < size; i++) {
            if (packed(i) && spans[i * 4 + 3] < CHUNK_BYTES / 2) {
                remaining += spans[i * 4 + 3];
            }
        }
        for (var i = 0; i < size; i++) {
            if (!packed(i)) {
                continue;
            }
            var offset = (int) spans[i * 4 + 2];
            var length = (int) spans[i * 4 + 3];
            if (length >= CHUNK_BYTES / 2) {
                // A value of half a chunk or more gets an array of its own, which no other value shares
                var own = Arrays.copyOfRange(data, offset, offset + length);
                homes[i] = MemorySegment.ofArray(own).asReadOnly();
                continue;
            }
            if (chunk == null || fill + length > chunk.length) {
                // Every chunk but the last is more than half full, so they waste at most a value's bytes each
                chunk = new byte[(int) Math.min(CHUNK_BYTES, remaining)];
                chunkView = MemorySegment.ofArray(chunk).asReadOnly();
                fill = 0;
            }
            System.arraycopy(data, offset, chunk, fill, length);
            homes[i] = chunkView;
            homeOffsets[i] = fill;
            fill += length;
            remaining -= length;
        }
    }

    // Whether write `index` puts a value the store keeps on a chunk in HOMES_CHUNKS
    private boolean packed(int index) {
        return kinds[index] == PUT && targets[index].residency() == Keyspace.Residency.PAGED;
    }

    /// {@return the read-only segment the store's version of the value of write `index` slices, or `null` if the store
    /// copies the value}
    ///
    /// The segment holds no write the batch makes after [#finish()].
    ///
    /// @param index a write that puts a value
    MemorySegment home(int index) {
        Objects.checkIndex(index, size);
        return switch (homesMode) {
            case HOMES_OWN -> view;
            case HOMES_CHUNKS -> homes[index];
            default -> null;
        };
    }

    /// {@return where in [#home(int)] the value of write `index` starts}
    ///
    /// @param index a write that puts a value and has a home
    int homeOffset(int index) {
        Objects.checkIndex(index, size);
        return switch (homesMode) {
            case HOMES_OWN -> Math.toIntExact(spans[index * 4 + 2]);
            case HOMES_CHUNKS -> homeOffsets[index];
            default -> 0;
        };
    }

    /// Starts the batch again, empty, once the store has applied its writes: it hands its array over to the store if
    /// the versions slice it, [#detach()], and gives it back to the pool otherwise.
    void finish() {
        var own = homesMode == HOMES_OWN;
        homes = null;
        homeOffsets = null;
        homesMode = HOMES_NONE;
        if (own) {
            detach();
            return;
        }
        if (spare != null) {
            pool.release(spare);
            spare = null;
        }
        pool.release(data);
        data = EMPTY;
        memory = EMPTY_MEMORY;
        view = EMPTY_MEMORY;
        clear();
    }

    /// Moves the encodings into an array of their size if they leave a sixteenth of the batch's unused, before the
    /// store reads the writes through the accessors.
    ///
    /// The store keeps the array it applies the writes from for as long as the versions slice it: until checkpoints
    /// have written them and no read needs them. Without this, a commit of one small write would keep the whole
    /// [#INITIAL_CAPACITY] of its batch, and a batch that grew by doubling a quarter of its array on average, every
    /// byte of which the store held for each write. The batch keeps its own array for its next writes.
    public void compact() {
        if (spare != null || used > data.length - data.length / 16) {
            return;
        }
        spare = data;
        data = Arrays.copyOf(data, (int) used);
        memory = MemorySegment.ofArray(data);
        view = memory.asReadOnly();
    }

    /// Hands the array holding the encodings over to the store and starts the batch again, empty.
    ///
    /// The store reads the writes through the accessors, and [#encodedBytes()], first: the segments the accessors
    /// returned stay valid, since the batch never writes the detached array again. That array never goes back to the
    /// pool: the versions slicing it keep it until checkpoints have written them and no read needs them, and the
    /// collector takes it then. The array the batch had before [#compact()] goes back to the pool at once, so a store
    /// committing small batches takes their memory from the pool rather than from the collector; the batch takes an
    /// array again with its next write.
    ///
    /// @return the array holding the encodings
    public byte[] detach() {
        var detached = data;
        if (spare != null) {
            pool.release(spare);
            spare = null;
        }
        data = EMPTY;
        memory = EMPTY_MEMORY;
        view = EMPTY_MEMORY;
        clear();
        return detached;
    }
}
