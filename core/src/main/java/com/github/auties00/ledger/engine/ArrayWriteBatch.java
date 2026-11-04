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

/// A [WriteBatch] that encodes into one growing array from its store's [HeapPool] and records each write as offsets
/// into it.
///
/// A range removal stores its bounds, each encoded or absent; the store reads them back through the accessors of
/// this class when it applies the batch, then takes the array over with [#detach()].
public final class ArrayWriteBatch implements WriteBatch {
    private static final int INITIAL_CAPACITY = 4_096;

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
    private long used;
    // The batch's own array while the encodings are in a compact copy for the store, or null
    private byte[] spare;

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
        this.data = pool.acquire(INITIAL_CAPACITY);
        this.memory = MemorySegment.ofArray(data);
        this.kinds = new byte[8];
        this.targets = new Keyspace<?, ?>[8];
        this.values = new Object[8];
        this.spans = new long[8 * 4];
    }

    @Override
    public <K, V> WriteBatch put(Keyspace<K, V> keyspace, K key, V value) {
        checkOpened(keyspace);
        Objects.requireNonNull(value, "value");
        var keyLength = checkKeyLength(keyspace.keys().sizeOf(Objects.requireNonNull(key, "key")));
        var valueLength = checkValueLength(keyspace.values().sizeOf(value));
        var keyOffset = reserve(keyLength);
        keyspace.keys().encode(key, memory.asSlice(keyOffset, keyLength));
        var valueOffset = reserve(valueLength);
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
        var keyOffset = reserve(keyLength);
        MemorySegment.copy(key, 0, memory, keyOffset, keyLength);
        var valueOffset = reserve(valueLength);
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
            var grown = pool.acquire((int) capacity);
            System.arraycopy(data, 0, grown, 0, (int) used);
            pool.release(data);
            data = grown;
            memory = MemorySegment.ofArray(data);
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

    /// {@return the value of write `index` as it was given, kept for resident key spaces; `null` otherwise}
    ///
    /// @param index the write
    public Object decodedValue(int index) {
        return values[Objects.checkIndex(index, size)];
    }

    private MemorySegment span(int index, int field) {
        Objects.checkIndex(index, size);
        var offset = spans[index * 4 + field];
        return offset == ABSENT ? null : memory.asSlice(offset, spans[index * 4 + field + 1]).asReadOnly();
    }

    /// {@return the store that created the batch}
    public Object owner() {
        return owner;
    }

    /// {@return the bytes the encodings occupy}
    public long encodedBytes() {
        return used;
    }

    /// Moves the encodings into an array of their size if they fill less than half of the batch's, before the store
    /// reads the writes through the accessors.
    ///
    /// The store keeps the array it applies the writes from for as long as the versions slice it: until checkpoints
    /// have written them and no read needs them. Without this, a commit of one small write would keep the whole
    /// [#INITIAL_CAPACITY] of its batch. The batch keeps its own array for its next writes.
    public void compact() {
        if (spare != null || used > data.length / 2) {
            return;
        }
        spare = data;
        data = Arrays.copyOf(data, (int) used);
        memory = MemorySegment.ofArray(data);
    }

    /// Hands the array holding the encodings over to the store and starts the batch again, empty, in the array it had
    /// before [#compact()], or in a fresh one from the pool.
    ///
    /// The store reads the writes through the accessors, and [#encodedBytes()], first: the segments the accessors
    /// returned stay valid, since the batch never writes the detached array again. The array never goes back to the
    /// pool: the versions slicing it keep it until checkpoints have written them and no read needs them, and the
    /// collector takes it then.
    ///
    /// @return the array holding the encodings
    public byte[] detach() {
        var detached = data;
        if (spare != null) {
            data = spare;
            spare = null;
        } else {
            data = pool.acquire(INITIAL_CAPACITY);
        }
        memory = MemorySegment.ofArray(data);
        clear();
        return detached;
    }
}
