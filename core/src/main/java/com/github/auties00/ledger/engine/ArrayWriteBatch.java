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

/// A [WriteBatch] that encodes into arrays and records each write as offsets into them.
///
/// A batch's first array has the size of its first write, which a batch of one write, the usual one, keeps as is; the
/// array grows by doubling, and from [#INITIAL_CAPACITY] it comes from the store's [HeapPool].
///
/// A batch that outgrows [#CHUNK_BYTES] moves to a rope of arrays of that size from the pool, which never grow, so no
/// batch copies its bytes more than that once: the values of paged key spaces go on chunks of their own, which the
/// store's versions keep as they are, and everything the store copies goes on others, which return to the pool once the
/// commit [ends][#finish()]. An array of over 1 MiB, which a doubling array of a batch of a thousand messages was, the
/// pool does not recycle and the collector gives regions of its own.
///
/// A value of half a [chunk][#CHUNK_BYTES] or more never enters that array: it takes an array of its own at the put,
/// which is private to the batch, is never written again, and is what the file of large values seals and the store's
/// version keeps, so the value is encoded once and copied by neither [#prepare(boolean)] nor the store.
///
/// A range removal stores its bounds, each encoded or absent; the store reads them back through the accessors of
/// this class when it applies the batch, then starts it again with [#finish()].
public final class ArrayWriteBatch implements WriteBatch {
    private static final int INITIAL_CAPACITY = 4_096;
    private static final byte[] EMPTY = new byte[0];
    private static final MemorySegment EMPTY_MEMORY = MemorySegment.ofArray(EMPTY);
    /// The writes a new batch has room for: most batches hold one or two.
    private static final int INITIAL_WRITES = 2;
    /// The most bytes of a batch's array, of the chunks of its rope, and of the array [#prepare(boolean)] copies values
    /// into: below half a region of the smallest the G1 collector makes, 1 MiB, so none takes a region of its own, as
    /// an object of half a region or more does.
    static final int CHUNK_BYTES = 256 * 1024;
    /// The least bytes of a value that takes an array of its own, which [#CHUNK_BYTES] could hold few of.
    private static final int OWN_BYTES = CHUNK_BYTES / 2;
    /// The bytes besides the values the store keeps that a batch may hold and still keep them on its own array.
    private static final int SLACK = 64;
    // No write has a home: the store copies every value it keeps
    private static final byte HOMES_NONE = 0;
    // Every write's home is the batch's own view
    private static final byte HOMES_OWN = 1;
    // A paged write's home is in homes, a resident one has none
    private static final byte HOMES_CHUNKS = 2;
    // A paged write's home is the home of its chunk of the rope, a resident one has none
    private static final byte HOMES_ROPE = 3;

    /// A write of a value under a key.
    public static final byte PUT = 0;
    /// A removal of the value under a key.
    public static final byte DELETE = 1;
    /// A removal of every key in a range.
    public static final byte DELETE_RANGE = 2;

    // The offset of a span the batch has no bytes for
    private static final long ABSENT = -1;
    // The offset of a value on an array of its own, in owns
    private static final long OWN = -2;
    // The bytes a log frame takes for a write besides its key and value: its kind, its key space and the lengths, as
    // variable-length numbers
    private static final int WRITE_OVERHEAD = 16;
    // The most bytes of keys, values and their overhead in a log frame, whose body is one array: the 16 MiB below the
    // largest array cover what checkRoom does not see, the batch's array and the frame's own header
    private static final long MAX_BYTES = Integer.MAX_VALUE - 8 - (16 << 20);
    // What reserve returns once the batch's encodings are on a rope: every span of a write then has its own place
    private static final long ON_ROPE = -1;

    private final Object owner;
    private final HeapPool pool;
    private final Map<String, Keyspace<?, ?>> keyspaces;
    private byte[] data;
    private MemorySegment memory;
    // A read-only view of memory, which the accessors slice: one object per slice, not two
    private MemorySegment view;
    private long used;
    // Whether a prepare made data the home of versions, which a commit that failed may have left attached: it is then
    // never written where they slice it, and never goes back to the pool
    private boolean exposed;
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
    // The array of each value of OWN_BYTES or more, or null before the first
    private byte[][] owns;
    // The bytes of the arrays in owns
    private long ownBytes;
    // The chunks of the rope by the number a span's offset holds in its high bits, or null while the encodings are on
    // data, whose spans hold 0 there: the number 0 is no chunk's
    private Chunk[] chunks;
    private int chunkCount;
    // The chunks the next key and the next paged value go on; null before the first, or for values once a prepare
    // exposed theirs
    private Chunk keyChunk;
    private Chunk valueChunk;
    // The bytes of the encodings on the rope
    private long ropeBytes;
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
        if (valueLength >= OWN_BYTES) {
            checkRoom(valueLength);
            var own = encodeOwn(keyspace.values(), value, (int) valueLength);
            var ownKeyOffset = encodeKey(keyspace, key, keyLength);
            addOwn(keyspace, keyspace.residency() == Keyspace.Residency.RESIDENT ? value : null, ownKeyOffset, keyLength, own);
            return this;
        }
        // One reservation for both, so a batch of one write grows its array once
        var keyOffset = reserve(keyLength + valueLength);
        if (keyOffset == ON_ROPE) {
            return putOnRope(keyspace, key, value, keyLength, (int) valueLength);
        }
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
        if (valueLength >= OWN_BYTES) {
            checkRoom(valueLength);
            var own = copyOwn(value);
            var ownKeyOffset = reserve(keyLength);
            if (ownKeyOffset == ON_ROPE) {
                checkRoom(keyLength);
                ownKeyOffset = takeKey(keyLength);
            }
            MemorySegment.copy(key, 0, slot(ownKeyOffset, keyLength), 0, keyLength);
            addOwn(keyspace, null, ownKeyOffset, keyLength, own);
            return this;
        }
        var keyOffset = reserve(keyLength + valueLength);
        if (keyOffset == ON_ROPE) {
            checkRoom(keyLength + valueLength);
            var keyPosition = takeKey(keyLength);
            var valuePosition = takeFor(keyspace, (int) valueLength);
            MemorySegment.copy(key, 0, slot(keyPosition, keyLength), 0, keyLength);
            MemorySegment.copy(value, 0, slot(valuePosition, (int) valueLength), 0, valueLength);
            add(PUT, keyspace, null, keyPosition, keyLength, valuePosition, valueLength);
            return this;
        }
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
        add(DELETE, keyspace, null, encodeKey(keyspace, key, keyLength), keyLength, ABSENT, 0);
        return this;
    }

    @Override
    public <K> WriteBatch deleteRange(Keyspace<K, ?> keyspace, KeyRange<K> range) {
        checkOpened(keyspace);
        Objects.requireNonNull(range, "range");
        var from = range.from().orElse(null);
        var to = range.to().orElse(null);
        var fromLength = from == null ? 0 : checkKeyLength(keyspace.keys().sizeOf(from));
        var toLength = to == null ? 0 : checkKeyLength(keyspace.keys().sizeOf(to));
        var fromOffset = ABSENT;
        var toOffset = ABSENT;
        if (fromLength + toLength > 0) {
            // One reservation for both: a bound reserved alone would not be among the spans a move to the rope rewrites
            var offset = reserve(fromLength + toLength);
            if (offset == ON_ROPE) {
                checkRoom(fromLength + toLength);
                fromOffset = from == null ? ABSENT : takeKey(fromLength);
                toOffset = to == null ? ABSENT : takeKey(toLength);
            } else {
                fromOffset = from == null ? ABSENT : offset;
                toOffset = to == null ? ABSENT : offset + fromLength;
            }
            if (from != null) {
                keyspace.keys().encode(from, slot(fromOffset, fromLength));
            }
            if (to != null) {
                keyspace.keys().encode(to, slot(toOffset, toLength));
            }
        }
        add(DELETE_RANGE, keyspace, null, fromOffset, fromLength, toOffset, toLength);
        return this;
    }

    /// {@return where the batch holds `key`, which it encoded there}
    private <K> long encodeKey(Keyspace<K, ?> keyspace, K key, int length) {
        var offset = reserve(length);
        if (offset == ON_ROPE) {
            checkRoom(length);
            offset = takeKey(length);
            keyspace.keys().encode(key, slot(offset, length));
            return offset;
        }
        keyspace.keys().encode(key, memory.asSlice(offset, length));
        return offset;
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

    /// {@return a new array holding the encoding of `value`, of `length` bytes}
    ///
    /// A byte array is cloned, which the compiler does without zeroing the new array first.
    private static <V> byte[] encodeOwn(ValueCodec<V> codec, V value, int length) {
        if (codec == BuiltinCodecs.BYTES_VALUE) {
            return Arrays.copyOf((byte[]) value, length);
        }
        var own = new byte[length];
        codec.encode(value, MemorySegment.ofArray(own));
        return own;
    }

    /// {@return a new array holding the bytes of `value`, cloned out of the array behind it if it shows one}
    ///
    /// A read-only segment hides its array, and a segment of another primitive type or of native memory has none.
    private static byte[] copyOwn(MemorySegment value) {
        if (value.heapBase().orElse(null) instanceof byte[] array) {
            var start = (int) value.address();
            return Arrays.copyOfRange(array, start, start + (int) value.byteSize());
        }
        return Segments.toBytes(value);
    }

    /// Records a put of a value on an array of its own, private to the batch.
    private void addOwn(Keyspace<?, ?> keyspace, Object value, long keyOffset, int keyLength, byte[] own) {
        add(PUT, keyspace, value, keyOffset, keyLength, OWN, own.length);
        if (owns == null) {
            owns = new byte[kinds.length][];
        }
        owns[size - 1] = own;
        ownBytes += own.length;
    }

    /// Checks that the batch has room for a write of `length` bytes more, which the store's log frame must hold too,
    /// its body being one array.
    private void checkRoom(long length) {
        checkRoom(encodedBytes() + (size + 1L) * WRITE_OVERHEAD, length);
    }

    /// Checks that a batch of `held` bytes has room for `length` bytes more.
    ///
    /// @throws IllegalArgumentException if it has not
    static void checkRoom(long held, long length) {
        if (held + length > MAX_BYTES) {
            throw new IllegalArgumentException("A batch holds at most 2 GiB");
        }
    }

    /// {@return where in the batch's array `length` bytes are reserved, or [#ON_ROPE] if the batch's encodings are on a
    /// rope, with no reservation made}
    ///
    /// @param length more than 0: on a rope, the array is empty, so every reservation comes here to [#grow(long)]
    private long reserve(long length) {
        var needed = used + length;
        if (needed > data.length) {
            return grow(length);
        }
        var offset = used;
        used = needed;
        return offset;
    }

    /// Makes room for a reservation of `length` bytes the batch's array has none for: in a larger array, up to
    /// [#CHUNK_BYTES], and on a rope past it.
    private long grow(long length) {
        if (chunks != null) {
            return ON_ROPE;
        }
        var needed = used + length;
        if (needed > CHUNK_BYTES) {
            moveToRope();
            return ON_ROPE;
        }
        var capacity = Math.min(CHUNK_BYTES, Math.max(needed, (long) data.length * 2));
        // Below the pool's smallest array, an array of the size the batch needs: the store keeps it without the
        // copy compact() would make. The pool takes back only its own sizes, so it ignores these arrays
        var grown = capacity < INITIAL_CAPACITY ? new byte[(int) capacity] : pool.acquire((int) capacity);
        System.arraycopy(data, 0, grown, 0, (int) used);
        if (!exposed) {
            pool.release(data);
        }
        exposed = false;
        data = grown;
        memory = MemorySegment.ofArray(data);
        view = memory.asReadOnly();
        var offset = used;
        used = needed;
        return offset;
    }

    /// Moves the encodings of the batch to a rope, rewriting the spans of its writes; only a put of a value below
    /// [#OWN_BYTES] or a key comes here, so none has a span that outgrows a chunk.
    private void moveToRope() {
        chunks = new Chunk[8];
        chunkCount = 1;
        for (var i = 0; i < size; i++) {
            if (spans[i * 4] != ABSENT) {
                spans[i * 4] = moveKey(spans[i * 4], (int) spans[i * 4 + 1]);
            }
            var second = spans[i * 4 + 2];
            if (second >= 0) {
                var length = (int) spans[i * 4 + 3];
                var position = packed(i) ? takeValue(length) : takeKey(length);
                moveBytes(second, position, length);
                spans[i * 4 + 2] = position;
            }
        }
        dropData();
        used = 0;
    }

    private long moveKey(long offset, int length) {
        var position = takeKey(length);
        moveBytes(offset, position, length);
        return position;
    }

    private void moveBytes(long offset, long position, int length) {
        System.arraycopy(data, (int) offset, chunks[(int) (position >>> 32)].array, (int) position, length);
    }

    private <K, V> WriteBatch putOnRope(Keyspace<K, V> keyspace, K key, V value, int keyLength, int valueLength) {
        checkRoom(keyLength + valueLength);
        var keyPosition = takeKey(keyLength);
        var valuePosition = takeFor(keyspace, valueLength);
        keyspace.keys().encode(key, slot(keyPosition, keyLength));
        keyspace.values().encode(value, slot(valuePosition, valueLength));
        add(PUT, keyspace, keyspace.residency() == Keyspace.Residency.RESIDENT ? value : null,
                keyPosition, keyLength, valuePosition, valueLength);
        return this;
    }

    /// {@return the rope's place for a value of `length` bytes of `keyspace`}
    ///
    /// A paged one goes on the chunks versions keep, a resident one, which the store copies, with the keys.
    private long takeFor(Keyspace<?, ?> keyspace, int length) {
        return keyspace.residency() == Keyspace.Residency.PAGED ? takeValue(length) : takeKey(length);
    }

    private long takeKey(int length) {
        if (keyChunk == null || keyChunk.fill + length > CHUNK_BYTES) {
            keyChunk = newChunk(false);
        }
        return take(keyChunk, length);
    }

    private long takeValue(int length) {
        if (valueChunk == null || valueChunk.fill + length > CHUNK_BYTES) {
            valueChunk = newChunk(true);
        }
        return take(valueChunk, length);
    }

    private long take(Chunk chunk, int length) {
        var position = (long) chunk.number << 32 | chunk.fill;
        chunk.fill += length;
        ropeBytes += length;
        return position;
    }

    private Chunk newChunk(boolean values) {
        if (chunkCount == chunks.length) {
            chunks = Arrays.copyOf(chunks, chunkCount * 2);
        }
        var chunk = new Chunk(chunkCount, pool.acquire(CHUNK_BYTES), values);
        chunks[chunkCount++] = chunk;
        return chunk;
    }

    /// {@return the writable segment of the `length` bytes at `position`, on the batch's array or its rope}
    private MemorySegment slot(long position, int length) {
        var chunk = (int) (position >>> 32);
        return chunk == 0 ? memory.asSlice(position, length) : chunks[chunk].memory.asSlice((int) position, length);
    }

    /// Lets go of the rope, if the batch has one: every chunk returns to the pool but those that are their values' home,
    /// which the versions may slice and no one writes again.
    private void leaveRope() {
        if (chunks == null) {
            return;
        }
        for (var i = 1; i < chunkCount; i++) {
            var chunk = chunks[i];
            if (chunk.home != chunk.view) {
                pool.release(chunk.array);
            }
        }
        chunks = null;
        chunkCount = 0;
        keyChunk = null;
        valueChunk = null;
        ropeBytes = 0;
    }

    /// One array of a batch's rope.
    private static final class Chunk {
        // The number the offsets of the spans on it hold in their high bits
        final int number;
        final byte[] array;
        final MemorySegment memory;
        // A read-only view of memory, which the accessors slice
        final MemorySegment view;
        // Whether it holds values of paged key spaces, which the store's versions keep; otherwise bytes the store copies
        final boolean values;
        // The bytes taken
        int fill;
        // What the versions of its values slice once a prepare exposed it: its view, or a copy of its bytes in an array
        // of their size; null for a chunk no prepare exposed
        MemorySegment home;

        Chunk(int number, byte[] array, boolean values) {
            this.number = number;
            this.array = array;
            this.memory = MemorySegment.ofArray(array);
            this.view = memory.asReadOnly();
            this.values = values;
        }
    }

    private void add(byte kind, Keyspace<?, ?> keyspace, Object value, long firstOffset, long firstLength,
                     long secondOffset, long secondLength) {
        if (size == kinds.length) {
            var capacity = size * 2;
            kinds = Arrays.copyOf(kinds, capacity);
            targets = Arrays.copyOf(targets, capacity);
            values = Arrays.copyOf(values, capacity);
            spans = Arrays.copyOf(spans, capacity * 4);
            if (owns != null) {
                owns = Arrays.copyOf(owns, capacity);
            }
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
        owns = null;
        ownBytes = 0;
        leaveRope();
        // What a commit that failed prepared
        homes = null;
        homeOffsets = null;
        homesMode = HOMES_NONE;
        if (exposed) {
            dropData();
        }
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

    /// {@return the bytes of the value of write `index`, 0 if it has none}
    ///
    /// @param index the write
    long valueBytes(int index) {
        Objects.checkIndex(index, size);
        return spans[index * 4 + 2] == ABSENT ? 0 : spans[index * 4 + 3];
    }

    /// {@return the array of its own that holds the whole value of write `index`, which no one writes, or `null` if the
    /// value has none}
    ///
    /// The store's version keeps it in place of a copy. Reads of a resident byte array return the instance the version
    /// holds, so this is never the array the caller passed to the put.
    ///
    /// @param index a write that puts a value
    byte[] ownValue(int index) {
        Objects.checkIndex(index, size);
        return owns == null ? null : owns[index];
    }

    /// {@return the array that holds the value of write `index`, at [#valueArrayOffset(int)]}
    ///
    /// @param index a write that puts a value
    byte[] valueArray(int index) {
        var offset = spans[Objects.checkIndex(index, size) * 4 + 2];
        if (offset == OWN) {
            return owns[index];
        }
        var chunk = (int) (offset >>> 32);
        return chunk == 0 ? data : chunks[chunk].array;
    }

    /// {@return where in [#valueArray(int)] the value of write `index` starts}
    ///
    /// @param index a write that puts a value
    int valueArrayOffset(int index) {
        var offset = spans[Objects.checkIndex(index, size) * 4 + 2];
        return offset == OWN ? 0 : (int) offset;
    }

    /// {@return the arrays of the batch's rope, none of them above [#CHUNK_BYTES], or 0 if it has none; for tests}
    int chunksForTests() {
        return chunks == null ? 0 : chunkCount - 1;
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
        if (offset < 0) {
            return offset == ABSENT ? null : MemorySegment.ofArray(owns[index]).asReadOnly();
        }
        var length = spans[index * 4 + field + 1];
        var chunk = (int) (offset >>> 32);
        return chunk == 0 ? view.asSlice(offset, length) : chunks[chunk].view.asSlice((int) offset, length);
    }

    /// {@return the store that created the batch}
    public Object owner() {
        return owner;
    }

    /// {@return the bytes the encodings occupy, on whichever arrays}
    public long encodedBytes() {
        return used + ownBytes + ropeBytes;
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
    /// thousand messages one array of over 2 MiB, which the collector gives regions of its own. A value on an array of
    /// its own stays there and counts for none of this. A batch on a rope has its paged values alone on their chunks
    /// already, which it hands over as they are, but for the last one, copied to an array of its bytes if it leaves
    /// more than [#SLACK] bytes or a sixteenth unused.
    ///
    /// A commit that fails prepares again when it is retried: a prepare writes nothing a former one exposed.
    ///
    /// @param copies whether the store copies every value it keeps, as a store in memory does, so no write has a home
    void prepare(boolean copies) {
        homes = null;
        homeOffsets = null;
        homesMode = HOMES_NONE;
        if (copies) {
            return;
        }
        if (chunks != null) {
            prepareRope();
            return;
        }
        var paged = 0L;
        var resident = 0L;
        for (var i = 0; i < size; i++) {
            if (kinds[i] == PUT && spans[i * 4 + 2] != OWN) {
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
        if (used - kept <= Math.max(SLACK, used / 16)) {
            compact();
            homesMode = HOMES_OWN;
            exposed = true;
            return;
        }
        if (paged == 0) {
            return;
        }
        homes = new MemorySegment[size];
        homeOffsets = new int[size];
        homesMode = HOMES_CHUNKS;
        // At most CHUNK_BYTES, as the batch's array is
        var chunk = new byte[(int) paged];
        var chunkView = MemorySegment.ofArray(chunk).asReadOnly();
        var fill = 0;
        for (var i = 0; i < size; i++) {
            if (!packed(i)) {
                continue;
            }
            var offset = (int) spans[i * 4 + 2];
            var length = (int) spans[i * 4 + 3];
            System.arraycopy(data, offset, chunk, fill, length);
            homes[i] = chunkView;
            homeOffsets[i] = fill;
            fill += length;
        }
    }

    /// Exposes the chunks of values no prepare has: from here on no one writes them, as a commit that fails may leave
    /// versions on them, so a write added for a retry starts a new chunk, and a retry finds each chunk's home as it was.
    ///
    /// Every chunk but the last is more than half full, as its next value did not fit, so they waste at most a value's
    /// bytes each.
    private void prepareRope() {
        homesMode = HOMES_ROPE;
        var last = valueChunk;
        valueChunk = null;
        for (var i = 1; i < chunkCount; i++) {
            var chunk = chunks[i];
            if (chunk.values && chunk.home == null) {
                var unused = chunk.array.length - chunk.fill;
                chunk.home = chunk == last && unused > Math.max(SLACK, chunk.array.length / 16)
                        ? MemorySegment.ofArray(Arrays.copyOf(chunk.array, chunk.fill)).asReadOnly()
                        : chunk.view;
            }
        }
    }

    // Whether write `index` puts a value the store keeps on a chunk in HOMES_CHUNKS
    private boolean packed(int index) {
        return kinds[index] == PUT && spans[index * 4 + 2] != OWN && targets[index].residency() == Keyspace.Residency.PAGED;
    }

    /// {@return the read-only segment the store's version of the value of write `index` slices, or `null` if the store
    /// copies the value or the value has an [array of its own][#ownValue(int)]}
    ///
    /// The segment holds no write the batch makes after [#finish()].
    ///
    /// @param index a write that puts a value
    MemorySegment home(int index) {
        Objects.checkIndex(index, size);
        return switch (homesMode) {
            case HOMES_OWN -> spans[index * 4 + 2] == OWN ? null : view;
            case HOMES_CHUNKS -> homes[index];
            case HOMES_ROPE -> packed(index) ? chunks[(int) (spans[index * 4 + 2] >>> 32)].home : null;
            default -> null;
        };
    }

    /// {@return where in [#home(int)] the value of write `index` starts}
    ///
    /// @param index a write that puts a value and has a home
    int homeOffset(int index) {
        Objects.checkIndex(index, size);
        return switch (homesMode) {
            case HOMES_OWN -> spans[index * 4 + 2] == OWN ? 0 : Math.toIntExact(spans[index * 4 + 2]);
            case HOMES_CHUNKS -> homeOffsets[index];
            case HOMES_ROPE -> packed(index) ? (int) spans[index * 4 + 2] : 0;
            default -> 0;
        };
    }

    /// Starts the batch again, empty, once the store has applied its writes: it hands over to the store the arrays the
    /// versions slice, and gives the others back to the pool.
    void finish() {
        dropData();
        clear();
    }

    /// Lets go of the batch's array, which goes back to the pool unless a prepare exposed it, and of the one it had
    /// before [#compact()], which does.
    private void dropData() {
        if (spare != null) {
            pool.release(spare);
            spare = null;
        }
        if (!exposed) {
            pool.release(data);
        }
        exposed = false;
        data = EMPTY;
        memory = EMPTY_MEMORY;
        view = EMPTY_MEMORY;
    }

    /// Moves the encodings into an array of their size if they leave a sixteenth of the batch's unused, before the
    /// store reads the writes through the accessors.
    ///
    /// The store keeps the array it applies the writes from for as long as the versions slice it: until checkpoints
    /// have written them and no read needs them. Without this, a commit of one small write would keep the whole
    /// [#INITIAL_CAPACITY] of its batch, and a batch that grew by doubling a quarter of its array on average, every
    /// byte of which the store held for each write. The batch keeps its own array for its next writes.
    public void compact() {
        if (chunks != null || spare != null || used > data.length - data.length / 16) {
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
    /// @throws IllegalStateException if the encodings are not all on that array, as those of a value of half a chunk or
    ///                               more and of a batch on a rope never are
    public byte[] detach() {
        if (ownBytes != 0 || chunks != null) {
            throw new IllegalStateException("The encodings are not on one array");
        }
        var detached = data;
        exposed = true;
        dropData();
        clear();
        return detached;
    }
}
