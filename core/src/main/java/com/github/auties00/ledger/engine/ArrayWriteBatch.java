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
import java.util.function.ToLongFunction;

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
/// version keeps, so the value is encoded once and copied by neither [#prepare(boolean, ValueSlab, Map)] nor the store.
/// So does a byte array of [#RESIDENT_OWN_BYTES] or more put to a resident key space of byte arrays, whose version
/// the store gives an array of its own in any case: cloned at the put, it is the one copy the store keeps, where on the
/// batch's array it would be copied again by the store and leave the batch's bytes for the collector.
///
/// A byte array of a G1 region or more put to a paged key space of byte arrays whose values of that length go to the
/// file of large values, as the store's `splitFrom` says, takes two arrays instead, a [SplitValue]: the version keeps
/// it until a checkpoint, and on one array its last region would be almost empty.
///
/// A range removal stores its bounds, each encoded or absent; the store reads them back through the accessors of
/// this class when it applies the batch, then starts it again with [#finish()].
///
/// The batch keeps, as it records each write, what the store would otherwise sum over the whole batch at each commit:
/// whether it removes a range, the bytes of the values it puts by residency, its puts to resident key spaces and its
/// largest value. A pass over the writes would be a loop of a method called once a commit, which the JIT compiles
/// only after tens of thousands of iterations, so a process making a few large commits would run it interpreted; kept
/// as the writes come, the totals cost a few instructions of a method the JIT compiles within the first thousands of
/// writes.
public final class ArrayWriteBatch implements WriteBatch {
    private static final int INITIAL_CAPACITY = 4_096;
    private static final byte[] EMPTY = new byte[0];
    private static final MemorySegment EMPTY_MEMORY = MemorySegment.ofArray(EMPTY);
    /// The writes a new batch has room for: most batches hold one or two.
    private static final int INITIAL_WRITES = 2;
    /// The most bytes of a batch's array, of the chunks of its rope, and of the array [#prepare(boolean, ValueSlab, Map)] copies values
    /// into: below half a region of the smallest the G1 collector makes, 1 MiB, so none takes a region of its own, as
    /// an object of half a region or more does.
    static final int CHUNK_BYTES = 256 * 1024;
    /// The least bytes of a value that takes an array of its own, which [#CHUNK_BYTES] could hold few of.
    private static final int OWN_BYTES = CHUNK_BYTES / 2;
    /// The bytes besides the values the store keeps that a batch may hold and still keep them on its own array.
    private static final int SLACK = 64;
    /// The least bytes of a byte array put to a resident key space of byte arrays that takes an array of its own.
    ///
    /// Below it, what an array of its own costs besides the value, the read-only view the store reads it through
    /// (about 100 bytes, against 40 for a slice of the batch's array) and the batch's array of those arrays, outweighs
    /// the bytes it spares the batch's array.
    static final int RESIDENT_OWN_BYTES = 2 * SLACK;
    /// The most writes of a batch whose values a [ValueSlab] takes, which fits the mask of the writes it took. A larger
    /// batch fills its own chunks in one go, and the slab's would come on top of what a history sync's batches take
    /// already.
    private static final int SLAB_WRITES = 16;
    // No write has a home: the store copies every value it keeps
    private static final byte HOMES_NONE = 0;
    // Every write's home is the batch's own view
    private static final byte HOMES_OWN = 1;
    // A paged write's home is packedHome, at the offset the high half of its key's length holds; a resident one has none
    private static final byte HOMES_CHUNKS = 2;
    // A paged write's home is the home of its chunk of the rope, a resident one has none
    private static final byte HOMES_ROPE = 3;
    // A paged write's home is in perWriteHomes, at the offset the high half of its key's length holds; a resident one
    // has none
    private static final byte HOMES_PER_WRITE = 4;
    // As HOMES_CHUNKS, but packedHome is the shared view of a slab's chunk, which no commit allocates
    private static final byte HOMES_SLAB = 5;

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
    // The least bytes of a value put to a key space that the batch splits, Long.MAX_VALUE for one it never splits; or
    // null if it splits none
    private final ToLongFunction<Keyspace<?, ?>> splitFrom;
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
    // In HOMES_CHUNKS and HOMES_SLAB, the read-only segment the values of the paged writes were copied into, and the
    // array behind it
    private MemorySegment packedHome;
    private byte[] packedHomeArray;
    // In HOMES_PER_WRITE, the read-only segment the value of each paged write was copied into: a slab's chunks, more than
    // one of them or beside the array of the values it did not take; and the array behind each
    private MemorySegment[] perWriteHomes;
    private byte[][] perWriteHomeArrays;

    private byte[] kinds;
    private Keyspace<?, ?>[] targets;
    private Object[] values;
    // Per write: first offset and length, then second offset and length (value, or upper bound of a range). Only the low
    // half of the first length is the length: in HOMES_CHUNKS, HOMES_SLAB and HOMES_PER_WRITE the high half is where
    // the write's value is in its home
    private long[] spans;
    // The array of each value of OWN_BYTES or more, and of each resident byte array of RESIDENT_OWN_BYTES or more, or
    // null before the first
    private byte[][] owns;
    // The last bytes of each value in owns that the batch split, whose first bytes owns holds, or null before the first
    private byte[][] ownTails;
    // The bytes of the arrays in owns and ownTails
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
    // The totals of the writes, written only by add() and clear(): whether a write removes a range
    private boolean removesRanges;
    // The bytes of the values put to paged key spaces, those on arrays of their own aside
    private long pagedValueBytes;
    // The bytes of the values put to resident key spaces, those on arrays of their own aside, and the number of those
    // puts, all of them
    private long residentValueBytes;
    private int residentPuts;
    // The bytes of the largest value put
    private long largestValueBytes;

    /// Creates an empty batch for a store that splits no value.
    ///
    /// @param owner the store, which alone may apply the batch
    /// @param pool the store's pool, which the batch draws its memory from
    /// @param keyspaces the store's key spaces by name, which the batch may write
    public ArrayWriteBatch(Object owner, HeapPool pool, Map<String, Keyspace<?, ?>> keyspaces) {
        this(owner, pool, keyspaces, null);
    }

    /// Creates an empty batch for a store.
    ///
    /// @param owner the store, which alone may apply the batch
    /// @param pool the store's pool, which the batch draws its memory from
    /// @param keyspaces the store's key spaces by name, which the batch may write
    /// @param splitFrom the least bytes of a value put to a key space that the batch keeps as a [SplitValue], if a
    ///                  region allows it, or [Long#MAX_VALUE] for a key space whose values it never splits: a key space
    ///                  that splits is paged, of byte arrays, and writes values of that length to the file of large
    ///                  values, so the store's version of one is always an [Version.External]; or `null` if the batch
    ///                  splits none
    ArrayWriteBatch(Object owner, HeapPool pool, Map<String, Keyspace<?, ?>> keyspaces, ToLongFunction<Keyspace<?, ?>> splitFrom) {
        this.owner = owner;
        this.pool = pool;
        this.keyspaces = keyspaces;
        this.splitFrom = splitFrom;
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
        if (valueLength >= OWN_BYTES || valueLength >= RESIDENT_OWN_BYTES && residentBytes(keyspace)) {
            checkRoom(valueLength);
            var head = splitHead(keyspace, valueLength);
            if (head > 0) {
                // Only a key space of byte arrays splits: the array is its own encoding
                var array = (byte[]) value;
                var first = Arrays.copyOf(array, head);
                var tail = Arrays.copyOfRange(array, head, (int) valueLength);
                addOwn(keyspace, null, encodeKey(keyspace, key, keyLength), keyLength, first, tail);
                return this;
            }
            var own = encodeOwn(keyspace.values(), value, (int) valueLength);
            var ownKeyOffset = encodeKey(keyspace, key, keyLength);
            addOwn(keyspace, keyspace.residency() == Keyspace.Residency.RESIDENT ? value : null, ownKeyOffset, keyLength, own, null);
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
        if (valueLength >= OWN_BYTES || valueLength >= RESIDENT_OWN_BYTES && residentBytes(keyspace)) {
            checkRoom(valueLength);
            var head = splitHead(keyspace, valueLength);
            var own = head > 0 ? copyOwn(value.asSlice(0, head)) : copyOwn(value);
            var tail = head > 0 ? copyOwn(value.asSlice(head)) : null;
            var ownKeyOffset = reserve(keyLength);
            if (ownKeyOffset == ON_ROPE) {
                checkRoom(keyLength);
                ownKeyOffset = takeKey(keyLength);
            }
            MemorySegment.copy(key, 0, slot(ownKeyOffset, keyLength), 0, keyLength);
            addOwn(keyspace, null, ownKeyOffset, keyLength, own, tail);
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

    /// {@return whether `keyspace` is resident and of byte arrays, whose versions the store always gives an array of
    /// their own: the test the store makes of a put's key space when it applies the put}
    private static boolean residentBytes(Keyspace<?, ?> keyspace) {
        return keyspace.values() == BuiltinCodecs.BYTES_VALUE && keyspace.residency() == Keyspace.Residency.RESIDENT;
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

    /// {@return the bytes of the head of a value of `length` bytes put to `keyspace` if the batch splits it, as
    /// [SplitValue#headLength(long, long)] gives them, or 0 if the value takes one array}
    ///
    /// Only a value of at least the smallest region G1 makes, put to a key space that splits values of its length,
    /// asks for the region, which the first time loads the management classes.
    private int splitHead(Keyspace<?, ?> keyspace, long length) {
        if (length < ValueSlab.SMALLEST_REGION || splitFrom == null || length < splitFrom.applyAsLong(keyspace)) {
            return 0;
        }
        return SplitValue.headLength(length, SplitValue.region());
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

    /// Records a put of a value on an array of its own, private to the batch, or on two if `tail` is not `null`.
    ///
    /// @param own the value, or its first bytes if it is split
    /// @param tail the rest of a split value, or `null`
    private void addOwn(Keyspace<?, ?> keyspace, Object value, long keyOffset, int keyLength, byte[] own, byte[] tail) {
        if (owns == null) {
            owns = new byte[kinds.length][];
        }
        if (tail != null && ownTails == null) {
            ownTails = new byte[kinds.length][];
        }
        var length = tail == null ? own.length : own.length + tail.length;
        add(PUT, keyspace, value, keyOffset, keyLength, OWN, length);
        owns[size - 1] = own;
        if (tail != null) {
            ownTails[size - 1] = tail;
        }
        ownBytes += length;
        // The value stays where it is, so add() is left without a branch no other write takes
        if (keyspace.residency() == Keyspace.Residency.PAGED) {
            pagedValueBytes -= length;
        } else {
            residentValueBytes -= length;
        }
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

    /// Moves the encodings of the batch to a rope, rewriting the spans of its writes; a value on an array of its own
    /// stays there, so only keys and values below [#OWN_BYTES] come here, and none has a span that outgrows a chunk.
    private void moveToRope() {
        chunks = new Chunk[8];
        chunkCount = 1;
        for (var i = 0; i < size; i++) {
            moveWrite(i);
        }
        dropData();
        used = 0;
    }

    /// Moves the bytes of write `index` to the rope, by a call of its own, which the JIT compiles once a few thousand
    /// writes have made it, as it would not compile the loop of a method called once a batch.
    private void moveWrite(int index) {
        if (spans[index * 4] != ABSENT) {
            spans[index * 4] = moveKey(spans[index * 4], (int) spans[index * 4 + 1]);
        }
        var second = spans[index * 4 + 2];
        if (second >= 0) {
            var length = (int) spans[index * 4 + 3];
            var position = packed(index) ? takeValue(length) : takeKey(length);
            moveBytes(second, position, length);
            spans[index * 4 + 2] = position;
        }
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
        // The array behind home: the chunk's own, which then never goes back to the pool, or the copy
        byte[] homeArray;

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
            if (ownTails != null) {
                ownTails = Arrays.copyOf(ownTails, capacity);
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
        // After the write is recorded, so arrays that fail to grow leave the totals those of the writes
        if (kind == PUT) {
            if (keyspace.residency() == Keyspace.Residency.PAGED) {
                pagedValueBytes += secondLength;
            } else {
                residentValueBytes += secondLength;
                residentPuts++;
            }
            // Without a branch: only the first puts of a batch raise it, and the JIT would leave the raise out of the
            // code it compiles from the puts after them
            largestValueBytes = Math.max(largestValueBytes, secondLength);
        } else if (kind == DELETE_RANGE) {
            // The second span of a range removal is its upper bound, not a value
            removesRanges = true;
        }
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
        removesRanges = false;
        pagedValueBytes = 0;
        residentValueBytes = 0;
        residentPuts = 0;
        largestValueBytes = 0;
        owns = null;
        ownTails = null;
        ownBytes = 0;
        leaveRope();
        // What a commit that failed prepared
        packedHome = null;
        packedHomeArray = null;
        perWriteHomes = null;
        perWriteHomeArrays = null;
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
    /// @throws IllegalStateException if the batch split the value, which it never joins: [#ownValue(int)] and
    ///                               [#ownTail(int)] hold it
    public MemorySegment value(int index) {
        return span(index, 2);
    }

    /// {@return the bytes of the value of write `index`, 0 if it has none}
    ///
    /// @param index the write
    long valueBytes(int index) {
        Objects.checkIndex(index, size);
        return spans[index * 4 + 2] == ABSENT ? 0 : spans[index * 4 + 3];
    }

    /// {@return whether a write of the batch removes a range}
    boolean removesRanges() {
        return removesRanges;
    }

    /// {@return the writes of the batch that put a value to a [Keyspace.Residency#RESIDENT] key space, whether the
    /// batch holds the value decoded or only encoded}
    int residentPuts() {
        return residentPuts;
    }

    /// {@return the bytes of the largest value the batch puts, 0 if it puts none}
    long largestValueBytes() {
        return largestValueBytes;
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

    /// {@return the last bytes of the value of write `index` if the batch split it, whose first bytes
    /// [#ownValue(int)] holds, or `null` if it did not}
    ///
    /// Both arrays are private to the batch and no one writes them.
    ///
    /// @param index the write
    byte[] ownTail(int index) {
        Objects.checkIndex(index, size);
        return ownTails == null ? null : ownTails[index];
    }

    /// {@return the array that holds the value of write `index`, at [#valueArrayOffset(int)]}
    ///
    /// @param index a write that puts a value
    /// @throws IllegalStateException if the batch split the value, which no one array holds
    byte[] valueArray(int index) {
        var offset = spans[Objects.checkIndex(index, size) * 4 + 2];
        if (offset == OWN) {
            checkWhole(index);
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
            if (offset == ABSENT) {
                return null;
            }
            checkWhole(index);
            return MemorySegment.ofArray(owns[index]).asReadOnly();
        }
        var length = (int) spans[index * 4 + field + 1];
        var chunk = (int) (offset >>> 32);
        return chunk == 0 ? view.asSlice(offset, length) : chunks[chunk].view.asSlice((int) offset, length);
    }

    /// Checks that the value of write `index`, on an array of its own, is not split.
    ///
    /// @throws IllegalStateException if it is: a value of a region or more is never joined without the caller knowing
    private void checkWhole(int index) {
        if (ownTails != null && ownTails[index] != null) {
            throw new IllegalStateException("The value of write " + index + " is split");
        }
    }

    /// {@return the read-only segment of the batch's array, the home of no value once the batch is on a rope; for tests}
    MemorySegment viewForTests() {
        return view;
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
    /// once the commit [ends][#finish()]. The small values of a small batch go to a [ValueSlab] while it takes them, and
    /// the batch's array goes back the same way. On the batch's array, a message committed beside a session of 1.5 KiB
    /// would keep the session's bytes, which the store holds a copy of already, until the next checkpoint. A value on an
    /// array of its own stays there and counts for none of this. A batch on a rope has its paged values alone on their
    /// chunks already, which it hands over as they are, but for one that leaves more than [#SLACK] bytes or a sixteenth unused,
    /// copied to an array of its bytes.
    ///
    /// A commit that fails prepares again when it is retried: a prepare writes nothing a former one exposed.
    ///
    /// @param copies whether the store copies every value it keeps, as a store in memory does, so no write has a home
    /// @param slab the store's chunks of values, or `null` if it has none
    /// @param states the store's key spaces by name
    void prepare(boolean copies, ValueSlab slab, Map<String, KeyspaceState> states) {
        packedHome = null;
        packedHomeArray = null;
        perWriteHomes = null;
        perWriteHomeArrays = null;
        homesMode = HOMES_NONE;
        if (copies) {
            return;
        }
        if (chunks != null) {
            prepareRope();
            return;
        }
        var paged = pagedValueBytes;
        if (paged > 0 && size <= SLAB_WRITES && slab != null && !slab.off() && placeOnSlab(slab, states)) {
            return;
        }
        // A batch of resident writes alone keeps them on its array as before, where the store's rule for resident
        // values finds it small enough; any other copies them, so the paged values' array holds none
        var kept = paged > 0 ? paged : residentValueBytes;
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
        homesMode = HOMES_CHUNKS;
        // At most CHUNK_BYTES, as the batch's array is
        var packer = new Packer((int) paged);
        for (var i = 0; i < size; i++) {
            packer.pack(i);
        }
    }

    /// Exposes the chunks of values no prepare has: from here on no one writes them, as a commit that fails may leave
    /// versions on them, so a write added for a retry starts a new chunk, and a retry finds each chunk's home as it was.
    ///
    /// The heap a commit is estimated to hold counts its values and not the arrays around them, and the checkpoint
    /// backlog's budget trusts that estimate, so no chunk keeps more than a sixteenth unused: values of 87 KiB fill two
    /// thirds of each. Only a value above a sixteenth of a chunk leaves that much behind it, so a batch of smaller ones
    /// copies its last chunk at most; a chunk copied goes back to the pool once the commit ends.
    private void prepareRope() {
        homesMode = HOMES_ROPE;
        valueChunk = null;
        for (var i = 1; i < chunkCount; i++) {
            var chunk = chunks[i];
            if (chunk.values && chunk.home == null) {
                var unused = chunk.array.length - chunk.fill;
                if (unused > Math.max(SLACK, chunk.array.length / 16)) {
                    chunk.homeArray = Arrays.copyOf(chunk.array, chunk.fill);
                    chunk.home = MemorySegment.ofArray(chunk.homeArray).asReadOnly();
                } else {
                    chunk.homeArray = chunk.array;
                    chunk.home = chunk.view;
                }
            }
        }
    }

    /// Gives `slab` the paged values it may take if it is accepting them, and counts them toward its rate either way:
    /// those that no unit of the file of large values holds too, of a key space that does not overwrite or remove most
    /// of what it writes.
    ///
    /// A batch whose paged values all landed on one chunk has that chunk for their home, as any batch has the one array
    /// it packs them into, with nothing allocated for it. Any other, which a chunk filling up in the middle of the batch
    /// or a value the slab declined makes, has a home per write, and the values the slab did not take go on an array of
    /// their bytes.
    ///
    /// @return whether it took any, so every paged write has its home
    private boolean placeOnSlab(ValueSlab slab, Map<String, KeyspaceState> states) {
        var accepting = slab.accepting();
        var eligible = 0L;
        var placedBytes = 0L;
        var placed = 0;
        var declined = false;
        ValueSlab.Chunk first = null;
        ValueSlab.Chunk[] several = null;
        for (var i = 0; i < size; i++) {
            if (!packed(i)) {
                continue;
            }
            var length = (int) spans[i * 4 + 3];
            var state = states.get(targets[i].name());
            if (length == 0 || length >= state.largeValues || !state.overwrites.worthwhile()) {
                declined = true;
                continue;
            }
            eligible += length;
            var home = accepting ? slab.place(data, (int) spans[i * 4 + 2], length, spans, i * 4 + 1) : null;
            if (home == null) {
                declined = true;
                continue;
            }
            placed |= 1 << i;
            placedBytes += length;
            if (first == null) {
                first = home;
            } else if (several == null && home != first) {
                several = new ValueSlab.Chunk[size];
            }
            if (several != null) {
                several[i] = home;
            }
        }
        slab.note(eligible);
        if (first == null) {
            return false;
        }
        if (several == null && !declined) {
            packedHome = first.view();
            packedHomeArray = first.array();
            homesMode = HOMES_SLAB;
            return true;
        }
        perWriteHomes = new MemorySegment[size];
        perWriteHomeArrays = new byte[size][];
        if (several != null) {
            for (var i = 0; i < size; i++) {
                if (several[i] != null) {
                    perWriteHomes[i] = several[i].view();
                    perWriteHomeArrays[i] = several[i].array();
                }
            }
        }
        homesMode = HOMES_PER_WRITE;
        Packer packer = null;
        for (var i = 0; i < size; i++) {
            if (!packed(i)) {
                continue;
            }
            if ((placed >>> i & 1) == 0) {
                if (packer == null) {
                    packer = new Packer((int) (pagedValueBytes - placedBytes));
                }
                packer.pack(i);
                perWriteHomes[i] = packedHome;
                perWriteHomeArrays[i] = packedHomeArray;
            } else if (perWriteHomes[i] == null) {
                perWriteHomes[i] = first.view();
                perWriteHomeArrays[i] = first.array();
            }
        }
        return true;
    }

    // Whether write `index` puts a value the store keeps on a chunk, in HOMES_CHUNKS, HOMES_SLAB, HOMES_PER_WRITE and
    // HOMES_ROPE
    private boolean packed(int index) {
        return kinds[index] == PUT && spans[index * 4 + 2] != OWN && targets[index].residency() == Keyspace.Residency.PAGED;
    }

    /// The array [#prepare(boolean, ValueSlab, Map)] copies the values of paged key spaces into, in the order of the writes.
    ///
    /// Each write is packed by a call of its own, which the JIT compiles once a few thousand writes have made it, as it
    /// would not compile the loop of a method called once a commit through the first large commits of a process.
    private final class Packer {
        private final byte[] chunk;
        private int fill;

        /// Makes the array for values of `bytes` bytes in all, and the home of those values.
        Packer(int bytes) {
            chunk = new byte[bytes];
            packedHome = MemorySegment.ofArray(chunk).asReadOnly();
            packedHomeArray = chunk;
        }

        /// Gives write `index` its home, if it puts a value to a paged key space.
        void pack(int index) {
            if (!packed(index)) {
                return;
            }
            var offset = (int) spans[index * 4 + 2];
            var length = (int) spans[index * 4 + 3];
            System.arraycopy(data, offset, chunk, fill, length);
            spans[index * 4 + 1] = (long) fill << 32 | (int) spans[index * 4 + 1];
            fill += length;
        }
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
            case HOMES_CHUNKS, HOMES_SLAB -> packed(index) ? packedHome : null;
            case HOMES_ROPE -> packed(index) ? chunks[(int) (spans[index * 4 + 2] >>> 32)].home : null;
            case HOMES_PER_WRITE -> packed(index) ? perWriteHomes[index] : null;
            default -> null;
        };
    }

    /// {@return the array [#home(int)] is a view of, holding the value at [#homeOffset(int)], or `null` if the write
    /// has no home}
    ///
    /// No one writes the array where the value lies once the prepare has returned, and the pool never gets it back,
    /// as [#finish()] says, so a log frame may refer to the value after the batch has started again: it is the batch's
    /// own array once a prepare exposed it, an array the values were copied into, a slab's chunk, or a chunk of the
    /// rope the versions keep or the copy of it they keep instead.
    ///
    /// @param index a write that puts a value
    byte[] homeArray(int index) {
        Objects.checkIndex(index, size);
        return switch (homesMode) {
            case HOMES_OWN -> spans[index * 4 + 2] == OWN ? null : data;
            case HOMES_CHUNKS, HOMES_SLAB -> packed(index) ? packedHomeArray : null;
            case HOMES_ROPE -> packed(index) ? chunks[(int) (spans[index * 4 + 2] >>> 32)].homeArray : null;
            case HOMES_PER_WRITE -> packed(index) ? perWriteHomeArrays[index] : null;
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
            case HOMES_CHUNKS, HOMES_SLAB, HOMES_PER_WRITE -> packed(index) ? (int) (spans[index * 4 + 1] >>> 32) : 0;
            case HOMES_ROPE -> packed(index) ? (int) spans[index * 4 + 2] : 0;
            default -> 0;
        };
    }

    /// {@return whether the last prepare put the values the store keeps on one chunk of a [ValueSlab], whose view the
    /// slab shares, so the commit allocates no array, view or session for them}
    boolean sharesChunk() {
        return homesMode == HOMES_SLAB;
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
}
