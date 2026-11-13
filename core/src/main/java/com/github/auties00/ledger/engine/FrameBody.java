package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.LedgerCorruptedException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/// The body of a log frame: the writes of one commit, entries of the key space catalog, or the epoch of an open.
///
/// A commit body is its writes, in order, each a kind byte, the key space id and the encoded bytes:
///
/// ```text
/// put       0, id, key length, key, value length, value
/// delete    1, id, key length, key
/// range     2, id, lower bound length, lower bound, upper bound length, upper bound   (0: no bound)
/// external  3, id, key length, key, value length, reference of the value's unit in ledger.blobs (20)
/// ```
///
/// A catalog body is entries of an id and a name; an epoch body is the epoch. A run body is the bodies of commits of
/// consecutive sequences, from the one its frame carries, each after its length:
///
/// ```text
/// run       body length, commit body, body length, commit body, ...   (at least one commit)
/// ```
///
/// Numbers are unsigned LEB128 varints; keys are never empty, so a bound of length 0 is an absent bound.
///
/// A commit body holds what it copies in an array of its own, and refers to the value of a put of at least its
/// [referable][#referable()] bytes that lies on an array the store's version keeps anyway: an array a write batch
/// hands over to the store, a slab's chunk, an array the store copied the value into for its version, or one a
/// transaction encoded it into. The log writer reads the [pieces][#pieces()] in order and seals the bytes the format
/// above says, with no trace of where they were. A body refers to values only where gathering them costs the writer
/// nothing: in a body of a batch of at most [LogWriter#RUN_MAX_BODY] bytes, which the writer copies into its output
/// whole before it seals it there anyway, from [#RUN_REFERABLE] bytes; and in a body of a transaction, whose length
/// is unknown until its last write, from [#SEALED_REFERABLE] bytes, values long enough for the writer to hand to the
/// cipher as they are. The body of a commit of one value of a kilobyte thus allocates an array for the value's key and
/// lengths alone, where it copied the value too.
///
/// A referred array is one no one writes and the store's [HeapPool] never hands out, so a frame may wait in the log's
/// queue while the batch it came from starts again on other arrays. It keeps the whole array reachable meanwhile, a
/// slab's chunk for a value of a few hundred bytes, until the writer seals it; the queue's limit bounds that.
///
/// A body made with a [HeapPool] keeps its own bytes past [#CHUNK_BYTES] in chunks of that size from the pool, every
/// one but the last full, rather than in one array of megabytes that the collector would give regions of their own and
/// that the log reads once: the body of a large batch, which refers to no value, and that of a transaction, whose
/// values of a few kilobytes it refers to. The log hands the chunks back once it sealed the frame ([#release()]); a
/// body the log never took goes back with [#discard()]. From its first chunk until then the body
/// [holds][HeapPool#hold()] the pool's overflow, where the chunks of the frames sealed before it wait, so a collection
/// while it fills keeps them for it; a body a stopped log leaves to the collector lets go of it ([#abandon()]). A
/// chunked body that refers to no value is read through [#chunkCount()], [#chunk(int)] and [#chunkLength(int)], one
/// that refers to values through its pieces, which then never span two chunks; [#array()] serves only a body of one
/// array. No pooled chunk is ever an array a body refers to: those are arrays the pool never hands out.
///
/// A write checks once that the array it writes has room for its longest encoding, and takes a path that checks each
/// field only when it may not: that path, which grows the array or goes on in the next chunk, runs at a chunk boundary
/// whichever field the boundary falls in, so the compiled write has no branch that only a rare alignment takes.
///
/// A body is at most [#MAX_BYTES] long. A batch never comes near it, as [ArrayWriteBatch] limits its own bytes below
/// it; a transaction checks each write with [#checkRoom(long)] before the write changes anything.
final class FrameBody {
    /// A frame holding a commit.
    static final byte COMMIT = 1;
    /// A frame naming key spaces.
    static final byte CATALOG = 2;
    /// A frame holding the epoch of an open, durable before the open writes a unit of [BlobFile].
    static final byte EPOCH = 3;
    /// A frame holding a run of commits of consecutive sequences, from the one it carries, which [LogWriter] seals as
    /// one message.
    static final byte COMMITS = 4;
    /// The bytes of a chunk of a body made with a pool: the size of the chunks of a batch's rope, so the frames of a
    /// history sync hand theirs on to the next batches; below half the smallest region of the G1 collector; and a
    /// multiple of the slice [LogWriter] seals at a time, so no slice spans two chunks.
    static final int CHUNK_BYTES = ArrayWriteBatch.CHUNK_BYTES;
    /// The shortest value the body of a batch the log writer seals in its output refers to rather than copies: below
    /// it, copying the value costs less than the reference and the piece it adds to the gather.
    static final int RUN_REFERABLE = 256;
    /// The shortest value the body of a transaction refers to rather than copies: the writer may seal such a body
    /// slice by slice, handing each long piece to the cipher as it is, and a call of the cipher costs about what
    /// copying a few kilobytes does.
    static final int SEALED_REFERABLE = 2 << 10;
    /// What [#referable()] is for a body that refers to no value.
    static final int NOT_REFERABLE = Integer.MAX_VALUE;
    /// The longest body: the longest whose frame AES-GCM can seal and recovery can read into one array, which the JVM
    /// limits to a few bytes under 2 GiB, with the frame's header and its tag or checksum.
    static final int MAX_BYTES = Integer.MAX_VALUE - 8 - LogWriter.FRAME_HEADER - Math.max(LogWriter.TAG_BYTES, Integer.BYTES);
    /// The most bytes a write takes beside its keys, value and unit reference, which [#checkRoom(long)] counts and a
    /// write checks the room of its array for: a kind and three numbers of up to ten bytes.
    static final int WRITE_FIELDS = 1 + 3 * 10;

    private static final byte PUT = 0;
    private static final byte DELETE = 1;
    private static final byte RANGE = 2;
    private static final byte EXTERNAL = 3;
    // The ints of laterSpans per reference: where in data the value belongs, where it starts in its array, its length
    private static final int SPAN_FIELDS = 3;
    // The references after the first a body makes room for when it needs a second
    private static final int LATER_REFERENCES = 4;

    // The body is its writer's, which writes it or discards it
    private static final byte OPEN = 0;
    // The log owns the body, which only it releases
    private static final byte HANDED = 1;
    // The body's pooled arrays went back to the pool; nothing reads or writes it again
    private static final byte RELEASED = 2;

    // The array of the bytes the body holds itself, in its first `used`: the only one, or the last chunk
    private byte[] data;
    private int used;
    // The values the body refers to and the shortest it refers to, one shared and empty while it refers to none: apart,
    // so the body of a commit that refers to nothing, as those of small values and of large batches do, has no field
    // of references besides this one
    private References referred;
    // The pool of a body made with one, its chunks and who owns them, or null for a body of one array that grows by
    // doubling: apart, as the references are, so the body of a small commit carries one field for them
    private Pooled pooled;

    FrameBody() {
        this(256);
    }

    /// Makes a body of one array that refers to no value, with room for `capacity` bytes before it grows.
    FrameBody(int capacity) {
        this(capacity, NOT_REFERABLE, null);
    }

    /// Makes a body of one array with room for `capacity` bytes of its own before it grows, which refers to the values
    /// of at least `referable` bytes a put offers on an array.
    FrameBody(int capacity, int referable) {
        this(capacity, referable, null);
    }

    /// Makes a body with room for `capacity` bytes of its own before it grows, which refers to the values of at least
    /// `referable` bytes a put offers on an array and keeps its own bytes past [#CHUNK_BYTES] in chunks from `pool`.
    ///
    /// A capacity of a chunk or more starts on a chunk of the pool; a smaller one on an array of its own, which grows
    /// to at most a chunk. Further chunks are taken as the bytes reach them, never ahead: [#capacityFor] overestimates.
    ///
    /// @param pool the pool, or `null` for a body of one array that grows by doubling
    FrameBody(int capacity, int referable, HeapPool pool) {
        this.referred = References.none(referable);
        if (pool == null) {
            this.data = new byte[Math.max(16, capacity)];
        } else {
            this.pooled = new Pooled(pool);
            this.data = capacity >= CHUNK_BYTES ? pooled.takeChunk() : new byte[Math.max(16, capacity)];
        }
    }

    /// {@return the empty body of the frame of a transaction, which refers to its values of [#SEALED_REFERABLE]
    /// bytes or more and keeps its own bytes past [#CHUNK_BYTES] in chunks from `pool`}
    ///
    /// @param pool the pool, or `null` for a body of one array
    static FrameBody forTransaction(HeapPool pool) {
        return new FrameBody(256, SEALED_REFERABLE, pool);
    }

    /// {@return the empty body of the frame of a commit of `writes`, whose large values are in their `units`, with
    /// room for what it will copy, near enough that it seldom grows}
    ///
    /// A commit whose body takes at most [LogWriter#RUN_MAX_BODY] bytes by [#capacityFor(ArrayWriteBatch, PageRef\[\])],
    /// which never estimates less than a body takes, refers to its values of [#RUN_REFERABLE] bytes or more, which its
    /// room leaves out: every put of a batch offers its value on an array, but one whose value is in its unit. Such a
    /// commit has few writes, and only one with a value that long visits them. The body of any other commit refers to
    /// nothing, as the writer seals it slice by slice from the body itself, and keeps its bytes past [#CHUNK_BYTES] in
    /// chunks from `pool`.
    ///
    /// @param pool the pool, or `null` for a body of one array
    static FrameBody forCommit(ArrayWriteBatch writes, PageRef[] units, HeapPool pool) {
        var capacity = capacityFor(writes, units);
        if (capacity > LogWriter.RUN_MAX_BODY) {
            return new FrameBody(capacity, NOT_REFERABLE, pool);
        }
        if (writes.largestValueBytes() < RUN_REFERABLE) {
            return new FrameBody(capacity, RUN_REFERABLE);
        }
        for (var i = 0; i < writes.size(); i++) {
            var length = writes.valueBytes(i);
            if (length >= RUN_REFERABLE && writes.kind(i) == ArrayWriteBatch.PUT && (units == null || units[i] == null)) {
                capacity -= (int) length;
            }
        }
        return new FrameBody(capacity, RUN_REFERABLE);
    }

    /// {@return the bytes the body of a commit of `writes` takes, its large values in their units, near enough that it
    /// seldom grows}
    ///
    /// A body that doubled from a small array allocated about twice its bytes, and kept up to twice as many in the
    /// log's queue as the queue counts.
    ///
    /// The encodings of the batch hold its keys, values and bounds, and the bytes a write whose codec threw had
    /// reserved, which only make the estimate larger: only a commit with large values visits its writes.
    static int capacityFor(ArrayWriteBatch writes, PageRef[] units) {
        // The kind, the key space and the lengths of each write, as variable-length numbers, and what they encode
        var bytes = 16 + 16L * writes.size() + writes.encodedBytes();
        if (units != null) {
            for (var i = 0; i < units.length; i++) {
                if (units[i] != null) {
                    // The value is in its unit
                    bytes += PageRef.BYTES - writes.valueBytes(i);
                }
            }
        }
        return (int) Math.min(bytes, 64L << 20);
    }

    /// {@return the bytes of a body of one array that refers to no value; only the first [#size()] are meaningful}
    ///
    /// @throws IllegalStateException if the body is chunked or released
    byte[] array() {
        assert referred.count == 0 : "the body refers to values";
        checkOneArray();
        return data;
    }

    /// Checks that the body's own bytes are in one array it still holds.
    ///
    /// @throws IllegalStateException if the body is chunked or released
    private void checkOneArray() {
        var pooled = this.pooled;
        if (pooled != null && (pooled.chunks != null || pooled.state == RELEASED)) {
            throw new IllegalStateException("The body is not one array");
        }
    }

    /// {@return the length of the body, the values it refers to included}
    int size() {
        return ownBytes() + referred.bytes;
    }

    /// {@return the bytes the body holds itself, in its chunks or its one array}
    private int ownBytes() {
        var pooled = this.pooled;
        return pooled == null ? used : pooled.base + used;
    }

    /// {@return the number of arrays the own bytes of the body are in: one, or its chunks}
    ///
    /// @throws IllegalStateException if the body is released
    int chunkCount() {
        checkHeld();
        var pooled = this.pooled;
        return pooled == null || pooled.chunks == null ? 1 : pooled.chunkCount;
    }

    /// {@return the array `index` of the own bytes of the body, in order, below [#chunkCount()]}
    ///
    /// @throws IllegalStateException if the body is released
    byte[] chunk(int index) {
        checkHeld();
        var pooled = this.pooled;
        return pooled == null || pooled.chunks == null ? data : pooled.chunks[index];
    }

    /// {@return the own bytes of the body in the array `index`: [#CHUNK_BYTES] for every chunk but the last}
    ///
    /// @throws IllegalStateException if the body is released
    int chunkLength(int index) {
        checkHeld();
        var pooled = this.pooled;
        return pooled == null || pooled.chunks == null || index == pooled.chunkCount - 1 ? used : CHUNK_BYTES;
    }

    /// Checks that the body still holds its bytes.
    ///
    /// @throws IllegalStateException if the body is released
    private void checkHeld() {
        var pooled = this.pooled;
        if (pooled != null && pooled.state == RELEASED) {
            throw new IllegalStateException("The body is released");
        }
    }

    /// Records that the log owns the body from now on, which then only it releases; called as the log takes it.
    void handOff() {
        var pooled = this.pooled;
        if (pooled != null) {
            pooled.state = HANDED;
        }
    }

    /// Gives the body's pooled arrays back to its pool, once the log sealed its frame and never reads it again; a body
    /// made without a pool keeps its array, and the arrays a body refers to are never the pool's. A second call does
    /// nothing.
    void release() {
        var pooled = this.pooled;
        if (pooled == null || pooled.state == RELEASED) {
            return;
        }
        pooled.state = RELEASED;
        if (pooled.chunks != null) {
            for (var i = 0; i < pooled.chunkCount; i++) {
                assert poison(pooled.chunks[i]);
                pooled.pool.release(pooled.chunks[i]);
            }
            pooled.chunks = null;
            pooled.chunkCount = 0;
        } else if (data.length == CHUNK_BYTES) {
            assert poison(data);
            pooled.pool.release(data);
        }
        // Once every chunk is back, so no collection between the two finds the pool's overflow unheld
        pooled.hold = null;
        pooled.spread = null;
        data = null;
    }

    /// Lets go of the [hold][HeapPool#hold()] on the pool's overflow the body took with its first chunk, and keeps its
    /// chunks, which never go back to the pool: for a body a stopped log leaves to the collector, whose frame a seal
    /// may still read. A body that took no chunk, or let go already, is left as it is.
    void abandon() {
        var pooled = this.pooled;
        if (pooled != null) {
            pooled.hold = null;
        }
    }

    /// {@return whether the body holds the pool's overflow, from its first chunk until its release; for tests}
    boolean holdsPoolForTests() {
        var pooled = this.pooled;
        return pooled != null && pooled.hold != null;
    }

    /// Fills `array`, about to go back to the pool, with a pattern, with assertions on: a frame sealed from a body after
    /// its release, or a value on an array a released body still held, reads it, which no recovery or read check then
    /// takes for the bytes it wrote.
    ///
    /// @return `true`, for an `assert`
    private static boolean poison(byte[] array) {
        Arrays.fill(array, (byte) 0x5A);
        return true;
    }

    /// Releases the body as [#release()] does, unless the log took it and releases it itself once it sealed it: for a
    /// commit taken back before its frame was queued.
    void discard() {
        var pooled = this.pooled;
        if (pooled != null && pooled.state == OPEN) {
            release();
        }
    }

    /// {@return the shortest value the body refers to rather than copies, [#NOT_REFERABLE] for none}
    int referable() {
        return referred.referable;
    }

    /// {@return the number of values the body refers to}
    int references() {
        return referred.count;
    }

    /// {@return the number of pieces whose bytes, in order, are the body: a run of its own bytes, then for each value
    /// it refers to that value and the run after it}
    ///
    /// The runs of a chunked body are split at its chunks' ends, so no piece spans two arrays.
    int pieces() {
        var spread = spread();
        return spread == null ? 2 * references() + 1 : spread.count;
    }

    /// {@return the array piece `piece` is part of, which no one writes while the body is queued}
    byte[] pieceArray(int piece) {
        var spread = spread();
        if (spread != null) {
            return spread.arrays[piece];
        }
        return (piece & 1) == 0 ? data : referred.array(piece >> 1);
    }

    /// {@return where piece `piece` starts in its array}
    int pieceOffset(int piece) {
        var spread = spread();
        if (spread != null) {
            return spread.offsets[piece];
        }
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return referred.offset(reference);
        }
        return reference == 0 ? 0 : referred.at(reference - 1);
    }

    /// {@return the bytes of piece `piece`, maybe 0}
    int pieceLength(int piece) {
        var spread = spread();
        if (spread != null) {
            return spread.lengths[piece];
        }
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return referred.length(reference);
        }
        return (reference == references() ? used : referred.at(reference)) - pieceOffset(piece);
    }

    /// Copies the body into `target` at `offset`, the values it refers to gathered.
    ///
    /// @return the offset after the body, `offset` plus [#size()]
    /// @throws IllegalStateException if the body is released
    int copyTo(byte[] target, int offset) {
        if (referred.count == 0 && pooled == null) {
            System.arraycopy(data, 0, target, offset, used);
            return offset + used;
        }
        checkHeld();
        if (referred.count == 0) {
            for (var i = 0; i < chunkCount(); i++) {
                var length = chunkLength(i);
                System.arraycopy(chunk(i), 0, target, offset, length);
                offset += length;
            }
            return offset;
        }
        for (var piece = 0; piece < pieces(); piece++) {
            var length = pieceLength(piece);
            System.arraycopy(pieceArray(piece), pieceOffset(piece), target, offset, length);
            offset += length;
        }
        return offset;
    }

    /// {@return the pieces of a chunked body that refers to values, as its bytes stand, or `null` for any other body}
    private Spread spread() {
        var pooled = this.pooled;
        if (pooled == null || pooled.chunks == null || referred.count == 0) {
            return null;
        }
        var spread = pooled.spread;
        // A write adds at least its kind byte, so the length tells whether the pieces still stand
        if (spread == null || spread.size != size()) {
            spread = new Spread(this);
            pooled.spread = spread;
        }
        return spread;
    }

    /// {@return the bytes of the body, gathered into an array of their length}
    ///
    /// @throws IllegalStateException if the body is released
    byte[] toBytes() {
        var bytes = new byte[size()];
        copyTo(bytes, 0);
        return bytes;
    }

    /// Checks that a write of `payload` bytes of keys and values fits in the body.
    ///
    /// @throws IllegalArgumentException if the write could take the body past [#MAX_BYTES]
    void checkRoom(long payload) {
        // The body is within the limit, so the subtraction cannot overflow
        if (payload > MAX_BYTES - WRITE_FIELDS - (long) size()) {
            throw new IllegalArgumentException("A commit takes at most " + MAX_BYTES + " bytes of the log, large values aside");
        }
    }

    void put(int keyspace, MemorySegment key, MemorySegment value) {
        var keyLength = key.byteSize();
        var valueLength = value.byteSize();
        if (data.length - used < WRITE_FIELDS + keyLength + valueLength) {
            putChecked(keyspace, key, value);
            return;
        }
        data[used++] = PUT;
        putVarInt(keyspace);
        putSegment(key, (int) keyLength);
        putSegment(value, (int) valueLength);
    }

    /// Records a put as [#put(int, MemorySegment, MemorySegment)] does, checking the room for each field: for a write
    /// that may grow the array or go on in the next chunk.
    private void putChecked(int keyspace, MemorySegment key, MemorySegment value) {
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeSegment(value);
    }

    /// Records a put whose value is the `length` bytes at `offset` of `array`, referring to them if they are at least
    /// [#referable()] bytes and copying them otherwise.
    ///
    /// @param array an array no one writes from now on and the store's pool never hands out
    void put(int keyspace, MemorySegment key, byte[] array, int offset, int length) {
        var keyLength = key.byteSize();
        var copied = length < referred.referable;
        if (data.length - used < WRITE_FIELDS + keyLength + (copied ? length : 0)) {
            putChecked(keyspace, key, array, offset, length);
            return;
        }
        data[used++] = PUT;
        putVarInt(keyspace);
        putSegment(key, (int) keyLength);
        putVarInt(length);
        if (copied) {
            System.arraycopy(array, offset, data, used, length);
            used += length;
            return;
        }
        refer(array, offset, length);
    }

    /// Records a put as [#put(int, MemorySegment, byte\[\], int, int)] does, checking the room for each field.
    private void putChecked(int keyspace, MemorySegment key, byte[] array, int offset, int length) {
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        if (length < referred.referable) {
            writeBytes(array, offset, length);
            return;
        }
        refer(array, offset, length);
    }

    /// Records the `length` bytes at `offset` of `array` as the next bytes of the body, by reference.
    private void refer(byte[] array, int offset, int length) {
        var references = referred;
        var at = ownBytes();
        if (references.count == 0) {
            referred = new References(references.referable, array, at, offset, length);
        } else {
            references.add(array, at, offset, length);
        }
    }

    /// The values a body refers to, in order, and the shortest it refers to.
    private static final class References {
        // What a body that refers to no value holds, by the shortest value it would refer to; never changed
        private static final References NONE = new References(NOT_REFERABLE);
        private static final References RUN = new References(RUN_REFERABLE);
        private static final References SEALED = new References(SEALED_REFERABLE);

        // The shortest value the body refers to
        private final int referable;
        // The values: their number and their bytes
        private int count;
        private int bytes;
        // The first, in fields of its own, as the body of a commit of one value has no other: its array, where in the
        // body's own bytes its value belongs, counted over the chunks, and where in its array the value starts and how
        // long it is
        private final byte[] firstArray;
        private final int firstAt;
        private final int firstOffset;
        private final int firstLength;
        // The references after the first, in order, or null before the second
        private byte[][] laterArrays;
        private int[] laterSpans;

        /// Makes the references of a body that refers to no value yet.
        private References(int referable) {
            this.referable = referable;
            this.firstArray = null;
            this.firstAt = 0;
            this.firstOffset = 0;
            this.firstLength = 0;
        }

        /// Makes the references of a body whose first reference is the `length` bytes at `offset` of `array`, which
        /// belong at `at` of the body's own bytes.
        References(int referable, byte[] array, int at, int offset, int length) {
            this.referable = referable;
            this.firstArray = array;
            this.firstAt = at;
            this.firstOffset = offset;
            this.firstLength = length;
            this.count = 1;
            this.bytes = length;
        }

        /// {@return the references of a body that refers to no value yet, and to the values of at least `referable`
        /// bytes later}
        static References none(int referable) {
            return switch (referable) {
                case NOT_REFERABLE -> NONE;
                case RUN_REFERABLE -> RUN;
                case SEALED_REFERABLE -> SEALED;
                default -> new References(referable);
            };
        }

        /// Records the reference after the first, whose value belongs at `at` of the body's own bytes.
        void add(byte[] array, int at, int offset, int length) {
            var later = count - 1;
            if (laterArrays == null) {
                laterArrays = new byte[LATER_REFERENCES][];
                laterSpans = new int[LATER_REFERENCES * SPAN_FIELDS];
            } else if (later == laterArrays.length) {
                laterArrays = Arrays.copyOf(laterArrays, later * 2);
                laterSpans = Arrays.copyOf(laterSpans, later * 2 * SPAN_FIELDS);
            }
            laterArrays[later] = array;
            var span = later * SPAN_FIELDS;
            laterSpans[span] = at;
            laterSpans[span + 1] = offset;
            laterSpans[span + 2] = length;
            count++;
            bytes += length;
        }

        /// {@return the array of reference `reference`}
        byte[] array(int reference) {
            return reference == 0 ? firstArray : laterArrays[reference - 1];
        }

        /// {@return where in the body's own bytes the value of reference `reference` belongs}
        int at(int reference) {
            return reference == 0 ? firstAt : laterSpans[(reference - 1) * SPAN_FIELDS];
        }

        /// {@return where the value of reference `reference` starts in its array}
        int offset(int reference) {
            return reference == 0 ? firstOffset : laterSpans[(reference - 1) * SPAN_FIELDS + 1];
        }

        /// {@return the bytes of the value of reference `reference`}
        int length(int reference) {
            return reference == 0 ? firstLength : laterSpans[(reference - 1) * SPAN_FIELDS + 2];
        }
    }

    /// Records a put whose value is the unit `ref` of [BlobFile], holding `length` bytes.
    void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
        var keyLength = key.byteSize();
        if (data.length - used < WRITE_FIELDS + keyLength + PageRef.BYTES) {
            putExternalChecked(keyspace, key, length, ref);
            return;
        }
        data[used++] = EXTERNAL;
        putVarInt(keyspace);
        putSegment(key, (int) keyLength);
        putVarInt(length);
        ref.write(data, used);
        used += PageRef.BYTES;
    }

    /// Records a put of a unit as [#putExternal(int, MemorySegment, long, PageRef)] does, checking the room for each
    /// field.
    private void putExternalChecked(int keyspace, MemorySegment key, long length, PageRef ref) {
        writeByte(EXTERNAL);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        if (data.length - used >= PageRef.BYTES || grow(PageRef.BYTES)) {
            ref.write(data, used);
            used += PageRef.BYTES;
        } else {
            // The reference spans two chunks
            var bytes = new byte[PageRef.BYTES];
            ref.write(bytes, 0);
            writeBytes(bytes, 0, bytes.length);
        }
    }

    /// {@return the body of an epoch frame for `epoch`}
    static FrameBody epoch(int epoch) {
        var body = new FrameBody();
        body.writeVarInt(Integer.toUnsignedLong(epoch));
        return body;
    }

    /// {@return the epoch an epoch body holds}
    ///
    /// @throws LedgerCorruptedException if the body is malformed
    static int readEpoch(byte[] body) {
        var reader = new Reader(body);
        var epoch = reader.readVarInt();
        if (reader.position != body.length || epoch > 0xFFFF_FFFFL) {
            throw new LedgerCorruptedException("A log frame holds a malformed epoch", null);
        }
        return (int) epoch;
    }

    /// {@return the bytes a commit body of `length` bytes takes in a run body: its length, then its bytes}
    static int runEntryBytes(int length) {
        return varIntBytes(length) + length;
    }

    /// Writes this commit body, whose own bytes are in one array, into `target` at `offset` as an entry of a run body,
    /// its length then its bytes, the values it refers to gathered.
    ///
    /// @return the offset after the entry, `offset` plus [#runEntryBytes(int)] of [#size()]
    /// @throws IllegalStateException if the body is chunked or released
    int writeRunEntry(byte[] target, int offset) {
        checkOneArray();
        var value = size();
        while ((value & ~0x7F) != 0) {
            target[offset++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        target[offset++] = (byte) value;
        return copyTo(target, offset);
    }

    /// {@return the bytes of `value` as an unsigned LEB128 varint}
    private static int varIntBytes(int value) {
        return value == 0 ? 1 : (38 - Integer.numberOfLeadingZeros(value)) / 7;
    }

    void delete(int keyspace, MemorySegment key) {
        var keyLength = key.byteSize();
        if (data.length - used < WRITE_FIELDS + keyLength) {
            deleteChecked(keyspace, key);
            return;
        }
        data[used++] = DELETE;
        putVarInt(keyspace);
        putSegment(key, (int) keyLength);
    }

    /// Records a removal as [#delete(int, MemorySegment)] does, checking the room for each field.
    private void deleteChecked(int keyspace, MemorySegment key) {
        writeByte(DELETE);
        writeVarInt(keyspace);
        writeSegment(key);
    }

    void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
        writeByte(RANGE);
        writeVarInt(keyspace);
        writeOptional(from);
        writeOptional(to);
    }

    void catalog(int keyspace, String name) {
        writeVarInt(keyspace);
        var bytes = name.getBytes(StandardCharsets.UTF_8);
        writeVarInt(bytes.length);
        writeBytes(bytes, 0, bytes.length);
    }

    /// {@return the bytes of `bound`, or 0 if it is `null`}
    static long boundBytes(MemorySegment bound) {
        return bound == null ? 0 : bound.byteSize();
    }

    private void writeOptional(MemorySegment bound) {
        if (bound == null) {
            writeVarInt(0);
        } else {
            writeSegment(bound);
        }
    }

    private void writeSegment(MemorySegment segment) {
        var length = Math.toIntExact(segment.byteSize());
        writeVarInt(length);
        if (length > data.length - used && !grow(length)) {
            writeSpanning(segment, length);
            return;
        }
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, used, length);
        used += length;
    }

    /// Writes the `length` bytes of `segment` across chunks, filling the last one first.
    private void writeSpanning(MemorySegment segment, int length) {
        for (var offset = 0; offset < length; ) {
            // A full chunk goes on in the next one first, so no copy is empty
            if (used == data.length) {
                nextChunk();
            }
            var part = Math.min(length - offset, data.length - used);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, data, used, part);
            used += part;
            offset += part;
        }
    }

    /// Writes the `length` bytes at `offset` of `bytes`.
    private void writeBytes(byte[] bytes, int offset, int length) {
        if (length > data.length - used && !grow(length)) {
            // Across chunks, filling the last one first
            for (var end = offset + length; offset < end; ) {
                if (used == data.length) {
                    nextChunk();
                }
                var part = Math.min(end - offset, data.length - used);
                System.arraycopy(bytes, offset, data, used, part);
                used += part;
                offset += part;
            }
            return;
        }
        System.arraycopy(bytes, offset, data, used, length);
        used += length;
    }

    /// Writes `value` as a varint where `data` has room for ten bytes after the ones written.
    private void putVarInt(long value) {
        while ((value & ~0x7FL) != 0) {
            data[used++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        data[used++] = (byte) value;
    }

    /// Writes the `length` bytes of `segment`, after their length, where `data` has room for them.
    private void putSegment(MemorySegment segment, int length) {
        putVarInt(length);
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, used, length);
        used += length;
    }

    private void writeByte(byte value) {
        if (used == data.length && !grow(1)) {
            nextChunk();
        }
        data[used++] = value;
    }

    private void writeVarInt(long value) {
        if (data.length - used < 10) {
            // Byte by byte, as the number may span two chunks
            while ((value & ~0x7FL) != 0) {
                writeByte((byte) ((value & 0x7F) | 0x80));
                value >>>= 7;
            }
            writeByte((byte) value);
            return;
        }
        putVarInt(value);
    }

    /// Makes room for `extra` bytes past the ones written, which `data` lacks.
    ///
    /// @return whether `data` now has them after the ones written; `false` for a chunked body, whose writer fills
    ///         `data` and goes on in the next chunk
    private boolean grow(int extra) {
        var needed = (long) used + extra;
        var pooled = this.pooled;
        if (pooled == null) {
            data = Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 8, Math.max(needed, data.length * 2L)));
            return true;
        }
        if (pooled.chunks != null) {
            return false;
        }
        if (needed <= CHUNK_BYTES) {
            data = Arrays.copyOf(data, (int) Math.min(CHUNK_BYTES, Math.max(needed, data.length * 2L)));
            return true;
        }
        // The bytes so far move to the first chunk once, where doubling would copy them again at each step; where the
        // references record their values belong stays where it was
        if (data.length != CHUNK_BYTES) {
            var first = pooled.takeChunk();
            System.arraycopy(data, 0, first, 0, used);
            data = first;
        }
        pooled.chunks = new byte[8][];
        pooled.chunks[0] = data;
        pooled.chunkCount = 1;
        return false;
    }

    /// Starts the next chunk, once `data`, the last one, is full.
    private void nextChunk() {
        assert used == CHUNK_BYTES : "a chunk left with " + (CHUNK_BYTES - used) + " bytes free";
        var pooled = this.pooled;
        pooled.base = Math.addExact(pooled.base, CHUNK_BYTES);
        if (pooled.chunkCount == pooled.chunks.length) {
            pooled.chunks = Arrays.copyOf(pooled.chunks, pooled.chunkCount * 2);
        }
        data = pooled.takeChunk();
        pooled.chunks[pooled.chunkCount++] = data;
        used = 0;
    }

    /// The pool of a body made with one, its chunks, and who owns them.
    private static final class Pooled {
        private final HeapPool pool;
        // The bytes of the full chunks before the body's last array, a multiple of CHUNK_BYTES
        private int base;
        // The chunks, the body's last array last, in the first chunkCount slots; null until the body passes CHUNK_BYTES
        private byte[][] chunks;
        private int chunkCount;
        // OPEN, HANDED or RELEASED; written by the body's writer until it hands the body off, then by the log alone
        private byte state;
        // The pieces of a chunked body that refers to values, as of its length then, or null
        private Spread spread;
        // The pool's hold on its overflow from the body's first chunk until its release, or null: the chunks of the
        // frame before it, which went back to the overflow as it was sealed, are this body's next ones, and those of a
        // large commit's next frame once this one is sealed
        private Object hold;

        Pooled(HeapPool pool) {
            this.pool = pool;
        }

        /// {@return a chunk of the pool, of exactly [#CHUNK_BYTES]}
        ///
        /// The first holds the pool's overflow for the body.
        byte[] takeChunk() {
            if (hold == null) {
                hold = pool.hold();
            }
            var chunk = pool.acquire(CHUNK_BYTES);
            if (chunk.length != CHUNK_BYTES) {
                throw new IllegalStateException("The pool gave a chunk of " + chunk.length + " bytes");
            }
            return chunk;
        }
    }

    /// The pieces of a chunked body that refers to values: its own runs split at the ends of its chunks, and the values
    /// it refers to, in order, with no empty piece.
    private static final class Spread {
        // The body's length the pieces are of
        private final int size;
        private final byte[][] arrays;
        private final int[] offsets;
        private final int[] lengths;
        private int count;

        Spread(FrameBody body) {
            var pooled = body.pooled;
            var references = body.referred;
            // A run of the body's own bytes takes a piece per chunk it touches, and each chunk end splits one run
            var most = 2 * references.count + pooled.chunkCount + 1;
            this.arrays = new byte[most][];
            this.offsets = new int[most];
            this.lengths = new int[most];
            var from = 0;
            for (var reference = 0; reference < references.count; reference++) {
                var at = references.at(reference);
                addOwn(pooled, from, at);
                add(references.array(reference), references.offset(reference), references.length(reference));
                from = at;
            }
            addOwn(pooled, from, pooled.base + body.used);
            this.size = body.size();
        }

        /// Adds the body's own bytes from `from` to `to`, a piece per chunk.
        private void addOwn(Pooled pooled, int from, int to) {
            while (from < to) {
                var offset = from % CHUNK_BYTES;
                var length = Math.min(to - from, CHUNK_BYTES - offset);
                add(pooled.chunks[from / CHUNK_BYTES], offset, length);
                from += length;
            }
        }

        private void add(byte[] array, int offset, int length) {
            if (length == 0) {
                return;
            }
            arrays[count] = array;
            offsets[count] = offset;
            lengths[count] = length;
            count++;
        }
    }

    /// Reads a commit body, passing each write to `visitor`.
    ///
    /// The segments the visitor receives are read-only slices of `body`, which the caller must not change afterward.
    ///
    /// @throws LedgerCorruptedException if the body is malformed
    static void readCommit(byte[] body, Visitor visitor) {
        var reader = new Reader(body);
        while (reader.position < body.length) {
            var kind = reader.readByte();
            var keyspace = Math.toIntExact(reader.readVarInt());
            switch (kind) {
                case PUT -> visitor.put(keyspace, reader.readSegment(), reader.readSegment());
                case DELETE -> visitor.delete(keyspace, reader.readSegment());
                case RANGE -> visitor.deleteRange(keyspace, reader.readOptional(), reader.readOptional());
                case EXTERNAL -> visitor.putExternal(keyspace, reader.readSegment(), reader.readVarInt(), reader.readRef());
                default -> throw new LedgerCorruptedException("A log frame holds an unknown write kind " + kind, null);
            }
        }
    }

    /// Reads a catalog body, passing each entry to `visitor`.
    ///
    /// @throws LedgerCorruptedException if the body is malformed
    static void readCatalog(byte[] body, CatalogVisitor visitor) {
        var reader = new Reader(body);
        while (reader.position < body.length) {
            var keyspace = Math.toIntExact(reader.readVarInt());
            var length = Math.toIntExact(reader.readVarInt());
            reader.check(length);
            visitor.entry(keyspace, new String(body, reader.position, length, StandardCharsets.UTF_8));
            reader.position += length;
        }
    }

    /// The commit bodies of a run body, read one at a time.
    static final class RunReader {
        private final Reader reader;
        private final int length;

        /// Reads the run body `run`.
        ///
        /// @throws LedgerCorruptedException if it holds no commit
        RunReader(byte[] run) {
            if (run.length == 0) {
                throw new LedgerCorruptedException("A log frame holds a run of no commits", null);
            }
            this.reader = new Reader(run);
            this.length = run.length;
        }

        /// {@return whether a commit follows the ones read}
        boolean hasNext() {
            return reader.position < length;
        }

        /// {@return a copy of the body of the next commit, an array of its own as the body of a commit frame is}
        ///
        /// @throws LedgerCorruptedException if the entry is malformed or runs past the run
        byte[] next() {
            var size = (int) reader.readLength();
            var body = Arrays.copyOfRange(reader.body, reader.position, reader.position + size);
            reader.position += size;
            return body;
        }

        /// Passes over the body of the next commit.
        ///
        /// @throws LedgerCorruptedException if the entry is malformed or runs past the run
        void skip() {
            var size = (int) reader.readLength();
            reader.position += size;
        }
    }

    /// Receives the writes of a commit body.
    interface Visitor {
        void put(int keyspace, MemorySegment key, MemorySegment value);

        void delete(int keyspace, MemorySegment key);

        void deleteRange(int keyspace, MemorySegment from, MemorySegment to);

        void putExternal(int keyspace, MemorySegment key, long length, PageRef ref);
    }

    /// Receives the entries of a catalog body.
    @FunctionalInterface
    interface CatalogVisitor {
        void entry(int keyspace, String name);
    }

    /// Reads the fields of a body, checking every length against what is left.
    private static final class Reader {
        private final byte[] body;
        private final MemorySegment memory;
        private int position;

        Reader(byte[] body) {
            this.body = body;
            this.memory = MemorySegment.ofArray(body).asReadOnly();
        }

        void check(long length) {
            if (length < 0 || length > body.length - position) {
                throw new LedgerCorruptedException("A log frame holds a field longer than the frame", null);
            }
        }

        byte readByte() {
            check(1);
            return body[position++];
        }

        long readVarInt() {
            var result = 0L;
            for (var shift = 0; shift < 64; shift += 7) {
                var next = readByte();
                result |= (long) (next & 0x7F) << shift;
                if ((next & 0x80) == 0) {
                    return result;
                }
            }
            throw new LedgerCorruptedException("A log frame holds a malformed number", null);
        }

        /// {@return a length, checked against what is left}
        long readLength() {
            var length = readVarInt();
            check(length);
            return length;
        }

        MemorySegment readSegment() {
            var length = readLength();
            var segment = memory.asSlice(position, length);
            position += (int) length;
            return segment;
        }

        PageRef readRef() {
            check(PageRef.BYTES);
            var ref = PageRef.read(body, position);
            position += PageRef.BYTES;
            return ref;
        }

        MemorySegment readOptional() {
            var segment = readSegment();
            return segment.byteSize() == 0 ? null : segment;
        }
    }
}
