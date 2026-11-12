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
/// A body made with a [HeapPool] keeps its bytes past [#CHUNK_BYTES] in chunks of that size from the pool, every one
/// but the last full, rather than in one array of megabytes that the collector would give regions of their own and that
/// the log reads once. The log hands the chunks back once it sealed the frame ([#release()]); a body the log never took
/// goes back with [#discard()]. A chunked body is read through [#chunkCount()], [#chunk(int)] and [#chunkLength(int)];
/// [#array()] serves only a body of one array.
///
/// A write checks once that the array it writes has room for its longest encoding, and takes a path that checks each
/// field only when it may not: that path, which grows the array or goes on in the next chunk, runs at a chunk boundary
/// whichever field the boundary falls in, so the compiled write has no branch that only a rare alignment takes.
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

    private static final byte PUT = 0;
    private static final byte DELETE = 1;
    private static final byte RANGE = 2;
    private static final byte EXTERNAL = 3;
    // The most bytes a write takes beside its keys, value and unit reference: a kind and three numbers of up to ten
    // bytes each
    private static final int WRITE_FIELDS = 1 + 3 * 10;

    // The body is its writer's, which writes it or discards it
    private static final byte OPEN = 0;
    // The log owns the body, which only it releases
    private static final byte HANDED = 1;
    // The body's pooled arrays went back to the pool; nothing reads or writes it again
    private static final byte RELEASED = 2;

    // The pool chunks come from, or null for a body of one array that grows by doubling
    private final HeapPool pool;
    // The array written: the body's only one, or its last chunk
    private byte[] data;
    // The bytes of data written
    private int size;
    // The bytes of the full chunks before data, a multiple of CHUNK_BYTES; 0 for a body of one array
    private int base;
    // The chunks, data last, in the first chunkCount slots; null until the body passes CHUNK_BYTES
    private byte[][] chunks;
    // The chunks in use, the first slots of chunks
    private int chunkCount;
    // OPEN, HANDED or RELEASED; written by the body's writer until it hands the body off, then by the log alone
    private byte state;

    FrameBody() {
        this(256);
    }

    /// Makes a body of one array with room for `capacity` bytes before it grows.
    FrameBody(int capacity) {
        this.pool = null;
        this.data = new byte[Math.max(16, capacity)];
    }

    /// Makes a body with room for `capacity` bytes before it grows, which keeps its bytes past [#CHUNK_BYTES] in
    /// chunks from `pool`.
    ///
    /// A capacity of a chunk or more starts on a chunk of the pool; a smaller one on an array of its own, which grows
    /// to at most a chunk. Further chunks are taken as the bytes reach them, never ahead: [#capacityFor] overestimates.
    FrameBody(int capacity, HeapPool pool) {
        this.pool = pool;
        this.data = pool != null && capacity >= CHUNK_BYTES ? takeChunk() : new byte[Math.max(16, capacity)];
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

    /// {@return the bytes of a body of one array; only the first [#size()] are meaningful}
    ///
    /// @throws IllegalStateException if the body is chunked or released
    byte[] array() {
        if (chunks != null || state == RELEASED) {
            throw new IllegalStateException("The body is not one array");
        }
        return data;
    }

    /// {@return the length of the body}
    int size() {
        return base + size;
    }

    /// {@return the number of arrays the body's bytes are in: one, or its chunks}
    ///
    /// @throws IllegalStateException if the body is released
    int chunkCount() {
        checkHeld();
        return chunks == null ? 1 : chunkCount;
    }

    /// {@return the array `index` of the body's bytes, in order, below [#chunkCount()]}
    ///
    /// @throws IllegalStateException if the body is released
    byte[] chunk(int index) {
        checkHeld();
        return chunks == null ? data : chunks[index];
    }

    /// {@return the bytes of the body in the array `index`: [#CHUNK_BYTES] for every chunk but the last}
    ///
    /// @throws IllegalStateException if the body is released
    int chunkLength(int index) {
        checkHeld();
        return chunks == null || index == chunkCount - 1 ? size : CHUNK_BYTES;
    }

    /// Checks that the body still holds its bytes.
    ///
    /// @throws IllegalStateException if the body is released
    private void checkHeld() {
        if (state == RELEASED) {
            throw new IllegalStateException("The body is released");
        }
    }

    /// {@return a copy of the bytes of the body, gathered from its chunks; for tests}
    byte[] toArray() {
        var copy = new byte[size()];
        var offset = 0;
        for (var i = 0; i < chunkCount(); i++) {
            var length = chunkLength(i);
            System.arraycopy(chunk(i), 0, copy, offset, length);
            offset += length;
        }
        return copy;
    }

    /// Records that the log owns the body from now on, which then only it releases; called as the log takes it.
    void handOff() {
        state = HANDED;
    }

    /// Gives the body's pooled arrays back to its pool, once the log sealed its frame and never reads it again; a body
    /// made without a pool keeps its array. A second call does nothing.
    void release() {
        if (pool == null || state == RELEASED) {
            return;
        }
        state = RELEASED;
        if (chunks != null) {
            for (var i = 0; i < chunkCount; i++) {
                assert poison(chunks[i]);
                pool.release(chunks[i]);
            }
            chunks = null;
            chunkCount = 0;
        } else if (data.length == CHUNK_BYTES) {
            assert poison(data);
            pool.release(data);
        }
        data = null;
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
        if (state == OPEN) {
            release();
        }
    }

    void put(int keyspace, MemorySegment key, MemorySegment value) {
        var keyLength = key.byteSize();
        var valueLength = value.byteSize();
        if (data.length - size < WRITE_FIELDS + keyLength + valueLength) {
            putChecked(keyspace, key, value);
            return;
        }
        data[size++] = PUT;
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

    /// Records a put whose value is the unit `ref` of [BlobFile], holding `length` bytes.
    void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
        var keyLength = key.byteSize();
        if (data.length - size < WRITE_FIELDS + keyLength + PageRef.BYTES) {
            putExternalChecked(keyspace, key, length, ref);
            return;
        }
        data[size++] = EXTERNAL;
        putVarInt(keyspace);
        putSegment(key, (int) keyLength);
        putVarInt(length);
        ref.write(data, size);
        size += PageRef.BYTES;
    }

    /// Records a put of a unit as [#putExternal(int, MemorySegment, long, PageRef)] does, checking the room for each
    /// field.
    private void putExternalChecked(int keyspace, MemorySegment key, long length, PageRef ref) {
        writeByte(EXTERNAL);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        if (data.length - size >= PageRef.BYTES || grow(PageRef.BYTES)) {
            ref.write(data, size);
            size += PageRef.BYTES;
        } else {
            // The reference spans two chunks
            var bytes = new byte[PageRef.BYTES];
            ref.write(bytes, 0);
            writeBytes(bytes);
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

    /// Writes this commit body, of one array, into `target` at `offset` as an entry of a run body, its length then its
    /// bytes.
    ///
    /// @return the offset after the entry, `offset` plus [#runEntryBytes(int)] of [#size()]
    /// @throws IllegalStateException if the body is chunked or released
    int writeRunEntry(byte[] target, int offset) {
        var body = array();
        var value = size;
        while ((value & ~0x7F) != 0) {
            target[offset++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        target[offset++] = (byte) value;
        System.arraycopy(body, 0, target, offset, size);
        return offset + size;
    }

    /// {@return the bytes of `value` as an unsigned LEB128 varint}
    private static int varIntBytes(int value) {
        return value == 0 ? 1 : (38 - Integer.numberOfLeadingZeros(value)) / 7;
    }

    void delete(int keyspace, MemorySegment key) {
        var keyLength = key.byteSize();
        if (data.length - size < WRITE_FIELDS + keyLength) {
            deleteChecked(keyspace, key);
            return;
        }
        data[size++] = DELETE;
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
        writeBytes(bytes);
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
        if (length > data.length - size && !grow(length)) {
            writeSpanning(segment, length);
            return;
        }
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, size, length);
        size += length;
    }

    /// Writes the `length` bytes of `segment` across chunks, filling the last one first.
    private void writeSpanning(MemorySegment segment, int length) {
        for (var offset = 0; offset < length; ) {
            // A full chunk goes on in the next one first, so no copy is empty
            if (size == data.length) {
                nextChunk();
            }
            var part = Math.min(length - offset, data.length - size);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, data, size, part);
            size += part;
            offset += part;
        }
    }

    private void writeBytes(byte[] bytes) {
        var length = bytes.length;
        if (length > data.length - size && !grow(length)) {
            // Across chunks, filling the last one first
            for (var offset = 0; offset < length; ) {
                if (size == data.length) {
                    nextChunk();
                }
                var part = Math.min(length - offset, data.length - size);
                System.arraycopy(bytes, offset, data, size, part);
                size += part;
                offset += part;
            }
            return;
        }
        System.arraycopy(bytes, 0, data, size, length);
        size += length;
    }

    /// Writes `value` as a varint where `data` has room for ten bytes after the ones written.
    private void putVarInt(long value) {
        while ((value & ~0x7FL) != 0) {
            data[size++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        data[size++] = (byte) value;
    }

    /// Writes the `length` bytes of `segment`, after their length, where `data` has room for them.
    private void putSegment(MemorySegment segment, int length) {
        putVarInt(length);
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, size, length);
        size += length;
    }

    private void writeByte(byte value) {
        if (size == data.length && !grow(1)) {
            nextChunk();
        }
        data[size++] = value;
    }

    private void writeVarInt(long value) {
        if (data.length - size < 10) {
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
        var needed = (long) size + extra;
        if (pool == null) {
            data = Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 8, Math.max(needed, data.length * 2L)));
            return true;
        }
        if (chunks != null) {
            return false;
        }
        if (needed <= CHUNK_BYTES) {
            data = Arrays.copyOf(data, (int) Math.min(CHUNK_BYTES, Math.max(needed, data.length * 2L)));
            return true;
        }
        // The bytes so far move to the first chunk once, where doubling would copy them again at each step
        if (data.length != CHUNK_BYTES) {
            var first = takeChunk();
            System.arraycopy(data, 0, first, 0, size);
            data = first;
        }
        chunks = new byte[8][];
        chunks[0] = data;
        chunkCount = 1;
        return false;
    }

    /// Starts the next chunk, once `data`, the last one, is full.
    private void nextChunk() {
        assert size == CHUNK_BYTES : "a chunk left with " + (CHUNK_BYTES - size) + " bytes free";
        base = Math.addExact(base, CHUNK_BYTES);
        if (chunkCount == chunks.length) {
            chunks = Arrays.copyOf(chunks, chunkCount * 2);
        }
        data = takeChunk();
        chunks[chunkCount++] = data;
        size = 0;
    }

    /// {@return a chunk of the pool, of exactly [#CHUNK_BYTES]}
    private byte[] takeChunk() {
        var chunk = pool.acquire(CHUNK_BYTES);
        if (chunk.length != CHUNK_BYTES) {
            throw new IllegalStateException("The pool gave a chunk of " + chunk.length + " bytes");
        }
        return chunk;
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
