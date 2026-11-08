package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.Keyspace;
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
/// A catalog body is entries of an id and a name; an epoch body is the epoch. Numbers are unsigned LEB128 varints;
/// keys are never empty, so a bound of length 0 is an absent bound.
///
/// A body holds what it copies in an array of its own and refers to the value of a put of at least
/// [#MIN_REFERENCED] bytes that the store's version keeps anyway: during an ingest, the log's queue would hold a second
/// copy of every queued value, up to its whole limit, in arrays large enough for the collector to give them regions of
/// their own. The log writer reads the [pieces][#pieces()] in order and writes the bytes the format above says, with
/// no trace of where they were. A referenced array is one no one writes and the pool of the store never hands out, so
/// a frame may stay queued while the batch it came from starts again on another array.
///
/// A body is at most [#MAX_BYTES] long: a write that would pass it throws before it changes the body, on the thread
/// that commits, which takes its commit back. Only the log writer would find out otherwise, past the commit's return,
/// when all it can do is stop the store.
final class FrameBody {
    /// A frame holding a commit.
    static final byte COMMIT = 1;
    /// A frame naming key spaces.
    static final byte CATALOG = 2;
    /// A frame holding the epoch of an open, durable before the open writes a unit of [BlobFile].
    static final byte EPOCH = 3;

    private static final byte PUT = 0;
    private static final byte DELETE = 1;
    private static final byte RANGE = 2;
    private static final byte EXTERNAL = 3;
    // The ints of spans per reference
    private static final int SPAN_FIELDS = 3;
    // The references a body with no estimate of them has room for at its first
    private static final int INITIAL_REFERENCES = 4;
    /// The shortest value a body refers to rather than copies: a reference takes 16 bytes of the body's tables, and a
    /// piece more for the writer to gather.
    static final int MIN_REFERENCED = 256;
    /// The longest body: the longest whose frame AES-GCM can seal and recovery can read into one array, which the JVM
    /// limits to a few bytes under 2 GiB, with the frame's header and its tag or checksum.
    static final int MAX_BYTES = Integer.MAX_VALUE - 8 - LogWriter.FRAME_HEADER - Math.max(LogWriter.TAG_BYTES, Integer.BYTES);
    /// The most a write takes beside its keys and value, which [#checkRoom(long)] counts: a kind and three numbers of up
    /// to ten bytes.
    static final int WRITE_FIELDS = 1 + 3 * 10;

    // The bytes the body holds itself
    private byte[] data;
    private int used;
    // The arrays of the referenced values, in the order of the body, in the first `references` slots; null before any
    private byte[][] referenced;
    // Per reference: where in data the value belongs, and where in its array it starts and how long it is
    private int[] spans;
    private int references;
    private int referencedBytes;

    FrameBody() {
        this(256);
    }

    /// Makes a body with room for `capacity` bytes before it grows.
    FrameBody(int capacity) {
        this.data = new byte[Math.max(16, capacity)];
    }

    /// {@return an empty body for a commit of `writes`, with room for what it will copy, its large values in their
    /// units, and for its references, near enough that it seldom grows}
    ///
    /// A body that doubled from a small array allocated about twice its bytes, and kept up to twice as many in the
    /// log's queue as the queue counts.
    static FrameBody forCommit(ArrayWriteBatch writes, PageRef[] units) {
        var bytes = 16L;
        var references = 0;
        for (var i = 0; i < writes.size(); i++) {
            // The kind, the key space and the lengths, as variable-length numbers
            bytes += 16 + writes.spanBytes(i);
            if (units != null && units[i] != null) {
                // The value is in its unit
                bytes += PageRef.BYTES - writes.valueBytes(i);
            } else if (refersToValue(writes, i)) {
                bytes -= writes.valueBytes(i);
                references++;
            }
        }
        var body = new FrameBody((int) Math.min(bytes, 64L << 20));
        if (references > 0) {
            body.referenced = new byte[references][];
            body.spans = new int[references * SPAN_FIELDS];
        }
        return body;
    }

    /// {@return whether the store will keep the value of write `index` of `writes` on an array a body may refer to, and
    /// the value is long enough for that}
    ///
    /// Only the size of the arrays the body starts with depends on it, so a wrong answer costs a growth of them.
    private static boolean refersToValue(ArrayWriteBatch writes, int index) {
        if (writes.kind(index) != ArrayWriteBatch.PUT || writes.valueBytes(index) < MIN_REFERENCED) {
            return false;
        }
        var keyspace = writes.keyspace(index);
        return keyspace.residency() == Keyspace.Residency.RESIDENT
                ? keyspace.values() != BuiltinCodecs.BYTES_VALUE
                : writes.home(index) != null;
    }

    /// {@return the number of pieces whose bytes, in order, are the body, each described by [#pieceArray(int)],
    /// [#pieceOffset(int)] and [#pieceLength(int)]}
    int pieces() {
        return 2 * references + 1;
    }

    /// {@return the array piece `piece` is part of, which no one writes while the body is queued}
    byte[] pieceArray(int piece) {
        return (piece & 1) == 0 ? data : referenced[piece >> 1];
    }

    /// {@return where piece `piece` starts in its array}
    int pieceOffset(int piece) {
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return spans[reference * SPAN_FIELDS + 1];
        }
        return reference == 0 ? 0 : spans[(reference - 1) * SPAN_FIELDS];
    }

    /// {@return the bytes of piece `piece`, maybe 0}
    int pieceLength(int piece) {
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return spans[reference * SPAN_FIELDS + 2];
        }
        return (reference == references ? used : spans[reference * SPAN_FIELDS]) - pieceOffset(piece);
    }

    /// {@return the length of the body, the values it refers to included, at most [#MAX_BYTES]}
    int size() {
        return used + referencedBytes;
    }

    /// Checks that a write of `payload` bytes of keys and values fits in the body, which it leaves as it was if not.
    ///
    /// @throws IllegalArgumentException if the write could take the body past [#MAX_BYTES]
    void checkRoom(long payload) {
        // The body is within the limit, so this subtraction cannot overflow
        if (payload > MAX_BYTES - WRITE_FIELDS - referencedBytes - used) {
            throw new IllegalArgumentException("A commit takes at most " + MAX_BYTES + " bytes of the log, large values aside");
        }
    }

    /// Records a put, copying its value.
    ///
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void put(int keyspace, MemorySegment key, MemorySegment value) {
        checkRoom(key.byteSize() + value.byteSize());
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeSegment(value);
    }

    /// Records a put whose value is the `length` bytes at `offset` of `array`, referring to them if they are at least
    /// [#MIN_REFERENCED].
    ///
    /// @param array an array no one writes from now on and the store's pool never hands out
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void put(int keyspace, MemorySegment key, byte[] array, int offset, int length) {
        checkRoom(key.byteSize() + length);
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        if (length < MIN_REFERENCED) {
            ensure(length);
            System.arraycopy(array, offset, data, used, length);
            used += length;
            return;
        }
        referencedBytes += length;
        if (referenced == null) {
            referenced = new byte[INITIAL_REFERENCES][];
            spans = new int[INITIAL_REFERENCES * SPAN_FIELDS];
        } else if (references == referenced.length) {
            referenced = Arrays.copyOf(referenced, references * 2);
            spans = Arrays.copyOf(spans, references * 2 * SPAN_FIELDS);
        }
        referenced[references] = array;
        var span = references * SPAN_FIELDS;
        spans[span] = used;
        spans[span + 1] = offset;
        spans[span + 2] = length;
        references++;
    }

    /// Records a put whose value is the unit `ref` of [BlobFile], holding `length` bytes.
    ///
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
        checkRoom(key.byteSize() + PageRef.BYTES);
        writeByte(EXTERNAL);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        ensure(PageRef.BYTES);
        ref.write(data, used);
        used += PageRef.BYTES;
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

    /// Records a removal of `key`.
    ///
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void delete(int keyspace, MemorySegment key) {
        checkRoom(key.byteSize());
        writeByte(DELETE);
        writeVarInt(keyspace);
        writeSegment(key);
    }

    /// Records a removal of a range, with `null` for an absent bound.
    ///
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void deleteRange(int keyspace, MemorySegment from, MemorySegment to) {
        checkRoom(boundBytes(from) + boundBytes(to));
        writeByte(RANGE);
        writeVarInt(keyspace);
        writeOptional(from);
        writeOptional(to);
    }

    /// Records the name of a key space.
    ///
    /// @throws IllegalArgumentException as [#checkRoom(long)] does
    void catalog(int keyspace, String name) {
        var bytes = name.getBytes(StandardCharsets.UTF_8);
        checkRoom(bytes.length);
        writeVarInt(keyspace);
        writeVarInt(bytes.length);
        ensure(bytes.length);
        System.arraycopy(bytes, 0, data, used, bytes.length);
        used += bytes.length;
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
        ensure(length);
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, used, length);
        used += length;
    }

    private void writeByte(byte value) {
        ensure(1);
        data[used++] = value;
    }

    private void writeVarInt(long value) {
        ensure(10);
        while ((value & ~0x7FL) != 0) {
            data[used++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        data[used++] = (byte) value;
    }

    /// Makes room in the array for `extra` more bytes, which [#checkRoom(long)] has allowed.
    private void ensure(int extra) {
        if (used + extra > data.length) {
            data = Arrays.copyOf(data, (int) Math.min(Math.max(used + extra, data.length * 2L), MAX_BYTES));
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

        MemorySegment readSegment() {
            var length = readVarInt();
            check(length);
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
