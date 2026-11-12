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
    /// The most bytes a write takes beside its keys and value, which [#checkRoom(long)] counts: a kind and three
    /// numbers of up to ten bytes.
    static final int WRITE_FIELDS = 1 + 3 * 10;

    private static final byte PUT = 0;
    private static final byte DELETE = 1;
    private static final byte RANGE = 2;
    private static final byte EXTERNAL = 3;
    // The ints of laterSpans per reference: where in data the value belongs, where it starts in its array, its length
    private static final int SPAN_FIELDS = 3;
    // The references after the first a body makes room for when it needs a second
    private static final int LATER_REFERENCES = 4;

    // The bytes the body holds itself, in the first `used`
    private byte[] data;
    private int used;
    // The shortest value the body refers to
    private final int referable;
    // The values the body refers to: their number and their bytes
    private int references;
    private int referencedBytes;
    // The first reference, in fields of its own, as the body of a commit of one value has no other: its array, where in
    // data its value belongs, and where in its array the value starts and how long it is
    private byte[] firstArray;
    private int firstAt;
    private int firstOffset;
    private int firstLength;
    // The references after the first, in order, or null before the second
    private byte[][] laterArrays;
    private int[] laterSpans;

    FrameBody() {
        this(256);
    }

    /// Makes a body that refers to no value, with room for `capacity` bytes before it grows.
    FrameBody(int capacity) {
        this(capacity, NOT_REFERABLE);
    }

    /// Makes a body with room for `capacity` bytes of its own before it grows, which refers to the values of at least
    /// `referable` bytes a put offers on an array.
    FrameBody(int capacity, int referable) {
        this.data = new byte[Math.max(16, capacity)];
        this.referable = referable;
    }

    /// {@return the empty body of the frame of a transaction, which refers to its values of [#SEALED_REFERABLE]
    /// bytes or more}
    static FrameBody forTransaction() {
        return new FrameBody(256, SEALED_REFERABLE);
    }

    /// {@return the empty body of the frame of a commit of `writes`, whose large values are in their `units`, with
    /// room for what it will copy, near enough that it seldom grows}
    ///
    /// A commit whose body takes at most [LogWriter#RUN_MAX_BODY] bytes by [#capacityFor(ArrayWriteBatch, PageRef\[\])],
    /// which never estimates less than a body takes, refers to its values of [#RUN_REFERABLE] bytes or more, which its
    /// room leaves out: every put of a batch offers its value on an array, but one whose value is in its unit. Such a
    /// commit has few writes, and only one with a value that long visits them. The body of any other commit refers to
    /// nothing, as the writer seals it slice by slice from the body itself.
    static FrameBody forCommit(ArrayWriteBatch writes, PageRef[] units) {
        var capacity = capacityFor(writes, units);
        if (capacity > LogWriter.RUN_MAX_BODY) {
            return new FrameBody(capacity, NOT_REFERABLE);
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

    /// {@return the bytes of a body that refers to no value; only the first [#size()] are meaningful}
    byte[] array() {
        assert references == 0 : "the body refers to values";
        return data;
    }

    /// {@return the length of the body, the values it refers to included}
    int size() {
        return used + referencedBytes;
    }

    /// {@return the shortest value the body refers to rather than copies, [#NOT_REFERABLE] for none}
    int referable() {
        return referable;
    }

    /// {@return the number of values the body refers to}
    int references() {
        return references;
    }

    /// {@return the number of pieces whose bytes, in order, are the body: a run of its own bytes, then for each value
    /// it refers to that value and the run after it}
    int pieces() {
        return 2 * references + 1;
    }

    /// {@return the array piece `piece` is part of, which no one writes while the body is queued}
    byte[] pieceArray(int piece) {
        return (piece & 1) == 0 ? data : referenceArray(piece >> 1);
    }

    /// {@return where piece `piece` starts in its array}
    int pieceOffset(int piece) {
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return reference == 0 ? firstOffset : laterSpans[(reference - 1) * SPAN_FIELDS + 1];
        }
        return reference == 0 ? 0 : referenceAt(reference - 1);
    }

    /// {@return the bytes of piece `piece`, maybe 0}
    int pieceLength(int piece) {
        var reference = piece >> 1;
        if ((piece & 1) != 0) {
            return reference == 0 ? firstLength : laterSpans[(reference - 1) * SPAN_FIELDS + 2];
        }
        return (reference == references ? used : referenceAt(reference)) - pieceOffset(piece);
    }

    /// {@return the array of reference `reference`}
    private byte[] referenceArray(int reference) {
        return reference == 0 ? firstArray : laterArrays[reference - 1];
    }

    /// {@return where in the body's own bytes the value of reference `reference` belongs}
    private int referenceAt(int reference) {
        return reference == 0 ? firstAt : laterSpans[(reference - 1) * SPAN_FIELDS];
    }

    /// Copies the body into `target` at `offset`, the values it refers to gathered.
    ///
    /// @return the offset after the body, `offset` plus [#size()]
    int copyTo(byte[] target, int offset) {
        if (references == 0) {
            System.arraycopy(data, 0, target, offset, used);
            return offset + used;
        }
        for (var piece = 0; piece < pieces(); piece++) {
            var length = pieceLength(piece);
            System.arraycopy(pieceArray(piece), pieceOffset(piece), target, offset, length);
            offset += length;
        }
        return offset;
    }

    /// {@return the bytes of the body, gathered into an array of their length}
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
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        if (length < referable) {
            ensure(length);
            System.arraycopy(array, offset, data, used, length);
            used += length;
            return;
        }
        refer(array, offset, length);
    }

    /// Records the `length` bytes at `offset` of `array` as the next bytes of the body, by reference.
    private void refer(byte[] array, int offset, int length) {
        if (references == 0) {
            firstArray = array;
            firstAt = used;
            firstOffset = offset;
            firstLength = length;
        } else {
            var later = references - 1;
            if (laterArrays == null) {
                laterArrays = new byte[LATER_REFERENCES][];
                laterSpans = new int[LATER_REFERENCES * SPAN_FIELDS];
            } else if (later == laterArrays.length) {
                laterArrays = Arrays.copyOf(laterArrays, later * 2);
                laterSpans = Arrays.copyOf(laterSpans, later * 2 * SPAN_FIELDS);
            }
            laterArrays[later] = array;
            var span = later * SPAN_FIELDS;
            laterSpans[span] = used;
            laterSpans[span + 1] = offset;
            laterSpans[span + 2] = length;
        }
        references++;
        referencedBytes += length;
    }

    /// Records a put whose value is the unit `ref` of [BlobFile], holding `length` bytes.
    void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
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

    /// {@return the bytes a commit body of `length` bytes takes in a run body: its length, then its bytes}
    static int runEntryBytes(int length) {
        return varIntBytes(length) + length;
    }

    /// Writes this commit body into `target` at `offset` as an entry of a run body, its length then its bytes, the
    /// values it refers to gathered.
    ///
    /// @return the offset after the entry, `offset` plus [#runEntryBytes(int)] of [#size()]
    int writeRunEntry(byte[] target, int offset) {
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

    private void ensure(int extra) {
        if (used + extra > data.length) {
            data = Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 8, Math.max((long) used + extra, data.length * 2L)));
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
