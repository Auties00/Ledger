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

    private static final byte PUT = 0;
    private static final byte DELETE = 1;
    private static final byte RANGE = 2;
    private static final byte EXTERNAL = 3;

    private byte[] data;
    private int size;

    FrameBody() {
        this(256);
    }

    /// Makes a body with room for `capacity` bytes before it grows.
    FrameBody(int capacity) {
        this.data = new byte[Math.max(16, capacity)];
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

    /// {@return the bytes of the body; only the first [#size()] are meaningful}
    byte[] array() {
        return data;
    }

    /// {@return the length of the body}
    int size() {
        return size;
    }

    void put(int keyspace, MemorySegment key, MemorySegment value) {
        writeByte(PUT);
        writeVarInt(keyspace);
        writeSegment(key);
        writeSegment(value);
    }

    /// Records a put whose value is the unit `ref` of [BlobFile], holding `length` bytes.
    void putExternal(int keyspace, MemorySegment key, long length, PageRef ref) {
        writeByte(EXTERNAL);
        writeVarInt(keyspace);
        writeSegment(key);
        writeVarInt(length);
        ensure(PageRef.BYTES);
        ref.write(data, size);
        size += PageRef.BYTES;
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

    /// Writes this commit body into `target` at `offset` as an entry of a run body, its length then its bytes.
    ///
    /// @return the offset after the entry, `offset` plus [#runEntryBytes(int)] of [#size()]
    int writeRunEntry(byte[] target, int offset) {
        var value = size;
        while ((value & ~0x7F) != 0) {
            target[offset++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        target[offset++] = (byte) value;
        System.arraycopy(data, 0, target, offset, size);
        return offset + size;
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
        System.arraycopy(bytes, 0, data, size, bytes.length);
        size += bytes.length;
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
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, data, size, length);
        size += length;
    }

    private void writeByte(byte value) {
        ensure(1);
        data[size++] = value;
    }

    private void writeVarInt(long value) {
        ensure(10);
        while ((value & ~0x7FL) != 0) {
            data[size++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        data[size++] = (byte) value;
    }

    private void ensure(int extra) {
        if (size + extra > data.length) {
            data = Arrays.copyOf(data, (int) Math.min(Integer.MAX_VALUE - 8, Math.max((long) size + extra, data.length * 2L)));
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
