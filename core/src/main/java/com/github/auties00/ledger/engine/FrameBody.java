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
/// A catalog body is entries of an id and a name; an epoch body is the epoch. Numbers are unsigned LEB128 varints;
/// keys are never empty, so a bound of length 0 is an absent bound.
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
            data = Arrays.copyOf(data, Math.max(size + extra, data.length * 2));
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
