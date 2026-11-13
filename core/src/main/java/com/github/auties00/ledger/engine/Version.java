package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/// One version of the value under a key, linked to the version it replaced.
///
/// A chain starts at the newest version. Its stamps decrease along the chain; a reader takes the first version whose
/// stamp is at or below its bound. Only the writer creates versions and cuts chains, through [#link(Version)].
///
/// A version reaches readers through the release store that makes it the head of its chain, which publishes the
/// plain store of [#older] the constructor makes; a volatile store there would wait, on most processors, for every
/// store before it to reach the cache.
///
/// A version whose value a unit of [BlobFile] also holds is an [External], which keeps what the file needs: 16 bytes
/// every other buffered write would carry for nothing.
///
/// The value of a paged version waiting for a checkpoint may leave its batch's array for the log, which holds it too
/// ([SpillQueue]): [#spill] replaces the source, once, with a [LogValue] or with an array of the value alone. Every
/// read loads the source once and takes the offset only with a segment, so a read that races the replacement reads
/// either the old source with its offset or the new one, which needs none.
sealed class Version permits Version.External {
    // Before HIDDEN, whose constructor uses it
    private static final VarHandle OLDER;
    private static final VarHandle SOURCE;

    static {
        try {
            OLDER = MethodHandles.lookup().findVarHandle(Version.class, "older", Version.class);
            SOURCE = MethodHandles.lookup().findVarHandle(Version.class, "source", Object.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /// What [KeyspaceState#resolve] returns for a key a reader sees removed.
    static final Version HIDDEN = new Version(Long.MIN_VALUE, (MemorySegment) null, null, null);

    /// The commit sequence in the high bits and the position of the write in its batch in the low bits.
    final long stamp;
    // The read-only segment the encoded value is part of, the array of its batch; or the array that is the value, which
    // a resident version keeps, as does a paged one its batch gave an array of its own or whose value left the batch's
    // array, and whose segment and session, 72 bytes, a read of its encoding makes; or the SplitValue of a large value
    // its batch kept in two arrays, which only an External holds; or the place of the value in the log ([LogValue]); or
    // `null` if this version removes the key. A slice of its own per version was 40 of the bytes a buffered write took.
    // Changed only by spill, from a segment, with a compare-and-set
    private Object source;
    // Where the value starts in source if source is a segment, and its length
    private final int offset;
    private final int length;
    /// The decoded value of a resident key space, or `null`.
    final Object decoded;
    /// The version this one replaced, or `null` once no reader can need it.
    volatile Version older;

    Version(long stamp, MemorySegment value, Object decoded, Version older) {
        this(stamp, value, 0, value == null ? 0 : Math.toIntExact(value.byteSize()), decoded, older);
    }

    /// Makes a version of the `length` bytes at `offset` of `source`, a read-only segment.
    Version(long stamp, MemorySegment source, int offset, int length, Object decoded, Version older) {
        this(stamp, (Object) source, offset, length, decoded, older);
    }

    /// Makes a version whose value is `value`, an array of its own that no one writes.
    Version(long stamp, byte[] value, Object decoded, Version older) {
        this(stamp, value, 0, value.length, decoded, older);
    }

    private Version(long stamp, Object source, int offset, int length, Object decoded, Version older) {
        this.stamp = stamp;
        this.source = source;
        this.offset = offset;
        this.length = length;
        this.decoded = decoded;
        OLDER.set(this, older);
    }

    /// {@return a version whose value is `value`, an array of its own that no one writes, which the unit `external` of
    /// [BlobFile] also holds if it is not `null`}
    static Version ofOwn(long stamp, byte[] value, Object decoded, Version older, PageRef external) {
        return external == null ? new Version(stamp, value, decoded, older)
                : new External(stamp, value, 0, value.length, decoded, older, external);
    }

    /// {@return a version whose value is `value`, kept in two arrays no one writes, which the unit `external` of
    /// [BlobFile] also holds}
    ///
    /// Only a value of a paged key space is split, so the version keeps no decoded value.
    static Version ofSplit(long stamp, SplitValue value, Version older, PageRef external) {
        return new External(stamp, value, 0, value.length(), null, older, external);
    }

    /// {@return a version of the `length` bytes at `offset` of `source`, a read-only segment, or of no value if
    /// `source` is `null`, which the unit `external` of [BlobFile] also holds if it is not `null`}
    static Version of(long stamp, MemorySegment source, int offset, int length, Object decoded, Version older, PageRef external) {
        return external == null ? new Version(stamp, source, offset, length, decoded, older)
                : new External(stamp, source, offset, length, decoded, older, external);
    }

    /// {@return the encoded value, read-only, or `null` if this version removes the key}
    ///
    /// The value of a [SplitValue] is joined into a new array on every call, which no version keeps: a scan or a read
    /// of the encoding of a large value no checkpoint has taken yet copies it whole. A value in the log is read from it.
    ///
    /// @throws LogSegmentView.Retired if the value is in a segment a checkpoint retired, which the tree of every reader
    ///                                that can still find this version holds
    /// @throws com.github.auties00.ledger.LedgerIoException if the value is in the log and cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the value is in the log and the log changed
    MemorySegment value() {
        var held = SOURCE.getAcquire(this);
        if (held == null) {
            return null;
        }
        if (held instanceof byte[] array) {
            return MemorySegment.ofArray(array).asReadOnly();
        }
        if (held instanceof SplitValue split) {
            return MemorySegment.ofArray(split.join()).asReadOnly();
        }
        if (held instanceof LogValue logged) {
            return MemorySegment.ofArray(logged.read(stamp, length)).asReadOnly();
        }
        var whole = (MemorySegment) held;
        return offset == 0 && length == whole.byteSize() ? whole : whole.asSlice(offset, length);
    }

    /// {@return the encoded value in a new array, or `null` if this version removes the key}
    ///
    /// A value on an array of its own is copied with an array copy, which spares the zeroing of the new array a copy
    /// of [#value] pays ([Segments#toBytes(byte\[\], int, int)]), and a [SplitValue] with one per array, which spares
    /// it for all but the tail's bytes; one on a read-only segment, which hides its array, cannot be. An array source is
    /// the value whole, at no offset. A value in the log is read into an array of its own.
    ///
    /// @throws LogSegmentView.Retired as [#value()] does
    byte[] copyBytes() {
        var held = SOURCE.getAcquire(this);
        if (held instanceof byte[] array) {
            return Segments.toBytes(array, 0, array.length);
        }
        if (held instanceof LogValue logged) {
            return logged.read(stamp, length);
        }
        if (held == null) {
            return null;
        }
        if (held instanceof SplitValue split) {
            return split.join();
        }
        // No slice of the segment: the copy reads the value where it is
        return Segments.toBytes((MemorySegment) held, offset, length);
    }

    /// {@return the bytes of the value, 0 for a removal}
    int valueLength() {
        return length;
    }

    /// {@return whether the value is in a log segment a checkpoint retired, so only the trees hold it now}
    ///
    /// Only a checkpoint retires segments, so the answer does not change while one runs.
    boolean inRetiredSegment() {
        return SOURCE.getAcquire(this) instanceof LogValue logged && logged.segment().retired();
    }

    /// Moves the value off the segment it is part of, once: to its place in the log if it is at least
    /// [SpillQueue#LOG_VALUE_MIN] bytes, else to an array of its own.
    ///
    /// The frame at `frameOffset` of `segment` holds the value durably, at `bodyIndex` of its body. The tag that
    /// authenticates the value read back is made from the bytes of the segment, which are those the frame sealed.
    ///
    /// @return whether the value moved: not if it had moved already, or is not on a segment
    boolean spill(SpillQueue.Crypto crypto, LogSegmentView segment, long frameOffset, int bodyIndex) {
        var held = SOURCE.getAcquire(this);
        if (!(held instanceof MemorySegment home) || external() != null) {
            return false;
        }
        Object moved = length >= SpillQueue.LOG_VALUE_MIN
                ? crypto.logValue(segment, frameOffset, bodyIndex, stamp, home, offset, length)
                : Segments.toBytes(home, offset, length);
        return SOURCE.compareAndSet(this, held, moved);
    }

    /// {@return whether the value is in the log ([LogValue]), where it stays until the version is dropped}
    boolean logged() {
        return SOURCE.getAcquire(this) instanceof LogValue;
    }

    /// Reads the value, which is in the log ([#logged()]), into `target` from `offset`.
    ///
    /// @throws LogSegmentView.Retired as [#value()] does
    /// @throws com.github.auties00.ledger.LedgerIoException if the log cannot be read
    /// @throws com.github.auties00.ledger.LedgerCorruptedException if the log changed
    void readLogged(byte[] target, int offset) {
        ((LogValue) SOURCE.getAcquire(this)).readInto(stamp, length, target, offset);
    }

    /// {@return the length of the encoded value, 0 for a removal}
    int length() {
        return length;
    }

    /// {@return the unit of [BlobFile] that also holds [#value], or `null` if the value is in memory and the log only}
    PageRef external() {
        return null;
    }

    /// A version whose value a unit of [BlobFile] also holds.
    static final class External extends Version {
        /// The unit.
        final PageRef unit;
        /// The sequence of the commit that replaced or removed this version, while no checkpoint has taken its unit,
        /// or [Long#MAX_VALUE]; written by that commit, which applies after the commits before it on the key, and read
        /// holding the publishing role.
        long replacedAt;

        External(long stamp, Object source, int offset, int length, Object decoded, Version older, PageRef unit) {
            super(stamp, source, offset, length, decoded, older);
            this.unit = unit;
            this.replacedAt = Long.MAX_VALUE;
        }

        @Override
        PageRef external() {
            return unit;
        }
    }

    /// Makes `older` the version after this one, or cuts the chain here if `null`.
    ///
    /// A release store: a reader that follows the old link still finds versions whose fields it can read, and one
    /// that follows the new link finds the versions it needs, since only those no reader needs leave.
    void link(Version older) {
        OLDER.setRelease(this, older);
    }

    /// {@return the bytes of the segment the value is part of, 0 for a removal or a value in the log; for tests of what
    /// memory keeps}
    long sourceBytesForTests() {
        return switch (SOURCE.getAcquire(this)) {
            case null -> 0;
            case byte[] array -> array.length;
            case SplitValue split -> split.length();
            case MemorySegment segment -> segment.byteSize();
            case LogValue _ -> 0;
            default -> throw new IllegalStateException();
        };
    }

    /// {@return the segment, array, [SplitValue] or place in the log the value is on, `null` for a removal; for tests of
    /// what memory keeps}
    Object sourceForTests() {
        return SOURCE.getAcquire(this);
    }

    /// {@return whether this version removes the key}
    boolean removes() {
        return source == null;
    }
}
