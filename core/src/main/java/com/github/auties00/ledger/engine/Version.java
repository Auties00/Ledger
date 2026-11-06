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
sealed class Version permits Version.External {
    // Before HIDDEN, whose constructor uses it
    private static final VarHandle OLDER;

    static {
        try {
            OLDER = MethodHandles.lookup().findVarHandle(Version.class, "older", Version.class);
        } catch (ReflectiveOperationException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    /// What [KeyspaceState#resolve] returns for a key a reader sees removed.
    static final Version HIDDEN = new Version(Long.MIN_VALUE, (MemorySegment) null, null, null);

    /// The commit sequence in the high bits and the position of the write in its batch in the low bits.
    final long stamp;
    // The read-only segment the encoded value is part of, the array of its batch; or the array that is the value, which
    // a resident version keeps, and whose segment and session, 72 bytes, a read of its encoding makes; or `null` if
    // this version removes the key. A slice of its own per version was 40 of the bytes a buffered write took
    private final Object source;
    // Where the value starts in source, and its length
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

    /// {@return a version of the `length` bytes at `offset` of `source`, a read-only segment, or of no value if
    /// `source` is `null`, which the unit `external` of [BlobFile] also holds if it is not `null`}
    static Version of(long stamp, MemorySegment source, int offset, int length, Object decoded, Version older, PageRef external) {
        return external == null ? new Version(stamp, source, offset, length, decoded, older)
                : new External(stamp, source, offset, length, decoded, older, external);
    }

    /// {@return the encoded value, read-only, or `null` if this version removes the key}
    MemorySegment value() {
        var held = source;
        if (held == null) {
            return null;
        }
        if (held instanceof byte[] array) {
            return MemorySegment.ofArray(array).asReadOnly();
        }
        var whole = (MemorySegment) held;
        return offset == 0 && length == whole.byteSize() ? whole : whole.asSlice(offset, length);
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

    /// {@return the bytes of the segment the value is part of, 0 for a removal; for tests of what memory keeps}
    long sourceBytesForTests() {
        return switch (source) {
            case null -> 0;
            case byte[] array -> array.length;
            case MemorySegment segment -> segment.byteSize();
            default -> throw new IllegalStateException();
        };
    }

    /// {@return whether this version removes the key}
    boolean removes() {
        return source == null;
    }
}
