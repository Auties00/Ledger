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
    static final Version HIDDEN = new Version(Long.MIN_VALUE, null, null, null);

    /// The commit sequence in the high bits and the position of the write in its batch in the low bits.
    final long stamp;
    /// The encoded value, read-only, or `null` if this version removes the key.
    final MemorySegment value;
    /// The decoded value of a resident key space, or `null`.
    final Object decoded;
    /// The version this one replaced, or `null` once no reader can need it.
    volatile Version older;

    Version(long stamp, MemorySegment value, Object decoded, Version older) {
        this.stamp = stamp;
        this.value = value;
        this.decoded = decoded;
        OLDER.set(this, older);
    }

    /// {@return a version of `value`, which the unit `external` of [BlobFile] also holds if it is not `null`}
    static Version of(long stamp, MemorySegment value, Object decoded, Version older, PageRef external) {
        return external == null ? new Version(stamp, value, decoded, older) : new External(stamp, value, decoded, older, external);
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

        External(long stamp, MemorySegment value, Object decoded, Version older, PageRef unit) {
            super(stamp, value, decoded, older);
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

    /// {@return whether this version removes the key}
    boolean removes() {
        return value == null;
    }
}
