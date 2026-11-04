package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// One version of the value under a key, linked to the version it replaced.
///
/// A chain starts at the newest version. Its stamps decrease along the chain; a reader takes the first version whose
/// stamp is at or below its bound. Only the writer creates versions and cuts chains, by clearing [#older].
final class Version {
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
    /// The unit of [BlobFile] that also holds [#value], or `null` if the value is in memory and the log only.
    final PageRef external;
    /// The sequence of the commit that replaced or removed this version, while no checkpoint has taken its unit, or
    /// [Long#MAX_VALUE]; guarded by the writer lock.
    long replacedAt;

    Version(long stamp, MemorySegment value, Object decoded, Version older) {
        this(stamp, value, decoded, older, null);
    }

    Version(long stamp, MemorySegment value, Object decoded, Version older, PageRef external) {
        this.stamp = stamp;
        this.value = value;
        this.decoded = decoded;
        this.older = older;
        this.external = external;
        this.replacedAt = Long.MAX_VALUE;
    }

    /// {@return whether this version removes the key}
    boolean removes() {
        return value == null;
    }
}
