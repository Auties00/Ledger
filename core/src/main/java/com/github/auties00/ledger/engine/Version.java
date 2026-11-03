package com.github.auties00.ledger.engine;

import java.lang.foreign.MemorySegment;

/// One version of the value under a key, linked to the version it replaced.
///
/// A chain starts at the newest version. Its stamps decrease along the chain; a reader takes the first version whose
/// stamp is at or below its bound. Only the writer creates versions and cuts chains, by clearing [#older].
final class Version {
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
        this.older = older;
    }

    /// {@return whether this version removes the key}
    boolean removes() {
        return value == null;
    }
}
