package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

/// Operations on the memory segments that hold encoded keys and values.
final class Segments {
    /// Orders segments as unsigned bytes, a prefix before every longer segment it starts.
    static final Comparator<MemorySegment> UNSIGNED = Segments::compare;

    private Segments() {
    }

    /// {@return a negative number, zero or a positive number as `left` sorts before, with or after `right`}
    static int compare(MemorySegment left, MemorySegment right) {
        var mismatch = left.mismatch(right);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == left.byteSize()) {
            return -1;
        }
        if (mismatch == right.byteSize()) {
            return 1;
        }
        return Byte.compareUnsigned(left.get(ValueLayout.JAVA_BYTE, mismatch), right.get(ValueLayout.JAVA_BYTE, mismatch));
    }

    /// {@return `key` encoded into a read-only segment of its own}
    ///
    /// @throws IllegalArgumentException if the key encodes to no bytes or more than [KeyCodec#MAX_LENGTH]
    static <K> MemorySegment encodeKey(KeyCodec<K> codec, K key) {
        var size = codec.sizeOf(key);
        if (size < 1 || size > KeyCodec.MAX_LENGTH) {
            throw new IllegalArgumentException("Keys are 1 to " + KeyCodec.MAX_LENGTH + " bytes: " + size);
        }
        var array = new byte[size];
        var segment = MemorySegment.ofArray(array);
        codec.encode(key, segment);
        return segment.asReadOnly();
    }
}
