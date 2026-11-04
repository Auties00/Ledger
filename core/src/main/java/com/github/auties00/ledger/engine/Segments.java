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

    /// {@return a read-only copy of `segment` in an array of its own}
    ///
    /// For a key that outlives the batch it came in: a slice would keep the batch's whole array reachable.
    static MemorySegment copy(MemorySegment segment) {
        return MemorySegment.ofArray(toBytes(segment)).asReadOnly();
    }

    /// {@return the bytes of `segment`, in a new array}
    ///
    /// `MemorySegment.toArray` does the same, but builds the name of the array class for its size check on every
    /// call, which costs more than copying a short value.
    static byte[] toBytes(MemorySegment segment) {
        var array = new byte[Math.toIntExact(segment.byteSize())];
        MemorySegment.copy(segment, 0, MemorySegment.ofArray(array), 0, array.length);
        return array;
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
