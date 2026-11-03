package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.ValueCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/// A [ValueCodec] made of three functions and a trust model.
///
/// The store calls [#decode] with its own memory; for an untrusted codec this method copies it first, so the decoder
/// always receives what its factory promised.
///
/// @param sizeOf returns the encoded size of a value
/// @param encoder writes a value in place
/// @param decoder reads a value
/// @param trusted whether the decoder reads the store's memory in place
/// @param <V> the value type
public record FunctionValueCodec<V>(ToLongFunction<? super V> sizeOf, BiConsumer<? super V, ? super MemorySegment> encoder,
                                    Function<? super MemorySegment, ? extends V> decoder, boolean trusted)
        implements ValueCodec<V> {
    /// Checks the functions.
    public FunctionValueCodec {
        Objects.requireNonNull(sizeOf, "sizeOf");
        Objects.requireNonNull(encoder, "encoder");
        Objects.requireNonNull(decoder, "decoder");
    }

    @Override
    public long sizeOf(V value) {
        return sizeOf.applyAsLong(value);
    }

    @Override
    public void encode(V value, MemorySegment target) {
        encoder.accept(value, target);
    }

    @Override
    public V decode(MemorySegment source) {
        return decoder.apply(trusted ? source : copy(source));
    }

    /// {@return a read-only copy of `source` on the heap, which nothing else references}
    static MemorySegment copy(MemorySegment source) {
        return MemorySegment.ofArray(source.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
    }
}
