package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/// A [KeyCodec] made of three functions and a trust model.
///
/// The store calls [#decode] with its own memory; for an untrusted codec this method copies it first, so the decoder
/// always receives what its factory promised.
///
/// @param sizeOf returns the encoded size of a key
/// @param encoder writes a key in place
/// @param decoder reads a key
/// @param trusted whether the decoder reads the store's memory in place
/// @param <K> the key type
public record FunctionKeyCodec<K>(ToIntFunction<? super K> sizeOf, BiConsumer<? super K, ? super MemorySegment> encoder,
                                  Function<? super MemorySegment, ? extends K> decoder, boolean trusted)
        implements KeyCodec<K> {
    /// Checks the functions.
    public FunctionKeyCodec {
        Objects.requireNonNull(sizeOf, "sizeOf");
        Objects.requireNonNull(encoder, "encoder");
        Objects.requireNonNull(decoder, "decoder");
    }

    @Override
    public int sizeOf(K key) {
        return sizeOf.applyAsInt(key);
    }

    @Override
    public void encode(K key, MemorySegment target) {
        encoder.accept(key, target);
    }

    @Override
    public K decode(MemorySegment source) {
        return decoder.apply(trusted ? source : FunctionValueCodec.copy(source));
    }
}
