package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.BuiltinCodecs;
import com.github.auties00.ledger.engine.FunctionKeyCodec;

import java.lang.foreign.MemorySegment;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/// Encodes keys to bytes whose order is the order of the keys, and decodes them back.
///
/// The store orders entries by comparing encoded keys as unsigned bytes, shorter before longer when one is a prefix
/// of the other. A codec must therefore encode keys so that this order is the order it wants: big-endian integers
/// with the sign bit flipped, strings in UTF-8, fields of a composite key one after the other, each of fixed length
/// or followed by a terminator that sorts below every byte it can contain.
///
/// A key is encoded in place, into memory the store provides, and decoded under the trust model of [ValueCodec]:
/// the decoder of an [#of] codec receives a copy it may keep, that of an [#ofTrusted] codec the store's own memory,
/// valid only during the call.
///
/// @param <K> the key type
public sealed interface KeyCodec<K> permits FunctionKeyCodec {
    /// The longest encoded key a store accepts.
    int MAX_LENGTH = 1_024;

    /// {@return a codec whose decoder receives a copy of each encoded key, which it may keep}
    ///
    /// @param sizeOf returns the exact number of bytes a key encodes to, from 1 to [#MAX_LENGTH]
    /// @param encoder writes a key into memory of exactly that size, valid only during the call
    /// @param decoder reads a key from a read-only copy of its encoding, which it owns
    /// @param <K> the key type
    static <K> KeyCodec<K> of(ToIntFunction<? super K> sizeOf, BiConsumer<? super K, ? super MemorySegment> encoder,
                              Function<? super MemorySegment, ? extends K> decoder) {
        return new FunctionKeyCodec<>(sizeOf, encoder, decoder, false);
    }

    /// {@return a codec whose decoder reads each encoded key in the store's own memory, without a copy}
    ///
    /// @param sizeOf returns the exact number of bytes a key encodes to, from 1 to [#MAX_LENGTH]
    /// @param encoder writes a key into memory of exactly that size, valid only during the call
    /// @param decoder reads a key from the store's read-only memory, valid only during the call; it must copy
    ///                whatever it keeps
    /// @param <K> the key type
    static <K> KeyCodec<K> ofTrusted(ToIntFunction<? super K> sizeOf, BiConsumer<? super K, ? super MemorySegment> encoder,
                                     Function<? super MemorySegment, ? extends K> decoder) {
        return new FunctionKeyCodec<>(sizeOf, encoder, decoder, true);
    }

    /// {@return a codec of byte arrays, ordered as unsigned bytes}
    ///
    /// It is trusted: decoding copies each key into an array of its own.
    static KeyCodec<byte[]> bytes() {
        return BuiltinCodecs.BYTES_KEY;
    }

    /// {@return a codec of strings in UTF-8, ordered by code point}
    ///
    /// It is trusted: decoding builds a string of its own.
    static KeyCodec<String> utf8() {
        return BuiltinCodecs.UTF8_KEY;
    }

    /// {@return a codec of `long` values in eight bytes, ordered numerically, negative values first}
    static KeyCodec<Long> int64() {
        return BuiltinCodecs.INT64;
    }

    /// {@return a codec of `int` values in four bytes, ordered numerically, negative values first}
    static KeyCodec<Integer> int32() {
        return BuiltinCodecs.INT32;
    }

    /// {@return the exact number of bytes `key` encodes to}
    ///
    /// @param key the key
    int sizeOf(K key);

    /// Encodes `key` into `target`.
    ///
    /// @param key the key
    /// @param target writable memory of exactly [#sizeOf] bytes, valid only during the call
    void encode(K key, MemorySegment target);

    /// Decodes a key [#encode] produced.
    ///
    /// @param source the encoded key; for a codec that is not [#trusted()], a copy the key may keep
    /// @return the key
    K decode(MemorySegment source);

    /// {@return whether the decoder reads the store's memory in place rather than a copy}
    boolean trusted();
}
