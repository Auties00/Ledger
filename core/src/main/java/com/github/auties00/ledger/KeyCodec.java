package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.BuiltinCodecs;

import java.lang.foreign.MemorySegment;

/// Encodes keys to bytes whose order is the order of the keys, and decodes them back.
///
/// The store orders entries by comparing encoded keys as unsigned bytes, shorter before longer when one is a prefix
/// of the other. A codec must therefore encode keys so that this order is the order it wants: big-endian integers
/// with the sign bit flipped, strings in UTF-8, fields of a composite key one after the other, each of fixed length
/// or followed by a terminator that sorts below every byte it can contain.
///
/// A key is encoded in place, into memory the store provides, and decoded from the store's own memory, so neither
/// direction copies it on the way.
///
/// @param <K> the key type
public interface KeyCodec<K> {
    /// The longest encoded key a store accepts.
    int MAX_LENGTH = 1_024;

    /// {@return the exact number of bytes `key` encodes to, from 1 to [#MAX_LENGTH]}
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
    /// @param source the encoded key, read-only and valid only during the call; a key that keeps any of it must copy
    ///               it
    /// @return the key
    K decode(MemorySegment source);

    /// {@return a codec of byte arrays, ordered as unsigned bytes}
    ///
    /// Keys are copied into an array when decoded.
    static KeyCodec<byte[]> bytes() {
        return BuiltinCodecs.BYTES_KEY;
    }

    /// {@return a codec of strings in UTF-8, ordered by code point}
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
}
