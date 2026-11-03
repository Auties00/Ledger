package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.BuiltinCodecs;

import java.lang.foreign.MemorySegment;

/// Encodes values to bytes and decodes them back.
///
/// A value is encoded in place, into memory the store provides, and decoded from the store's own memory: the page
/// cache, or the memory a recent write still occupies. Neither direction copies the bytes on the way, so the only
/// copies of a value are the encoding, the encryption and decryption, and the decoded object.
///
/// A protobuf message type, for example, encodes and decodes straight on the segments:
///
/// {@snippet lang = java :
/// enum MessageCodec implements ValueCodec<Message> {
///     INSTANCE;
///
///     public long sizeOf(Message message) {
///         return MessageSpec.sizeOf(message);
///     }
///
///     public void encode(Message message, MemorySegment target) {
///         MessageSpec.encode(message, ProtobufOutputStream.toMemorySegment(target));
///     }
///
///     public Message decode(MemorySegment source) {
///         return MessageSpec.decode(ProtobufInputStream.fromMemorySegment(source));
///     }
/// }
/// }
///
/// A library that only reads and writes [java.nio.ByteBuffer] can bridge with `segment.asByteBuffer()`, a view of
/// the same memory, at the cost of the view.
///
/// The values of a [Keyspace.Residency#RESIDENT] key space are decoded once, when the store opens or when they are
/// written, and every read returns the same instance: their type should be immutable.
///
/// @param <V> the value type
public interface ValueCodec<V> {
    /// The longest encoded value a store accepts: larger values belong in files of their own.
    long MAX_LENGTH = 1L << 30;

    /// {@return the exact number of bytes `value` encodes to, from 0 to [#MAX_LENGTH]}
    ///
    /// @param value the value
    long sizeOf(V value);

    /// Encodes `value` into `target`.
    ///
    /// @param value the value
    /// @param target writable memory of exactly [#sizeOf] bytes, valid only during the call
    void encode(V value, MemorySegment target);

    /// Decodes a value [#encode] produced.
    ///
    /// @param source the encoded value, read-only and valid only during the call: the store reuses the memory
    ///               afterward, so a value that keeps any of it, a view included, must copy it
    /// @return the value
    V decode(MemorySegment source);

    /// {@return a codec of byte arrays}
    ///
    /// Values are copied into an array when decoded.
    static ValueCodec<byte[]> bytes() {
        return BuiltinCodecs.BYTES_VALUE;
    }

    /// {@return a codec of strings in UTF-8}
    static ValueCodec<String> utf8() {
        return BuiltinCodecs.UTF8_VALUE;
    }
}
