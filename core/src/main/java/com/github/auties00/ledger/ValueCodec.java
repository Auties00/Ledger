package com.github.auties00.ledger;

import com.github.auties00.ledger.engine.BuiltinCodecs;
import com.github.auties00.ledger.engine.FunctionValueCodec;

import java.lang.foreign.MemorySegment;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/// Encodes values to bytes and decodes them back.
///
/// A value is encoded in place, into memory the store provides. How it is decoded depends on the trust the codec
/// was created with:
///
/// | Factory | The decoder receives | Cost |
/// |---|---|---|
/// | [#of] | a copy of the encoded value, which the decoded value may keep, views included | one copy per decode |
/// | [#ofTrusted] | the store's own memory, valid only during the call | none |
///
/// A trusted decoder must copy whatever it keeps: the store reuses its memory once the call returns, so a view kept
/// past it would read whatever the store writes there next. Choose [#of] for a type whose decoded values keep views
/// of their input, a protobuf type that reads `BYTES` fields as buffers for example, and [#ofTrusted] for one whose
/// values own everything they hold.
///
/// {@snippet lang = java :
/// var messages = ValueCodec.ofTrusted(
///         MessageSpec::sizeOf,
///         (message, target) -> MessageSpec.encode(message, ProtobufOutputStream.toMemorySegment(target)),
///         source -> MessageSpec.decode(ProtobufInputStream.fromMemorySegment(source)));
/// }
///
/// The values of a [Keyspace.Residency#RESIDENT] key space are decoded once, when the store opens or when they are
/// written, and every read returns the same instance: their type should be immutable.
///
/// @param <V> the value type
public sealed interface ValueCodec<V> permits FunctionValueCodec {
    /// The longest encoded value a store accepts: larger values belong in files of their own.
    long MAX_LENGTH = 1L << 30;

    /// {@return a codec whose decoder receives a copy of each encoded value, which it may keep}
    ///
    /// @param sizeOf returns the exact number of bytes a value encodes to, from 0 to [#MAX_LENGTH]
    /// @param encoder writes a value into memory of exactly that size, valid only during the call
    /// @param decoder reads a value from a read-only copy of its encoding, which it owns
    /// @param <V> the value type
    static <V> ValueCodec<V> of(ToLongFunction<? super V> sizeOf, BiConsumer<? super V, ? super MemorySegment> encoder,
                                Function<? super MemorySegment, ? extends V> decoder) {
        return new FunctionValueCodec<>(sizeOf, encoder, decoder, false);
    }

    /// {@return a codec whose decoder reads each encoded value in the store's own memory, without a copy}
    ///
    /// @param sizeOf returns the exact number of bytes a value encodes to, from 0 to [#MAX_LENGTH]
    /// @param encoder writes a value into memory of exactly that size, valid only during the call
    /// @param decoder reads a value from the store's read-only memory, valid only during the call; it must copy
    ///                whatever it keeps
    /// @param <V> the value type
    static <V> ValueCodec<V> ofTrusted(ToLongFunction<? super V> sizeOf, BiConsumer<? super V, ? super MemorySegment> encoder,
                                       Function<? super MemorySegment, ? extends V> decoder) {
        return new FunctionValueCodec<>(sizeOf, encoder, decoder, true);
    }

    /// {@return a codec of byte arrays}
    ///
    /// It is trusted: decoding copies each value into an array of its own.
    static ValueCodec<byte[]> bytes() {
        return BuiltinCodecs.BYTES_VALUE;
    }

    /// {@return a codec of strings in UTF-8}
    ///
    /// It is trusted: decoding builds a string of its own.
    static ValueCodec<String> utf8() {
        return BuiltinCodecs.UTF8_VALUE;
    }

    /// {@return the exact number of bytes `value` encodes to}
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
    /// @param source the encoded value; for a codec that is not [#trusted()], a copy the value may keep
    /// @return the value
    V decode(MemorySegment source);

    /// {@return whether the decoder reads the store's memory in place rather than a copy}
    boolean trusted();
}
