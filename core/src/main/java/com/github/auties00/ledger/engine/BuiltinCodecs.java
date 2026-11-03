package com.github.auties00.ledger.engine;

import com.github.auties00.ledger.KeyCodec;
import com.github.auties00.ledger.ValueCodec;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/// The codecs [KeyCodec] and [ValueCodec] offer.
public final class BuiltinCodecs {
    private static final ValueLayout.OfLong BIG_ENDIAN_LONG = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
    private static final ValueLayout.OfInt BIG_ENDIAN_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

    /// Byte array keys, ordered as unsigned bytes.
    public static final KeyCodec<byte[]> BYTES_KEY = new KeyCodec<>() {
        @Override
        public int sizeOf(byte[] key) {
            return key.length;
        }

        @Override
        public void encode(byte[] key, MemorySegment target) {
            MemorySegment.copy(key, 0, target, ValueLayout.JAVA_BYTE, 0, key.length);
        }

        @Override
        public byte[] decode(MemorySegment source) {
            return source.toArray(ValueLayout.JAVA_BYTE);
        }
    };

    /// Byte array values.
    public static final ValueCodec<byte[]> BYTES_VALUE = new ValueCodec<>() {
        @Override
        public long sizeOf(byte[] value) {
            return value.length;
        }

        @Override
        public void encode(byte[] value, MemorySegment target) {
            MemorySegment.copy(value, 0, target, ValueLayout.JAVA_BYTE, 0, value.length);
        }

        @Override
        public byte[] decode(MemorySegment source) {
            return source.toArray(ValueLayout.JAVA_BYTE);
        }
    };

    /// String keys in UTF-8, ordered by code point.
    public static final KeyCodec<String> UTF8_KEY = new KeyCodec<>() {
        @Override
        public int sizeOf(String key) {
            return Math.toIntExact(utf8Length(key));
        }

        @Override
        public void encode(String key, MemorySegment target) {
            encodeUtf8(key, target);
        }

        @Override
        public String decode(MemorySegment source) {
            return new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
    };

    /// String values in UTF-8.
    public static final ValueCodec<String> UTF8_VALUE = new ValueCodec<>() {
        @Override
        public long sizeOf(String value) {
            return utf8Length(value);
        }

        @Override
        public void encode(String value, MemorySegment target) {
            encodeUtf8(value, target);
        }

        @Override
        public String decode(MemorySegment source) {
            return new String(source.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
    };

    /// `long` keys, big-endian with the sign bit flipped so negative values sort first.
    public static final KeyCodec<Long> INT64 = new KeyCodec<>() {
        @Override
        public int sizeOf(Long key) {
            return Long.BYTES;
        }

        @Override
        public void encode(Long key, MemorySegment target) {
            target.set(BIG_ENDIAN_LONG, 0, key ^ Long.MIN_VALUE);
        }

        @Override
        public Long decode(MemorySegment source) {
            return source.get(BIG_ENDIAN_LONG, 0) ^ Long.MIN_VALUE;
        }
    };

    /// `int` keys, big-endian with the sign bit flipped so negative values sort first.
    public static final KeyCodec<Integer> INT32 = new KeyCodec<>() {
        @Override
        public int sizeOf(Integer key) {
            return Integer.BYTES;
        }

        @Override
        public void encode(Integer key, MemorySegment target) {
            target.set(BIG_ENDIAN_INT, 0, key ^ Integer.MIN_VALUE);
        }

        @Override
        public Integer decode(MemorySegment source) {
            return source.get(BIG_ENDIAN_INT, 0) ^ Integer.MIN_VALUE;
        }
    };

    private BuiltinCodecs() {
    }

    /// Writes `value` into `target` in UTF-8, without an intermediate array; an unpaired surrogate becomes `?`, as
    /// [String#getBytes(java.nio.charset.Charset)] makes it.
    private static void encodeUtf8(String value, MemorySegment target) {
        var offset = 0L;
        for (var i = 0; i < value.length(); i++) {
            var c = value.charAt(i);
            if (c < 0x80) {
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) c);
            } else if (c < 0x800) {
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0xC0 | c >> 6));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | c & 0x3F));
            } else if (Character.isHighSurrogate(c) && i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1))) {
                var codePoint = Character.toCodePoint(c, value.charAt(++i));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0xF0 | codePoint >> 18));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | codePoint >> 12 & 0x3F));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | codePoint >> 6 & 0x3F));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | codePoint & 0x3F));
            } else if (Character.isSurrogate(c)) {
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) '?');
            } else {
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0xE0 | c >> 12));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | c >> 6 & 0x3F));
                target.set(ValueLayout.JAVA_BYTE, offset++, (byte) (0x80 | c & 0x3F));
            }
        }
    }

    /// {@return the number of bytes `value` takes in UTF-8, counting an unpaired surrogate as the `?` it encodes to}
    private static long utf8Length(String value) {
        var length = 0L;
        for (var i = 0; i < value.length(); i++) {
            var c = value.charAt(i);
            if (c < 0x80) {
                length += 1;
            } else if (c < 0x800) {
                length += 2;
            } else if (Character.isHighSurrogate(c) && i + 1 < value.length() && Character.isLowSurrogate(value.charAt(i + 1))) {
                length += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                length += 1;
            } else {
                length += 3;
            }
        }
        return length;
    }
}
