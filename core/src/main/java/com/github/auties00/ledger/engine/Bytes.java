package com.github.auties00.ledger.engine;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/// Big-endian reads and writes of fixed-width integers in byte arrays, the byte order of every file of a store.
final class Bytes {
    private static final VarHandle SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.BIG_ENDIAN);

    private Bytes() {
    }

    static int u16(byte[] array, int offset) {
        return (short) SHORT.get(array, offset) & 0xFFFF;
    }

    static void u16(byte[] array, int offset, int value) {
        SHORT.set(array, offset, (short) value);
    }

    static int i32(byte[] array, int offset) {
        return (int) INT.get(array, offset);
    }

    static void i32(byte[] array, int offset, int value) {
        INT.set(array, offset, value);
    }

    static long i64(byte[] array, int offset) {
        return (long) LONG.get(array, offset);
    }

    static void i64(byte[] array, int offset, long value) {
        LONG.set(array, offset, value);
    }
}
