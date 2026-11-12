package com.github.auties00.ledger.engine;

/// Where a unit of the data file is, and the nonce it was sealed with, which tells it apart from any unit written
/// earlier in the same pages; or, with a slot, one of the delta records a [delta page][DeltaLeaves] holds.
///
/// @param page the number of its first page
/// @param pages the number of pages, at least 1
/// @param nonceHigh the first 8 bytes of the nonce
/// @param nonceLow the last 4 bytes of the nonce
/// @param slot the record of the delta page, from 0, or -1 for the unit itself
record PageRef(int page, int pages, long nonceHigh, int nonceLow, int slot) {
    /// The bytes a reference takes in a unit: page, pages, nonce; never the slot, which a branch records apart.
    static final int BYTES = 4 + 4 + 8 + 4;

    /// Names the unit itself.
    PageRef(int page, int pages, long nonceHigh, int nonceLow) {
        this(page, pages, nonceHigh, nonceLow, -1);
    }

    /// {@return the reference written at `offset` of `array`}
    static PageRef read(byte[] array, int offset) {
        return new PageRef(Bytes.i32(array, offset), Bytes.i32(array, offset + 4), Bytes.i64(array, offset + 8),
                Bytes.i32(array, offset + 16));
    }

    /// Writes this reference, without its slot, at `offset` of `array`.
    void write(byte[] array, int offset) {
        Bytes.i32(array, offset, page);
        Bytes.i32(array, offset + 4, pages);
        Bytes.i64(array, offset + 8, nonceHigh);
        Bytes.i32(array, offset + 16, nonceLow);
    }

    /// {@return whether the reference names a delta record rather than a unit}
    boolean isRecord() {
        return slot >= 0;
    }

    /// {@return the unit that holds what the reference names: the reference itself, or the delta page of a record}
    PageRef unit() {
        return slot < 0 ? this : new PageRef(page, pages, nonceHigh, nonceLow);
    }

    /// {@return whether the header of `unit` names this reference's pages and nonce}
    boolean describes(byte[] unit) {
        return describes(unit, 0);
    }

    /// {@return whether the header of the unit at `offset` of `array` names this reference's pages and nonce}
    boolean describes(byte[] array, int offset) {
        return Bytes.i32(array, offset + PageFormat.PAGES) == pages
                && Bytes.i32(array, offset + PageFormat.PAGE) == page
                && Bytes.i64(array, offset + PageFormat.NONCE) == nonceHigh
                && Bytes.i32(array, offset + PageFormat.NONCE + 8) == nonceLow;
    }
}
