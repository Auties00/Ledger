package com.github.auties00.ledger.engine;

/// Where a unit of the data file is, and the nonce it was sealed with, which tells it apart from any unit written
/// earlier in the same pages.
///
/// @param page the number of its first page
/// @param pages the number of pages, at least 1
/// @param nonceHigh the first 8 bytes of the nonce
/// @param nonceLow the last 4 bytes of the nonce
record PageRef(int page, int pages, long nonceHigh, int nonceLow) {
    /// The bytes a reference takes in a unit: page, pages, nonce.
    static final int BYTES = 4 + 4 + 8 + 4;

    /// {@return the reference written at `offset` of `array`}
    static PageRef read(byte[] array, int offset) {
        return new PageRef(Bytes.i32(array, offset), Bytes.i32(array, offset + 4), Bytes.i64(array, offset + 8),
                Bytes.i32(array, offset + 16));
    }

    /// Writes this reference at `offset` of `array`.
    void write(byte[] array, int offset) {
        Bytes.i32(array, offset, page);
        Bytes.i32(array, offset + 4, pages);
        Bytes.i64(array, offset + 8, nonceHigh);
        Bytes.i32(array, offset + 16, nonceLow);
    }

    /// {@return whether the header of `unit` names this reference's pages and nonce}
    boolean describes(byte[] unit) {
        return Bytes.i32(unit, PageFormat.PAGES) == pages
                && Bytes.i32(unit, PageFormat.PAGE) == page
                && Bytes.i64(unit, PageFormat.NONCE) == nonceHigh
                && Bytes.i32(unit, PageFormat.NONCE + 8) == nonceLow;
    }
}
