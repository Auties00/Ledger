package com.github.auties00.ledger.engine;

/// The state a checkpoint made durable, as a superblock of the data file records it.
///
/// Content of a superblock unit, after the [PageFormat] header:
///
/// ```text
///  0  checkpoint number, 8 bytes
///  8  sequence of the last commit it holds, 8 bytes
/// 16  first log segment recovery reads, 8 bytes
/// 24  pages of the file, 4 bytes
/// 28  reference of the catalog blob, 20 bytes
/// 48  reference of the free list blob, 20 bytes
/// ```
///
/// @param number the checkpoint number, from 1; superblock `number % 2` holds it
/// @param sequence the sequence of the last commit the checkpoint holds
/// @param logStart the first log segment that may hold a later commit; no segment before it is needed, and no later
///                 segment is ever numbered below it
/// @param pageCount the pages of the file the checkpoint uses; a page past them is free
/// @param catalog the catalog blob
/// @param freeList the free list blob
record Superblock(long number, long sequence, long logStart, int pageCount, PageRef catalog, PageRef freeList) {
    static final int CONTENT = 68;

    /// {@return the superblock written in `unit`, an opened superblock unit}
    static Superblock read(byte[] unit) {
        var at = PageFormat.HEADER;
        return new Superblock(Bytes.i64(unit, at), Bytes.i64(unit, at + 8), Bytes.i64(unit, at + 16),
                Bytes.i32(unit, at + 24), PageRef.read(unit, at + 28), PageRef.read(unit, at + 48));
    }

    /// Writes this superblock's content into `unit`.
    void write(byte[] unit) {
        var at = PageFormat.HEADER;
        Bytes.i64(unit, at, number);
        Bytes.i64(unit, at + 8, sequence);
        Bytes.i64(unit, at + 16, logStart);
        Bytes.i32(unit, at + 24, pageCount);
        catalog.write(unit, at + 28);
        freeList.write(unit, at + 48);
    }

    /// {@return the page this superblock is written to}
    int slot() {
        return (int) (number % 2);
    }
}
