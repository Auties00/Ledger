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
/// 68  pages of ledger.blobs, 4 bytes
/// 72  reference of the blob holding the free list of ledger.blobs, 20 bytes, or zeros for none
/// 92  epoch of the open that wrote the checkpoint, 4 bytes
/// 96  reference of the table of delta pages, 20 bytes, or zeros for none
/// ```
///
/// A superblock that ends at 68 bytes predates the file of large values, which it records as empty; one that ends at
/// 96 bytes predates delta records, so its trees hold none.
///
/// @param number the checkpoint number, from 1; superblock `number % 2` holds it
/// @param sequence the sequence of the last commit the checkpoint holds
/// @param logStart the first log segment that may hold a later commit; no segment before it is needed, and no later
///                 segment is ever numbered below it
/// @param pageCount the pages of the file the checkpoint uses; a page past them is free
/// @param catalog the catalog blob
/// @param freeList the free list blob
/// @param blobPageCount the pages of [BlobFile] the checkpoint uses, or 0
/// @param blobFreeList the blob holding the free list of [BlobFile], or `null` if it uses no page
/// @param epoch the epoch of the open that wrote the checkpoint, which every later open exceeds
/// @param deltaTable the blob holding the table of delta pages ([DeltaTable]), or `null` if the trees hold no delta
///                   record
record Superblock(long number, long sequence, long logStart, int pageCount, PageRef catalog, PageRef freeList,
                  int blobPageCount, PageRef blobFreeList, int epoch, PageRef deltaTable) {
    /// The bytes of a superblock's content.
    static final int CONTENT = 116;
    /// The bytes of the content of a superblock written before delta records.
    private static final int CONTENT_BEFORE_DELTAS = 96;

    /// {@return the superblock written in `unit`, an opened superblock unit}
    static Superblock read(byte[] unit) {
        var at = PageFormat.HEADER;
        var number = Bytes.i64(unit, at);
        var sequence = Bytes.i64(unit, at + 8);
        var logStart = Bytes.i64(unit, at + 16);
        var pageCount = Bytes.i32(unit, at + 24);
        var catalog = PageRef.read(unit, at + 28);
        var freeList = PageRef.read(unit, at + 48);
        var length = Bytes.i32(unit, PageFormat.LENGTH);
        if (length < CONTENT_BEFORE_DELTAS) {
            return new Superblock(number, sequence, logStart, pageCount, catalog, freeList, 0, null, 0, null);
        }
        var blobFreeList = PageRef.read(unit, at + 72);
        var deltaTable = length < CONTENT ? null : PageRef.read(unit, at + 96);
        return new Superblock(number, sequence, logStart, pageCount, catalog, freeList, Bytes.i32(unit, at + 68),
                blobFreeList.pages() == 0 ? null : blobFreeList, Bytes.i32(unit, at + 92),
                deltaTable == null || deltaTable.pages() == 0 ? null : deltaTable);
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
        Bytes.i32(unit, at + 68, blobPageCount);
        (blobFreeList == null ? new PageRef(0, 0, 0, 0) : blobFreeList).write(unit, at + 72);
        Bytes.i32(unit, at + 92, epoch);
        (deltaTable == null ? new PageRef(0, 0, 0, 0) : deltaTable).write(unit, at + 96);
    }

    /// {@return the page this superblock is written to}
    int slot() {
        return (int) (number % 2);
    }
}
