package com.github.auties00.ledger.engine;

/// The layout of the data file, `ledger.data`, which holds the state of a store as of its last checkpoint.
///
/// The file is a sequence of 4 KiB pages. Pages 0 and 1 hold the two superblocks, written alternately; the other
/// pages hold units a checkpoint writes into pages no durable state uses: one page for a node of a key space's tree, a
/// run of consecutive pages for a blob (a value too large for a leaf, the catalog, the free list). Every unit is
/// sealed on its own:
///
/// ```text
///  0  kind, 1 byte
///  1  format version, 1 byte
///  2  level of a node (0 for a leaf), 1 byte
///  3  reserved, 1 byte
///  4  pages in the unit, 4 bytes
///  8  number of its first page, 4 bytes
/// 12  bytes of content, 4 bytes
/// 16  nonce, 12 bytes
/// 28  reserved, 4 bytes
/// 32  content, encrypted, then zero up to the tag
///     AES-GCM tag, 16 bytes; without encryption a CRC-32C of everything before it, then 12 zero bytes
/// ```
///
/// The header is the additional data of the GCM tag, so a unit read at another position, or under another kind,
/// fails authentication. The nonce of each unit is new (see [Checkpointer]), and whatever refers to a unit (a parent
/// node, a leaf holding a large value, a superblock) records its nonce: a unit left over from an earlier checkpoint
/// in the same pages fails the check even when it authenticates.
final class PageFormat {
    static final int SIZE = 4_096;
    static final int HEADER = 32;
    static final int TAG = 16;
    static final byte VERSION = 1;

    static final byte SUPERBLOCK = 1;
    static final byte LEAF = 2;
    static final byte BRANCH = 3;
    static final byte VALUE = 4;
    static final byte CATALOG = 5;
    static final byte FREE_LIST = 6;
    /// A value of the file of large values, `ledger.blobs`.
    static final byte EXTERNAL = 7;

    /// The first page a unit may use; pages 0 and 1 are the superblocks.
    static final int FIRST_UNIT_PAGE = 2;

    static final int KIND = 0;
    static final int FORMAT = 1;
    static final int LEVEL = 2;
    static final int PAGES = 4;
    static final int PAGE = 8;
    static final int LENGTH = 12;
    static final int NONCE = 16;

    private PageFormat() {
    }

    /// {@return the bytes of content a unit of `pages` pages holds}
    static int capacity(int pages) {
        return pages * SIZE - HEADER - TAG;
    }

    /// {@return the pages of a unit holding `content` bytes}
    static int pagesFor(long content) {
        return Math.toIntExact((content + HEADER + TAG + SIZE - 1) / SIZE);
    }

    /// {@return the byte offset of page `page` in the file}
    static long offset(int page) {
        return (long) page * SIZE;
    }
}
