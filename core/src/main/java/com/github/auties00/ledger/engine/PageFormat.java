package com.github.auties00.ledger.engine;

/// The layout of the data file, `ledger.data`, which holds the state of a store as of its last checkpoint.
///
/// The file is a sequence of 4 KiB pages. Pages 0 and 1 hold the two superblocks, written alternately; the other
/// pages hold units a checkpoint writes into pages no durable state uses: one page for a node of a key space's tree or
/// for the delta records of several leaves ([DeltaLeaves]), a run of consecutive pages for a blob (a value too large
/// for a leaf, the catalog, the free list, the table of delta pages). Every unit is sealed on its own:
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
/// 28  search index of a node of the second format, 1 byte; 0 in every other unit
/// 29  reserved, 3 bytes
/// 32  content, encrypted, then zero up to the tag
///     AES-GCM tag, 16 bytes; without encryption a CRC-32C of everything before it, then 12 zero bytes
/// ```
///
/// Every unit is of format [#VERSION], but a leaf or a branch of format [#INDEXED], which holds a search index between
/// the offsets of its entries and the entries ([NodeIndex]). A reader finds the entries of either through their
/// offsets, so the first format's readers read both, and a store holds nodes of both formats.
///
/// The header is the additional data of the GCM tag, so a unit read at another position, or under another kind,
/// fails authentication. The nonce of each unit is new (see [Checkpointer]), and whatever refers to a unit (a parent
/// node, a leaf holding a large value, a superblock) records its nonce: a unit left over from an earlier checkpoint
/// in the same pages fails the check even when it authenticates.
///
/// In memory, the array of a unit holds its header and content, and may end there or anywhere up to the end of the
/// unit: no reader looks past the content, so a unit a checkpoint builds holds nothing more, and one the page cache
/// reads holds what [#cachedLength(int, int)] keeps.
final class PageFormat {
    static final int SIZE = 4_096;
    static final int HEADER = 32;
    static final int TAG = 16;
    static final byte VERSION = 1;
    /// The format of a leaf or a branch that holds a search index ([NodeIndex]).
    static final byte INDEXED = 2;

    static final byte SUPERBLOCK = 1;
    static final byte LEAF = 2;
    static final byte BRANCH = 3;
    static final byte VALUE = 4;
    static final byte CATALOG = 5;
    static final byte FREE_LIST = 6;
    /// A value of the file of large values, `ledger.blobs`.
    static final byte EXTERNAL = 7;
    /// The delta records of several leaves ([DeltaLeaves]).
    static final byte DELTA = 8;
    /// The table of the delta pages in use, with the records each holds ([DeltaTable]).
    static final byte DELTA_TABLE = 9;

    /// The first page a unit may use; pages 0 and 1 are the superblocks.
    static final int FIRST_UNIT_PAGE = 2;

    static final int KIND = 0;
    static final int FORMAT = 1;
    static final int LEVEL = 2;
    static final int PAGES = 4;
    static final int PAGE = 8;
    static final int LENGTH = 12;
    static final int NONCE = 16;
    /// The header byte that describes the search index of a node of format [#INDEXED].
    static final int INDEX = 28;
    /// The least padding a unit the page cache takes must have for the cache to keep it without the padding. Opening
    /// a unit into an array of the pool and copying its content out costs what opening it into an array of its own
    /// does, which the JVM zeroes first, so the bound only spares the round trip through the pool for a few bytes: with
    /// a bound of 512, a warm store of chats would keep 1,519 of the 3,791 units it caches whole, 364,000 bytes of
    /// padding.
    static final int TRIM_LEAST = 64;
    /// The most pages of a unit the page cache keeps without its padding.
    static final int TRIMMED_PAGES = 16;

    private PageFormat() {
    }

    /// {@return the bytes of content a unit of `pages` pages holds}
    static int capacity(int pages) {
        return pages * SIZE - HEADER - TAG;
    }

    /// {@return whether a unit of `pages` pages holds `content` bytes of content}
    static boolean fits(int pages, int content) {
        return content >= 0 && content <= capacity(pages);
    }

    /// {@return the bytes the page cache keeps of a unit of `pages` pages with `content` bytes of content, which
    /// [fit][#fits(int, int)] them: its header and content if the unit has at most [#TRIMMED_PAGES] pages and that
    /// drops at least [#TRIM_LEAST] bytes, or the whole unit}
    ///
    /// The pages [#pagesFor(long)] gives pad a unit by less than a page and a tag, so one of more than 16 pages is kept
    /// whole: dropping its padding would copy all its content to save less than a sixteenth of it.
    static int cachedLength(int pages, int content) {
        var length = pages * SIZE;
        var kept = HEADER + content;
        return pages <= TRIMMED_PAGES && length - kept >= TRIM_LEAST ? kept : length;
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
